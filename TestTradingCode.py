import os
import requests
import pandas as pd
import numpy as np
import time
import logging
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "8298845980:AAFrhgrdngO6b1vV9poLyw7c_yT0afTkMg4")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1003853071475")
TOP_SYMBOLS_LIMIT = 70
PORT = int(os.environ.get("PORT", "8080"))
ALERT_EXPIRY_HOURS = 4

KUCOIN_BASE = "https://api.kucoin.com"
TF_MAP = {"1m": "1min", "60m": "1hour"}

TRIPLING_PAIRS = [
    (  9,  27,  3, "1m", "1m"),
    ( 12,  36,  4, "1m", "1m"),
    ( 15,  45,  5, "1m", "1m"),
    ( 18,  54,  6, "1m", "1m"),
    ( 21,  63,  7, "1m", "1m"),
    ( 24,  72,  8, "1m", "1m"),
    ( 27,  81,  9, "1m", "1m"),
    ( 30,  90, 10, "1m", "1m"),
    ( 45, 135, 15, "1m", "1m"),
    ( 60, 180, 20, "60m","1m"),
    ( 90, 270, 30, "60m","1m"),
    (120, 360, 40, "60m","1m"),
    (180, 540, 60, "60m","60m"),
]

# تسلسل الفريمات من الأصغر للأكبر — كل فريم يلغي الذي يليه فقط
TIMEFRAME_CHAIN = [9, 12, 15, 18, 21, 24, 27, 30, 45, 60, 90, 120, 180]
NEXT_TF = {TIMEFRAME_CHAIN[i]: TIMEFRAME_CHAIN[i+1] for i in range(len(TIMEFRAME_CHAIN)-1)}
# مثال: NEXT_TF[9] = 12, NEXT_TF[12] = 15, ...

FAST_FETCH_CANDLES = {"1m": 3500, "60m": 250}
API_FETCH_CANDLES  = {"1m": 15_000, "60m": 2_000}
CACHE_MAX_CANDLES  = {"1m": 16_000, "60m": 2_500}
EPOCH = pd.Timestamp("1970-01-01", tz="UTC")

alerted_keys       = {}
alerted_keys_lock  = threading.Lock()
trades_history     = deque(maxlen=2000)
trades_lock        = threading.Lock()
near_signals       = deque(maxlen=500)
near_signals_lock  = threading.Lock()
symbols_cache      = []
symbols_cache_lock = threading.Lock()
ohlcv_cache        = {}
ohlcv_cache_lock   = threading.Lock()

# ── حالة التشبع البيعي لكل عملة ──────────────────────────────────────────────
# smi_state[symbol] = entry_min اللي يجب مراقبته حالياً (None = لا يوجد)
smi_state      = {}
smi_state_lock = threading.Lock()

fast_prefetch_done = threading.Event()
prefetch_done      = threading.Event()

diag_counts = {
    "total": 0, "no_data": 0,
    "smi_oversold": 0,
    "macd_red": 0,
    "donchian_entry": 0,
    "donchian_confirm": 0,
    "macd_confirm": 0,
    "ema50": 0,
    "rsi_stoch": 0,
    "passed": 0,
}
diag_lock = threading.Lock()
cache_diag_logged = threading.Event()
_local = threading.local()

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def to_kucoin_symbol(symbol):
    if "-" in symbol: return symbol
    if symbol.endswith("USDT"): return symbol[:-4] + "-USDT"
    return symbol

def from_kucoin_symbol(symbol):
    return symbol.replace("-", "")

def get_session():
    if not hasattr(_local, "s"):
        s = requests.Session()
        s.headers.update({"Accept-Encoding": "gzip", "User-Agent": "Mozilla/5.0"})
        _local.s = s
    return _local.s

def delete_webhook():
    try:
        r = get_session().post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteWebhook",
            json={"drop_pending_updates": True}, timeout=10,
        ).json()
        if r.get("ok"): log.info("✅ تم حذف الـ Webhook")
        else: log.warning(f"⚠️ deleteWebhook: {r}")
    except Exception as e:
        log.error(f"deleteWebhook error: {e}")

def cleanup_alerted_keys():
    now = datetime.now(timezone.utc)
    with alerted_keys_lock:
        expired = [k for k, t in list(alerted_keys.items())
                   if now - t > timedelta(hours=ALERT_EXPIRY_HOURS)]
        for k in expired: del alerted_keys[k]

def save_signal(symbol, price, entry_min, confirm_min, third_min):
    with trades_lock:
        trades_history.append({
            "time"     : datetime.now(timezone.utc),
            "symbol"   : symbol,
            "price"    : price,
            "timeframe": f"{entry_min}m/{confirm_min}m/{third_min}m",
        })

def send_telegram(msg, chat_id=None):
    target = chat_id or TELEGRAM_CHAT_ID
    try:
        r = get_session().post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": target, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        ).json()
        if not r.get("ok"): log.error(f"Telegram error: {r}")
        return r.get("ok", False)
    except Exception as e:
        log.error(f"Telegram send error: {e}")
        return False

def get_report(period="today"):
    now = datetime.now(timezone.utc)
    if period == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end, title = now, "📅 إشارات اليوم"
    elif period == "yesterday":
        end   = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=1)
        title = "📅 إشارات أمس"
    else:
        start = now - timedelta(days=7)
        end, title = now, "🗓️ آخر 7 أيام"
    with trades_lock:
        rows = [t for t in trades_history if start <= t["time"] < end]
    if not rows: return f"<b>{title}:</b>\nلا توجد إشارات."
    lines = [f"<b>{title} ({len(rows)})</b>\n" + "━" * 15]
    for t in rows:
        lines.append(
            f"✅ {t['symbol']} | {t['timeframe']} | "
            f"{t['price']:.4g} | {t['time'].strftime('%H:%M UTC')}"
        )
    return "\n".join(lines)

# ──────────────────────────────────────────────
# OHLCV
# ──────────────────────────────────────────────
def _parse_klines(resp):
    df = pd.DataFrame(resp, columns=["ts","open","close","high","low","vol","turnover"])
    for c in ["open","high","low","close","vol"]: df[c] = df[c].astype(float)
    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="s", utc=True)
    return df.sort_values("ts").reset_index(drop=True)[["ts","open","high","low","close","vol"]]

def get_ohlcv(symbol, tf, limit=500):
    kc_sym  = to_kucoin_symbol(symbol)
    kc_tf   = TF_MAP.get(tf, "1min")
    end_sec = int(time.time())
    tf_sec  = 60 if tf == "1m" else 3600
    start_sec = end_sec - limit * tf_sec
    try:
        resp = get_session().get(
            f"{KUCOIN_BASE}/api/v1/market/candles",
            params={"symbol": kc_sym, "type": kc_tf,
                    "startAt": start_sec, "endAt": end_sec},
            timeout=10,
        ).json()
        data = resp.get("data", [])
        if data: return _parse_klines(data)
        log.warning(f"⚠️ رد غير صالح {symbol}/{tf}: {str(resp)[:120]}")
    except Exception as e:
        log.error(f"get_ohlcv {symbol} {tf}: {e}")
    return pd.DataFrame()

def get_ohlcv_full(symbol, tf, target):
    kc_sym  = to_kucoin_symbol(symbol)
    kc_tf   = TF_MAP.get(tf, "1min")
    tf_sec  = 60 if tf == "1m" else 3600
    KUCOIN_MAX = 1500
    all_dfs, end_sec, fetched, retries = [], int(time.time()), 0, 0
    while fetched < target:
        batch     = min(KUCOIN_MAX, target - fetched)
        start_sec = end_sec - batch * tf_sec
        try:
            resp = get_session().get(
                f"{KUCOIN_BASE}/api/v1/market/candles",
                params={"symbol": kc_sym, "type": kc_tf,
                        "startAt": start_sec, "endAt": end_sec},
                timeout=15,
            ).json()
            data = resp.get("data", [])
            if not data:
                retries += 1
                if retries >= 3: break
                time.sleep(2 ** retries); continue
            df = _parse_klines(data)
            all_dfs.insert(0, df)
            fetched += len(df); retries = 0; end_sec = start_sec - 1
            if len(df) < batch: break
            time.sleep(0.2)
        except requests.exceptions.Timeout:
            retries += 1
            if retries >= 3: break
            time.sleep(2 ** retries)
        except Exception as e:
            retries += 1
            log.error(f"full fetch {symbol}/{tf}: {e}")
            if retries >= 3: break
            time.sleep(2 ** retries)
    return (pd.concat(all_dfs).drop_duplicates(subset="ts")
            .sort_values("ts").reset_index(drop=True) if all_dfs else pd.DataFrame())

def cache_merge(symbol, tf, new_df):
    if new_df.empty: return
    key = (symbol, tf); maxc = CACHE_MAX_CANDLES.get(tf, 5000)
    with ohlcv_cache_lock:
        old = ohlcv_cache.get(key)
        if old is not None and not old.empty:
            merged = pd.concat([old, new_df]).drop_duplicates(subset="ts").sort_values("ts")
            ohlcv_cache[key] = merged.tail(maxc).reset_index(drop=True)
        else:
            ohlcv_cache[key] = new_df.tail(maxc).reset_index(drop=True)

def get_cached(symbol, tf):
    with ohlcv_cache_lock:
        df = ohlcv_cache.get((symbol, tf))
    return df.copy() if df is not None else pd.DataFrame()

# ──────────────────────────────────────────────
# Prefetch & Cache Update
# ──────────────────────────────────────────────
def prefetch_all(symbols):
    total = len(symbols)
    log.info(f"⚡ المرحلة الأولى (سريعة): {total} عملة")
    fast_success = fast_failed = 0
    for i, sym in enumerate(symbols):
        for tf, n in FAST_FETCH_CANDLES.items():
            fetched = False
            for attempt in range(3):
                try:
                    df = get_ohlcv_full(sym, tf, target=n)
                    if not df.empty:
                        cache_merge(sym, tf, df)
                        fast_success += 1; fetched = True; break
                    time.sleep(0.5 * (attempt + 1))
                except Exception as e:
                    log.error(f"fast prefetch {sym} {tf} ({attempt+1}): {e}")
                    time.sleep(attempt + 1)
            if not fetched: fast_failed += 1
            time.sleep(0.2)
        if (i + 1) % 10 == 0 or i == total - 1:
            log.info(f"⚡ سريع: {i+1}/{total} | نجح: {fast_success} | فشل: {fast_failed}")

    with ohlcv_cache_lock:
        fast_cache_size = len(ohlcv_cache)
        filled_1m = sum(1 for s in symbols
                        if (s, "1m") in ohlcv_cache and not ohlcv_cache[(s, "1m")].empty)

    if fast_cache_size == 0:
        log.error("❌ الكاش السريع فارغ!")
        send_telegram("❌ <b>فشل التحميل السريع!</b>\n💡 استخدم /fetchtest\n🔄 أرسل /reload")
        return

    fast_prefetch_done.set()
    log.info(f"✅ المرحلة الأولى اكتملت | كاش: {fast_cache_size}")
    send_telegram(
        f"⚡ <b>التحميل السريع اكتمل — الإشارات بدأت!</b>\n"
        f"📈 عملات: {total} | ✅ مكتملة: {filled_1m} | 💾 مفاتيح: {fast_cache_size}\n"
        f"❌ فشل: {fast_failed} | 📦 جاري تحميل البيانات الكاملة..."
    )

    log.info(f"📦 المرحلة الثانية (كاملة): {total} عملة")
    full_success = 0
    for i, sym in enumerate(symbols):
        for tf, n in API_FETCH_CANDLES.items():
            try:
                df = get_ohlcv_full(sym, tf, target=n)
                if not df.empty:
                    cache_merge(sym, tf, df); full_success += 1
                time.sleep(0.2)
            except Exception as e:
                log.error(f"full prefetch {sym} {tf}: {e}")
        if (i + 1) % 10 == 0 or i == total - 1:
            log.info(f"📦 كامل: {i+1}/{total}")

    with ohlcv_cache_lock:
        full_cache_size = len(ohlcv_cache)
        sample = list(ohlcv_cache.items())[:3]

    prefetch_done.set()
    diag_lines = ["🔬 <b>عينة من الكاش:</b>"]
    for (sym, tf), df in sample:
        diag_lines.append(f"• {sym} [{tf}]: {len(df)} شمعة")
    send_telegram(
        f"✅ <b>التحميل الكامل اكتمل</b>\n"
        f"📈 عملات: {total} | 💾 مفاتيح: {full_cache_size}\n"
        f"✔️ نجح: {full_success}/{total*2}\n\n" + "\n".join(diag_lines)
    )

def _update_batch(symbols, tf, limit):
    for sym in symbols:
        try:
            df = get_ohlcv(sym, tf, limit=limit)
            if not df.empty: cache_merge(sym, tf, df)
            time.sleep(0.15)
        except Exception as e:
            log.error(f"update {sym} {tf}: {e}")

def cache_updater_1m():
    while True:
        time.sleep(45)
        if fast_prefetch_done.is_set():
            with symbols_cache_lock: syms = list(symbols_cache)
            if syms: _update_batch(syms, "1m", limit=15)

def cache_updater_60m():
    while True:
        time.sleep(45 * 60)
        if fast_prefetch_done.is_set():
            with symbols_cache_lock: syms = list(symbols_cache)
            if syms: _update_batch(syms, "60m", limit=5)

# ──────────────────────────────────────────────
# Resample
# ──────────────────────────────────────────────
def resample_ohlcv(df, minutes):
    if df.empty or minutes <= 0: return pd.DataFrame()
    try:
        r = (df.copy().set_index("ts")
             .resample(f"{minutes}min", closed="left", label="left", origin=EPOCH)
             .agg({"open":"first","high":"max","low":"min","close":"last","vol":"sum"})
             .dropna())
        return r.iloc[:-1].reset_index()
    except Exception as e:
        log.error(f"resample error: {e}")
        return pd.DataFrame()

# ──────────────────────────────────────────────
# المؤشرات
# ──────────────────────────────────────────────

def check_macd_green(df):
    if len(df) < 35: return False
    c   = df["close"]
    ml  = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    sig = ml.ewm(span=9, adjust=False).mean()
    return bool((ml - sig).iloc[-1] > 0)

def check_macd_red(df):
    if len(df) < 35: return False
    c   = df["close"]
    ml  = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    sig = ml.ewm(span=9, adjust=False).mean()
    return bool((ml - sig).iloc[-1] < 0)

def _dchannel_trend(closes, hh_prev, ll_prev):
    n = len(closes)
    trend = np.zeros(n, dtype=np.int8)
    for i in range(1, n):
        if np.isnan(hh_prev[i]) or np.isnan(ll_prev[i]):
            trend[i] = 0
        elif closes[i] > hh_prev[i]:
            trend[i] = 1
        elif closes[i] < ll_prev[i]:
            trend[i] = -1
        else:
            trend[i] = trend[i - 1]
    return trend

def check_donchian_ribbon(df, length=20, direction="green"):
    min_len = length + 2
    if len(df) < min_len: return False
    closes = df["close"].values
    highs  = df["high"].values
    lows   = df["low"].values
    def get_trend(ln):
        hh = pd.Series(highs).rolling(ln).max().shift(1).values
        ll = pd.Series(lows).rolling(ln).min().shift(1).values
        return int(_dchannel_trend(closes, hh, ll)[-1])
    main_trend = get_trend(length)
    sub_trends = [get_trend(l) for l in range(length - 1, max(length - 10, 4), -1)]
    if direction == "green":
        return main_trend == 1 and sum(t == 1 for t in sub_trends) >= 5
    else:
        return main_trend == -1 and sum(t == -1 for t in sub_trends) >= 5

def check_ema50_below(df):
    if len(df) < 50: return False
    ema = df["close"].ewm(span=50, adjust=False).mean()
    return bool(df["close"].iloc[-1] < ema.iloc[-1])

def calc_smi(high, low, close, k=10, d=3, ema=10, smooth=1):
    hh  = high.rolling(k).max()
    ll  = low.rolling(k).min()
    mid = (hh + ll) / 2
    ds  = (close - mid).ewm(span=d, adjust=False).mean().ewm(span=d, adjust=False).mean()
    hls = ((hh - ll) / 2).ewm(span=d, adjust=False).mean().ewm(span=d, adjust=False).mean()
    smi = 200 * ds / (hls.abs() + 1e-10)
    if smooth > 1: smi = smi.rolling(smooth).mean()
    sig = smi.ewm(span=ema, adjust=False).mean()
    return smi, sig

def check_smi_oversold(df, threshold=-40, lookback=5):
    if len(df) < 30: return False
    smi, _ = calc_smi(df["high"], df["low"], df["close"])
    return bool(smi.iloc[-lookback:].min() <= threshold)

def wilder_rma(series, period):
    return series.ewm(alpha=1/period, adjust=False).mean()

def calc_rsi_tv(close, period=14):
    delta = close.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta.clip(upper=0))
    return 100 - (100 / (1 + wilder_rma(gain, period) / (wilder_rma(loss, period) + 1e-10)))

def check_rsi_stoch(df, lookback=5):
    if len(df) < 50: return False
    close, high, low = df["close"], df["high"], df["low"]
    rsi    = calc_rsi_tv(close, period=14)
    rsi_ma = rsi.rolling(14).mean()
    if rsi.iloc[-20:].min() > 35: return False
    rsi_cross = any(
        rsi.iloc[i-1] < rsi_ma.iloc[i-1] and rsi.iloc[i] >= rsi_ma.iloc[i]
        for i in range(-10, 0)
    )
    if not rsi_cross: return False
    lo15  = low.rolling(15).min()
    hi15  = high.rolling(15).max()
    k_raw = 100 * (close - lo15) / (hi15 - lo15 + 1e-10)
    k     = k_raw.rolling(3).mean()
    return any(k.iloc[i-1] < 20 and k.iloc[i] >= 20 for i in range(-lookback, 0))

# ──────────────────────────────────────────────
# منطق تتابع الفريمات (الجديد)
# ──────────────────────────────────────────────
def on_smi_oversold(symbol, entry_min):
    """
    لما يصير تشبع بيعي في entry_min:
    - يلغي الفريم الأكبر التالي فقط (NEXT_TF[entry_min])
    - يحفظ entry_min كالفريم النشط للعملة
    """
    next_tf = NEXT_TF.get(entry_min)  # الفريم الأكبر الذي يُلغى
    with smi_state_lock:
        current = smi_state.get(symbol)
        # لو كان ينتظر فريم أكبر، ألغِه وانزل للأصغر
        if current is None or current >= entry_min:
            smi_state[symbol] = entry_min
            if next_tf:
                log.info(f"🔄 {symbol}: تشبع بيعي {entry_min}m → ألغى {next_tf}m وبدأ مراقبة {entry_min}m")

def get_active_entry(symbol):
    """يرجع الـ entry_min النشط للعملة، أو None"""
    with smi_state_lock:
        return smi_state.get(symbol)

def clear_active_entry(symbol):
    """بعد الإشارة أو انتهاء الصلاحية، امسح الحالة"""
    with smi_state_lock:
        smi_state.pop(symbol, None)

# ──────────────────────────────────────────────
# Diagnostics
# ──────────────────────────────────────────────
DIAG_LABELS = {
    "no_data"         : "بيانات ناقصة",
    "smi_oversold"    : "SMI مش في التشبع البيعي",
    "macd_red"        : "MACD الرئيسي مش أحمر",
    "donchian_entry"  : "Donchian Ribbon الرئيسي مش أخضر",
    "donchian_confirm": "Donchian Ribbon Confirm مش أخضر",
    "macd_confirm"    : "MACD Confirm مش أخضر (×3)",
    "ema50"           : "السعر فوق EMA50",
    "rsi_stoch"       : "RSI/Stochastic ما اتحقق",
}

DIAG_PASS_LABELS = {
    "no_data"         : "بيانات كافية ✅",
    "smi_oversold"    : "SMI في التشبع البيعي ✅",
    "macd_red"        : "MACD الرئيسي أحمر ✅",
    "donchian_entry"  : "Donchian Ribbon الرئيسي أخضر ✅",
    "donchian_confirm": "Donchian Ribbon Confirm أخضر ✅",
    "macd_confirm"    : "MACD Confirm أخضر (×3) ✅",
    "ema50"           : "السعر تحت EMA50 ✅",
    "rsi_stoch"       : "RSI/Stochastic اتحقق ✅",
}

def build_diag_msg(reset=False):
    with diag_lock:
        t = diag_counts["total"] or 1
        non_total = {k: v for k, v in diag_counts.items() if k not in ["total", "passed"]}
        worst_k = max(non_total, key=lambda k: non_total[k])
        worst_v = non_total[worst_k]
        lines = [
            "🔍 <b>تقرير التشخيص</b>", "━━━━━━━━━━━━━━━",
            f"📊 إجمالي الفحوصات: <b>{t}</b>", "",
        ]
        remaining = t
        for k, fail_label in DIAG_LABELS.items():
            failed  = diag_counts[k]
            passed  = remaining - failed
            pass_pct = int(passed / t * 100)
            fail_pct = int(failed / t * 100)
            bar = "█" * (pass_pct // 10) + "░" * (10 - pass_pct // 10)
            lines.append(
                f"✅ {DIAG_PASS_LABELS[k]}\n"
                f"    {bar} نجح: {passed} ({pass_pct}%) | فشل: {failed} ({fail_pct}%)"
            )
            remaining = passed
        lines += [
            "", f"🏆 اجتازت الكل: <b>{diag_counts['passed']}</b>",
            "━━━━━━━━━━━━━━━",
            f"⚠️ أكثر سبب فشل: <b>{DIAG_LABELS.get(worst_k, worst_k)}</b> ({worst_v})",
        ]
        if reset:
            for k in diag_counts: diag_counts[k] = 0
    return "\n".join(lines)

def send_diag_report():
    while True:
        time.sleep(3600)
        send_telegram(build_diag_msg(reset=True))

# ──────────────────────────────────────────────
# ✅ scan_symbol — مع منطق تتابع الفريمات
# ──────────────────────────────────────────────
def scan_symbol(symbol, entry_min, confirm_min, third_min, ec_api, t_api):
    """
    المنطق الجديد:
    1. دائماً نفحص SMI على فريم entry_min
    2. لو تشبع بيعي → نسجل الحالة ونلغي الفريم الأكبر التالي
    3. لو العملة عندها حالة نشطة تساوي entry_min → نكمل باقي الشروط
    4. لو الحالة النشطة أصغر من entry_min → هذا الواتشر متجاوَز، نتجاهل
    """
    raw_ec = get_cached(symbol, ec_api)
    raw_t  = get_cached(symbol, t_api)

    if not cache_diag_logged.is_set():
        log.info(f"🔍 كاش | {symbol} | ec[{ec_api}]={len(raw_ec)} | t[{t_api}]={len(raw_t)}")
        cache_diag_logged.set()

    with diag_lock: diag_counts["total"] += 1

    if raw_ec.empty or len(raw_ec) < 50 or raw_t.empty or len(raw_t) < 50:
        with diag_lock: diag_counts["no_data"] += 1
        return

    df_entry   = resample_ohlcv(raw_ec, entry_min)
    df_confirm = resample_ohlcv(raw_ec, confirm_min)
    df_third   = resample_ohlcv(raw_t,  third_min)

    if any(d.empty or len(d) < 25 for d in [df_entry, df_confirm, df_third]):
        with diag_lock: diag_counts["no_data"] += 1
        return

    # ① SMI تشبع بيعي — دائماً نفحصه ونحدّث الحالة
    smi_ok = check_smi_oversold(df_entry)
    if smi_ok:
        on_smi_oversold(symbol, entry_min)
    else:
        with diag_lock: diag_counts["smi_oversold"] += 1

    # لو الحالة النشطة للعملة ليست هذا الفريم → توقف
    # (إما لم يصير تشبع بعد، أو فريم أصغر أخذ الأولوية)
    active = get_active_entry(symbol)
    if active != entry_min:
        return

    # ② MACD أحمر على فريم الدخول
    if not check_macd_red(df_entry):
        with diag_lock: diag_counts["macd_red"] += 1
        return

    # ③ Donchian Trend Ribbon أخضر على فريم الدخول
    if not check_donchian_ribbon(df_entry, direction="green"):
        with diag_lock: diag_counts["donchian_entry"] += 1
        return

    # ④ Donchian Trend Ribbon أخضر على فريم التأكيد (×3)
    if not check_donchian_ribbon(df_confirm, direction="green"):
        with diag_lock: diag_counts["donchian_confirm"] += 1
        return

    # ⑤ MACD أخضر على فريم التأكيد (×3)
    if not check_macd_green(df_confirm):
        with diag_lock: diag_counts["macd_confirm"] += 1
        return

    # ⑥ EMA50 — السعر تحت الخط
    if not check_ema50_below(df_entry):
        with diag_lock: diag_counts["ema50"] += 1
        return

    # ⑦ RSI تقاطع + Stochastic على فريم الدخول (÷3)
    if not check_rsi_stoch(df_third):
        with diag_lock: diag_counts["rsi_stoch"] += 1
        price = df_entry["close"].iloc[-1]
        with near_signals_lock:
            near_signals.append({
                "time"  : datetime.now(timezone.utc),
                "symbol": symbol,
                "price" : price,
                "tf"    : f"{entry_min}m/{confirm_min}m/{third_min}m",
            })
        return

    # ✅ كل الشروط اتحققت
    with diag_lock: diag_counts["passed"] += 1

    last_ts = df_entry["ts"].iloc[-1].strftime("%Y%m%d%H%M") if "ts" in df_entry.columns else "x"
    key = f"{symbol}_{entry_min}_{last_ts}"

    with alerted_keys_lock:
        if key in alerted_keys: return
        alerted_keys[key] = datetime.now(timezone.utc)

    # امسح الحالة بعد الإشارة حتى ينتظر تشبع بيعي جديد
    clear_active_entry(symbol)

    price       = df_entry["close"].iloc[-1]
    now_utc     = datetime.now(timezone.utc)
    candle_time = df_entry["ts"].iloc[-1].strftime("%Y-%m-%d %H:%M UTC")
    signal_time = now_utc.strftime("%Y-%m-%d %H:%M UTC")

    save_signal(symbol, price, entry_min, confirm_min, third_min)
    send_telegram(
        f"🚨 <b>إشارة دخول</b>\n"
        f"🪙 <b>{symbol}</b>\n"
        f"🎯 الدخول: <b>{entry_min}m</b> | تأكيد: {confirm_min}m | ثالث: {third_min}m\n"
        f"💰 السعر: {price:.6g}\n"
        f"🕯️ إغلاق الشمعة: {candle_time}\n"
        f"🕐 وقت الإشارة: {signal_time}"
    )

# ──────────────────────────────────────────────
# Candle Watcher
# ──────────────────────────────────────────────
def get_next_close(tf_minutes):
    now   = datetime.now(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    total = int((now - epoch).total_seconds() / 60)
    return epoch + timedelta(minutes=((total // tf_minutes) + 1) * tf_minutes)

def candle_watcher(entry_min, confirm_min, third_min, ec_api, t_api):
    while True:
        nxt  = get_next_close(entry_min)
        wait = (nxt - datetime.now(timezone.utc)).total_seconds()
        time.sleep(max(wait, 0) + 2.0)
        cleanup_alerted_keys()
        if not fast_prefetch_done.is_set(): continue
        with symbols_cache_lock: syms = list(symbols_cache)
        if not syms: continue
        cache_diag_logged.clear()
        fn = partial(
            scan_symbol,
            entry_min=entry_min, confirm_min=confirm_min,
            third_min=third_min, ec_api=ec_api, t_api=t_api,
        )
        with ThreadPoolExecutor(max_workers=4) as ex:
            ex.map(fn, syms)

# ──────────────────────────────────────────────
# Telegram Commands
# ──────────────────────────────────────────────
def poll_telegram_commands():
    last_id = 0
    log.info("📡 بدء الاستماع لأوامر التلقرام…")
    while True:
        try:
            r = get_session().get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={"offset": last_id + 1, "timeout": 30},
                timeout=35,
            ).json()
            if not r.get("ok"):
                time.sleep(5); continue
            for upd in r.get("result", []):
                last_id = upd["update_id"]
                msg     = upd.get("message", {})
                txt     = msg.get("text", "").strip()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if not txt or not chat_id: continue
                log.info(f"📩 أمر: '{txt}' من {chat_id}")

                if txt == "1":   send_telegram(get_report("today"), chat_id)
                elif txt == "2": send_telegram(get_report("yesterday"), chat_id)
                elif txt == "3": send_telegram(get_report("week"), chat_id)
                elif txt in ("/سبب", "/diag"):
                    send_telegram(
                        "⚠️ لا توجد بيانات." if diag_counts["total"] == 0
                        else build_diag_msg(reset=False), chat_id
                    )
                elif txt == "/top6":
                    with near_signals_lock:
                        rows = list(near_signals)[-30:]
                    if not rows:
                        send_telegram("⏳ لا توجد عملات وصلت للشرط السادس بعد.", chat_id)
                    else:
                        lines = [f"<b>🎯 عملات اجتازت 6 شروط — RSI/Stoch فقط باقي ({len(rows)}):</b>\n" + "━" * 15]
                        for row in reversed(rows):
                            lines.append(
                                f"🔸 {row['symbol']} | {row['tf']} | "
                                f"{row['price']:.6g} | {row['time'].strftime('%H:%M UTC')}"
                            )
                        send_telegram("\n".join(lines), chat_id)
                elif txt == "/state":
                    with smi_state_lock:
                        active_states = dict(smi_state)
                    if not active_states:
                        send_telegram("📭 لا توجد عملات في حالة انتظار حالياً.", chat_id)
                    else:
                        lines = [f"<b>📊 حالات التشبع البيعي النشطة ({len(active_states)}):</b>\n" + "━" * 15]
                        for sym, tf_min in sorted(active_states.items(), key=lambda x: x[1]):
                            lines.append(f"🔸 {sym} → ينتظر دخول على <b>{tf_min}m</b>")
                        send_telegram("\n".join(lines), chat_id)
                elif txt == "/status":
                    with trades_lock:       cnt    = len(trades_history)
                    with alerted_keys_lock: active = len(alerted_keys)
                    with ohlcv_cache_lock:  keys   = len(ohlcv_cache)
                    with near_signals_lock: near   = len(near_signals)
                    with smi_state_lock:    states = len(smi_state)
                    send_telegram(
                        f"🤖 البوت يعمل — KuCoin API\n"
                        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
                        f"📊 إجمالي الإشارات: {cnt}\n"
                        f"🎯 قريبة من الإشارة (6/7): {near}\n"
                        f"🔄 عملات في حالة انتظار: {states}\n"
                        f"🔑 تنبيهات نشطة: {active}\n"
                        f"💾 الكاش: {keys} مفتاح\n"
                        f"⚡ تحميل سريع: {'✅' if fast_prefetch_done.is_set() else '⏳'}\n"
                        f"📦 تحميل كامل: {'✅' if prefetch_done.is_set() else '⏳'}",
                        chat_id,
                    )
                elif txt == "/cache":
                    with ohlcv_cache_lock:
                        sample = list(ohlcv_cache.items())[:5]
                        total_keys = len(ohlcv_cache)
                    lines = [f"🔬 <b>الكاش ({total_keys} مفتاح):</b>"]
                    for (sym, tf), df in sample:
                        lines.append(f"• {sym} [{tf}]: {len(df)} شمعة")
                    send_telegram("\n".join(lines), chat_id)
                elif txt == "/fetchtest":
                    send_telegram("🔄 جاري اختبار KuCoin API...", chat_id)
                    results = []
                    for tf in ["1m", "60m"]:
                        df = get_ohlcv("BTCUSDT", tf, limit=10)
                        results.append(
                            f"✅ BTCUSDT [{tf}]: {len(df)} شمعة — آخر سعر {df['close'].iloc[-1]:.4g}"
                            if not df.empty else f"❌ BTCUSDT [{tf}]: فشل"
                        )
                    with ohlcv_cache_lock: cache_keys = len(ohlcv_cache)
                    results += [f"💾 الكاش: {cache_keys} مفتاح",
                                f"⚡ {'✅' if fast_prefetch_done.is_set() else '⏳'}"]
                    send_telegram("🧪 <b>نتيجة اختبار KuCoin:</b>\n" + "\n".join(results), chat_id)
                elif txt == "/reload":
                    with symbols_cache_lock: syms = list(symbols_cache)
                    if not syms:
                        send_telegram("⚠️ لا توجد عملات.", chat_id)
                    else:
                        fast_prefetch_done.clear(); prefetch_done.clear()
                        # امسح حالات التشبع عند إعادة التحميل
                        with smi_state_lock: smi_state.clear()
                        threading.Thread(target=prefetch_all, args=(syms,), daemon=True).start()
                        send_telegram(f"🚀 بدأ إعادة التحميل لـ {len(syms)} عملة...", chat_id)
                elif txt == "/help":
                    send_telegram(
                        "📋 <b>الأوامر المتاحة:</b>\n"
                        "1️⃣  <code>1</code> — إشارات اليوم\n"
                        "2️⃣  <code>2</code> — إشارات أمس\n"
                        "3️⃣  <code>3</code> — آخر 7 أيام\n"
                        "🎯  <code>/top6</code> — عملات اجتازت 6 شروط\n"
                        "📊  <code>/state</code> — عملات في حالة انتظار\n"
                        "📊  <code>/status</code> — حالة البوت\n"
                        "🔬  <code>/cache</code> — فحص الكاش\n"
                        "🧪  <code>/fetchtest</code> — اختبار KuCoin\n"
                        "🔄  <code>/reload</code> — إعادة تحميل الكاش\n"
                        "🔍  <code>/سبب</code> — تشخيص الإشارات\n"
                        "📋  <code>/help</code> — قائمة الأوامر",
                        chat_id,
                    )
        except requests.exceptions.Timeout:
            log.warning("⏱️ getUpdates timeout")
        except Exception as e:
            log.error(f"Polling error: {e}")
            time.sleep(10)

# ──────────────────────────────────────────────
# Symbols Loop
# ──────────────────────────────────────────────
def update_symbols_loop():
    first = True
    while True:
        try:
            resp = get_session().get(
                f"{KUCOIN_BASE}/api/v1/market/allTickers", timeout=20,
            ).json()
            tickers = resp.get("data", {}).get("ticker", [])
            if tickers:
                top = sorted(
                    [t for t in tickers if t["symbol"].endswith("-USDT")],
                    key=lambda x: float(x.get("volValue", 0)), reverse=True,
                )[:TOP_SYMBOLS_LIMIT]
                new_syms = [from_kucoin_symbol(t["symbol"]) for t in top]
                with symbols_cache_lock:
                    symbols_cache.clear(); symbols_cache.extend(new_syms)
                log.info(f"✅ عملات: {len(new_syms)} — أول 5: {new_syms[:5]}")
                if first:
                    first = False
                    threading.Thread(target=prefetch_all, args=(list(new_syms),), daemon=True).start()
            else:
                if first: _start_fallback(); first = False
        except Exception as e:
            log.error(f"Symbols loop error: {e}")
            if first: _start_fallback(); first = False
        time.sleep(3600)

def _start_fallback():
    fallback = ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
                "DOGEUSDT","ADAUSDT","AVAXUSDT","DOTUSDT","MATICUSDT",
                "LINKUSDT","UNIUSDT","LTCUSDT","ATOMUSDT","NEARUSDT"]
    with symbols_cache_lock:
        symbols_cache.clear(); symbols_cache.extend(fallback)
    log.warning(f"⚠️ fallback: {len(fallback)} عملة")
    threading.Thread(target=prefetch_all, args=(list(fallback),), daemon=True).start()

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
    def log_message(self, *_): pass

# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    log.info("🚀 Tripling Strategy Bot — KuCoin API")
    delete_webhook()
    threading.Thread(target=update_symbols_loop, daemon=True).start()
    while not symbols_cache: time.sleep(1)
    threading.Thread(target=poll_telegram_commands, daemon=True).start()
    threading.Thread(target=cache_updater_1m,       daemon=True).start()
    threading.Thread(target=cache_updater_60m,      daemon=True).start()
    threading.Thread(target=send_diag_report,       daemon=True).start()
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", PORT), HealthHandler).serve_forever(),
        daemon=True,
    ).start()
    log.info("⏳ انتظار اكتمال التحميل السريع…")
    fast_prefetch_done.wait()
    log.info("✅ بدء الواتشرز")
    for entry_min, confirm_min, third_min, ec_api, t_api in TRIPLING_PAIRS:
        threading.Thread(
            target=candle_watcher,
            args=(entry_min, confirm_min, third_min, ec_api, t_api),
            daemon=True,
        ).start()
    log.info("✅ جميع الواتشرز تعمل.")
    while True: time.sleep(60)

if __name__ == "__main__":
    main()