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

# ──────────────────────────────────────────────
# ⚙️ الإعدادات الرئيسية
# ──────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "ضع_توكن_تلقرام_هنا")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "ضع_chat_id_هنا")

# MEXC API — للقراءة العامة لا يحتاج مفتاح
MEXC_BASE         = "https://api.mexc.com"
TOP_SYMBOLS_LIMIT = 200          # ← رفعنا من 100 إلى 200
PORT              = int(os.environ.get("PORT", "8080"))
ALERT_EXPIRY_HOURS = 4
NEAR6_EXPIRY_HOURS = 2

# ──────────────────────────────────────────────
# تعيين الفريمات — MEXC يستخدم "1m" و "60m" مباشرة
# ──────────────────────────────────────────────
TF_MAP = {"1m": "1m", "60m": "60m"}

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

TIMEFRAME_CHAIN = [9, 12, 15, 18, 21, 24, 27, 30, 45, 60, 90, 120, 180]
NEXT_TF = {TIMEFRAME_CHAIN[i]: TIMEFRAME_CHAIN[i+1] for i in range(len(TIMEFRAME_CHAIN)-1)}

FAST_FETCH_CANDLES = {"1m": 3500, "60m": 250}
API_FETCH_CANDLES  = {"1m": 15_000, "60m": 2_000}
CACHE_MAX_CANDLES  = {"1m": 16_000, "60m": 2_500}
EPOCH = pd.Timestamp("1970-01-01", tz="UTC")

WARMUP_EMA   = 200
WARMUP_MACD  = 200
WARMUP_SMI   = 100
WARMUP_RSI   = 200
WARMUP_STOCH = 100
WARMUP_DON   = 50
MIN_CANDLES  = 250

# ──────────────────────────────────────────────
# الحالة المشتركة
# ──────────────────────────────────────────────
alerted_keys        = {}
alerted_keys_lock   = threading.Lock()
trades_history      = deque(maxlen=2000)
trades_lock         = threading.Lock()
near_signals        = {}
near_signals_lock   = threading.Lock()
near_signals_6      = {}
near_signals_6_lock = threading.Lock()
symbols_cache       = []
symbols_cache_lock  = threading.Lock()
ohlcv_cache         = {}
ohlcv_cache_lock    = threading.Lock()

fast_prefetch_done = threading.Event()
prefetch_done      = threading.Event()

diag_counts = {
    "total"           : 0,
    "no_data"         : 0,
    "smi_oversold"    : 0,
    "active_skip"     : 0,
    "macd_red"        : 0,
    "donchian_entry"  : 0,
    "donchian_confirm": 0,
    "macd_confirm"    : 0,
    "ema50"           : 0,
    "rsi_stoch"       : 0,
    "passed"          : 0,
}
diag_lock         = threading.Lock()
cache_diag_logged = threading.Event()
_local            = threading.local()

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
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

def cleanup_near6():
    now = datetime.now(timezone.utc)
    with near_signals_6_lock:
        expired = [k for k, v in list(near_signals_6.items())
                   if now - v["time"] > timedelta(hours=NEAR6_EXPIRY_HOURS)]
        for k in expired: del near_signals_6[k]

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
# ✅ MEXC OHLCV — Parser
# ──────────────────────────────────────────────
def _parse_mexc_klines(resp):
    """
    MEXC format:
    [open_time, open, high, low, close, volume,
     close_time, quote_vol, trades,
     taker_buy_base, taker_buy_quote, ignore]
    الـ timestamp بالـ milliseconds
    """
    df = pd.DataFrame(resp, columns=[
        "ts", "open", "high", "low", "close", "vol",
        "close_time", "quote_vol", "trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    for c in ["open", "high", "low", "close", "vol"]:
        df[c] = df[c].astype(float)
    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms", utc=True)
    return df.sort_values("ts").reset_index(drop=True)[
        ["ts", "open", "high", "low", "close", "vol"]
    ]

# ──────────────────────────────────────────────
# ✅ MEXC get_ohlcv
# ──────────────────────────────────────────────
def get_ohlcv(symbol, tf, limit=500):
    """جلب بيانات OHLCV من MEXC — الرمز بدون شرطة مثل BTCUSDT"""
    mexc_tf = TF_MAP.get(tf, "1m")
    try:
        resp = get_session().get(
            f"{MEXC_BASE}/api/v3/klines",
            params={
                "symbol"  : symbol,
                "interval": mexc_tf,
                "limit"   : min(limit, 1000),   # MEXC max per request = 1000
            },
            timeout=10,
        ).json()
        if isinstance(resp, list) and resp:
            return _parse_mexc_klines(resp)
        log.warning(f"⚠️ رد غير صالح {symbol}/{tf}: {str(resp)[:120]}")
    except Exception as e:
        log.error(f"get_ohlcv {symbol} {tf}: {e}")
    return pd.DataFrame()

# ──────────────────────────────────────────────
# ✅ MEXC get_ohlcv_full — جلب تاريخ طويل
# ──────────────────────────────────────────────
def get_ohlcv_full(symbol, tf, target):
    mexc_tf  = TF_MAP.get(tf, "1m")
    tf_ms    = 60_000 if tf == "1m" else 3_600_000   # milliseconds
    MEXC_MAX = 1000
    all_dfs, end_ms, fetched, retries = [], int(time.time() * 1000), 0, 0

    while fetched < target:
        batch    = min(MEXC_MAX, target - fetched)
        start_ms = end_ms - batch * tf_ms
        try:
            resp = get_session().get(
                f"{MEXC_BASE}/api/v3/klines",
                params={
                    "symbol"   : symbol,
                    "interval" : mexc_tf,
                    "startTime": start_ms,
                    "endTime"  : end_ms,
                    "limit"    : batch,
                },
                timeout=15,
            ).json()
            if not isinstance(resp, list) or not resp:
                retries += 1
                if retries >= 3: break
                time.sleep(2 ** retries); continue
            df = _parse_mexc_klines(resp)
            all_dfs.insert(0, df)
            fetched += len(df); retries = 0
            end_ms = start_ms - 1
            if len(df) < batch: break
            time.sleep(0.15)
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

# ──────────────────────────────────────────────
# Cache helpers
# ──────────────────────────────────────────────
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

# ──────────────────────────────────────────────
# ✅ تحديث الكاش المتوازي — بدلاً من التسلسلي
# ──────────────────────────────────────────────
def _update_batch(symbols, tf, limit):
    """تحديث متوازي بـ 30 worker — 200 عملة في 3-5 ثوانٍ"""
    def fetch_one(sym):
        try:
            df = get_ohlcv(sym, tf, limit=limit)
            if not df.empty:
                cache_merge(sym, tf, df)
        except Exception as e:
            log.error(f"update {sym} {tf}: {e}")

    with ThreadPoolExecutor(max_workers=30) as ex:
        ex.map(fetch_one, symbols)

def cache_updater_1m():
    """
    ✅ تحديث ذكي للكاش:
    - قبل 10 ثوانٍ من إغلاق الشمعة: يبدأ التحديث
    - يضمن أن الكاش جاهز تماماً عند الفحص
    """
    while True:
        if not fast_prefetch_done.is_set():
            time.sleep(5)
            continue

        # احسب متى تغلق الشمعة القادمة
        nxt  = get_next_close(1)
        wait = (nxt - datetime.now(timezone.utc)).total_seconds()

        # ابدأ التحديث قبل 10 ثوانٍ من الإغلاق
        pre_update = wait - 10
        if pre_update > 0:
            time.sleep(pre_update)

        # الآن حدّث الكاش بشكل متوازي
        with symbols_cache_lock: syms = list(symbols_cache)
        if syms:
            log.info(f"🔄 تحديث 1m لـ {len(syms)} عملة (قبل إغلاق الشمعة)")
            _update_batch(syms, "1m", limit=5)

        # انتظر بقية الوقت حتى إغلاق الشمعة + ثانية واحدة
        remaining = (nxt - datetime.now(timezone.utc)).total_seconds() + 1.0
        if remaining > 0:
            time.sleep(remaining)

def cache_updater_60m():
    while True:
        time.sleep(45 * 60)
        if fast_prefetch_done.is_set():
            with symbols_cache_lock: syms = list(symbols_cache)
            if syms:
                _update_batch(syms, "60m", limit=5)

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
# Indicators
# ──────────────────────────────────────────────
def wilder_rma(series, period):
    return series.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

def _calc_macd_hist(close):
    ml  = close.ewm(span=12, min_periods=12, adjust=False).mean() \
        - close.ewm(span=26, min_periods=26, adjust=False).mean()
    sig = ml.ewm(span=9, min_periods=9, adjust=False).mean()
    return ml - sig

def _calc_macd_full(close):
    macd_line   = close.ewm(span=12, min_periods=12, adjust=False).mean() \
                - close.ewm(span=26, min_periods=26, adjust=False).mean()
    signal_line = macd_line.ewm(span=9, min_periods=9, adjust=False).mean()
    histogram   = macd_line - signal_line
    return macd_line, signal_line, histogram

def check_macd_green(df):
    if len(df) < WARMUP_MACD: return False
    hist = _calc_macd_hist(df["close"])
    return bool(hist.iloc[-2] > 0)

def check_macd_red(df):
    if len(df) < WARMUP_MACD: return False
    hist = _calc_macd_hist(df["close"])
    return bool(hist.iloc[-2] < 0)

def _dchannel_trend(closes, hh_prev, ll_prev):
    n     = len(closes)
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
    if len(df) < WARMUP_DON + length + 3: return False
    closes = df["close"].values
    highs  = df["high"].values
    lows   = df["low"].values

    def get_trend(ln):
        hh = pd.Series(highs).rolling(ln, min_periods=ln).max().shift(1).values
        ll = pd.Series(lows).rolling(ln, min_periods=ln).min().shift(1).values
        tr = _dchannel_trend(closes, hh, ll)
        return int(tr[-2])

    main_trend = get_trend(length)
    sub_trends = [get_trend(l) for l in range(length - 1, max(length - 10, 4), -1)]
    if direction == "green":
        return main_trend == 1 and sum(t == 1 for t in sub_trends) >= 5
    else:
        return main_trend == -1 and sum(t == -1 for t in sub_trends) >= 5

def check_ema50_below(df):
    if len(df) < WARMUP_EMA: return False
    ema = df["close"].ewm(span=50, min_periods=50, adjust=False).mean()
    return bool(df["close"].iloc[-2] < ema.iloc[-2])

def calc_smi(high, low, close, k=10, d=3, ema_len=10, smooth=1):
    hh    = high.rolling(k, min_periods=k).max()
    ll    = low.rolling(k, min_periods=k).min()
    diff  = hh - ll
    rdiff = close - (hh + ll) / 2
    avgrel  = rdiff.ewm(span=d, min_periods=d, adjust=False).mean()
    avgdiff = diff.ewm(span=d, min_periods=d, adjust=False).mean()
    smi_arr = np.where(avgdiff != 0, (avgrel / (avgdiff / 2)) * 100, 0.0)
    smi     = pd.Series(smi_arr, index=close.index)
    if smooth > 1:
        smi = smi.rolling(smooth, min_periods=smooth).mean()
    sig = smi.ewm(span=ema_len, min_periods=ema_len, adjust=False).mean()
    return smi, sig

def check_smi_oversold(df, threshold=-40):
    if len(df) < WARMUP_SMI: return False
    smi, _ = calc_smi(df["high"], df["low"], df["close"])
    return bool(smi.iloc[-2] <= threshold)

def get_smi_value(df):
    if len(df) < WARMUP_SMI: return None, None
    smi, sig = calc_smi(df["high"], df["low"], df["close"])
    return round(float(smi.iloc[-2]), 2), round(float(sig.iloc[-2]), 2)

def calc_rsi_tv(close, period=14):
    delta = close.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta.clip(upper=0))
    up    = wilder_rma(gain, period)
    down  = wilder_rma(loss, period)
    return 100.0 - (100.0 / (1.0 + up / (down + 1e-10)))

def calc_stoch_tv(close, high, low, k_len=15, k_smooth=3, d_smooth=3):
    lo  = low.rolling(k_len, min_periods=k_len).min()
    hi  = high.rolling(k_len, min_periods=k_len).max()
    raw = 100.0 * (close - lo) / (hi - lo + 1e-10)
    k   = raw.rolling(k_smooth, min_periods=k_smooth).mean()
    d   = k.rolling(d_smooth, min_periods=d_smooth).mean()
    return k, d

def check_rsi_stoch(df, lookback=10):
    if len(df) < WARMUP_RSI: return False
    close = df["close"]
    high  = df["high"]
    low   = df["low"]
    rsi    = calc_rsi_tv(close, period=14)
    rsi_ma = rsi.rolling(14, min_periods=14).mean()
    if rsi.iloc[-20:-1].min() > 35:
        return False
    rsi_cross = any(
        rsi.iloc[i-1] < rsi_ma.iloc[i-1] and rsi.iloc[i] >= rsi_ma.iloc[i]
        for i in range(-lookback, -1)
    )
    if not rsi_cross:
        return False
    k, d = calc_stoch_tv(close, high, low, k_len=15, k_smooth=3, d_smooth=3)
    cross_20 = any(
        k.iloc[i-1] <= 20 and k.iloc[i] > 20
        for i in range(-lookback, -1)
    )
    cross_d = any(
        k.iloc[i-1] <= d.iloc[i-1] and k.iloc[i] > d.iloc[i]
        for i in range(-lookback, -1)
    )
    return rsi_cross and cross_20 and cross_d

# ──────────────────────────────────────────────
# ✅ /check5 — قراءة أي عملة على فريم 5 دقائق
# ──────────────────────────────────────────────
def handle_check5(chat_id, symbol="BTCUSDT"):
    send_telegram(f"🔄 جاري جلب بيانات {symbol} — فريم 5 دقايق...", chat_id)
    try:
        df_raw = get_ohlcv(symbol, "1m", limit=600)
        cached = get_cached(symbol, "1m")
        if not cached.empty and len(cached) > len(df_raw):
            df_raw = cached

        if df_raw.empty:
            send_telegram("❌ فشل جلب البيانات من MEXC", chat_id)
            return

        df5 = resample_ohlcv(df_raw, 5)
        if df5.empty or len(df5) < MIN_CANDLES:
            send_telegram(
                f"⚠️ شموع غير كافية: {len(df5)} (المطلوب {MIN_CANDLES})\n"
                f"💡 جرب بعد اكتمال التحميل الكامل", chat_id
            )
            return

        price      = df5["close"].iloc[-2]
        candle_ts  = df5["ts"].iloc[-2].strftime("%Y-%m-%d %H:%M UTC")
        fetch_ts   = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

        rsi_series = calc_rsi_tv(df5["close"], period=14)
        rsi_val    = round(float(rsi_series.iloc[-2]), 2)

        k_series, d_series = calc_stoch_tv(df5["close"], df5["high"], df5["low"])
        stoch_k = round(float(k_series.iloc[-2]), 2)
        stoch_d = round(float(d_series.iloc[-2]), 2)

        macd_line, signal_line, histogram = _calc_macd_full(df5["close"])
        macd_hist_val   = round(float(histogram.iloc[-2]), 4)
        macd_line_val   = round(float(macd_line.iloc[-2]), 4)
        signal_line_val = round(float(signal_line.iloc[-2]), 4)
        macd_color      = "🟢" if macd_hist_val > 0 else "🔴"

        smi_val, smi_sig = get_smi_value(df5)

        don_green = check_donchian_ribbon(df5, direction="green")
        don_red   = check_donchian_ribbon(df5, direction="red")
        if don_green:   don_color = "🟢 أخضر (صاعد)"
        elif don_red:   don_color = "🔴 أحمر (هابط)"
        else:           don_color = "⚪ محايد"

        rsi_zone   = "🔴 تشبع بيعي" if rsi_val < 30 else ("🟠 تشبع شرائي" if rsi_val > 70 else "🟡 محايد")
        stoch_zone = "🔴 تشبع بيعي" if stoch_k < 20 else ("🟠 تشبع شرائي" if stoch_k > 80 else "🟡 محايد")
        smi_zone   = ("🔴 تشبع بيعي" if smi_val is not None and smi_val <= -40
                      else ("🟠 تشبع شرائي" if smi_val is not None and smi_val >= 40 else "🟡 محايد"))

        send_telegram(
            f"📊 <b>{symbol} — فريم 5 دقايق</b>\n"
            f"🕯️ الشمعة المغلقة: <b>{candle_ts}</b>\n"
            f"🕐 وقت الجلب: {fetch_ts}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"💰 السعر: <b>{price:.2f}$</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🎀 Donchian Ribbon (20): {don_color}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📈 RSI (14): <b>{rsi_val}</b>  {rsi_zone}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📉 Stoch K(15,3): <b>{stoch_k}</b>  {stoch_zone}\n"
            f"    Stoch D(3):   <b>{stoch_d}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"⚡ MACD Histogram: {macd_color} <b>{macd_hist_val}</b>\n"
            f"    MACD Line:     <b>{macd_line_val}</b>\n"
            f"    Signal Line:   <b>{signal_line_val}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🔵 SMI: <b>{smi_val}</b>  {smi_zone}\n"
            f"    Signal: <b>{smi_sig}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📦 شموع الـ5m: {len(df5)} | بيانات الـ1m: {len(df_raw)}",
            chat_id
        )

    except Exception as e:
        log.error(f"check5 error: {e}")
        send_telegram(f"❌ خطأ في /check5: {e}", chat_id)

# ──────────────────────────────────────────────
# Diagnostics
# ──────────────────────────────────────────────
DIAG_LABELS = {
    "no_data"         : "بيانات ناقصة",
    "smi_oversold"    : "SMI مش في التشبع البيعي",
    "active_skip"     : "⭐ الفريم الأكبر في تشبع بيعي (تم إلغاؤها)",
    "macd_red"        : "MACD الرئيسي مش أحمر",
    "donchian_entry"  : "Donchian Ribbon الرئيسي مش أخضر",
    "donchian_confirm": "Donchian Ribbon Confirm مش أخضر",
    "macd_confirm"    : "MACD Confirm مش أخضر (×3)",
    "ema50"           : "السعر فوق EMA50",
    "rsi_stoch"       : "RSI/Stochastic ما اتحقق",
}

STEP_LABELS = {
    "no_data"         : "بيانات كافية ✅",
    "smi_oversold"    : "① تشبع بيعي SMI ✅",
    "active_skip"     : "⭐ الفريم الأكبر ليس في تشبع بيعي ✅",
    "macd_red"        : "③ MACD أحمر ✅",
    "donchian_entry"  : "④ Donchian Ribbon أخضر ✅",
    "donchian_confirm": "⑤ Donchian Confirm أخضر ✅",
    "macd_confirm"    : "⑥ MACD Confirm أخضر (×3) ✅",
    "ema50"           : "⑦ السعر تحت EMA50 ✅",
    "rsi_stoch"       : "⑧ RSI تقاطع + Stochastic ✅",
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
        for k, pass_label in STEP_LABELS.items():
            failed   = diag_counts[k]
            passed   = remaining - failed
            pass_pct = int(passed / t * 100)
            fail_pct = int(failed / t * 100)
            bar = "█" * (pass_pct // 10) + "░" * (10 - pass_pct // 10)
            lines.append(
                f"{pass_label}\n"
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
# scan_symbol
# ──────────────────────────────────────────────
def scan_symbol(symbol, entry_min, confirm_min, third_min, ec_api, t_api):
    raw_ec = get_cached(symbol, ec_api)
    raw_t  = get_cached(symbol, t_api)

    if not cache_diag_logged.is_set():
        log.info(f"🔍 كاش | {symbol} | ec[{ec_api}]={len(raw_ec)} | t[{t_api}]={len(raw_t)}")
        cache_diag_logged.set()

    with diag_lock: diag_counts["total"] += 1

    near_key = f"{symbol}_{entry_min}_{confirm_min}_{third_min}"

    if raw_ec.empty or len(raw_ec) < 50 or raw_t.empty or len(raw_t) < 50:
        with diag_lock: diag_counts["no_data"] += 1
        return

    df_entry   = resample_ohlcv(raw_ec, entry_min)
    df_confirm = resample_ohlcv(raw_ec, confirm_min)
    df_third   = resample_ohlcv(raw_t,  third_min)

    if any(d.empty or len(d) < MIN_CANDLES for d in [df_entry, df_confirm, df_third]):
        with diag_lock: diag_counts["no_data"] += 1
        return

    if not check_smi_oversold(df_entry):
        with diag_lock: diag_counts["smi_oversold"] += 1
        return

    next_tf = NEXT_TF.get(entry_min)
    if next_tf is not None:
        df_next = resample_ohlcv(raw_ec, next_tf)
        if not df_next.empty and len(df_next) >= MIN_CANDLES:
            if check_smi_oversold(df_next):
                smi_val, sig_val = get_smi_value(df_next)
                with near_signals_6_lock:
                    near_signals_6[near_key] = {
                        "time"    : datetime.now(timezone.utc),
                        "symbol"  : symbol,
                        "price"   : df_entry["close"].iloc[-2],
                        "tf"      : f"{entry_min}m/{confirm_min}m/{third_min}m",
                        "next_tf" : next_tf,
                        "smi_next": smi_val,
                        "sig_next": sig_val,
                        "stage"   : f"محجوبة — الفريم {next_tf}m في تشبع بيعي",
                    }
                with diag_lock: diag_counts["active_skip"] += 1
                return

    with near_signals_6_lock:
        near_signals_6.pop(near_key, None)

    if not check_macd_red(df_entry):
        with diag_lock: diag_counts["macd_red"] += 1
        return

    if not check_donchian_ribbon(df_entry, direction="green"):
        with diag_lock: diag_counts["donchian_entry"] += 1
        return

    if not check_donchian_ribbon(df_confirm, direction="green"):
        with diag_lock: diag_counts["donchian_confirm"] += 1
        return

    if not check_macd_green(df_confirm):
        with diag_lock: diag_counts["macd_confirm"] += 1
        return

    early_key = f"EARLY_{near_key}"
    with alerted_keys_lock:
        if early_key not in alerted_keys:
            alerted_keys[early_key] = datetime.now(timezone.utc)
            price = df_entry["close"].iloc[-2]
            send_telegram(
                f"⏳ <b>تنبيه مبكر — شرط 6/8</b>\n"
                f"🪙 <b>{symbol}</b>\n"
                f"🎯 {entry_min}m/{confirm_min}m/{third_min}m\n"
                f"💰 السعر: {price:.6g}\n"
                f"⚠️ باقي: EMA50 + RSI/Stoch"
            )

    if not check_ema50_below(df_entry):
        with diag_lock: diag_counts["ema50"] += 1
        return

    if not check_rsi_stoch(df_third):
        with diag_lock: diag_counts["rsi_stoch"] += 1
        price = df_entry["close"].iloc[-2]
        with near_signals_lock:
            near_signals[near_key] = {
                "time"  : datetime.now(timezone.utc),
                "symbol": symbol,
                "price" : price,
                "tf"    : f"{entry_min}m/{confirm_min}m/{third_min}m",
            }
        return

    with near_signals_lock:
        near_signals.pop(near_key, None)
    with near_signals_6_lock:
        near_signals_6.pop(near_key, None)

    with diag_lock: diag_counts["passed"] += 1

    last_ts = df_entry["ts"].iloc[-2].strftime("%Y%m%d%H%M") if "ts" in df_entry.columns else "x"
    key = f"{symbol}_{entry_min}_{last_ts}"

    with alerted_keys_lock:
        if key in alerted_keys: return
        alerted_keys[key] = datetime.now(timezone.utc)

    price       = df_entry["close"].iloc[-2]
    now_utc     = datetime.now(timezone.utc)
    candle_time = df_entry["ts"].iloc[-2].strftime("%Y-%m-%d %H:%M UTC")
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
# ✅ Candle Watcher — مع 50 worker للفحص السريع
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
        # انتظر إغلاق الشمعة + ثانيتين للتأكد
        time.sleep(max(wait, 0) + 2.0)
        cleanup_alerted_keys()
        cleanup_near6()
        if not fast_prefetch_done.is_set(): continue
        with symbols_cache_lock: syms = list(symbols_cache)
        if not syms: continue
        cache_diag_logged.clear()
        fn = partial(
            scan_symbol,
            entry_min=entry_min, confirm_min=confirm_min,
            third_min=third_min, ec_api=ec_api, t_api=t_api,
        )
        # ✅ رفعنا من 6 إلى 50 worker — 200 عملة تُفحص في 2-4 ثوانٍ
        with ThreadPoolExecutor(max_workers=50) as ex:
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
                    now = datetime.now(timezone.utc)
                    with near_signals_lock:
                        rows8 = list(near_signals.values())
                    if not rows8:
                        send_telegram("⏳ لا توجد عملات قريبة من الإشارة (7/8).", chat_id)
                    else:
                        lines = [f"<b>🔴 باقي شرط واحد فقط — RSI/Stoch ({len(rows8)}):</b>\n" + "━" * 15]
                        for row in reversed(rows8):
                            age = int((now - row["time"]).total_seconds() / 60)
                            lines.append(
                                f"🔸 {row['symbol']} | {row['tf']} | "
                                f"{row['price']:.6g} | منذ {age} دقيقة"
                            )
                        send_telegram("\n".join(lines), chat_id)
                elif txt == "/blocked":
                    now = datetime.now(timezone.utc)
                    with near_signals_6_lock:
                        rows6 = [
                            v for v in near_signals_6.values()
                            if now - v["time"] <= timedelta(hours=NEAR6_EXPIRY_HOURS)
                        ]
                    if not rows6:
                        send_telegram(
                            f"⏳ لا توجد عملات محجوبة خلال آخر {NEAR6_EXPIRY_HOURS} ساعات.",
                            chat_id
                        )
                    else:
                        lines = [
                            f"<b>⭐ محجوبة بالفريم الأكبر ({len(rows6)}) — آخر {NEAR6_EXPIRY_HOURS}س:</b>\n"
                            + "━" * 15
                        ]
                        for row in reversed(rows6):
                            age = int((now - row["time"]).total_seconds() / 60)
                            smi_txt = (
                                f" | SMI الفريم {row['next_tf']}m: {row['smi_next']}/{row['sig_next']}"
                                if row.get("smi_next") is not None else ""
                            )
                            lines.append(
                                f"🔹 {row['symbol']} | {row['tf']} | "
                                f"{row['price']:.6g}{smi_txt} | منذ {age} دقيقة"
                            )
                        send_telegram("\n".join(lines), chat_id)
                elif txt == "/status":
                    with trades_lock:        cnt   = len(trades_history)
                    with alerted_keys_lock:  active = len(alerted_keys)
                    with ohlcv_cache_lock:   keys  = len(ohlcv_cache)
                    with near_signals_lock:  near  = len(near_signals)
                    with near_signals_6_lock: near6 = len(near_signals_6)
                    send_telegram(
                        f"🤖 البوت يعمل — MEXC API\n"
                        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
                        f"📊 إجمالي الإشارات: {cnt}\n"
                        f"🔴 قريبة من الإشارة (7/8): {near}\n"
                        f"⭐ محجوبة بالفريم الأكبر: {near6}\n"
                        f"🔑 تنبيهات نشطة: {active}\n"
                        f"💾 الكاش: {keys} مفتاح\n"
                        f"⚡ تحميل سريع: {'✅' if fast_prefetch_done.is_set() else '⏳'}\n"
                        f"📦 تحميل كامل: {'✅' if prefetch_done.is_set() else '⏳'}",
                        chat_id,
                    )
                elif txt == "/cache":
                    with ohlcv_cache_lock:
                        sample     = list(ohlcv_cache.items())[:5]
                        total_keys = len(ohlcv_cache)
                    lines = [f"🔬 <b>الكاش ({total_keys} مفتاح):</b>"]
                    for (sym, tf), df in sample:
                        lines.append(f"• {sym} [{tf}]: {len(df)} شمعة")
                    send_telegram("\n".join(lines), chat_id)
                elif txt == "/fetchtest":
                    send_telegram("🔄 جاري اختبار MEXC API...", chat_id)
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
                    send_telegram("🧪 <b>نتيجة اختبار MEXC:</b>\n" + "\n".join(results), chat_id)
                elif txt == "/reload":
                    with symbols_cache_lock: syms = list(symbols_cache)
                    if not syms:
                        send_telegram("⚠️ لا توجد عملات.", chat_id)
                    else:
                        fast_prefetch_done.clear(); prefetch_done.clear()
                        threading.Thread(target=prefetch_all, args=(syms,), daemon=True).start()
                        send_telegram(f"🚀 بدأ إعادة التحميل لـ {len(syms)} عملة...", chat_id)

                elif txt.startswith("/check5"):
                    parts  = txt.split()
                    symbol = parts[1].upper() if len(parts) > 1 else "BTCUSDT"
                    if not symbol.endswith("USDT"):
                        symbol = symbol + "USDT"
                    threading.Thread(
                        target=handle_check5, args=(chat_id, symbol), daemon=True
                    ).start()

                elif txt == "/help":
                    send_telegram(
                        "📋 <b>الأوامر المتاحة:</b>\n"
                        "1️⃣  <code>1</code> — إشارات اليوم\n"
                        "2️⃣  <code>2</code> — إشارات أمس\n"
                        "3️⃣  <code>3</code> — آخر 7 أيام\n"
                        "🔴  <code>/top6</code> — عملات وصلت 7/8 شروط (باقي RSI/Stoch)\n"
                        "⭐  <code>/blocked</code> — عملات محجوبة بالفريم الأكبر\n"
                        "📊  <code>/status</code> — حالة البوت\n"
                        "🔬  <code>/cache</code> — فحص الكاش\n"
                        "🧪  <code>/fetchtest</code> — اختبار MEXC\n"
                        "🔄  <code>/reload</code> — إعادة تحميل الكاش\n"
                        "🔍  <code>/سبب</code> — تشخيص الإشارات\n"
                        "📊  <code>/check5 BTC</code> — قراءة BTC 5 دقايق (أو أي عملة)\n"
                        "📋  <code>/help</code> — قائمة الأوامر",
                        chat_id,
                    )
        except requests.exceptions.Timeout:
            log.warning("⏱️ getUpdates timeout")
        except Exception as e:
            log.error(f"Polling error: {e}")
            time.sleep(10)

# ──────────────────────────────────────────────
# ✅ جلب قائمة العملات من MEXC
# ──────────────────────────────────────────────
def update_symbols_loop():
    first = True
    while True:
        try:
            resp = get_session().get(
                f"{MEXC_BASE}/api/v3/ticker/24hr",
                timeout=20,
            ).json()
            if isinstance(resp, list) and resp:
                top = sorted(
                    [t for t in resp if t["symbol"].endswith("USDT")
                     and "_" not in t["symbol"]],   # نستبعد رموز غريبة
                    key=lambda x: float(x.get("quoteVolume", 0)),
                    reverse=True,
                )[:TOP_SYMBOLS_LIMIT]
                new_syms = [t["symbol"] for t in top]
                with symbols_cache_lock:
                    symbols_cache.clear(); symbols_cache.extend(new_syms)
                log.info(f"✅ عملات MEXC: {len(new_syms)} — أول 5: {new_syms[:5]}")
                if first:
                    first = False
                    threading.Thread(
                        target=prefetch_all, args=(list(new_syms),), daemon=True
                    ).start()
            else:
                log.warning(f"⚠️ MEXC ticker رد غير متوقع: {str(resp)[:100]}")
                if first: _start_fallback(); first = False
        except Exception as e:
            log.error(f"Symbols loop error: {e}")
            if first: _start_fallback(); first = False
        time.sleep(3600)

def _start_fallback():
    fallback = [
        "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
        "DOGEUSDT","ADAUSDT","AVAXUSDT","DOTUSDT","MATICUSDT",
        "LINKUSDT","UNIUSDT","LTCUSDT","ATOMUSDT","NEARUSDT",
    ]
    with symbols_cache_lock:
        symbols_cache.clear(); symbols_cache.extend(fallback)
    log.warning(f"⚠️ fallback: {len(fallback)} عملة")
    threading.Thread(target=prefetch_all, args=(list(fallback),), daemon=True).start()

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
    def log_message(self, *_): pass

# ──────────────────────────────────────────────
# ✅ BTC 5m Auto Watcher — يُرسل بعد كل إغلاق شمعة 5 دقائق
# ──────────────────────────────────────────────
def check5_watcher():
    """يُرسل تقرير BTC كل 5 دقائق تلقائياً بعد إغلاق الشمعة"""
    while True:
        nxt  = get_next_close(5)
        wait = (nxt - datetime.now(timezone.utc)).total_seconds()
        time.sleep(max(wait, 0) + 2.0)   # + ثانيتان بعد الإغلاق
        if not fast_prefetch_done.is_set():
            continue
        # أرسل في thread منفصل لعدم تأخير الـ watcher
        threading.Thread(
            target=handle_check5,
            args=(TELEGRAM_CHAT_ID, "BTCUSDT"),
            daemon=True,
        ).start()

# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    log.info("🚀 Tripling Strategy Bot — MEXC API (v3)")
    delete_webhook()
    threading.Thread(target=update_symbols_loop, daemon=True).start()
    while not symbols_cache: time.sleep(1)
    threading.Thread(target=poll_telegram_commands, daemon=True).start()
    threading.Thread(target=cache_updater_1m,       daemon=True).start()   # ✅ متوازي ذكي
    threading.Thread(target=cache_updater_60m,      daemon=True).start()   # ✅ متوازي
    threading.Thread(target=send_diag_report,       daemon=True).start()
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", PORT), HealthHandler).serve_forever(),
        daemon=True,
    ).start()
    log.info("⏳ انتظار اكتمال التحميل السريع…")
    fast_prefetch_done.wait()
    log.info("✅ بدء الواتشرز")

    # ✅ واتشر BTC 5 دقائق التلقائي
    threading.Thread(target=check5_watcher, daemon=True).start()

    # ✅ واتشرز الإشارات الرئيسية — 50 worker لكل زوج
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