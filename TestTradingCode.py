import os
import requests
import pandas as pd
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
    (120, 360, 40, "60m","1m"),
    (180, 540, 60, "60m","60m"),
]

# ✅ مرحلة أولى سريعة → الإشارات تبدأ بعد اكتمال كل العملات
FAST_FETCH_CANDLES = {"1m": 600, "60m": 200}
# ✅ مرحلة ثانية كاملة → في الخلفية
API_FETCH_CANDLES  = {"1m": 7_680, "60m": 1_500}
CACHE_MAX_CANDLES  = {"1m": 8_500, "60m": 2_200}
EPOCH = pd.Timestamp("1970-01-01", tz="UTC")

alerted_keys       = {}
alerted_keys_lock  = threading.Lock()
trades_history     = deque(maxlen=2000)
trades_lock        = threading.Lock()
symbols_cache      = []
symbols_cache_lock = threading.Lock()
ohlcv_cache        = {}
ohlcv_cache_lock   = threading.Lock()

# ✅ حدثان منفصلان
fast_prefetch_done = threading.Event()
prefetch_done      = threading.Event()

diag_counts = {
    "total": 0, "no_data": 0, "macd_confirm": 0,
    "donchian_confirm": 0, "smi_oversold": 0, "macd_entry": 0,
    "donchian_entry": 0, "ema50": 0, "rsi_stoch": 0,
    "donchian_third": 0, "passed": 0,
}
diag_lock = threading.Lock()
cache_diag_logged = threading.Event()
_local = threading.local()

def get_session() -> requests.Session:
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
        if r.get("ok"):
            log.info("✅ تم حذف الـ Webhook")
        else:
            log.warning(f"⚠️ deleteWebhook: {r}")
    except Exception as e:
        log.error(f"deleteWebhook error: {e}")

def cleanup_alerted_keys():
    now = datetime.now(timezone.utc)
    with alerted_keys_lock:
        expired = [k for k, t in list(alerted_keys.items()) if now - t > timedelta(hours=ALERT_EXPIRY_HOURS)]
        for k in expired:
            del alerted_keys[k]

def save_signal(symbol, price, entry_min, confirm_min, third_min):
    with trades_lock:
        trades_history.append({
            "time"     : datetime.now(timezone.utc),
            "symbol"   : symbol,
            "price"    : price,
            "timeframe": f"{entry_min}m/{confirm_min}m/{third_min}m",
        })

def send_telegram(msg: str, chat_id: str = None) -> bool:
    target = chat_id or TELEGRAM_CHAT_ID
    try:
        r = get_session().post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": target, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        ).json()
        if not r.get("ok"):
            log.error(f"Telegram sendMessage failed: {r}")
        return r.get("ok", False)
    except Exception as e:
        log.error(f"Telegram send error: {e}")
        return False

def get_report(period="today") -> str:
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
    if not rows:
        return f"<b>{title}:</b>\nلا توجد إشارات."
    lines = [f"<b>{title} ({len(rows)})</b>\n" + "━" * 15]
    for t in rows:
        lines.append(
            f"✅ {t['symbol']} | {t['timeframe']} | "
            f"{t['price']:.4g} | {t['time'].strftime('%H:%M')}"
        )
    return "\n".join(lines)

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
                log.warning(f"getUpdates not ok: {r}")
                time.sleep(5)
                continue

            for upd in r.get("result", []):
                last_id = upd["update_id"]
                msg     = upd.get("message", {})
                txt     = msg.get("text", "").strip()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if not txt or not chat_id:
                    continue
                log.info(f"📩 أمر: '{txt}' من {chat_id}")

                if txt == "1":
                    send_telegram(get_report("today"), chat_id)
                elif txt == "2":
                    send_telegram(get_report("yesterday"), chat_id)
                elif txt == "3":
                    send_telegram(get_report("week"), chat_id)
                elif txt in ("/سبب", "/diag"):
                    if diag_counts["total"] == 0:
                        send_telegram("⚠️ لا توجد بيانات تشخيص بعد.", chat_id)
                    else:
                        send_telegram(build_diag_msg(reset=False), chat_id)
                elif txt == "/status":
                    with trades_lock:       cnt  = len(trades_history)
                    with alerted_keys_lock: active = len(alerted_keys)
                    with ohlcv_cache_lock:  keys = len(ohlcv_cache)
                    send_telegram(
                        f"🤖 البوت يعمل\n"
                        f"📊 إجمالي الإشارات: {cnt}\n"
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
                    lines = [f"🔬 <b>الكاش ({total_keys} مفتاح إجمالي):</b>"]
                    for (sym, tf), df in sample:
                        lines.append(f"• {sym} [{tf}]: {len(df)} شمعة")
                    send_telegram("\n".join(lines), chat_id)
                elif txt == "/fetchtest":
                    send_telegram("🔄 جاري اختبار MEXC API...", chat_id)
                    results = []
                    for tf in ["1m", "60m"]:
                        df = get_ohlcv("BTCUSDT", tf, limit=10)
                        if df.empty:
                            results.append(f"❌ BTCUSDT [{tf}]: فشل الجلب")
                        else:
                            results.append(
                                f"✅ BTCUSDT [{tf}]: {len(df)} شمعة — "
                                f"آخر سعر {df['close'].iloc[-1]:.4g}"
                            )
                    with ohlcv_cache_lock:
                        cache_keys = len(ohlcv_cache)
                    results.append(f"\n💾 الكاش: {cache_keys} مفتاح")
                    results.append(f"⚡ سريع: {'✅' if fast_prefetch_done.is_set() else '⏳'}")
                    results.append(f"📦 كامل: {'✅' if prefetch_done.is_set() else '⏳'}")
                    send_telegram("🧪 <b>نتيجة اختبار MEXC:</b>\n" + "\n".join(results), chat_id)
                elif txt == "/reload":
                    send_telegram("🔄 جاري إعادة تحميل الكاش...", chat_id)
                    with symbols_cache_lock:
                        syms = list(symbols_cache)
                    if not syms:
                        send_telegram("⚠️ لا توجد عملات في القائمة.", chat_id)
                    else:
                        fast_prefetch_done.clear()
                        prefetch_done.clear()
                        threading.Thread(target=prefetch_all, args=(syms,), daemon=True).start()
                        send_telegram(f"🚀 بدأ إعادة التحميل لـ {len(syms)} عملة...", chat_id)
                elif txt == "/help":
                    send_telegram(
                        "📋 <b>الأوامر المتاحة:</b>\n"
                        "1️⃣  <code>1</code> — إشارات اليوم\n"
                        "2️⃣  <code>2</code> — إشارات أمس\n"
                        "3️⃣  <code>3</code> — آخر 7 أيام\n"
                        "📊  <code>/status</code> — حالة البوت والكاش\n"
                        "🔬  <code>/cache</code> — فحص الكاش\n"
                        "🧪  <code>/fetchtest</code> — اختبار اتصال MEXC\n"
                        "🔄  <code>/reload</code> — إعادة تحميل الكاش\n"
                        "🔍  <code>/سبب</code> — تشخيص غياب الإشارات\n"
                        "📋  <code>/help</code> — قائمة الأوامر",
                        chat_id,
                    )

        except requests.exceptions.Timeout:
            log.warning("⏱️ getUpdates timeout — إعادة المحاولة...")
        except Exception as e:
            log.error(f"❌ Polling error: {e}")
            time.sleep(10)

def _parse_klines(resp) -> pd.DataFrame:
    df = pd.DataFrame(
        resp,
        columns=["ts","open","high","low","close","vol","close_ts","quote_vol"],
    )
    for c in ["open","high","low","close","vol"]:
        df[c] = df[c].astype(float)
    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms", utc=True)
    return df[["ts","open","high","low","close","vol"]]

def get_ohlcv(symbol: str, tf: str, limit: int = 500) -> pd.DataFrame:
    try:
        resp = get_session().get(
            "https://api.mexc.com/api/v3/klines",
            params={"symbol": symbol, "interval": tf, "limit": limit},
            timeout=10,
        ).json()
        if isinstance(resp, list) and resp:
            return _parse_klines(resp)
        log.warning(f"⚠️ رد غير صالح {symbol}/{tf}: {str(resp)[:120]}")
    except Exception as e:
        log.error(f"get_ohlcv {symbol} {tf}: {e}")
    return pd.DataFrame()

def get_ohlcv_full(symbol: str, tf: str, target: int) -> pd.DataFrame:
    all_dfs = []
    end_ms  = None
    fetched = 0
    retries = 0

    while fetched < target:
        limit  = min(1000, target - fetched)
        params = {"symbol": symbol, "interval": tf, "limit": limit}
        if end_ms is not None:
            params["endTime"] = end_ms
        try:
            resp = get_session().get(
                "https://api.mexc.com/api/v3/klines",
                params=params,
                timeout=15,
            ).json()

            if not isinstance(resp, list) or not resp:
                log.warning(f"⚠️ رد غير صالح {symbol}/{tf}: {str(resp)[:120]}")
                retries += 1
                if retries >= 3:
                    break
                time.sleep(2 ** retries)
                continue

            all_dfs.insert(0, _parse_klines(resp))
            fetched += len(resp)
            retries = 0

            if len(resp) < limit:
                break
            end_ms = int(resp[0][0]) - 1
            time.sleep(0.15)

        except requests.exceptions.Timeout:
            retries += 1
            log.warning(f"⏱️ timeout {symbol}/{tf} (محاولة {retries})")
            if retries >= 3:
                break
            time.sleep(2 ** retries)
        except Exception as e:
            retries += 1
            log.error(f"full fetch error {symbol}/{tf} (محاولة {retries}): {e}")
            if retries >= 3:
                break
            time.sleep(2 ** retries)

    return (
        pd.concat(all_dfs).drop_duplicates(subset="ts").sort_values("ts").reset_index(drop=True)
        if all_dfs else pd.DataFrame()
    )

def cache_merge(symbol: str, tf: str, new_df: pd.DataFrame):
    if new_df.empty:
        return
    key  = (symbol, tf)
    maxc = CACHE_MAX_CANDLES.get(tf, 5000)
    with ohlcv_cache_lock:
        old = ohlcv_cache.get(key)
        if old is not None and not old.empty:
            merged = pd.concat([old, new_df]).drop_duplicates(subset="ts").sort_values("ts")
            ohlcv_cache[key] = merged.tail(maxc).reset_index(drop=True)
        else:
            ohlcv_cache[key] = new_df.tail(maxc).reset_index(drop=True)

def get_cached(symbol: str, tf: str) -> pd.DataFrame:
    with ohlcv_cache_lock:
        df = ohlcv_cache.get((symbol, tf))
    return df.copy() if df is not None else pd.DataFrame()

def prefetch_all(symbols: list):
    total = len(symbols)

    # ── المرحلة الأولى: سريعة ────────────────────────────────────────
    log.info(f"⚡ المرحلة الأولى (سريعة): {total} عملة…")
    fast_success = 0
    fast_failed  = 0

    for i, sym in enumerate(symbols):
        for tf, n in FAST_FETCH_CANDLES.items():
            fetched = False
            for attempt in range(3):          # ✅ retry لكل عملة
                try:
                    df = get_ohlcv(sym, tf, limit=n)
                    if not df.empty:
                        cache_merge(sym, tf, df)
                        fast_success += 1
                        fetched = True
                        break
                    else:
                        log.warning(f"⚠️ سريع فارغ: {sym} [{tf}] (محاولة {attempt+1})")
                        time.sleep(0.3 * (attempt + 1))
                except Exception as e:
                    log.error(f"fast prefetch {sym} {tf} (محاولة {attempt+1}): {e}")
                    time.sleep(1 * (attempt + 1))
            if not fetched:
                fast_failed += 1
                log.warning(f"❌ فشل نهائي (سريع): {sym} [{tf}]")

            time.sleep(0.2)          # ✅ زيادة من 0.1 إلى 0.2 لتجنب rate limit

        if (i + 1) % 20 == 0 or i == total - 1:
            log.info(f"⚡ سريع: {i+1}/{total} | نجح: {fast_success} | فشل: {fast_failed}")

    # ── التحقق من الكاش بعد اكتمال كل العملات ──
    with ohlcv_cache_lock:
        fast_cache_size = len(ohlcv_cache)
        # عدد العملات التي لديها بيانات 1m
        filled_1m = sum(
            1 for sym in symbols
            if (sym, "1m") in ohlcv_cache and not ohlcv_cache[(sym, "1m")].empty
        )

    # ✅ التحقق قبل تفعيل fast_prefetch_done
    if fast_cache_size == 0:
        log.error("❌ الكاش السريع فارغ تماماً! MEXC API فشل.")
        send_telegram(
            "❌ <b>فشل التحميل السريع!</b>\n"
            "MEXC API لا يستجيب.\n"
            "💡 استخدم /fetchtest للتشخيص\n"
            "🔄 أرسل /reload للمحاولة مجدداً"
        )
        return

    # ✅ fast_prefetch_done يُفعَّل فقط بعد اكتمال كل العملات
    fast_prefetch_done.set()
    log.info(
        f"✅ المرحلة الأولى اكتملت | كاش: {fast_cache_size} مفتاح | "
        f"مكتملة 1m: {filled_1m}/{total} | فشل: {fast_failed}"
    )
    send_telegram(
        f"⚡ <b>التحميل السريع اكتمل — الإشارات بدأت!</b>\n"
        f"📈 عملات: {total} | ✅ مكتملة: {filled_1m} | 💾 مفاتيح: {fast_cache_size}\n"
        f"❌ فشل: {fast_failed} | 📦 جاري تحميل البيانات الكاملة في الخلفية..."
    )

    # ── المرحلة الثانية: كاملة تاريخية ──────────────────────────────
    log.info(f"📦 المرحلة الثانية (كاملة): {total} عملة…")
    full_success = 0
    for i, sym in enumerate(symbols):
        for tf, n in API_FETCH_CANDLES.items():
            try:
                df = get_ohlcv_full(sym, tf, target=n)
                if not df.empty:
                    cache_merge(sym, tf, df)
                    full_success += 1
                else:
                    log.warning(f"⚠️ كامل فارغ: {sym} [{tf}]")
                time.sleep(0.15)
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

    log.info(f"✅ التحميل الكامل اكتمل | كاش: {full_cache_size} مفتاح")
    send_telegram(
        f"✅ <b>التحميل الكامل اكتمل</b>\n"
        f"📈 عملات: {total} | 💾 مفاتيح: {full_cache_size}\n"
        f"✔️ نجح: {full_success}/{total*2}\n\n" +
        "\n".join(diag_lines)
    )

def _update_batch(symbols, tf, limit):
    for sym in symbols:
        try:
            df = get_ohlcv(sym, tf, limit=limit)
            if not df.empty:
                cache_merge(sym, tf, df)
            time.sleep(0.12)
        except Exception as e:
            log.error(f"update {sym} {tf}: {e}")

def cache_updater_1m():
    while True:
        time.sleep(45)
        if fast_prefetch_done.is_set():
            with symbols_cache_lock:
                syms = list(symbols_cache)
            if syms:
                _update_batch(syms, "1m", limit=15)

def cache_updater_60m():
    while True:
        time.sleep(45 * 60)
        if fast_prefetch_done.is_set():
            with symbols_cache_lock:
                syms = list(symbols_cache)
            if syms:
                _update_batch(syms, "60m", limit=5)

def resample_ohlcv(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty or minutes <= 0:
        return pd.DataFrame()
    try:
        r = (
            df.copy().set_index("ts")
            .resample(f"{minutes}min", closed="left", label="left", origin=EPOCH)
            .agg({"open":"first","high":"max","low":"min","close":"last","vol":"sum"})
            .dropna()
        )
        return r.iloc[:-1].reset_index()
    except Exception as e:
        log.error(f"resample error: {e}")
        return pd.DataFrame()

def calc_smi(high, low, close, k=10, d=3, ema=10, smooth=1):
    hh  = high.rolling(k).max()
    ll  = low.rolling(k).min()
    mid = (hh + ll) / 2
    ds  = (close - mid).ewm(span=d, adjust=False).mean().ewm(span=d, adjust=False).mean()
    hls = ((hh - ll) / 2).ewm(span=d, adjust=False).mean().ewm(span=d, adjust=False).mean()
    smi = 200 * ds / (hls.abs() + 1e-10)
    if smooth > 1:
        smi = smi.rolling(smooth).mean()
    sig = smi.ewm(span=ema, adjust=False).mean()
    return smi, sig

def check_smi_oversold(df: pd.DataFrame, threshold=-40, lookback=5) -> bool:
    if len(df) < 30:
        return False
    smi, _ = calc_smi(df["high"], df["low"], df["close"])
    return bool(smi.iloc[-lookback:].min() <= threshold)

def check_macd_green(df: pd.DataFrame) -> bool:
    if len(df) < 35:
        return False
    c   = df["close"]
    ml  = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    sig = ml.ewm(span=9, adjust=False).mean()
    return bool((ml - sig).iloc[-1] > 0)

def check_macd_entry(df: pd.DataFrame, entry_min: int) -> bool:
    if len(df) < 35:
        return False
    c    = df["close"]
    ml   = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    sig  = ml.ewm(span=9, adjust=False).mean()
    hist = ml - sig
    if hist.iloc[-1] >= 0 or sig.iloc[-1] < 0 or sig.iloc[-1] < hist.iloc[-1]:
        return False
    c24  = max(int(1440 / entry_min), 1)
    peak = ml.iloc[-c24:].max() if len(ml) >= c24 else ml.max()
    if peak > 0 and ml.iloc[-1] > peak * 0.20:
        return False
    return True

def check_donchian(df: pd.DataFrame, period=20, direction="green") -> bool:
    if len(df) < period:
        return False
    hi  = df["high"].rolling(period).max()
    lo  = df["low"].rolling(period).min()
    mid = (hi + lo) / 2
    return (df["close"].iloc[-1] >= mid.iloc[-1]) if direction == "green" else (df["close"].iloc[-1] < mid.iloc[-1])

def check_ema50_below(df: pd.DataFrame) -> bool:
    if len(df) < 50:
        return False
    ema = df["close"].ewm(span=50, adjust=False).mean()
    return bool(df["close"].iloc[-1] < ema.iloc[-1])

def wilder_rma(series: pd.Series, period: int) -> pd.Series:
    result = series.copy() * 0.0
    result.iloc[period - 1] = series.iloc[:period].mean()
    alpha = 1 / period
    for i in range(period, len(series)):
        result.iloc[i] = result.iloc[i - 1] * (1 - alpha) + series.iloc[i] * alpha
    return result

def calc_rsi_tv(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta.clip(upper=0))
    avg_g = wilder_rma(gain, period)
    avg_l = wilder_rma(loss, period)
    rs    = avg_g / (avg_l + 1e-10)
    return 100 - (100 / (1 + rs))

def check_rsi_stoch(df: pd.DataFrame, lookback=5) -> bool:
    if len(df) < 50:
        return False
    close, high, low = df["close"], df["high"], df["low"]
    rsi    = calc_rsi_tv(close, period=14)
    rsi_ma = rsi.rolling(14).mean()
    if rsi.iloc[-20:].min() > 35:
        return False
    rsi_cross = any(
        rsi.iloc[i-1] < rsi_ma.iloc[i-1] and rsi.iloc[i] >= rsi_ma.iloc[i]
        for i in range(-10, 0)
    )
    if not rsi_cross:
        return False
    lo15  = low.rolling(15).min()
    hi15  = high.rolling(15).max()
    k_raw = 100 * (close - lo15) / (hi15 - lo15 + 1e-10)
    k     = k_raw.rolling(3).mean()
    return any(k.iloc[i-1] < 20 and k.iloc[i] >= 20 for i in range(-lookback, 0))

DIAG_LABELS = {
    "no_data"         : "بيانات ناقصة",
    "macd_confirm"    : "MACD Confirm مش أخضر",
    "donchian_confirm": "Donchian Confirm مش مناسب",
    "smi_oversold"    : "SMI مش في منطقة التشبع البيعي",
    "macd_entry"      : "MACD Entry شروطه ما اتحققت",
    "donchian_entry"  : "Donchian Entry مش أخضر",
    "ema50"           : "السعر فوق EMA50",
    "rsi_stoch"       : "RSI/Stochastic ما اتحقق",
    "donchian_third"  : "Donchian Third مش أحمر",
}

def build_diag_msg(reset: bool = False) -> str:
    with diag_lock:
        t         = diag_counts["total"] or 1
        non_total = {k: v for k, v in diag_counts.items() if k not in ["total", "passed"]}
        worst_k   = max(non_total, key=lambda k: non_total[k])
        worst_v   = non_total[worst_k]
        lines = [
            "🔍 <b>تقرير التشخيص</b>",
            "━━━━━━━━━━━━━━━",
            f"📊 إجمالي الفحوصات: <b>{t}</b>",
            "",
        ]
        for k, label in DIAG_LABELS.items():
            count = diag_counts[k]
            pct   = int(count / t * 100)
            bar   = "█" * (pct // 10) + "░" * (10 - pct // 10)
            lines.append(f"❌ {label}\n    {bar} {count} ({pct}%)")
        lines += [
            "",
            f"✅ اجتازت الكل: <b>{diag_counts['passed']}</b>",
            "━━━━━━━━━━━━━━━",
            f"🏆 أكثر سبب فشل: <b>{DIAG_LABELS.get(worst_k, worst_k)}</b> ({worst_v})",
        ]
        if reset:
            for k in diag_counts:
                diag_counts[k] = 0
    return "\n".join(lines)

def send_diag_report():
    while True:
        time.sleep(3600)
        send_telegram(build_diag_msg(reset=True))

def scan_symbol(symbol, entry_min, confirm_min, third_min, ec_api, t_api):
    raw_ec = get_cached(symbol, ec_api)
    raw_t  = get_cached(symbol, t_api)

    if not cache_diag_logged.is_set():
        log.info(f"🔍 كاش | {symbol} | ec[{ec_api}]={len(raw_ec)} | t[{t_api}]={len(raw_t)}")
        cache_diag_logged.set()

    with diag_lock:
        diag_counts["total"] += 1

    if raw_ec.empty or len(raw_ec) < 50 or raw_t.empty or len(raw_t) < 50:
        with diag_lock:
            diag_counts["no_data"] += 1
        # ✅ تسجيل تفصيلي لمعرفة أي عملات تفشل
        log.debug(f"no_data: {symbol} | ec[{ec_api}]={len(raw_ec)} | t[{t_api}]={len(raw_t)}")
        return

    df_entry   = resample_ohlcv(raw_ec, entry_min)
    df_confirm = resample_ohlcv(raw_ec, confirm_min)
    df_third   = resample_ohlcv(raw_t,  third_min)

    if any(d.empty or len(d) < 25 for d in [df_entry, df_confirm, df_third]):
        with diag_lock:
            diag_counts["no_data"] += 1
        return

    if not check_macd_green(df_confirm):
        with diag_lock: diag_counts["macd_confirm"] += 1
        return
    if not check_donchian(df_confirm, direction="green"):
        with diag_lock: diag_counts["donchian_confirm"] += 1
        return
    if not check_smi_oversold(df_entry):
        with diag_lock: diag_counts["smi_oversold"] += 1
        return
    if not check_macd_entry(df_entry, entry_min):
        with diag_lock: diag_counts["macd_entry"] += 1
        return
    if not check_donchian(df_entry, direction="green"):
        with diag_lock: diag_counts["donchian_entry"] += 1
        return
    if not check_ema50_below(df_entry):
        with diag_lock: diag_counts["ema50"] += 1
        return
    if not check_rsi_stoch(df_third):
        with diag_lock: diag_counts["rsi_stoch"] += 1
        return
    if not check_donchian(df_third, direction="red"):
        with diag_lock: diag_counts["donchian_third"] += 1
        return

    with diag_lock:
        diag_counts["passed"] += 1

    last_ts = df_entry["ts"].iloc[-1].strftime("%Y%m%d%H%M") if "ts" in df_entry.columns else "x"
    key = f"{symbol}_{entry_min}_{last_ts}"

    with alerted_keys_lock:
        if key in alerted_keys:
            return
        alerted_keys[key] = datetime.now(timezone.utc)

    price = df_entry["close"].iloc[-1]
    save_signal(symbol, price, entry_min, confirm_min, third_min)
    send_telegram(
        f"🚨 <b>إشارة دخول</b>\n🪙 <b>{symbol}</b>\n"
        f"🎯 الدخول: <b>{entry_min}m</b>\n💰 السعر: {price:.6g}"
    )

def get_next_close(tf_minutes: int) -> datetime:
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
        # ✅ ينتظر حتى اكتمال التحميل السريع لكل العملات
        if not fast_prefetch_done.is_set():
            continue
        with symbols_cache_lock:
            syms = list(symbols_cache)
        if not syms:
            continue
        cache_diag_logged.clear()
        fn = partial(
            scan_symbol,
            entry_min=entry_min, confirm_min=confirm_min,
            third_min=third_min, ec_api=ec_api, t_api=t_api,
        )
        with ThreadPoolExecutor(max_workers=4) as ex:
            ex.map(fn, syms)

def update_symbols_loop():
    first = True
    while True:
        try:
            resp = get_session().get(
                "https://api.mexc.com/api/v3/ticker/24hr", timeout=15
            ).json()
            if isinstance(resp, list):
                top = sorted(
                    [s for s in resp if s["symbol"].endswith("USDT")],
                    key=lambda x: float(x.get("quoteVolume", 0)),
                    reverse=True,
                )[:TOP_SYMBOLS_LIMIT]
                new_syms = [s["symbol"] for s in top]
                with symbols_cache_lock:
                    symbols_cache.clear()
                    symbols_cache.extend(new_syms)
                if first:
                    first = False
                    threading.Thread(target=prefetch_all, args=(list(new_syms),), daemon=True).start()
        except Exception as e:
            log.error(f"Symbols loop error: {e}")
        time.sleep(3600)

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *_): pass

def main():
    log.info("🚀 Tripling Strategy Bot — Starting")
    delete_webhook()
    threading.Thread(target=update_symbols_loop, daemon=True).start()
    while not symbols_cache:
        time.sleep(1)
    threading.Thread(target=poll_telegram_commands, daemon=True).start()
    threading.Thread(target=cache_updater_1m,       daemon=True).start()
    threading.Thread(target=cache_updater_60m,      daemon=True).start()
    threading.Thread(target=send_diag_report,       daemon=True).start()
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", PORT), HealthHandler).serve_forever(),
        daemon=True,
    ).start()

    log.info("⏳ انتظار اكتمال التحميل السريع لكل العملات…")
    fast_prefetch_done.wait()  # ✅ ينتظر اكتمال التحميل السريع لكل العملات
    log.info("✅ بدء الواتشرز")

    for entry_min, confirm_min, third_min, ec_api, t_api in TRIPLING_PAIRS:
        threading.Thread(
            target=candle_watcher,
            args=(entry_min, confirm_min, third_min, ec_api, t_api),
            daemon=True,
        ).start()

    log.info("✅ جميع الواتشرز تعمل.")
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()