import os
import requests
import pandas as pd
import time
import logging
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── الإعدادات ───────────────────────────────────────────────────────────────
TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN", "8298845980:AAHPepkUjfwOFasLYybmgzJRY6N69LbLMF8")
TELEGRAM_CHAT_ID   = "-1003853071475"
TOP_SYMBOLS_LIMIT  = 100
PORT               = int(os.environ.get("PORT", "8080"))
ALERT_EXPIRY_HOURS = 4

PAIRS_BY_BASE = {
    "15m": [
        ("15m", None, 45),
        ("45m", 45,  135),
    ],
    "30m": [
        ("30m", None, 90),
    ],
    "60m": [
        ("1h",  None, 180),
        ("2h",  120,  360),
    ],
    "4h": [
        ("4h",  None, 720),
    ],
}

BASE_TF_MINUTES = {
    "15m": 15,
    "30m": 30,
    "60m": 60,
    "4h":  240,
}

BASE_TF_SMALL_FETCH = {
    "15m": ("5m",  5),
    "30m": ("5m",  5),
    "60m": ("5m",  5),
    "4h":  ("5m",  5),
}

alerted_keys       = {}
alerted_keys_lock  = threading.Lock()
trades_history     = deque(maxlen=2000)
trades_lock        = threading.Lock()
symbols_cache      = []
symbols_cache_lock = threading.Lock()


# ─── مساعدات ─────────────────────────────────────────────────────────────────

def cleanup_alerted_keys():
    now = datetime.now(timezone.utc)
    with alerted_keys_lock:
        expired = [
            k for k, t in list(alerted_keys.items())
            if now - t > timedelta(hours=ALERT_EXPIRY_HOURS)
        ]
        for k in expired:
            del alerted_keys[k]
    if expired:
        log.info(f"Cleaned {len(expired)} expired keys")


def save_signal(symbol, price, tf):
    with trades_lock:
        trades_history.append({
            "time":      datetime.now(timezone.utc),
            "symbol":    symbol,
            "price":     price,
            "timeframe": tf
        })


def send_telegram(message: str) -> bool:
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML"
        }, timeout=10).json()
        if not resp.get("ok"):
            log.error(f"Telegram Error: {resp.get('description')}")
            return False
        log.info("Telegram sent ✅")
        return True
    except Exception as e:
        log.error(f"Telegram error: {e}")
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
        end, title = now, "🗓️ إشارات آخر 7 أيام"

    with trades_lock:
        filtered = [t for t in trades_history if start <= t["time"] < end]

    if not filtered:
        return f"<b>{title}:</b>\nلا توجد إشارات."

    lines = [f"<b>{title} ({len(filtered)})</b>\n" + "━" * 15]
    for t in filtered:
        lines.append(
            f"✅ {t['symbol']} | {t['timeframe']} | "
            f"{t['price']:.4g} | {t['time'].strftime('%H:%M')}"
        )
    return "\n".join(lines)


def poll_telegram_commands():
    last_id = 0
    log.info("Telegram polling started")
    while True:
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={"offset": last_id + 1, "timeout": 30},
                timeout=35
            ).json()
            if resp.get("ok"):
                for update in resp.get("result", []):
                    last_id = update["update_id"]
                    text    = update.get("message", {}).get("text", "").strip()
                    if text == "1":
                        send_telegram(get_report("today"))
                    elif text == "2":
                        send_telegram(get_report("yesterday"))
                    elif text == "3":
                        send_telegram(get_report("week"))
                    elif text == "/status":
                        with trades_lock:
                            count = len(trades_history)
                        with alerted_keys_lock:
                            active = len(alerted_keys)
                        send_telegram(
                            f"🤖 البوت يعمل\n"
                            f"📊 إجمالي الإشارات: {count}\n"
                            f"🔑 مفاتيح نشطة: {active}"
                        )
        except Exception as e:
            log.error(f"poll error: {e}")
            time.sleep(10)


# ─── جلب البيانات ─────────────────────────────────────────────────────────────

_thread_local = threading.local()

def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({"Accept-Encoding": "gzip"})
        _thread_local.session = s
    return _thread_local.session


def get_ohlcv(symbol: str, tf: str, limit: int = 500, retries: int = 3) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            resp = get_session().get(
                "https://api.mexc.com/api/v3/klines",
                params={"symbol": symbol, "interval": tf, "limit": limit},
                timeout=8
            ).json()

            if not resp or not isinstance(resp, list):
                log.warning(f"get_ohlcv empty response {symbol} {tf} (attempt {attempt+1})")
                time.sleep(1)
                continue

            df = pd.DataFrame(resp, columns=[
                "ts", "open", "high", "low", "close",
                "vol", "close_ts", "quote_vol"
            ])
            for col in ["open", "high", "low", "close", "vol"]:
                df[col] = df[col].astype(float)
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            return df

        except Exception as e:
            log.error(f"get_ohlcv error {symbol} {tf} (attempt {attempt+1}): {e}")
            time.sleep(1)

    return pd.DataFrame()


def resample_ohlcv(df: pd.DataFrame, target_minutes: int) -> pd.DataFrame:
    if df.empty or target_minutes <= 0:
        return pd.DataFrame()
    resampled = df.copy().set_index("ts").resample(
        f"{target_minutes}min", closed="left", label="left"
    ).agg({
        "open":  "first",
        "high":  "max",
        "low":   "min",
        "close": "last",
        "vol":   "sum"
    }).dropna()
    result = resampled.iloc[:-1].reset_index()
    if result.empty:
        log.warning(f"resample_ohlcv: نتيجة فارغة بعد التجميع إلى {target_minutes}m")
    return result


# ─── المؤشرات ─────────────────────────────────────────────────────────────────

def calc_smi(high, low, close, k_len=10, d_len=3, ema_len=10):
    hh       = high.rolling(k_len).max()
    ll       = low.rolling(k_len).min()
    midpoint = (hh + ll) / 2
    diff     = close - midpoint
    hl_half  = (hh - ll) / 2

    ds   = diff.ewm(span=d_len, adjust=False).mean()
    ds2  = ds.ewm(span=d_len, adjust=False).mean()
    hls  = hl_half.ewm(span=d_len, adjust=False).mean()
    hls2 = hls.ewm(span=d_len, adjust=False).mean()

    smi    = 200 * ds2 / (hls2.abs() + 1e-10)
    signal = smi.ewm(span=ema_len, adjust=False).mean()
    return smi, signal


def check_smi_oversold(df: pd.DataFrame, oversold=-40, lookback=5) -> bool:
    """SMI تشبع بيعي ≤ -40"""
    if df.empty or len(df) < 30:
        return False
    smi, _ = calc_smi(df["high"], df["low"], df["close"])
    return smi.iloc[-lookback:].min() <= oversold


def check_macd_green(df: pd.DataFrame) -> bool:
    """MACD هيستوغرام موجب (أخضر ولو خفيف)"""
    if df.empty or len(df) < 35:
        return False
    close     = df["close"]
    ema12     = close.ewm(span=12, adjust=False).mean()
    ema26     = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal    = macd_line.ewm(span=9, adjust=False).mean()
    hist      = macd_line - signal
    return hist.iloc[-1] > 0


def check_macd_entry(df: pd.DataFrame, entry_minutes: int) -> bool:
    """
    شروط MACD فريم الدخول:
    1. هيستوغرام أحمر (سالب)
    2. Signal فوق الهيستوغرام
    3. Signal لا يتجاوز 20% من أقصى ارتفاع له في آخر 24 ساعة
    """
    if df.empty or len(df) < 35:
        return False

    close     = df["close"]
    ema12     = close.ewm(span=12, adjust=False).mean()
    ema26     = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal    = macd_line.ewm(span=9, adjust=False).mean()
    hist      = macd_line - signal

    # شرط 1: هيستوغرام أحمر
    if hist.iloc[-1] >= 0:
        return False

    # شرط 2: Signal فوق الهيستوغرام
    if signal.iloc[-1] < hist.iloc[-1]:
        return False

    # شرط 3: Signal لا يتجاوز 20% من أقصى ارتفاع في آخر 24 ساعة
    candles_24h = max(int((24 * 60) / entry_minutes), 1)
    recent      = signal.iloc[-candles_24h:] if len(signal) >= candles_24h else signal
    max_signal  = recent.max()
    if max_signal > 0 and signal.iloc[-1] > max_signal * 0.20:
        return False

    return True


def check_donchian_green(df: pd.DataFrame, period: int = 20) -> bool:
    """
    Donchian Trend Ribbon أخضر:
    السعر فوق أو مساوي للمتوسط بين أعلى وأدنى آخر 20 شمعة
    """
    if df.empty or len(df) < period:
        return False
    high_max = df["high"].rolling(period).max()
    low_min  = df["low"].rolling(period).min()
    mid      = (high_max + low_min) / 2
    return df["close"].iloc[-1] >= mid.iloc[-1]
def check_donchian_red(df: pd.DataFrame, period: int = 20) -> bool:
    if df.empty or len(df) < period:
        return False
    high_max = df["high"].rolling(period).max()
    low_min  = df["low"].rolling(period).min()
    mid      = (high_max + low_min) / 2
    return df["close"].iloc[-1] < mid.iloc[-1]


def check_close_below_ema50(df: pd.DataFrame) -> bool:
    """الشمعة تغلق تحت EMA 50"""
    if df.empty or len(df) < 50:
        return False
    ema50 = df["close"].ewm(span=50, adjust=False).mean()
    return df["close"].iloc[-1] < ema50.iloc[-1]


def check_rsi_stoch(df: pd.DataFrame, stoch_lookback=5) -> bool:
    """
    فريم الثلث (للدخول فقط):
    1. RSI يتقاطع إيجابي فوق SMA14
    2. Stochastic فوق 20
    """
    if df.empty or len(df) < 40:
        return False

    close = df["close"]
    high  = df["high"]
    low   = df["low"]

    # RSI
    delta  = close.diff()
    gain   = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
    loss   = (-delta.clip(upper=0)).ewm(span=14, adjust=False).mean()
    rsi    = 100 - (100 / (1 + gain / (loss + 1e-10)))
    rsi_ma = rsi.rolling(14).mean()

    rsi_crossed = any(
        rsi.iloc[i - 1] < rsi_ma.iloc[i - 1] and rsi.iloc[i] >= rsi_ma.iloc[i]
        for i in range(-5, 0)
    )
    if not rsi_crossed:
        return False

    # Stochastic
    low15  = low.rolling(15).min()
    high15 = high.rolling(15).max()
    k_raw  = 100 * (close - low15) / (high15 - low15 + 1e-10)
    k      = k_raw.rolling(3).mean()

    stoch_ok = any(
        k.iloc[i - 1] < 20 and k.iloc[i] >= 20
        for i in range(-stoch_lookback, 0)
    )
    return stoch_ok


# ─── المسح ────────────────────────────────────────────────────────────────────

def scan_symbol(symbol: str, base_tf: str):
    raw_df = get_ohlcv(symbol, base_tf, limit=500)
    if raw_df.empty:
        return

    # جلب بيانات الفريم الصغير للثلث (5m)
    small_tf, small_minutes = BASE_TF_SMALL_FETCH.get(base_tf, ("5m", 5))
    raw_small_df = get_ohlcv(symbol, small_tf, limit=500)

    for entry_label, entry_min, confirm_min in PAIRS_BY_BASE.get(base_tf, []):

        # فريم الدخول
        df_entry = (resample_ohlcv(raw_df, entry_min)
                    if entry_min
                    else raw_df.iloc[:-1].reset_index(drop=True))
        if df_entry.empty:
            continue

        actual_entry_min = entry_min if entry_min else BASE_TF_MINUTES[base_tf]

        # ─── شروط فريم الدخول ───────────────────────────

        # 1. SMI تشبع بيعي
        if not check_smi_oversold(df_entry):
            log.debug(f"{symbol} {entry_label}: SMI لم يصل تشبع بيعي")
            continue

        # 2. MACD أحمر + Signal فوق الهيستوغرام + Signal ≤ 20% من أقصى ارتفاع
        if not check_macd_entry(df_entry, actual_entry_min):
            log.debug(f"{symbol} {entry_label}: MACD فريم الدخول لم يتحقق")
            continue

        # 3. Donchian Trend Ribbon أخضر
        if not check_donchian_red(df_entry):
            log.debug(f"{symbol} {entry_label}: Donchian فريم الدخول ليس أحمر")
            continue

        # 4. الشمعة تغلق تحت EMA 50
        if not check_close_below_ema50(df_entry):
            log.debug(f"{symbol} {entry_label}: السعر فوق EMA50")
            continue

        # ─── شروط فريم التأكيد ×3 ───────────────────────

        df_confirm = (resample_ohlcv(raw_df, confirm_min)
                      if confirm_min
                      else raw_df.iloc[:-1].reset_index(drop=True))
        if df_confirm.empty:
            continue

        # 5. MACD أخضر
        if not check_macd_green(df_confirm):
            log.debug(f"{symbol} {entry_label}: MACD فريم التأكيد ليس أخضر")
            continue

        # 6. Donchian Trend Ribbon أخضر
        if not check_donchian_green(df_confirm):
            log.debug(f"{symbol} {entry_label}: Donchian فريم التأكيد ليس أخضر")
            continue

        # ─── شروط فريم الثلث (RSI + Stoch) ─────────────

        third_minutes = max(round(actual_entry_min / 3), 1)
        df_third = pd.DataFrame()

        if not raw_small_df.empty:
            df_third = resample_ohlcv(raw_small_df, third_minutes)

        if df_third.empty:
            log.debug(f"{symbol} {entry_label}: فريم الثلث فارغ ({third_minutes}m)")
            continue

        # 7. RSI تقاطع إيجابي + Stochastic فوق 20
            if not check_rsi_stoch(df_third):
            log.debug(f"{symbol} {entry_label}: RSI/Stoch فريم الثلث لم يتحقق")
            continue
            if not check_donchian_red(df_third):
            continue

        # ─── إرسال الإشارة ───────────────────────────────

        last_close_ts = df_entry["ts"].iloc[-1].strftime("%Y%m%d%H%M") if "ts" in df_entry.columns else "unknown"
        key = f"{symbol}_{entry_label}_{last_close_ts}"

        should_send = False
        with alerted_keys_lock:
            if key not in alerted_keys:
                alerted_keys[key] = datetime.now(timezone.utc)
                should_send = True

        if should_send:
            price = df_entry["close"].iloc[-1]
            save_signal(symbol, price, entry_label)
            send_telegram(
                f"🚨 <b>إشارة دخول مكتملة 100%</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"🪙 العملة: <b>{symbol}</b>\n"
                f"📊 فريم التأكيد: {confirm_min}m | MACD 🟢 | Donchian 🟢\n"
                f"🎯 فريم الدخول: <b>{entry_label}</b> | SMI 📉 | MACD 🔴 | Donchian 🟢 | EMA50 ✅\n"
                f"⚡ فريم الثلث: {third_minutes}m | RSI ↑ | Stoch >20\n"
                f"💰 سعر الدخول: {price:.6g}\n"
                f"🕐 الوقت: {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
                f"━━━━━━━━━━━━━━"
            )
            log.info(f"✅ Signal sent: {symbol} {entry_label}")


# ─── الواتشر ──────────────────────────────────────────────────────────────────

def get_next_close(tf_minutes: int) -> datetime:
    now   = datetime.now(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    n     = int((now - epoch).total_seconds() / 60) // tf_minutes
    return epoch + timedelta(minutes=(n + 1) * tf_minutes)


def candle_watcher(base_tf: str, tf_minutes: int):
    log.info(f"⏱️ Watcher ready: {base_tf} ({tf_minutes}m)")

    while True:
        next_close   = get_next_close(tf_minutes)
        wait_seconds = (next_close - datetime.now(timezone.utc)).total_seconds()

        sleep_time = max(wait_seconds, 0) + 1.5
        log.info(
            f"⏳ {base_tf}: إغلاق بعد "
            f"{int(max(wait_seconds,0)//60)}د {int(max(wait_seconds,0)%60)}ث"
        )
        time.sleep(sleep_time)

        log.info(f"🕯️ شمعة أُغلقت: {base_tf} → مسح الآن...")
        cleanup_alerted_keys()

        with symbols_cache_lock:
            symbols = list(symbols_cache)

        if not symbols:
            log.warning(f"{base_tf}: قائمة العملات فارغة")
            time.sleep(10)
            continue

        start_scan = time.time()

        def _scan(s): return scan_symbol(s, base_tf)
        with ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(_scan, symbols)

        elapsed = time.time() - start_scan
        log.info(f"✅ {base_tf}: انتهى المسح ({len(symbols)} عملة) في {elapsed:.1f}ث")


# ─── تحديث العملات ───────────────────────────────────────────────────────────

def update_symbols_loop():
    while True:
        try:
            resp = get_session().get(
                "https://api.mexc.com/api/v3/ticker/24hr",
                timeout=15
            ).json()

            if isinstance(resp, list):
                top = sorted(
                    [s for s in resp if s["symbol"].endswith("USDT")],
                    key=lambda x: float(x.get("quoteVolume", 0)),
                    reverse=True
                )[:TOP_SYMBOLS_LIMIT]

                with symbols_cache_lock:
                    symbols_cache.clear()
                    symbols_cache.extend([s["symbol"] for s in top])

                log.info(
                    f"✅ تحديث العملات: {len(symbols_cache)} | "
                    f"أعلى 5: {symbols_cache[:5]}"
                )
            else:
                log.error(f"Ticker error: {resp}")

        except Exception as e:
            log.error(f"update_symbols error: {e}")

        time.sleep(3600)


# ─── Health Check ─────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("🚀 Starting Bot - SMI + MACD + Donchian + RSI + Stoch Strategy")

    send_telegram(
        "🚀 <b>تم تشغيل البوت!</b>\n"
        "⚡ الاستراتيجية:\n\n"
        "<b>فريم التأكيد (×3):</b>\n"
        "1️⃣ MACD أخضر\n"
        "2️⃣ Donchian Trend Ribbon أخضر\n\n"
        "<b>فريم الدخول:</b>\n"
        "3️⃣ SMI تشبع بيعي (أسفل -40)\n"
        "4️⃣ MACD أحمر + Signal فوق الهيستوغرام\n"
        "5️⃣ Donchian Trend Ribbon أخضر\n"
        "6️⃣ الشمعة تغلق تحت EMA 50\n\n"
        "<b>فريم الثلث (للدخول فقط):</b>\n"
        "7️⃣ RSI يتقاطع إيجابي فوق SMA14\n"
        "8️⃣ Stochastic يتخطى 20\n\n"
        "الفريمات النشطة:\n"
        "• 15m → تأكيد 45m  | ثلث 5m\n"
        "• 45m → تأكيد 135m | ثلث 15m\n"
        "• 30m → تأكيد 90m  | ثلث 10m\n"
        "• 1h  → تأكيد 3h   | ثلث 20m\n"
        "• 2h  → تأكيد 6h   | ثلث 40m\n"
        "• 4h  → تأكيد 12h  | ثلث 80m\n\n"
        "الأوامر:\n"
        "1 = إشارات اليوم\n2 = إشارات أمس\n"
        "3 = آخر 7 أيام\n/status = حالة البوت"
    )

    threading.Thread(target=update_symbols_loop, daemon=True).start()

    log.info("⏳ جاري تحميل العملات...")
    while not symbols_cache:
        time.sleep(1)
    log.info(f"✅ تم تحميل {len(symbols_cache)} عملة")

    threading.Thread(target=poll_telegram_commands, daemon=True).start()

    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", PORT), HealthHandler).serve_forever(),
        daemon=True
    ).start()

    for base_tf, tf_minutes in BASE_TF_MINUTES.items():
        threading.Thread(
            target=candle_watcher,
            args=(base_tf, tf_minutes),
            daemon=True
        ).start()
        log.info(f"✅ Watcher: {base_tf} ({tf_minutes}m)")

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
