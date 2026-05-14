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
        ("15m", None, 40),
        ("45m", 45,  120),
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

alerted_keys       = {}
alerted_keys_lock  = threading.Lock()
trades_history     = deque(maxlen=2000)
trades_lock        = threading.Lock()
symbols_cache      = []
symbols_cache_lock = threading.Lock()


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
    if df.empty or len(df) < 30:
        return False
    smi, _ = calc_smi(df["high"], df["low"], df["close"])
    return smi.iloc[-lookback:].min() <= oversold


def check_macd_green(df: pd.DataFrame) -> bool:
    if df.empty or len(df) < 35:
        return False
    close     = df["close"]
    ema12     = close.ewm(span=12, adjust=False).mean()
    ema26     = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal    = macd_line.ewm(span=9, adjust=False).mean()
    hist      = macd_line - signal
    return hist.iloc[-1] > 0


def check_entry_signals(df: pd.DataFrame, stoch_lookback=5) -> bool:
    if df.empty or len(df) < 40:
        return False

    close = df["close"]
    high  = df["high"]
    low   = df["low"]

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

    low15  = low.rolling(15).min()
    high15 = high.rolling(15).max()
    k_raw  = 100 * (close - low15) / (high15 - low15 + 1e-10)
    k      = k_raw.rolling(3).mean()

    stoch_crossed = any(
        k.iloc[i - 1] < 20 and k.iloc[i] >= 20
        for i in range(-stoch_lookback, 0)
    )
    return stoch_crossed


def scan_symbol(symbol: str, base_tf: str):
    raw_df = get_ohlcv(symbol, base_tf, limit=500)
    if raw_df.empty:
        return

    for entry_label, entry_min, confirm_min in PAIRS_BY_BASE.get(base_tf, []):

        df_entry = (resample_ohlcv(raw_df, entry_min)
                    if entry_min
                    else raw_df.iloc[:-1].reset_index(drop=True))
        if df_entry.empty:
            continue

        if not check_smi_oversold(df_entry):
            log.debug(f"{symbol} {entry_label}: SMI لم يصل تشبع بيعي")
            continue

        if not check_macd_green(df_entry):
            log.debug(f"{symbol} {entry_label}: MACD فريم الدخول ليس أخضر")
            continue

        df_confirm = (resample_ohlcv(raw_df, confirm_min)
                      if confirm_min
                      else raw_df.iloc[:-1].reset_index(drop=True))
        if df_confirm.empty:
            continue
        if not check_macd_green(df_confirm):
            log.debug(f"{symbol} {entry_label}: MACD فريم التأكيد ليس أخضر")
            continue

        if not check_entry_signals(df_entry):
            log.debug(f"{symbol} {entry_label}: RSI/Stoch لم يتحقق")
            continue

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
                f"📊 فريم التأكيد: {confirm_min}m ✅\n"
                f"🎯 فريم الدخول: <b>{entry_label} ✅</b>\n"
                f"💰 سعر الدخول: {price:.6g}\n"
                f"🕐 الوقت: {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
                f"━━━━━━━━━━━━━━"
            )
            log.info(f"✅ Signal sent: {symbol} {entry_label}")


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


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass


def main():
    log.info("🚀 Starting Bot - SMI + MACD + RSI + Stoch Strategy")

    send_telegram(
        "🚀 <b>تم تشغيل البوت!</b>\n"
        "⚡ الاستراتيجية الجديدة:\n\n"
        "1️⃣ SMI تشبع بيعي (أسفل -40)\n"
        "2️⃣ MACD أخضر (فريم الدخول)\n"
        "3️⃣ MACD أخضر (فريم التأكيد ×3)\n"
        "4️⃣ RSI يتقاطع فوق SMA14\n"
        "5️⃣ Stochastic يتخطى 20 ← دخول\n\n"
        "الفريمات النشطة:\n"
        "• 15m → 40m\n• 45m → 2h\n• 30m → 90m\n"
        "• 1h  → 3h\n• 2h  → 6h\n• 4h  → 12h\n\n"
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
