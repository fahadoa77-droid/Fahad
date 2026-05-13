import os
import requests
import pandas as pd
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ✅ إصلاح: بيانات حساسة من متغيرات البيئة
TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
TOP_SYMBOLS_LIMIT  = 100
PORT               = int(os.environ.get("PORT", "8080"))
ALERT_EXPIRY_HOURS = 4

PAIRS_BY_BASE = {
    "15m": [
        ("15m", None, 45),
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
trades_history     = []
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
        if len(trades_history) > 2000:
            trades_history.pop(0)


# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# DATA - Session مستقلة لكل thread
# ─────────────────────────────────────────────

_thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({"Accept-Encoding": "gzip"})
        _thread_local.session = s
    return _thread_local.session


def get_ohlcv(symbol: str, tf: str, limit: int = 500, retries: int = 3) -> pd.DataFrame:
    """✅ إصلاح: إضافة retry عند فشل الطلب"""
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
    """✅ إصلاح: حماية من target_minutes غير صالح أو df فارغ"""
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
    return resampled.iloc[:-1].reset_index()


# ─────────────────────────────────────────────
# STRATEGY
# ─────────────────────────────────────────────

def check_logic(df: pd.DataFrame):
    if df.empty or len(df) < 35:
        return False, 0, 0

    close     = df["close"]
    ema12     = close.ewm(span=12, adjust=False).mean()
    ema26     = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal    = macd_line.ewm(span=9, adjust=False).mean()
    hist      = macd_line - signal

    m        = macd_line.iloc[-2]
    h        = hist.iloc[-2]
    max_macd = macd_line.tail(24).max()

    cond1 = h < 0
    cond2 = m >= h
    cond3 = (m <= max_macd * 0.20) if max_macd > 0 else (m <= 0)

    return all([cond1, cond2, cond3]), m, h


def scan_symbol(symbol: str, base_tf: str):
    raw_df = get_ohlcv(symbol, base_tf, limit=500)
    if raw_df.empty:
        return

    for entry_label, entry_min, confirm_min in PAIRS_BY_BASE.get(base_tf, []):
        df_confirm = (resample_ohlcv(raw_df, confirm_min)
                      if confirm_min
                      else raw_df.iloc[:-1].reset_index(drop=True))
        is_confirmed, mc, hc = check_logic(df_confirm)

        if not is_confirmed:
            continue

        df_entry = (resample_ohlcv(raw_df, entry_min)
                    if entry_min
                    else raw_df.iloc[:-1].reset_index(drop=True))

        # ✅ إصلاح: حماية من df_entry فارغ قبل الوصول إلى بياناته
        if df_entry.empty:
            continue

        is_entry, m, h = check_logic(df_entry)

        log.info(
            f"🔍 {symbol} | {entry_label}→{confirm_min}m "
            f"entry={is_entry} m={m:.6f}"
        )

        if not is_entry:
            continue

        # ✅ إصلاح: التحقق من وجود عمود ts قبل القراءة
        last_ts = df_entry["ts"].iloc[-1] if "ts" in df_entry.columns else None
        key     = f"{symbol}_{entry_label}_{last_ts}"

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


# ─────────────────────────────────────────────
# CANDLE CLOSE WATCHER
# ─────────────────────────────────────────────

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

        # ✅ إصلاح: تقليل عدد الـ workers لتجنب حظر IP من MEXC
        with ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(lambda s: scan_symbol(s, base_tf), symbols)

        elapsed = time.time() - start_scan
        log.info(f"✅ {base_tf}: انتهى المسح ({len(symbols)} عملة) في {elapsed:.1f}ث")


# ─────────────────────────────────────────────
# SYMBOLS UPDATER
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    # ✅ إصلاح: التحقق من وجود المتغيرات الضرورية
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("❌ TELEGRAM_TOKEN أو TELEGRAM_CHAT_ID غير موجود في متغيرات البيئة!")
        return

    log.info("🚀 Starting MACD Bot - Candle Close Mode")

    send_telegram(
        "🚀 <b>تم تشغيل البوت!</b>\n"
        "⚡ وضع: تنبيه فوري عند إغلاق الشمعة\n\n"
        "الفريمات النشطة:\n"
        "• 15m → 45m\n• 45m → 2h\n• 30m → 90m\n"
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
