import os
import requests
import pandas as pd
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

# ──────────────────────────────────────────────────────────

# CONFIG & LOGGING

# ──────────────────────────────────────────────────────────

logging.basicConfig(
level=logging.INFO,
format=”%(asctime)s %(levelname)s %(message)s”
)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN     = “8298845980:AAHPepkUjfwOFasLYybmgzJRY6N69LbLMF8”
TELEGRAM_CHAT_ID   = “-1003853071475”
TOP_SYMBOLS_LIMIT  = 100
PORT               = int(os.environ.get(“PORT”, “8080”))
ALERT_EXPIRY_HOURS = 4

# ──────────────────────────────────────────────────────────

# الفريمات مرتبة حسب base_tf

# كل base_tf له قائمة أزواج: (entry_label, entry_min, confirm_min)

# entry_min=None → فريم مباشر بدون resampling

# ──────────────────────────────────────────────────────────

PAIRS_BY_BASE = {
“15m”: [
(“15m”, None, 45),   # 15m → 45m
(“45m”, 45,  120),   # 45m → 2h
],
“30m”: [
(“30m”, None, 90),   # 30m → 90m
],
“60m”: [
(“1h”,  None, 180),  # 1h  → 3h
(“2h”,  120,  360),  # 2h  → 6h
],
“4h”: [
(“4h”,  None, 720),  # 4h  → 12h
],
}

# مدة كل base_tf بالدقائق

BASE_TF_MINUTES = {
“15m”: 15,
“30m”: 30,
“60m”: 60,
“4h”:  240,
}

# ──────────────────────────────────────────────────────────

# STATE

# ──────────────────────────────────────────────────────────

alerted_keys       = {}
trades_history     = []
trades_lock        = threading.Lock()
symbols_cache      = []
symbols_cache_lock = threading.Lock()

def cleanup_alerted_keys():
now     = datetime.now(timezone.utc)
expired = [
k for k, t in alerted_keys.items()
if now - t > timedelta(hours=ALERT_EXPIRY_HOURS)
]
for k in expired:
del alerted_keys[k]
if expired:
log.info(f”Cleaned {len(expired)} expired keys”)

def save_signal(symbol, price, tf):
with trades_lock:
trades_history.append({
“time”:      datetime.now(timezone.utc),
“symbol”:    symbol,
“price”:     price,
“timeframe”: tf
})
if len(trades_history) > 2000:
trades_history.pop(0)

# ──────────────────────────────────────────────────────────

# TELEGRAM

# ──────────────────────────────────────────────────────────

def send_telegram(message: str) -> bool:
try:
url  = f”https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage”
resp = requests.post(url, json={
“chat_id”:    TELEGRAM_CHAT_ID,
“text”:       message,
“parse_mode”: “HTML”
}, timeout=10).json()

```
    if not resp.get("ok"):
        log.error(f"Telegram Error: {resp.get('description')}")
        return False
    log.info("Telegram sent ✅")
    return True
except Exception as e:
    log.error(f"Telegram error: {e}")
    return False
```

def get_report(period=“today”):
now = datetime.now(timezone.utc)
if period == “today”:
start = now.replace(hour=0, minute=0, second=0, microsecond=0)
end, title = now, “📅 إشارات اليوم”
elif period == “yesterday”:
end   = now.replace(hour=0, minute=0, second=0, microsecond=0)
start = end - timedelta(days=1)
title = “📅 إشارات أمس”
else:
start = now - timedelta(days=7)
end, title = now, “🗓️ إشارات آخر 7 أيام”

```
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
```

def poll_telegram_commands():
last_id = 0
log.info(“Telegram polling started”)
while True:
try:
resp = requests.get(
f”https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates”,
params={“offset”: last_id + 1, “timeout”: 30},
timeout=35
).json()

```
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
                    send_telegram(
                        f"🤖 البوت يعمل\n"
                        f"📊 إجمالي الإشارات: {count}\n"
                        f"🔑 مفاتيح نشطة: {len(alerted_keys)}"
                    )
    except Exception as e:
        log.error(f"poll error: {e}")
        time.sleep(10)
```

# ──────────────────────────────────────────────────────────

# DATA

# ──────────────────────────────────────────────────────────

def get_ohlcv(symbol: str, tf: str, limit: int = 500) -> pd.DataFrame:
try:
resp = requests.get(
“https://api.mexc.com/api/v3/klines”,
params={“symbol”: symbol, “interval”: tf, “limit”: limit},
timeout=10
).json()

```
    if not resp or not isinstance(resp, list):
        return pd.DataFrame()

    df = pd.DataFrame(resp, columns=[
        "ts", "open", "high", "low", "close",
        "vol", "close_ts", "quote_vol"
    ])
    for col in ["open", "high", "low", "close", "vol"]:
        df[col] = df[col].astype(float)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df

except Exception as e:
    log.error(f"get_ohlcv error {symbol} {tf}: {e}")
    return pd.DataFrame()
```

def resample_ohlcv(df: pd.DataFrame, target_minutes: int) -> pd.DataFrame:
if df.empty:
return df

```
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
```

def get_df(symbol: str, base_tf: str, resample_min) -> pd.DataFrame:
df = get_ohlcv(symbol, base_tf, limit=500)
if resample_min is not None:
df = resample_ohlcv(df, resample_min)
return df

# ──────────────────────────────────────────────────────────

# STRATEGY

# ──────────────────────────────────────────────────────────

def check_logic(df: pd.DataFrame):
if df.empty or len(df) < 35:
return False, 0, 0

```
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
```

def scan_symbol(symbol: str, base_tf: str):
“”“امسح عملة واحدة لكل الأزواج المرتبطة بـ base_tf”””
for entry_label, entry_min, confirm_min in PAIRS_BY_BASE.get(base_tf, []):

```
    df_confirm           = get_df(symbol, base_tf, confirm_min)
    is_confirmed, mc, hc = check_logic(df_confirm)

    if not is_confirmed:
        continue

    df_entry         = get_df(symbol, base_tf, entry_min)
    is_entry, m, h   = check_logic(df_entry)

    log.info(
        f"🔍 {symbol} | {entry_label}→{confirm_min}m "
        f"entry={is_entry} m={m:.6f}"
    )

    if not is_entry:
        continue

    last_ts = df_entry["ts"].iloc[-1] if not df_entry.empty else None
    key     = f"{symbol}_{entry_label}_{last_ts}"

    if key not in alerted_keys:
        alerted_keys[key] = datetime.now(timezone.utc)
        price = df_entry["close"].iloc[-1]
        save_signal(symbol, price, entry_label)

        send_telegram(
            f"🚨 <b>إشارة دخول مكتملة 100%</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"🪙 العملة: <b>{symbol}</b>\n"
            f"📊 فريم التأكيد: {confirm_min}m ✅\n"
            f"🎯 فريم الدخول: <b>{entry_label} ✅</b>\n"
            f"💰 سعر الدخول: {price:.6g}\n"
            f"🕐 الوقت: "
            f"{datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
            f"━━━━━━━━━━━━━━"
        )
        log.info(f"✅ Signal sent: {symbol} {entry_label}")
```

# ──────────────────────────────────────────────────────────

# CANDLE CLOSE WATCHER

# ──────────────────────────────────────────────────────────

def get_next_close(tf_minutes: int) -> datetime:
“”“احسب وقت إغلاق الشمعة القادمة بالضبط”””
now   = datetime.now(timezone.utc)
epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
n     = int((now - epoch).total_seconds() / 60 // tf_minutes)
return epoch + timedelta(minutes=(n + 1) * tf_minutes)

def candle_watcher(base_tf: str, tf_minutes: int):
“””
ينتظر إغلاق الشمعة بالضبط ثم يمسح كل العملات فوراً.
+3 ثواني بافر للـ API.
“””
log.info(f”⏱️ Watcher ready: {base_tf} ({tf_minutes}m)”)

```
while True:
    next_close   = get_next_close(tf_minutes)
    wait_seconds = (next_close - datetime.now(timezone.utc)).total_seconds()

    if wait_seconds > 0:
        log.info(
            f"⏳ {base_tf}: إغلاق بعد "
            f"{int(wait_seconds//60)}د {int(wait_seconds%60)}ث"
        )
        time.sleep(wait_seconds + 3)

    log.info(f"🕯️ شمعة أُغلقت: {base_tf} → مسح الآن...")
    cleanup_alerted_keys()

    with symbols_cache_lock:
        symbols = list(symbols_cache)

    if not symbols:
        log.warning(f"{base_tf}: قائمة العملات فارغة")
        time.sleep(10)
        continue

    with ThreadPoolExecutor(max_workers=15) as executor:
        executor.map(lambda s: scan_symbol(s, base_tf), symbols)

    log.info(f"✅ {base_tf}: انتهى المسح ({len(symbols)} عملة)")
```

# ──────────────────────────────────────────────────────────

# SYMBOLS UPDATER

# ──────────────────────────────────────────────────────────

def update_symbols_loop():
“”“يحدّث قائمة أعلى العملات حجماً كل ساعة”””
while True:
try:
resp = requests.get(
“https://api.mexc.com/api/v3/ticker/24hr”,
timeout=15
).json()

```
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
```

# ──────────────────────────────────────────────────────────

# HEALTH CHECK

# ──────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
def do_GET(self):
self.send_response(200)
self.end_headers()
self.wfile.write(b”OK”)

```
def log_message(self, *args):
    pass
```

# ──────────────────────────────────────────────────────────

# MAIN

# ──────────────────────────────────────────────────────────

def main():
log.info(“🚀 Starting MACD Bot - Candle Close Mode”)

```
send_telegram(
    "🚀 <b>تم تشغيل البوت!</b>\n"
    "⚡ وضع: تنبيه فوري عند إغلاق الشمعة\n\n"
    "الفريمات النشطة:\n"
    "• 15m → 45m\n"
    "• 45m → 2h\n"
    "• 30m → 90m\n"
    "• 1h  → 3h\n"
    "• 2h  → 6h\n"
    "• 4h  → 12h\n\n"
    "الأوامر:\n"
    "1 = إشارات اليوم\n"
    "2 = إشارات أمس\n"
   "3 = آخر 7 أيام\n"
    "/status = حالة البوت"
)

# حدّث العملات أولاً
threading.Thread(target=update_symbols_loop, daemon=True).start()

# انتظر حتى تمتلئ القائمة
log.info("⏳ جاري تحميل العملات...")
while not symbols_cache:
    time.sleep(1)
log.info(f"✅ تم تحميل {len(symbols_cache)} عملة")

# شغّل Telegram polling
threading.Thread(target=poll_telegram_commands, daemon=True).start()

# شغّل Health Check
threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", PORT), HealthHandler).serve_forever(),
    daemon=True
).start()

# شغّل watcher مستقل لكل base_tf
for base_tf, tf_minutes in BASE_TF_MINUTES.items():
    threading.Thread(
        target=candle_watcher,
        args=(base_tf, tf_minutes),
        daemon=True
    ).start()
    log.info(f"✅ Watcher: {base_tf} ({tf_minutes}m)")

# ابقِ البرنامج شغّالاً
while True:
    time.sleep(60)
```

if __name__ == "__main__":
main()