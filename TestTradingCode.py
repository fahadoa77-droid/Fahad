import os
import requests
import pandas as pd
import numpy as np
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

# ──────────────────────────────────────────────────────────

# CONFIG & LOGGING

# ──────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format=”%(asctime)s %(levelname)s %(message)s”)
log = logging.getLogger(**name**)

# بيانات التلجرام

TELEGRAM_TOKEN   = “8750346745:AAEJBJP_lCr6RLDWr9pj3tvpuRkt6f2tbpg”
TELEGRAM_CHAT_ID = “-1003562604082”
TOP_SYMBOLS_LIMIT = 100
PORT              = int(os.environ.get(“PORT”, “8080”))
SCAN_EVERY_SEC    = 300

# تعريف العلاقات (فريم الدخول: فريم التأكيد المضروب في 3)

HTF_RELATION = {
“15m”: “45m”,
“30m”: “90m”,
“45m”: “2h”,
“1h”:  “3h”,
“2h”:  “6h”,
“4h”:  “12h”
}

# ──────────────────────────────────────────────────────────

# STATE MANAGEMENT

# ──────────────────────────────────────────────────────────

alerted_keys = set()
trades_history = []
trades_lock = threading.Lock()

def save_signal(symbol, price, tf):
signal = {
“time”: datetime.now(timezone.utc),
“symbol”: symbol,
“price”: price,
“timeframe”: tf
}
with trades_lock:
trades_history.append(signal)
if len(trades_history) > 2000: trades_history.pop(0)

# ──────────────────────────────────────────────────────────

# TELEGRAM LOGIC

# ──────────────────────────────────────────────────────────

def send_telegram(message):
try:
url = f”https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage”
payload = {“chat_id”: TELEGRAM_CHAT_ID, “text”: message, “parse_mode”: “HTML”}
requests.post(url, json=payload, timeout=10).raise_for_status()
except Exception as e:
log.error(f”Telegram Send Error: {e}”)

def get_report(period=“today”):
now = datetime.now(timezone.utc)
if period == “today”:
start = now.replace(hour=0, minute=0, second=0, microsecond=0)
end = now
title = “📅 إشارات اليوم المكتملة”
elif period == “yesterday”:
end = now.replace(hour=0, minute=0, second=0, microsecond=0)
start = end - timedelta(days=1)
title = “📅 إشارات أمس المكتملة”
else:
start = now - timedelta(days=7)
end = now
title = “🗓️ إشارات آخر 7 أيام”

```
with trades_lock:
    filtered = [t for t in trades_history if start <= t["time"] < end]

if not filtered: return f"<b>{title}:</b>\nلا توجد إشارات محققة بالكامل."

lines = [f"<b>{title} ({len(filtered)})</b>\n" + "━"*15]
for t in filtered:
    lines.append(f"✅ {t['symbol']} | {t['timeframe']} | {t['price']:.4g} | {t['time'].strftime('%H:%M')}")
return "\n".join(lines)
```

def poll_telegram_commands():
last_id = 0
while True:
try:
url = f”https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates”
resp = requests.get(url, params={“offset”: last_id + 1, “timeout”: 30}, timeout=35).json()
if resp.get(“ok”):
for update in resp.get(“result”, []):
last_id = update[“update_id”]
msg = update.get(“message”, {})
text = msg.get(“text”, “”).strip()
if text == “1”: send_telegram(get_report(“today”))
elif text == “2”: send_telegram(get_report(“yesterday”))
elif text == “3”: send_telegram(get_report(“week”))
elif text == “/status”: send_telegram(“🤖 البوت يعمل (تنبيهات مكتملة فقط)…”)
except: time.sleep(10)

# ──────────────────────────────────────────────────────────

# DATA FETCHING

# ──────────────────────────────────────────────────────────

def get_ohlcv(symbol, tf):
try:
params = {“symbol”: symbol, “interval”: tf, “limit”: 50}
resp = requests.get(“https://api.mexc.com/api/v3/klines”, params=params, timeout=10).json()
df = pd.DataFrame(resp, columns=[“ts”, “open”, “high”, “low”, “close”, “vol”, “close_ts”, “quote_vol”])
df[“close”] = df[“close”].astype(float)
df[“ts”] = pd.to_datetime(df[“ts”], unit=“ms”)
return df
except: return pd.DataFrame()

# ──────────────────────────────────────────────────────────

# HIERARCHICAL STRATEGY (Modified Logic)

# ──────────────────────────────────────────────────────────

def check_logic(df):
if df.empty or len(df) < 35:
return False, 0, 0

```
close = df["close"]
ema12 = close.ewm(span=12, adjust=False).mean()
ema26 = close.ewm(span=26, adjust=False).mean()
macd_line = ema12 - ema26
hist = macd_line - macd_line.ewm(span=9, adjust=False).mean()

m, h = macd_line.iloc[-2], hist.iloc[-2]

cond1 = h < 0
cond2 = m >= h
max_macd = macd_line.tail(24).max()
cond3 = m <= (max_macd * 0.20) if max_macd > 0 else m <= 0

is_fully_passed = all([cond1, cond2, cond3])

return is_fully_passed, m, h
```

def run_strategy_cycle(symbol):
for entry_tf, confirm_tf in HTF_RELATION.items():
df_confirm = get_ohlcv(symbol, confirm_tf)
is_confirmed, _, _ = check_logic(df_confirm)

```
    if not is_confirmed:
        continue

    df_entry = get_ohlcv(symbol, entry_tf)
    is_entry, m, h = check_logic(df_entry)

    if not is_entry:
        continue

    last_ts = df_entry["ts"].iloc[-2] if not df_entry.empty else None
    key = f"{symbol}_{entry_tf}_{last_ts}"

    if key not in alerted_keys:
        alerted_keys.add(key)
        save_signal(symbol, df_entry["close"].iloc[-2], entry_tf)

        msg = (
            f"🚨 <b>إشارة دخول مكتملة 100%</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"🪙 العملة: <b>{symbol}</b>\n"
            f"📊 فريم التأكيد: {confirm_tf} (✅)\n"
            f"🎯 فريم الدخول: <b>{entry_tf} (✅)</b>\n"
            f"💰 سعر الدخول: {df_entry['close'].iloc[-2]:.6g}\n"
            f"━━━━━━━━━━━━━━"
        )
        send_telegram(msg)
```

# ──────────────────────────────────────────────────────────

# HEALTH CHECK SERVER

# ──────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
def do_GET(self):
self.send_response(200)
self.end_headers()
self.wfile.write(b”OK”)
def log_message(self, *args):
pass

# ──────────────────────────────────────────────────────────

# RUNNER

# ──────────────────────────────────────────────────────────

def main():
log.info(“Starting Hierarchical MACD Bot (Final Signal Only)…”)
threading.Thread(target=poll_telegram_commands, daemon=True).start()

```
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT), HealthHandler).serve_forever(), daemon=True).start()

send_telegram("🚀 <b>تم تشغيل البوت بنجاح!</b>\nلن يتم إرسال أي تنبيه إلا في حال اكتمال كافة الشروط.")

while True:
    try:
        resp = requests.get("https://api.mexc.com/api/v3/ticker/24hr").json()
        symbols = sorted([s for s in resp if s["symbol"].endswith("USDT")],
                         key=lambda x: float(x["quoteVolume"]), reverse=True)[:TOP_SYMBOLS_LIMIT]
        top_symbols = [s["symbol"] for s in symbols]

        with ThreadPoolExecutor(max_workers=15) as executor:
            executor.map(run_strategy_cycle, top_symbols)

        log.info("Cycle complete.")
        time.sleep(SCAN_EVERY_SEC)
    except Exception as e:
        log.error(f"Loop Error: {e}")
        time.sleep(30)
```

if **name** == “**main**”:
main()
