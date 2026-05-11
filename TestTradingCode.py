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

logging.basicConfig(level=logging.INFO, format=”%(asctime)s %(levelname)s %(message)s”)
log = logging.getLogger(**name**)

TELEGRAM_TOKEN   = “8750346745:AAEJBJP_lCr6RLDWr9pj3tvpuRkt6f2tbpg”
TELEGRAM_CHAT_ID = “-1003853071475”
TOP_SYMBOLS_LIMIT = 100
PORT              = int(os.environ.get(“PORT”, “8080”))
SCAN_EVERY_SEC    = 300

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

# alerted_keys مع وقت الإنتهاء لمنع التراكم اللانهائي

alerted_keys: dict[str, datetime] = {}
ALERT_EXPIRY_HOURS = 4

trades_history = []
trades_lock = threading.Lock()

def cleanup_alerted_keys():
“”“تنظيف المفاتيح القديمة كل دورة”””
now = datetime.now(timezone.utc)
expired = [k for k, t in alerted_keys.items() if now - t > timedelta(hours=ALERT_EXPIRY_HOURS)]
for k in expired:
del alerted_keys[k]
if expired:
log.info(f”Cleaned {len(expired)} expired alert keys”)

def save_signal(symbol, price, tf):
signal = {
“time”: datetime.now(timezone.utc),
“symbol”: symbol,
“price”: price,
“timeframe”: tf
}
with trades_lock:
trades_history.append(signal)
if len(trades_history) > 2000:
trades_history.pop(0)

# ──────────────────────────────────────────────────────────

# TELEGRAM LOGIC

# ──────────────────────────────────────────────────────────

def send_telegram(message: str) -> bool:
“”“إرسال رسالة تلجرام مع تفاصيل الخطأ”””
try:
url = f”https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage”
payload = {
“chat_id”: TELEGRAM_CHAT_ID,
“text”: message,
“parse_mode”: “HTML”
}
resp = requests.post(url, json=payload, timeout=10)
data = resp.json()

```
    if not data.get("ok"):
        log.error(f"Telegram API Error: {data.get('description')} | error_code={data.get('error_code')}")
        return False

    log.info("Telegram message sent successfully ✅")
    return True

except requests.exceptions.Timeout:
    log.error("Telegram timeout - تحقق من الاتصال بالإنترنت")
except requests.exceptions.ConnectionError:
    log.error("Telegram connection error - لا يوجد اتصال")
except Exception as e:
    log.error(f"Telegram unexpected error: {e}")
return False
```

def get_report(period=“today”):
now = datetime.now(timezone.utc)
if period == “today”:
start = now.replace(hour=0, minute=0, second=0, microsecond=0)
end   = now
title = “📅 إشارات اليوم”
elif period == “yesterday”:
end   = now.replace(hour=0, minute=0, second=0, microsecond=0)
start = end - timedelta(days=1)
title = “📅 إشارات أمس”
else:
start = now - timedelta(days=7)
end   = now
title = “🗓️ إشارات آخر 7 أيام”

```
with trades_lock:
    filtered = [t for t in trades_history if start <= t["time"] < end]

if not filtered:
    return f"<b>{title}:</b>\nلا توجد إشارات."

lines = [f"<b>{title} ({len(filtered)})</b>\n" + "━"*15]
for t in filtered:
    lines.append(
        f"✅ {t['symbol']} | {t['timeframe']} | "
        f"{t['price']:.4g} | {t['time'].strftime('%H:%M')}"
    )
return "\n".join(lines)
```

def poll_telegram_commands():
last_id = 0
log.info(“Telegram command polling started”)
while True:
try:
url = f”https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates”
resp = requests.get(
url,
params={“offset”: last_id + 1, “timeout”: 30},
timeout=35
).json()

```
        if resp.get("ok"):
            for update in resp.get("result", []):
                last_id = update["update_id"]
                msg  = update.get("message", {})
                text = msg.get("text", "").strip()

                if   text == "1":       send_telegram(get_report("today"))
                elif text == "2":       send_telegram(get_report("yesterday"))
                elif text == "3":       send_telegram(get_report("week"))
                elif text == "/status":
                    with trades_lock:
                        count = len(trades_history)
                    send_telegram(
                        f"🤖 البوت يعمل\n"
                        f"📊 إجمالي الإشارات: {count}\n"
                        f"🔑 مفاتيح نشطة: {len(alerted_keys)}"
                    )
        else:
            log.warning(f"Telegram getUpdates not OK: {resp}")

    except Exception as e:
        log.error(f"poll_telegram_commands error: {e}")
        time.sleep(10)
```

# ──────────────────────────────────────────────────────────

# DATA FETCHING

# ──────────────────────────────────────────────────────────

def get_ohlcv(symbol: str, tf: str) -> pd.DataFrame:
“”“جلب بيانات الشموع مع تسجيل الأخطاء”””
try:
params = {“symbol”: symbol, “interval”: tf, “limit”: 50}
resp = requests.get(
“https://api.mexc.com/api/v3/klines”,
params=params,
timeout=10
).json()

```
    if not resp or not isinstance(resp, list):
        log.warning(f"Empty/invalid response for {symbol} {tf}: {resp}")
        return pd.DataFrame()

    df = pd.DataFrame(
        resp,
        columns=["ts","open","high","low","close","vol","close_ts","quote_vol"]
    )
    df["close"] = df["close"].astype(float)
    df["ts"]    = pd.to_datetime(df["ts"], unit="ms")
    return df

except Exception as e:
    log.error(f"get_ohlcv error {symbol} {tf}: {e}")
    return pd.DataFrame()
```

# ──────────────────────────────────────────────────────────

# STRATEGY LOGIC

# ──────────────────────────────────────────────────────────

def check_logic(df: pd.DataFrame):
“””
شروط MACD الثلاثة:
1. الهستوغرام سالب
2. خط MACD >= الهستوغرام
3. خط MACD <= 20% من أعلى قيمة خلال 24 شمعة
“””
if df.empty or len(df) < 35:
return False, 0, 0

```
close    = df["close"]
ema12    = close.ewm(span=12, adjust=False).mean()
ema26    = close.ewm(span=26, adjust=False).mean()
macd_line = ema12 - ema26
signal   = macd_line.ewm(span=9, adjust=False).mean()
hist     = macd_line - signal

m = macd_line.iloc[-2]
h = hist.iloc[-2]

cond1 = h < 0
cond2 = m >= h
max_macd = macd_line.tail(24).max()
cond3 = (m <= max_macd * 0.20) if max_macd > 0 else (m <= 0)

passed = all([cond1, cond2, cond3])
return passed, m, h
```

def run_strategy_cycle(symbol: str):
for entry_tf, confirm_tf in HTF_RELATION.items():
# ── فريم التأكيد ──
df_confirm = get_ohlcv(symbol, confirm_tf)
is_confirmed, mc, hc = check_logic(df_confirm)

```
    log.debug(f"{symbol} | HTF={confirm_tf} confirmed={is_confirmed} | m={mc:.6f} h={hc:.6f}")

    if not is_confirmed:
        continue

    # ── فريم الدخول ──
    df_entry = get_ohlcv(symbol, entry_tf)
    is_entry, m, h = check_logic(df_entry)

    log.info(f"🔍 {symbol} HTF={confirm_tf}✅ | LTF={entry_tf} entry={is_entry} | m={m:.6f} h={h:.6f}")

    if not is_entry:
        continue

    last_ts = df_entry["ts"].iloc[-2] if not df_entry.empty else None
    key     = f"{symbol}_{entry_tf}_{last_ts}"

    if key not in alerted_keys:
        alerted_keys[key] = datetime.now(timezone.utc)
        price = df_entry["close"].iloc[-2]
        save_signal(symbol, price, entry_tf)

        msg = (
            f"🚨 <b>إشارة دخول مكتملة 100%</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"🪙 العملة: <b>{symbol}</b>\n"
            f"📊 فريم التأكيد: {confirm_tf} ✅\n"
            f"🎯 فريم الدخول: <b>{entry_tf} ✅</b>\n"
            f"💰 سعر الدخول: {price:.6g}\n"
            f"🕐 الوقت: {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
            f"━━━━━━━━━━━━━━"
        )
        log.info(f"Signal sent: {symbol} {entry_tf}")
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
log.info(“Starting Hierarchical MACD Bot…”)

```
# ── اختبار اتصال تلجرام فوراً ──
log.info("Testing Telegram connection...")
ok = send_telegram("🚀 <b>تم تشغيل البوت بنجاح!</b>\n⏳ سيبدأ المسح الآن...\n\nالأوامر:\n1 = إشارات اليوم\n2 = إشارات أمس\n3 = آخر 7 أيام\n/status = حالة البوت")
if not ok:
    log.error("❌ فشل إرسال رسالة تلجرام - تحقق من TOKEN و CHAT_ID")

threading.Thread(target=poll_telegram_commands, daemon=True).start()
threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", PORT), HealthHandler).serve_forever(),
    daemon=True
).start()

cycle = 0
while True:
    try:
        cycle += 1
        log.info(f"=== Cycle #{cycle} started ===")

        # جلب أعلى 100 عملة بالحجم
        resp = requests.get(
            "https://api.mexc.com/api/v3/ticker/24hr",
            timeout=15
        ).json()

        if not isinstance(resp, list):
            log.error(f"Unexpected ticker response: {resp}")
            time.sleep(30)
            continue

        symbols_data = sorted(
            [s for s in resp if s["symbol"].endswith("USDT")],
            key=lambda x: float(x.get("quoteVolume", 0)),
            reverse=True
        )[:TOP_SYMBOLS_LIMIT]

        top_symbols = [s["symbol"] for s in symbols_data]
        log.info(f"Scanning {len(top_symbols)} symbols | Top: {top_symbols[:5]}")

        cleanup_alerted_keys()

        with ThreadPoolExecutor(max_workers=15) as executor:
            executor.map(run_strategy_cycle, top_symbols)

        log.info(f"=== Cycle #{cycle} complete. Sleeping {SCAN_EVERY_SEC}s ===")
        time.sleep(SCAN_EVERY_SEC)

    except Exception as e:
        log.error(f"Main loop error: {e}")
        time.sleep(30)
```

if **name** == “**main**”:
main()