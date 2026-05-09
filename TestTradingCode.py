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
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ضع توكن البوت الخاص بك هنا (تأكد من سريته دائماً)
TELEGRAM_TOKEN   = "8750346745:AAEJBJP_lCr6RLDWr9pj3tvpuRkt6f2tbpg"
TELEGRAM_CHAT_ID = "-1003562604082"
TOP_SYMBOLS_LIMIT = 100
PORT              = int(os.environ.get("PORT", "8080"))
SCAN_EVERY_SEC    = 300  # فحص كل 5 دقائق
TIMEFRAME         = "1h" # فريم الساعة

# ──────────────────────────────────────────────────────────
# STATE MANAGEMENT
# ──────────────────────────────────────────────────────────
alerted_keys = set()
trades_history = []
state_lock = threading.Lock()
trades_lock = threading.Lock()

def save_signal(symbol, price, m, s, h):
    signal = {
        "time": datetime.now(timezone.utc),
        "symbol": symbol,
        "price": price,
        "macd": m,
        "signal": s,
        "hist": h
    }
    with trades_lock:
        trades_history.append(signal)
        if len(trades_history) > 500: trades_history.pop(0)

# ──────────────────────────────────────────────────────────
# TELEGRAM LOGIC (Commands & Sending)
# ──────────────────────────────────────────────────────────
def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10).raise_for_status()
    except Exception as e:
        log.error(f"Telegram Send Error: {e}")

def get_report(period="today"):
    now = datetime.now(timezone.utc)
    if period == "today":
        start = now.replace(hour=0, minute=0, second=0)
        title = "إشارات اليوم"
    else:
        start = now - timedelta(days=7)
        title = "إشارات الأسبوع"
    
    with trades_lock:
        filtered = [t for t in trades_history if t["time"] >= start]
    
    if not filtered: return f"<b>{title}:</b>\nلا توجد إشارات حتى الآن."
    
    lines = [f"<b>{title} ({len(filtered)})</b>\n" + "━"*15]
    for t in filtered:
        lines.append(f"✅ {t['symbol']} | {t['price']:.4g} | {t['time'].strftime('%H:%M')}")
    return "\n".join(lines)

def poll_telegram_commands():
    last_id = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            resp = requests.get(url, params={"offset": last_id + 1, "timeout": 30}, timeout=35).json()
            if resp.get("ok"):
                for update in resp.get("result", []):
                    last_id = update["update_id"]
                    msg = update.get("message", {})
                    text = msg.get("text", "").lower()
                    if text == "/today": send_telegram(get_report("today"))
                    elif text == "/week": send_telegram(get_report("week"))
                    elif text == "/status": send_telegram("🤖 Bot is active and scanning MEXC...")
        except: time.sleep(10)

# ──────────────────────────────────────────────────────────
# DATA FETCHING (MEXC REST API)
# ──────────────────────────────────────────────────────────
MEXC_BASE = "https://api.mexc.com/api/v3"

def get_top_symbols():
    try:
        resp = requests.get(f"{MEXC_BASE}/ticker/24hr").json()
        # تصفية أزواج USDT فقط وترتيبها حسب السيولة
        symbols = [s for s in resp if s['symbol'].endswith('USDT')]
        symbols.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
        return [s['symbol'] for s in symbols[:TOP_SYMBOLS_LIMIT]]
    except Exception as e:
        log.error(f"Error fetching symbols: {e}")
        return []

def get_ohlcv(symbol):
    try:
        params = {"symbol": symbol, "interval": TIMEFRAME, "limit": 100}
        resp = requests.get(f"{MEXC_BASE}/klines", params=params).json()
        df = pd.DataFrame(resp, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts', 'quote_vol'])
        df['close'] = df['close'].astype(float)
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df
    except: return pd.DataFrame()

# ──────────────────────────────────────────────────────────
# MACD STRATEGY
# ──────────────────────────────────────────────────────────
def check_macd(symbol):
    df = get_ohlcv(symbol)
    if df.empty or len(df) < 35: return

    # حساب المؤشرات
    close = df['close']
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    hist = macd_line - signal_line

    # القيم الأخيرة (للشمعة المغلقة)
    m, s, h = macd_line.iloc[-2], signal_line.iloc[-2], hist.iloc[-2]
    last_ts = df['ts'].iloc[-2]

    # منع التكرار
    key = f"{symbol}_{last_ts}"
    if key in alerted_keys: return
    
    # --- الشروط ---
    # 1. الهيستوجرام أحمر
    cond1 = h < 0
    # 2. الماكد (الأزرق) فوق الهيستوجرام (لا يغلق أسفله)
    cond2 = m >= h
    # 3. الماكد قريب من خط الصفر (أقل من 20% من أقصى ارتفاع مؤخراً)
    max_macd = macd_line.tail(24).max()
    cond3 = m <= (max_macd * 0.20) if max_macd > 0 else m <= 0

    if cond1 and cond2 and cond3:
        alerted_keys.add(key)
        save_signal(symbol, close.iloc[-2], m, s, h)
        
        msg = (
            f"📡 <b>إشارة MACD جديدة</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"🪙 العملة: <b>{symbol}</b>\n"
            f"⏱ الفريم: {TIMEFRAME}\n"
            f"💰 السعر: {close.iloc[-2]:.6g}\n"
            f"📊 Hist: {h:.6g} (🔴)\n"
            f"━━━━━━━━━━━━━━\n"
            f"✅ شروط الاستراتيجية محققة"
        )
        send_telegram(msg)
        log.info(f"Signal Found: {symbol}")

# ──────────────────────────────────────────────────────────
# MAIN RUNNER
# ──────────────────────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is alive")

def run_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    server.serve_forever()

def main():
    log.info("Starting MACD Multi-Threaded Bot...")
    
    # تشغيل سيرفر الصحة والأوامر في خلفية البرنامج
    threading.Thread(target=run_health_server, daemon=True).start()
    threading.Thread(target=poll_telegram_commands, daemon=True).start()

    send_telegram("🚀 <b>تم تشغيل البوت بنجاح!</b>\nيتم الآن مراقبة أفضل 100 عملة.")

    while True:
        symbols = get_top_symbols()
        # استخدام ThreadPoolExecutor لتسريع الفحص (Parallel Processing)
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(check_macd, symbols)
        
        log.info(f"Scan complete. Sleeping {SCAN_EVERY_SEC}s")
        time.sleep(SCAN_EVERY_SEC)

if __name__ == "__main__":
    main()