import os
import requests
import pandas as pd
import numpy as np
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler

# إعدادات التسجيل
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- إعدادات تلغرام (عدل هنا فقط) ---
TELEGRAM_TOKEN = "8750346745:AAEJBJP_1Cr6RLDWr9pj3tvpuRkt6f2tbpg"
TELEGRAM_CHAT_ID = "-1003562604082"
TOP_SYMBOLS_LIMIT = int(os.environ.get("TOP_SYMBOLS_LIMIT", "50"))
PORT = int(os.environ.get("PORT", "8080"))
PARALLEL_WORKERS = 8
CANDLE_DELAY_SEC = 3

TIMEFRAMES = [
    ("15m", 15, 5, 45, 45),
    ("30m", 30, 10, 90, 60),
    ("1h", 60, 20, 180, 90),
    ("2h", 120, 40, 360, 150),
    ("4h", 240, 80, 720, 270),
    ("1d", 1440, 480, 4320, 1470),
]
# --- تخزين البيانات المؤقتة ---
_bar_cache = {}
cache_lock = threading.Lock()
history_signals = []
history_lock = threading.Lock()

def send_telegram(message):
    """إرسال رسالة نصية إلى تلغرام"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        log.error(f"Telegram error: {e}")

def print_signal(msg):
    """طباعة الإشارة في الكونسول"""
    print(f"\n{msg}\n" + "-"*40)
def get_top_symbols(limit=50):
    """جلب أعلى العملات من حيث حجم التداول (USDT)"""
    url = "https://api.mexc.com/api/v3/ticker/24hr"
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        valid = [d for d in data if d['symbol'].endswith('USDT')]
        valid.sort(key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
        return [v['symbol'] for v in valid[:limit]]
    except Exception as e:
        log.error(f"Error fetching symbols: {e}")
        return []

def fetch_candles(symbol, interval, limit=100):
    """جلب بيانات الشموع من MEXC مع التخزين المؤقت"""
    cache_key = f"{symbol}_{interval}"
    with cache_lock:
        if cache_key in _bar_cache:
            return _bar_cache[cache_key]
            
    url = "https://api.mexc.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'vol', 'close_time', 'q_vol'])
        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].astype(float)
        with cache_lock:
            _bar_cache[cache_key] = df
        return df
    except Exception as e:
        return pd.DataFrame()
def calculate_smi(df, q_len=5, r_len=20, s_len=5):
    """حساب مؤشر SMI (Stochastic Momentum Index)"""
    if len(df) < r_len + s_len: return pd.Series(), pd.Series()
    
    high_max = df['high'].rolling(q_len).max()
    low_min = df['low'].rolling(q_len).min()
    center = (high_max + low_min) / 2
    diff = df['close'] - center
    
    def double_smooth(series, n, m):
        return series.ewm(span=n, adjust=False).mean().ewm(span=m, adjust=False).mean()

    rel_diff = double_smooth(diff, r_len, s_len)
    diff_range = high_max - low_min
    avg_range = double_smooth(diff_range, r_len, s_len)
    
    smi = 100 * (rel_diff / (avg_range / 2))
    signal = smi.ewm(span=s_len, adjust=False).mean()
    return smi, signal

def calculate_macd(df):
    """حساب مؤشر MACD"""
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal

def check_rocket_entry(df):
    """فحص شروط استراتيجية Rocket Entry"""
    if len(df) < 50: return False, ""
    
    smi, smi_sig = calculate_smi(df)
    macd, macd_sig = calculate_macd(df)
    
    if smi.empty or macd.empty: return False, ""
    
    curr_smi = smi.iloc[-1]
    prev_smi = smi.iloc[-2]
    curr_sig = smi_sig.iloc[-1]
    prev_sig = smi_sig.iloc[-2]
    
    # شرط التقاطع من الأسفل (SMI)
    smi_cross_up = (prev_smi < prev_sig) and (curr_smi > curr_sig)
    # شرط أن يكون الـ SMI في منطقة تشبع بيعي
    smi_oversold = curr_smi < -40
    # تقاطع الماكد
    macd_up = macd.iloc[-1] > macd_sig.iloc[-1]
    
    if smi_cross_up and smi_oversold and macd_up:
        return True, f"SMI: {curr_smi:.2f} (Oversold Cross)"
    return False, ""
def process_symbol(symbol, now_time):
    """تحليل عملة واحدة عبر جميع الفريمات"""
    for tf_name, tf_min, lookback, skip_bars, cleanup in TIMEFRAMES:
        df = fetch_candles(symbol, tf_name, limit=100)
        if df.empty: continue
        
        triggered, detail = check_rocket_entry(df)
        if triggered:
            # منع تكرار نفس الإشارة في فترة قصيرة
            with history_lock:
                already = any(h['s'] == symbol and h['tf'] == tf_name for h in history_signals[-10:])
            
            if not already:
                msg = (
                    f"🚀 <b>ROCKET ENTRY!</b>\n"
                    f"💎 Symbol: #{symbol}\n"
                    f"⏳ Timeframe: {tf_name}\n"
                    f"📊 Detail: {detail}\n"
                    f"⏰ Time: {now_time.strftime('%H:%M:%S')} UTC"
                )
                print_signal(msg)
                send_telegram(msg)
                with history_lock:
                    history_signals.append({'s': symbol, 'tf': tf_name, 't': now_time})

def wait_for_next_candle():
    """مزامنة الانتظار حتى إغلاق الشمعة القادمة"""
    now = datetime.now(timezone.utc)
    # المزامنة على فريم 15 دقيقة كأصغر فريم
    sleep_sec = (15 * 60) - (now.minute % 15 * 60 + now.second) + CANDLE_DELAY_SEC
    if sleep_sec > 0:
        log.info(f"Waiting {sleep_sec}s for next candle cycle...")
        time.sleep(sleep_sec)
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is Running")

def run_health_check():
    httpd = HTTPServer(('0.0.0.0', PORT), HealthCheckHandler)
    httpd.serve_forever()

def main():
    threading.Thread(target=run_health_check, daemon=True).start()
    
    startup_msg = (
        f"🤖 <b>MEXC Bot Started</b>\n"
        f"────────────────\n"
        f"The bot is now monitoring the market..."
    )
    print_signal(startup_msg)
    send_telegram(startup_msg)
    
    symbols = get_top_symbols(limit=TOP_SYMBOLS_LIMIT)
    if not symbols:
        print("Failed to load symbols."); return

    while True:
        try:
            wait_for_next_candle()
            now = datetime.now(timezone.utc)
            log.info(f"Scanning at {now.strftime('%H:%M:%S')} UTC")
            
            with cache_lock:
                _bar_cache.clear()
            
            with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
                for sym in symbols:
                    executor.submit(process_symbol, sym, now)
                    
        except Exception as e:
            log.error(f"Main loop error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
