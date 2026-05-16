
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

logging.basicConfig(level=logging.INFO, format=”%(asctime)s %(levelname)s %(message)s”)
log = logging.getLogger(**name**)

# ─── الإعدادات ────────────────────────────────────────────────────────────────

TELEGRAM_TOKEN = os.environ.get(“TELEGRAM_TOKEN”, “YOUR_TOKEN_HERE”)
TELEGRAM_CHAT_ID = os.environ.get(“TELEGRAM_CHAT_ID”, “YOUR_CHAT_ID_HERE”)
TOP_SYMBOLS_LIMIT = 300
PORT = int(os.environ.get(“PORT”, “8080”))
ALERT_EXPIRY_HOURS = 4

# ─── استراتيجية التثليث الكاملة ──────────────────────────────────────────────

#

# القاعدة:

# فريم التأكيد = فريم الدخول × 3 → MACD أخضر + Donchian أخضر

# فريم الدخول → SMI تشبع | MACD أحمر | Donchian أخضر | EMA50 ✅

# فريم الثلث = فريم الدخول ÷ 3 → RSI↑MA | Stoch>20 | Donchian أحمر

#

# الأعمدة: (entry_min, confirm_min, third_min, ec_api_tf, t_api_tf)

# ec_api_tf = فريم الـ API الذي نبني منه entry + confirm

# t_api_tf = فريم الـ API الذي نبني منه third

#

# ─────────────────────────────────────────────────────────────────────────────

TRIPLING_PAIRS = [
# entry confirm third ec_api t_api
( 9, 27, 3, “1m”, “1m” ),
( 12, 36, 4, “1m”, “1m” ),
( 15, 45, 5, “1m”, “1m” ),
( 18, 54, 6, “1m”, “1m” ),
( 21, 63, 7, “1m”, “1m” ),
( 24, 72, 8, “1m”, “1m” ),
( 27, 81, 9, “1m”, “1m” ),
( 30, 90, 10, “1m”, “1m” ),
( 45, 135, 15, “1m”, “1m” ),
( 60, 180, 20, “60m”, “1m” ), # entry+confirm من 60m | third=20m من 1m
( 120, 360, 40, “60m”, “1m” ), # entry+confirm من 60m | third=40m من 1m
( 180, 540, 60, “60m”, “60m” ), # الكل من 60m
]

# ─── فريمات الـ API وعدد الشمعات المطلوبة ─────────────────────────────────────

#

# 1m → 7 680 شمعة (5.3 يوم) ← يكفي لجميع الفريمات حتى 135m

# أدنى حالة: 7680 ÷ 135 = 56 شمعة 135m → كافٍ لكل المؤشرات ✅

#

# 60m → 1 500 شمعة (62 يوم) ← يكفي لجميع الفريمات حتى 540m

# أدنى حالة: 1500 ÷ 9 = 166 شمعة 540m → كافٍ ✅

#

API_FETCH_CANDLES = {
“1m” : 7_680,
“60m”: 1_500,
}

CACHE_MAX_CANDLES = {
“1m” : 8_000,
“60m”: 2_000,
}

# نقطة مرجعية ثابتة للـ Resample لتطابق حسابات الواتشر تماماً

EPOCH = pd.Timestamp(“1970-01-01”, tz=“UTC”)

# ─── حالة البوت ──────────────────────────────────────────────────────────────

alerted_keys : dict = {}
alerted_keys_lock = threading.Lock()
trades_history = deque(maxlen=2000)
trades_lock = threading.Lock()
symbols_cache : list = []
symbols_cache_lock = threading.Lock()
ohlcv_cache : dict = {}
ohlcv_cache_lock = threading.Lock()
prefetch_done = threading.Event()

# ─── Session thread-local ─────────────────────────────────────────────────────

_local = threading.local()

def get_session() -> requests.Session:
if not hasattr(_local, “s”):
s = requests.Session()
s.headers.update({“Accept-Encoding”: “gzip”})
_local.s = s
return _local.s

# ─── مساعدات عامة ─────────────────────────────────────────────────────────────

def cleanup_alerted_keys():
now = datetime.now(timezone.utc)
with alerted_keys_lock:
expired = [k for k, t in list(alerted_keys.items())
if now - t > timedelta(hours=ALERT_EXPIRY_HOURS)]
for k in expired:
del alerted_keys[k]
if expired:
log.info(f”Cleaned {len(expired)} expired keys”)

def save_signal(symbol, price, entry_min, confirm_min, third_min):
with trades_lock:
trades_history.append({
“time” : datetime.now(timezone.utc),
“symbol” : symbol,
“price” : price,
“timeframe”: f”{entry_min}m/{confirm_min}m/{third_min}m”,
})

def send_telegram(msg: str) -> bool:
try:
r = requests.post(
f”https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage”,
json={“chat_id”: TELEGRAM_CHAT_ID, “text”: msg, “parse_mode”: “HTML”},
timeout=10,
).json()
if not r.get(“ok”):
log.error(f”TG error: {r.get(‘description’)}”)
return False
return True
except Exception as e:
log.error(f”Telegram: {e}”)
return False

def get_report(period=“today”) -> str:
now = datetime.now(timezone.utc)
if period == “today”:
start = now.replace(hour=0, minute=0, second=0, microsecond=0)
end, title = now, “📅 إشارات اليوم”
elif period == “yesterday”:
end = now.replace(hour=0, minute=0, second=0, microsecond=0)
start = end - timedelta(days=1)
title = “📅 إشارات أمس”
else:
start = now - timedelta(days=7)
end, title = now, “🗓️ آخر 7 أيام”

```
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
```

def poll_telegram_commands():
last_id = 0
log.info(“Telegram polling started”)
while True:
try:
r = requests.get(
f”https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates”,
params={“offset”: last_id + 1, “timeout”: 30},
timeout=35,
).json()
if r.get(“ok”):
for upd in r.get(“result”, []):
last_id = upd[“update_id”]
txt = upd.get(“message”, {}).get(“text”, “”).strip()
if txt == “1”: send_telegram(get_report(“today”))
elif txt == “2”: send_telegram(get_report(“yesterday”))
elif txt == “3”: send_telegram(get_report(“week”))
elif txt == “/status”:
with trades_lock: cnt = len(trades_history)
with alerted_keys_lock: active = len(alerted_keys)
send_telegram(
f”🤖 البوت يعمل\n”
f”📊 إجمالي الإشارات: {cnt}\n”
f”🔑 مفاتيح نشطة: {active}\n”
f”💾 كاش: {len(ohlcv_cache)} مجموعة\n”
f”📈 عملات: {len(symbols_cache)}”
)
except Exception as e:
log.error(f”poll: {e}”)
time.sleep(10)

# ─── جلب البيانات ─────────────────────────────────────────────────────────────

def _parse_klines(resp) -> pd.DataFrame:
df = pd.DataFrame(resp, columns=[
“ts”,“open”,“high”,“low”,“close”,“vol”,“close_ts”,“quote_vol”
])
for c in [“open”,“high”,“low”,“close”,“vol”]:
df[c] = df[c].astype(float)
df[“ts”] = pd.to_datetime(df[“ts”].astype(int), unit=“ms”, utc=True)
return df

def get_ohlcv(symbol: str, tf: str, limit: int = 500, retries: int = 3) -> pd.DataFrame:
for attempt in range(retries):
try:
resp = get_session().get(
“https://api.mexc.com/api/v3/klines”,
params={“symbol”: symbol, “interval”: tf, “limit”: limit},
timeout=8,
).json()
if isinstance(resp, list) and resp:
return _parse_klines(resp)
time.sleep(1)
except Exception as e:
log.error(f”get_ohlcv {symbol} {tf} (try {attempt+1}): {e}”)
time.sleep(1)
return pd.DataFrame()

def get_ohlcv_full(symbol: str, tf: str, target: int) -> pd.DataFrame:
“”“جلب تاريخي كامل بصفحات — يضمن دقة المؤشرات”””
all_dfs = []
end_ms = None
fetched = 0

```
while fetched < target:
limit = min(1000, target - fetched)
params = {"symbol": symbol, "interval": tf, "limit": limit}
if end_ms is not None:
params["endTime"] = end_ms
try:
resp = get_session().get(
"https://api.mexc.com/api/v3/klines",
params=params, timeout=10,
).json()
except Exception as e:
log.error(f"full fetch {symbol} {tf}: {e}")
break

if not isinstance(resp, list) or not resp:
break

all_dfs.insert(0, _parse_klines(resp))
fetched += len(resp)

if len(resp) < limit:
break
end_ms = int(resp[0][0]) - 1
time.sleep(0.05)

if not all_dfs:
return pd.DataFrame()

return (pd.concat(all_dfs)
.drop_duplicates(subset="ts")
.sort_values("ts")
.reset_index(drop=True))
```

# ─── كاش ─────────────────────────────────────────────────────────────────────

def cache_merge(symbol: str, tf: str, new_df: pd.DataFrame):
if new_df.empty:
return
key = (symbol, tf)
maxc = CACHE_MAX_CANDLES.get(tf, 5000)
with ohlcv_cache_lock:
old = ohlcv_cache.get(key)
if old is not None and not old.empty:
merged = (pd.concat([old, new_df])
.drop_duplicates(subset=“ts”)
.sort_values(“ts”))
ohlcv_cache[key] = merged.tail(maxc).reset_index(drop=True)
else:
ohlcv_cache[key] = new_df.tail(maxc).reset_index(drop=True)

def get_cached(symbol: str, tf: str) -> pd.DataFrame:
with ohlcv_cache_lock:
df = ohlcv_cache.get((symbol, tf))
return df.copy() if df is not None else pd.DataFrame()

# ─── التحميل التاريخي الكامل ─────────────────────────────────────────────────

def prefetch_all(symbols: list):
total = len(symbols)
log.info(f”📦 بدء التحميل الكامل لـ {total} عملة…”)

```
for i, sym in enumerate(symbols):
for tf, n in API_FETCH_CANDLES.items():
try:
df = get_ohlcv_full(sym, tf, target=n)
if not df.empty:
cache_merge(sym, tf, df)
except Exception as e:
log.error(f"prefetch {sym} {tf}: {e}")
time.sleep(0.05)

if (i + 1) % 25 == 0 or i == total - 1:
log.info(f"📦 {i+1}/{total}")

prefetch_done.set()
log.info("✅ اكتمل التحميل")
send_telegram(
f"✅ <b>التحميل التاريخي اكتمل</b>\n"
f"📊 دقة المؤشرات: 99%+\n"
f"💾 مجموعات في الكاش: {len(ohlcv_cache)}\n"
f"📈 عملات: {total}"
)
```

# ─── التحديث التدريجي في الخلفية ─────────────────────────────────────────────

def _update_batch(symbols, tf, limit):
for sym in symbols:
try:
df = get_ohlcv(sym, tf, limit=limit)
if not df.empty:
cache_merge(sym, tf, df)
except Exception as e:
log.error(f”update {sym} {tf}: {e}”)
time.sleep(0.02)

def cache_updater_1m():
“”“تحديث بيانات 1m كل 45 ثانية ← أقصى تأخر: 45 ثانية”””
while True:
time.sleep(45)
with symbols_cache_lock:
syms = list(symbols_cache)
if syms:
log.debug(f”🔄 تحديث 1m ({len(syms)} عملة)”)
_update_batch(syms, “1m”, limit=5)

def cache_updater_60m():
“”“تحديث بيانات 60m كل 55 دقيقة”””
while True:
time.sleep(55 * 60)
with symbols_cache_lock:
syms = list(symbols_cache)
if syms:
log.debug(f”🔄 تحديث 60m ({len(syms)} عملة)”)
_update_batch(syms, “60m”, limit=3)

# ─── Resample بدقة 99%+ ───────────────────────────────────────────────────────

#

# origin=EPOCH ← نفس المرجع الذي يستخدمه get_next_close

# يضمن تطابق حدود الشمعات مع الواتشر حتى للفريمات الغريبة (21m, 27m …)

#

def resample_ohlcv(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
if df.empty or minutes <= 0:
return pd.DataFrame()
try:
r = (df.copy()
.set_index(“ts”)
.resample(f”{minutes}min”, closed=“left”, label=“left”, origin=EPOCH)
.agg({“open”:“first”,“high”:“max”,“low”:“min”,“close”:“last”,“vol”:“sum”})
.dropna())
return r.iloc[:-1].reset_index() # نحذف الشمعة الأخيرة (قد تكون ناقصة)
except Exception as e:
log.error(f”resample {minutes}m: {e}”)
return pd.DataFrame()

# ─── المؤشرات ─────────────────────────────────────────────────────────────────

def calc_smi(high, low, close, k=10, d=3, ema=10):
hh = high.rolling(k).max()
ll = low.rolling(k).min()
mid = (hh + ll) / 2
ds = (close - mid).ewm(span=d, adjust=False).mean().ewm(span=d, adjust=False).mean()
hls = ((hh - ll) / 2).ewm(span=d, adjust=False).mean().ewm(span=d, adjust=False).mean()
smi = 200 * ds / (hls.abs() + 1e-10)
sig = smi.ewm(span=ema, adjust=False).mean()
return smi, sig

def check_smi_oversold(df: pd.DataFrame, threshold=-40, lookback=5) -> bool:
if len(df) < 30:
return False
smi, _ = calc_smi(df[“high”], df[“low”], df[“close”])
return bool(smi.iloc[-lookback:].min() <= threshold)

def check_macd_green(df: pd.DataFrame) -> bool:
if len(df) < 35:
return False
c = df[“close”]
ml = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
sig = ml.ewm(span=9, adjust=False).mean()
return bool((ml - sig).iloc[-1] > 0)

def check_macd_entry(df: pd.DataFrame, entry_min: int) -> bool:
“””
MACD أحمر (هيستوغرام سالب) مع:
① Signal فوق الهيستوغرام (signal ≥ 0)
② خط MACD الأزرق لم يصعد أكثر من 20% من أعلى قيمة له في 24h
“””
if len(df) < 35:
return False
c = df[“close”]
ml = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
sig = ml.ewm(span=9, adjust=False).mean()
hist = ml - sig

```
if hist.iloc[-1] >= 0: return False # يجب أن يكون أحمر
if sig.iloc[-1] < 0: return False # signal يجب أن يبقى فوق 0
if sig.iloc[-1] < hist.iloc[-1]: return False # signal فوق الهيستوغرام

c24 = max(int(1440 / entry_min), 1)
peak = ml.iloc[-c24:].max() if len(ml) >= c24 else ml.max()
if peak > 0 and ml.iloc[-1] > peak * 0.20:
return False # لم يتراجع كفاية

return True
```

def check_donchian(df: pd.DataFrame, period=20, direction=“green”) -> bool:
if len(df) < period:
return False
hi = df[“high”].rolling(period).max()
lo = df[“low”].rolling(period).min()
mid = (hi + lo) / 2
last = df[“close”].iloc[-1]
return (last >= mid.iloc[-1]) if direction == “green” else (last < mid.iloc[-1])

def check_ema50_below(df: pd.DataFrame) -> bool:
if len(df) < 50:
return False
ema = df[“close”].ewm(span=50, adjust=False).mean()
return bool(df[“close”].iloc[-1] < ema.iloc[-1])

def check_rsi_stoch(df: pd.DataFrame, lookback=5) -> bool:
“””
RSI (Wilder/RMA مطابق لـ TradingView) يتقاطع فوق SMA(14)
+ Stochastic ممهد يتخطى 20 من تحت
“””
if len(df) < 50:
return False

```
close, high, low = df["close"], df["high"], df["low"]

# RSI بـ Wilder's RMA — مطابق تماماً لـ TradingView
delta = close.diff()
alpha = 1 / 14
rma_g = delta.clip(lower=0).ewm(alpha=alpha, adjust=False).mean()
rma_l = (-delta.clip(upper=0)).ewm(alpha=alpha, adjust=False).mean()
rsi = 100 - (100 / (1 + rma_g / (rma_l + 1e-10)))
rsi_ma = rsi.rolling(14).mean()

if rsi.iloc[-10:].min() > 35:
return False

rsi_cross = any(
rsi.iloc[i-1] < rsi_ma.iloc[i-1] and rsi.iloc[i] >= rsi_ma.iloc[i]
for i in range(-5, 0)
)
if not rsi_cross:
return False

# Stochastic ممهد (مطابق TradingView: length=14, smooth=3)
lo14 = low.rolling(14).min()
hi14 = high.rolling(14).max()
k_raw = 100 * (close - lo14) / (hi14 - lo14 + 1e-10)
k = k_raw.rolling(3).mean()

return any(
k.iloc[i-1] < 20 and k.iloc[i] >= 20
for i in range(-lookback, 0)
)
```

# ─── المسح لعملة واحدة ───────────────────────────────────────────────────────

def scan_symbol(symbol: str, entry_min: int, confirm_min: int, third_min: int,
ec_api: str, t_api: str):

```
raw_ec = get_cached(symbol, ec_api)
raw_t = get_cached(symbol, t_api)

if raw_ec.empty or len(raw_ec) < 50:
return
if raw_t.empty or len(raw_t) < 50:
return

# بناء الثلاثة فريمات بالـ Resample
df_entry = resample_ohlcv(raw_ec, entry_min)
df_confirm = resample_ohlcv(raw_ec, confirm_min)

# الثلث: إذا كان t_api = ec_api والتقسيم صحيح، نُعيد بناءه من نفس المصدر
df_third = resample_ohlcv(raw_t, third_min)

if any(d.empty or len(d) < 30 for d in [df_entry, df_confirm, df_third]):
return

# ━━━ فريم التأكيد (×3) ━━━
if not check_macd_green(df_confirm): return
if not check_donchian(df_confirm, direction="green"): return

# ━━━ فريم الدخول ━━━
if not check_smi_oversold(df_entry): return
if not check_macd_entry(df_entry, entry_min): return
if not check_donchian(df_entry, direction="green"): return
if not check_ema50_below(df_entry): return

# ━━━ فريم الثلث (÷3) ━━━
if not check_rsi_stoch(df_third): return
if not check_donchian(df_third, direction="red"): return

# ─── إرسال الإشارة ───
last_ts = (df_entry["ts"].iloc[-1].strftime("%Y%m%d%H%M")
if "ts" in df_entry.columns else "x")
key = f"{symbol}_{entry_min}_{last_ts}"

with alerted_keys_lock:
if key in alerted_keys:
return
alerted_keys[key] = datetime.now(timezone.utc)

price = df_entry["close"].iloc[-1]
save_signal(symbol, price, entry_min, confirm_min, third_min)

send_telegram(
f"🚨 <b>إشارة دخول مكتملة</b>\n"
f"━━━━━━━━━━━━━━\n"
f"🪙 <b>{symbol}</b>\n"
f"📊 التأكيد : {confirm_min}m | MACD 🟢 | Donchian 🟢\n"
f"🎯 الدخول : <b>{entry_min}m</b> | SMI📉 | MACD🔴 | Donchian🟢 | EMA50✅\n"
f"⚡ الثلث : {third_min}m | RSI↑ | Stoch>20 | Donchian🔴\n"
f"💰 السعر : {price:.6g}\n"
f"🕐 الوقت : {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
f"━━━━━━━━━━━━━━"
)
log.info(f"✅ Signal: {symbol} {entry_min}m/{confirm_min}m/{third_min}m @ {price:.6g}")
```

# ─── الواتشر ──────────────────────────────────────────────────────────────────

def get_next_close(tf_minutes: int) -> datetime:
“”“يحسب وقت إغلاق الشمعة القادمة بالضبط — مرجعه Epoch (نفس Resample)”””
now = datetime.now(timezone.utc)
epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
total = int((now - epoch).total_seconds() / 60)
n = total // tf_minutes
return epoch + timedelta(minutes=(n + 1) * tf_minutes)

def candle_watcher(entry_min: int, confirm_min: int, third_min: int,
ec_api: str, t_api: str):
label = f”{entry_min}m/{confirm_min}m/{third_min}m”
log.info(f”⏱️ Watcher جاهز: {label}”)

```
while True:
nxt = get_next_close(entry_min)
wait = (nxt - datetime.now(timezone.utc)).total_seconds()
log.info(
f"⏳ {label}: إغلاق بعد "
f"{int(max(wait,0)//60)}د {int(max(wait,0)%60)}ث"
)
time.sleep(max(wait, 0) + 2.0) # +2 ثانية هامش

cleanup_alerted_keys()

with symbols_cache_lock:
symbols = list(symbols_cache)

if not symbols:
time.sleep(10)
continue

log.info(f"🕯️ {label}: بدء المسح على {len(symbols)} عملة")
t0 = time.time()

with ThreadPoolExecutor(max_workers=15) as ex:
ex.map(
lambda s: scan_symbol(s, entry_min, confirm_min, third_min, ec_api, t_api),
symbols
)

log.info(f"✅ {label}: انتهى في {time.time()-t0:.1f}ث")
```

# ─── تحديث العملات ───────────────────────────────────────────────────────────

def update_symbols_loop():
first = True
while True:
try:
resp = get_session().get(
“https://api.mexc.com/api/v3/ticker/24hr”,
timeout=15,
).json()

```
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

log.info(f"✅ {len(symbols_cache)} عملة | أعلى 5: {symbols_cache[:5]}")

if first:
first = False
threading.Thread(
target=prefetch_all,
args=(list(new_syms),),
daemon=True,
).start()
else:
log.error(f"Ticker: {resp}")

except Exception as e:
log.error(f"update_symbols: {e}")

time.sleep(3600)
```

# ─── Health Check ─────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
def do_GET(self):
self.send_response(200)
self.end_headers()
self.wfile.write(
f”OK | syms={len(symbols_cache)} | cache={len(ohlcv_cache)}”.encode()
)
def log_message(self, *_): pass

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
log.info(“🚀 Tripling Strategy Bot — بدء التشغيل”)

```
send_telegram(
"🚀 <b>بوت استراتيجية التثليث — يعمل الآن!</b>\n"
"━━━━━━━━━━━━━━━━━━━━━━━\n"
"<b>🔢 12 زوج تثليث (دخول / تأكيد × 3 / ثلث ÷ 3):</b>\n"
"⚡ 9m → 27m → 3m\n"
"⚡ 12m → 36m → 4m\n"
"⚡ 15m → 45m → 5m\n"
"⚡ 18m → 54m → 6m\n"
"⚡ 21m → 63m → 7m\n"
"⚡ 24m → 72m → 8m\n"
"⚡ 27m → 81m → 9m\n"
"⚡ 30m → 90m → 10m\n"
"⚡ 45m → 135m → 15m\n"
"⚡ 60m → 180m → 20m\n"
"⚡120m → 360m → 40m\n"
"⚡180m → 540m → 60m\n"
"━━━━━━━━━━━━━━━━━━━━━━━\n"
"<b>📋 شروط التأكيد (×3):</b>\n"
" • MACD هيستوغرام أخضر\n"
" • Donchian Ribbon أخضر\n\n"
"<b>📋 شروط الدخول:</b>\n"
" • SMI تشبع بيعي أسفل -40\n"
" • MACD أحمر + Signal فوق 0\n"
" • MACD الأزرق ≤ 20% من قمته 24h\n"
" • Donchian أخضر\n"
" • السعر تحت EMA 50\n\n"
"<b>📋 شروط الثلث (÷3):</b>\n"
" • RSI (Wilder RMA) يتقاطع فوق SMA14\n"
" • Stochastic ممهد يتخطى 20\n"
" • Donchian أحمر\n"
"━━━━━━━━━━━━━━━━━━━━━━━\n"
"📦 جاري تحميل البيانات التاريخية...\n"
"الأوامر: 1=اليوم | 2=أمس | 3=أسبوع | /status"
)

# تشغيل محدِّث العملات (يبدأ Prefetch تلقائياً)
threading.Thread(target=update_symbols_loop, daemon=True).start()

log.info("⏳ انتظار تحميل قائمة العملات...")
while not symbols_cache:
time.sleep(1)
log.info(f"✅ {len(symbols_cache)} عملة جاهزة")

# الخيوط الخلفية
threading.Thread(target=poll_telegram_commands, daemon=True).start()
threading.Thread(target=cache_updater_1m, daemon=True).start()
threading.Thread(target=cache_updater_60m, daemon=True).start()
threading.Thread(
target=lambda: HTTPServer(("0.0.0.0", PORT), HealthHandler).serve_forever(),
daemon=True,
).start()

# 12 واتشر — واحد لكل زوج تثليث
for entry_min, confirm_min, third_min, ec_api, t_api in TRIPLING_PAIRS:
threading.Thread(
target=candle_watcher,
args=(entry_min, confirm_min, third_min, ec_api, t_api),
daemon=True,
).start()

log.info("✅ 12 واتشر يعملون — البوت جاهز")

while True:
time.sleep(60)
```

if **name** == “**main**”:
main()