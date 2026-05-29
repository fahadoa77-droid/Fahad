import os
import requests
import pandas as pd
import numpy as np
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

# ──────────────────────────────────────────────
# ⚙️ الإعدادات الرئيسية
# ──────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "8298845980:AAFrhgrdngO6b1vV9poLyw7c_yT0afTkMg4")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1003853071475")

# MEXC API — للقراءة العامة لا يحتاج مفتاح
MEXC_BASE         = "https://api.mexc.com"
TOP_SYMBOLS_LIMIT = 200
PORT              = int(os.environ.get("PORT", "8080"))
ALERT_EXPIRY_HOURS = 4
NEAR6_EXPIRY_HOURS = 2

# ──────────────────────────────────────────────
# تعيين الفريمات
# ──────────────────────────────────────────────
TF_MAP = {"1m": "1m", "60m": "60m"}

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
    ( 90, 270, 30, "60m","1m"),
    (120, 360, 40, "60m","1m"),
    (180, 540, 60, "60m","60m"),
]

TIMEFRAME_CHAIN = [9, 12, 15, 18, 21, 24, 27, 30, 45, 60, 90, 120, 180]
NEXT_TF = {TIMEFRAME_CHAIN[i]: TIMEFRAME_CHAIN[i+1] for i in range(len(TIMEFRAME_CHAIN)-1)}

FAST_FETCH_CANDLES = {"1m": 3500, "60m": 250}
API_FETCH_CANDLES  = {"1m": 15_000, "60m": 2_000}
CACHE_MAX_CANDLES  = {"1m": 16_000, "60m": 2_500}
EPOCH = pd.Timestamp("1970-01-01", tz="UTC")

WARMUP_EMA   = 200
WARMUP_MACD  = 200
WARMUP_SMI   = 100
WARMUP_RSI   = 200
WARMUP_STOCH = 100
WARMUP_DON   = 50
MIN_CANDLES  = 250

# ──────────────────────────────────────────────
# الحالة المشتركة
# ──────────────────────────────────────────────
alerted_keys        = {}
alerted_keys_lock   = threading.Lock()
trades_history      = deque(maxlen=2000)
trades_lock         = threading.Lock()
near_signals        = {}
near_signals_lock   = threading.Lock()
near_signals_6      = {}
near_signals_6_lock = threading.Lock()
symbols_cache       = []
symbols_cache_lock  = threading.Lock()
ohlcv_cache         = {}
ohlcv_cache_lock    = threading.Lock()

fast_prefetch_done = threading.Event()
prefetch_done      = threading.Event()

diag_counts = {
    "total"           : 0,
    "no_data"         : 0,
    "smi_oversold"    : 0,
    "active_skip"     : 0,
    "macd_red"        : 0,
    "donchian_entry"  : 0,
    "donchian_confirm": 0,
    "macd_confirm"    : 0,
    "ema50"           : 0,
    "rsi_stoch"       : 0,
    "passed"          : 0,
}
diag_lock         = threading.Lock()
cache_diag_logged = threading.Event()
_local            = threading.local()

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def get_session():
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
        if r.get("ok"): log.info("✅ تم حذف الـ Webhook")
    except Exception as e:
        log.error(f"deleteWebhook error: {e}")

def cleanup_alerted_keys():
    now = datetime.now(timezone.utc)
    with alerted_keys_lock:
        expired = [k for k, t in list(alerted_keys.items())
                   if now - t > timedelta(hours=ALERT_EXPIRY_HOURS)]
        for k in expired: del alerted_keys[k]

def cleanup_near6():
    now = datetime.now(timezone.utc)
    with near_signals_6_lock:
        expired = [k for k, v in list(near_signals_6.items())
                   if now - v["time"] > timedelta(hours=NEAR6_EXPIRY_HOURS)]
        for k in expired: del near_signals_6[k]

def save_signal(symbol, price, entry_min, confirm_min, third_min):
    with trades_lock:
        trades_history.append({
            "time"     : datetime.now(timezone.utc),
            "symbol"   : symbol,
            "price"    : price,
            "timeframe": f"{entry_min}m/{confirm_min}m/{third_min}m",
        })

def send_telegram(msg, chat_id=None):
    target = chat_id or TELEGRAM_CHAT_ID
    try:
        r = get_session().post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": target, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        ).json()
        return r.get("ok", False)
    except Exception as e:
        log.error(f"Telegram send error: {e}")
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
        end, title = now, "🗓️ آخر 7 أيام"
    with trades_lock:
        rows = [t for t in trades_history if start <= t["time"] < end]
    if not rows: return f"<b>{title}:</b>\nلا توجد إشارات."
    lines = [f"<b>{title} ({len(rows)})</b>\n" + "━" * 15]
    for t in rows:
        lines.append(
            f"✅ {t['symbol']} | {t['timeframe']} | "
            f"{t['price']:.4g} | {t['time'].strftime('%H:%M UTC')}"
        )
    return "\n".join(lines)

def _parse_mexc_klines(resp):
    df = pd.DataFrame(resp, columns=[
        "ts", "open", "high", "low", "close", "vol",
        "close_time", "quote_vol", "trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    for c in ["open", "high", "low", "close", "vol"]:
        df[c] = df[c].astype(float)
    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms", utc=True)
    return df.sort_values("ts").reset_index(drop=True)[
        ["ts", "open", "high", "low", "close", "vol"]
    ]

def get_ohlcv(symbol, tf, limit=500):
    mexc_tf = TF_MAP.get(tf, "1m")
    try:
        resp = get_session().get(
            f"{MEXC_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": mexc_tf, "limit": min(limit, 1000)},
            timeout=10,
        ).json()
        if isinstance(resp, list) and resp:
            return _parse_mexc_klines(resp)
    except Exception as e:
        log.error(f"get_ohlcv {symbol} {tf}: {e}")
    return pd.DataFrame()

def get_ohlcv_full(symbol, tf, target):
    mexc_tf  = TF_MAP.get(tf, "1m")
    tf_ms    = 60_000 if tf == "1m" else 3_600_000
    MEXC_MAX = 1000
    all_dfs, end_ms, fetched, retries = [], int(time.time() * 1000), 0, 0
    while fetched < target:
        batch    = min(MEXC_MAX, target - fetched)
        start_ms = end_ms - batch * tf_ms
        try:
            resp = get_session().get(
                f"{MEXC_BASE}/api/v3/klines",
                params={"symbol": symbol, "interval": mexc_tf, "startTime": start_ms, "endTime": end_ms, "limit": batch},
                timeout=15,
            ).json()
            if not isinstance(resp, list) or not resp:
                retries += 1
                if retries >= 3: break
                time.sleep(2 ** retries); continue
            df = _parse_mexc_klines(resp)
            all_dfs.insert(0, df)
            fetched += len(df); retries = 0
            end_ms = start_ms - 1
            if len(df) < batch: break
        except Exception as e:
            retries += 1
            if retries >= 3: break
            time.sleep(2)
    return (pd.concat(all_dfs).drop_duplicates(subset="ts").sort_values("ts").reset_index(drop=True) if all_dfs else pd.DataFrame())

def cache_merge(symbol, tf, new_df):
    if new_df.empty: return
    key = (symbol, tf); maxc = CACHE_MAX_CANDLES.get(tf, 5000)
    with ohlcv_cache_lock:
        old = ohlcv_cache.get(key)
        if old is not None and not old.empty:
            merged = pd.concat([old, new_df]).drop_duplicates(subset="ts").sort_values("ts")
            ohlcv_cache[key] = merged.tail(maxc).reset_index(drop=True)
        else:
            ohlcv_cache[key] = new_df.tail(maxc).reset_index(drop=True)

def get_cached(symbol, tf):
    with ohlcv_cache_lock:
        df = ohlcv_cache.get((symbol, tf))
    return df.copy() if df is not None else pd.DataFrame()

def prefetch_all(symbols):
    for sym in symbols:
        for tf, n in FAST_FETCH_CANDLES.items():
            df = get_ohlcv_full(sym, tf, target=n)
            cache_merge(sym, tf, df)
    fast_prefetch_done.set()
    for sym in symbols:
        for tf, n in API_FETCH_CANDLES.items():
            df = get_ohlcv_full(sym, tf, target=n)
            cache_merge(sym, tf, df)
    prefetch_done.set()
    send_telegram("✅ <b>التحميل الكامل اكتمل وجاهز للعمل!</b>")

def _update_batch(symbols, tf, limit):
    def fetch_one(sym):
        df = get_ohlcv(sym, tf, limit=limit)
        if not df.empty: cache_merge(sym, tf, df)
    with ThreadPoolExecutor(max_workers=30) as ex:
        ex.map(fetch_one, symbols)

def cache_updater_1m():
    while True:
        if not fast_prefetch_done.is_set(): time.sleep(5); continue
        with symbols_cache_lock: syms = list(symbols_cache)
        if syms: _update_batch(syms, "1m", limit=5)
        time.sleep(55)

def cache_updater_60m():
    while True:
        time.sleep(3600)
        if fast_prefetch_done.is_set():
            with symbols_cache_lock: syms = list(symbols_cache)
            if syms: _update_batch(syms, "60m", limit=5)

def resample_ohlcv(df, minutes):
    if df.empty: return pd.DataFrame()
    return (df.copy().set_index("ts")
             .resample(f"{minutes}min", closed="left", label="left", origin=EPOCH)
             .agg({"open":"first","high":"max","low":"min","close":"last","vol":"sum"})
             .dropna().iloc[:-1].reset_index())

def check_macd_red(df):
    ml = df["close"].ewm(span=12, adjust=False).mean() - df["close"].ewm(span=26, adjust=False).mean()
    sig = ml.ewm(span=9, adjust=False).mean()
    return bool((ml - sig).iloc[-2] < 0)

def check_macd_green(df):
    ml = df["close"].ewm(span=12, adjust=False).mean() - df["close"].ewm(span=26, adjust=False).mean()
    sig = ml.ewm(span=9, adjust=False).mean()
    return bool((ml - sig).iloc[-2] > 0)

def check_donchian_ribbon(df, direction="green"):
    hh = df["high"].rolling(20).max().shift(1)
    ll = df["low"].rolling(20).min().shift(1)
    if direction == "green": return bool(df["close"].iloc[-2] > hh.iloc[-2])
    return bool(df["close"].iloc[-2] < ll.iloc[-2])

def check_ema50_below(df):
    ema = df["close"].ewm(span=50, adjust=False).mean()
    return bool(df["close"].iloc[-2] < ema.iloc[-2])

def check_smi_oversold(df):
    hh = df["high"].rolling(10).max()
    ll = df["low"].rolling(10).min()
    smi = ((df["close"] - (hh + ll) / 2) / ((hh - ll) / 2 + 1e-10)) * 100
    return bool(smi.iloc[-2] <= -40)

def check_rsi_stoch(df):
    delta = df["close"].diff()
    rsi = 100 - (100 / (1 + (delta.clip(lower=0).ewm(span=14).mean() / (-delta.clip(upper=0).ewm(span=14).mean() + 1e-10))))
    k = (df["close"] - df["low"].rolling(15).min()) / (df["high"].rolling(15).max() - df["low"].rolling(15).min() + 1e-10) * 100
    return bool(rsi.iloc[-2] < 35 and k.iloc[-2] < 20)

def scan_symbol(symbol, entry_min, confirm_min, third_min, ec_api, t_api):
    raw_ec = get_cached(symbol, ec_api)
    raw_t  = get_cached(symbol, t_api)
    if raw_ec.empty or raw_t.empty: return
    df_entry = resample_ohlcv(raw_ec, entry_min)
    df_confirm = resample_ohlcv(raw_ec, confirm_min)
    df_third = resample_ohlcv(raw_t, third_min)
    if df_entry.empty or df_confirm.empty or df_third.empty: return
    if check_smi_oversold(df_entry) and check_macd_red(df_entry) and \
       check_donchian_ribbon(df_entry, "green") and check_donchian_ribbon(df_confirm, "green") and \
       check_macd_green(df_confirm) and check_ema50_below(df_entry) and check_rsi_stoch(df_third):
        save_signal(symbol, df_entry["close"].iloc[-2], entry_min, confirm_min, third_min)
        send_telegram(f"🚨 <b>إشارة دخول:</b> {symbol} | {entry_min}m")

def candle_watcher(entry_min, confirm_min, third_min, ec_api, t_api):
    while True:
        time.sleep(30)
        if not fast_prefetch_done.is_set(): continue
        with symbols_cache_lock: syms = list(symbols_cache)
        fn = partial(scan_symbol, entry_min=entry_min, confirm_min=confirm_min, third_min=third_min, ec_api=ec_api, t_api=t_api)
        with ThreadPoolExecutor(max_workers=20) as ex: ex.map(fn, syms)

def poll_telegram_commands():
    last_id = 0
    while True:
        try:
            r = get_session().get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", params={"offset": last_id + 1, "timeout": 30}).json()
            for upd in r.get("result", []):
                last_id = upd["update_id"]
                txt = upd.get("message", {}).get("text", "")
                chat_id = str(upd.get("message", {}).get("chat", {}).get("id", ""))
                if txt == "/status": send_telegram("🤖 البوت يعمل بكفاءة.", chat_id)
        except: time.sleep(10)

def update_symbols_loop():
    while True:
        try:
            resp = get_session().get(f"{MEXC_BASE}/api/v3/ticker/24hr").json()
            top = sorted([t for t in resp if t["symbol"].endswith("USDT")], key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)[:TOP_SYMBOLS_LIMIT]
            with symbols_cache_lock: symbols_cache[:] = [t["symbol"] for t in top]
            if not fast_prefetch_done.is_set(): threading.Thread(target=prefetch_all, args=(symbols_cache,), daemon=True).start()
        except: pass
        time.sleep(3600)

def main():
    threading.Thread(target=update_symbols_loop, daemon=True).start()
    threading.Thread(target=poll_telegram_commands, daemon=True).start()
    threading.Thread(target=cache_updater_1m, daemon=True).start()
    for params in TRIPLING_PAIRS:
        threading.Thread(target=candle_watcher, args=params, daemon=True).start()
    while True: time.sleep(60)

if __name__ == "__main__":
    main()