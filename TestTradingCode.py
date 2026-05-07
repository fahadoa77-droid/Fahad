import os
import requests
import pandas as pd
import numpy as np
import time
import logging
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

# ═══════════════════════════════════════════════════════════
#  Logging Setup
# ═══════════════════════════════════════════════════════════
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ╔══════════════════════════════════════════════════════════╗
# ║              ⚙️  Settings - Edit here only               ║
# ╠══════════════════════════════════════════════════════════╣
# ║  1) Go to @BotFather on Telegram and create a new bot   ║
# ║     Copy the token and put it in TELEGRAM_TOKEN          ║
# ║                                                          ║
# ║  2) Go to @userinfobot on Telegram                       ║
# ║     Copy the ID and put it in TELEGRAM_CHAT_ID           ║
# ║                                                          ║
# ║  3) Edit the number of coins and timeframes as needed    ║
# ╚══════════════════════════════════════════════════════════╝

# Settings loaded from environment variables (for Railway / cloud hosting)
# If not set, falls back to the hardcoded defaults below
TELEGRAM_TOKEN="8750346745:AAEJBJP_lCr6RLDWr9pj3tvpuRkt6f2tbpg"
TELEGRAM_CHAT_ID="-1003562604082"
TOP_SYMBOLS_LIMIT = int(os.environ.get("TOP_SYMBOLS_LIMIT", "50"))
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "60"))

# PORT for health check server (Railway requires a listening port)
PORT = int(os.environ.get("PORT", "8080"))

# DEBUG_MODE: True = Prints details for every failed condition (useful for testing only)
DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"

# ─── Timeframes ──────────────────────────────────────────
# (label, main_frame, entry_frame, confirm_frame, cancel_frame) — in minutes
TIMEFRAMES = [
    ("15m",   15,    5,    45,   45),
    ("30m",   30,   10,    90,   60),
    ("1h",    60,   20,   180,   90),
    ("2h",   120,   40,   360,  150),
    ("4h",   240,   80,   720,  270),
    ("1d",  1440,  480,  4320, 1470),
]

# ═══════════════════════════════════════════════════════════
#                   Core Code - Do NOT edit
# ═══════════════════════════════════════════════════════════

state = {}

# ─── Health Check Server (Required by Railway) ───────────
class HealthHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler that responds to health checks."""
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        active_filters = len(state)
        self.wfile.write(f"Bot is running. Active filters: {active_filters}\n".encode())

    def log_message(self, format, *args):
        pass  # Suppress HTTP access logs

def start_health_server():
    """Start a background HTTP server for Railway health checks."""
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        log.info(f"Health check server started on port {PORT}")
    except Exception as e:
        log.warning(f"Could not start health server on port {PORT}: {e}")

# ─── Terminal Output ─────────────────────────────────────
def print_signal(msg: str):
    print("\n" + "="*60)
    print(msg)
    print("="*60 + "\n")

# ─── Telegram Sender ─────────────────────────────────────
def send_telegram(message: str):
    if not TELEGRAM_TOKEN or TELEGRAM_TOKEN == "PUT_YOUR_BOT_TOKEN_HERE":
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=15
        )
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ─── Leverage Calculation ────────────────────────────────
def calc_leverage(entry_price: float, lowest_low: float) -> int:
    if lowest_low <= 0 or entry_price <= 0 or lowest_low >= entry_price:
        return 40
    drop_pct = (entry_price - lowest_low) / entry_price * 100
    if drop_pct >= 1.30:
        return 40
    elif drop_pct >= 0.85:
        return 50
    else:
        return 70

# ─── Data Fetching — Fixed for TradingView matching ─────
def _base_interval(minutes: int):
    binance_map = [
        (1,    "1m"),  (3,    "3m"),  (5,    "5m"),
        (15,  "15m"),  (30,  "30m"),  (60,   "1h"),
        (120,  "2h"),  (240,  "4h"),  (360,  "6h"),
        (480,  "8h"),  (720, "12h"),  (1440, "1d"),
        (4320, "3d"),  (10080,"1w"),
    ]
    for m, l in binance_map:
        if minutes <= m:
            return l, m
    return "1w", 10080

def get_bars(symbol: str, minutes: int, limit: int = 300) -> pd.DataFrame:
    """Fetch OHLCV bars from Binance with retry logic and request caching."""
    try:
        base_label, base_min = _base_interval(minutes)
        # Fetch additional data for warmup (important for EMA/RSI accuracy)
        fetch_limit = min(1000, max(int(limit * (minutes / base_min)) + 50, 300))

        # Simple per-cycle cache to avoid redundant API calls
        cache_key = f"{symbol}_{base_label}_{fetch_limit}"
        if not hasattr(get_bars, "_cache"):
            get_bars._cache = {}
        if cache_key in get_bars._cache:
            df = get_bars._cache[cache_key].copy()
        else:
            # Retry up to 2 times on failure
            resp = None
            for attempt in range(2):
                try:
                    resp = requests.get(
                        "https://api.binance.com/api/v3/klines",
                        params={"symbol": symbol, "interval": base_label, "limit": fetch_limit},
                        timeout=20
                    )
                    if resp.status_code == 200:
                        break
                except requests.exceptions.RequestException:
                    if attempt == 1:
                        raise
                    time.sleep(1)

            if resp is None or resp.status_code != 200:
                return pd.DataFrame()

            data = resp.json()
            if not data or isinstance(data, dict):
                return pd.DataFrame()

            df = pd.DataFrame(data, columns=[
                "t","open","high","low","close","volume",
                "close_time","quote_vol","trades",
                "taker_buy_base","taker_buy_quote","ignore"
            ])
            # ✅ Fix: UTC Standardization
            df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
            for col in ["open","high","low","close","volume"]:
                df[col] = df[col].astype(float)
            df = df.set_index("t").sort_index()[["open","high","low","close","volume"]]

            get_bars._cache[cache_key] = df.copy()

        # ✅ Fix: Correct Resample matching TradingView candle boundaries
        if base_min != minutes:
            df = df.resample(
                f"{minutes}min",
                origin='start_day',
                closed='left',
                label='left'
            ).agg({
                "open":"first","high":"max","low":"min",
                "close":"last","volume":"sum"
            }).dropna()

        return df.tail(limit)
    except Exception as e:
        log.error(f"get_bars error {symbol} {minutes}m: {e}")
        return pd.DataFrame()

# ─── Top Symbols ─────────────────────────────────────────
def get_top_symbols(limit=50) -> list:
    try:
        resp = requests.get("https://api.binance.com/api/v3/ticker/24hr", timeout=15)
        if resp.status_code != 200:
            log.error(f"get_top_symbols HTTP error: {resp.status_code}")
            return []
        tickers = resp.json()
        if not isinstance(tickers, list):
            log.error(f"get_top_symbols unexpected response type")
            return []
        usdt = [t for t in tickers if isinstance(t, dict) and t.get("symbol","").endswith("USDT")]
        usdt.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
        symbols = [t["symbol"] for t in usdt[:limit]]
        log.info(f"Loaded {len(symbols)} symbols")
        return symbols
    except Exception as e:
        log.error(f"get_top_symbols error: {e}")
        return []

# ─── Indicators — Matching TradingView ───────────────────
def ema(s: pd.Series, p: int) -> pd.Series:
    """EMA matching ta.ema in Pine Script (adjust=False)"""
    return s.ewm(span=p, adjust=False).mean()

def sma(s: pd.Series, p: int) -> pd.Series:
    return s.rolling(p).mean()

def rma(series: pd.Series, period: int) -> pd.Series:
    """
    ✅ Wilder's Smoothing (RMA) — Exactly matching ta.rma in Pine Script
    1st value = SMA of 1st period items
    Subsequent = (prev * (period-1) + current) / period
    """
    result = pd.Series(index=series.index, dtype='float64')
    valid = series.dropna()
    if len(valid) < period:
        return result

    first_idx = valid.index[period - 1]
    result.loc[first_idx] = valid.iloc[:period].mean()

    prev = result.loc[first_idx]
    for i in range(period, len(valid)):
        idx = valid.index[i]
        prev = (prev * (period - 1) + valid.iloc[i]) / period
        result.loc[idx] = prev

    return result

def calc_ema40(df):
    return ema(df["close"], 40)

def calc_smi(df, k_len=10, d_len=3, ema_len=10):
    """
    ✅ SMI matching TradingView
    SMI = 200 * double_ema(distance) / double_ema(range)
    """
    ll  = df["low"].rolling(k_len).min()
    hh  = df["high"].rolling(k_len).max()
    distance  = df["close"] - (hh + ll) / 2
    range_val = hh - ll

    # Double smoothing: EMA of EMA using d_len for BOTH passes
    dist_smooth  = ema(ema(distance, d_len), d_len)
    range_smooth = ema(ema(range_val, d_len), d_len)

    smi_val = 200 * dist_smooth / range_smooth.replace(0, np.nan)
    signal  = ema(smi_val.fillna(0), ema_len)
    return smi_val, signal

def calc_macd(df, fast=12, slow=26, sig=9):
    """MACD matching TradingView (EMA with adjust=False)"""
    ml = ema(df["close"], fast) - ema(df["close"], slow)
    sl = ema(ml, sig)
    return ml, sl, ml - sl

def calc_rsi(df, period=14):
    """✅ RSI using Wilder's method (RMA) — Exactly matching TradingView"""
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta.clip(upper=0))
    avg_gain = rma(gain, period)
    avg_loss = rma(loss, period)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_stoch(df, k_len=15, k_smooth=3, d_smooth=3):
    ll   = df["low"].rolling(k_len).min()
    hh   = df["high"].rolling(k_len).max()
    k    = 100 * (df["close"] - ll) / (hh - ll).replace(0, np.nan)
    ks   = k.rolling(k_smooth).mean()
    return ks, ks.rolling(d_smooth).mean()

def calc_donchian(df, period=10):
    upper = df["high"].rolling(period).max()
    lower = df["low"].rolling(period).min()
    return df["close"] > (upper + lower) / 2

# ─── Filter Check ────────────────────────────────────────
def check_filter(symbol, main_min, confirm_min) -> bool:
    try:
        df_m = get_bars(symbol, main_min, 200)
        df_c = get_bars(symbol, confirm_min, 100)
        if df_m.empty or len(df_m) < 60:
            return False

        # 1- SMI ≤ -40
        smi_v, _ = calc_smi(df_m)
        if pd.isna(smi_v.iloc[-1]) or smi_v.iloc[-1] > -40:
            if DEBUG_MODE: log.info(f"{symbol} [{main_min}m] Filter: SMI={smi_v.iloc[-1]:.2f} (needs ≤ -40)")
            return False

        # 2- Price < EMA 40
        ema40 = calc_ema40(df_m)
        if df_m["close"].iloc[-1] >= ema40.iloc[-1]:
            if DEBUG_MODE: log.info(f"{symbol} [{main_min}m] Filter: Price >= EMA40")
            return False

        # 3- Donchian GREEN (Main)
        if not calc_donchian(df_m).iloc[-1]:
            if DEBUG_MODE: log.info(f"{symbol} [{main_min}m] Filter: Donchian RED")
            return False

        # 4- MACD Main RED (histogram < 0)
        _, _, hist_m = calc_macd(df_m)
        if hist_m.iloc[-1] >= 0:
            if DEBUG_MODE: log.info(f"{symbol} [{main_min}m] Filter: MACD hist >= 0")
            return False

        # 5- MACD Confirmation GREEN (histogram > 0)
        if df_c.empty or len(df_c) < 30:
            return False
        _, _, hist_c = calc_macd(df_c)
        if hist_c.iloc[-1] <= 0:
            if DEBUG_MODE: log.info(f"{symbol} [{main_min}m] Filter: MACD confirm hist <= 0")
            return False

        # 6- Donchian GREEN (Confirmation)
        if not calc_donchian(df_c).iloc[-1]:
            if DEBUG_MODE: log.info(f"{symbol} [{main_min}m] Filter: Donchian confirm RED")
            return False

        # 7- MACD Line ≤ 20% of Weekly Max Positive
        # ✅ Fix: replaced deprecated df.last("7D") with .loc[] mask
        seven_days_ago = df_m.index[-1] - pd.Timedelta(days=7)
        week = df_m.loc[df_m.index >= seven_days_ago]
        if not week.empty and len(week) >= 26:
            ml_w, _, _ = calc_macd(week)
            pos = ml_w[ml_w > 0]
            if not pos.empty and ml_w.iloc[-1] > pos.max() * 0.20:
                if DEBUG_MODE: log.info(f"{symbol} [{main_min}m] Filter: Weekly MACD too high")
                return False

        # 8- MACD Line ≥ Histogram
        ml_f, _, hist_f = calc_macd(df_m)
        if ml_f.iloc[-1] < hist_f.iloc[-1]:
            if DEBUG_MODE: log.info(f"{symbol} [{main_min}m] Filter: MACD line < histogram")
            return False

        return True
    except Exception as e:
        log.error(f"check_filter error {symbol}: {e}")
        return False

# ─── Entry Check ─────────────────────────────────────────
def check_entry(symbol, entry_min) -> tuple:
    try:
        df = get_bars(symbol, entry_min, 100)
        if df.empty or len(df) < 30:
            return False, None

        # 1- SMI ≤ -40
        smi_v, _ = calc_smi(df)
        if pd.isna(smi_v.iloc[-1]) or smi_v.iloc[-1] > -40:
            return False, None

        # 2- Donchian RED
        if calc_donchian(df).iloc[-1]:
            return False, None

        # 3- RSI ≤ 35 and Crossover SMA from below
        rsi   = calc_rsi(df)
        r_sma = sma(rsi, 14)
        if pd.isna(rsi.iloc[-1]) or rsi.iloc[-1] > 35:
            return False, None

        rsi_clean = rsi.dropna()
        rsma_clean = r_sma.dropna()
        common = rsi_clean.index.intersection(rsma_clean.index)
        if len(common) < 2:
            return False, None
        # Crossover: RSI was below SMA and now above or equal
        if not ((rsi.loc[common[-2]] < r_sma.loc[common[-2]]) and
                (rsi.loc[common[-1]] >= r_sma.loc[common[-1]])):
            return False, None

        # 4- Stochastic Above 20
        k, _ = calc_stoch(df)
        if pd.isna(k.iloc[-1]) or k.iloc[-1] <= 20:
            return False, None

        return True, df["close"].iloc[-1]
    except Exception as e:
        log.error(f"check_entry error {symbol}: {e}")
        return False, None

# ─── Ongoing Confirmation Check ──────────────────────────
def check_ongoing(symbol, confirm_min) -> bool:
    try:
        df = get_bars(symbol, confirm_min, 30)
        if df.empty or len(df) < 10:
            return False
        _, _, hist = calc_macd(df)
        return hist.iloc[-1] > 0 and calc_donchian(df).iloc[-1]
    except Exception as e:
        log.error(f"check_ongoing error {symbol}: {e}")
        return False

# ─── Cancellation Check ─────────────────────────────────
def check_cancel(symbol, cancel_min, filter_time) -> bool:
    try:
        df = get_bars(symbol, cancel_min, 20)
        if df.empty:
            return False
        ft = pd.Timestamp(filter_time)
        if ft.tzinfo is None:
            ft = ft.tz_localize("UTC")
        recent = df[df.index > ft]
        if recent.empty:
            return False
        smi_v, _ = calc_smi(df)
        for idx in recent.index:
            if idx in smi_v.index:
                v = smi_v[idx]
                if not pd.isna(v) and v <= -40:
                    return True
        return False
    except Exception as e:
        log.error(f"check_cancel error {symbol}: {e}")
        return False

# ─── Update Lowest Low ───────────────────────────────────
def update_lowest_low(symbol, main_min, filter_time, current_low) -> float:
    try:
        df = get_bars(symbol, main_min, 100)
        if df.empty:
            return current_low
        ft = pd.Timestamp(filter_time)
        if ft.tzinfo is None:
            ft = ft.tz_localize("UTC")
        since = df[df.index >= ft]
        if since.empty:
            return current_low
        return min(since["low"].min(), current_low)
    except Exception as e:
        log.error(f"update_lowest_low error {symbol}: {e}")
        return current_low

# ─── Main Loop ───────────────────────────────────────────
def main():
    # Start health check server (needed for Railway)
    start_health_server()

    startup_msg = "✅ Bot is running! Loading symbols..."
    print_signal(startup_msg)
    send_telegram(startup_msg)

    symbols = get_top_symbols(limit=TOP_SYMBOLS_LIMIT)
    if not symbols:
        print("❌ Failed to load symbols. Check internet connection.")
        return

    ready_msg = f"✅ Loaded {len(symbols)} symbols — Monitoring {len(TIMEFRAMES)} timeframes"
    print_signal(ready_msg)
    send_telegram(ready_msg)

    log.info("Monitoring started...")

    while True:
        try:
            now = datetime.now(timezone.utc)

            # Clear cache at the start of each cycle
            get_bars._cache = {}

            for symbol in symbols:
                for (label, main_min, entry_min, confirm_min, cancel_min) in TIMEFRAMES:
                    key = (symbol, label)
                    try:
                        sym_state = state.get(key, {})

                        if sym_state.get("filter_time"):
                            filter_time = sym_state["filter_time"]

                            sym_state["lowest_low"] = update_lowest_low(
                                symbol, main_min, filter_time,
                                sym_state.get("lowest_low", float("inf"))
                            )

                            if check_cancel(symbol, cancel_min, filter_time):
                                msg = (
                                    f"🚫 Signal Cancelled\n"
                                    f"Symbol: {symbol} | Frame: {label}\n"
                                    f"Reason: Oversold in cancellation frame ({cancel_min}m)"
                                )
                                print_signal(msg)
                                send_telegram(msg)
                                state.pop(key, None)
                                continue

                            if not check_ongoing(symbol, confirm_min):
                                state.pop(key, None)
                                continue

                            entry, price = check_entry(symbol, entry_min)
                            if entry and price:
                                lowest_low = sym_state.get("lowest_low", price)
                                if lowest_low == float("inf"):
                                    lowest_low = price
                                leverage = calc_leverage(price, lowest_low)
                                drop_pct  = (price - lowest_low) / price * 100 if price > 0 else 0

                                msg = (
                                    f"🟢 BUY SIGNAL!\n"
                                    f"Symbol: <b>{symbol}</b>\n"
                                    f"Frame: <b>{label}</b>\n"
                                    f"Entry Price: <b>{price:.6f}</b>\n"
                                    f"Lowest Low since Filter: <b>{lowest_low:.6f}</b> ({drop_pct:.2f}%)\n"
                                    f"Suggested Leverage: <b>{leverage}x</b>\n"
                                    f"Target: Double Capital (2x)\n"
                                    f"Time: {now.strftime('%Y-%m-%d %H:%M')} UTC"
                                )
                                print_signal(msg)
                                send_telegram(msg)
                                log.info(f"SIGNAL: {symbol} [{label}] @ {price} | {leverage}x")
                                state.pop(key, None)

                        else:
                            if check_filter(symbol, main_min, confirm_min):
                                log.info(f"{symbol} [{label}]: ✅ Filter Met")
                                send_telegram(f"🔎 Filter Met: {symbol} | {label}")
                                df_now   = get_bars(symbol, main_min, 5)
                                init_low = df_now["low"].iloc[-1] if not df_now.empty else float("inf")
                                state[key] = {"filter_time": now, "lowest_low": init_low}

                    except Exception as e:
                        log.error(f"{symbol} [{label}]: {e}")

                    time.sleep(0.1)

            log.info(f"Cycle finished — Active filters: {len(state)} — Waiting for next cycle...")
            time.sleep(SCAN_INTERVAL_SEC)

        except KeyboardInterrupt:
            log.info("Bot stopped ✋")
            break
        except Exception as e:
            log.error(f"General Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()