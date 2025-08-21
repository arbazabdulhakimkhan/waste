import os, time, json, traceback, threading
from datetime import datetime, timedelta
import ccxt
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import requests

load_dotenv()

# ----------------------------
# CONFIG - Auto from Environment Variables
# ----------------------------
MODE = os.getenv("MODE", "paper").lower()
EXCHANGE_ID = os.getenv("EXCHANGE_ID", "kucoin")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTC/USDT").split(",") if s.strip()]
ENTRY_TF = os.getenv("ENTRY_TF", "1h")
HTF = os.getenv("HTF", "4h")

# DYNAMIC 20% ALLOCATION
TOTAL_PORTFOLIO_CAPITAL = float(os.getenv("TOTAL_PORTFOLIO_CAPITAL", "10000"))
PER_COIN_ALLOCATION = float(os.getenv("PER_COIN_ALLOCATION", "0.20"))
PER_COIN_CAP_USD = TOTAL_PORTFOLIO_CAPITAL * PER_COIN_ALLOCATION

RISK_PERCENT = float(os.getenv("RISK_PERCENT", "0.02"))
RR_FIXED = float(os.getenv("RR_FIXED", "5.0"))
DYNAMIC_RR = os.getenv("DYNAMIC_RR", "true").lower() == "true"
MIN_RR = float(os.getenv("MIN_RR", "4.0"))
MAX_RR = float(os.getenv("MAX_RR", "6.0"))

ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
ATR_MULT_SL = float(os.getenv("ATR_MULT_SL", "1.5"))
USE_ATR_STOPS = os.getenv("USE_ATR_STOPS", "true").lower() == "true"
USE_H1_FILTER = os.getenv("USE_H1_FILTER", "true").lower() == "true"
USE_VOLUME_FILTER = os.getenv("USE_VOLUME_FILTER", "true").lower() == "true"
VOL_LOOKBACK = int(os.getenv("VOL_LOOKBACK", "20"))
VOL_MIN_RATIO = float(os.getenv("VOL_MIN_RATIO", "0.5"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "25"))
BIAS_CONFIRM_BEAR = int(os.getenv("BIAS_CONFIRM_BEAR", "2"))

MAX_DRAWDOWN = float(os.getenv("MAX_DRAWDOWN", "0.20"))
MAX_TRADE_SIZE = float(os.getenv("MAX_TRADE_SIZE", "100000"))
SLIPPAGE_RATE = float(os.getenv("SLIPPAGE_RATE", "0.0005"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.001"))
SLEEP_CAP = int(os.getenv("SLEEP_CAP", "60"))

# Auto-disable modeled fees/slippage in live mode
if MODE == "live":
    SLIPPAGE_RATE = 0.0
    FEE_RATE = 0.0

# KuCoin API keys
API_KEY = os.getenv("KUCOIN_API_KEY", "68a793568711080001ae0433")
API_SECRET = os.getenv("KUCOIN_SECRET", "75403411-c75b-4964-ba6f-aecbcf6af91f")
API_PASSPHRASE = os.getenv("KUCOIN_PASSPHRASE", "Arbazkucoin11")

# Telegram alerts
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "7715595055:AAHyOcJUzDGrDgNaN5x5u-HG9sqdjitqGYs")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "677683819")

LOG_PREFIX = "[BOT]"

# ----------------------------
# Utilities
# ----------------------------
def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}
        )
    except Exception:
        pass

def timeframe_to_minutes(tf: str) -> int:
    tf = tf.strip().lower()
    if tf.endswith("m"): return int(tf[:-1])
    if tf.endswith("h"): return int(tf[:-1]) * 60
    if tf.endswith("d"): return int(tf[:-1]) * 1440
    raise ValueError(f"Unsupported timeframe: {tf}")

def now_utc_naive():
    return datetime.utcnow().replace(tzinfo=None)

def get_exchange():
    if MODE == "live":
        ex = getattr(ccxt, EXCHANGE_ID)({
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "password": API_PASSPHRASE,
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        })
    else:
        ex = getattr(ccxt, EXCHANGE_ID)({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        })
    return ex

def fetch_ohlcv_df(exchange, symbol, timeframe, limit=500):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    if not ohlcv:
        return pd.DataFrame(columns=["Open","High","Low","Close","Volume"])
    df = pd.DataFrame(ohlcv, columns=["timestamp","Open","High","Low","Close","Volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_localize(None)
    df.set_index("timestamp", inplace=True)
    for col in ["Open","High","Low","Close","Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna()

def calculate_atr(df, period=14):
    hl = df['High'] - df['Low']
    hc = (df['High'] - df['Close'].shift()).abs()
    lc = (df['Low'] - df['Close'].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def state_files_for_symbol(symbol: str):
    tag = symbol.replace("/", "_")
    return f"state_{tag}.json", f"{tag}_live_trades.csv"

def load_state(state_file):
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            s = json.load(f)
        s["entry_time"] = pd.to_datetime(s["entry_time"]) if s.get("entry_time") else None
        s["last_processed_ts"] = pd.to_datetime(s["last_processed_ts"]) if s.get("last_processed_ts") else None
        return s
    return {
        "capital": PER_COIN_CAP_USD,
        "position": 0,
        "entry_price": 0.0,
        "entry_sl": 0.0,
        "entry_tp": 0.0,
        "entry_time": None,
        "entry_size": 0.0,
        "peak_equity": PER_COIN_CAP_USD,
        "last_processed_ts": None,
        "bearish_count": 0
    }

def save_state(state_file, state):
    s = dict(state)
    s["entry_time"] = state["entry_time"].isoformat() if state["entry_time"] is not None else None
    s["last_processed_ts"] = state["last_processed_ts"].isoformat() if state["last_processed_ts"] is not None else None
    with open(state_file, "w") as f:
        json.dump(s, f, indent=2)

def append_trade(csv_file, row):
    write_header = not os.path.exists(csv_file)
    pd.DataFrame([row]).to_csv(csv_file, mode="a", header=write_header, index=False)

# ----------------------------
# Precision (live)
# ----------------------------
class MarketInfo:
    def __init__(self, exchange, symbol):
        mkts = exchange.load_markets()
        m = mkts[symbol]
        self.base = m["base"]
        self.quote = m["quote"]
        self.amount_min = m["limits"]["amount"]["min"] or 0.0
        self.cost_min = (m["limits"].get("cost") or {}).get("min") or 0.0
        self.amount_prec = m.get("precision", {}).get("amount", 6)
        self.price_prec = m.get("precision", {}).get("price", 6)
        self.symbol = symbol
        self.exchange = exchange
    def round_amount(self, amt): return float(self.exchange.amount_to_precision(self.symbol, amt))
    def round_price(self, px): return float(self.exchange.price_to_precision(self.symbol, px))

def place_market_buy(exchange, mi: MarketInfo, base_qty: float):
    base_qty = mi.round_amount(max(base_qty, mi.amount_min))
    if base_qty <= 0: raise ValueError(f"Amount too small: {base_qty}")
    return exchange.create_market_buy_order(mi.symbol, base_qty)

def place_market_sell(exchange, mi: MarketInfo, base_qty: float):
    base_qty = mi.round_amount(base_qty)
    if base_qty <= 0: raise ValueError(f"Sell amount too small: {base_qty}")
    return exchange.create_market_sell_order(mi.symbol, base_qty)

def avg_fill_price_from_order(order):
    price = order.get("average") or order.get("price")
    if price: return float(price)
    if "trades" in order and order["trades"]:
        notional = 0.0; qty = 0.0
        for t in order["trades"]:
            p = float(t["price"]); a = float(t["amount"])
            notional += p*a; qty += a
        if qty > 0: return notional / qty
    return None

# ----------------------------
# Strategy core per bar
# ----------------------------
def process_bar(symbol, entry_df, htf_df, state, exchange=None, market_info: MarketInfo=None):
    m = entry_df.copy()
    h = htf_df.copy()

    # Bias and HTF trend
    m["Bias"] = 0
    m.loc[m["Close"] > m["Close"].shift(1), "Bias"] = 1
    m.loc[m["Close"] < m["Close"].shift(1), "Bias"] = -1

    h["Trend"] = 0
    h.loc[h["Close"] > h["Close"].shift(1), "Trend"] = 1
    h.loc[h["Close"] < h["Close"].shift(1), "Trend"] = -1
    m["H4_Trend"] = h["Trend"].reindex(m.index, method="ffill").fillna(0).astype(int)

    # Indicators
    m["ATR"] = calculate_atr(m, ATR_PERIOD) if USE_ATR_STOPS else np.nan
    if USE_VOLUME_FILTER: m["Avg_Volume"] = m["Volume"].rolling(VOL_LOOKBACK).mean()
    m["RSI"] = calculate_rsi(m["Close"], RSI_PERIOD)

    ts = m.index[-1]
    price = float(m["Close"].iloc[-1])
    open_px = float(m["Open"].iloc[-1])
    prev_close = float(m["Close"].iloc[-2]) if len(m) >= 2 else price
    bias = int(m["Bias"].iloc[-1])
    h1_trend = int(m["H4_Trend"].iloc[-1])

    # Drawdown block
    state["peak_equity"] = max(state["peak_equity"], state["capital"])
    curr_dd = (state["peak_equity"] - state["capital"]) / state["peak_equity"] if state["peak_equity"] > 0 else 0.0
    blocked = curr_dd >= MAX_DRAWDOWN

    trade_row = None

    # Permanent stop
    if blocked and not state.get("permanently_stopped", False):
        state["permanently_stopped"] = True
        
        if state["position"] == 1:
            if MODE == "live":
                try:
                    base_qty = state["entry_size"]
                    order = place_market_sell(exchange, market_info, base_qty)
                    fill_px = avg_fill_price_from_order(order) or price
                    exit_price = float(fill_px)
                except Exception as e:
                    send_telegram(f"{symbol} FORCED EXIT error: {e}")
                    raise
            else:
                exit_price = price
                
            pnl = state["entry_size"] * (exit_price - state["entry_price"])
            pnl -= state["entry_size"] * SLIPPAGE_RATE
            pnl -= (exit_price * state["entry_size"]) * FEE_RATE
            state["capital"] += pnl
            
            trade_row = {
                "Symbol": symbol,
                "Trade_ID": int(time.time()),
                "Entry_DateTime": state["entry_time"].isoformat(),
                "Exit_DateTime": ts.isoformat(),
                "Position": "Long",
                "Entry_Price": round(state["entry_price"], 6),
                "Exit_Price": round(exit_price, 6),
                "Take_Profit": round(state["entry_tp"], 6),
                "Stop_Loss": round(state["entry_sl"], 6),
                "Position_Size_Base": round(state["entry_size"], 8),
                "PnL_$": round(pnl, 2),
                "Win": 1 if pnl > 0 else 0,
                "Exit_Reason": "MAX DRAWDOWN - PERMANENT STOP",
                "Capital_After": round(state["capital"], 2),
                "Mode": MODE
            }
            
            state.update({"position": 0, "entry_price": 0.0, "entry_sl": 0.0,
                          "entry_tp": 0.0, "entry_time": None, "entry_size": 0.0})
            
            msg = f"{LOG_PREFIX} {symbol} PERMANENTLY STOPPED DUE TO MAX DRAWDOWN | Final Cap={state['capital']:.2f}"
            print(msg)
            send_telegram(f"🛑 {symbol} PERMANENTLY STOPPED - Max Drawdown Reached!")
            
            return state, trade_row

    blocked = state.get("permanently_stopped", False)

    # Exits
    if state["position"] == 1 and not blocked:
        exit_flag = False
        exit_price = price
        exit_reason = ""

        if price >= state["entry_tp"]:
            exit_flag, exit_price, exit_reason = True, state["entry_tp"], "Take Profit"
            state["bearish_count"] = 0
        elif price <= state["entry_sl"]:
            exit_flag, exit_price, exit_reason = True, state["entry_sl"], "Stop Loss"
            state["bearish_count"] = 0
        elif USE_H1_FILTER and h1_trend < 0:
            exit_flag, exit_price, exit_reason = True, price, "4H Trend Reversal"
            state["bearish_count"] = 0
        elif bias < 0:
            state["bearish_count"] += 1
            if state["bearish_count"] >= BIAS_CONFIRM_BEAR:
                exit_flag, exit_price, exit_reason = True, price, "Bias Reversal"
                state["bearish_count"] = 0
        else:
            state["bearish_count"] = 0

        if exit_flag:
            if MODE == "live":
                try:
                    base_qty = state["entry_size"]
                    order = place_market_sell(exchange, market_info, base_qty)
                    fill_px = avg_fill_price_from_order(order) or price
                    exit_price = float(fill_px)
                except Exception as e:
                    send_telegram(f"{symbol} Exit SELL error: {e}")
                    raise

            pnl = state["entry_size"] * (exit_price - state["entry_price"])
            pnl -= state["entry_size"] * SLIPPAGE_RATE
            pnl -= (exit_price * state["entry_size"]) * FEE_RATE
            state["capital"] += pnl

            trade_row = {
                "Symbol": symbol,
                "Trade_ID": int(time.time()),
                "Entry_DateTime": state["entry_time"].isoformat(),
                "Exit_DateTime": ts.isoformat(),
                "Position": "Long",
                "Entry_Price": round(state["entry_price"], 6),
                "Exit_Price": round(exit_price, 6),
                "Take_Profit": round(state["entry_tp"], 6),
                "Stop_Loss": round(state["entry_sl"], 6),
                "Position_Size_Base": round(state["entry_size"], 8),
                "PnL_$": round(pnl, 2),
                "Win": 1 if pnl > 0 else 0,
                "Exit_Reason": exit_reason,
                "Capital_After": round(state["capital"], 2),
                "Mode": MODE
            }

            state.update({"position": 0, "entry_price": 0.0, "entry_sl": 0.0,
                          "entry_tp": 0.0, "entry_time": None, "entry_size": 0.0})

            msg = f"{LOG_PREFIX} {symbol} {ts} EXIT {exit_reason} @ {exit_price:.4f} | PnL={pnl:.2f} | Cap={state['capital']:.2f}"
            print(msg)
            send_telegram(msg)

    # Entries
    if state["position"] == 0 and not blocked:
        bullish_sweep = (price > open_px) and (price > prev_close)
        vol_ok = True
        if USE_VOLUME_FILTER and not np.isnan(m["Avg_Volume"].iloc[-1]):
            vol_ok = m["Volume"].iloc[-1] >= VOL_MIN_RATIO * m["Avg_Volume"].iloc[-1]
        rsi_ok = True if np.isnan(m["RSI"].iloc[-1]) else m["RSI"].iloc[-1] > RSI_OVERSOLD
        h1_ok = (not USE_H1_FILTER) or (h1_trend == 1)

        if bias == 1 and bullish_sweep and vol_ok and rsi_ok and h1_ok:
            if USE_ATR_STOPS:
                atr_val = float(m["ATR"].iloc[-1])
                if np.isnan(atr_val) or atr_val <= 0:
                    return state, trade_row
                sl = price - (ATR_MULT_SL * atr_val)
            else:
                sweep_buffer = min(max(price * 0.0005, 0.0005), 0.0015)
                sl = price * (1 - sweep_buffer)

            risk = abs(price - sl)
            if risk <= 0:
                return state, trade_row

            rr_ratio = RR_FIXED
            if DYNAMIC_RR and USE_ATR_STOPS:
                recent_atr = float(m["ATR"].iloc[-6:-1].mean()) if len(m) >= 6 else np.nan
                curr_atr = float(m["ATR"].iloc[-1])
                if not np.isnan(recent_atr) and recent_atr > 0:
                    if curr_atr > recent_atr * 1.2: rr_ratio = MIN_RR
                    elif curr_atr < recent_atr * 0.8: rr_ratio = MAX_RR
            tp = price + rr_ratio * risk

            # Per-coin cap-aware sizing
            per_coin_cap = PER_COIN_CAP_USD
            available_cap = min(state["capital"], per_coin_cap)
            size_base = (available_cap * RISK_PERCENT) / risk
            size_base = min(size_base, MAX_TRADE_SIZE / price)
            size_base = min(size_base, per_coin_cap / price)

            if size_base > 0:
                entry_price_used = price
                if MODE == "live":
                    try:
                        mi = market_info
                        size_base = max(size_base, mi.amount_min)
                        size_base = mi.round_amount(size_base)
                        order = place_market_buy(exchange, mi, size_base)
                        fill_px = avg_fill_price_from_order(order) or price
                        entry_price_used = float(fill_px)
                    except Exception as e:
                        send_telegram(f"{symbol} Entry BUY error: {e}")
                        raise

                state["position"] = 1
                state["entry_price"] = entry_price_used
                state["entry_sl"] = sl
                state["entry_tp"] = tp
                state["entry_time"] = ts
                state["entry_size"] = size_base
                state["bearish_count"] = 0

                state["capital"] -= (size_base * SLIPPAGE_RATE)
                state["capital"] -= (entry_price_used * size_base * FEE_RATE)

                msg = f"{LOG_PREFIX} {symbol} {ts} ENTRY Long @ {entry_price_used:.4f} | SL={sl:.4f} TP={tp:.4f} RR={rr_ratio:.2f} SizeBase={size_base:.6f} Cap={state['capital']:.2f} Mode={MODE}"
                print(msg)
                send_telegram(msg)

    state["last_processed_ts"] = ts
    state["peak_equity"] = max(state["peak_equity"], state["capital"])
    return state, trade_row

# ----------------------------
# Worker per symbol
# ----------------------------
def worker(symbol):
    state_file, trades_csv = state_files_for_symbol(symbol)
    exchange = get_exchange()
    market_info = MarketInfo(exchange, symbol) if MODE == "live" else None
    state = load_state(state_file)
    tf_minutes = timeframe_to_minutes(ENTRY_TF)

    print(f"{LOG_PREFIX} Start | {symbol} | TF={ENTRY_TF}/{HTF} | Mode={MODE} | Capital={state['capital']:.2f} | CapLimit={PER_COIN_CAP_USD:.2f}")
    send_telegram(f"Started {symbol} {ENTRY_TF}/{HTF} Mode={MODE} Cap={PER_COIN_CAP_USD}")

    while True:
        try:
            entry_df = fetch_ohlcv_df(exchange, symbol, ENTRY_TF, limit=400)
            htf_df = fetch_ohlcv_df(exchange, symbol, HTF, limit=600)

            if entry_df.empty or htf_df.empty:
                print(f"{LOG_PREFIX} {symbol} No data; wait 30s")
                time.sleep(30)
                continue

            latest_ts = entry_df.index[-1]

            if state["last_processed_ts"] is None:
                state["last_processed_ts"] = entry_df.index[-2] if len(entry_df) > 1 else entry_df.index[-1]
                save_state(state_file, state)

            if latest_ts > state["last_processed_ts"]:
                state, trade = process_bar(symbol, entry_df, htf_df, state, exchange=exchange, market_info=market_info)
                if trade is not None:
                    append_trade(trades_csv, trade)
                save_state(state_file, state)

            next_close = latest_ts + timedelta(minutes=tf_minutes)
            sleep_sec = (next_close - now_utc_naive()).total_seconds()
            sleep_sec = max(5, sleep_sec + 3)
            time.sleep(min(sleep_sec, SLEEP_CAP))

        except ccxt.RateLimitExceeded:
            print(f"{LOG_PREFIX} {symbol} Rate limit; sleep 10s")
            time.sleep(10)
        except Exception as e:
            err = f"{LOG_PREFIX} {symbol} ERROR: {e}"
            print(err)
            send_telegram(err)
            time.sleep(10)

# ----------------------------
# Main
# ----------------------------
def main():
    threads = []
    for sym in SYMBOLS:
        t = threading.Thread(target=worker, args=(sym,), daemon=True)
        t.start()
        threads.append(t)
    while True:
        time.sleep(3600)

if __name__ == "__main__":
    main()
