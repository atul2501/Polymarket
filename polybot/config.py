"""All knobs in one place. Any numeric value can be overridden by an env var of the same name."""
import os
from pathlib import Path


def _env(name, default):
    return type(default)(os.getenv(name, default))


DRY_RUN = os.getenv("DRY_RUN", "1") != "0"  # paper trading; live path not implemented in v1

# Polymarket asset slug -> Binance symbol used as fast spot proxy (markets resolve on Chainlink)
ASSETS = {k: v for k, v in {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT", "xrp": "XRPUSDT"}.items()
          if k in os.getenv("ASSETS", "btc").split(",")}
WINDOWS = {k: v for k, v in {"5m": 300, "15m": 900}.items() if k in os.getenv("WINDOWS", "5m").split(",")}
STRATEGIES = os.getenv("STRATEGIES", "arb,fair,snipe").split(",")  # maker off: lost ~$0.20/round to adverse selection

# Sizing: wallet = BASE_BALANCE * TRADE_SCALE, each trade spends at most MAX_PER_TRADE of it
BASE_BALANCE = _env("BASE_BALANCE", 1000.0)
TRADE_SCALE = _env("TRADE_SCALE", 0.1)
MAX_PER_TRADE = _env("MAX_PER_TRADE", 0.2)
MIN_SHARES = 5  # Polymarket orderMinSize

# Strategy thresholds
MIN_EDGE = _env("MIN_EDGE", 0.01)            # arb: 1 - (ask_up + ask_down + fees) must be >= this
FAIR_MIN_EDGE = _env("FAIR_MIN_EDGE", 0.05)  # fair: model prob - ask - fee must be >= this
VOL_MULT = _env("VOL_MULT", 0.8)            # fitted by backtest.py (logloss); sigma multiplier (fat tails, Binance vs Chainlink noise)
MODEL_WEIGHT = _env("MODEL_WEIGHT", 0.5)    # fair p = w*model + (1-w)*market mid
FAIR_MIN_ASK = _env("FAIR_MIN_ASK", 0.30)   # fair: only buy asks inside [min, max]; long shots lost 3/3
FAIR_MAX_ASK = _env("FAIR_MAX_ASK", 1.0)
FAIR_MIN_T = _env("FAIR_MIN_T", 0)          # fair: only trade when min < t_left <= max
FAIR_MAX_T = _env("FAIR_MAX_T", 300)
MAKER_CENTER = os.getenv("MAKER_CENTER", "model")  # "model" (blended p) or "mid" (book mid)
MAKER_HEDGE_MAX = _env("MAKER_HEDGE_MAX", 1.06)  # hedge a one-sided fill while pair + fee <= this ($ per share)

# v3: exits, re-entry, sizing, risk
EXIT_MARGIN = _env("EXIT_MARGIN", 0.03)     # sell when bid - fee >= model value of the held side + this (replay: +$15.57 vs +$10.85 hold)
EXIT_LAST_SEC = _env("EXIT_LAST_SEC", 10)   # exits allowed until this many seconds before close (book empties ~20s)
MAX_ENTRIES = {"arb": 1, "fair": int(os.getenv("FAIR_ENTRIES", 1)), "snipe": 1, "maker": 1}  # per round; FAIR_ENTRIES=2
# re-enables re-entry/flip after an exit (replay: -$1.53 train, +$0.11 test -> off)
KELLY = _env("KELLY", 0.25)                 # stake = KELLY * edge / (1 - price) * wallet, capped by MAX_PER_TRADE
KELLY_SIZING = os.getenv("KELLY_SIZING", "0") != "0"  # replay: worse on test half (+$3.68 vs +$9.07 flat) -> off
TRADE_ON_BINANCE = os.getenv("TRADE_ON_BINANCE", "0") != "0"  # Binance fallback beat was ~$100 off: record, don't trade
MAX_HOURLY_LOSS = _env("MAX_HOURLY_LOSS", 10.0)  # pause new entries while the last hour lost more than this
VOL_PCTL = _env("VOL_PCTL", 0.8)            # skip fair entries when sigma is above this percentile of the last hour
VOL_FILTER = os.getenv("VOL_FILTER", "0") != "0"  # replay: worse on both halves -> off
SNIPE_FROM_SEC = _env("SNIPE_FROM_SEC", 150)
SNIPE_MIN_FAIR = _env("SNIPE_MIN_FAIR", 0.97)
SNIPE_MAX_ASK = _env("SNIPE_MAX_ASK", 0.95)
MAKER_EDGE = _env("MAKER_EDGE", 0.04)        # maker: bid_up + bid_down = 1 - MAKER_EDGE
NO_TRADE_LAST_SEC = _env("NO_TRADE_LAST_SEC", 30)  # book is decided ~20-40s before close (60s TWAP)

# Used only if a market has no feeSchedule. fee/share = rate * p * (p*(1-p))^exponent
DEFAULT_FEE = {"rate": 0.07, "exponent": 1}

# Kill switches
MAX_DAILY_LOSS = _env("MAX_DAILY_LOSS", 50.0)
MAX_OPEN_EXPOSURE = _env("MAX_OPEN_EXPOSURE", 60.0)
MAX_API_ERRORS = _env("MAX_API_ERRORS", 10)

POLL_SEC = _env("POLL_SEC", 0.25)
CHAINLINK = os.getenv("CHAINLINK", "1") != "0"  # Chainlink prices via Polymarket live-data socket
BOOK_WS = os.getenv("BOOK_WS", "1") != "0"      # order books via CLOB websocket (REST fallback)
LEDGER_PATH = Path(os.getenv("LEDGER_PATH", Path(__file__).parent / "ledger.db"))

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
BINANCE = "https://api.binance.com"
