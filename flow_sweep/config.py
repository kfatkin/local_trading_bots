import logging
import os
from datetime import time as dt_time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


PROJECT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_DIR / ".env")

ET = ZoneInfo("America/New_York")
UTC = timezone.utc


def env_flag(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_float(name, default):
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return float(value)


def env_int(name, default):
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return int(value)


API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
PAPER = env_flag("ALPACA_PAPER", True)
AWS_PROFILE = os.getenv("AWS_PROFILE", "trading_bot").strip()
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
UW_TABLE_NAME = os.getenv("UW_TABLE_NAME", "uw-data")
FLOW_SCORE_PARTITION = os.getenv("UW_FLOW_SCORE_PARTITION", "_flow_scores_trading_bot")
ALPACA_DATA_FEED = os.getenv("ALPACA_DATA_FEED", "iex" if PAPER else "sip").strip() or None
DASHBOARD_ENABLED = env_flag("BOT_DASHBOARD_ENABLED", True)
DASHBOARD_HOST = os.getenv("BOT_DASHBOARD_HOST", "127.0.0.1").strip() or "127.0.0.1"
DASHBOARD_PORT = env_int("BOT_DASHBOARD_PORT", 8765)

RUNTIME_DIR = Path(os.getenv("BOT_RUNTIME_DIR", str(PROJECT_DIR / "runtime")))
STATE_FILE_PATH = RUNTIME_DIR / "state.json"
LOG_FILE_PATH = RUNTIME_DIR / "flow-sweep-bot.log"

CLIENT_ORDER_PREFIX = os.getenv("CLIENT_ORDER_PREFIX", "fsw")
SYMBOLS = ["AMD", "AAPL", "AMZN", "GOOGL", "META", "MU", "INTC", "NVDA", "TSLA", "STX", "SNDK"]

MIN_FLOW_SCORE = env_int("FLOW_SWEEP_MIN_SCORE", 70)
CONSENSUS_THRESHOLD = env_float("FLOW_SWEEP_CONSENSUS_THRESHOLD", 0.60)
TRADE_ALLOCATION_PCT = env_float("FLOW_SWEEP_TRADE_ALLOCATION_PCT", 0.05)
TARGET_DELTA = env_float("FLOW_SWEEP_TARGET_DELTA", 0.30)
TARGET_R_MULTIPLE = env_float("FLOW_SWEEP_TARGET_R_MULTIPLE", 2.0)
BREAKEVEN_TRIGGER_R_MULTIPLE = env_float("FLOW_SWEEP_BREAKEVEN_TRIGGER_R_MULTIPLE", 1.5)
ENTRY_RECLAIM_CLOSE_MIN_RANGE_PCT = env_float("FLOW_SWEEP_ENTRY_RECLAIM_CLOSE_MIN_RANGE_PCT", 0.50)
ENTRY_LEVEL_CLEARANCE_MIN_RANGE_PCT = env_float("FLOW_SWEEP_ENTRY_LEVEL_CLEARANCE_MIN_RANGE_PCT", 0.10)
ENTRY_MAX_TARGET_R_MULTIPLE = env_float("FLOW_SWEEP_ENTRY_MAX_TARGET_R_MULTIPLE", 8.0)
OPTION_PREVIEW_REFRESH_SECONDS = env_int("FLOW_SWEEP_OPTION_PREVIEW_REFRESH_SECONDS", 300)
OPTION_EXPIRATION_LOOKAHEAD_DAYS = env_int("FLOW_SWEEP_OPTION_EXPIRATION_LOOKAHEAD_DAYS", 21)
OPTION_MAX_SPREAD_PCT = env_float("FLOW_SWEEP_OPTION_MAX_SPREAD_PCT", 0.30)
OPTION_MIN_VOLUME = env_int("FLOW_SWEEP_OPTION_MIN_VOLUME", 1)
OPTION_MIN_OPEN_INTEREST = env_int("FLOW_SWEEP_OPTION_MIN_OPEN_INTEREST", 0)
OPTION_MAX_DELTA_DISTANCE = env_float("FLOW_SWEEP_OPTION_MAX_DELTA_DISTANCE", 0.15)
OPTION_CANDIDATES_BELOW_TARGET = env_int("FLOW_SWEEP_OPTION_CANDIDATES_BELOW_TARGET", 3)
OPTION_CANDIDATES_ABOVE_TARGET = env_int("FLOW_SWEEP_OPTION_CANDIDATES_ABOVE_TARGET", 2)
OPTION_MAX_ACCOUNT_BALANCE_PCT = env_float("FLOW_SWEEP_OPTION_MAX_ACCOUNT_BALANCE_PCT", 0.20)
OPTION_MAX_QUOTE_AGE_SECONDS = env_int("FLOW_SWEEP_OPTION_MAX_QUOTE_AGE_SECONDS", 300)
TRADE_EVENT_LOG_LIMIT = env_int("FLOW_SWEEP_TRADE_EVENT_LOG_LIMIT", 300)

PREMARKET_START = dt_time(4, 0)
REGULAR_OPEN = dt_time(9, 30)
ENTRY_WINDOW_START = dt_time(10, 0)
ENTRY_WINDOW_END = dt_time(15, 0)
EOD_EXIT_TIME = dt_time(15, 55)

LOGGER = logging.getLogger("flow_sweep_bot")


def ensure_runtime_dir():
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def configure_logging():
    ensure_runtime_dir()
    if LOGGER.handlers:
        return

    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    LOGGER.addHandler(stream_handler)

    file_handler = logging.FileHandler(LOG_FILE_PATH)
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def validate_configuration():
    if not API_KEY or not SECRET_KEY:
        raise RuntimeError("Missing Alpaca credentials. Set ALPACA_API_KEY and ALPACA_SECRET_KEY in .env.")
