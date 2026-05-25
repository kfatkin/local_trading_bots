import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import pandas_market_calendars as mcal
import yfinance as yf
from boto3.dynamodb.conditions import Attr, Key
from botocore.config import Config
from dotenv import load_dotenv

from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.live.stock import StockDataStream
from alpaca.data.requests import OptionChainRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest
from alpaca.trading.stream import TradingStream


PROJECT_DIR = Path(__file__).resolve().parent
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

PREMARKET_START = dt_time(4, 0)
REGULAR_OPEN = dt_time(9, 30)
ENTRY_WINDOW_START = dt_time(9, 45)
ENTRY_WINDOW_END = dt_time(10, 30)
EOD_EXIT_TIME = dt_time(15, 55)

LOGGER = logging.getLogger("flow_sweep_bot")
STATE_LOCK = threading.RLock()
CONTEXT_LOCK = threading.RLock()

trade_client = TradingClient(API_KEY, SECRET_KEY, paper=PAPER)
stock_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
option_client = OptionHistoricalDataClient(API_KEY, SECRET_KEY)
stock_stream = StockDataStream(API_KEY, SECRET_KEY)
trading_stream = TradingStream(API_KEY, SECRET_KEY, paper=PAPER)
NYSE = mcal.get_calendar("NYSE")

active_positions = {}
pending_entry_orders = {}
pending_exit_orders = {}
daily_trade_state = {"session": None, "traded_symbols": []}

daily_context = {
    "session": None,
    "prior_session": None,
    "prepared": False,
    "prepared_at": None,
    "setups": {},
    "decisions": {},
    "last_attempt_monotonic": 0.0,
}
five_minute_builders = {}
last_completed_bars = {}


@dataclass(frozen=True)
class KeyLevel:
    name: str
    price: float


@dataclass(frozen=True)
class FlowBias:
    symbol: str
    direction: str
    consensus: float
    bullish_premium: float
    bearish_premium: float
    total_premium: float
    top_score: int
    row_count: int


@dataclass(frozen=True)
class TradeSetup:
    symbol: str
    bias: FlowBias
    support_levels: tuple[KeyLevel, ...]
    resistance_levels: tuple[KeyLevel, ...]


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


def normalize_text(value):
    if hasattr(value, "value"):
        value = value.value
    if value is None:
        return ""
    return str(value)


def get_value(payload, field, default=None):
    if isinstance(payload, dict):
        return payload.get(field, default)
    return getattr(payload, field, default)


def to_float(value, default=0.0):
    if value in (None, ""):
        return default
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int_qty(value):
    if value in (None, ""):
        return 0
    return int(float(value))


def decimal_to_native(value):
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, dict):
        return {key: decimal_to_native(item) for key, item in value.items()}
    if isinstance(value, list):
        return [decimal_to_native(item) for item in value]
    return value


def create_client_order_id(symbol, action):
    suffix = datetime.now(UTC).strftime("%m%d%H%M%S%f")[-12:]
    return f"{CLIENT_ORDER_PREFIX}-{symbol.lower()}-{action}-{suffix}"


def is_option_asset(payload):
    return normalize_text(get_value(payload, "asset_class", "")).lower() == "us_option"


def state_snapshot_locked():
    return {
        "active_positions": active_positions,
        "pending_entry_orders": pending_entry_orders,
        "pending_exit_orders": pending_exit_orders,
        "daily_trade_state": daily_trade_state,
    }


def persist_state_locked():
    ensure_runtime_dir()
    temp_path = STATE_FILE_PATH.with_suffix(".json.tmp")
    with temp_path.open("w", encoding="utf-8") as state_file:
        json.dump(state_snapshot_locked(), state_file, indent=2, sort_keys=True)
    os.replace(temp_path, STATE_FILE_PATH)


def persist_state():
    with STATE_LOCK:
        persist_state_locked()


def load_state_from_disk():
    ensure_runtime_dir()
    if not STATE_FILE_PATH.exists():
        LOGGER.info("No existing state file found at startup.")
        return

    with STATE_FILE_PATH.open("r", encoding="utf-8") as state_file:
        snapshot = json.load(state_file)

    with STATE_LOCK:
        active_positions.clear()
        active_positions.update(snapshot.get("active_positions", {}))
        pending_entry_orders.clear()
        pending_entry_orders.update(snapshot.get("pending_entry_orders", {}))
        pending_exit_orders.clear()
        pending_exit_orders.update(snapshot.get("pending_exit_orders", {}))
        daily_trade_state.clear()
        daily_trade_state.update(snapshot.get("daily_trade_state", {"session": None, "traded_symbols": []}))
        daily_trade_state.setdefault("traded_symbols", [])

    LOGGER.info(
        "Loaded local state: active_positions=%s pending_entry_orders=%s pending_exit_orders=%s traded_symbols=%s",
        len(active_positions),
        len(pending_entry_orders),
        len(pending_exit_orders),
        len(daily_trade_state.get("traded_symbols", [])),
    )


def reset_daily_trade_state_if_needed(session_date: date):
    session_key = session_date.isoformat()
    with STATE_LOCK:
        if daily_trade_state.get("session") == session_key:
            return
        daily_trade_state["session"] = session_key
        daily_trade_state["traded_symbols"] = []
        persist_state_locked()


def mark_symbol_traded(symbol):
    with STATE_LOCK:
        traded = daily_trade_state.setdefault("traded_symbols", [])
        if symbol not in traded:
            traded.append(symbol)
            persist_state_locked()


def was_symbol_traded_today(symbol):
    with STATE_LOCK:
        return symbol in set(daily_trade_state.get("traded_symbols", []))


def reserved_exit_qty(symbol):
    reserved_qty = 0
    for order_state in pending_exit_orders.values():
        if order_state["symbol"] != symbol:
            continue
        reserved_qty += max(order_state["qty"] - order_state["filled_qty"], 0)
    return reserved_qty


def log_startup_context():
    account = trade_client.get_account()
    clock = trade_client.get_clock()
    LOGGER.info("Authenticated with Alpaca. paper=%s account_status=%s buying_power=%s", PAPER, account.status, account.buying_power)
    LOGGER.info(
        "Starting flow sweep bot for %d symbols. market_open=%s next_open=%s next_close=%s",
        len(SYMBOLS),
        clock.is_open,
        clock.next_open,
        clock.next_close,
    )
    LOGGER.info(
        "UW source: table=%s region=%s profile=%s partition=%s min_score=>%s consensus>=%.0f%%",
        UW_TABLE_NAME,
        AWS_REGION,
        AWS_PROFILE or "default",
        FLOW_SCORE_PARTITION,
        MIN_FLOW_SCORE,
        CONSENSUS_THRESHOLD * 100,
    )
    LOGGER.info(
        "Orders: allocation=%.1f%% buying_power target_delta=%.2f premium_cap=none data_feed=%s",
        TRADE_ALLOCATION_PCT * 100,
        TARGET_DELTA,
        ALPACA_DATA_FEED or "default",
    )


def boto3_table():
    config = Config(retries={"max_attempts": 3, "mode": "adaptive"}, connect_timeout=5, read_timeout=10)
    if AWS_PROFILE:
        session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    else:
        session = boto3.Session(region_name=AWS_REGION)
    dynamodb = session.resource("dynamodb", region_name=AWS_REGION, config=config)
    return dynamodb.Table(UW_TABLE_NAME)


def iso_utc(timestamp):
    return timestamp.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_schedule(start_date, end_date):
    return NYSE.schedule(start_date=start_date, end_date=end_date)


def resolve_trading_sessions(now_et=None):
    now_et = now_et or datetime.now(ET)
    start = now_et.date() - timedelta(days=21)
    end = now_et.date() + timedelta(days=10)
    schedule = get_schedule(start, end)
    if schedule.empty:
        raise RuntimeError("Unable to load NYSE schedule")

    now_utc = now_et.astimezone(UTC)
    session_dates = [idx.date() for idx in schedule.index]
    trading_idx = None

    for idx, session_date in enumerate(session_dates):
        market_close = schedule.iloc[idx]["market_close"].to_pydatetime().astimezone(UTC)
        if session_date > now_et.date() or (session_date == now_et.date() and now_utc <= market_close):
            trading_idx = idx
            break

    if trading_idx is None or trading_idx == 0:
        raise RuntimeError("Unable to resolve current and prior NYSE sessions")

    trading_day = session_dates[trading_idx]
    prior_day = session_dates[trading_idx - 1]
    return schedule, trading_idx, trading_day, prior_day


def previous_calendar_week_sessions(schedule, trading_day):
    week_start = trading_day - timedelta(days=trading_day.weekday() + 7)
    week_end = week_start + timedelta(days=6)
    sessions = [idx.date() for idx in schedule.index if week_start <= idx.date() <= week_end]
    if sessions:
        return sessions
    prior_sessions = [idx.date() for idx in schedule.index if idx.date() < trading_day]
    return prior_sessions[-5:]


def clean_bars_df(df, symbol=None):
    if df is None or df.empty:
        return pd.DataFrame()

    bars = df.copy()
    if isinstance(bars.index, pd.MultiIndex):
        if symbol and symbol in bars.index.get_level_values(0):
            bars = bars.xs(symbol, level=0)
        elif "symbol" in bars.index.names:
            bars = bars.xs(symbol, level="symbol")

    bars.columns = [str(column).lower() for column in bars.columns]
    needed = ["open", "high", "low", "close", "volume"]
    if any(column not in bars.columns for column in needed):
        return pd.DataFrame()

    bars = bars[needed]
    bars.index = pd.to_datetime(bars.index)
    if bars.index.tz is None:
        bars.index = bars.index.tz_localize(UTC)
    bars.index = bars.index.tz_convert(ET)
    return bars.sort_index()


def alpaca_bars(symbol, timeframe, start_et, end_et):
    kwargs = {
        "symbol_or_symbols": symbol,
        "timeframe": timeframe,
        "start": start_et.astimezone(UTC),
        "end": end_et.astimezone(UTC),
    }
    if ALPACA_DATA_FEED:
        kwargs["feed"] = ALPACA_DATA_FEED
    request = StockBarsRequest(**kwargs)
    response = stock_client.get_stock_bars(request)
    return clean_bars_df(response.df, symbol=symbol)


def yahoo_bars(symbol, interval, start_et, end_et, prepost=False):
    try:
        df = yf.Ticker(symbol).history(
            interval=interval,
            start=start_et.astimezone(ET).replace(tzinfo=None),
            end=end_et.astimezone(ET).replace(tzinfo=None),
            prepost=prepost,
            auto_adjust=False,
        )
    except Exception as exc:
        LOGGER.warning("Yahoo bar fetch failed for %s: %s", symbol, exc)
        return pd.DataFrame()
    return clean_bars_df(df, symbol=symbol)


def fetch_bars(symbol, timeframe, yahoo_interval, start_et, end_et, prepost=False):
    try:
        bars = alpaca_bars(symbol, timeframe, start_et, end_et)
    except Exception as exc:
        LOGGER.warning("Alpaca bar fetch failed for %s: %s", symbol, exc)
        bars = pd.DataFrame()

    if not bars.empty:
        return bars

    bars = yahoo_bars(symbol, yahoo_interval, start_et, end_et, prepost=prepost)
    if not bars.empty:
        LOGGER.info("Using Yahoo fallback bars for %s", symbol)
    return bars


def daily_bars(symbol, start_day, end_day):
    start_et = datetime.combine(start_day, dt_time(0, 0), ET)
    end_et = datetime.combine(end_day + timedelta(days=1), dt_time(0, 0), ET)
    return fetch_bars(symbol, TimeFrame.Day, "1d", start_et, end_et, prepost=False)


def intraday_bars(symbol, start_et, end_et, prepost=True):
    return fetch_bars(symbol, TimeFrame(1, TimeFrameUnit.Minute), "1m", start_et, end_et, prepost=prepost)


def get_day_high_low(symbol, session_day):
    bars = daily_bars(symbol, session_day, session_day)
    if bars.empty:
        start_et = datetime.combine(session_day, REGULAR_OPEN, ET)
        end_et = datetime.combine(session_day, dt_time(16, 0), ET)
        bars = intraday_bars(symbol, start_et, end_et, prepost=False)
    if bars.empty:
        return None
    session_bars = bars[bars.index.date == session_day]
    if session_bars.empty:
        return None
    return float(session_bars["high"].max()), float(session_bars["low"].min())


def get_week_high_low(symbol, session_days):
    if not session_days:
        return None
    bars = daily_bars(symbol, min(session_days), max(session_days))
    if bars.empty:
        return None
    session_set = set(session_days)
    mask = [idx.date() in session_set for idx in bars.index]
    session_bars = bars[mask]
    if session_bars.empty:
        return None
    return float(session_bars["high"].max()), float(session_bars["low"].min())


def get_premarket_high_low(symbol, trading_day):
    start_et = datetime.combine(trading_day, PREMARKET_START, ET)
    end_et = datetime.combine(trading_day, REGULAR_OPEN, ET)
    bars = intraday_bars(symbol, start_et, end_et, prepost=True)
    if bars.empty:
        return None
    premarket = bars[(bars.index >= start_et) & (bars.index < end_et)]
    if premarket.empty:
        return None
    return float(premarket["high"].max()), float(premarket["low"].min())


def query_flow_scores(symbol, prior_open_utc, prior_close_utc):
    table = boto3_table()
    kwargs = {
        "IndexName": "GSI3",
        "KeyConditionExpression": Key("ticker").eq(symbol) & Key("scored_at").between(iso_utc(prior_open_utc), iso_utc(prior_close_utc)),
        "FilterExpression": Attr("PK").eq(FLOW_SCORE_PARTITION) & Attr("composite_score").gt(MIN_FLOW_SCORE),
        "ScanIndexForward": False,
    }

    items = []
    while True:
        response = table.query(**kwargs)
        items.extend(decimal_to_native(item) for item in response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def summarize_flow_rows(symbol, rows):
    bullish_premium = 0.0
    bearish_premium = 0.0
    top_score = 0
    used_rows = 0

    for row in rows:
        direction = str(row.get("direction") or "neutral").lower()
        if direction not in {"bullish", "bearish"}:
            continue
        score = int(to_float(row.get("composite_score"), 0))
        premium = to_float(row.get("premium"), 0.0) or to_float(row.get("largest_premium"), 0.0)
        if premium <= 0:
            continue
        top_score = max(top_score, score)
        used_rows += 1
        if direction == "bullish":
            bullish_premium += premium
        else:
            bearish_premium += premium

    total_premium = bullish_premium + bearish_premium
    summary = {
        "symbol": symbol,
        "status": "skipped",
        "reason": "No high-score directional premium",
        "raw_row_count": len(rows),
        "directional_row_count": used_rows,
        "top_score": top_score,
        "bullish_premium": round(bullish_premium, 2),
        "bearish_premium": round(bearish_premium, 2),
        "total_premium": round(total_premium, 2),
        "consensus": None,
        "direction": "neutral",
        "option_type": None,
        "trigger_levels": [],
        "target_levels": [],
    }

    if total_premium <= 0:
        return None, summary

    direction = "bullish" if bullish_premium >= bearish_premium else "bearish"
    winning_premium = bullish_premium if direction == "bullish" else bearish_premium
    consensus = winning_premium / total_premium
    summary.update(
        {
            "direction": direction,
            "consensus": round(consensus, 4),
            "option_type": "CALL" if direction == "bullish" else "PUT",
        }
    )

    if consensus < CONSENSUS_THRESHOLD:
        LOGGER.info(
            "%s skipped: mixed high-score flow consensus=%.1f%% bull=$%.0f bear=$%.0f rows=%s",
            symbol,
            consensus * 100,
            bullish_premium,
            bearish_premium,
            used_rows,
        )
        summary["reason"] = "Mixed high-score flow below consensus threshold"
        return None, summary

    summary.update({"status": "flow_bias", "reason": "Awaiting chart levels"})
    bias = FlowBias(symbol, direction, consensus, bullish_premium, bearish_premium, total_premium, top_score, used_rows)
    return bias, summary


def build_flow_bias(symbol, prior_open_utc, prior_close_utc):
    try:
        rows = query_flow_scores(symbol, prior_open_utc, prior_close_utc)
    except Exception as exc:
        LOGGER.warning("Flow score read failed for %s: %s", symbol, exc)
        return None
    bias, _summary = summarize_flow_rows(symbol, rows)
    return bias


def build_trade_setup(symbol, bias, trading_day, prior_day, week_sessions):
    prior_range = get_day_high_low(symbol, prior_day)
    week_range = get_week_high_low(symbol, week_sessions)
    premarket_range = get_premarket_high_low(symbol, trading_day)

    if not prior_range or not week_range or not premarket_range:
        LOGGER.warning("%s skipped: missing levels prior=%s week=%s premarket=%s", symbol, bool(prior_range), bool(week_range), bool(premarket_range))
        return None

    prior_high, prior_low = prior_range
    week_high, week_low = week_range
    premarket_high, premarket_low = premarket_range
    support_levels = (KeyLevel("premarket_low", premarket_low), KeyLevel("prior_day_low", prior_low), KeyLevel("prior_week_low", week_low))
    resistance_levels = (KeyLevel("premarket_high", premarket_high), KeyLevel("prior_day_high", prior_high), KeyLevel("prior_week_high", week_high))

    LOGGER.info(
        "%s %s setup: consensus=%.1f%% top_score=%s bull=$%.0f bear=$%.0f levels PM %.2f/%.2f PD %.2f/%.2f PW %.2f/%.2f",
        symbol,
        bias.direction,
        bias.consensus * 100,
        bias.top_score,
        bias.bullish_premium,
        bias.bearish_premium,
        premarket_high,
        premarket_low,
        prior_high,
        prior_low,
        week_high,
        week_low,
    )
    return TradeSetup(symbol, bias, support_levels, resistance_levels)


def key_level_to_dict(level):
    return {"name": level.name, "price": round(float(level.price), 4)}


def setup_plan_summary(setup):
    if setup.bias.direction == "bullish":
        trigger_levels = setup.support_levels
        target_levels = sorted(setup.resistance_levels, key=lambda level: level.price)
        action = "Buy calls after a 5m sweep below one of these lows and close back above it"
        option_type = "CALL"
    else:
        trigger_levels = setup.resistance_levels
        target_levels = sorted(setup.support_levels, key=lambda level: level.price, reverse=True)
        action = "Buy puts after a 5m sweep above one of these highs and close back below it"
        option_type = "PUT"

    return {
        "status": "ready",
        "reason": "Watching entry window",
        "action": action,
        "option_type": option_type,
        "trigger_levels": [key_level_to_dict(level) for level in trigger_levels],
        "target_levels": [key_level_to_dict(level) for level in target_levels],
        "entry_window": f"{ENTRY_WINDOW_START.strftime('%H:%M')}-{ENTRY_WINDOW_END.strftime('%H:%M')} ET",
    }


def prepare_daily_context(now_et=None, force=False):
    now_et = now_et or datetime.now(ET)
    with CONTEXT_LOCK:
        if not force and time.monotonic() - daily_context["last_attempt_monotonic"] < 60:
            return
        daily_context["last_attempt_monotonic"] = time.monotonic()

        schedule, trading_idx, trading_day, prior_day = resolve_trading_sessions(now_et)
        if daily_context["prepared"] and daily_context["session"] == trading_day.isoformat():
            return

        reset_daily_trade_state_if_needed(trading_day)

        if now_et.date() != trading_day or now_et.time() < REGULAR_OPEN:
            LOGGER.info("Waiting for %s premarket to complete before preparing setups. prior_session=%s", trading_day, prior_day)
            return

        prior_open = schedule.iloc[trading_idx - 1]["market_open"].to_pydatetime().astimezone(UTC)
        prior_close = schedule.iloc[trading_idx - 1]["market_close"].to_pydatetime().astimezone(UTC)
        week_sessions = previous_calendar_week_sessions(schedule, trading_day)

        setups = {}
        decisions = {}
        for symbol in SYMBOLS:
            try:
                rows = query_flow_scores(symbol, prior_open, prior_close)
            except Exception as exc:
                LOGGER.warning("Flow score read failed for %s: %s", symbol, exc)
                decisions[symbol] = {
                    "symbol": symbol,
                    "status": "error",
                    "reason": f"Flow score read failed: {exc}",
                    "direction": "neutral",
                    "consensus": None,
                    "top_score": 0,
                    "bullish_premium": 0.0,
                    "bearish_premium": 0.0,
                    "total_premium": 0.0,
                    "raw_row_count": 0,
                    "directional_row_count": 0,
                    "option_type": None,
                    "trigger_levels": [],
                    "target_levels": [],
                }
                continue

            bias, decision = summarize_flow_rows(symbol, rows)
            decisions[symbol] = decision
            if not bias:
                continue
            setup = build_trade_setup(symbol, bias, trading_day, prior_day, week_sessions)
            if setup:
                setups[symbol] = setup
                decisions[symbol].update(setup_plan_summary(setup))
            else:
                decisions[symbol].update({"status": "skipped", "reason": "Missing one or more chart levels"})

        daily_context.update(
            {
                "session": trading_day.isoformat(),
                "prior_session": prior_day.isoformat(),
                "prepared": True,
                "prepared_at": datetime.now(UTC).isoformat(),
                "setups": setups,
                "decisions": decisions,
            }
        )
        LOGGER.info("Prepared %s flow sweep setups for session=%s prior_session=%s", len(setups), trading_day, prior_day)


def current_setup(symbol):
    with CONTEXT_LOCK:
        return daily_context.get("setups", {}).get(symbol)


def default_decision(symbol):
        return {
                "symbol": symbol,
                "status": "pending",
                "reason": "Waiting for daily preparation",
                "direction": "neutral",
                "consensus": None,
                "top_score": 0,
                "bullish_premium": 0.0,
                "bearish_premium": 0.0,
                "total_premium": 0.0,
                "raw_row_count": 0,
                "directional_row_count": 0,
                "option_type": None,
                "trigger_levels": [],
                "target_levels": [],
        }


def round_or_none(value, digits=4):
        if value in (None, ""):
                return None
        try:
                return round(float(value), digits)
        except (TypeError, ValueError):
                return None


def position_payload(symbol, position):
        return {
                "symbol": symbol,
                "managed": bool(position.get("managed", True)),
                "option_symbol": position.get("option_symbol"),
                "option_type": position.get("option_type"),
                "entry_status": position.get("entry_status"),
                "total_qty": to_int_qty(position.get("total_qty", 0)),
                "requested_qty": to_int_qty(position.get("requested_qty", 0)),
                "entry_underlying": round_or_none(position.get("entry_underlying")),
                "stop_underlying": round_or_none(position.get("stop_underlying")),
                "target_underlying": round_or_none(position.get("target_underlying")),
                "target_name": position.get("target_name"),
                "swept_level": position.get("swept_level"),
                "swept_level_price": round_or_none(position.get("swept_level_price")),
        }


def pending_exit_payload(order_id, order_state):
        return {
                "order_id": order_id,
                "symbol": order_state.get("symbol"),
                "qty": to_int_qty(order_state.get("qty", 0)),
                "filled_qty": to_int_qty(order_state.get("filled_qty", 0)),
                "reason": order_state.get("reason"),
        }


def completed_bar_payload(symbol, bar):
        return {
                "symbol": symbol,
                "bucket": bar["bucket"].isoformat() if bar.get("bucket") else None,
                "close_time": bar["close_time"].isoformat() if bar.get("close_time") else None,
                "open": round_or_none(bar.get("open")),
                "high": round_or_none(bar.get("high")),
                "low": round_or_none(bar.get("low")),
                "close": round_or_none(bar.get("close")),
                "volume": round_or_none(bar.get("volume"), 0),
        }


def dashboard_status_payload():
        now_et = datetime.now(ET)
        display_host = "localhost" if DASHBOARD_HOST in {"0.0.0.0", "::"} else DASHBOARD_HOST

        with STATE_LOCK:
                active = [position_payload(symbol, position.copy()) for symbol, position in active_positions.items()]
                pending_entries = [{"order_id": order_id, "symbol": symbol} for order_id, symbol in pending_entry_orders.items()]
                pending_exits = [pending_exit_payload(order_id, state.copy()) for order_id, state in pending_exit_orders.items()]
                daily_trades = dict(daily_trade_state)

        with CONTEXT_LOCK:
                decisions_by_symbol = dict(daily_context.get("decisions") or {})
                decisions = [decisions_by_symbol.get(symbol, default_decision(symbol)) for symbol in SYMBOLS]
                recent_bars = [completed_bar_payload(symbol, bar.copy()) for symbol, bar in last_completed_bars.items()]
                context = {
                        "session": daily_context.get("session"),
                        "prior_session": daily_context.get("prior_session"),
                        "prepared": bool(daily_context.get("prepared")),
                        "prepared_at": daily_context.get("prepared_at"),
                        "ready_count": sum(1 for decision in decisions if decision.get("status") == "ready"),
                        "last_attempt_seconds_ago": round(time.monotonic() - daily_context.get("last_attempt_monotonic", 0.0), 1),
                }

        return {
                "bot": {
                        "name": "Flow Sweep Bot",
                        "paper": PAPER,
                        "symbols": SYMBOLS,
                        "min_flow_score": MIN_FLOW_SCORE,
                        "consensus_threshold": CONSENSUS_THRESHOLD,
                        "trade_allocation_pct": TRADE_ALLOCATION_PCT,
                        "target_delta": TARGET_DELTA,
                        "entry_window": f"{ENTRY_WINDOW_START.strftime('%H:%M')}-{ENTRY_WINDOW_END.strftime('%H:%M')} ET",
                        "dashboard_url": f"http://{display_host}:{DASHBOARD_PORT}",
                },
                "clock": {"now_et": now_et.isoformat()},
                "daily_context": context,
                "daily_trade_state": daily_trades,
                "decisions": decisions,
                "active_positions": active,
                "pending_entry_orders": pending_entries,
                "pending_exit_orders": pending_exits,
                "recent_5m_bars": sorted(recent_bars, key=lambda item: item.get("symbol") or ""),
        }


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Flow Sweep Bot</title>
    <style>
        :root {
            color-scheme: light dark;
            --bg: #f7f8fa;
            --panel: #ffffff;
            --text: #1f2933;
            --muted: #637083;
            --border: #d9dee7;
            --bull: #0f766e;
            --bear: #b42318;
            --ready: #0f766e;
            --skip: #6b7280;
            --warn: #b54708;
        }
        @media (prefers-color-scheme: dark) {
            :root {
                --bg: #111827;
                --panel: #182230;
                --text: #e5e7eb;
                --muted: #a7b0c0;
                --border: #2f3b4d;
            }
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            background: var(--bg);
            color: var(--text);
            font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }
        header {
            padding: 20px 24px 12px;
            border-bottom: 1px solid var(--border);
            background: var(--panel);
            position: sticky;
            top: 0;
            z-index: 2;
        }
        h1 { margin: 0 0 8px; font-size: 22px; letter-spacing: 0; }
        main { padding: 20px 24px 32px; display: grid; gap: 16px; }
        .meta { display: flex; flex-wrap: wrap; gap: 10px; color: var(--muted); }
        .pill {
            border: 1px solid var(--border);
            border-radius: 999px;
            padding: 4px 10px;
            background: color-mix(in srgb, var(--panel) 85%, var(--bg));
            white-space: nowrap;
        }
        .grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
        .panel {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 14px;
        }
        .panel h2 { margin: 0 0 10px; font-size: 16px; letter-spacing: 0; }
        .metric { color: var(--muted); font-size: 12px; text-transform: uppercase; }
        .metric-value { font-size: 20px; font-weight: 700; margin-top: 2px; }
        table { width: 100%; border-collapse: collapse; background: var(--panel); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
        th, td { padding: 10px 8px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }
        th { color: var(--muted); font-size: 12px; text-transform: uppercase; background: color-mix(in srgb, var(--panel) 88%, var(--bg)); }
        tr:last-child td { border-bottom: 0; }
        .symbol { font-weight: 700; }
        .bullish { color: var(--bull); font-weight: 700; }
        .bearish { color: var(--bear); font-weight: 700; }
        .neutral { color: var(--muted); font-weight: 700; }
        .status-ready { color: var(--ready); font-weight: 700; }
        .status-skipped, .status-pending { color: var(--skip); font-weight: 700; }
        .status-error { color: var(--warn); font-weight: 700; }
        .levels { display: flex; flex-wrap: wrap; gap: 6px; }
        .level { border: 1px solid var(--border); border-radius: 6px; padding: 3px 6px; white-space: nowrap; }
        .muted { color: var(--muted); }
        .section-title { margin: 8px 0 6px; font-size: 18px; }
        .wide { overflow-x: auto; }
        @media (max-width: 980px) { .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
        @media (max-width: 640px) { header, main { padding-left: 12px; padding-right: 12px; } .grid { grid-template-columns: 1fr; } th, td { padding: 8px 6px; } }
    </style>
</head>
<body>
    <header>
        <h1>Flow Sweep Bot</h1>
        <div class="meta" id="meta"></div>
    </header>
    <main>
        <section class="grid" id="metrics"></section>
        <section>
            <h2 class="section-title">Decision Board</h2>
            <div class="wide"><table id="decisions"></table></div>
        </section>
        <section>
            <h2 class="section-title">Active Positions</h2>
            <div class="wide"><table id="positions"></table></div>
        </section>
        <section>
            <h2 class="section-title">Recent 5m Bars</h2>
            <div class="wide"><table id="bars"></table></div>
        </section>
    </main>
    <script>
        const money = new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
        const price = new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        function esc(value) {
            return String(value ?? '').replace(/[&<>'"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[c]));
        }
        function pct(value) { return value == null ? '-' : `${(Number(value) * 100).toFixed(1)}%`; }
        function usd(value) { return value == null ? '-' : money.format(Number(value)); }
        function px(value) { return value == null ? '-' : price.format(Number(value)); }
        function levels(items) {
            if (!items || !items.length) return '<span class="muted">none</span>';
            return `<div class="levels">${items.map(item => `<span class="level">${esc(item.name)} ${px(item.price)}</span>`).join('')}</div>`;
        }
        function table(elementId, headers, rows, emptyText) {
            const el = document.getElementById(elementId);
            if (!rows.length) {
                el.innerHTML = `<tbody><tr><td class="muted">${esc(emptyText)}</td></tr></tbody>`;
                return;
            }
            el.innerHTML = `<thead><tr>${headers.map(h => `<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.join('')}</tbody>`;
        }
        function metric(label, value) {
            return `<div class="panel"><div class="metric">${esc(label)}</div><div class="metric-value">${esc(value)}</div></div>`;
        }
        async function refresh() {
            const response = await fetch('/api/status', { cache: 'no-store' });
            const data = await response.json();
            const ready = data.daily_context.ready_count || 0;
            document.getElementById('meta').innerHTML = [
                `Now ${esc(new Date(data.clock.now_et).toLocaleString())}`,
                `Session ${esc(data.daily_context.session || '-')}`,
                `Prior ${esc(data.daily_context.prior_session || '-')}`,
                `Mode ${data.bot.paper ? 'paper' : 'live'}`,
                `Entry ${esc(data.bot.entry_window)}`,
                `Refresh 5s`
            ].map(item => `<span class="pill">${item}</span>`).join('');
            document.getElementById('metrics').innerHTML = [
                metric('Ready Setups', ready),
                metric('Active Positions', data.active_positions.length),
                metric('Pending Orders', data.pending_entry_orders.length + data.pending_exit_orders.length),
                metric('Allocation', `${(data.bot.trade_allocation_pct * 100).toFixed(1)}% BP`)
            ].join('');
            table('decisions', ['Symbol', 'Decision', 'Consensus', 'Calls/Puts From', 'Targets', 'Reason'], data.decisions.map(d => {
                const directionClass = d.direction === 'bullish' ? 'bullish' : d.direction === 'bearish' ? 'bearish' : 'neutral';
                return `<tr>
                    <td class="symbol">${esc(d.symbol)}</td>
                    <td><span class="${directionClass}">${esc(d.direction)}</span><br><span class="status-${esc(d.status)}">${esc(d.status)}</span><br><span class="muted">${esc(d.option_type || '-')} score ${esc(d.top_score || 0)}</span></td>
                    <td>${pct(d.consensus)}<br><span class="muted">Bull ${usd(d.bullish_premium)} / Bear ${usd(d.bearish_premium)}</span></td>
                    <td>${levels(d.trigger_levels)}</td>
                    <td>${levels(d.target_levels)}</td>
                    <td>${esc(d.reason || '')}<br><span class="muted">Rows ${esc(d.directional_row_count || 0)} of ${esc(d.raw_row_count || 0)}</span></td>
                </tr>`;
            }), 'No decisions prepared yet.');
            table('positions', ['Symbol', 'Option', 'Qty', 'Stop', 'Target', 'Status'], data.active_positions.map(p => `<tr>
                <td class="symbol">${esc(p.symbol)}</td>
                <td>${esc(p.option_symbol || '-')}<br><span class="muted">${esc(p.option_type || '-')}</span></td>
                <td>${esc(p.total_qty)} / ${esc(p.requested_qty)}</td>
                <td>${px(p.stop_underlying)}</td>
                <td>${esc(p.target_name || '-')} ${px(p.target_underlying)}</td>
                <td>${esc(p.entry_status || '-')}<br><span class="muted">Swept ${esc(p.swept_level || '-')}</span></td>
            </tr>`), 'No active positions.');
            table('bars', ['Symbol', 'Close Time', 'O/H/L/C', 'Volume'], data.recent_5m_bars.map(b => `<tr>
                <td class="symbol">${esc(b.symbol)}</td>
                <td>${b.close_time ? esc(new Date(b.close_time).toLocaleTimeString()) : '-'}</td>
                <td>${px(b.open)} / ${px(b.high)} / ${px(b.low)} / ${px(b.close)}</td>
                <td>${esc(b.volume ?? '-')}</td>
            </tr>`), 'No completed 5-minute bars yet.');
        }
        refresh().catch(console.error);
        setInterval(() => refresh().catch(console.error), 5000);
    </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self):
                path = urlparse(self.path).path
                if path in {"/", "/index.html"}:
                        self.send_response(200)
                        self.send_header("Content-Type", "text/html; charset=utf-8")
                        self.send_header("Cache-Control", "no-store")
                        self.end_headers()
                        self.wfile.write(DASHBOARD_HTML.encode("utf-8"))
                        return

                if path == "/api/status":
                        payload = dashboard_status_payload()
                        body = json.dumps(payload, default=str).encode("utf-8")
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json; charset=utf-8")
                        self.send_header("Cache-Control", "no-store")
                        self.end_headers()
                        self.wfile.write(body)
                        return

                if path == "/health":
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json; charset=utf-8")
                        self.end_headers()
                        self.wfile.write(b'{"ok": true}')
                        return

                self.send_response(404)
                self.end_headers()

        def log_message(self, _format, *args):
                LOGGER.debug("Dashboard request: " + _format, *args)


def start_dashboard_thread():
        if not DASHBOARD_ENABLED:
                LOGGER.info("Dashboard disabled by BOT_DASHBOARD_ENABLED=0")
                return None

        try:
                server = ThreadingHTTPServer((DASHBOARD_HOST, DASHBOARD_PORT), DashboardHandler)
        except OSError as exc:
                LOGGER.error("Dashboard failed to start on %s:%s: %s", DASHBOARD_HOST, DASHBOARD_PORT, exc)
                return None

        thread = threading.Thread(target=server.serve_forever, name="flow-sweep-dashboard", daemon=True)
        thread.start()
        display_host = "localhost" if DASHBOARD_HOST in {"0.0.0.0", "::"} else DASHBOARD_HOST
        LOGGER.info("Dashboard running at http://%s:%s", display_host, DASHBOARD_PORT)
        return server


def option_contract_type(data):
    return normalize_text(get_value(data, "contract_type", "")).lower().replace("contracttype.", "")


def get_option_delta(data):
    greeks = get_value(data, "greeks")
    if not greeks:
        return None
    delta = get_value(greeks, "delta")
    if delta in (None, ""):
        return None
    return float(delta)


def get_option_ask(data):
    quote = get_value(data, "latest_quote")
    if not quote:
        return 0.0
    return to_float(get_value(quote, "ask_price"), 0.0)


def get_best_option_contract(symbol, option_type):
    req = OptionChainRequest(underlying_symbol=symbol)
    chain = option_client.get_option_chain(req)
    valid_contracts = []
    desired = "call" if option_type == "CALL" else "put"

    for contract_symbol, data in chain.items():
        if option_contract_type(data) != desired:
            continue

        premium = get_option_ask(data)
        delta = get_option_delta(data)
        expiration = get_value(data, "expiration_date")
        if premium <= 0 or delta is None or not expiration:
            continue

        valid_contracts.append({"symbol": contract_symbol, "premium": premium, "delta": abs(delta), "expiration": expiration})

    if not valid_contracts:
        return None

    valid_contracts.sort(key=lambda contract: (contract["expiration"], abs(TARGET_DELTA - contract["delta"])))
    return valid_contracts[0]


def target_level_for_entry(setup, entry_price):
    if setup.bias.direction == "bullish":
        candidates = [level for level in setup.resistance_levels if level.price > entry_price]
        candidates.sort(key=lambda level: level.price)
    else:
        candidates = [level for level in setup.support_levels if level.price < entry_price]
        candidates.sort(key=lambda level: level.price, reverse=True)
    return candidates[0] if candidates else None


def swept_level_for_bar(setup, bar):
    if setup.bias.direction == "bullish":
        swept = [level for level in setup.support_levels if bar["low"] < level.price and bar["close"] > level.price]
    else:
        swept = [level for level in setup.resistance_levels if bar["high"] > level.price and bar["close"] < level.price]
    if not swept:
        return None
    return min(swept, key=lambda level: abs(bar["open"] - level.price))


def execute_entry(symbol, setup, swept_level, signal_bar):
    with STATE_LOCK:
        if symbol in active_positions or symbol in pending_entry_orders.values():
            return
    if was_symbol_traded_today(symbol):
        return

    option_type = "CALL" if setup.bias.direction == "bullish" else "PUT"
    contract = get_best_option_contract(symbol, option_type)
    if not contract:
        LOGGER.info("Skipping %s: no %s contract with usable quote/greeks", symbol, option_type)
        return

    target_level = target_level_for_entry(setup, signal_bar["close"])
    if not target_level:
        LOGGER.info("Skipping %s: no target level beyond entry %.2f", symbol, signal_bar["close"])
        return

    account = trade_client.get_account()
    trade_allocation = float(account.buying_power) * TRADE_ALLOCATION_PCT
    contract_cost = contract["premium"] * 100
    qty = math.floor(trade_allocation / contract_cost)
    if qty < 1:
        LOGGER.info("Skipping %s: allocation %.2f cannot buy 1x %s at %.2f", symbol, trade_allocation, contract["symbol"], contract["premium"])
        return

    stop_underlying = signal_bar["low"] if option_type == "CALL" else signal_bar["high"]
    LOGGER.info(
        "ENTER %s %s after %s sweep %.2f close %.2f stop %.2f target %s %.2f: %sx %s ask=%.2f delta=%.2f exp=%s",
        symbol,
        option_type,
        swept_level.name,
        swept_level.price,
        signal_bar["close"],
        stop_underlying,
        target_level.name,
        target_level.price,
        qty,
        contract["symbol"],
        contract["premium"],
        contract["delta"],
        contract["expiration"],
    )

    req = MarketOrderRequest(
        symbol=contract["symbol"],
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        client_order_id=create_client_order_id(symbol, "entry"),
    )
    try:
        order = trade_client.submit_order(order_data=req)
    except Exception as exc:
        LOGGER.error("Entry order failed for %s: %s", symbol, exc)
        return

    order_id = str(order.id)
    with STATE_LOCK:
        pending_entry_orders[order_id] = symbol
        active_positions[symbol] = {
            "managed": True,
            "symbol": symbol,
            "option_symbol": contract["symbol"],
            "option_type": option_type,
            "entry_underlying": signal_bar["close"],
            "stop_underlying": stop_underlying,
            "target_underlying": target_level.price,
            "target_name": target_level.name,
            "swept_level": swept_level.name,
            "swept_level_price": swept_level.price,
            "entry_order_id": order_id,
            "entry_status": "submitted",
            "total_qty": 0,
            "requested_qty": qty,
            "entry_submitted_at": datetime.now(UTC).isoformat(),
        }
        mark_symbol_traded(symbol)
        persist_state_locked()

    LOGGER.info("Submitted entry order %s for %sx %s", order_id, qty, contract["symbol"])


def execute_exit(symbol, exit_qty, reason):
    with STATE_LOCK:
        position = active_positions.get(symbol)
        if not position:
            return False
        available_qty = position["total_qty"] - reserved_exit_qty(symbol)
        exit_qty = min(exit_qty, available_qty)
        option_symbol = position["option_symbol"]

    if exit_qty <= 0:
        return False

    LOGGER.info("EXIT %s (%s) %sx %s", symbol, reason, exit_qty, option_symbol)
    req = MarketOrderRequest(
        symbol=option_symbol,
        qty=exit_qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY,
        client_order_id=create_client_order_id(symbol, "exit"),
    )
    try:
        order = trade_client.submit_order(order_data=req)
    except Exception as exc:
        LOGGER.error("Exit order failed for %s: %s", symbol, exc)
        return False

    order_id = str(order.id)
    with STATE_LOCK:
        pending_exit_orders[order_id] = {"symbol": symbol, "qty": exit_qty, "filled_qty": 0, "reason": reason}
        persist_state_locked()

    LOGGER.info("Submitted exit order %s for %s", order_id, symbol)
    return True


def canonical_trade_event(value):
    event = normalize_text(value).lower()
    if event == "partially_filled":
        return "partial_fill"
    return event


def log_trade_update(event, order):
    order_id = get_value(order, "id", "unknown")
    order_symbol = get_value(order, "symbol", "unknown")
    order_status = get_value(order, "status", "unknown")
    filled_qty = get_value(order, "filled_qty", get_value(order, "qty", 0))
    LOGGER.info("TRADE_UPDATE event=%s status=%s symbol=%s filled_qty=%s order_id=%s", event, order_status, order_symbol, filled_qty, order_id)


def process_entry_update(symbol, order_id, event, order):
    with STATE_LOCK:
        position = active_positions.get(symbol)
        if not position:
            pending_entry_orders.pop(order_id, None)
            persist_state_locked()
            return

        if event in {"partial_fill", "fill"}:
            filled_qty = to_int_qty(get_value(order, "filled_qty", 0))
            if filled_qty > 0:
                position["entry_status"] = event
                position["total_qty"] = filled_qty

        if event == "fill":
            position["entry_status"] = "filled"
            pending_entry_orders.pop(order_id, None)

        if event in {"canceled", "expired", "rejected"}:
            pending_entry_orders.pop(order_id, None)
            if position.get("total_qty", 0) <= 0:
                active_positions.pop(symbol, None)

        persist_state_locked()


def process_exit_update(order_id, event, order, order_state):
    with STATE_LOCK:
        symbol = order_state["symbol"]
        position = active_positions.get(symbol)
        if not position:
            pending_exit_orders.pop(order_id, None)
            persist_state_locked()
            return

        filled_qty = to_int_qty(get_value(order, "filled_qty", order_state["filled_qty"]))
        filled_delta = max(filled_qty - order_state["filled_qty"], 0)
        if filled_delta > 0:
            position["total_qty"] = max(position["total_qty"] - filled_delta, 0)
            order_state["filled_qty"] = filled_qty

        if position["total_qty"] <= 0:
            active_positions.pop(symbol, None)

        if event in {"fill", "canceled", "expired", "rejected"}:
            pending_exit_orders.pop(order_id, None)

        persist_state_locked()


def fetch_open_option_orders():
    try:
        open_orders = trade_client.get_orders(filter=GetOrdersRequest(limit=500, nested=False))
    except Exception as exc:
        LOGGER.error("Failed to fetch open orders during reconciliation: %s", exc)
        return {}
    return {str(order.id): order for order in open_orders if is_option_asset(order)}


def fetch_open_option_positions():
    try:
        positions = trade_client.get_all_positions()
    except Exception as exc:
        LOGGER.error("Failed to fetch positions during reconciliation: %s", exc)
        return {}
    return {normalize_text(get_value(position, "symbol", "")): position for position in positions if is_option_asset(position)}


def safe_get_order_by_id(order_id):
    try:
        return trade_client.get_order_by_id(order_id)
    except Exception as exc:
        LOGGER.warning("Unable to fetch order %s during reconciliation: %s", order_id, exc)
        return None


def add_unmanaged_position(option_symbol, quantity):
    placeholder_key = f"UNMANAGED:{option_symbol}"
    with STATE_LOCK:
        active_positions[placeholder_key] = {
            "managed": False,
            "option_symbol": option_symbol,
            "option_type": "UNKNOWN",
            "stop_underlying": None,
            "target_underlying": None,
            "total_qty": quantity,
            "requested_qty": quantity,
            "entry_order_id": None,
            "entry_status": "filled",
        }
        persist_state_locked()


def reconcile_state():
    remote_orders = fetch_open_option_orders()
    remote_positions = fetch_open_option_positions()

    with STATE_LOCK:
        tracked_entry_orders = list(pending_entry_orders.items())
        tracked_exit_orders = list(pending_exit_orders.items())

    for order_id, symbol in tracked_entry_orders:
        order = remote_orders.get(order_id) or safe_get_order_by_id(order_id)
        if order:
            process_entry_update(symbol, order_id, canonical_trade_event(get_value(order, "status", "")), order)

    for order_id, order_state in tracked_exit_orders:
        order = remote_orders.get(order_id) or safe_get_order_by_id(order_id)
        if order:
            process_exit_update(order_id, canonical_trade_event(get_value(order, "status", "")), order, order_state)

    with STATE_LOCK:
        managed_symbols = [symbol for symbol, position in active_positions.items() if position.get("managed", True)]

    for symbol in managed_symbols:
        with STATE_LOCK:
            position = active_positions.get(symbol)
            has_pending_entry = symbol in pending_entry_orders.values()
        if not position:
            continue

        remote_position = remote_positions.pop(position["option_symbol"], None)
        if remote_position:
            with STATE_LOCK:
                latest_position = active_positions.get(symbol)
                if latest_position:
                    latest_position["total_qty"] = to_int_qty(get_value(remote_position, "qty", 0))
                    if latest_position["total_qty"] > 0 and not has_pending_entry:
                        latest_position["entry_status"] = "filled"
                    persist_state_locked()
            continue

        if not has_pending_entry:
            LOGGER.warning("Dropping stale local position for %s because Alpaca has no open option position.", symbol)
            with STATE_LOCK:
                active_positions.pop(symbol, None)
                persist_state_locked()

    for option_symbol, remote_position in remote_positions.items():
        LOGGER.warning("Found open option position %s at startup without matching local strategy state. Position will be logged but not actively managed.", option_symbol)
        add_unmanaged_position(option_symbol, to_int_qty(get_value(remote_position, "qty", 0)))

    with STATE_LOCK:
        persist_state_locked()

    LOGGER.info("Reconciled startup state: active_positions=%s pending_entry_orders=%s pending_exit_orders=%s", len(active_positions), len(pending_entry_orders), len(pending_exit_orders))


async def handle_trade_update(data):
    event = get_value(data, "event", "unknown")
    order = get_value(data, "order", {})
    order_id = str(get_value(order, "id", ""))

    log_trade_update(event, order)
    if not order_id:
        return

    entry_symbol = pending_entry_orders.get(order_id)
    if entry_symbol:
        process_entry_update(entry_symbol, order_id, event, order)
        return

    exit_state = pending_exit_orders.get(order_id)
    if exit_state:
        process_exit_update(order_id, event, order, exit_state)


def bar_timestamp_et(bar):
    timestamp = get_value(bar, "timestamp")
    if timestamp is None:
        return datetime.now(ET)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(ET)


def minute_bar_dict(bar, timestamp_et):
    return {
        "timestamp": timestamp_et,
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": float(get_value(bar, "volume", 0) or 0),
    }


def floor_5m(timestamp_et):
    minute = (timestamp_et.minute // 5) * 5
    return timestamp_et.replace(minute=minute, second=0, microsecond=0)


def update_five_minute_bar(symbol, minute_bar):
    bucket = floor_5m(minute_bar["timestamp"])
    builder = five_minute_builders.get(symbol)
    if builder is None:
        five_minute_builders[symbol] = {"bucket": bucket, "open": minute_bar["open"], "high": minute_bar["high"], "low": minute_bar["low"], "close": minute_bar["close"], "volume": minute_bar["volume"]}
        return None

    if builder["bucket"] == bucket:
        builder["high"] = max(builder["high"], minute_bar["high"])
        builder["low"] = min(builder["low"], minute_bar["low"])
        builder["close"] = minute_bar["close"]
        builder["volume"] += minute_bar["volume"]
        return None

    completed = dict(builder)
    completed["close_time"] = builder["bucket"] + timedelta(minutes=5)
    with CONTEXT_LOCK:
        last_completed_bars[symbol] = completed.copy()
    five_minute_builders[symbol] = {"bucket": bucket, "open": minute_bar["open"], "high": minute_bar["high"], "low": minute_bar["low"], "close": minute_bar["close"], "volume": minute_bar["volume"]}
    return completed


def in_entry_window(close_time_et):
    close_clock = close_time_et.time()
    return ENTRY_WINDOW_START <= close_clock <= ENTRY_WINDOW_END


def process_completed_five_minute_bar(symbol, bar):
    if not in_entry_window(bar["close_time"]):
        return

    setup = current_setup(symbol)
    if not setup or was_symbol_traded_today(symbol):
        return

    swept_level = swept_level_for_bar(setup, bar)
    if swept_level:
        execute_entry(symbol, setup, swept_level, bar)


def manage_open_position_with_bar(symbol, minute_bar):
    with STATE_LOCK:
        position = active_positions.get(symbol)
        if not position or not position.get("managed", True):
            return
        is_live_position = position["entry_status"] in {"partial_fill", "filled"} and position["total_qty"] > 0
        if not is_live_position:
            return
        option_type = position["option_type"]
        total_qty = position["total_qty"]
        stop_underlying = float(position["stop_underlying"])
        target_underlying = float(position["target_underlying"])

    if option_type == "CALL":
        stop_hit = minute_bar["low"] <= stop_underlying
        target_hit = minute_bar["high"] >= target_underlying
    else:
        stop_hit = minute_bar["high"] >= stop_underlying
        target_hit = minute_bar["low"] <= target_underlying

    if stop_hit:
        execute_exit(symbol, total_qty, "STOP_SWEEP_EXTREME")
        return
    if target_hit:
        execute_exit(symbol, total_qty, f"TARGET_{position.get('target_name', 'LEVEL')}")
        return
    if minute_bar["timestamp"].time() >= EOD_EXIT_TIME:
        execute_exit(symbol, total_qty, "EOD_EXIT")


async def handle_bar(bar):
    symbol = bar.symbol
    timestamp_et = bar_timestamp_et(bar)
    prepare_daily_context(timestamp_et)

    minute_bar = minute_bar_dict(bar, timestamp_et)
    manage_open_position_with_bar(symbol, minute_bar)

    completed = update_five_minute_bar(symbol, minute_bar)
    if completed:
        process_completed_five_minute_bar(symbol, completed)


def start_trading_stream():
    trading_stream.subscribe_trade_updates(handle_trade_update)
    trading_stream.run()


def main():
    configure_logging()
    validate_configuration()
    load_state_from_disk()
    log_startup_context()
    reconcile_state()
    start_dashboard_thread()
    prepare_daily_context(force=True)
    trading_thread = threading.Thread(target=start_trading_stream, name="alpaca-trading-stream", daemon=True)
    trading_thread.start()
    stock_stream.subscribe_bars(handle_bar, *SYMBOLS)
    stock_stream.run()


if __name__ == "__main__":
    main()