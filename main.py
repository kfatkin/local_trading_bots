import json
import logging
import math
import os
import threading
from datetime import datetime

from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.live.stock import StockDataStream
from alpaca.data.requests import OptionChainRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest
from alpaca.trading.stream import TradingStream


def env_flag(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


# --- CONFIGURATION ---
API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
PAPER = env_flag("ALPACA_PAPER", True)
RUNTIME_DIR = os.getenv("BOT_RUNTIME_DIR", "/app/runtime")
STATE_FILE_PATH = os.path.join(RUNTIME_DIR, "state.json")
LOG_FILE_PATH = os.path.join(RUNTIME_DIR, "powerbar-bot.log")
CLIENT_ORDER_PREFIX = "pb"

LOGGER = logging.getLogger("powerbar_bot")
STATE_LOCK = threading.RLock()

trade_client = TradingClient(API_KEY, SECRET_KEY, paper=PAPER)
stock_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
option_client = OptionHistoricalDataClient(API_KEY, SECRET_KEY)
stock_stream = StockDataStream(API_KEY, SECRET_KEY)
trading_stream = TradingStream(API_KEY, SECRET_KEY, paper=PAPER)

SYMBOLS = ["TSLA", "NVDA", "GOOGL", "AMD", "META", "NFLX", "MSFT", "AAPL", "AMZN", "INTC", "PLTR"]

# State management for active trades
active_positions = {}
pending_entry_orders = {}
pending_exit_orders = {}


def ensure_runtime_dir():
    os.makedirs(RUNTIME_DIR, exist_ok=True)


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
        raise RuntimeError(
            "Missing Alpaca credentials. Set ALPACA_API_KEY and ALPACA_SECRET_KEY in .env."
        )


def log_startup_context():
    account = trade_client.get_account()
    clock = trade_client.get_clock()
    LOGGER.info(
        f"[{datetime.now()}] Authenticated with Alpaca. "
        f"paper={PAPER} account_status={account.status} buying_power={account.buying_power}"
    )
    LOGGER.info(
        f"[{datetime.now()}] Starting StockDataStream for {len(SYMBOLS)} symbols. "
        f"market_open={clock.is_open} next_open={clock.next_open} next_close={clock.next_close}"
    )
    LOGGER.info(f"[{datetime.now()}] Starting TradingStream for account order updates. paper={PAPER}")
    LOGGER.info(f"[{datetime.now()}] Runtime directory: {RUNTIME_DIR}")


def get_value(payload, field, default=None):
    if isinstance(payload, dict):
        return payload.get(field, default)
    return getattr(payload, field, default)


def normalize_text(value):
    if hasattr(value, "value"):
        value = value.value
    if value is None:
        return ""
    return str(value)


def canonical_trade_event(value):
    event = normalize_text(value).lower()
    if event == "partially_filled":
        return "partial_fill"
    return event


def to_int_qty(value):
    if value in (None, ""):
        return 0
    return int(float(value))


def allocate_exit_quantities(total_qty):
    tp1_qty = math.floor(total_qty * 0.75)
    tp2_qty = total_qty - tp1_qty
    return tp1_qty, tp2_qty


def create_client_order_id(symbol, action):
    suffix = datetime.utcnow().strftime("%m%d%H%M%S%f")[-12:]
    return f"{CLIENT_ORDER_PREFIX}-{symbol.lower()}-{action}-{suffix}"


def is_option_asset(payload):
    return normalize_text(get_value(payload, "asset_class", "")).lower() == "us_option"


def state_snapshot_locked():
    return {
        "active_positions": active_positions,
        "pending_entry_orders": pending_entry_orders,
        "pending_exit_orders": pending_exit_orders,
    }


def persist_state_locked():
    ensure_runtime_dir()
    temp_path = f"{STATE_FILE_PATH}.tmp"
    with open(temp_path, "w", encoding="utf-8") as state_file:
        json.dump(state_snapshot_locked(), state_file, indent=2, sort_keys=True)
    os.replace(temp_path, STATE_FILE_PATH)


def persist_state():
    with STATE_LOCK:
        persist_state_locked()


def load_state_from_disk():
    ensure_runtime_dir()
    if not os.path.exists(STATE_FILE_PATH):
        LOGGER.info("No existing state file found at startup.")
        return

    with open(STATE_FILE_PATH, "r", encoding="utf-8") as state_file:
        snapshot = json.load(state_file)

    with STATE_LOCK:
        active_positions.clear()
        active_positions.update(snapshot.get("active_positions", {}))
        pending_entry_orders.clear()
        pending_entry_orders.update(snapshot.get("pending_entry_orders", {}))
        pending_exit_orders.clear()
        pending_exit_orders.update(snapshot.get("pending_exit_orders", {}))

    LOGGER.info(
        "Loaded local state: active_positions=%s pending_entry_orders=%s pending_exit_orders=%s",
        len(active_positions),
        len(pending_entry_orders),
        len(pending_exit_orders),
    )


def reserved_exit_qty(symbol):
    reserved_qty = 0
    for order_state in pending_exit_orders.values():
        if order_state["symbol"] != symbol:
            continue
        reserved_qty += max(order_state["qty"] - order_state["filled_qty"], 0)
    return reserved_qty


def log_trade_update(event, order):
    order_id = get_value(order, "id", "unknown")
    order_symbol = get_value(order, "symbol", "unknown")
    order_status = get_value(order, "status", "unknown")
    filled_qty = get_value(order, "filled_qty", get_value(order, "qty", 0))
    LOGGER.info(
        f"[{datetime.now()}] TRADE_UPDATE event={event} status={order_status} "
        f"symbol={order_symbol} filled_qty={filled_qty} order_id={order_id}"
    )


def cap_target_quantities(position):
    remaining_qty = max(to_int_qty(position.get("total_qty", 0)), 0)
    tp1_qty = min(to_int_qty(position.get("tp1_qty", 0)), remaining_qty)
    remaining_qty -= tp1_qty
    tp2_qty = min(to_int_qty(position.get("tp2_qty", 0)), remaining_qty)
    position["tp1_qty"] = tp1_qty
    position["tp2_qty"] = tp2_qty


def get_power_bar_setup(symbol):
    """Evaluates the 2-minute chart for a Power Bar setup."""
    req = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame(2, TimeFrameUnit.Minute),
        limit=50,
    )
    bars = stock_client.get_stock_bars(req).df
    if bars.empty:
        return None

    bars["sma20"] = bars["close"].rolling(window=20).mean()
    current_bar = bars.iloc[-1]
    prev_bars = bars.iloc[-6:-1]

    local_resistance = bars["high"].iloc[-11:-1].max()
    local_support = bars["low"].iloc[-11:-1].min()

    avg_body_size = (prev_bars["close"] - prev_bars["open"]).abs().mean()
    current_body_size = abs(current_bar["close"] - current_bar["open"])

    is_power_bar = current_body_size > (avg_body_size * 2)
    near_sma = abs(current_bar["open"] - current_bar["sma20"]) / current_bar["sma20"] < 0.002

    if is_power_bar and near_sma and current_bar["close"] > current_bar["open"]:
        if current_bar["close"] > local_resistance and current_bar["close"] > current_bar["sma20"]:
            return "CALL", current_bar["close"], current_bar["low"]

    if is_power_bar and near_sma and current_bar["close"] < current_bar["open"]:
        if current_bar["close"] < local_support and current_bar["close"] < current_bar["sma20"]:
            return "PUT", current_bar["close"], current_bar["high"]

    return None


def get_best_option_contract(symbol, option_type):
    req = OptionChainRequest(underlying_symbol=symbol)
    chain = option_client.get_option_chain(req)
    valid_contracts = []

    for contract_symbol, data in chain.items():
        if data.contract_type.lower() != option_type.lower():
            continue

        premium = data.latest_quote.ask_price if data.latest_quote else 0
        delta = abs(data.greeks.delta) if data.greeks and data.greeks.delta else 1.0

        if 0 < premium <= 4.00 and delta <= 0.30:
            valid_contracts.append(
                {
                    "symbol": contract_symbol,
                    "premium": premium,
                    "delta": delta,
                    "expiration": data.expiration_date,
                }
            )

    if not valid_contracts:
        return None, None

    valid_contracts.sort(key=lambda contract: (contract["expiration"], abs(0.30 - contract["delta"])))
    return valid_contracts[0]["symbol"], valid_contracts[0]["premium"]


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
                position["tp1_qty"], position["tp2_qty"] = allocate_exit_quantities(filled_qty)

        if event == "fill":
            position["entry_status"] = "filled"
            pending_entry_orders.pop(order_id, None)

        if event in {"canceled", "expired", "rejected"}:
            pending_entry_orders.pop(order_id, None)
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

        if event in {"canceled", "expired", "rejected"}:
            remaining_qty = max(order_state["qty"] - order_state["filled_qty"], 0)
            target_field = order_state["target_field"]
            if target_field:
                position[target_field] += remaining_qty

        if position["total_qty"] <= 0:
            active_positions.pop(symbol, None)
        else:
            cap_target_quantities(position)

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
            "sl_price": None,
            "tp1_price": None,
            "tp2_price": None,
            "tp1_qty": 0,
            "tp2_qty": 0,
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
        if not order:
            continue
        process_entry_update(symbol, order_id, canonical_trade_event(get_value(order, "status", "")), order)

    for order_id, order_state in tracked_exit_orders:
        order = remote_orders.get(order_id) or safe_get_order_by_id(order_id)
        if not order:
            continue
        process_exit_update(order_id, canonical_trade_event(get_value(order, "status", "")), order, order_state)

    with STATE_LOCK:
        managed_symbols = [
            symbol for symbol, position in active_positions.items() if position.get("managed", True)
        ]

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
                    cap_target_quantities(latest_position)
                    persist_state_locked()
            continue

        if not has_pending_entry:
            LOGGER.warning("Dropping stale local position for %s because Alpaca has no open option position.", symbol)
            with STATE_LOCK:
                active_positions.pop(symbol, None)
                persist_state_locked()

    for option_symbol, remote_position in remote_positions.items():
        LOGGER.warning(
            "Found open option position %s at startup without matching local strategy state. "
            "Position will be logged but not actively managed until recreated by the bot.",
            option_symbol,
        )
        add_unmanaged_position(option_symbol, to_int_qty(get_value(remote_position, "qty", 0)))

    with STATE_LOCK:
        persist_state_locked()

    LOGGER.info(
        "Reconciled startup state: active_positions=%s pending_entry_orders=%s pending_exit_orders=%s",
        len(active_positions),
        len(pending_entry_orders),
        len(pending_exit_orders),
    )


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


def execute_entry(symbol, setup_data):
    option_type, entry_price, stop_loss_price = setup_data

    if symbol in active_positions:
        return

    contract_symbol, premium = get_best_option_contract(symbol, option_type)
    if not contract_symbol:
        return

    buying_power = float(trade_client.get_account().buying_power)
    trade_allocation = buying_power * 0.05
    contract_cost = premium * 100
    qty = math.floor(trade_allocation / contract_cost)

    if qty < 4:
        LOGGER.info("Skipping %s: Need qty >= 4 to split 75/25 correctly. Calculated: %s", symbol, qty)
        return

    risk = abs(entry_price - stop_loss_price)
    if option_type == "CALL":
        tp1_price = entry_price + (risk * 1.5)
        tp2_price = entry_price + (risk * 2.0)
    else:
        tp1_price = entry_price - (risk * 1.5)
        tp2_price = entry_price - (risk * 2.0)

    tp1_qty, tp2_qty = allocate_exit_quantities(qty)

    LOGGER.info(
        f"[{datetime.now()}] ENTER {option_type} on {symbol} @ {entry_price}. "
        f"SL: {stop_loss_price}, TP1: {tp1_price}, TP2: {tp2_price}"
    )

    req = MarketOrderRequest(
        symbol=contract_symbol,
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        client_order_id=create_client_order_id(symbol, "entry"),
    )
    try:
        order = trade_client.submit_order(order_data=req)
    except Exception as exc:
        LOGGER.error("[%s] Entry order failed for %s: %s", datetime.now(), symbol, exc)
        return

    order_id = str(order.id)
    with STATE_LOCK:
        pending_entry_orders[order_id] = symbol
        active_positions[symbol] = {
            "managed": True,
            "option_symbol": contract_symbol,
            "option_type": option_type,
            "sl_price": stop_loss_price,
            "tp1_price": tp1_price,
            "tp2_price": tp2_price,
            "tp1_qty": tp1_qty,
            "tp2_qty": tp2_qty,
            "total_qty": 0,
            "requested_qty": qty,
            "entry_order_id": order_id,
            "entry_status": "submitted",
        }
        persist_state_locked()

    LOGGER.info("[%s] Submitted entry order %s for %sx %s", datetime.now(), order_id, qty, contract_symbol)


def execute_exit(symbol, exit_qty, reason, target_field=None):
    with STATE_LOCK:
        position = active_positions[symbol]
        available_qty = position["total_qty"] - reserved_exit_qty(symbol)
        exit_qty = min(exit_qty, available_qty)
    if exit_qty <= 0:
        return False

    LOGGER.info("[%s] EXIT (%s) %sx %s", datetime.now(), reason, exit_qty, position["option_symbol"])

    action = "stop"
    if target_field == "tp1_qty":
        action = "tp1"
    elif target_field == "tp2_qty":
        action = "tp2"

    req = MarketOrderRequest(
        symbol=position["option_symbol"],
        qty=exit_qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY,
        client_order_id=create_client_order_id(symbol, action),
    )
    try:
        order = trade_client.submit_order(order_data=req)
    except Exception as exc:
        LOGGER.error("[%s] Exit order failed for %s: %s", datetime.now(), symbol, exc)
        return False

    order_id = str(order.id)
    with STATE_LOCK:
        pending_exit_orders[order_id] = {
            "symbol": symbol,
            "qty": exit_qty,
            "filled_qty": 0,
            "reason": reason,
            "target_field": target_field,
        }
        if target_field:
            position[target_field] = max(position[target_field] - exit_qty, 0)
        persist_state_locked()

    LOGGER.info("[%s] Submitted exit order %s for %s", datetime.now(), order_id, symbol)
    return True


async def handle_bar(bar):
    """Processes incoming 1-minute bars from WebSocket."""
    symbol = bar.symbol

    with STATE_LOCK:
        position = active_positions.get(symbol)

    if position:
        is_live_position = position["entry_status"] in {"partial_fill", "filled"} and position["total_qty"] > 0
        if not is_live_position:
            return

        sl_hit = (position["option_type"] == "CALL" and bar.close < position["sl_price"]) or (
            position["option_type"] == "PUT" and bar.close > position["sl_price"]
        )
        if sl_hit:
            execute_exit(symbol, position["total_qty"], "STOP_LOSS")
            return

        tp1_hit = (position["option_type"] == "CALL" and bar.high >= position["tp1_price"]) or (
            position["option_type"] == "PUT" and bar.low <= position["tp1_price"]
        )
        tp2_hit = (position["option_type"] == "CALL" and bar.high >= position["tp2_price"]) or (
            position["option_type"] == "PUT" and bar.low <= position["tp2_price"]
        )

        if tp1_hit and position["tp1_qty"] > 0:
            execute_exit(symbol, position["tp1_qty"], "TP1 (1.5R)", target_field="tp1_qty")

        if tp2_hit and position["tp2_qty"] > 0:
            execute_exit(symbol, position["tp2_qty"], "TP2 (2.0R)", target_field="tp2_qty")

    if bar.timestamp.minute % 2 == 0:
        setup = get_power_bar_setup(symbol)
        if setup:
            execute_entry(symbol, setup)


def start_trading_stream():
    trading_stream.subscribe_trade_updates(handle_trade_update)
    trading_stream.run()


def main():
    configure_logging()
    validate_configuration()
    load_state_from_disk()
    log_startup_context()
    reconcile_state()
    trading_thread = threading.Thread(target=start_trading_stream, name="alpaca-trading-stream", daemon=True)
    trading_thread.start()
    stock_stream.subscribe_bars(handle_bar, *SYMBOLS)
    stock_stream.run()


if __name__ == "__main__":
    main()