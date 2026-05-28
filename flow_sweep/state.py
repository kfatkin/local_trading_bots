import json
import os
import threading
from datetime import datetime
from decimal import Decimal

from .config import ET, LOGGER, STATE_FILE_PATH, TRADE_EVENT_LOG_LIMIT, ensure_runtime_dir


STATE_LOCK = threading.RLock()
CONTEXT_LOCK = threading.RLock()

active_positions = {}
pending_entry_orders = {}
pending_exit_orders = {}
daily_trade_state = {"session": None, "traded_symbols": [], "events": [], "daily_profit_lock": {}}

daily_context = {
    "session": None,
    "prior_session": None,
    "prepared": False,
    "prepared_at": None,
    "account": {},
    "contract_previews_refreshed_at": None,
    "contract_previews_last_monotonic": 0.0,
    "setups": {},
    "decisions": {},
    "last_attempt_monotonic": 0.0,
}
five_minute_builders = {}
last_completed_bars = {}


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
        daily_trade_state.update(snapshot.get("daily_trade_state", {"session": None, "traded_symbols": [], "events": [], "daily_profit_lock": {}}))
        daily_trade_state.setdefault("traded_symbols", [])
        daily_trade_state.setdefault("events", [])
        daily_trade_state.setdefault("daily_profit_lock", {})

    LOGGER.info(
        "Loaded local state: active_positions=%s pending_entry_orders=%s pending_exit_orders=%s traded_symbols=%s",
        len(active_positions),
        len(pending_entry_orders),
        len(pending_exit_orders),
        len(daily_trade_state.get("traded_symbols", [])),
    )


def reset_daily_trade_state_if_needed(session_date):
    session_key = session_date.isoformat()
    with STATE_LOCK:
        if daily_trade_state.get("session") == session_key:
            return
        daily_trade_state["session"] = session_key
        daily_trade_state["traded_symbols"] = []
        daily_trade_state["events"] = []
        daily_trade_state["daily_profit_lock"] = {}
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


def json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def record_trade_event_locked(event_type, symbol=None, event_id=None, **fields):
    events = daily_trade_state.setdefault("events", [])
    if event_id and any(event.get("event_id") == event_id for event in events):
        return

    event = {
        "timestamp": datetime.now(ET).isoformat(),
        "event_type": event_type,
    }
    if symbol:
        event["symbol"] = symbol
    if event_id:
        event["event_id"] = event_id
    event.update({key: json_safe(value) for key, value in fields.items() if value is not None})
    events.append(event)
    if len(events) > TRADE_EVENT_LOG_LIMIT:
        del events[: len(events) - TRADE_EVENT_LOG_LIMIT]


def record_trade_event(event_type, symbol=None, event_id=None, **fields):
    with STATE_LOCK:
        record_trade_event_locked(event_type, symbol=symbol, event_id=event_id, **fields)
        persist_state_locked()


def reserved_exit_qty(symbol):
    reserved_qty = 0
    for order_state in pending_exit_orders.values():
        if order_state["symbol"] != symbol:
            continue
        reserved_qty += max(order_state["qty"] - order_state["filled_qty"], 0)
    return reserved_qty


def current_setup(symbol):
    with CONTEXT_LOCK:
        return daily_context.get("setups", {}).get(symbol)
