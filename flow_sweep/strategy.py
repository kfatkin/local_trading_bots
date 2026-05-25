import math
import time
from datetime import datetime, timedelta

from alpaca.data.requests import OptionChainRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest

from .clients import option_client, trade_client, trading_stream
from .config import (
    ALPACA_DATA_FEED,
    AWS_PROFILE,
    AWS_REGION,
    CONSENSUS_THRESHOLD,
    ENTRY_WINDOW_END,
    ENTRY_WINDOW_START,
    EOD_EXIT_TIME,
    ET,
    FLOW_SCORE_PARTITION,
    LOGGER,
    MIN_FLOW_SCORE,
    PAPER,
    REGULAR_OPEN,
    SYMBOLS,
    TARGET_DELTA,
    TRADE_ALLOCATION_PCT,
    UTC,
    UW_TABLE_NAME,
)
from .flow_data import query_flow_scores, summarize_flow_rows
from .market_data import (
    get_day_high_low,
    get_premarket_high_low,
    get_week_high_low,
    previous_calendar_week_sessions,
    resolve_trading_sessions,
)
from .models import KeyLevel, TradeSetup
from .state import (
    CONTEXT_LOCK,
    STATE_LOCK,
    active_positions,
    current_setup,
    daily_context,
    five_minute_builders,
    last_completed_bars,
    mark_symbol_traded,
    pending_entry_orders,
    pending_exit_orders,
    persist_state_locked,
    reset_daily_trade_state_if_needed,
    reserved_exit_qty,
    was_symbol_traded_today,
)
from .utils import create_client_order_id, get_value, is_option_asset, key_level_to_dict, normalize_text, to_float, to_int_qty


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


def level_payload(name, label, side, price=None, status="ready", role="skipped", note=None):
    return {
        "name": name,
        "label": label,
        "side": side,
        "price": round(float(price), 4) if price not in (None, "") else None,
        "status": status,
        "role": role,
        "note": note,
    }


def missing_level(name, label, side):
    return level_payload(name, label, side, status="missing", note="Level unavailable")


def pending_premarket_levels():
    return [
        level_payload("premarket_low", "Premarket Low", "support", status="pending", note="Pending until upcoming premarket completes"),
        level_payload("premarket_high", "Premarket High", "resistance", status="pending", note="Pending until upcoming premarket completes"),
    ]


def build_symbol_key_levels(symbol, trading_day, prior_day, week_sessions, include_premarket):
    prior_range = get_day_high_low(symbol, prior_day)
    week_range = get_week_high_low(symbol, week_sessions)
    premarket_range = get_premarket_high_low(symbol, trading_day) if include_premarket else None

    if premarket_range:
        premarket_high, premarket_low = premarket_range
        premarket_levels = [
            level_payload("premarket_low", "Premarket Low", "support", premarket_low),
            level_payload("premarket_high", "Premarket High", "resistance", premarket_high),
        ]
    elif include_premarket:
        premarket_levels = [
            missing_level("premarket_low", "Premarket Low", "support"),
            missing_level("premarket_high", "Premarket High", "resistance"),
        ]
    else:
        premarket_levels = pending_premarket_levels()

    if prior_range:
        prior_high, prior_low = prior_range
        prior_levels = [
            level_payload("prior_day_low", "Prior Day Low", "support", prior_low),
            level_payload("prior_day_high", "Prior Day High", "resistance", prior_high),
        ]
    else:
        prior_levels = [
            missing_level("prior_day_low", "Prior Day Low", "support"),
            missing_level("prior_day_high", "Prior Day High", "resistance"),
        ]

    if week_range:
        week_high, week_low = week_range
        week_levels = [
            level_payload("prior_week_low", "Prior Week Low", "support", week_low),
            level_payload("prior_week_high", "Prior Week High", "resistance", week_high),
        ]
    else:
        week_levels = [
            missing_level("prior_week_low", "Prior Week Low", "support"),
            missing_level("prior_week_high", "Prior Week High", "resistance"),
        ]

    return premarket_levels + prior_levels + week_levels


def apply_level_roles(levels, bias):
    if not bias:
        return [{**level, "role": "skipped", "note": level.get("note") or "Skipped: no actionable directional bias"} for level in levels]

    observed_side = "support" if bias.direction == "bullish" else "resistance"
    observed_note = "Observed for call entries after a 5m sweep and close back above" if bias.direction == "bullish" else "Observed for put entries after a 5m sweep and close back below"
    skipped_note = "Skipped for this bullish setup" if bias.direction == "bullish" else "Skipped for this bearish setup"
    role_levels = []
    for level in levels:
        is_observed = level["side"] == observed_side
        note = observed_note if is_observed else skipped_note
        role_levels.append({**level, "role": "observed" if is_observed else "skipped", "note": level.get("note") or note})
    return role_levels


def setup_from_key_levels(symbol, bias, key_levels):
    levels_by_name = {level["name"]: level for level in key_levels if level.get("price") is not None and level.get("status") == "ready"}
    needed = {"premarket_low", "premarket_high", "prior_day_low", "prior_day_high", "prior_week_low", "prior_week_high"}
    if not needed.issubset(levels_by_name):
        LOGGER.warning(
            "%s skipped: missing levels premarket=%s prior=%s week=%s",
            symbol,
            "premarket_low" in levels_by_name and "premarket_high" in levels_by_name,
            "prior_day_low" in levels_by_name and "prior_day_high" in levels_by_name,
            "prior_week_low" in levels_by_name and "prior_week_high" in levels_by_name,
        )
        return None

    support_levels = (
        KeyLevel("premarket_low", levels_by_name["premarket_low"]["price"]),
        KeyLevel("prior_day_low", levels_by_name["prior_day_low"]["price"]),
        KeyLevel("prior_week_low", levels_by_name["prior_week_low"]["price"]),
    )
    resistance_levels = (
        KeyLevel("premarket_high", levels_by_name["premarket_high"]["price"]),
        KeyLevel("prior_day_high", levels_by_name["prior_day_high"]["price"]),
        KeyLevel("prior_week_high", levels_by_name["prior_week_high"]["price"]),
    )

    LOGGER.info(
        "%s %s setup: consensus=%.1f%% top_score=%s bull=$%.0f bear=$%.0f levels PM %.2f/%.2f PD %.2f/%.2f PW %.2f/%.2f",
        symbol,
        bias.direction,
        bias.consensus * 100,
        bias.top_score,
        bias.bullish_premium,
        bias.bearish_premium,
        levels_by_name["premarket_high"]["price"],
        levels_by_name["premarket_low"]["price"],
        levels_by_name["prior_day_high"]["price"],
        levels_by_name["prior_day_low"]["price"],
        levels_by_name["prior_week_high"]["price"],
        levels_by_name["prior_week_low"]["price"],
    )
    return TradeSetup(symbol, bias, support_levels, resistance_levels)


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


def flow_error_decision(symbol, exc):
    return {
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
        "flow_rows": [],
        "key_levels": [],
    }


def query_prior_session_flow(prior_open, prior_close):
    biases = {}
    decisions = {}
    for symbol in SYMBOLS:
        try:
            rows = query_flow_scores(symbol, prior_open, prior_close)
        except Exception as exc:
            LOGGER.warning("Flow score read failed for %s: %s", symbol, exc)
            decisions[symbol] = flow_error_decision(symbol, exc)
            continue

        bias, decision = summarize_flow_rows(symbol, rows)
        decisions[symbol] = decision
        if bias:
            biases[symbol] = bias
    return biases, decisions


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

        prior_open = schedule.iloc[trading_idx - 1]["market_open"].to_pydatetime().astimezone(UTC)
        prior_close = schedule.iloc[trading_idx - 1]["market_close"].to_pydatetime().astimezone(UTC)
        biases, decisions = query_prior_session_flow(prior_open, prior_close)
        week_sessions = previous_calendar_week_sessions(schedule, trading_day)
        include_premarket = now_et.date() == trading_day and now_et.time() >= REGULAR_OPEN

        for symbol, decision in decisions.items():
            key_levels = build_symbol_key_levels(symbol, trading_day, prior_day, week_sessions, include_premarket)
            decision["key_levels"] = apply_level_roles(key_levels, biases.get(symbol))

        if now_et.date() != trading_day or now_et.time() < REGULAR_OPEN:
            for symbol, decision in decisions.items():
                if decision.get("status") == "flow_bias":
                    decision.update(
                        {
                            "status": "flow_preview",
                            "reason": f"High-score flow loaded from {prior_day}; waiting for {trading_day} levels",
                        }
                    )

            daily_context.update(
                {
                    "session": trading_day.isoformat(),
                    "prior_session": prior_day.isoformat(),
                    "prepared": False,
                    "prepared_at": datetime.now(UTC).isoformat(),
                    "setups": {},
                    "decisions": decisions,
                }
            )
            high_score_rows = sum(len(decision.get("flow_rows", [])) for decision in decisions.values())
            LOGGER.info(
                "Loaded %s high-score flow rows from prior_session=%s. Waiting for %s premarket to complete before preparing setups.",
                high_score_rows,
                prior_day,
                trading_day,
            )
            return

        setups = {}
        for symbol, bias in biases.items():
            if not bias:
                continue
            setup = setup_from_key_levels(symbol, bias, decisions[symbol].get("key_levels", []))
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
