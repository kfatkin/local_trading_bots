import time
import threading
from datetime import datetime, timedelta

from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest

from .clients import trade_client, trading_stream
from .config import (
    ALPACA_DATA_FEED,
    AWS_PROFILE,
    AWS_REGION,
    CONSENSUS_THRESHOLD,
    BREAKEVEN_TRIGGER_R_MULTIPLE,
    CONTINUATION_DISPLACEMENT_LOOKBACK,
    CONTINUATION_DISPLACEMENT_MIN_RANGE_MULTIPLE,
    CONTINUATION_MAX_ZONE_AGE_BARS,
    DAILY_PROFIT_LOCK_BLOCKS_NEW_ENTRIES,
    DAILY_PROFIT_LOCK_DRAWDOWN_PCT,
    DAILY_PROFIT_LOCK_ENABLED,
    DAILY_PROFIT_LOCK_FLOOR_PCT,
    DAILY_PROFIT_LOCK_TRIGGER_PCT,
    ENABLE_CONTINUATION_FVG,
    ENTRY_LEVEL_CLEARANCE_MIN_RANGE_PCT,
    ENTRY_MAX_TARGET_R_MULTIPLE,
    ENTRY_RECLAIM_CLOSE_MIN_RANGE_PCT,
    ENTRY_WINDOW_END,
    ENTRY_WINDOW_START,
    EOD_EXIT_TIME,
    ET,
    FLOW_SCORE_PARTITION,
    LOGGER,
    MIN_FLOW_SCORE,
    NO_PROGRESS_EXIT_ENABLED,
    NO_PROGRESS_MIN_OPTION_GAIN_PCT,
    NO_PROGRESS_MINUTES,
    OPTION_PREVIEW_REFRESH_SECONDS,
    PARTIAL_EXIT_PCT,
    PAPER,
    PRE_BREAKEVEN_OPTION_FLOOR_PCT,
    PRE_BREAKEVEN_OPTION_HARD_STOP_LOSS_PCT,
    PRE_BREAKEVEN_OPTION_LOCK_ENABLED,
    PRE_BREAKEVEN_OPTION_LOCK_TRIGGER_PCT,
    REGULAR_OPEN,
    RUNNER_EXTENSION_R_MULTIPLE,
    RUNNER_PROFIT_FLOOR_ENTRY_MULTIPLE,
    RUNNER_PROFIT_FLOOR_PARTIAL_MULTIPLE,
    RUNNER_STALL_MINUTES,
    RUNNER_STALL_R_MULTIPLE,
    SECOND_PARTIAL_EXIT_PCT,
    SECOND_PARTIAL_EXIT_R_MULTIPLE,
    SYMBOLS,
    TARGET_R_MULTIPLE,
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
from .models import FlowBias, KeyLevel, TradeSetup
from .oi_watchlist import load_morning_watchlist
from .option_selection import account_balance_summary, build_contract_preview, empty_contract_preview, option_market_snapshot, validate_entry_contract
from .state import (
    CONTEXT_LOCK,
    STATE_LOCK,
    active_positions,
    current_setup,
    daily_context,
    daily_trade_state,
    five_minute_builders,
    last_completed_bars,
    mark_symbol_traded,
    pending_entry_orders,
    pending_exit_orders,
    persist_state_locked,
    record_trade_event,
    record_trade_event_locked,
    reset_daily_trade_state_if_needed,
    reserved_exit_qty,
    was_symbol_traded_today,
)
from .utils import create_client_order_id, get_value, is_option_asset, key_level_to_dict, normalize_text, to_int_qty


CONTRACT_PREVIEW_LOCK = threading.Lock()
CONTRACT_PREVIEW_STATUSES = {"orb_wait", "ready"}
pending_sweep_confirmations = {}
continuation_contexts = {}
opening_ranges = {}


def log_startup_context():
    account = trade_client.get_account()
    account_summary = account_balance_summary(account)
    clock = trade_client.get_clock()
    LOGGER.info(
        "Authenticated with Alpaca. paper=%s account_status=%s account_balance=%s balance_field=%s buying_power=%s",
        PAPER,
        account.status,
        account_summary["account_balance"],
        account_summary["account_balance_field"],
        account_summary.get("buying_power"),
    )
    LOGGER.info(
        "Starting local ORB options bot. market_open=%s next_open=%s next_close=%s",
        clock.is_open,
        clock.next_open,
        clock.next_close,
    )
    LOGGER.info(
        "Morning watchlist source: UW OI change using fattytrades-compatible filters. aws_profile=%s aws_region=%s",
        AWS_PROFILE or "default",
        AWS_REGION,
    )
    LOGGER.info(
        "Orders: allocation=%.1f%% account_balance target_delta=%.2f planned premium band from watchlist data_feed=%s",
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
    observed_note = "Observed for call entries after a meaningful sweep/reclaim and confirming 5m candle" if bias.direction == "bullish" else "Observed for put entries after a meaningful sweep/rejection and confirming 5m candle"
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
        action = "Buy calls after a confirmed sweep/reclaim"
        if ENABLE_CONTINUATION_FVG:
            action = "Buy calls after either a confirmed sweep/reclaim or a bullish 5m continuation FVG pullback"
        option_type = "CALL"
    else:
        trigger_levels = setup.resistance_levels
        target_levels = sorted(setup.support_levels, key=lambda level: level.price, reverse=True)
        action = "Buy puts after a confirmed sweep/rejection"
        if ENABLE_CONTINUATION_FVG:
            action = "Buy puts after either a confirmed sweep/rejection or a bearish 5m continuation FVG pullback"
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


def should_preview_contract(decision):
    return (
        decision.get("status") in CONTRACT_PREVIEW_STATUSES
        and decision.get("option_type") in {"CALL", "PUT"}
        and decision.get("direction") in {"bullish", "bearish"}
    )


def attach_contract_previews(decisions):
    try:
        account = account_balance_summary()
    except Exception as exc:
        LOGGER.warning("Unable to load Alpaca account balance for contract previews: %s", exc)
        for symbol, decision in decisions.items():
            decision["contract_preview"] = empty_contract_preview(
                symbol,
                decision.get("option_type"),
                status="error",
                reason=f"Account balance read failed: {exc}",
            )
        return {}

    for symbol, decision in decisions.items():
        option_type = decision.get("option_type")
        contract_plan = decision.get("contract_plan") or None
        if should_preview_contract(decision):
            decision["contract_preview"] = build_contract_preview(symbol, option_type, account=account, contract_plan=contract_plan)
        else:
            decision["contract_preview"] = empty_contract_preview(symbol, option_type, reason="No planned entry for this symbol", account=account)
    return account


def refreshed_preview_context(decisions, reason="scheduled"):
    account = attach_contract_previews(decisions)
    return {
        "account": account,
        "contract_previews_refreshed_at": datetime.now(UTC).isoformat(),
        "contract_previews_last_monotonic": time.monotonic(),
        "contract_previews_refresh_reason": reason,
    }


def publish_contract_preview(symbol, contract, reason):
    with CONTEXT_LOCK:
        decisions = daily_context.get("decisions", {})
        if symbol in decisions:
            decisions[symbol]["contract_preview"] = contract
        daily_context["contract_previews_refreshed_at"] = datetime.now(UTC).isoformat()
        daily_context["contract_previews_last_monotonic"] = time.monotonic()
        daily_context["contract_previews_refresh_reason"] = reason


def build_fresh_entry_contract(symbol, option_type):
    with CONTEXT_LOCK:
        decision = dict((daily_context.get("decisions") or {}).get(symbol) or {})
    contract = build_contract_preview(symbol, option_type, contract_plan=decision.get("contract_plan") or None)
    publish_contract_preview(symbol, contract, "entry_recheck")
    LOGGER.info(
        "Entry contract recheck %s %s selected=%s status=%s delta=%s bid=%s ask=%s qty=%s reason=%s",
        symbol,
        option_type,
        contract.get("symbol"),
        contract.get("status"),
        contract.get("delta"),
        contract.get("bid"),
        contract.get("ask"),
        contract.get("quantity"),
        contract.get("reason"),
    )
    return contract


def refresh_contract_previews_if_needed(force=False, reason="scheduled"):
    with CONTRACT_PREVIEW_LOCK:
        with CONTEXT_LOCK:
            if not daily_context.get("decisions"):
                return False
            last_refresh = daily_context.get("contract_previews_last_monotonic", 0.0) or 0.0
            if not force and time.monotonic() - last_refresh < OPTION_PREVIEW_REFRESH_SECONDS:
                return False
            decisions = {symbol: dict(decision) for symbol, decision in daily_context.get("decisions", {}).items()}

        preview_context = refreshed_preview_context(decisions, reason=reason)

        with CONTEXT_LOCK:
            current_decisions = daily_context.get("decisions", {})
            for symbol, decision in decisions.items():
                if symbol in current_decisions:
                    current_decisions[symbol]["contract_preview"] = decision.get("contract_preview")
            daily_context.update(preview_context)

    LOGGER.info("Refreshed contract previews reason=%s", reason)
    return True


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
        "contract_preview": empty_contract_preview(symbol),
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


def opening_range_levels(symbol):
    orb = opening_ranges.get(symbol)
    if not orb or not orb.get("completed"):
        return [
            level_payload("opening_range_low", "Opening 5m Low", "support", status="pending", note="Waiting for the first 5m range"),
            level_payload("opening_range_high", "Opening 5m High", "resistance", status="pending", note="Waiting for the first 5m range"),
        ]
    return [
        level_payload("opening_range_low", "Opening 5m Low", "support", orb.get("low"), note="1m closes back inside this range count toward the stop"),
        level_payload("opening_range_high", "Opening 5m High", "resistance", orb.get("high"), note="1m closes back inside this range count toward the stop"),
    ]


def refresh_intraday_decisions(now_et=None):
    now_et = now_et or datetime.now(ET)
    with CONTEXT_LOCK:
        decisions = daily_context.get("decisions") or {}
        for symbol, decision in decisions.items():
            orb = opening_ranges.get(symbol)
            decision["key_levels"] = opening_range_levels(symbol)
            if now_et.time() < REGULAR_OPEN:
                decision["status"] = "staged"
                decision["reason"] = "Ticker staged in watchlist; contract selection begins at the regular open"
                decision["trigger_levels"] = []
                decision["target_levels"] = []
            elif not orb or not orb.get("completed"):
                decision["status"] = "orb_wait"
                decision["reason"] = "Contract planned; waiting for the opening 5m range to complete"
                decision["trigger_levels"] = []
                decision["target_levels"] = []
            else:
                trigger_level = orb["high"] if decision.get("direction") == "bullish" else orb["low"]
                trigger_name = "opening_range_high" if decision.get("direction") == "bullish" else "opening_range_low"
                decision["trigger_levels"] = [key_level_to_dict(KeyLevel(trigger_name, trigger_level))]
                decision["target_levels"] = []
                decision["status"] = "ready"
                decision["reason"] = f"Waiting for a 1m close outside the opening 5m range {orb['low']:.2f}-{orb['high']:.2f}"


def morning_watchlist_setups(now_et):
    decisions = load_morning_watchlist(now_et=now_et)
    setups = {}
    for symbol, decision in decisions.items():
        direction = decision.get("direction") or "neutral"
        setups[symbol] = TradeSetup(
            symbol,
            FlowBias(
                symbol,
                direction,
                float(decision.get("consensus") or 0.0),
                float(decision.get("bullish_premium") or 0.0),
                float(decision.get("bearish_premium") or 0.0),
                float(decision.get("total_premium") or 0.0),
                int(decision.get("top_score") or 0),
                int(decision.get("directional_row_count") or 0),
            ),
            tuple(),
            tuple(),
        )
    return setups, decisions


def prepare_daily_context(now_et=None, force=False):
    now_et = now_et or datetime.now(ET)
    with CONTEXT_LOCK:
        if not force and time.monotonic() - daily_context["last_attempt_monotonic"] < 60:
            return
        daily_context["last_attempt_monotonic"] = time.monotonic()

        schedule, trading_idx, trading_day, prior_day = resolve_trading_sessions(now_et)
        if daily_context["prepared"] and daily_context["session"] == trading_day.isoformat():
            refresh_intraday_decisions(now_et)
            return

        reset_daily_trade_state_if_needed(trading_day)
        if daily_context.get("session") != trading_day.isoformat():
            pending_sweep_confirmations.clear()
            continuation_contexts.clear()
            opening_ranges.clear()

        setups, decisions = morning_watchlist_setups(now_et)
        refresh_intraday_decisions(now_et)
        preview_context = refreshed_preview_context(
            decisions,
            reason="preopen_watchlist" if now_et.time() < REGULAR_OPEN else "opening_watchlist",
        )

        daily_context.update(
            {
                "session": trading_day.isoformat(),
                "prior_session": prior_day.isoformat(),
                "prepared": True,
                "prepared_at": datetime.now(UTC).isoformat(),
                "setups": setups,
                "decisions": decisions,
                **preview_context,
            }
        )
        LOGGER.info("Prepared %s morning OI watchlist setups for session=%s prior_session=%s", len(setups), trading_day, prior_day)


def risk_plan_for_entry(option_type, entry_underlying, stop_underlying, target_underlying):
    if option_type == "CALL":
        risk_underlying = entry_underlying - stop_underlying
        reward_underlying = target_underlying - entry_underlying
        breakeven_trigger_underlying = entry_underlying + risk_underlying * BREAKEVEN_TRIGGER_R_MULTIPLE
    else:
        risk_underlying = stop_underlying - entry_underlying
        reward_underlying = entry_underlying - target_underlying
        breakeven_trigger_underlying = entry_underlying - risk_underlying * BREAKEVEN_TRIGGER_R_MULTIPLE

    if risk_underlying <= 0 or reward_underlying <= 0:
        return None

    return {
        "risk_underlying": round(risk_underlying, 4),
        "reward_underlying": round(reward_underlying, 4),
        "target_r_multiple": round(reward_underlying / risk_underlying, 4),
        "breakeven_trigger_underlying": round(breakeven_trigger_underlying, 4),
    }


def target_level_for_entry(setup, option_type, entry_price, stop_underlying):
    risk_underlying = entry_price - stop_underlying if option_type == "CALL" else stop_underlying - entry_price
    if risk_underlying <= 0:
        return None, None

    if option_type == "CALL":
        fixed_target = entry_price + risk_underlying * TARGET_R_MULTIPLE
        candidates = [level for level in setup.resistance_levels if level.price >= fixed_target]
        candidates.sort(key=lambda level: level.price)
    else:
        fixed_target = entry_price - risk_underlying * TARGET_R_MULTIPLE
        candidates = [level for level in setup.support_levels if level.price <= fixed_target]
        candidates.sort(key=lambda level: level.price, reverse=True)

    target_level = candidates[0] if candidates else KeyLevel("fixed_2r", fixed_target)
    return target_level, risk_plan_for_entry(option_type, entry_price, stop_underlying, target_level.price)


def bar_range(bar):
    return max(float(bar["high"]) - float(bar["low"]), 0.0)


def sweep_reclaim_quality_reason(setup, level, bar):
    width = bar_range(bar)
    if width <= 0:
        return "zero-range sweep candle"

    min_clearance = width * ENTRY_LEVEL_CLEARANCE_MIN_RANGE_PCT
    if setup.bias.direction == "bullish":
        if bar["close"] < bar["low"] + width * ENTRY_RECLAIM_CLOSE_MIN_RANGE_PCT:
            return "reclaim candle did not close in the upper half"
        if bar["close"] - level.price < min_clearance:
            return "reclaim close did not clear the swept level enough"
    else:
        if bar["close"] > bar["high"] - width * ENTRY_RECLAIM_CLOSE_MIN_RANGE_PCT:
            return "rejection candle did not close in the lower half"
        if level.price - bar["close"] < min_clearance:
            return "rejection close did not clear the swept level enough"
    return None


def swept_level_for_bar(setup, bar):
    if setup.bias.direction == "bullish":
        swept = [level for level in setup.support_levels if bar["low"] < level.price and bar["close"] > level.price]
    else:
        swept = [level for level in setup.resistance_levels if bar["high"] > level.price and bar["close"] < level.price]
    if not swept:
        return None
    qualified = [level for level in swept if not sweep_reclaim_quality_reason(setup, level, bar)]
    if not qualified:
        return None
    return min(qualified, key=lambda level: abs(bar["open"] - level.price))


def confirmation_entry_ready(setup, swept_level, sweep_bar, confirmation_bar):
    width = bar_range(confirmation_bar)
    if width <= 0:
        return False, "zero-range confirmation candle"

    if setup.bias.direction == "bullish":
        if confirmation_bar["low"] < swept_level.price:
            return False, "confirmation candle did not hold above swept support"
        strong_close = confirmation_bar["close"] >= confirmation_bar["low"] + width * ENTRY_RECLAIM_CLOSE_MIN_RANGE_PCT
        broke_signal = confirmation_bar["high"] > sweep_bar["high"]
        if not (strong_close or broke_signal):
            return False, "confirmation candle lacked bullish follow-through"
    else:
        if confirmation_bar["high"] > swept_level.price:
            return False, "confirmation candle did not hold below swept resistance"
        strong_close = confirmation_bar["close"] <= confirmation_bar["high"] - width * ENTRY_RECLAIM_CLOSE_MIN_RANGE_PCT
        broke_signal = confirmation_bar["low"] < sweep_bar["low"]
        if not (strong_close or broke_signal):
            return False, "confirmation candle lacked bearish follow-through"
    return True, "confirmed"


def entry_risk_quality_reason(risk_plan):
    target_r = risk_plan.get("target_r_multiple") if risk_plan else None
    if target_r is None:
        return "missing risk plan"
    if target_r > ENTRY_MAX_TARGET_R_MULTIPLE:
        return f"planned target {target_r:.2f}R exceeds max {ENTRY_MAX_TARGET_R_MULTIPLE:.2f}R"
    return None


def strong_directional_close(direction, bar):
    width = bar_range(bar)
    if width <= 0:
        return False
    if direction == "bullish":
        return bar["close"] > bar["open"] and bar["close"] >= bar["low"] + width * ENTRY_RECLAIM_CLOSE_MIN_RANGE_PCT
    return bar["close"] < bar["open"] and bar["close"] <= bar["high"] - width * ENTRY_RECLAIM_CLOSE_MIN_RANGE_PCT


def trim_tail(items, limit=20):
    if len(items) > limit:
        del items[:-limit]


def new_continuation_context():
    return {"recent_bars": [], "swing_highs": [], "swing_lows": [], "active_zone": None}


def update_continuation_swings(context):
    bars = context["recent_bars"]
    if len(bars) < 3:
        return

    left_bar, pivot_bar, right_bar = bars[-3], bars[-2], bars[-1]
    pivot_time = pivot_bar["close_time"]

    if left_bar["high"] < pivot_bar["high"] > right_bar["high"]:
        swing_highs = context["swing_highs"]
        if not swing_highs or swing_highs[-1]["close_time"] != pivot_time:
            swing_highs.append({"price": round(float(pivot_bar["high"]), 4), "close_time": pivot_time})
            trim_tail(swing_highs)

    if left_bar["low"] > pivot_bar["low"] < right_bar["low"]:
        swing_lows = context["swing_lows"]
        if not swing_lows or swing_lows[-1]["close_time"] != pivot_time:
            swing_lows.append({"price": round(float(pivot_bar["low"]), 4), "close_time": pivot_time})
            trim_tail(swing_lows)


def latest_pivot_before(points, before_time):
    for point in reversed(points):
        if point["close_time"] < before_time:
            return point
    return None


def latest_pivot_between(points, after_time, before_time):
    for point in reversed(points):
        if after_time < point["close_time"] < before_time:
            return point
    return None


def average_prior_range(recent_bars):
    if len(recent_bars) < 2:
        return 0.0
    lookback_slice = recent_bars[-(CONTINUATION_DISPLACEMENT_LOOKBACK + 1) : -1]
    if not lookback_slice:
        return 0.0
    return sum(bar_range(item) for item in lookback_slice) / len(lookback_slice)


def build_continuation_zone(setup, context, bar):
    recent_bars = context["recent_bars"]
    if len(recent_bars) < 3:
        return None

    prior_range = average_prior_range(recent_bars)
    if prior_range <= 0:
        return None
    if bar_range(bar) < prior_range * CONTINUATION_DISPLACEMENT_MIN_RANGE_MULTIPLE:
        return None
    if not strong_directional_close(setup.bias.direction, bar):
        return None

    anchor_bar = recent_bars[-3]
    if setup.bias.direction == "bullish":
        break_pivot = latest_pivot_before(context["swing_highs"], bar["close_time"])
        if not break_pivot or bar["close"] <= break_pivot["price"]:
            return None
        structure_pivot = latest_pivot_between(context["swing_lows"], break_pivot["close_time"], bar["close_time"])
        if not structure_pivot or bar["low"] <= anchor_bar["high"]:
            return None
        zone_low = round(float(anchor_bar["high"]), 4)
        zone_high = round(float(bar["low"]), 4)
    else:
        break_pivot = latest_pivot_before(context["swing_lows"], bar["close_time"])
        if not break_pivot or bar["close"] >= break_pivot["price"]:
            return None
        structure_pivot = latest_pivot_between(context["swing_highs"], break_pivot["close_time"], bar["close_time"])
        if not structure_pivot or bar["high"] >= anchor_bar["low"]:
            return None
        zone_low = round(float(bar["high"]), 4)
        zone_high = round(float(anchor_bar["low"]), 4)

    return {
        "setup_type": "continuation_fvg",
        "signal_name": "continuation_fvg",
        "direction": setup.bias.direction,
        "zone_low": min(zone_low, zone_high),
        "zone_high": max(zone_low, zone_high),
        "break_level_price": break_pivot["price"],
        "break_level_time": break_pivot["close_time"],
        "structure_price": structure_pivot["price"],
        "structure_time": structure_pivot["close_time"],
        "armed_at": bar["close_time"],
        "signal_bar": bar.copy(),
        "age_bars": 0,
    }


def continuation_zone_status(setup, zone, bar):
    if setup.bias.direction == "bullish":
        if bar["low"] <= zone["structure_price"]:
            return "invalidated", "continuation structure low failed"
    else:
        if bar["high"] >= zone["structure_price"]:
            return "invalidated", "continuation structure high failed"

    touched_zone = bar["low"] <= zone["zone_high"] and bar["high"] >= zone["zone_low"]
    if not touched_zone:
        return "waiting", "zone not touched"
    if not strong_directional_close(setup.bias.direction, bar):
        return "waiting", "touch bar lacked directional confirmation"

    zone_mid = (zone["zone_low"] + zone["zone_high"]) / 2
    if setup.bias.direction == "bullish" and bar["close"] < zone_mid:
        return "waiting", "touch bar closed below the FVG midpoint"
    if setup.bias.direction == "bearish" and bar["close"] > zone_mid:
        return "waiting", "touch bar closed above the FVG midpoint"
    return "ready", "confirmed"


def age_active_continuation_zone(context):
    zone = context.get("active_zone")
    if not zone:
        return None
    zone["age_bars"] += 1
    if zone["age_bars"] > CONTINUATION_MAX_ZONE_AGE_BARS:
        context["active_zone"] = None
        return zone
    return None


def sweep_entry_metadata(setup, swept_level, signal_bar, sweep_bar):
    option_type = "CALL" if setup.bias.direction == "bullish" else "PUT"
    stop_underlying = sweep_bar["low"] if option_type == "CALL" else sweep_bar["high"]
    return {
        "setup_type": "oi_5morb",
        "signal_name": swept_level.name,
        "signal_price": swept_level.price,
        "signal_label": f"confirmed {swept_level.name} sweep",
        "stop_underlying": stop_underlying,
        "stop_mode": "underlying_sweep_extreme",
        "signal_time": sweep_bar.get("close_time"),
        "position_fields": {
            "swept_level": swept_level.name,
            "swept_level_price": swept_level.price,
            "sweep_signal_close_time": sweep_bar["close_time"].isoformat() if sweep_bar.get("close_time") else None,
            "entry_confirmation_close_time": signal_bar["close_time"].isoformat() if signal_bar.get("close_time") else None,
        },
        "event_fields": {
            "swept_level": swept_level.name,
            "swept_level_price": swept_level.price,
            "sweep_signal_close_time": sweep_bar.get("close_time"),
            "entry_confirmation_close_time": signal_bar.get("close_time"),
        },
    }


def continuation_entry_metadata(zone, signal_bar):
    signal_price = round((zone["zone_low"] + zone["zone_high"]) / 2, 4)
    return {
        "setup_type": zone["setup_type"],
        "signal_name": zone["signal_name"],
        "signal_price": signal_price,
        "signal_label": "continuation FVG",
        "stop_underlying": zone["structure_price"],
        "stop_mode": "underlying_structure_swing",
        "signal_time": zone.get("armed_at"),
        "position_fields": {
            "swept_level": zone["signal_name"],
            "swept_level_price": signal_price,
            "continuation_zone_low": zone["zone_low"],
            "continuation_zone_high": zone["zone_high"],
            "continuation_break_level": zone["break_level_price"],
            "continuation_structure_price": zone["structure_price"],
            "continuation_signal_close_time": zone["armed_at"].isoformat() if zone.get("armed_at") else None,
            "entry_confirmation_close_time": signal_bar["close_time"].isoformat() if signal_bar.get("close_time") else None,
        },
        "event_fields": {
            "continuation_zone_low": zone["zone_low"],
            "continuation_zone_high": zone["zone_high"],
            "continuation_break_level": zone["break_level_price"],
            "continuation_structure_price": zone["structure_price"],
            "continuation_signal_close_time": zone.get("armed_at"),
            "entry_confirmation_close_time": signal_bar.get("close_time"),
        },
    }


def orb_entry_metadata(setup, signal_bar):
    option_type = "CALL" if setup.bias.direction == "bullish" else "PUT"
    orb = opening_ranges.get(setup.symbol) or {}
    breakout_level = orb.get("high") if option_type == "CALL" else orb.get("low")
    breakout_name = "opening_range_high" if option_type == "CALL" else "opening_range_low"
    return {
        "setup_type": "opening_range_breakout",
        "signal_name": breakout_name,
        "signal_price": breakout_level,
        "signal_label": "1m ORB close breakout",
        "stop_underlying": orb.get("low") if option_type == "CALL" else orb.get("high"),
        "stop_mode": "orb_range_reentry",
        "signal_time": signal_bar.get("timestamp") or signal_bar.get("close_time"),
        "position_fields": {
            "opening_range_high": orb.get("high"),
            "opening_range_low": orb.get("low"),
            "orb_completed_at": orb.get("completed_at").isoformat() if hasattr(orb.get("completed_at"), "isoformat") else orb.get("completed_at"),
            "orb_breakout_close_time": signal_bar.get("timestamp").isoformat() if hasattr(signal_bar.get("timestamp"), "isoformat") else signal_bar.get("timestamp"),
        },
        "event_fields": {
            "opening_range_high": orb.get("high"),
            "opening_range_low": orb.get("low"),
            "orb_completed_at": orb.get("completed_at"),
            "orb_breakout_close_time": signal_bar.get("timestamp"),
        },
    }


def execute_entry(symbol, setup, signal_bar, entry_metadata):
    with STATE_LOCK:
        if symbol in active_positions or symbol in pending_entry_orders.values():
            return False
    if daily_profit_lock_blocks_entries():
        record_trade_event(
            "entry_blocked",
            symbol=symbol,
            event_id=f"entry-blocked-daily-profit-lock:{symbol}",
            reason="daily profit lock active",
            daily_profit_lock=daily_trade_state.get("daily_profit_lock"),
        )
        return False
    if was_symbol_traded_today(symbol):
        record_trade_event("entry_skipped", symbol=symbol, event_id=f"entry-skipped-already-traded:{symbol}", reason="symbol already traded today")
        return False

    option_type = "CALL" if setup.bias.direction == "bullish" else "PUT"
    setup_type = entry_metadata["setup_type"]
    signal_name = entry_metadata["signal_name"]
    signal_price = entry_metadata.get("signal_price")
    signal_label = entry_metadata.get("signal_label", signal_name)
    stop_underlying = float(entry_metadata["stop_underlying"])
    signal_time = entry_metadata.get("signal_time")
    stop_mode = entry_metadata.get("stop_mode", "underlying_sweep_extreme")
    position_fields = dict(entry_metadata.get("position_fields") or {})
    event_fields = dict(entry_metadata.get("event_fields") or {})

    contract = build_fresh_entry_contract(symbol, option_type)
    preflight = validate_entry_contract(contract, require_market_open=True)
    contract["entry_preflight"] = preflight
    publish_contract_preview(symbol, contract, "entry_preflight")
    if not preflight.get("ok"):
        LOGGER.warning("Skipping %s: Alpaca entry preflight failed: %s", symbol, "; ".join(preflight.get("blocking") or []))
        record_trade_event(
            "entry_blocked",
            symbol=symbol,
            option_symbol=contract.get("symbol"),
            option_type=option_type,
            reason="Alpaca entry preflight failed",
            blocking=preflight.get("blocking"),
            warnings=preflight.get("warnings"),
        )
        return False
    if contract.get("status") != "ready" or contract.get("quantity", 0) < 1:
        LOGGER.info("Skipping %s: no usable %s contract preview. status=%s reason=%s", symbol, option_type, contract.get("status"), contract.get("reason"))
        record_trade_event(
            "entry_skipped",
            symbol=symbol,
            option_symbol=contract.get("symbol"),
            option_type=option_type,
            reason=contract.get("reason"),
            status=contract.get("status"),
        )
        return False

    qty = to_int_qty(contract.get("quantity", 0))
    signal_text = f"{signal_name} {signal_price:.2f}" if signal_price is not None else signal_name
    LOGGER.info(
        "ENTER %s %s via %s (%s) entry close %.2f OR high/low %.2f/%.2f: %sx %s ask=%.2f delta=%.4f gamma=%s theta=%s exp=%s account_balance=%.2f allocation=%.2f",
        symbol,
        option_type,
        signal_label,
        signal_text,
        signal_bar["close"],
        position_fields.get("opening_range_high") or 0.0,
        position_fields.get("opening_range_low") or 0.0,
        qty,
        contract["symbol"],
        contract["ask"],
        contract["delta"],
        contract.get("gamma"),
        contract.get("theta"),
        contract["expiration"],
        contract.get("account_balance", 0.0),
        contract.get("allocation_amount", 0.0),
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
        record_trade_event(
            "entry_error",
            symbol=symbol,
            option_symbol=contract["symbol"],
            option_type=option_type,
            qty=qty,
            reason=str(exc),
        )
        return False

    order_id = str(order.id)
    with STATE_LOCK:
        pending_entry_orders[order_id] = symbol
        active_positions[symbol] = {
            "managed": True,
            "symbol": symbol,
            "setup_type": setup_type,
            "option_symbol": contract["symbol"],
            "option_type": option_type,
            "entry_underlying": signal_bar["close"],
            "initial_stop_underlying": stop_underlying,
            "stop_underlying": stop_underlying,
            "stop_mode": stop_mode,
            "target_underlying": None,
            "target_name": "option_100pct",
            "target_exit_method": "market_on_option_gain",
            "target_r_multiple": None,
            "breakeven_active": False,
            "breakeven_stop_option_price": contract.get("ask"),
            "profit_lock_start_pct": 0.25,
            "profit_lock_step_pct": 0.25,
            "profit_lock_pct": None,
            "profit_lock_option_price": None,
            "profit_target_pct": 1.0,
            "range_reentry_close_count": 0,
            "swept_level": signal_name,
            "swept_level_price": signal_price,
            "signal_name": signal_name,
            "signal_price": signal_price,
            "entry_order_id": order_id,
            "entry_status": "submitted",
            "entry_option_delta": contract.get("delta"),
            "entry_option_gamma": contract.get("gamma"),
            "entry_option_theta": contract.get("theta"),
            "entry_option_ask": contract.get("ask"),
            "entry_contract_cost": contract.get("contract_cost"),
            "entry_account_balance": contract.get("account_balance"),
            "entry_allocation_amount": contract.get("allocation_amount"),
            "entry_preflight": preflight,
            "total_qty": 0,
            "requested_qty": qty,
            "signal_time": signal_time.isoformat() if hasattr(signal_time, "isoformat") else signal_time,
            "entry_submitted_at": datetime.now(UTC).isoformat(),
            **position_fields,
        }
        mark_symbol_traded(symbol)
        record_trade_event_locked(
            "entry_submitted",
            symbol=symbol,
            event_id=f"entry-submitted:{order_id}",
            order_id=order_id,
            setup_type=setup_type,
            option_symbol=contract["symbol"],
            option_type=option_type,
            qty=qty,
            side="buy",
            order_type="market",
            entry_underlying=signal_bar["close"],
            stop_underlying=stop_underlying,
            swept_level=signal_name,
            swept_level_price=signal_price,
            signal_name=signal_name,
            signal_price=signal_price,
            signal_time=signal_time,
            ask=contract.get("ask"),
            delta=contract.get("delta"),
            preflight_ok=preflight.get("ok"),
            **event_fields,
        )
        persist_state_locked()

    LOGGER.info("Submitted entry order %s for %sx %s", order_id, qty, contract["symbol"])
    return True


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
        record_trade_event(
            "exit_error",
            symbol=symbol,
            option_symbol=option_symbol,
            qty=exit_qty,
            side="sell",
            reason=reason,
            error=str(exc),
        )
        return False

    order_id = str(order.id)
    with STATE_LOCK:
        pending_exit_orders[order_id] = {"symbol": symbol, "qty": exit_qty, "filled_qty": 0, "reason": reason}
        record_trade_event_locked(
            "exit_submitted",
            symbol=symbol,
            event_id=f"exit-submitted:{order_id}",
            order_id=order_id,
            option_symbol=option_symbol,
            qty=exit_qty,
            side="sell",
            order_type="market",
            reason=reason,
        )
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
                filled_avg_price = get_value(order, "filled_avg_price")
                if filled_avg_price not in (None, ""):
                    position["entry_option_fill_price"] = float(filled_avg_price)
                    if not position.get("breakeven_active"):
                        position["breakeven_stop_option_price"] = float(filled_avg_price)
                record_trade_event_locked(
                    "entry_fill",
                    symbol=symbol,
                    event_id=f"entry-fill:{order_id}:{event}:{filled_qty}",
                    order_id=order_id,
                    option_symbol=position.get("option_symbol"),
                    qty=filled_qty,
                    filled_avg_price=get_value(order, "filled_avg_price"),
                    status=get_value(order, "status"),
                    event=event,
                )

        if event == "fill":
            position["entry_status"] = "filled"
            pending_entry_orders.pop(order_id, None)

        if event in {"canceled", "expired", "rejected"}:
            record_trade_event_locked(
                "entry_order_closed",
                symbol=symbol,
                event_id=f"entry-closed:{order_id}:{event}",
                order_id=order_id,
                option_symbol=position.get("option_symbol"),
                status=get_value(order, "status"),
                event=event,
            )
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
            option_symbol = position.get("option_symbol")
            exit_reason = order_state.get("reason")
            filled_avg_price = get_value(order, "filled_avg_price")
            position["total_qty"] = max(position["total_qty"] - filled_delta, 0)
            order_state["filled_qty"] = filled_qty
            if exit_reason == f"PARTIAL_{BREAKEVEN_TRIGGER_R_MULTIPLE:.2f}R":
                position["partial_exit_filled_at"] = datetime.now(UTC).isoformat()
                if filled_avg_price not in (None, ""):
                    position["partial_exit_fill_price"] = float(filled_avg_price)
                    update_runner_profit_floor(position, filled_avg_price)
            elif exit_reason == f"PARTIAL_{SECOND_PARTIAL_EXIT_R_MULTIPLE:.2f}R":
                position["second_partial_filled_at"] = datetime.now(UTC).isoformat()
                position["second_partial_taken"] = True
                if filled_avg_price not in (None, ""):
                    position["second_partial_fill_price"] = float(filled_avg_price)
            record_trade_event_locked(
                "exit_fill",
                symbol=symbol,
                event_id=f"exit-fill:{order_id}:{event}:{filled_qty}",
                order_id=order_id,
                option_symbol=option_symbol,
                qty=filled_delta,
                filled_qty=filled_qty,
                remaining_qty=position["total_qty"],
                filled_avg_price=filled_avg_price,
                reason=exit_reason,
                status=get_value(order, "status"),
                event=event,
            )

        if position["total_qty"] <= 0:
            active_positions.pop(symbol, None)

        if event in {"fill", "canceled", "expired", "rejected"}:
            record_trade_event_locked(
                "exit_order_closed",
                symbol=symbol,
                event_id=f"exit-closed:{order_id}:{event}",
                order_id=order_id,
                option_symbol=get_value(order, "symbol"),
                status=get_value(order, "status"),
                reason=order_state.get("reason"),
                event=event,
            )
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
        record_trade_event_locked(
            "broker_unmanaged_position",
            symbol=placeholder_key,
            event_id=f"broker-unmanaged:{option_symbol}",
            option_symbol=option_symbol,
            qty=quantity,
            reason="Alpaca position found without matching local strategy state",
        )
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
                record_trade_event_locked(
                    "reconcile_dropped_position",
                    symbol=symbol,
                    event_id=f"reconcile-dropped:{symbol}:{position.get('option_symbol')}",
                    option_symbol=position.get("option_symbol"),
                    reason="Alpaca has no open option position",
                )
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
    return ENTRY_WINDOW_START <= close_clock < ENTRY_WINDOW_END


def process_completed_five_minute_bar(symbol, bar):
    refresh_contract_previews_if_needed(reason="five_minute_bar")
    if bar.get("bucket") and bar["bucket"].time() == REGULAR_OPEN:
        opening_ranges[symbol] = {
            "high": round(float(bar["high"]), 4),
            "low": round(float(bar["low"]), 4),
            "completed": True,
            "completed_at": bar.get("close_time"),
        }
        refresh_intraday_decisions(bar.get("close_time") or datetime.now(ET))
        LOGGER.info("%s opening 5m range set %.2f-%.2f", symbol, opening_ranges[symbol]["low"], opening_ranges[symbol]["high"])


def process_orb_entry_signal(symbol, minute_bar):
    if daily_profit_lock_blocks_entries():
        return
    orb = opening_ranges.get(symbol)
    if not orb or not orb.get("completed"):
        return
    timestamp = minute_bar.get("timestamp")
    if not timestamp or timestamp.time() < ENTRY_WINDOW_START or timestamp.time() >= ENTRY_WINDOW_END:
        return
    completed_at = orb.get("completed_at")
    if completed_at and timestamp < completed_at:
        return

    setup = current_setup(symbol)
    if not setup or was_symbol_traded_today(symbol):
        return
    with STATE_LOCK:
        if symbol in active_positions or symbol in pending_entry_orders.values():
            return

    if setup.bias.direction == "bullish":
        triggered = minute_bar["close"] > orb["high"]
    else:
        triggered = minute_bar["close"] < orb["low"]
    if triggered:
        execute_entry(symbol, setup, minute_bar, orb_entry_metadata(setup, minute_bar))


def breakeven_trigger_hit(option_type, minute_bar, trigger_underlying):
    if trigger_underlying in (None, ""):
        return False
    trigger_underlying = float(trigger_underlying)
    if option_type == "CALL":
        return minute_bar["high"] >= trigger_underlying
    return minute_bar["low"] <= trigger_underlying


def parse_event_time(value):
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def runner_profit_floor_price(position, reference_option_price=None):
    candidates = []
    entry_price = position.get("entry_option_fill_price") or position.get("breakeven_stop_option_price") or position.get("entry_option_ask")
    if entry_price not in (None, "") and RUNNER_PROFIT_FLOOR_ENTRY_MULTIPLE > 0:
        candidates.append(float(entry_price) * RUNNER_PROFIT_FLOOR_ENTRY_MULTIPLE)
    if reference_option_price not in (None, "") and RUNNER_PROFIT_FLOOR_PARTIAL_MULTIPLE > 0:
        candidates.append(float(reference_option_price) * RUNNER_PROFIT_FLOOR_PARTIAL_MULTIPLE)
    if not candidates:
        return None
    return round(max(candidates), 4)


def update_runner_profit_floor(position, reference_option_price=None):
    floor_price = runner_profit_floor_price(position, reference_option_price)
    if floor_price is None:
        return None
    current_floor = position.get("runner_profit_floor_option_price")
    if current_floor not in (None, ""):
        floor_price = max(float(current_floor), floor_price)
    position["runner_profit_floor_option_price"] = floor_price
    position["stop_mode"] = "option_profit_floor"
    return floor_price


def position_r_multiple(position, underlying_price):
    risk = float(position.get("risk_underlying") or 0.0)
    if risk <= 0 or underlying_price in (None, ""):
        return 0.0
    entry = float(position.get("entry_underlying") or 0.0)
    if position.get("option_type") == "CALL":
        return (float(underlying_price) - entry) / risk
    return (entry - float(underlying_price)) / risk


def favorable_bar_r_multiple(position, minute_bar):
    if position.get("option_type") == "CALL":
        return position_r_multiple(position, minute_bar.get("high"))
    return position_r_multiple(position, minute_bar.get("low"))


def close_bar_r_multiple(position, minute_bar):
    return position_r_multiple(position, minute_bar.get("close"))


def runner_management_ready(position):
    total_qty = to_int_qty(position.get("total_qty", 0))
    requested_qty = to_int_qty(position.get("requested_qty", total_qty))
    return bool(
        position.get("partial_exit_filled_at")
        or position.get("partial_exit_skipped_reason")
        or (requested_qty > 0 and total_qty < requested_qty)
    )


def update_runner_progress(symbol, minute_bar):
    with STATE_LOCK:
        position = active_positions.get(symbol)
        if not position:
            return None
        favorable_r = favorable_bar_r_multiple(position, minute_bar)
        prior_max_r = float(position.get("runner_max_r_multiple") or 0.0)
        max_r = max(prior_max_r, favorable_r)
        changed = max_r != prior_max_r
        position["runner_max_r_multiple"] = round(max_r, 4)
        if max_r >= RUNNER_EXTENSION_R_MULTIPLE and not position.get("runner_extension_reached"):
            position["runner_extension_reached"] = True
            timestamp = minute_bar.get("timestamp")
            position["runner_extension_reached_at"] = timestamp.isoformat() if hasattr(timestamp, "isoformat") else timestamp
            changed = True
        if changed:
            persist_state_locked()
        return dict(position)


def planned_second_partial_exit_qty(total_qty, original_qty):
    total_qty = to_int_qty(total_qty)
    original_qty = to_int_qty(original_qty)
    if total_qty <= 1 or original_qty <= 1 or SECOND_PARTIAL_EXIT_PCT <= 0:
        return 0
    requested_qty = int(original_qty * SECOND_PARTIAL_EXIT_PCT)
    if requested_qty <= 0:
        requested_qty = 1
    return min(requested_qty, total_qty - 1)


def request_second_partial_exit(symbol, total_qty):
    with STATE_LOCK:
        position = active_positions.get(symbol)
        original_qty = to_int_qty(position.get("requested_qty", total_qty)) if position else total_qty
    exit_qty = planned_second_partial_exit_qty(total_qty, original_qty)
    if exit_qty <= 0:
        return False
    if not execute_exit(symbol, exit_qty, f"PARTIAL_{SECOND_PARTIAL_EXIT_R_MULTIPLE:.2f}R"):
        return False

    requested_at = datetime.now(UTC).isoformat()
    with STATE_LOCK:
        position = active_positions.get(symbol)
        if position:
            position["second_partial_taken"] = True
            position["second_partial_requested_at"] = requested_at
            position["second_partial_qty_requested"] = exit_qty
            record_trade_event_locked(
                "second_partial_exit_requested",
                symbol=symbol,
                event_id=f"second-partial-exit-requested:{symbol}:{requested_at}",
                option_symbol=position.get("option_symbol"),
                qty=exit_qty,
                remaining_qty=max(to_int_qty(position.get("total_qty", 0)) - exit_qty, 0),
                second_partial_exit_pct=SECOND_PARTIAL_EXIT_PCT,
                second_partial_trigger_r_multiple=SECOND_PARTIAL_EXIT_R_MULTIPLE,
            )
            persist_state_locked()
    return True


def runner_stall_exit_hit(position, minute_bar):
    if RUNNER_STALL_MINUTES <= 0 or not runner_management_ready(position):
        return False
    if position.get("runner_extension_reached"):
        return False
    partial_time = parse_event_time(position.get("partial_exit_filled_at") or position.get("partial_exit_requested_at"))
    bar_time = parse_event_time(minute_bar.get("timestamp") or minute_bar.get("close_time"))
    if not partial_time or not bar_time:
        return False
    elapsed_minutes = (bar_time - partial_time).total_seconds() / 60.0
    if elapsed_minutes < RUNNER_STALL_MINUTES:
        return False
    return close_bar_r_multiple(position, minute_bar) <= RUNNER_STALL_R_MULTIPLE


def activate_breakeven_stop(symbol):
    with STATE_LOCK:
        position = active_positions.get(symbol)
        if not position or position.get("breakeven_active"):
            return
        position["breakeven_active"] = True
        runner_floor = update_runner_profit_floor(position)
        position["breakeven_activated_at"] = datetime.now(UTC).isoformat()
        record_trade_event_locked(
            "breakeven_activated",
            symbol=symbol,
            event_id=f"breakeven:{symbol}:{position.get('breakeven_activated_at')}",
            option_symbol=position.get("option_symbol"),
            breakeven_stop_option_price=position.get("breakeven_stop_option_price"),
            runner_profit_floor_option_price=runner_floor,
            breakeven_trigger_underlying=position.get("breakeven_trigger_underlying"),
        )
        persist_state_locked()
    LOGGER.info("%s runner profit floor activated at option price %.2f", symbol, float(position.get("runner_profit_floor_option_price") or 0.0))


def planned_partial_exit_qty(total_qty):
    total_qty = to_int_qty(total_qty)
    if total_qty <= 1 or PARTIAL_EXIT_PCT <= 0:
        return 0
    requested_qty = int(total_qty * PARTIAL_EXIT_PCT)
    if requested_qty <= 0:
        requested_qty = 1
    return min(requested_qty, total_qty - 1)


def mark_partial_exit_unavailable(symbol, reason):
    requested_at = datetime.now(UTC).isoformat()
    with STATE_LOCK:
        position = active_positions.get(symbol)
        if not position:
            return
        position["partial_exit_taken"] = True
        position["partial_exit_skipped_reason"] = reason
        position["partial_exit_requested_at"] = requested_at
        record_trade_event_locked(
            "partial_exit_skipped",
            symbol=symbol,
            event_id=f"partial-exit-skipped:{symbol}:{requested_at}",
            option_symbol=position.get("option_symbol"),
            qty=position.get("total_qty"),
            reason=reason,
            partial_exit_pct=PARTIAL_EXIT_PCT,
            partial_exit_trigger_r_multiple=BREAKEVEN_TRIGGER_R_MULTIPLE,
        )
        persist_state_locked()


def request_partial_exit(symbol, total_qty):
    exit_qty = planned_partial_exit_qty(total_qty)
    if exit_qty <= 0:
        mark_partial_exit_unavailable(symbol, "quantity below minimum runner size")
        return False

    if not execute_exit(symbol, exit_qty, f"PARTIAL_{BREAKEVEN_TRIGGER_R_MULTIPLE:.2f}R"):
        return False

    requested_at = datetime.now(UTC).isoformat()
    with STATE_LOCK:
        position = active_positions.get(symbol)
        if position:
            position["partial_exit_taken"] = True
            position["partial_exit_requested_at"] = requested_at
            position["partial_exit_qty_requested"] = exit_qty
            record_trade_event_locked(
                "partial_exit_requested",
                symbol=symbol,
                event_id=f"partial-exit-requested:{symbol}:{requested_at}",
                option_symbol=position.get("option_symbol"),
                qty=exit_qty,
                remaining_qty=max(to_int_qty(position.get("total_qty", 0)) - exit_qty, 0),
                partial_exit_pct=PARTIAL_EXIT_PCT,
                partial_exit_trigger_r_multiple=BREAKEVEN_TRIGGER_R_MULTIPLE,
                partial_exit_trigger_underlying=position.get("partial_exit_trigger_underlying"),
            )
            persist_state_locked()
    return True


def option_profit_stop_hit(symbol, option_symbol, stop_price, stop_label):
    if stop_price in (None, ""):
        return False
    try:
        snapshot = option_market_snapshot(option_symbol)
    except Exception as exc:
        LOGGER.warning("Unable to fetch option snapshot for %s %s stop: %s", option_symbol, stop_label, exc)
        return False

    market_price = snapshot.get("market_price")
    if market_price is None:
        LOGGER.warning("Unable to evaluate %s stop for %s: option market price unavailable", stop_label, option_symbol)
        return False

    with STATE_LOCK:
        position = active_positions.get(symbol)
        if position:
            position["latest_option_market_price"] = market_price
            position["latest_option_bid"] = snapshot.get("bid")
            position["latest_option_ask"] = snapshot.get("ask")
            position["latest_option_quote_time"] = snapshot.get("quote_time")
            persist_state_locked()

    return float(market_price) <= float(stop_price)


def option_breakeven_stop_hit(symbol, option_symbol, breakeven_price):
    return option_profit_stop_hit(symbol, option_symbol, breakeven_price, "breakeven")


def entry_option_price(position):
    value = position.get("entry_option_fill_price") or position.get("breakeven_stop_option_price") or position.get("entry_option_ask")
    if value in (None, ""):
        return None
    return float(value)


def current_option_market_price(symbol, option_symbol):
    try:
        snapshot = option_market_snapshot(option_symbol)
    except Exception as exc:
        LOGGER.warning("Unable to fetch option snapshot for %s pre-breakeven risk: %s", option_symbol, exc)
        return None

    market_price = snapshot.get("market_price")
    if market_price is None:
        LOGGER.warning("Unable to evaluate pre-breakeven risk for %s: option market price unavailable", option_symbol)
        return None

    with STATE_LOCK:
        position = active_positions.get(symbol)
        if position:
            position["latest_option_market_price"] = market_price
            position["latest_option_bid"] = snapshot.get("bid")
            position["latest_option_ask"] = snapshot.get("ask")
            position["latest_option_quote_time"] = snapshot.get("quote_time")
            persist_state_locked()
    return float(market_price)


def no_progress_exit_hit(position, minute_bar, entry_price, market_price):
    if not NO_PROGRESS_EXIT_ENABLED or NO_PROGRESS_MINUTES <= 0:
        return False
    entry_time = parse_event_time(position.get("entry_filled_at") or position.get("entry_submitted_at") or position.get("signal_time"))
    bar_time = parse_event_time(minute_bar.get("timestamp") or minute_bar.get("close_time"))
    if not entry_time or not bar_time:
        return False
    elapsed_minutes = (bar_time - entry_time).total_seconds() / 60.0
    if elapsed_minutes < NO_PROGRESS_MINUTES:
        return False
    gain_pct = (market_price - entry_price) / entry_price
    with STATE_LOCK:
        active = active_positions.get(position.get("symbol"))
        if active:
            active["pre_breakeven_no_progress_elapsed_minutes"] = round(elapsed_minutes, 2)
            active["pre_breakeven_no_progress_gain_pct"] = round(gain_pct, 4)
            persist_state_locked()
    return gain_pct <= NO_PROGRESS_MIN_OPTION_GAIN_PCT


def manage_pre_breakeven_option_risk(symbol, position, minute_bar):
    option_symbol = position.get("option_symbol")
    if not option_symbol:
        return False
    entry_price = entry_option_price(position)
    if entry_price is None or entry_price <= 0:
        return False

    market_price = current_option_market_price(symbol, option_symbol)
    if market_price is None:
        return False

    floor_price = None
    with STATE_LOCK:
        active = active_positions.get(symbol)
        if active:
            peak_price = max(float(active.get("pre_breakeven_peak_option_price") or 0.0), market_price)
            active["pre_breakeven_peak_option_price"] = round(peak_price, 4)
            floor_price = active.get("pre_breakeven_option_floor_price")
            if floor_price not in (None, ""):
                floor_price = float(floor_price)
            if (
                PRE_BREAKEVEN_OPTION_LOCK_ENABLED
                and PRE_BREAKEVEN_OPTION_LOCK_TRIGGER_PCT > 0
                and market_price >= entry_price * (1.0 + PRE_BREAKEVEN_OPTION_LOCK_TRIGGER_PCT)
            ):
                next_floor = round(entry_price * (1.0 + PRE_BREAKEVEN_OPTION_FLOOR_PCT), 4)
                if floor_price is None or next_floor > floor_price:
                    active["pre_breakeven_option_floor_price"] = next_floor
                    active["pre_breakeven_option_floor_armed_at"] = datetime.now(UTC).isoformat()
                    active["stop_mode"] = "pre_breakeven_option_floor"
                    floor_price = next_floor
                    record_trade_event_locked(
                        "pre_breakeven_option_floor_armed",
                        symbol=symbol,
                        event_id=f"pre-breakeven-floor:{symbol}:{active['pre_breakeven_option_floor_armed_at']}",
                        option_symbol=option_symbol,
                        entry_option_price=entry_price,
                        market_price=market_price,
                        floor_price=floor_price,
                        trigger_pct=PRE_BREAKEVEN_OPTION_LOCK_TRIGGER_PCT,
                        floor_pct=PRE_BREAKEVEN_OPTION_FLOOR_PCT,
                    )
            persist_state_locked()

    if floor_price is not None and market_price <= floor_price:
        execute_exit(symbol, to_int_qty(position.get("total_qty", 0)), "STOP_PRE_BREAKEVEN_OPTION_FLOOR")
        return True

    if PRE_BREAKEVEN_OPTION_HARD_STOP_LOSS_PCT > 0:
        hard_stop = entry_price * (1.0 - PRE_BREAKEVEN_OPTION_HARD_STOP_LOSS_PCT)
        if market_price <= hard_stop:
            execute_exit(symbol, to_int_qty(position.get("total_qty", 0)), "STOP_PRE_BREAKEVEN_OPTION_LOSS")
            return True

    if no_progress_exit_hit(position, minute_bar, entry_price, market_price):
        execute_exit(symbol, to_int_qty(position.get("total_qty", 0)), "STOP_PRE_BREAKEVEN_NO_PROGRESS")
        return True
    return False


def account_equity_snapshot():
    try:
        account = trade_client.get_account()
    except Exception as exc:
        LOGGER.warning("Unable to load Alpaca account for daily profit lock: %s", exc)
        return None

    current = None
    for field in ("portfolio_value", "equity", "cash"):
        value = get_value(account, field)
        if value not in (None, "") and float(value) > 0:
            current = float(value)
            break
    if current is None:
        return None

    baseline_value = get_value(account, "last_equity")
    baseline = float(baseline_value) if baseline_value not in (None, "") and float(baseline_value) > 0 else current
    return {"current_equity": current, "baseline_equity": baseline}


def refresh_daily_profit_lock(now=None, flatten_on_trigger=False):
    now = now or datetime.now(ET)
    with STATE_LOCK:
        state = daily_trade_state.setdefault("daily_profit_lock", {})
        if not DAILY_PROFIT_LOCK_ENABLED:
            state["enabled"] = False
            persist_state_locked()
            return state.copy()

        session = now.date().isoformat()
        if state.get("session") != session:
            state.clear()
            state.update({"enabled": True, "session": session})
            persist_state_locked()

    snapshot = account_equity_snapshot()
    if snapshot is None:
        with STATE_LOCK:
            return dict(daily_trade_state.setdefault("daily_profit_lock", {}))

    current_equity = snapshot["current_equity"]
    triggered_now = False
    with STATE_LOCK:
        state = daily_trade_state.setdefault("daily_profit_lock", {})
        day_open_equity = float(state.get("day_open_equity") or 0.0)
        if day_open_equity <= 0:
            day_open_equity = snapshot["baseline_equity"] or current_equity
            state["day_open_equity"] = round(day_open_equity, 2)

        peak_equity = max(float(state.get("peak_equity") or day_open_equity), current_equity)
        state["peak_equity"] = round(peak_equity, 2)
        state["latest_equity"] = round(current_equity, 2)
        state["latest_checked_at"] = now.isoformat()
        latest_gain_pct = (current_equity - day_open_equity) / day_open_equity
        peak_gain_pct = (peak_equity - day_open_equity) / day_open_equity
        state["latest_gain_pct"] = round(latest_gain_pct, 5)
        state["peak_gain_pct"] = round(peak_gain_pct, 5)

        if peak_gain_pct >= DAILY_PROFIT_LOCK_TRIGGER_PCT:
            if not state.get("active"):
                state["active"] = True
                state["activated_at"] = now.isoformat()
                record_trade_event_locked(
                    "daily_profit_lock_activated",
                    symbol="*",
                    event_id=f"daily-profit-lock-activated:{now.isoformat()}",
                    day_open_equity=day_open_equity,
                    peak_equity=peak_equity,
                    peak_gain_pct=peak_gain_pct,
                    trigger_pct=DAILY_PROFIT_LOCK_TRIGGER_PCT,
                )
            floor_by_pct = day_open_equity * (1.0 + DAILY_PROFIT_LOCK_FLOOR_PCT)
            floor_by_drawdown = peak_equity - (day_open_equity * DAILY_PROFIT_LOCK_DRAWDOWN_PCT)
            lock_floor = max(floor_by_pct, floor_by_drawdown)
            state["lock_floor_equity"] = round(lock_floor, 2)
            state["lock_floor_gain_pct"] = round((lock_floor - day_open_equity) / day_open_equity, 5)
            state["max_drawdown_pct"] = DAILY_PROFIT_LOCK_DRAWDOWN_PCT

            if current_equity <= lock_floor and not state.get("triggered"):
                state["triggered"] = True
                state["triggered_at"] = now.isoformat()
                state["trigger_reason"] = "daily equity gave back to profit-lock floor"
                triggered_now = True
                record_trade_event_locked(
                    "daily_profit_lock_triggered",
                    symbol="*",
                    event_id=f"daily-profit-lock-triggered:{now.isoformat()}",
                    day_open_equity=day_open_equity,
                    peak_equity=peak_equity,
                    current_equity=current_equity,
                    lock_floor_equity=lock_floor,
                    latest_gain_pct=latest_gain_pct,
                    peak_gain_pct=peak_gain_pct,
                )
        persist_state_locked()
        result = dict(state)

    if flatten_on_trigger and (triggered_now or result.get("triggered")):
        flatten_for_daily_profit_lock()
    return result


def daily_profit_lock_blocks_entries():
    if not DAILY_PROFIT_LOCK_ENABLED:
        return False
    with STATE_LOCK:
        state = daily_trade_state.get("daily_profit_lock") or {}
        return bool(state.get("triggered") or (DAILY_PROFIT_LOCK_BLOCKS_NEW_ENTRIES and state.get("active")))


def daily_profit_lock_triggered():
    with STATE_LOCK:
        return bool((daily_trade_state.get("daily_profit_lock") or {}).get("triggered"))


def flatten_for_daily_profit_lock():
    with STATE_LOCK:
        positions = [
            (symbol, to_int_qty(position.get("total_qty", 0)))
            for symbol, position in active_positions.items()
            if position.get("managed", True) and to_int_qty(position.get("total_qty", 0)) > 0
        ]
    for symbol, qty in positions:
        execute_exit(symbol, qty, "DAILY_PROFIT_LOCK")


def manage_open_position_with_bar(symbol, minute_bar):
    with STATE_LOCK:
        position = active_positions.get(symbol)
        if not position or not position.get("managed", True):
            return
        is_live_position = position["entry_status"] in {"partial_fill", "filled"} and position["total_qty"] > 0
        if not is_live_position:
            return
        total_qty = position["total_qty"]
        option_symbol = position["option_symbol"]

    opening_range_high = position.get("opening_range_high")
    opening_range_low = position.get("opening_range_low")
    if opening_range_high not in (None, "") and opening_range_low not in (None, ""):
        inside_range = float(opening_range_low) <= float(minute_bar["close"]) <= float(opening_range_high)
        with STATE_LOCK:
            latest = active_positions.get(symbol)
            if latest:
                latest["range_reentry_close_count"] = (int(latest.get("range_reentry_close_count") or 0) + 1) if inside_range else 0
                latest["last_underlying_close"] = minute_bar["close"]
                persist_state_locked()
                if int(latest.get("range_reentry_close_count") or 0) >= 3:
                    execute_exit(symbol, total_qty, "STOP_ORB_RANGE_REENTRY_3_CLOSES")
                    return

    entry_price = entry_option_price(position)
    market_price = current_option_market_price(symbol, option_symbol)
    if market_price is not None:
        with STATE_LOCK:
            latest = active_positions.get(symbol)
            if latest:
                prior_peak = float(latest.get("highest_option_market_price") or 0.0)
                latest["highest_option_market_price"] = round(max(prior_peak, market_price), 4)
                if entry_price not in (None, 0):
                    latest["latest_option_gain_pct"] = round((market_price - entry_price) / entry_price, 4)
                persist_state_locked()

    if entry_price and market_price is not None and entry_price > 0:
        gain_pct = (market_price - entry_price) / entry_price
        if gain_pct >= 1.0:
            execute_exit(symbol, total_qty, "TARGET_OPTION_100PCT")
            return

        profit_lock_pct = None
        if gain_pct >= 0.25:
            profit_lock_pct = max(0.0, int((gain_pct - 0.25) // 0.25) * 0.25)

        with STATE_LOCK:
            latest = active_positions.get(symbol)
            if latest and profit_lock_pct is not None:
                current_lock = latest.get("profit_lock_pct")
                if current_lock in (None, "") or profit_lock_pct > float(current_lock):
                    latest["profit_lock_pct"] = round(profit_lock_pct, 4)
                    latest["profit_lock_option_price"] = round(entry_price * (1.0 + profit_lock_pct), 4)
                    latest["breakeven_active"] = profit_lock_pct <= 0.0
                    record_trade_event_locked(
                        "profit_lock_advanced",
                        symbol=symbol,
                        event_id=f"profit-lock:{symbol}:{minute_bar['timestamp'].isoformat()}:{profit_lock_pct:.2f}",
                        option_symbol=option_symbol,
                        gain_pct=round(gain_pct, 4),
                        profit_lock_pct=round(profit_lock_pct, 4),
                        profit_lock_option_price=latest["profit_lock_option_price"],
                    )
                    persist_state_locked()

        with STATE_LOCK:
            latest = active_positions.get(symbol)
            lock_price = latest.get("profit_lock_option_price") if latest else None
            lock_pct = latest.get("profit_lock_pct") if latest else None
        if lock_price not in (None, "") and market_price <= float(lock_price):
            lock_label = "BE" if float(lock_pct or 0.0) <= 0.0 else f"{int(float(lock_pct) * 100)}PCT"
            execute_exit(symbol, total_qty, f"STOP_OPTION_TRAIL_{lock_label}")
            return
    if minute_bar["timestamp"].time() >= EOD_EXIT_TIME:
        execute_exit(symbol, total_qty, "EOD_EXIT")


async def handle_bar(bar):
    symbol = bar.symbol
    timestamp_et = bar_timestamp_et(bar)
    prepare_daily_context(timestamp_et)
    refresh_daily_profit_lock(timestamp_et, flatten_on_trigger=True)
    if daily_profit_lock_triggered():
        return

    minute_bar = minute_bar_dict(bar, timestamp_et)
    manage_open_position_with_bar(symbol, minute_bar)

    completed = update_five_minute_bar(symbol, minute_bar)
    if completed:
        process_completed_five_minute_bar(symbol, completed)
    process_orb_entry_signal(symbol, minute_bar)


def start_trading_stream():
    trading_stream.subscribe_trade_updates(handle_trade_update)
    trading_stream.run()
