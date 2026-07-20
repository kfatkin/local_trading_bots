import math
from datetime import datetime, timedelta

from alpaca.data.requests import OptionBarsRequest, OptionSnapshotRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.enums import AssetStatus, ContractType
from alpaca.trading.requests import GetOptionContractsRequest

from .clients import option_client, raw_option_client, trade_client
from .config import (
    ET,
    LOGGER,
    OPTION_CANDIDATES_ABOVE_TARGET,
    OPTION_CANDIDATES_BELOW_TARGET,
    OPTION_EXPIRATION_LOOKAHEAD_DAYS,
    OPTION_MAX_ACCOUNT_BALANCE_PCT,
    OPTION_MAX_DELTA_DISTANCE,
    OPTION_MAX_QUOTE_AGE_SECONDS,
    OPTION_MAX_SPREAD_PCT,
    OPTION_MIN_OPEN_INTEREST,
    OPTION_MIN_VOLUME,
    TARGET_DELTA,
    TRADE_ALLOCATION_PCT,
    UTC,
)
from .utils import get_value, normalize_text, round_or_none


def optional_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def optional_int(value):
    number = optional_float(value)
    if number is None:
        return None
    return int(number)


def truthy(value):
    if isinstance(value, bool):
        return value
    return normalize_text(value).strip().lower() in {"1", "true", "yes", "on"}


def option_type_enum(option_type):
    return ContractType.CALL if option_type == "CALL" else ContractType.PUT


def iso_date(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return normalize_text(value) or None


def account_balance_summary(account=None):
    account = account or trade_client.get_account()
    balance_field = None
    balance = None
    for field in ("portfolio_value", "equity", "cash"):
        amount = optional_float(get_value(account, field))
        if amount is not None and amount > 0:
            balance_field = field
            balance = amount
            break
    if balance is None:
        balance_field = "buying_power"
        balance = optional_float(get_value(account, "buying_power")) or 0.0

    return {
        "account_balance": round(balance, 2),
        "account_balance_field": balance_field,
        "portfolio_value": round_or_none(get_value(account, "portfolio_value"), 2),
        "equity": round_or_none(get_value(account, "equity"), 2),
        "cash": round_or_none(get_value(account, "cash"), 2),
        "buying_power": round_or_none(get_value(account, "buying_power"), 2),
    }


def empty_contract_preview(symbol, option_type=None, status="not_planned", reason="No planned contract for this symbol", account=None):
    preview = {
        "status": status,
        "reason": reason,
        "symbol": None,
        "underlying_symbol": symbol,
        "option_type": option_type,
        "target_delta": TARGET_DELTA,
        "quantity": 0,
        "warnings": [],
        "candidates": [],
        "updated_at": datetime.now(ET).isoformat(),
    }
    if account:
        preview.update(account)
    return preview


def fetch_active_contracts(symbol, option_type, expiration=None):
    today = datetime.now(ET).date()
    expiration_lte = today + timedelta(days=OPTION_EXPIRATION_LOOKAHEAD_DAYS)
    contract_type = option_type_enum(option_type)
    contracts = []
    page_token = None

    while True:
        request = GetOptionContractsRequest(
            underlying_symbols=[symbol],
            status=AssetStatus.ACTIVE,
            type=contract_type,
            expiration_date_gte=expiration or today,
            expiration_date_lte=expiration or expiration_lte,
            limit=1000,
            page_token=page_token,
        )
        response = trade_client.get_option_contracts(request)
        contracts.extend(get_value(response, "option_contracts", []) or [])
        page_token = get_value(response, "next_page_token")
        if not page_token:
            break

    return [contract for contract in contracts if get_value(contract, "tradable", True)]


def quote_payload(snapshot):
    quote = get_value(snapshot, "latest_quote") or get_value(snapshot, "latestQuote")
    if not quote:
        return {}
    bid = optional_float(get_value(quote, "bid_price", get_value(quote, "bp")))
    ask = optional_float(get_value(quote, "ask_price", get_value(quote, "ap")))
    mid = round((bid + ask) / 2, 4) if bid is not None and ask is not None and bid > 0 and ask > 0 else None
    spread = round(ask - bid, 4) if bid is not None and ask is not None and ask >= bid else None
    spread_pct = round(spread / mid, 4) if spread is not None and mid else None
    return {
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
        "spread_pct": spread_pct,
        "bid_size": optional_int(get_value(quote, "bid_size", get_value(quote, "bs"))),
        "ask_size": optional_int(get_value(quote, "ask_size", get_value(quote, "as"))),
        "quote_time": timestamp_text(get_value(quote, "timestamp", get_value(quote, "t"))),
    }


def greeks_payload(snapshot):
    greeks = get_value(snapshot, "greeks")
    if not greeks:
        return {}
    delta = optional_float(get_value(greeks, "delta"))
    return {
        "delta": delta,
        "delta_abs": abs(delta) if delta is not None else None,
        "gamma": optional_float(get_value(greeks, "gamma")),
        "theta": optional_float(get_value(greeks, "theta")),
        "vega": optional_float(get_value(greeks, "vega")),
        "rho": optional_float(get_value(greeks, "rho")),
    }


def latest_trade_payload(snapshot):
    trade = get_value(snapshot, "latest_trade") or get_value(snapshot, "latestTrade")
    if not trade:
        return {}
    return {
        "last": optional_float(get_value(trade, "price", get_value(trade, "p"))),
        "last_trade_size": optional_int(get_value(trade, "size", get_value(trade, "s"))),
        "last_trade_time": timestamp_text(get_value(trade, "timestamp", get_value(trade, "t"))),
    }


def option_market_snapshot(option_symbol):
    snapshot = fetch_option_snapshots([option_symbol]).get(option_symbol)
    if not snapshot:
        return {"symbol": option_symbol, "market_price": None, "reason": "snapshot unavailable"}

    quote = quote_payload(snapshot)
    trade = latest_trade_payload(snapshot)
    market_price = quote.get("bid") or trade.get("last") or quote.get("mid")
    return {
        "symbol": option_symbol,
        "market_price": market_price,
        **quote,
        **trade,
    }


def timestamp_text(value):
    if not value:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return normalize_text(value) or None


def parse_timestamp(value):
    text = timestamp_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def quote_age_seconds(quote_time, now=None):
    parsed = parse_timestamp(quote_time)
    if not parsed:
        return None
    now = now or datetime.now(UTC)
    return max((now - parsed.astimezone(UTC)).total_seconds(), 0.0)


def daily_bar_payload(snapshot):
    bar = get_value(snapshot, "daily_bar") or get_value(snapshot, "dailyBar")
    if not bar:
        return {}
    timestamp = get_value(bar, "timestamp", get_value(bar, "t"))
    timestamp_value = timestamp_text(timestamp)
    return {
        "volume": optional_int(get_value(bar, "volume", get_value(bar, "v"))),
        "trade_count": optional_int(get_value(bar, "trade_count", get_value(bar, "n"))),
        "vwap": optional_float(get_value(bar, "vwap", get_value(bar, "vw"))),
        "volume_date": timestamp_value[:10] if timestamp_value else None,
    }


def candidate_from_snapshot(contract, snapshot, option_type):
    quote = quote_payload(snapshot)
    greeks = greeks_payload(snapshot)
    ask = quote.get("ask")
    delta_abs = greeks.get("delta_abs")
    expiration = get_value(contract, "expiration_date")
    strike = optional_float(get_value(contract, "strike_price"))
    if ask is None or ask <= 0 or delta_abs is None or expiration is None or strike is None:
        return None

    size = optional_float(get_value(contract, "size")) or 100.0
    candidate = {
        "symbol": get_value(contract, "symbol"),
        "underlying_symbol": get_value(contract, "underlying_symbol"),
        "option_type": option_type,
        "asset_status": normalize_text(get_value(contract, "status")),
        "tradable": bool(get_value(contract, "tradable", True)),
        "strike": strike,
        "expiration": iso_date(expiration),
        "contract_size": int(size),
        "open_interest": optional_int(get_value(contract, "open_interest")),
        "open_interest_date": iso_date(get_value(contract, "open_interest_date")),
        "implied_volatility": optional_float(get_value(snapshot, "implied_volatility", get_value(snapshot, "impliedVolatility"))),
        "delta_distance": abs(TARGET_DELTA - delta_abs),
        "price": ask,
        "contract_cost": round(ask * size, 2),
    }
    candidate.update(quote)
    candidate.update(greeks)
    candidate.update(latest_trade_payload(snapshot))
    candidate.update(daily_bar_payload(snapshot))
    return candidate


def candidate_delta_band(candidates):
    near_target = [candidate for candidate in candidates if candidate["delta_distance"] <= OPTION_MAX_DELTA_DISTANCE]
    scoped_candidates = near_target or candidates
    below = sorted(
        [candidate for candidate in scoped_candidates if candidate["delta_abs"] <= TARGET_DELTA],
        key=lambda candidate: TARGET_DELTA - candidate["delta_abs"],
    )[:OPTION_CANDIDATES_BELOW_TARGET]
    above = sorted(
        [candidate for candidate in scoped_candidates if candidate["delta_abs"] > TARGET_DELTA],
        key=lambda candidate: candidate["delta_abs"] - TARGET_DELTA,
    )[:OPTION_CANDIDATES_ABOVE_TARGET]
    band = below + above
    if band:
        return band
    return sorted(scoped_candidates, key=lambda candidate: candidate["delta_distance"])[: OPTION_CANDIDATES_BELOW_TARGET + OPTION_CANDIDATES_ABOVE_TARGET]


def fetch_recent_option_volumes(option_symbols):
    if not option_symbols:
        return {}
    start = datetime.now(ET) - timedelta(days=10)
    try:
        bars = option_client.get_option_bars(OptionBarsRequest(symbol_or_symbols=list(option_symbols), timeframe=TimeFrame.Day, start=start))
    except Exception as exc:
        LOGGER.warning("Unable to load option volume bars for contract preview: %s", exc)
        return {}

    volumes = {}
    for option_symbol, symbol_bars in (get_value(bars, "data", {}) or {}).items():
        if not symbol_bars:
            continue
        latest_bar = symbol_bars[-1]
        timestamp = get_value(latest_bar, "timestamp")
        volumes[option_symbol] = {
            "volume": optional_int(get_value(latest_bar, "volume")),
            "trade_count": optional_int(get_value(latest_bar, "trade_count")),
            "vwap": optional_float(get_value(latest_bar, "vwap")),
            "volume_date": timestamp.date().isoformat() if timestamp else None,
        }
    return volumes


def fetch_option_snapshots(option_symbols):
    snapshots = {}
    symbols = list(option_symbols)
    for start in range(0, len(symbols), 100):
        chunk = symbols[start : start + 100]
        snapshots.update(raw_option_client.get_option_snapshot(OptionSnapshotRequest(symbol_or_symbols=chunk)))
    return snapshots


def apply_liquidity_checks(candidate):
    warnings = []
    blocking = []
    bid = candidate.get("bid")
    ask = candidate.get("ask")
    spread_pct = candidate.get("spread_pct")
    volume = candidate.get("volume")
    open_interest = candidate.get("open_interest")

    if bid is None or bid <= 0:
        blocking.append("no bid")
    if ask is None or ask <= 0:
        blocking.append("no ask")
    if spread_pct is None:
        blocking.append("spread unavailable")
    elif spread_pct > OPTION_MAX_SPREAD_PCT:
        blocking.append(f"spread {spread_pct * 100:.1f}% > {OPTION_MAX_SPREAD_PCT * 100:.0f}%")

    if volume is None:
        if OPTION_MIN_VOLUME > 0:
            blocking.append("volume unavailable")
        else:
            warnings.append("volume unavailable")
    elif volume < OPTION_MIN_VOLUME:
        blocking.append(f"volume {volume} < {OPTION_MIN_VOLUME}")

    if open_interest is None:
        warnings.append("open interest unavailable")
    elif open_interest < OPTION_MIN_OPEN_INTEREST:
        blocking.append(f"open interest {open_interest} < {OPTION_MIN_OPEN_INTEREST}")

    candidate["liquidity_warnings"] = warnings + blocking
    candidate["liquidity_pass"] = not blocking
    return candidate


def liquidity_rank(candidate, anchor_strike=None):
    spread_pct = candidate.get("spread_pct") if candidate.get("spread_pct") is not None else math.inf
    volume = candidate.get("volume") or 0
    open_interest = candidate.get("open_interest") or 0
    strike_distance = abs(float(candidate.get("strike") or 0.0) - float(anchor_strike)) if anchor_strike not in (None, "") else math.inf
    return (
        0 if candidate.get("liquidity_pass") else 1,
        strike_distance,
        spread_pct,
        -volume,
        -open_interest,
        candidate.get("delta_distance", math.inf),
        candidate.get("contract_cost", math.inf),
    )


def summarize_candidate(candidate):
    keys = (
        "symbol",
        "strike",
        "expiration",
        "delta",
        "gamma",
        "theta",
        "bid",
        "ask",
        "spread_pct",
        "volume",
        "open_interest",
        "liquidity_pass",
    )
    return {key: candidate.get(key) for key in keys}


def best_contract_candidate(symbol, option_type, contract_plan=None):
    contract_plan = contract_plan or {}
    today = datetime.now(ET).date()
    requested_expiration = contract_plan.get("expiration")
    expiration = None
    if isinstance(requested_expiration, str) and requested_expiration:
        try:
            expiration = datetime.fromisoformat(requested_expiration).date()
        except ValueError:
            expiration = None
    planned_expired = bool(expiration and expiration < today)
    # If the planned expiry is stale, search active expirations from today forward.
    search_expiration = expiration if expiration and not planned_expired else None

    min_price = optional_float(contract_plan.get("min_price"))
    max_price = optional_float(contract_plan.get("max_price"))
    anchor_strike = optional_float(contract_plan.get("strike"))
    exact_symbol = normalize_text(contract_plan.get("option_symbol")) or None

    contracts = fetch_active_contracts(symbol, option_type, expiration=search_expiration)
    if not contracts:
        return None, [], "No active option contracts found"

    by_expiration = {}
    for contract in contracts:
        expiration = get_value(contract, "expiration_date")
        by_expiration.setdefault(expiration, []).append(contract)

    for expiration in sorted(by_expiration):
        metadata_by_symbol = {get_value(contract, "symbol"): contract for contract in by_expiration[expiration]}
        snapshots = fetch_option_snapshots(metadata_by_symbol)
        candidates = []
        for contract_symbol, contract in metadata_by_symbol.items():
            snapshot = snapshots.get(contract_symbol)
            if not snapshot:
                continue
            candidate = candidate_from_snapshot(contract, snapshot, option_type)
            if candidate:
                candidates.append(candidate)

        if not candidates:
            continue

        if exact_symbol:
            exact_candidates = [candidate for candidate in candidates if normalize_text(candidate.get("symbol")) == exact_symbol]
            if exact_candidates:
                candidates = exact_candidates + [candidate for candidate in candidates if candidate not in exact_candidates]

        if min_price is not None or max_price is not None:
            priced = []
            for candidate in candidates:
                ask_price = candidate.get("ask")
                if ask_price is None:
                    continue
                if min_price is not None and ask_price < min_price:
                    continue
                if max_price is not None and ask_price > max_price:
                    continue
                priced.append(candidate)
            if priced:
                candidates = priced

        band = candidate_delta_band(candidates)
        missing_volume_symbols = [candidate["symbol"] for candidate in band if candidate.get("volume") is None]
        volumes = fetch_recent_option_volumes(missing_volume_symbols)
        enriched = []
        for candidate in band:
            candidate.update(volumes.get(candidate["symbol"], {}))
            enriched.append(apply_liquidity_checks(candidate))

        selected = min(enriched, key=lambda candidate: liquidity_rank(candidate, anchor_strike=anchor_strike))
        reason = "Selected from nearest expiration candidate band"
        if requested_expiration and expiration and not planned_expired:
            reason = f"Selected from planned expiration {expiration.isoformat()}"
        elif planned_expired:
            reason = "Planned expiration was stale; selected from nearest active expiration candidate band"
        if min_price is not None or max_price is not None:
            reason += f" within ${min_price if min_price is not None else 0:.2f}-${max_price if max_price is not None else 9999:.2f}"
        if not selected.get("liquidity_pass"):
            reason = "Selected best available contract, but liquidity filters did not all pass"
        return selected, sorted(enriched, key=lambda candidate: candidate["delta_distance"]), reason

    return None, [], "No contracts with usable quote and Greeks found"


def apply_position_size(candidate, account):
    balance = account.get("account_balance") or 0.0
    contract_cost = candidate.get("contract_cost") or 0.0
    allocation_amount = round(balance * TRADE_ALLOCATION_PCT, 2)
    max_single_contract_amount = round(balance * OPTION_MAX_ACCOUNT_BALANCE_PCT, 2)
    quantity = math.floor(allocation_amount / contract_cost) if contract_cost > 0 else 0
    minimum_one_contract = False
    sizing_warnings = []
    status = "ready"

    if quantity < 1:
        if contract_cost > 0 and contract_cost <= max_single_contract_amount:
            quantity = 1
            minimum_one_contract = True
            sizing_warnings.append("One-contract minimum overrides the allocation size")
        else:
            quantity = 0
            status = "too_expensive"
            sizing_warnings.append("One contract exceeds the maximum 20% account-balance cap")

    return {
        "status": status,
        "quantity": int(quantity),
        "allocation_pct": TRADE_ALLOCATION_PCT,
        "allocation_amount": allocation_amount,
        "max_single_contract_pct": OPTION_MAX_ACCOUNT_BALANCE_PCT,
        "max_single_contract_amount": max_single_contract_amount,
        "minimum_one_contract": minimum_one_contract,
        "sizing_warnings": sizing_warnings,
    }


def validate_entry_contract(contract_preview, require_market_open=True):
    now = datetime.now(UTC)
    blocking = []
    warnings = []
    symbol = contract_preview.get("symbol")
    qty = optional_int(contract_preview.get("quantity")) or 0
    contract_cost = optional_float(contract_preview.get("contract_cost")) or 0.0
    estimated_notional = round(qty * contract_cost, 2)

    if contract_preview.get("status") != "ready":
        blocking.append(f"contract status is {contract_preview.get('status') or 'unknown'}")
    if not symbol:
        blocking.append("missing option symbol")
    if qty < 1:
        blocking.append("quantity is less than 1")
    if optional_float(contract_preview.get("bid")) is None or optional_float(contract_preview.get("bid")) <= 0:
        blocking.append("missing positive bid")
    if optional_float(contract_preview.get("ask")) is None or optional_float(contract_preview.get("ask")) <= 0:
        blocking.append("missing positive ask")
    if optional_float(contract_preview.get("delta_abs")) is None:
        blocking.append("missing delta")
    if contract_preview.get("liquidity_pass") is False:
        blocking.append("liquidity filters did not pass")
    if contract_preview.get("tradable") is False:
        blocking.append("contract is not tradable")

    quote_age = quote_age_seconds(contract_preview.get("quote_time"), now=now)
    if quote_age is None:
        warnings.append("quote timestamp unavailable")
    elif quote_age > OPTION_MAX_QUOTE_AGE_SECONDS:
        message = f"quote age {quote_age:.0f}s > {OPTION_MAX_QUOTE_AGE_SECONDS}s"
        if require_market_open:
            blocking.append(message)
        else:
            warnings.append(message)

    account_summary = {}
    market_open = None
    try:
        account = trade_client.get_account()
        account_summary = account_balance_summary(account)
        if truthy(get_value(account, "trading_blocked", False)):
            blocking.append("account trading is blocked")
        if truthy(get_value(account, "account_blocked", False)):
            blocking.append("account is blocked")
        buying_power = optional_float(get_value(account, "buying_power"))
        if buying_power is not None and estimated_notional > buying_power:
            blocking.append(f"estimated notional ${estimated_notional:.2f} exceeds buying power ${buying_power:.2f}")
    except Exception as exc:
        blocking.append(f"account preflight failed: {exc}")

    try:
        clock = trade_client.get_clock()
        market_open = bool(get_value(clock, "is_open", False))
        if require_market_open and not market_open:
            blocking.append("Alpaca market clock is closed")
    except Exception as exc:
        blocking.append(f"market clock preflight failed: {exc}")

    return {
        "ok": not blocking,
        "blocking": blocking,
        "warnings": warnings,
        "checked_at": now.isoformat(),
        "require_market_open": require_market_open,
        "market_open": market_open,
        "estimated_notional": estimated_notional,
        "quote_age_seconds": round(quote_age, 1) if quote_age is not None else None,
        **account_summary,
    }


def build_contract_preview(symbol, option_type, account=None, contract_plan=None):
    account = account or account_balance_summary()
    try:
        selected, candidates, reason = best_contract_candidate(symbol, option_type, contract_plan=contract_plan)
    except Exception as exc:
        LOGGER.warning("Contract preview failed for %s %s: %s", symbol, option_type, exc)
        return empty_contract_preview(symbol, option_type, status="error", reason=f"Contract preview failed: {exc}", account=account)

    if not selected:
        preview = empty_contract_preview(symbol, option_type, status="unavailable", reason=reason, account=account)
        preview["candidates"] = [summarize_candidate(candidate) for candidate in candidates]
        return preview

    sizing = apply_position_size(selected, account)
    if not selected.get("liquidity_pass"):
        sizing["status"] = "unavailable"
        sizing["quantity"] = 0
    warnings = list(selected.get("liquidity_warnings") or []) + list(sizing.get("sizing_warnings") or [])
    preview = {
        **selected,
        **account,
        **sizing,
        "reason": reason,
        "target_delta": TARGET_DELTA,
        "warnings": warnings,
        "candidates": [summarize_candidate(candidate) for candidate in candidates],
        "candidate_count": len(candidates),
        "updated_at": datetime.now(ET).isoformat(),
    }
    return preview