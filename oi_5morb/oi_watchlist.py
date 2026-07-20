import json
import re
from collections import defaultdict
from datetime import date, datetime
from base64 import b64decode
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3
from botocore.config import Config

from .config import (
    ET,
    LOGGER,
    OI_CHANGE_API_URL,
    OI_CHANGE_AWS_PROFILE,
    OI_CHANGE_AWS_REGION,
    OI_CHANGE_SECRET_NAME,
    OI_WATCHLIST_MAX_DTE,
    OI_WATCHLIST_MAX_CONTRACT_PRICE,
    OI_WATCHLIST_MAX_MULTI_LEG_SHARE,
    OI_WATCHLIST_MIN_ASK_MID_RATIO,
    OI_WATCHLIST_MIN_CONTRACT_PRICE,
    OI_WATCHLIST_REQUIRE_ASK_SIDE,
    REGULAR_OPEN,
    UW_API_KEY,
)


UW_API_BASE_URL = "https://api.unusualwhales.com"
UW_CLIENT_API_ID = "local-trading-bots-oi-orb"
UW_REQUEST_TIMEOUT_SECONDS = 30
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
YAHOO_BATCH_SIZE = 100
EXCLUDED_QUOTE_TYPES = {"ETF", "INDEX", "MUTUALFUND"}
API_KEY_FIELDS = (
    "unusual_whales",
    "unusualwhales",
    "unusual_whales_api_key",
    "unusualwhales_api_key",
    "unusualWhales",
    "unusualWhalesApiKey",
    "unusual_whales_api",
    "uw_api_key",
    "uw-api-key",
    "uw",
)


def _normalize_key_name(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _looks_like_uw_key(value):
    normalized = str(value or "").strip()
    if len(normalized) < 12:
        return False
    return bool(re.match(r"^[A-Za-z0-9_\-\.]+$", normalized))


def _extract_uw_key_from_payload(payload):
    field_aliases = {_normalize_key_name(name) for name in API_KEY_FIELDS}

    def walk(node):
        if isinstance(node, dict):
            # First pass: direct known field aliases.
            for raw_key, raw_value in node.items():
                key = _normalize_key_name(raw_key)
                if key in field_aliases and isinstance(raw_value, str) and raw_value.strip():
                    return raw_value.strip()

            # Second pass: fuzzy key matching.
            for raw_key, raw_value in node.items():
                key = _normalize_key_name(raw_key)
                if "unusual" in key and "whale" in key and isinstance(raw_value, str) and raw_value.strip():
                    return raw_value.strip()
                if key.startswith("uw") and "key" in key and isinstance(raw_value, str) and raw_value.strip():
                    return raw_value.strip()

            # Third pass: recurse nested structures.
            for raw_value in node.values():
                nested = walk(raw_value)
                if nested:
                    return nested
            return None

        if isinstance(node, list):
            for item in node:
                nested = walk(item)
                if nested:
                    return nested
            return None

        if isinstance(node, str) and _looks_like_uw_key(node):
            return node.strip()
        return None

    return walk(payload)


def _http_json(url, headers=None, params=None, timeout=UW_REQUEST_TIMEOUT_SECONDS):
    query = f"?{urlencode(params)}" if params else ""
    request_headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
    }
    if headers:
        request_headers.update(headers)
    request = Request(f"{url}{query}", headers=request_headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"HTTP {exc.code} from {url}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Unable to reach {url}: {exc}") from exc
    return json.loads(payload)


def _secrets_manager_client():
    config = Config(retries={"max_attempts": 3, "mode": "adaptive"}, connect_timeout=5, read_timeout=10)
    profile = OI_CHANGE_AWS_PROFILE or None
    if profile:
        session = boto3.Session(profile_name=profile, region_name=OI_CHANGE_AWS_REGION)
    else:
        session = boto3.Session(region_name=OI_CHANGE_AWS_REGION)
    return session.client("secretsmanager", region_name=OI_CHANGE_AWS_REGION, config=config)


def resolve_uw_api_key():
    if UW_API_KEY:
        return UW_API_KEY

    client = _secrets_manager_client()
    response = client.get_secret_value(SecretId=OI_CHANGE_SECRET_NAME)
    secret_text = response.get("SecretString")
    if not secret_text and response.get("SecretBinary"):
        secret_text = b64decode(response.get("SecretBinary")).decode("utf-8", errors="replace")

    secret_text = (secret_text or "").strip()
    if not secret_text:
        raise RuntimeError(f"Secret {OI_CHANGE_SECRET_NAME} is empty; cannot resolve a Unusual Whales API key")

    try:
        payload = json.loads(secret_text)
    except json.JSONDecodeError:
        if _looks_like_uw_key(secret_text):
            return secret_text
        raise RuntimeError(
            f"Secret {OI_CHANGE_SECRET_NAME} is not valid JSON and does not look like a direct API key string"
        )

    value = _extract_uw_key_from_payload(payload)
    if value:
        return value
    raise RuntimeError(f"Secret {OI_CHANGE_SECRET_NAME} is missing a Unusual Whales API key field")


def _to_number(value):
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if parsed == parsed else None
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        try:
            parsed = float(normalized)
        except ValueError:
            return None
        return parsed if parsed == parsed else None
    return None


def _normalize_date(value):
    if isinstance(value, str) and value.strip():
        return value.strip()
    return datetime.now(ET).date().isoformat()


def _row_value(row, *keys):
    if not isinstance(row, dict):
        return None
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _parse_option_symbol(option_symbol):
    match = re.match(r"^([A-Z]+)(\d{6})([CP])(\d{8})$", option_symbol.strip().upper())
    if not match:
        return {"option_type": "unknown", "expiration": None, "strike": None}

    yymmdd = match.group(2)
    cp_flag = match.group(3)
    strike_raw = match.group(4)
    expiration = f"20{yymmdd[:2]}-{yymmdd[2:4]}-{yymmdd[4:6]}"
    strike = int(strike_raw) / 1000
    return {
        "option_type": "call" if cp_flag == "C" else "put",
        "expiration": expiration,
        "strike": strike,
    }


def _compute_dte(current_date, expiration):
    if not expiration:
        return None
    try:
        current = datetime.strptime(current_date, "%Y-%m-%d").date()
        expiry = datetime.strptime(expiration, "%Y-%m-%d").date()
    except ValueError:
        return None
    return (expiry - current).days


def _normalize_row(row, session_date=None):
    option_symbol = str(_row_value(row, "option_symbol", "optionSymbol") or "").strip().upper()
    underlying_symbol = str(_row_value(row, "underlying_symbol", "underlyingSymbol") or "").strip().upper()
    if not option_symbol or not underlying_symbol:
        return None

    contract = _parse_option_symbol(option_symbol)
    current_date = _normalize_date(_row_value(row, "curr_date", "currentDate"))
    previous_date = _normalize_date(_row_value(row, "last_date", "previousDate"))
    option_type = contract["option_type"]
    if option_type not in {"call", "put"}:
        return None

    effective_current_date = (session_date or datetime.now(ET).date()).isoformat()

    return {
        "optionSymbol": option_symbol,
        "underlyingSymbol": underlying_symbol,
        "optionType": option_type,
        "expiration": contract["expiration"],
        "strike": contract["strike"],
        "currentDate": effective_current_date,
        "previousDate": previous_date,
        # DTE must be relative to today's session date, not a stale OI snapshot date.
        "dte": _compute_dte(effective_current_date, contract["expiration"]),
        "currentOi": _to_number(_row_value(row, "curr_oi", "currentOi")),
        "previousOi": _to_number(_row_value(row, "last_oi", "previousOi")),
        "oiDiff": _to_number(_row_value(row, "oi_diff_plain", "oiDiff")),
        "oiChangeRatio": _to_number(_row_value(row, "oi_change", "oiChangeRatio")),
        "oiChangePct": (_to_number(_row_value(row, "oi_change", "oiChangeRatio")) or 0.0) * 100,
        "volume": _to_number(_row_value(row, "volume")),
        "trades": _to_number(_row_value(row, "trades")),
        "avgPrice": _to_number(_row_value(row, "avg_price", "avgPrice")),
        "lastBid": _to_number(_row_value(row, "last_bid", "lastBid")),
        "lastAsk": _to_number(_row_value(row, "last_ask", "lastAsk")),
        "lastFill": _to_number(_row_value(row, "last_fill", "lastFill")),
        "rank": _to_number(_row_value(row, "rnk", "rank")),
        "percentageOfTotal": _to_number(_row_value(row, "percentage_of_total", "percentageOfTotal")),
        "previousAskVolume": _to_number(_row_value(row, "prev_ask_volume", "previousAskVolume")) or 0.0,
        "previousBidVolume": _to_number(_row_value(row, "prev_bid_volume", "previousBidVolume")) or 0.0,
        "previousMidVolume": _to_number(_row_value(row, "prev_mid_volume", "previousMidVolume")) or 0.0,
        "previousNeutralVolume": _to_number(_row_value(row, "prev_neutral_volume", "previousNeutralVolume")) or 0.0,
        "previousMultiLegVolume": _to_number(_row_value(row, "prev_multi_leg_volume", "previousMultiLegVolume")) or 0.0,
        "previousStockMultiLegVolume": _to_number(_row_value(row, "prev_stock_multi_leg_volume", "previousStockMultiLegVolume")) or 0.0,
        "previousTotalPremium": _to_number(_row_value(row, "prev_total_premium", "previousTotalPremium")) or 0.0,
    }


def _fetch_yahoo_quote_types(symbols):
    classifications = {}
    unique = sorted({symbol.strip().upper() for symbol in symbols if symbol and symbol.strip()})
    headers = {
        "User-Agent": "Mozilla/5.0 local-trading-bots",
        "Accept": "application/json, text/plain,*/*",
    }

    for index in range(0, len(unique), YAHOO_BATCH_SIZE):
        batch = unique[index : index + YAHOO_BATCH_SIZE]
        payload = _http_json(YAHOO_QUOTE_URL, headers=headers, params={"symbols": ",".join(batch)}, timeout=15)
        results = payload.get("quoteResponse", {}).get("result", []) if isinstance(payload, dict) else []
        for result in results:
            if not isinstance(result, dict):
                continue
            symbol = str(result.get("symbol") or "").strip().upper()
            if symbol:
                classifications[symbol] = result
    return classifications


def _should_exclude_symbol(summary):
    if not summary:
        return False
    quote_type = str(summary.get("quoteType") or "").strip().upper()
    if quote_type in EXCLUDED_QUOTE_TYPES:
        return True
    label = f"{summary.get('longName') or ''} {summary.get('shortName') or ''}".upper()
    return "EXCHANGE TRADED NOTE" in label or bool(re.search(r"(^|\W)ETN(\W|$)", label))


def fetch_oi_change_rows():
    url = OI_CHANGE_API_URL or f"{UW_API_BASE_URL}/api/market/oi-change"
    headers = {"Accept": "application/json, text/plain, */*"}

    if "unusualwhales.com" in url:
        api_key = resolve_uw_api_key()
        headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "X-API-Key": api_key,
                "Origin": "https://unusualwhales.com",
                "Referer": "https://unusualwhales.com/",
                "UW-CLIENT-API-ID": UW_CLIENT_API_ID,
            }
        )

    payload = _http_json(url, headers=headers)
    if not isinstance(payload, dict):
        return []

    snapshot = payload.get("snapshot")
    if isinstance(snapshot, dict):
        rows = snapshot.get("contracts")
        if isinstance(rows, list):
            return rows

    rows = payload.get("data")
    return rows if isinstance(rows, list) else []


def _row_side_metrics(row):
    ask_volume = float(row.get("previousAskVolume") or 0.0)
    bid_volume = float(row.get("previousBidVolume") or 0.0)
    mid_volume = float(row.get("previousMidVolume") or 0.0)
    neutral_volume = float(row.get("previousNeutralVolume") or 0.0)
    multi_leg_volume = float(row.get("previousMultiLegVolume") or 0.0) + float(row.get("previousStockMultiLegVolume") or 0.0)
    total_side_volume = ask_volume + bid_volume + mid_volume
    denominator = total_side_volume if total_side_volume > 0 else 1.0
    watchlist_denominator = max(float(row.get("volume") or 0.0), total_side_volume + neutral_volume + multi_leg_volume, 1.0)
    ask_mid_ratio = (ask_volume + mid_volume) / denominator
    bid_ratio = bid_volume / denominator
    multi_leg_share = multi_leg_volume / watchlist_denominator

    if ask_volume >= mid_volume and ask_volume >= bid_volume and ask_volume > 0:
        side = "ask"
    elif mid_volume >= bid_volume and mid_volume > 0:
        side = "mid"
    elif bid_volume > 0:
        side = "bid"
    else:
        side = "unknown"

    return {
        "ask_volume": ask_volume,
        "bid_volume": bid_volume,
        "mid_volume": mid_volume,
        "neutral_volume": neutral_volume,
        "multi_leg_volume": multi_leg_volume,
        "ask_mid_ratio": round(ask_mid_ratio, 4),
        "bid_ratio": round(bid_ratio, 4),
        "multi_leg_share": round(multi_leg_share, 4),
        "side": side,
    }


def _qualifies_for_watchlist(row):
    dte = row.get("dte")
    oi_diff = row.get("oiDiff")
    metrics = _row_side_metrics(row)
    if dte is None or dte < 0 or dte >= OI_WATCHLIST_MAX_DTE:
        return False, [f"DTE {dte if dte is not None else 'unknown'} outside < {OI_WATCHLIST_MAX_DTE}"]
    if oi_diff is None or oi_diff <= 0:
        return False, ["open interest did not increase"]
    if OI_WATCHLIST_REQUIRE_ASK_SIDE and metrics["side"] != "ask":
        return False, [f"flow side {metrics['side']} is not ask-side"]

    reasons = []
    if metrics["ask_mid_ratio"] < OI_WATCHLIST_MIN_ASK_MID_RATIO:
        reasons.append(
            f"ask+mid {metrics['ask_mid_ratio'] * 100:.0f}% below {OI_WATCHLIST_MIN_ASK_MID_RATIO * 100:.0f}%"
        )
    if metrics["multi_leg_share"] > OI_WATCHLIST_MAX_MULTI_LEG_SHARE:
        reasons.append(
            f"multi-leg {metrics['multi_leg_share'] * 100:.0f}% above {OI_WATCHLIST_MAX_MULTI_LEG_SHARE * 100:.0f}%"
        )
    return not reasons, reasons


def _candidate_sort_key(candidate):
    return (
        -float(candidate.get("oiDiff") or 0.0),
        -float(candidate.get("ask_mid_ratio") or 0.0),
        -float(candidate.get("previousTotalPremium") or 0.0),
        -float(candidate.get("volume") or 0.0),
        float(candidate.get("dte") or 9999),
    )


def _candidate_display_row(candidate):
    direction = "bullish" if candidate["optionType"] == "call" else "bearish"
    reasons = [
        f"OI +{int(candidate['oiDiff'])}" if candidate.get("oiDiff") is not None else "OI delta unavailable",
        f"{candidate.get('side') or 'unknown'} side",
        f"Ask+Mid {candidate['ask_mid_ratio'] * 100:.0f}%",
        f"Bid {candidate['bid_ratio'] * 100:.0f}%",
        f"Multi-leg {candidate['multi_leg_share'] * 100:.0f}%",
    ]
    if candidate.get("dte") is not None:
        reasons.append(f"{int(candidate['dte'])} DTE")
    return {
        "kind": "oi_change",
        "scored_at": f"{candidate['currentDate']}T{REGULAR_OPEN.strftime('%H:%M:%S')}",
        "score": int(candidate.get("oiDiff") or 0),
        "tier": f"{int(candidate.get('dte') or 0)} DTE",
        "direction": direction,
        "premium": round(float(candidate.get("previousTotalPremium") or 0.0), 2),
        "largest_premium": round(float(candidate.get("avgPrice") or candidate.get("lastAsk") or 0.0), 2),
        "spot_price": None,
        "confidence": candidate.get("ask_mid_ratio"),
        "ask_side_pct": round(float(candidate.get("ask_volume") or 0.0) / max(float(candidate.get("ask_volume") or 0.0) + float(candidate.get("bid_volume") or 0.0) + float(candidate.get("mid_volume") or 0.0), 1.0), 4),
        "mid_side_pct": candidate.get("ask_mid_ratio"),
        "bid_side_pct": candidate.get("bid_ratio"),
        "cross_expiry_cluster": False,
        "dark_pool_confirmed": False,
        "alerts_ingested": int(candidate.get("trades") or 0),
        "reasons": reasons,
        "expiry": candidate.get("expiration"),
        "strike": candidate.get("strike"),
        "option_type": candidate.get("optionType"),
        "side": candidate.get("side"),
        "size": int(candidate.get("volume") or 0),
        "price": candidate.get("avgPrice") or candidate.get("lastAsk") or candidate.get("lastFill"),
        "option_symbol": candidate.get("optionSymbol"),
        "dte": candidate.get("dte"),
        "oi_diff": candidate.get("oiDiff"),
        "oi_change_pct": candidate.get("oiChangePct"),
        "multi_leg_share": candidate.get("multi_leg_share"),
    }


def _review_display_row(candidate, accepted, review_reasons):
    row = _candidate_display_row(candidate)
    row.update(
        {
            "review_status": "accepted" if accepted else "rejected",
            "review_reasons": review_reasons,
            "review_summary": "Accepted into the watch list" if accepted else (review_reasons[0] if review_reasons else "Rejected"),
        }
    )
    return row


def load_morning_watchlist(now_et=None):
    now_et = now_et or datetime.now(ET)
    try:
        raw_rows = fetch_oi_change_rows()
    except Exception as exc:
        # Fail open so the bot and dashboard still run even if UW temporarily blocks API access.
        LOGGER.warning("Morning OI watchlist fetch failed; continuing with empty watchlist: %s", exc)
        return {}
    normalized = [
        row
        for row in (
            _normalize_row(item, session_date=now_et.date())
            for item in raw_rows
            if isinstance(item, dict)
        )
        if row
    ]

    quote_types = {}
    try:
        quote_types = _fetch_yahoo_quote_types([row["underlyingSymbol"] for row in normalized])
    except Exception as exc:
        LOGGER.warning("OI watchlist Yahoo quote classification failed; continuing without exclusions: %s", exc)

    candidates = []
    reviewed_by_symbol = defaultdict(list)
    for row in normalized:
        if _should_exclude_symbol(quote_types.get(row["underlyingSymbol"])):
            continue
        qualifies, reject_reasons = _qualifies_for_watchlist(row)
        metrics = _row_side_metrics(row)
        enriched = {**row, **metrics, "reject_reasons": reject_reasons}
        review_reasons = [
            f"DTE {int(enriched['dte'])} accepted" if enriched.get("dte") is not None else "DTE unknown",
            f"OI +{int(enriched['oiDiff'])}" if enriched.get("oiDiff") is not None else "OI delta unavailable",
            f"{enriched['side']} side",
            f"Ask+Mid {enriched['ask_mid_ratio'] * 100:.0f}%",
            f"Multi-leg {enriched['multi_leg_share'] * 100:.0f}%",
        ]
        reviewed_by_symbol[enriched["underlyingSymbol"]].append(
            _review_display_row(enriched, qualifies, reject_reasons if not qualifies else review_reasons)
        )
        if qualifies:
            candidates.append(enriched)

    grouped = defaultdict(list)
    for candidate in candidates:
        grouped[candidate["underlyingSymbol"]].append(candidate)

    decisions = {}
    for symbol, rows in grouped.items():
        ordered = sorted(rows, key=_candidate_sort_key)
        primary = ordered[0]
        bullish_premium = round(sum(float(row.get("previousTotalPremium") or 0.0) for row in ordered if row["optionType"] == "call"), 2)
        bearish_premium = round(sum(float(row.get("previousTotalPremium") or 0.0) for row in ordered if row["optionType"] == "put"), 2)
        total_premium = round(bullish_premium + bearish_premium, 2)
        direction = "bullish" if primary["optionType"] == "call" else "bearish"
        mixed_directions = len({row["optionType"] for row in ordered}) > 1
        decision_status = "flow_preview" if now_et.time() < REGULAR_OPEN else "orb_wait"
        decision_reason = (
            "Watchlist loaded; waiting for the regular open"
            if decision_status == "flow_preview"
            else "Waiting for the opening 5m range to complete"
        )
        if mixed_directions:
            decision_reason += "; top-ranked contract selected for execution"
        reviewed_rows = sorted(reviewed_by_symbol.get(symbol, []), key=lambda row: (-int(row.get("oi_diff") or 0), row.get("dte") or 9999))
        decisions[symbol] = {
            "symbol": symbol,
            "status": decision_status,
            "reason": decision_reason,
            "direction": direction,
            "consensus": primary["ask_mid_ratio"],
            "top_score": int(primary.get("oiDiff") or 0),
            "bullish_premium": bullish_premium,
            "bearish_premium": bearish_premium,
            "total_premium": total_premium,
            "raw_row_count": len(ordered),
            "directional_row_count": len(ordered),
            "option_type": "CALL" if direction == "bullish" else "PUT",
            "trigger_levels": [],
            "target_levels": [],
            "reviewed_rows": reviewed_rows,
            "reviewed_count": len(reviewed_rows),
            "reviewed_accepted_count": sum(1 for row in reviewed_rows if row.get("review_status") == "accepted"),
            "reviewed_rejected_count": sum(1 for row in reviewed_rows if row.get("review_status") == "rejected"),
            "flow_rows": [_candidate_display_row(candidate) for candidate in ordered],
            "key_levels": [],
            "contract_plan": {
                "expiration": primary.get("expiration"),
                "strike": primary.get("strike"),
                "option_type": "CALL" if primary["optionType"] == "call" else "PUT",
                "option_symbol": primary.get("optionSymbol"),
                "min_price": OI_WATCHLIST_MIN_CONTRACT_PRICE,
                "max_price": OI_WATCHLIST_MAX_CONTRACT_PRICE,
            },
        }

    return decisions