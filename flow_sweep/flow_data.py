from boto3.dynamodb.conditions import Attr, Key

from .clients import boto3_table
from .config import CONSENSUS_THRESHOLD, FLOW_SCORE_PARTITION, LOGGER, MIN_FLOW_SCORE
from .models import FlowBias
from .utils import decimal_to_native, iso_utc, to_float


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


def first_present(row, *fields):
    for field in fields:
        value = row.get(field)
        if value not in (None, ""):
            return value
    return None


def bool_or_none(value):
    if value in (None, ""):
        return None
    return bool(value)


def flow_row_payload(row):
    reasons = row.get("signal_reasons") or []
    alert_ids = row.get("contributing_alert_ids") or []
    if not isinstance(reasons, list):
        reasons = [str(reasons)]
    if not isinstance(alert_ids, list):
        alert_ids = [str(alert_ids)]

    return {
        "scored_at": row.get("scored_at") or str(row.get("SK", "")).split("#", 1)[0],
        "score": int(to_float(row.get("composite_score"), 0)),
        "tier": row.get("tier"),
        "direction": str(row.get("direction") or "neutral").lower(),
        "premium": round(to_float(row.get("premium"), 0.0), 2),
        "largest_premium": round(to_float(row.get("largest_premium"), 0.0), 2),
        "spot_price": round(to_float(row.get("spot_price"), 0.0), 4) if row.get("spot_price") not in (None, "") else None,
        "confidence": round(to_float(row.get("confidence"), 0.0), 4) if row.get("confidence") not in (None, "") else None,
        "ask_side_pct": round(to_float(row.get("ask_side_pct"), 0.0), 4) if row.get("ask_side_pct") not in (None, "") else None,
        "is_sweep": bool_or_none(row.get("is_sweep")),
        "sweep_count": int(to_float(row.get("sweep_count"), 0)),
        "repeat_hit_count": int(to_float(row.get("repeat_hit_count"), 0)),
        "cross_expiry_cluster": bool_or_none(row.get("cross_expiry_cluster")),
        "dark_pool_confirmed": bool_or_none(row.get("dark_pool_confirmed")),
        "alerts_ingested": int(to_float(row.get("alerts_ingested"), 0)),
        "alerts_filtered": int(to_float(row.get("alerts_filtered"), 0)),
        "signal_id": row.get("signal_id"),
        "reasons": reasons,
        "contributing_alert_ids": alert_ids,
        "expiry": first_present(row, "expiry", "expiration", "expiration_date", "option_expiration"),
        "strike": first_present(row, "strike", "strike_price", "option_strike"),
        "option_type": first_present(row, "option_type", "contract_type", "put_call", "call_put"),
        "side": first_present(row, "side", "trade_side", "order_side"),
        "size": first_present(row, "size", "quantity", "contracts", "volume"),
        "price": first_present(row, "price", "trade_price", "option_price"),
    }


def summarize_flow_rows(symbol, rows):
    bullish_premium = 0.0
    bearish_premium = 0.0
    flow_rows = [flow_row_payload(row) for row in rows]
    top_score = max((row["score"] for row in flow_rows), default=0)
    used_rows = 0

    for row in rows:
        direction = str(row.get("direction") or "neutral").lower()
        if direction not in {"bullish", "bearish"}:
            continue
        premium = to_float(row.get("premium"), 0.0) or to_float(row.get("largest_premium"), 0.0)
        if premium <= 0:
            continue
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
        "flow_rows": flow_rows,
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
