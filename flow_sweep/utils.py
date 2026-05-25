from datetime import datetime
from decimal import Decimal

from .config import CLIENT_ORDER_PREFIX, UTC


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


def iso_utc(timestamp):
    return timestamp.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def round_or_none(value, digits=4):
    if value in (None, ""):
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def key_level_to_dict(level):
    return {"name": level.name, "price": round(float(level.price), 4)}
