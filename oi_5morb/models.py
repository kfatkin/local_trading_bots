from dataclasses import dataclass


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
