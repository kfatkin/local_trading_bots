# Flow Sweep Entry Improvements

## Implemented In This Pass

- Start entries at 10:00 ET instead of 09:45 ET to avoid the highest-noise opening churn from the latest backtest.
- Treat the first sweep/reclaim candle as a signal only; enter only after the next 5-minute candle confirms.
- Require the sweep candle to close meaningfully back through the swept level:
  - calls: close in the upper half and at least 10% of candle range above the swept support;
  - puts: close in the lower half and at least 10% of candle range below the swept resistance.
- Keep the stop at the original sweep candle extreme, not the confirmation candle extreme.
- Skip entries where the planned opposing-level target is above 8R, which usually means the risk is too tight or the nearest opposing level is too far away for this scalp structure.

## Next Improvements To Test

- Opening range structure filter: require bullish entries to show a higher low after 10:00 ET and bearish entries to show a lower high.
- Volatility-aware stop buffer: add a small ATR or bar-range buffer beyond the sweep extreme, then require the trade to still offer at least 2R.
- Relative volume filter: require the sweep and/or confirmation candle to trade above recent 5-minute average volume.
- Symbol-specific throttles: review whether high-volatility names like TSLA/SNDK need wider buffers, later start times, or separate max target-R limits.
- Market regime filter: skip long calls when the symbol and broad market are both below VWAP/first-hour trend, and skip puts when both are above.
- Contract behavior replay: extend the backtest to include historical option bid/ask marks so win rate and stop-outs reflect option premium behavior, not only underlying R.

## Backtest Notes

- The latest pre-change 40-session run had 15 trades, 10 stops, 2 targets, 2 EOD exits, 1 breakeven, and about +0.51R total.
- The main observed failure cluster was 09:45 entries: 7 of 10 stops came from the first eligible 5-minute close.
- The new rules intentionally reduce trade count to favor cleaner acceptance after a level sweep.
