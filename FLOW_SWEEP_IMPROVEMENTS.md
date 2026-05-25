# Flow Sweep Entry Improvements

## Implemented In This Pass

- Start entries at 10:00 ET instead of 09:45 ET to avoid the highest-noise opening churn from the latest backtest, while allowing entries through 15:00 ET so the bot is not restricted to a narrow morning-only window.
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
- Expanding the confirmed-entry window from 10:00-10:30 ET to 10:00-15:00 ET improved the 40-session test to 13 trades, 5 wins, 6 losses, 2 breakeven exits, 2.31 profit factor, and +7.83R total.

## Continuation FVG / Order Block Strategy Candidate

This model is intended to complement the level-sweep strategy, not replace it outright. The flow bias still comes first: only look for bullish continuation when prior-session scored flow is bullish, and only look for bearish continuation when prior-session scored flow is bearish.

### Bullish Continuation

1. Wait until after 10:00 ET so the opening churn has had time to settle.
2. Build 5-minute market structure from confirmed swing highs and swing lows.
3. Require bullish structure to form: price makes a swing low, then breaks and closes above the most recent confirmed swing high.
4. Require displacement on the break candle: above-average 5-minute range/body and a close in the upper part of the candle.
5. Identify the bullish fair value gap from the displacement leg, using the three-candle pattern where the current candle low is above the high from two candles earlier.
6. Optionally identify the bullish order block as the last bearish candle before the displacement break.
7. Buy calls on the first pullback into the FVG or order-block zone if price holds above the structure low.
8. Stop goes below the order block, FVG, or most recent swing low. Target uses the nearest opposing key level when it offers at least 2R, otherwise fixed 2R.

### Bearish Continuation

1. Wait until after 10:00 ET.
2. Build 5-minute market structure from confirmed swing highs and swing lows.
3. Require bearish structure to form: price makes a swing high, then breaks and closes below the most recent confirmed swing low.
4. Require displacement on the break candle: above-average 5-minute range/body and a close in the lower part of the candle.
5. Identify the bearish fair value gap from the displacement leg, using the three-candle pattern where the current candle high is below the low from two candles earlier.
6. Optionally identify the bearish order block as the last bullish candle before the displacement break.
7. Buy puts on the first pullback into the FVG or order-block zone if price holds below the structure high.
8. Stop goes above the order block, FVG, or most recent swing high. Target uses the nearest opposing key level when it offers at least 2R, otherwise fixed 2R.

### Mechanical Definitions To Backtest

- Swing high: a 5-minute candle whose high is higher than the highs of one or two candles before and after it.
- Swing low: a 5-minute candle whose low is lower than the lows of one or two candles before and after it.
- Bullish break of structure: a 5-minute close above the latest confirmed swing high.
- Bearish break of structure: a 5-minute close below the latest confirmed swing low.
- Bullish FVG: current candle low is above the high from two candles earlier.
- Bearish FVG: current candle high is below the low from two candles earlier.
- Displacement: range or body is at least 1.2x to 1.5x the recent 5-minute average range and closes in the directional half of the candle.
- Trade throttle: allow at most one structure-continuation trade per symbol per day, but allow multiple symbols to trigger.

### Backtest Plan

- Add a separate `structure_fvg` setup type to the backtest output so level sweeps and continuation entries can be compared directly.
- Test FVG-only entries, order-block-only entries, and a combined FVG/OB zone entry.
- Compare trade count, win rate, profit factor, average R, max stop-out streak, and time-of-day distribution against the level-sweep model.
- Only promote the structure model to live trading if it improves trade count without bringing back the high stop-out rate seen in the first 09:45 sweep model.
