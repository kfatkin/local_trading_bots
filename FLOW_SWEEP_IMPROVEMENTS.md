# OI 5mORB Entry Improvements

## Implemented In This Pass

- Start entries at 10:00 ET instead of 09:45 ET to avoid the highest-noise opening churn from the latest backtest, while allowing entries until before 14:00 ET so the bot is not restricted to a narrow morning-only window.
- Treat the first sweep/reclaim candle as a signal only; enter only after the next 5-minute candle confirms.
- Require the sweep candle to close meaningfully back through the swept level:
  - calls: close in the upper half and at least 10% of candle range above the swept support;
  - puts: close in the lower half and at least 10% of candle range below the swept resistance.
- Keep the stop at the original sweep candle extreme, not the confirmation candle extreme.
- Skip entries where the planned opposing-level target is above 8R, which usually means the risk is too tight or the nearest opposing level is too far away for this scalp structure.
- Add a continuation fair value gap strategy as an alternative, bias-aligned setup. The live bot and backtest now allow both strategies, with the first valid armed setup winning for a symbol.
- Raise the default premium-consensus requirement from 60% to 70% so mixed prior-session flow is filtered out.
- Restrict new entries to the higher-quality window from 10:00 ET until before 14:00 ET.
- Keep the continuation FVG implementation available behind `FLOW_SWEEP_ENABLE_CONTINUATION_FVG`, but disable it by default until it has a better standalone baseline.
- Add a 50% partial-profit request at the 1.5R breakeven trigger when the live position has enough contracts to leave a runner.

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
- The first combined sweep-plus-continuation run produced 14 trades, 1.73 profit factor, and +5.83R total. Continuation FVG contributed only 1 trade in that sample, and it stopped out, so the continuation defaults still need tuning.
- The next-pass changes should be measured against the 13-trade sweep-only baseline: 38.5% win rate, 2.31 profit factor, and +7.83R total. The key hypothesis is that 70% consensus, no fresh noon-or-later entries, and 1.5R partials improve realized win rate without fully starving trade count.
- The first next-pass 40-session run produced 7 trades, 5 wins, 2 losses, 0 breakeven exits, 71.4% win rate, 5.33 profit factor, and +8.67R total. This improved the prior sweep-only total R and profit factor, but cut trade count nearly in half.

## Continuation FVG Strategy

This model complements the level-sweep strategy rather than replacing it. The flow bias still comes first: only look for bullish continuation when prior-session scored flow is bullish, and only look for bearish continuation when prior-session scored flow is bearish.

### Bullish Continuation

1. Wait until after 10:00 ET so the opening churn has had time to settle.
2. Build 5-minute market structure from confirmed swing highs and swing lows.
3. Require bullish structure to form: price makes a swing low, then breaks and closes above the most recent confirmed swing high.
4. Require displacement on the break candle: above-average 5-minute range/body and a close in the upper part of the candle.
5. Identify the bullish fair value gap from the displacement leg, using the three-candle pattern where the current candle low is above the high from two candles earlier.
6. Buy calls on the first pullback into the FVG if price touches the zone and then closes back in the bullish direction.
7. Stop goes below the most recent structure swing low. Target uses the nearest opposing key level when it offers at least 2R, otherwise fixed 2R.

### Bearish Continuation

1. Wait until after 10:00 ET.
2. Build 5-minute market structure from confirmed swing highs and swing lows.
3. Require bearish structure to form: price makes a swing high, then breaks and closes below the most recent confirmed swing low.
4. Require displacement on the break candle: above-average 5-minute range/body and a close in the lower part of the candle.
5. Identify the bearish fair value gap from the displacement leg, using the three-candle pattern where the current candle high is below the low from two candles earlier.
6. Buy puts on the first pullback into the FVG if price touches the zone and then closes back in the bearish direction.
7. Stop goes above the most recent structure swing high. Target uses the nearest opposing key level when it offers at least 2R, otherwise fixed 2R.

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

- The combined backtest should emit `setup_type` on every row so level sweeps and continuation entries can be compared directly.
- Compare trade count, win rate, profit factor, average R, max stop-out streak, and time-of-day distribution against the prior sweep-only model.
- If the continuation model adds trade count but weakens expectancy, tighten the displacement and zone-age thresholds before adding more setup types.
