# Flow Sweep Options Trading Bot

An automated options trading bot built with Python and `alpaca-py`. It reads prior-session scored options flow from `uw-hub` DynamoDB data, builds a bullish or bearish watchlist, then waits for 5-minute reclaim/rejection setups around important market levels.

## Strategy

The bot watches:

- `AMD`
- `AAPL`
- `AMZN`
- `GOOGL`
- `META`
- `MU`
- `INTC`
- `NVDA`
- `TSLA`
- `STX`
- `SNDK`

Before the trading session, it queries the `uw-data` DynamoDB table with AWS profile `trading_bot` and looks for `_flow_scores_trading_bot` rows from the prior NYSE regular session where `composite_score > 70`.

For each symbol, it premium-weights bullish and bearish high-score flow. A setup is enabled only when one side has at least 60% of the total directional premium.

For bullish flow, the bot watches premarket low, prior-day low, and prior-week low. If a 5-minute candle between 09:45 and 10:30 ET sweeps the closest relevant low and closes back above it, the bot buys calls.

For bearish flow, the bot watches premarket high, prior-day high, and prior-week high. If a 5-minute candle between 09:45 and 10:30 ET sweeps the closest relevant high and closes back below it, the bot buys puts.

Contracts are selected from the nearest expiration, including 0DTE when available, by choosing the contract closest to absolute `0.30` delta. There is no premium cap.

## Risk And Exits

- Default account mode is paper trading through `ALPACA_PAPER=true` or the built-in default.
- Position size is 5% of available buying power.
- Stop is the sweep candle extreme.
- Target is the first opposing key level hit: premarket, prior-day, or prior-week high for calls; premarket, prior-day, or prior-week low for puts.
- Remaining positions are closed near end of day at 15:55 ET.
- Local runtime state is persisted under `runtime/state.json` so open option positions can be reconciled after restart.

## Configuration

Create `.env` in this folder with Alpaca credentials:

```env
ALPACA_API_KEY=your_paper_api_key_here
ALPACA_SECRET_KEY=your_paper_secret_key_here
ALPACA_PAPER=true
```

Optional settings:

```env
AWS_PROFILE=trading_bot
AWS_REGION=us-east-2
UW_TABLE_NAME=uw-data
UW_FLOW_SCORE_PARTITION=_flow_scores_trading_bot
FLOW_SWEEP_MIN_SCORE=70
FLOW_SWEEP_CONSENSUS_THRESHOLD=0.60
FLOW_SWEEP_TRADE_ALLOCATION_PCT=0.05
FLOW_SWEEP_TARGET_DELTA=0.30
ALPACA_DATA_FEED=iex
```

The Docker launcher mounts `$HOME/.aws` read-only so the container can use the local `trading_bot` AWS profile.

## Run Locally

```bash
chmod +x setup.sh
./setup.sh
```

Manual Docker run:

```bash
docker build -t flow-sweep-bot .
mkdir -p runtime
docker rm -f flow-sweep-bot 2>/dev/null || true
docker run --rm --name flow-sweep-bot \
  --env-file .env \
  -e AWS_PROFILE="${AWS_PROFILE:-trading_bot}" \
  -e AWS_REGION="${AWS_REGION:-us-east-2}" \
  -v "$HOME/.aws:/root/.aws:ro" \
  -v "$(pwd)/runtime:/app/runtime" \
  flow-sweep-bot
```

## Disclaimer

Use at your own risk. This software is for educational and experimental purposes only. Options trading carries substantial risk, and automated trading can execute orders rapidly. Test in paper trading before considering live mode.
