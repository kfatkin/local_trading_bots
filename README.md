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

Contracts are selected from the nearest active expiration, including 0DTE when available. The selector first scopes candidates to contracts within `0.15` absolute delta of the configured `0.30` target when available, builds a candidate band from the three contracts just below target delta and the two just above it, then ranks that band by liquidity using spread, recent option volume, and open interest. Contract previews are refreshed when the market-open setup is prepared, on the normal 5-minute review cadence, and immediately before an entry order is submitted.

## Risk And Exits

- Default account mode is paper trading through `ALPACA_PAPER=true` or the built-in default.
- Position size is 5% of Alpaca account balance, using portfolio value/equity/cash before falling back to buying power.
- If the 5% allocation cannot buy one contract, the bot can still buy one contract when that contract costs no more than 20% of account balance.
- Stop is the sweep candle extreme.
- Target is bot-managed. The bot uses the closest opposing key level only when that level offers at least 2R from the underlying entry-to-stop distance; otherwise it uses a fixed 2R underlying target so trades can still run into all-time highs or lows.
- Once the underlying reaches 1.5R, the bot switches the stop mode to option breakeven and exits if the option market price falls back to the entry option price.
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
FLOW_SWEEP_TARGET_R_MULTIPLE=2.0
FLOW_SWEEP_BREAKEVEN_TRIGGER_R_MULTIPLE=1.5
FLOW_SWEEP_OPTION_PREVIEW_REFRESH_SECONDS=300
FLOW_SWEEP_OPTION_EXPIRATION_LOOKAHEAD_DAYS=21
FLOW_SWEEP_OPTION_MAX_SPREAD_PCT=0.30
FLOW_SWEEP_OPTION_MIN_VOLUME=1
FLOW_SWEEP_OPTION_MIN_OPEN_INTEREST=0
FLOW_SWEEP_OPTION_MAX_DELTA_DISTANCE=0.15
FLOW_SWEEP_OPTION_CANDIDATES_BELOW_TARGET=3
FLOW_SWEEP_OPTION_CANDIDATES_ABOVE_TARGET=2
FLOW_SWEEP_OPTION_MAX_ACCOUNT_BALANCE_PCT=0.20
FLOW_SWEEP_OPTION_MAX_QUOTE_AGE_SECONDS=300
FLOW_SWEEP_TRADE_EVENT_LOG_LIMIT=300
ALPACA_DATA_FEED=iex
BOT_DASHBOARD_ENABLED=true
BOT_DASHBOARD_HOST=127.0.0.1
BOT_DASHBOARD_PORT=8765
```

The Docker launcher mounts `$HOME/.aws` read-only so the container can use the local `trading_bot` AWS profile.

## Code Layout

The runtime entrypoint is intentionally small: `main.py` delegates to the `flow_sweep` package.

- `flow_sweep/config.py`: environment, constants, logging, runtime paths.
- `flow_sweep/clients.py`: Alpaca, DynamoDB, and NYSE calendar clients.
- `flow_sweep/state.py`: runtime state, persistence, and locks.
- `flow_sweep/market_data.py`: Alpaca/Yahoo market data and key level helpers.
- `flow_sweep/flow_data.py`: `uw-hub` DynamoDB reads and flow-bias scoring.
- `flow_sweep/strategy.py`: setup preparation, entries, exits, streams, reconciliation.
- `flow_sweep/dashboard.py`: local dashboard and `/api/status` endpoint.
- `flow_sweep/app.py`: CLI orchestration for live mode and smoke tests.

## Dashboard

When the bot is running, open:

```text
http://127.0.0.1:8765
```

The dashboard shows the current session, prior session, Alpaca account and market-clock status, flow decision for each watched symbol, all six key levels, the planned option contract, target levels, active positions, Alpaca broker open orders, pending bot orders, recent completed 5-minute bars, and the current session trade log.

The key-level column lists premarket low/high, prior-day low/high, and prior-week low/high. Bullish setups mark support levels as observed for call entries after a sweep and 5-minute close back above. Bearish setups mark resistance levels as observed for put entries after a sweep and 5-minute close back below. The opposite side is shown as skipped for that setup, and upcoming-session premarket levels stay pending until they are available.

Each watched symbol also has an expandable high-score flow row showing all prior-session `_flow_scores_trading_bot` records above the configured score threshold. These are aggregate score rows from `uw-hub`; contract columns will populate when the underlying score row includes contract fields.

The decision table makes the planned entries explicit:

- Bullish symbols show the lows where the bot will look for call entries.
- Bearish symbols show the highs where the bot will look for put entries.

The planned contract column refreshes from Alpaca on the 5-minute strategy cadence and is also refreshed opportunistically by the dashboard with a 5-minute cache. It shows the selected contract symbol, strike, expiration, delta, gamma, theta, ask price, estimated cost per contract, planned quantity, spread, recent option volume, open interest, account balance, allocation amount, and any sizing/liquidity warnings. Live entries force one more Alpaca contract recheck right before order submission, then run an entry preflight against the selected contract, account, buying power, market clock, liquidity checks, and quote freshness before submitting the market buy.

When a trade is active, the dashboard combines Alpaca broker truth with the bot plan. The active-position row shows Alpaca quantity, average entry, current mark, market value, cost basis, unrealized PnL, the bot-managed stop mode, the 1.5R breakeven trigger, the breakeven option stop price, and the selected take-profit target. Broker open option orders are listed separately from bot-local pending orders.

The daily trade log is persisted in `runtime/state.json` and resets with each trading session. It records entry skips/blocks, submitted entry orders, entry fills, submitted exits, exit fills, terminal order states, breakeven activation, and broker reconciliation events. This gives the operator a bottom-of-dashboard audit trail even after a local restart.

The same status is available as JSON at:

```text
http://127.0.0.1:8765/api/status
```

## Run Locally

```bash
chmod +x setup.sh
./setup.sh
```

`setup.sh` uses an isolated Docker config at `runtime/docker-config` by default. This avoids WSL/Docker Desktop credential-helper failures while pulling public base images such as `python:3.11-slim`. It also defaults `DOCKER_BUILDKIT=0` on this machine because the legacy builder path avoids the same credential-helper failure. If you prefer your normal Docker config, run with `USE_HOST_DOCKER_CONFIG=1 ./setup.sh`; if BuildKit is fixed locally, run with `DOCKER_BUILDKIT=1 ./setup.sh`.

Before startup, `setup.sh` stops older local bot containers and tries to kill any local process listening on the dashboard port, which defaults to `8765`.

The script starts the container detached, waits for the dashboard health endpoint, then exits back to your shell. To watch the live bot logs after startup:

```bash
docker logs -f flow-sweep-bot
```

Or run startup and immediately follow logs:

```bash
BOT_FOLLOW_LOGS=1 ./setup.sh
```

Safe startup smoke test without opening Alpaca streams or submitting orders:

```bash
python3 main.py --smoke-test
```

No-trade Alpaca entry preflight for the currently selected contracts:

```bash
python3 main.py --entry-preflight
```

This checks the live Alpaca account, clock, contract chain, snapshots, liquidity filters, buying power, and quote freshness without submitting orders. When run outside market hours, quote-age issues are reported as warnings rather than blocking failures.

Historical backtest using UW scored-flow rows and underlying price data:

```bash
python3 scripts/backtest_flow_sweep.py --sessions 40
```

The backtest writes markdown and CSV logs to `runtime/backtests/`. It measures the strategy in underlying R multiples and approximates the breakeven stop as an underlying entry-price stop because historical option bid/ask replay is not included.

Docker smoke test after building the image:

```bash
docker run --rm --env-file .env --entrypoint python flow-sweep-bot main.py --smoke-test
```

Manual Docker run:

```bash
export DOCKER_CONFIG="${DOCKER_CONFIG:-$(pwd)/runtime/docker-config}"
mkdir -p "$DOCKER_CONFIG"
test -f "$DOCKER_CONFIG/config.json" || printf '{}\n' > "$DOCKER_CONFIG/config.json"
docker build -t flow-sweep-bot .
mkdir -p runtime
docker rm -f flow-sweep-bot 2>/dev/null || true
docker run --rm --name flow-sweep-bot \
  --env-file .env \
  -e AWS_PROFILE="${AWS_PROFILE:-trading_bot}" \
  -e AWS_REGION="${AWS_REGION:-us-east-2}" \
  -e BOT_DASHBOARD_HOST="0.0.0.0" \
  -e BOT_DASHBOARD_PORT="${BOT_DASHBOARD_PORT:-8765}" \
  -p "${BOT_DASHBOARD_PORT:-8765}:${BOT_DASHBOARD_PORT:-8765}" \
  -v "$HOME/.aws:/root/.aws:ro" \
  -v "$(pwd)/runtime:/app/runtime" \
  flow-sweep-bot
```

## Disclaimer

Use at your own risk. This software is for educational and experimental purposes only. Options trading carries substantial risk, and automated trading can execute orders rapidly. Test in paper trading before considering live mode.
