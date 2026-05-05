# Alpaca Power Bar Options Trading Bot

An automated options trading bot built with Python and `alpaca-py`. This bot uses Alpaca's WebSocket API to stream real-time 1-minute market data, evaluating high-beta, large-cap equities for a specific "Power Bar" technical setup on the 2-minute timeframe. When triggered, it automatically sizes and routes long Call or Put option orders with predefined risk management.

## Features

* **Real-Time Data Ingestion:** Utilizes Alpaca's `StockDataStream` for zero-latency bar evaluation.
* **Automated Options Execution:** Scans the options chain for the nearest expiration with an absolute Delta $\le$ 0.30 and a premium under $4.00.
* **Dynamic Position Sizing:** Automatically calculates position sizes based on 5% of your available Account Buying Power.
* **Advanced Trade Management:** * Automatically exits 75% of the position at 1.5R (Reward/Risk).
  * Exits the remaining 25% at 2.0R.
  * Triggers a Stop Loss if the underlying asset closes above/below the origin of the Power Bar.
* **Dockerized:** Ready to run locally or deploy to cloud infrastructure (AWS ECS, EKS, etc.) with minimal configuration.

## Strategy: The "Power Bar"

The bot scans a predefined list of tickers (`TSLA`, `NVDA`, `AMD`, `META`, `NFLX`, `MSFT`, `AAPL`, `AMZN`) for the following conditions on a 2-minute chart:
1. **Volatility/Size:** The current candle's body is at least 2x larger than the average body size of the previous 5 candles.
2. **Moving Average Anchor:** The candle opens near the 20-period Simple Moving Average (SMA).
3. **Liquidity Sweep / Resistance Break:** The candle closes above the highest high (for longs) or below the lowest low (for shorts) of the previous 10 periods.

## Prerequisites

* An [Alpaca](https://alpaca.markets/) Trading Account (Paper trading highly recommended for initial setup).
* Docker installed on your local machine.
* *For macOS users using Colima:* Ensure Colima is installed and running to manage your Docker daemon.

## Installation & Setup

1. **Clone or create the project directory** and ensure `main.py`, `requirements.txt`, and `Dockerfile` are present.
2. **Create an Environment File:**
   In the root of the project directory, create a file named `.env` and add your Alpaca Paper API credentials:
   ```env
   ALPACA_API_KEY=your_paper_api_key_here
   ALPACA_SECRET_KEY=your_paper_secret_key_here

## Mac OS X commands:

- colima start
- docker build -t powerbar-bot .
- docker run --env-file .env powerbar-bot

### Issues?

- colima stop
- colima delete
- colima start

## DISCLAIMER
USE AT YOUR OWN RISK. This software is for educational and experimental purposes only. Options trading carries a high level of risk and may not be suitable for all investors. The automated nature of this script means it can execute trades rapidly and incur losses quickly. Always run new algorithmic trading scripts in a Paper Trading environment over an extended period to verify logic before deploying real capital. The authors of this script assume no responsibility for any financial losses incurred.