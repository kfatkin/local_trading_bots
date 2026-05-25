from datetime import datetime, time as dt_time, timedelta

import pandas as pd
import yfinance as yf
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from .clients import nyse_calendar, stock_client
from .config import ALPACA_DATA_FEED, ET, LOGGER, PREMARKET_START, REGULAR_OPEN, UTC


def get_schedule(start_date, end_date):
    return nyse_calendar.schedule(start_date=start_date, end_date=end_date)


def resolve_trading_sessions(now_et=None):
    now_et = now_et or datetime.now(ET)
    start = now_et.date() - timedelta(days=21)
    end = now_et.date() + timedelta(days=10)
    schedule = get_schedule(start, end)
    if schedule.empty:
        raise RuntimeError("Unable to load NYSE schedule")

    now_utc = now_et.astimezone(UTC)
    session_dates = [idx.date() for idx in schedule.index]
    trading_idx = None

    for idx, session_date in enumerate(session_dates):
        market_close = schedule.iloc[idx]["market_close"].to_pydatetime().astimezone(UTC)
        if session_date > now_et.date() or (session_date == now_et.date() and now_utc <= market_close):
            trading_idx = idx
            break

    if trading_idx is None or trading_idx == 0:
        raise RuntimeError("Unable to resolve current and prior NYSE sessions")

    trading_day = session_dates[trading_idx]
    prior_day = session_dates[trading_idx - 1]
    return schedule, trading_idx, trading_day, prior_day


def previous_calendar_week_sessions(schedule, trading_day):
    week_start = trading_day - timedelta(days=trading_day.weekday() + 7)
    week_end = week_start + timedelta(days=6)
    sessions = [idx.date() for idx in schedule.index if week_start <= idx.date() <= week_end]
    if sessions:
        return sessions
    prior_sessions = [idx.date() for idx in schedule.index if idx.date() < trading_day]
    return prior_sessions[-5:]


def clean_bars_df(df, symbol=None):
    if df is None or df.empty:
        return pd.DataFrame()

    bars = df.copy()
    if isinstance(bars.index, pd.MultiIndex):
        if symbol and symbol in bars.index.get_level_values(0):
            bars = bars.xs(symbol, level=0)
        elif "symbol" in bars.index.names:
            bars = bars.xs(symbol, level="symbol")

    bars.columns = [str(column).lower() for column in bars.columns]
    needed = ["open", "high", "low", "close", "volume"]
    if any(column not in bars.columns for column in needed):
        return pd.DataFrame()

    bars = bars[needed]
    bars.index = pd.to_datetime(bars.index)
    if bars.index.tz is None:
        bars.index = bars.index.tz_localize(UTC)
    bars.index = bars.index.tz_convert(ET)
    return bars.sort_index()


def alpaca_bars(symbol, timeframe, start_et, end_et):
    kwargs = {
        "symbol_or_symbols": symbol,
        "timeframe": timeframe,
        "start": start_et.astimezone(UTC),
        "end": end_et.astimezone(UTC),
    }
    if ALPACA_DATA_FEED:
        kwargs["feed"] = ALPACA_DATA_FEED
    request = StockBarsRequest(**kwargs)
    response = stock_client.get_stock_bars(request)
    return clean_bars_df(response.df, symbol=symbol)


def yahoo_bars(symbol, interval, start_et, end_et, prepost=False):
    try:
        df = yf.Ticker(symbol).history(
            interval=interval,
            start=start_et.astimezone(ET).replace(tzinfo=None),
            end=end_et.astimezone(ET).replace(tzinfo=None),
            prepost=prepost,
            auto_adjust=False,
        )
    except Exception as exc:
        LOGGER.warning("Yahoo bar fetch failed for %s: %s", symbol, exc)
        return pd.DataFrame()
    return clean_bars_df(df, symbol=symbol)


def fetch_bars(symbol, timeframe, yahoo_interval, start_et, end_et, prepost=False):
    try:
        bars = alpaca_bars(symbol, timeframe, start_et, end_et)
    except Exception as exc:
        LOGGER.warning("Alpaca bar fetch failed for %s: %s", symbol, exc)
        bars = pd.DataFrame()

    if not bars.empty:
        return bars

    bars = yahoo_bars(symbol, yahoo_interval, start_et, end_et, prepost=prepost)
    if not bars.empty:
        LOGGER.info("Using Yahoo fallback bars for %s", symbol)
    return bars


def daily_bars(symbol, start_day, end_day):
    start_et = datetime.combine(start_day, dt_time(0, 0), ET)
    end_et = datetime.combine(end_day + timedelta(days=1), dt_time(0, 0), ET)
    return fetch_bars(symbol, TimeFrame.Day, "1d", start_et, end_et, prepost=False)


def intraday_bars(symbol, start_et, end_et, prepost=True):
    return fetch_bars(symbol, TimeFrame(1, TimeFrameUnit.Minute), "1m", start_et, end_et, prepost=prepost)


def get_day_high_low(symbol, session_day):
    bars = daily_bars(symbol, session_day, session_day)
    if bars.empty:
        start_et = datetime.combine(session_day, REGULAR_OPEN, ET)
        end_et = datetime.combine(session_day, dt_time(16, 0), ET)
        bars = intraday_bars(symbol, start_et, end_et, prepost=False)
    if bars.empty:
        return None
    session_bars = bars[bars.index.date == session_day]
    if session_bars.empty:
        return None
    return float(session_bars["high"].max()), float(session_bars["low"].min())


def get_week_high_low(symbol, session_days):
    if not session_days:
        return None
    bars = daily_bars(symbol, min(session_days), max(session_days))
    if bars.empty:
        return None
    session_set = set(session_days)
    mask = [idx.date() in session_set for idx in bars.index]
    session_bars = bars[mask]
    if session_bars.empty:
        return None
    return float(session_bars["high"].max()), float(session_bars["low"].min())


def get_premarket_high_low(symbol, trading_day):
    start_et = datetime.combine(trading_day, PREMARKET_START, ET)
    end_et = datetime.combine(trading_day, REGULAR_OPEN, ET)
    bars = intraday_bars(symbol, start_et, end_et, prepost=True)
    if bars.empty:
        return None
    premarket = bars[(bars.index >= start_et) & (bars.index < end_et)]
    if premarket.empty:
        return None
    return float(premarket["high"].max()), float(premarket["low"].min())
