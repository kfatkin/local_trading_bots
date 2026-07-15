import boto3
import pandas_market_calendars as mcal
from botocore.config import Config

from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.live.stock import StockDataStream
from alpaca.trading.client import TradingClient
from alpaca.trading.stream import TradingStream

from .config import API_KEY, AWS_PROFILE, AWS_REGION, PAPER, SECRET_KEY, UW_TABLE_NAME


trade_client = TradingClient(API_KEY, SECRET_KEY, paper=PAPER)
stock_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
option_client = OptionHistoricalDataClient(API_KEY, SECRET_KEY)
raw_option_client = OptionHistoricalDataClient(API_KEY, SECRET_KEY, raw_data=True)
stock_stream = StockDataStream(API_KEY, SECRET_KEY)
trading_stream = TradingStream(API_KEY, SECRET_KEY, paper=PAPER)
nyse_calendar = mcal.get_calendar("NYSE")


def boto3_table():
    config = Config(retries={"max_attempts": 3, "mode": "adaptive"}, connect_timeout=5, read_timeout=10)
    if AWS_PROFILE:
        session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    else:
        session = boto3.Session(region_name=AWS_REGION)
    dynamodb = session.resource("dynamodb", region_name=AWS_REGION, config=config)
    return dynamodb.Table(UW_TABLE_NAME)
