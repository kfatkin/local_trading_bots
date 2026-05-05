import os
import math
import asyncio
from datetime import datetime
from alpaca.trading.client import TradingClient
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, OptionChainRequest
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.live.stock import StockDataStream

# --- CONFIGURATION ---
API_KEY = os.getenv('ALPACA_API_KEY')
SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
PAPER = True 

trade_client = TradingClient(API_KEY, SECRET_KEY, paper=PAPER)
stock_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
option_client = OptionHistoricalDataClient(API_KEY, SECRET_KEY)
stock_stream = StockDataStream(API_KEY, SECRET_KEY)

SYMBOLS = ['TSLA', 'NVDA', 'AMD', 'META', 'NFLX', 'MSFT', 'AAPL', 'AMZN']

# State management for active trades
# Format: { 'TSLA': {'option_symbol': 'TSLA26...', 'side': 'CALL', 'qty': 10, 'tp1_qty': 7, ...} }
active_positions = {}

def get_power_bar_setup(symbol):
    """Evaluates the 2-minute chart for a Power Bar setup."""
    req = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame(2, TimeFrameUnit.Minute),
        limit=50
    )
    bars = stock_client.get_stock_bars(req).df
    if bars.empty: return None

    bars['sma20'] = bars['close'].rolling(window=20).mean()
    current_bar = bars.iloc[-1]
    prev_bars = bars.iloc[-6:-1]
    
    local_resistance = bars['high'].iloc[-11:-1].max()
    local_support = bars['low'].iloc[-11:-1].min()
    
    avg_body_size = (prev_bars['close'] - prev_bars['open']).abs().mean()
    current_body_size = abs(current_bar['close'] - current_bar['open'])
    
    is_power_bar = current_body_size > (avg_body_size * 2)
    near_sma = abs(current_bar['open'] - current_bar['sma20']) / current_bar['sma20'] < 0.002
    
    if is_power_bar and near_sma and current_bar['close'] > current_bar['open']:
        if current_bar['close'] > local_resistance and current_bar['close'] > current_bar['sma20']:
            return 'CALL', current_bar['close'], current_bar['low']
            
    if is_power_bar and near_sma and current_bar['close'] < current_bar['open']:
        if current_bar['close'] < local_support and current_bar['close'] < current_bar['sma20']:
            return 'PUT', current_bar['close'], current_bar['high']
            
    return None

def get_best_option_contract(symbol, option_type):
    req = OptionChainRequest(underlying_symbol=symbol)
    chain = option_client.get_option_chain(req)
    valid_contracts = []
    
    for contract_symbol, data in chain.items():
        if data.contract_type.lower() != option_type.lower(): continue
            
        premium = data.latest_quote.ask_price if data.latest_quote else 0
        delta = abs(data.greeks.delta) if data.greeks and data.greeks.delta else 1.0
        
        if 0 < premium <= 4.00 and delta <= 0.30:
            valid_contracts.append({
                'symbol': contract_symbol, 'premium': premium, 
                'delta': delta, 'expiration': data.expiration_date
            })
            
    if not valid_contracts: return None, None
    valid_contracts.sort(key=lambda x: (x['expiration'], abs(0.30 - x['delta'])))
    return valid_contracts[0]['symbol'], valid_contracts[0]['premium']

def execute_entry(symbol, setup_data):
    option_type, entry_price, stop_loss_price = setup_data
    
    if symbol in active_positions:
        return # Already in a trade for this underlying

    contract_symbol, premium = get_best_option_contract(symbol, option_type)
    if not contract_symbol: return
        
    buying_power = float(trade_client.get_account().buying_power)
    trade_allocation = buying_power * 0.05
    contract_cost = premium * 100
    qty = math.floor(trade_allocation / contract_cost)
    
    if qty < 4: 
        print(f"Skipping {symbol}: Need qty >= 4 to split 75/25 correctly. Calculated: {qty}")
        return

    # Calculate Risk Multiples based on underlying stock price
    risk = abs(entry_price - stop_loss_price)
    
    if option_type == 'CALL':
        tp1_price = entry_price + (risk * 1.5)
        tp2_price = entry_price + (risk * 2.0)
    else:
        tp1_price = entry_price - (risk * 1.5)
        tp2_price = entry_price - (risk * 2.0)

    tp1_qty = math.floor(qty * 0.75)
    tp2_qty = qty - tp1_qty

    print(f"[{datetime.now()}] ENTER {option_type} on {symbol} @ {entry_price}. SL: {stop_loss_price}, TP1: {tp1_price}, TP2: {tp2_price}")
    
    req = MarketOrderRequest(symbol=contract_symbol, qty=qty, side=OrderSide.BUY, time_in_force=TimeInForce.DAY)
    trade_client.submit_order(order_data=req)
    
    # Save to state manager
    active_positions[symbol] = {
        'option_symbol': contract_symbol,
        'option_type': option_type,
        'sl_price': stop_loss_price,
        'tp1_price': tp1_price,
        'tp2_price': tp2_price,
        'tp1_qty': tp1_qty,
        'tp2_qty': tp2_qty,
        'total_qty': qty
    }

def execute_exit(symbol, exit_qty, reason):
    pos = active_positions[symbol]
    print(f"[{datetime.now()}] EXIT ({reason}) {exit_qty}x {pos['option_symbol']}")
    
    req = MarketOrderRequest(
        symbol=pos['option_symbol'], qty=exit_qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY
    )
    trade_client.submit_order(order_data=req)
    
    pos['total_qty'] -= exit_qty
    if pos['total_qty'] <= 0:
        del active_positions[symbol]

async def handle_bar(bar):
    """Processes incoming 1-minute bars from WebSocket."""
    symbol = bar.symbol
    
    # 1. Check Exit Conditions (Stop Loss is based on bar close, TPs on bar high/low)
    if symbol in active_positions:
        pos = active_positions[symbol]
        
        # Stop Loss Logic
        sl_hit = (pos['option_type'] == 'CALL' and bar.close < pos['sl_price']) or \
                 (pos['option_type'] == 'PUT' and bar.close > pos['sl_price'])
                 
        if sl_hit:
            execute_exit(symbol, pos['total_qty'], "STOP_LOSS")
            return

        # Take Profit Logic (Using High/Low for intraday sweeps)
        tp1_hit = (pos['option_type'] == 'CALL' and bar.high >= pos['tp1_price']) or \
                  (pos['option_type'] == 'PUT' and bar.low <= pos['tp1_price'])
                  
        tp2_hit = (pos['option_type'] == 'CALL' and bar.high >= pos['tp2_price']) or \
                  (pos['option_type'] == 'PUT' and bar.low <= pos['tp2_price'])

        if tp1_hit and pos['tp1_qty'] > 0:
            execute_exit(symbol, pos['tp1_qty'], "TP1 (1.5R)")
            pos['tp1_qty'] = 0 # Mark TP1 as filled
            
        if tp2_hit and pos['tp2_qty'] > 0:
            execute_exit(symbol, pos['tp2_qty'], "TP2 (2.0R)")
            pos['tp2_qty'] = 0

    # 2. Check Entry Conditions (Only trigger setup scan on even minutes)
    if bar.timestamp.minute % 2 == 0:
        setup = get_power_bar_setup(symbol)
        if setup:
            execute_entry(symbol, setup)

async def main():
    print("Connecting to Alpaca WebSocket...")
    stock_stream.subscribe_bars(handle_bar, *SYMBOLS)
    await stock_stream._run_forever()

if __name__ == "__main__":
    asyncio.run(main())