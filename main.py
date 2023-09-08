#main.py
import ccxt.pro
import asyncio
import logging
import time
import json
import pprint
import sys
import datetime

from storage import meta, table_ohlcv, table_orderbook, table_trades, table_ticker, table_logs
from sqlalchemy import create_engine

#symbols
btc_inverse_perp = 'BTC/USD:BTC'
btc_linear_perp = 'BTC/USDT:USDT'

#settings
timeframe = '1m'
orderbook_depth = 50
timeout = 10 #seconds
candle_limit = 1

print('Python version: ', sys.version_info)
if sys.version_info < (3,7):
    print("This script requires Python 3.7 or higher.")
    sys.exit(1)
print('CCXT version', ccxt.pro.__version__)

async def watch_order_book(exchange, symbol, orderbook_depth, engine):
    '''
    Watch the order book for a specific symbol.
    Orderbook is unique from api, updates on deltas
    
    :param exchange: The exchange object
    :param symbol: The trading symbol
    :param orderbook_depth: The depth of the order book
    :param engine: SQLalchemy engine
    '''
    name = getattr(exchange, 'name')
    
    while True:
        try:
            orderbook = await exchange.watch_order_book(symbol, orderbook_depth)
            conn = await engine.connect()

            async with conn.begin():
                await conn.execute(
                    table_orderbook.insert().values(
                        exchange = name,
                        symbol = orderbook['symbol'],
                        asks = orderbook['asks'],
                        bids = orderbook['bids'],
                        nonce = orderbook['nonce'],
                        datetime = datetime.datetime.fromisoformat(orderbook['datetime']),
                        created_at = orderbook['timestamp']
                    )
            await conn.close()
        
        except Exception as e:
            print(str(e))
            raise e
    
async def watch_trades(exchange, symbol, engine):
    '''
    Watch the trades for a specific symbol.
    Trades are unique from the api
    
    :param exchange: The exchange object
    :param symbol: The trading symbol
    '''
    name = getattr(exchange, 'name')
    
    while True:
        try:
            trades = await exchange.watch_trades(symbol)
            
            conn = await engine.connect()
            for trade in trades:
                async with conn.begin():
                    await conn.execute(
                        table_trades.insert().values(
                            exchange = name,
                            symbol = symbol,
                            trade_id = trade['id'],
                            order_id = trade['order'],
                            order_type = trade['type'],
                            trade_side = trade['side'],
                            taker_maker = trade['takerOrMaker'],
                            executed_price = trade['price'],
                            base_amount = trade['amount'],
                            cost = trade['cost'],
                            fee = trade['fee'],
                            fees = trade['fees'] if trade['fees'] else None,
                            datetime = datetime.datetime.fromisoformat(trade['datetime']),
                            created_at = trade['timestamp'])
                    )
            await conn.close()
                    
        except Exception as e:
            print(str(e))
            raise e

async def watch_ohlcv(exchange, symbol, timeframe, candle_limit, engine):
    '''
    Watch the OHLCV data for a specific symbol.

    :param exchange: The exchange object
    :param symbol: The trading symbol
    :param timeframe: The timeframe for the OHLCV data
    :param candle_limit: The number of candles to fetch
    '''
    last_candle = None
    name = getattr(exchange, 'name')
    
    while True:
        try:
            candle = await exchange.watch_ohlcv(symbol, timeframe, None, candle_limit)
            conn = await engine.connect()
            
            if last_candle is None:
                last_candle = candle
            
            #if timestamps are not equal
            if last_candle[0][0] != candle[0][0]:
                datetime_str = exchange.iso8601(last_candle[0][0])
                datetime_obj = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')

                async with conn.begin():
                    await conn.execute(
                        table_ohlcv.insert().values(
                            exchange = name,
                            symbol = symbol,
                            open_price = last_candle[0][1],
                            high_price = last_candle[0][2],
                            low_price = last_candle[0][3],
                            close_price = last_candle[0][4],
                            candle_volume = last_candle[0][5],
                            created_at = last_candle[0][0],
                            datetime = datetime.datetime.utcfromtimestamp(last_candle[0][0]/1000))
                        )
            await conn.close()
            last_candle = candle

        except Exception as e:
            print(str(e))
            raise e
            
async def watch_ticker(exchange, symbol, engine):
    '''
    Watch the ticker of an exchange for a specific symbol.

    :param exchange: The exchange object
    :param symbol: The trading symbol
    '''
    name = getattr(exchange, 'name')
        
   while True:
        try:
            ticker = await exchange.watch_ticker(symbol)
            
            conn = await engine.connect()
            async with conn.begin():
                await conn.execute(
                    table_ticker.insert().values(
                        exchange = name,
                        symbol = symbol,
                        ask = ticker['ask'],
                        ask_volume = ticker['askVolume'],
                        bid = ticker['bid'],
                        bid_volume = ticker['bidVolume'],
                        open_24h = ticker['open'],
                        high_24h = ticker['high'],
                        low_24h = ticker['low'],
                        close_24h = ticker['close'],
                        last_price = ticker['last'],
                        vwap = ticker['vwap'],
                        previous_close_price = ticker['previousClose'],
                        price_change = ticker['change'],
                        percentage_change = ticker['percentage'],
                        average_price = ticker['average'],
                        base_volume = ticker['baseVolume'],
                        quote_volume = ticker['quoteVolume'],
                        info = ticker['info'],
                        datetime = datetime.datetime.fromisoformat(ticker['datetime']),
                        created_at = ticker['timestamp'])
                    )
            await conn.close()
            
        except Exception as e:
            print(str(e))
            raise e

async def main():
    engine = create_async_engine(
        'sqlite+aiosqlite:///dataaa.db', 
        echo = True)
    async with engine.begin() as conn:
        await conn.run_sync(meta.create_all)
    
    exchange = ccxt.pro.bybit({'newUpdates':True,'enableRateLimit': True, 'verbose':True})
    
    await exchange.load_markets()        
    symbol = btc_inverse_perp
    
    loops = []
    if exchange.has["watchOHLCV"]:
        loops.append(
            watch_ohlcv(exchange, symbol, timeframe, candle_limit, engine))
    if exchange.has["watchTicker"]:
        loops.append(
            watch_ticker(exchange, symbol, engine))
    if exchange.has["watchTrades"]:
        loops.append(
            watch_trades(exchange, symbol, engine))
    if exchange.has["watchOrderBook"]:
        loops.append(
            watch_order_book(exchange, symbol, orderbook_depth, engine))
    
    while True:
        await asyncio.gather(*loops)
    await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())
