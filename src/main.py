import ccxt.pro
import asyncio
import logging
import time
import json
import pprint
import sys
import datetime
import configparser
import yaml
from asyncio import gather


from storage import meta, table_ohlcv, table_orderbook, table_trades, table_ticker, table_logs
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from sqlalchemy.orm import sessionmaker

print('Python version: ', sys.version_info)
if sys.version_info < (3,7):
    print("This script requires Python 3.7 or higher.")
    sys.exit(1)
print('CCXT version', ccxt.pro.__version__)

# ###############################################################


async def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)
    
async def exchange_exists(exchange_name):
    pass

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

            async with engine() as session:
                async with session.begin():
                    await session.execute(
                        table_orderbook.insert().values(
                            exchange = name,
                            symbol = orderbook['symbol'],
                            asks = orderbook['asks'],
                            bids = orderbook['bids'],
                            nonce = orderbook['nonce'],
                            datetime = datetime.datetime.fromisoformat(orderbook['datetime']),
                            created_at = orderbook['timestamp']))
        except Exception as e:
            print(str(e))
            raise e
    
async def watch_trades(exchange, symbol, engine):
    '''
    Watch the trades for a specific symbol.
    Trades are unique from the api
    
    :param exchange: The exchange object
    :param symbol: The trading symbol
    :param engine: SQLalchemy engine
    '''
    name = getattr(exchange, 'name')
    
    while True:
        try:
            trades = await exchange.watch_trades(symbol)
            
            async with engine() as session:
                async with session.begin():
                    for trade in trades:
                        await session.execute(
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
                                created_at = trade['timestamp']))                    
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
    :param engine: SQLalchemy engine
    '''
    last_candle = None
    name = getattr(exchange, 'name')
    
    while True:
        try:
            candle = await exchange.watch_ohlcv(symbol, timeframe, None, candle_limit)
            
            async with engine() as session:
                async with session.begin():
                    if last_candle is None:
                        last_candle = candle
                    
                    #if timestamps are not equal
                    if last_candle[0][0] != candle[0][0]:
                        datetime_str = exchange.iso8601(last_candle[0][0])
                        datetime_obj = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        
                        await session.execute(
                            table_ohlcv.insert().values(
                                exchange = name,
                                symbol = symbol,
                                open_price = last_candle[0][1],
                                high_price = last_candle[0][2],
                                low_price = last_candle[0][3],
                                close_price = last_candle[0][4],
                                candle_volume = last_candle[0][5],
                                created_at = last_candle[0][0],
                                datetime = datetime.datetime.utcfromtimestamp(last_candle[0][0]/1000)))
                    last_candle = candle

        except Exception as e:
            print(str(e))
            raise e
            
async def watch_ticker(exchange, symbol, engine):
    '''
    Watch the ticker of an exchange for a specific symbol.

    :param exchange: The exchange object
    :param symbol: The trading symbol
    :param engine: SQLalchemy engine
    '''
    name = getattr(exchange, 'name')
    
    while True:
        try:
            ticker = await exchange.watch_ticker(symbol)
            
            async with engine() as session:
                async with session.begin():
                    await session.execute(
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
                            created_at = ticker['timestamp']))            
        except Exception as e:
            print(str(e))
            raise e
        
async def watch_market_data(exchange, symbol, engine, timeframe, candle_limit, orderbook_depth):
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
    await asyncio.gather(*loops)

async def exchange_loop(exchange_id, exchange, symbols, engine, timeframe, candle_limit, orderbook_depth):
    print(f"Processing {exchange_id} with symbols {symbols}")
    await gather(*[watch_market_data(exchange, symbol, engine, timeframe, candle_limit, orderbook_depth) for symbol in symbols])
    await exchange.close()

async def database_setup(user, password, host, port, db_name):
    
    # Create temporary engine to create database    
    temp_url = f'mysql+aiomysql://{user}:{password}@{host}:{port}/'
    temp_engine = create_async_engine(temp_url, echo=True)
    async with temp_engine.begin() as conn:
        await conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
    await temp_engine.dispose()
    
    # Connect to database
    engine_url = f'{temp_url}{db_name}'
    engine = create_async_engine(engine_url, echo=True)
    
    # Create async db session
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    
    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(meta.create_all)

    return async_session

async def initialize_exchanges(exchange_names):
    # Check exchanges and instantiate exchange objects and place in dict
    valid_exchanges = {}
    for exchange_name in exchange_names:
        try:
            exchange_class = getattr(ccxt.pro, exchange_name)            
            exchange = exchange_class({'enableRateLimit': True, 
                                       'async_support': True,
                                       'newUpdates': True,
                                       'verbose': False})
            valid_exchanges[exchange_name] = exchange
        except AttributeError:
            print(f"Exchange {exchange_name} is not supported by ccxt.pro")
        except Exception as e:
            print(f"An error occurred while initializing {exchange_name}: {str(e)}")
    return valid_exchanges

async def main():
    
    # Read config file
    config = await load_config('../config/config.yaml')
    
    # Initialize database
    async_session = await database_setup(user = config['credentials']['user'],
                                                 password = config['credentials']['password'],
                                                 host = config['credentials']['host'],
                                                 port = config['credentials']['port'],
                                                 db_name = config['credentials']['db_name'])
    # Initialize exchange 
    exchange_objects = await initialize_exchanges(exchange_names = config['exchanges'].keys())
    
    # Load markets
    for exchange_id, exchange in exchange_objects.items():
        try:
            markets = await exchange.load_markets()
            print(f"Markets loaded for {exchange_id}")
        except Exception as e:
            print(f"Error loading markets for {exchange_id}: {str(e)}")
    

    # Run loops for each exchange and its symbols
    loops = [exchange_loop(
        exchange_id, 
        exchange_objects[exchange_id], 
        config['exchanges'][exchange_id]['symbols'], 
        async_session, 
        config['settings']['timeframe'], 
        config['settings']['candle_limit'], 
        config['settings']['orderbook_depth']) 
             for exchange_id in config['exchanges']]
    
    await gather(*loops, return_exceptions=False)
        
if __name__ == "__main__":
    asyncio.run(main())
    
# https://github.com/ccxt/ccxt/blob/master/examples/py/async-fetch-order-book-from-many-exchanges.py