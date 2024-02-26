import ccxt.pro
import asyncio
import sys
import datetime
import yaml

from typing import List, Callable
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from storage import meta, table_ohlcv, table_orderbook
from storage import table_trades, table_ticker, table_logs

print('Python version: ', sys.version_info)
if sys.version_info < (3, 7):
    print("This script requires Python 3.7 or higher.")
    sys.exit(1)
print('CCXT version', ccxt.pro.__version__)


async def exchange_exists(exchange_name):
    pass


async def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


async def watch_order_book(exchange: ccxt.pro.Exchange,
                           symbol: str,
                           orderbook_depth: int,
                           session_factory: Callable[[], AsyncSession]) -> None:
    '''
    Continously watch the orderbook for a
    specific symbol / exchange pair
    and update its table with the new realtime info.

    :param exchange: The exchange object
    :param symbol: The specific trading symbol to watch
    :param orderbook_depth: The orderbook depth
    :param session_factory: Generates AsyncSession for database
           operations.
    '''
    name = getattr(exchange, 'name')

    while True:
        try:
            orderbook = await exchange.watch_order_book(symbol, orderbook_depth)

            async with session_factory() as session:
                async with session.begin():
                    await session.execute(
                        table_orderbook.insert().values(
                            exchange=name,
                            symbol=orderbook['symbol'],
                            asks=orderbook['asks'],
                            bids=orderbook['bids'],
                            nonce=orderbook['nonce'],
                            datetime=datetime.datetime.fromisoformat(
                                orderbook['datetime']),
                            created_at=orderbook['timestamp']))
        except Exception as e:
            print(str(e))
            raise e


async def watch_trades(exchange: ccxt.pro.Exchange,
                       symbol: str,
                       session_factory: Callable[[], AsyncSession]) -> None:
    '''
    Continously watch the trades for a specific symbol / exchange pair
    and update its table with the new realtime info.

    :param exchange: The exchange object
    :param symbol: The specific trading symbol to watch
    :param session_factory: Generates AsyncSession for database
           operations.
    '''
    name = getattr(exchange, 'name')

    while True:
        try:
            trades = await exchange.watch_trades(symbol)

            async with session_factory() as session:
                async with session.begin():
                    for trade in trades:
                        await session.execute(
                            table_trades.insert().values(
                                exchange=name,
                                symbol=symbol,
                                trade_id=trade['id'],
                                order_id=trade['order'],
                                order_type=trade['type'],
                                trade_side=trade['side'],
                                taker_maker=trade['takerOrMaker'],
                                executed_price=trade['price'],
                                base_amount=trade['amount'],
                                cost=trade['cost'],
                                fee=trade['fee'],
                                fees=trade['fees'] if trade['fees'] else None,
                                datetime=datetime.datetime.fromisoformat(
                                    trade['datetime']),
                                created_at=trade['timestamp']))
        except Exception as e:
            print(str(e))
            raise e


async def watch_ohlcv(exchange: ccxt.pro.Exchange,
                      symbol: str,
                      timeframe: str,
                      candle_limit: int,
                      session_factory: Callable[[], AsyncSession]) -> None:
    '''
    Continously watch the ticker for a specific symbol / exchange pair
    and update its table with the new realtime info.
    OHLCV stream does not push unique / only new data,
    so it is cached and checked.

    :param exchange: The exchange object
    :param symbol: The specific trading symbol to watch
    :param timeframe: The timeframe for the OHLCV data
    :param candle_limit: The number of candles to fetch
    :param session_factory: Generates AsyncSession for database
           operations.
    '''
    last_candle = None
    name = getattr(exchange, 'name')

    while True:
        try:
            candle = await exchange.watch_ohlcv(symbol, timeframe, None, candle_limit)

            async with session_factory() as session:
                async with session.begin():
                    if last_candle is None:
                        last_candle = candle

                    #if timestamps are not equal
                    if last_candle[0][0] != candle[0][0]:

                        await session.execute(
                            table_ohlcv.insert().values(
                                exchange=name,
                                symbol=symbol,
                                open_price=last_candle[0][1],
                                high_price=last_candle[0][2],
                                low_price=last_candle[0][3],
                                close_price=last_candle[0][4],
                                candle_volume=last_candle[0][5],
                                created_at=last_candle[0][0],
                                datetime=datetime.datetime.utcfromtimestamp(
                                    last_candle[0][0]/1000)))
                    last_candle = candle
        except Exception as e:
            print(str(e))
            raise e


async def watch_ticker(exchange: ccxt.pro.Exchange,
                       symbol: str,
                       session_factory: Callable[[], AsyncSession]) -> None:
    '''
    Continously watch the ticker for a specific symbol / exchange pair
    and update its table with the new realtime info.

    :param exchange: The exchange object
    :param symbol: The specific trading symbol to watch
    :param session_factory: Generates AsyncSession for database
    operations.
    '''
    name = getattr(exchange, 'name')

    while True:
        try:
            ticker = await exchange.watch_ticker(symbol)

            async with session_factory() as session:
                async with session.begin():
                    await session.execute(
                        table_ticker.insert().values(
                            exchange=name,
                            symbol=symbol,
                            ask=ticker['ask'],
                            ask_volume=ticker['askVolume'],
                            bid=ticker['bid'],
                            bid_volume=ticker['bidVolume'],
                            open_24h=ticker['open'],
                            high_24h=ticker['high'],
                            low_24h=ticker['low'],
                            close_24h=ticker['close'],
                            last_price=ticker['last'],
                            vwap=ticker['vwap'],
                            previous_close_price=ticker['previousClose'],
                            price_change=ticker['change'],
                            percentage_change=ticker['percentage'],
                            average_price=ticker['average'],
                            base_volume=ticker['baseVolume'],
                            quote_volume=ticker['quoteVolume'],
                            info=ticker['info'],
                            datetime=datetime.datetime.fromisoformat(ticker['datetime']),
                            created_at=ticker['timestamp']))
        except Exception as e:
            print(str(e))
            raise e


async def watch_market_data(exchange: ccxt.pro.Exchange,
                            symbol: str,
                            session_factory: Callable[[], AsyncSession],
                            timeframe: str,
                            candle_limit: int,
                            orderbook_depth: int) -> None:
    '''
    Watch websocket streams for a specific symbol / exchange pair.
    Starts concurrent tasks for streaming OHLCV, ticker updates,
    trades, and order book snapshots. Each stream is fetched
    and inserted asynchronously.

    :param exchange: The exchange object to watch the market data on.
    :param symbol: The trading symbol to watch.
    :param session_factory: Generates AsyncSession for database operations.
    :param timeframe: The timeframe for the OHLCV data.
    :param candle_limit: The number of candles to fetch for OHLCV data.
    :param orderbook_depth: The depth of the order book to maintain.
    '''

    loops = []
    if exchange.has["watchOHLCV"]:
        loops.append(
            watch_ohlcv(exchange, symbol, timeframe, candle_limit, session_factory))
    if exchange.has["watchTicker"]:
        loops.append(
            watch_ticker(exchange, symbol, session_factory))
    if exchange.has["watchTrades"]:
        loops.append(
            watch_trades(exchange, symbol, session_factory))
    if exchange.has["watchOrderBook"]:
        loops.append(
            watch_order_book(exchange, symbol, orderbook_depth, session_factory))
    await asyncio.gather(*loops)


async def exchange_loop(exchange_id: str,
                        exchange: ccxt.pro.Exchange,
                        symbols: List[str],
                        session_factory: Callable[[], AsyncSession],
                        timeframe: str,
                        candle_limit: int,
                        orderbook_depth: int) -> None:
    '''
    Remove. Call watch_market_data directly
    Exchanges do not need to be closed
    '''

    print(f"Processing {exchange_id} with symbols {symbols}")

    await asyncio.gather(*[watch_market_data(exchange, symbol, session_factory, timeframe, candle_limit, orderbook_depth) for symbol in symbols])
    #await exchange.close()


async def database_setup(user: str,
                         password: str,
                         host: str,
                         port: int,
                         db_name: str) -> sessionmaker:
    '''
    Creates a database if it doesn't exists using a temporary engine,
    connects to the database and creates an asynchronous session.
    Tables are created and asynchronous session factory is returned.
    Expire on commit set to false for efficency.

    :param user: The database username.
    :param password: The database password.
    :param host: The host.
    :param port: The database port number.
    :param db_name: Name of the database to be created / connected to.
    :return: An asynchronous session factory for performing database operations.
    '''

    # Create temporary engine to create database
    temp_url = f'mysql+aiomysql://{user}:{password}@{host}:{port}/'
    temp_engine = create_async_engine(temp_url, echo=True)
    async with temp_engine.begin() as conn:
        await conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
    await temp_engine.dispose()

    # Connect to database
    engine_url = f'{temp_url}{db_name}'
    engine = create_async_engine(engine_url, echo=True)

    # Create async db session factory
    async_session_factory = sessionmaker(engine,
                                         expire_on_commit=False,
                                         class_=AsyncSession)

    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(meta.create_all)

    return async_session_factory


async def initialize_exchanges(exchange_names: list[str]) -> dict[str, ccxt.pro.Exchange]:
    '''
    Initializes and returns a dictionary of CCXT Pro
    exchange objects for each exchange name provided.
    Only exchanges supported by CCXT Pro are initialized.
    Unsupported exchanges throw an exception and are skipped.
    Each exchange object is configured with rate limit enabled,
    asynchronous support, new updates, and verbosity.

    :param exchange_names: A list of exchange names (str) to be initialized.
    :return: A dictionary where keys are exchange names
             (str) and values are corresponding ccxt.pro.Exchange objects.
             Only successfully initialized exchanges are included.
    '''

    valid_exchanges = {}
    for exchange_name in exchange_names:
        try:
            exchange_class = getattr(ccxt.pro, exchange_name)
            exchange = exchange_class({'enableRateLimit': True,
                                       'async_support': True,
                                       'newUpdates': True,
                                       'verbose': True})
            valid_exchanges[exchange_name] = exchange
        except AttributeError:
            print(f"Exchange {exchange_name} is not supported by ccxt.pro")
        except Exception as e:
            print(f"An error occurred while initializing {exchange_name}: {str(e)}")

    return valid_exchanges


async def main():

    # Read config file
    config = await load_config('../config/config.yaml')

    # Initialize database and get
    async_session_factory = await database_setup(user=config['credentials']['user'],
                                                 password=config['credentials']['password'],
                                                 host=config['credentials']['host'],
                                                 port=config['credentials']['port'],
                                                 db_name=config['credentials']['db_name'])
    # Initialize exchange
    exchange_objects = await initialize_exchanges(exchange_names=config['exchanges'].keys())

    # Load markets
    for exchange_id, exchange in exchange_objects.items():
        try:
            markets = await exchange.load_markets()
            print(f"Markets loaded for {exchange_id}")
            print(markets)
        except Exception as e:
            print(f"Error loading markets for {exchange_id}: {str(e)}")

    # Run loops for each exchange and its symbols
    loops = [exchange_loop(
        exchange_id=exchange_id,
        exchange=exchange_objects[exchange_id],
        symbols=config['exchanges'][exchange_id]['symbols'],
        session_factory=async_session_factory,
        timeframe=config['settings']['timeframe'],
        candle_limit=config['settings']['candle_limit'],
        orderbook_depth=config['settings']['orderbook_depth'])
             for exchange_id in config['exchanges']]

    await asyncio.gather(*loops, return_exceptions=False)

if __name__ == "__main__":
    asyncio.run(main())
