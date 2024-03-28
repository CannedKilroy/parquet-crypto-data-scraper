#datastorage
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, JSON, REAL, DATETIME, INT, UniqueConstraint, BigInteger
meta = MetaData()

table_orderbook = Table(
   'orderbook', 
   meta, 
   Column('id', Integer, primary_key = True),
   Column('exchange', String(32), index = True, nullable = False),
   Column('symbol', String(16), index = True, nullable = False),
   
   Column('asks', JSON, nullable = False),
   Column('bids', JSON, nullable = False),
   Column('nonce', String(32), nullable = True),
   
   Column('datetime', DATETIME),
   Column('created_at', BigInteger, nullable = False, index = True)
   )

table_ticker = Table(
    'ticker',
    meta,
    Column('id', Integer, primary_key = True),
    Column('exchange', String(32), index = True),
    Column('symbol', String(16), index = True),
   
    Column('ask', REAL),
    Column('ask_volume', REAL),
    Column('bid', REAL),
    Column('bid_volume', REAL),
    Column('open_24h', REAL), #OHLCV
    Column('high_24h', REAL),
    Column('low_24h', REAL),
    Column('close_24h', REAL),
    Column('last_price',REAL), #same as close
    Column('vwap', REAL),
    Column('previous_close_price', REAL),
    Column('price_change', REAL), #last-open
    Column('percentage_change', REAL),
    Column('average_price', REAL),
    Column('base_volume', REAL),
    Column('quote_volume', REAL),
    Column('info', JSON), #original ticker data from exchange
   
    Column('datetime', DATETIME),
    Column('created_at', BigInteger, index = True)
)

table_trades = Table(
    'trades',
    meta,
    Column('id', Integer, primary_key = True),
    Column('exchange', String(32), index = True),
    Column('symbol', String(16), index = True),
   
    Column('trade_id', String(64)),
    Column('order_id', String(64)),
    Column('order_type', String(32)),
    Column('trade_side', String(32)),
    Column('taker_maker', String(16)),
    Column('executed_price', REAL),
    Column('base_amount', REAL),
    Column('cost', REAL),
    Column('fee', JSON),
    Column('fees', JSON),
   
    Column('datetime', DATETIME),
    Column('created_at', BigInteger, index = True)
)

table_ohlcv = Table(
    'ohlcv',
    meta,
    Column('id', Integer, primary_key = True),
    Column('exchange', String(32), index = True),
    Column('symbol', String(16), index = True),
   
    Column('open_price', REAL),
    Column('high_price', REAL),
    Column('low_price', REAL),
    Column('close_price', REAL),
    Column('candle_volume', REAL),
    
    Column('datetime', DATETIME),    
    Column('created_at', BigInteger, index = True)
    )

table_logs = Table(
    'logs',
    meta,
    Column('id', Integer, primary_key = True),
    Column('exchange', String(32)),
    Column('symbol', String(32)),
    
    Column('error_type', String(64)),
    Column('message', String(512)),
    Column('stream', String(32)),
    
    Column('created_at', BigInteger, index = True)
    )
