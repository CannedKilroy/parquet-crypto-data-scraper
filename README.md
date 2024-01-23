![Alt Text](https://github.com/CannedKilroy/crypto/blob/main/Assets/ccxt_resize.png)
# Cryptocurrency Futures Data Capture Tool

Python script that captures and stores realtime cryptocurrency websocket data asynchronously, using the ccxt library and sqlalchemy with MySQL. Compatible with ccxt supported exchange and symbols. After exchange connection is made and tables created, websocket streams including OHLCV, ticker, trades, and orderbook data are checked for uniquness and then inserted into a their own table.

## Dependencies
- Python 3.7 <=
- SQLalchemy
- MySQL

## Setup
- Clone repo to local machine
  - `git clone https://github.com/CannedKilroy/crypto.git`
- Install dependencies
  - `pip install -r requirements.txt`
- Edit config.ini database credentials 

## Usage
- Run main.py

## DB Diagram
![Alt Text](https://github.com/CannedKilroy/crypto/blob/main/Assets/crypto_websocket_stream_resized.png)

## Note
The inverse bitcoin futures contract on bybit generates approximatly 15-25 gigabytes of data a day.
Adding more exchanges / symbols to watch would generate alot of data 

### TODO:
- function that translates bybit symbol to ccxt symbol
- switch to time series database to handle more data
- expand config file to handle multiple exchanges and symbols, ohlcv timeframe, orderbook depth, timeout
- wrap main to run continously and handle errors better
- add async.sleep() to each stream to breakup execution and send heartbeat more often
- add catch all except that catches keyboard interupts etc to close the db properly then exit
- database normalization

## Links used:
- https://github.com/ccxt/ccxt/blob/master/examples/ccxt.pro/py/one-exchange-different-streams.py
- https://docs.ccxt.com/#/?id=error-handling
- https://github.com/ccxt/ccxt/blob/master/examples/ccxt.pro/py/build-ohlcv-many-symbols.py
