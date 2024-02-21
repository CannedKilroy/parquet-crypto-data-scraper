![Alt Text](https://github.com/CannedKilroy/crypto/blob/main/Assets/ccxt_resize.png)
# Cryptocurrency Futures Data Capture Tool

A python-based data pipeline that uses the CCXT and SQLAlchemy libraries to asynchronously capture and store real-time cryptocurrency exchange data from WebSocket feeds into a MySQL database. Designed to support multiple exchanges and tickers simultaneously, this tool is compatible with all CCXT-supported exchanges. 

After creating the database and tables, it connects to the exchanges, loads market data, and then captures the streams:
- OHLCV
- ticker information
- time and sales (recent trades)
- order book

## Dependencies
- Python 3.7 <=
- SQLalchemy
- MySQL
- CCXT

## Setup
- Clone repo to local machine
  - `git clone https://github.com/CannedKilroy/crypto.git`
- Install dependencies
  - `pip install -r requirements.txt`
- Edit `config` file inside `config/` for database credentials and other settings

## Usage
- Run main.py

## DB Diagram
![Alt Text](https://github.com/CannedKilroy/crypto/blob/main/Assets/crypto_websocket_stream_resized.png)

## Note
The inverse bitcoin futures contract on bybit generates approximatly 15-25 gigabytes of data a day.
Adding more exchanges / symbols to watch would generate alot of data 

### TODO:
- seperate out config reading
- loops and checking for exchanges and symbols
- logging
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
