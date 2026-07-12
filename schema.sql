-- QuestDB schema for Binance klines.
-- This is created automatically by main.py on startup via ensure_table().
-- Provided here for manual reference / inspection.

CREATE TABLE IF NOT EXISTS binance_klines (
    ts              TIMESTAMP,          -- designated timestamp (= kline open time)
    symbol          SYMBOL CAPACITY 1000,
    kline_interval  SYMBOL,            -- e.g. '5m', '1h'
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          DOUBLE,
    close_time      LONG,              -- epoch ms from Binance
    quote_volume    DOUBLE,
    trades          LONG,
    taker_buy_base  DOUBLE,
    taker_buy_quote DOUBLE
) TIMESTAMP(ts)
PARTITION BY DAY
WAL
DEDUP UPSERT KEYS(ts, symbol, kline_interval);
