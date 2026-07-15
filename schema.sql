-- QuestDB schema for Binance klines.
-- Tables are created automatically by binance-live-ingestor on startup.
-- Each interval gets its own table: binance_klines_1m, binance_klines_5m.

CREATE TABLE IF NOT EXISTS binance_klines_5m (
    timestamp       TIMESTAMP,          -- designated timestamp (= close_time)
    symbol          SYMBOL CAPACITY 1000,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          DOUBLE,
    open_time       TIMESTAMP,
    close_time      TIMESTAMP,
    quote_volume    DOUBLE,
    trades          LONG,
    taker_buy_base  DOUBLE,
    taker_buy_quote DOUBLE
) TIMESTAMP(timestamp)
PARTITION BY DAY
WAL
DEDUP UPSERT KEYS(timestamp, symbol);
