-- QuestDB schema for Binance klines.
-- Tables are created automatically by binance-live-ingestor on startup.
-- Each interval gets its own table: binance_klines_1m, binance_klines_5m.
-- Provided here for manual reference / inspection.

-- For 5m klines (repeat with _1m for 1m):
CREATE TABLE IF NOT EXISTS binance_klines_5m (
    ts              TIMESTAMP,          -- designated timestamp (= close_time)
    symbol          SYMBOL CAPACITY 1000,
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
DEDUP UPSERT KEYS(ts, symbol);
