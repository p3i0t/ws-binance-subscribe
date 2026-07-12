# binance-subscribe

Subscribes to Binance futures klines via WebSocket and ingests them into QuestDB.
On startup it also backfills up to N days of historical klines via the REST API.

## Quick start

```bash
uv run main.py          # local run
docker compose up --build   # containerized (QuestDB on host via host.docker.internal)
```

QuestDB must be running and reachable on the configured host/port before launch.

## What it does

1. **Creates** the `binance_klines` table with `WAL` + `DEDUP UPSERT KEYS(ts, symbol, kline_interval)` so duplicate writes (REST vs WS, restarts, reconnects) collapse to a single row.
2. **Backfills** up to `BACKFILL_DAYS` of 5m klines via the REST `/klines` endpoint, rate-limited to stay under Binance's weight budget.
3. **Subscribes** to live `@kline_5m` WebSocket streams and ingests closed bars.

Both backfill and WS run concurrently. The designated timestamp is the kline open time (not ingestion time), so dedup works correctly.

## Configuration (env vars)

| Variable | Default | Description |
|---|---|---|
| `QUESTDB_HOST` | `127.0.0.1` | QuestDB host |
| `QUESTDB_HTTP_PORT` | `9000` | QuestDB HTTP port (used for ILP + /exec) |
| `QUESTDB_USER` | `admin` | QuestDB user |
| `QUESTDB_PASSWORD` | `quest` | QuestDB password |
| `BACKFILL_DAYS` | `3` | How many days of history to backfill |
| `BACKFILL_CONCURRENCY` | `5` | Parallel REST requests during backfill |
| `BACKFILL_LIMIT` | `1000` | Klines per REST request (affects API weight) |
| `KLINE_INTERVAL` | `5m` | Kline interval for backfill |

## Schema

See [schema.sql](schema.sql). The table is auto-created on startup.
