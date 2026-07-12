# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Commands

```bash
uv run main.py                        # run the pipeline
uv sync                               # install deps
docker compose up --build             # containerized run (QuestDB on host via host.docker.internal)
```

## Architecture

Single-file pipeline (`main.py`) that subscribes to Binance futures/spot WebSocket streams and writes raw events to QuestDB via the ILP protocol.

**Data flow (backfill):** Binance REST `/klines` -> `Backfiller._fetch_symbol` -> `Sender.row()` -> QuestDB

**Data flow (live):** Binance WS -> `_ws_worker` (asyncio + websockets) -> `Channel.matches` + `Channel.parser` -> `Sender.row()` -> QuestDB

**Dedup:** QuestDB WAL `DEDUP UPSERT KEYS(ts, symbol, kline_interval)` ensures the same kline from REST or WS is stored once.

**Key abstractions:**

- **`Channel`** -- ties together a Binance WS endpoint (`public` or `market`), a stream-name builder, an event matcher, and a row parser. Adding a new stream type is defining one new `Channel` constant + its parser function. Parsers return `(table, symbols_dict, columns_dict, designated_ts)` or `None`.
- **`Backfiller`** -- fetches historical klines from the Binance REST `/klines` endpoint, rate-limited by a `RateLimiter` (weight-based, 80% of Binance limit). On startup it queries QuestDB for the latest kline per symbol and only fetches the gap up to now.
- **`ensure_table()`** -- creates the `binance_klines` table with `WAL` + `DEDUP UPSERT KEYS(ts, symbol, kline_interval)` via the QuestDB `/exec` HTTP endpoint before any ingestion begins.
- **`BinanceFutures` / `BinanceSpot`** -- market feed classes that know their REST (symbol discovery + klines) and WS base URLs. `resolve_symbols()` hits the exchange info endpoint; pass `symbols='all'` to auto-discover all trading pairs, or an explicit list. Results are cached.
- **`FeedHandler`** -- orchestrator: resolves symbols per feed, groups channels by WS endpoint, builds flat stream lists, splits into 1000-stream chunks (Binance limit), spawns one `_ws_worker` per chunk.
- **`_ws_worker`** -- long-lived asyncio task: subscribes in batches of 100, reads events in a loop, matches each event to its channel, parses, and writes to QuestDB. Proactively reconnects every 23 h (before Binance's 24 h forced close).

**Parsers:** each parser returns `(table_name, symbols_dict, columns_dict, designated_ts)` or `None` to skip. The kline designated timestamp is the open time (epoch ms x 1,000,000 as `TimestampNanos`), so DEDUP keys on `(open_time, symbol, interval)`. The kline parser skips mid-candle ticks (`k['x']` must be true).

**QuestDB:** `Sender.from_conf()` with `auto_flush_rows=100` and `auto_flush_interval=1000` (ms). Config via env vars: `QUESTDB_HOST`, `QUESTDB_HTTP_PORT`, `QUESTDB_USER`, `QUESTDB_PASSWORD`. Backfill config: `BACKFILL_DAYS`, `BACKFILL_CONCURRENCY`, `BACKFILL_LIMIT`, `KLINE_INTERVAL`.

## Dependencies

Python 3.12, managed with `uv`. Runtime deps: `aiohttp`, `websockets`, `questdb`.
