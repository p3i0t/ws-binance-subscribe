"""
Binance klines service.

On startup:
  1. Creates the ``binance_klines`` table (WAL + dedup) in QuestDB.
  2. Backfills up to N days of historical klines via Binance REST API.
  3. Subscribes to live klines via WebSocket and ingests closed bars.

Duplicate handling is delegated to QuestDB WAL ``DEDUP UPSERT KEYS`` so that
the same kline from either REST or WS is stored exactly once.
"""

import asyncio
from enum import Enum
import json
import logging
import os
import time

import aiohttp
import websockets
import typer
from questdb.ingress import Sender, TimestampNanos

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)-7s %(message)s')
logger = logging.getLogger('binance')

# ─── Config (defaults from env; overridden by CLI flags) ──────────────────────
QUESTDB_HOST      = os.environ.get("QUESTDB_HOST", "127.0.0.1")
QUESTDB_HTTP_PORT = int(os.environ.get("QUESTDB_HTTP_PORT", "9000"))
QUESTDB_USER      = os.environ.get("QUESTDB_USER", "admin")
QUESTDB_PASSWORD  = os.environ.get("QUESTDB_PASSWORD", "quest")

# ─── Binance URLs ─────────────────────────────────────────────────────────────
_FUTURES_EXCHANGE = "https://fapi.binance.com/fapi/v1/exchangeInfo"
_FUTURES_KLINES   = "https://fapi.binance.com/fapi/v1/klines"
_FUTURES_WS       = {"public": "wss://fstream.binance.com/public/ws",
                     "market": "wss://fstream.binance.com/market/ws"}
_SPOT_EXCHANGE    = "https://api.binance.com/api/v3/exchangeInfo"
_SPOT_KLINES      = "https://api.binance.com/api/v3/klines"
_SPOT_WS          = {"public": "wss://stream.binance.com:9443/public/ws",
                     "market": "wss://stream.binance.com:9443/market/ws"}


# ─── Interval helpers ─────────────────────────────────────────────────────────

_INTERVAL_UNITS = {'s': 1_000, 'm': 60_000, 'h': 3_600_000,
                   'd': 86_400_000, 'w': 604_800_000}


def interval_ms(interval: str) -> int:
    """Binance interval string to milliseconds (e.g. '5m' -> 300_000)."""
    return int(interval[:-1]) * _INTERVAL_UNITS[interval[-1]]


def klines_weight(limit: int) -> int:
    """Binance REST weight for a klines request with the given *limit*."""
    if limit <= 100:
        return 1
    if limit <= 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


# ─── QuestDB helpers ──────────────────────────────────────────────────────────

_TABLE_DDL = """\
CREATE TABLE IF NOT EXISTS binance_klines (
    ts              TIMESTAMP,
    symbol          SYMBOL CAPACITY 1000,
    kline_interval  SYMBOL,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          DOUBLE,
    close_time      LONG,
    quote_volume    DOUBLE,
    trades          LONG,
    taker_buy_base  DOUBLE,
    taker_buy_quote DOUBLE
) TIMESTAMP(ts)
PARTITION BY DAY
WAL
DEDUP UPSERT KEYS(ts, symbol, kline_interval)"""


def _qdb_exec_url():
    return f"http://{QUESTDB_HOST}:{QUESTDB_HTTP_PORT}/exec"


def _qdb_auth_params():
    return {'user': QUESTDB_USER, 'password': QUESTDB_PASSWORD}


def _sender_conf():
    return (f"http::addr={QUESTDB_HOST}:{QUESTDB_HTTP_PORT};"
            f"username={QUESTDB_USER};password={QUESTDB_PASSWORD};"
            f"auto_flush_rows=100;auto_flush_interval=1000;")


async def ensure_table():
    """Create the binance_klines table with WAL + dedup if it doesn't exist."""
    async with aiohttp.ClientSession() as session:
        params = {**_qdb_auth_params(), 'query': _TABLE_DDL}
        async with session.get(_qdb_exec_url(), params=params) as resp:
            if resp.status != 200:
                raise RuntimeError(
                    f"QuestDB DDL failed ({resp.status}): {await resp.text()}")
            logger.info("QuestDB table 'binance_klines' ready (WAL + DEDUP).")


async def get_latest_timestamps(interval: str) -> dict:
    """Return {symbol: epoch_ms} for the latest kline per symbol."""
    query = (f"SELECT symbol, cast(max(ts) as long) / 1000 "
             f"FROM binance_klines WHERE kline_interval = '{interval}' "
             f"GROUP BY symbol")
    try:
        async with aiohttp.ClientSession() as session:
            params = {**_qdb_auth_params(), 'query': query}
            async with session.get(_qdb_exec_url(), params=params) as resp:
                if resp.status != 200:
                    logger.warning("get_latest_timestamps: HTTP %d", resp.status)
                    return {}
                data = await resp.json()
                return {row[0]: int(row[1])
                        for row in data.get('dataset', [])}
    except Exception as e:
        logger.warning("get_latest_timestamps failed: %s", e)
        return {}


# ─── Rate limiter ─────────────────────────────────────────────────────────────

class RateLimiter:
    """Fixed-window weight-based limiter for the Binance REST API."""

    def __init__(self, weight_per_minute=2400, safety_factor=0.8):
        self._max_weight = int(weight_per_minute * safety_factor)
        self._lock = asyncio.Lock()
        self._window_start = time.monotonic()
        self._consumed = 0

    async def acquire(self, weight: int):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._window_start
            if elapsed >= 60:
                self._window_start = now
                self._consumed = 0
            elif self._consumed + weight > self._max_weight:
                wait = 60 - elapsed + 0.5
                logger.info("Rate limit window full (%d/%d), sleeping %.0fs",
                            self._consumed, self._max_weight, wait)
                await asyncio.sleep(wait)
                self._window_start = time.monotonic()
                self._consumed = 0
            self._consumed += weight


# ─── Backfiller ───────────────────────────────────────────────────────────────

class Backfiller:
    """Backfill historical klines from the Binance REST API into QuestDB."""

    def __init__(self, klines_url: str, interval: str, backfill_days: int,
                 rate_limiter: RateLimiter, max_concurrency: int = 5,
                 limit: int = 1000):
        self.klines_url = klines_url
        self.interval = interval
        self.iv_ms = interval_ms(interval)
        self.backfill_days = backfill_days
        self.rate_limiter = rate_limiter
        self._sem = asyncio.Semaphore(max_concurrency)
        self.limit = limit

    async def run(self, symbols: list):
        """Backfill every symbol, writing results through a shared ILP sender."""
        now_ms = int(time.time() * 1000)
        backfill_start = now_ms - self.backfill_days * 86_400_000
        latest = await get_latest_timestamps(self.interval)

        total = len(symbols)
        done = 0
        rows_written = 0

        conf = _sender_conf()
        async with aiohttp.ClientSession() as session:
            with Sender.from_conf(conf) as sender:

                async def _do(sym):
                    nonlocal done, rows_written
                    start = latest.get(sym)
                    if start is None:
                        start = backfill_start
                    elif start:
                        start = start + 1  # close_time is last ms of bar; +1 = next bar open
                    if start >= now_ms:
                        done += 1
                        return
                    rows = await self._fetch_symbol(session, sym, start, now_ms)
                    for row_ts, syms, cols in rows:
                        sender.row('binance_klines', symbols=syms,
                                   columns=cols, at=TimestampNanos(row_ts))
                    rows_written += len(rows)
                    done += 1
                    if done % 50 == 0:
                        sender.flush()
                        logger.info("Backfill %d/%d symbols, %d rows",
                                    done, total, rows_written)

                await asyncio.gather(*[_do(s) for s in symbols])
                sender.flush()
                logger.info("Backfill complete: %d/%d symbols, %d rows",
                            done, total, rows_written)

    async def _fetch_symbol(self, session: aiohttp.ClientSession,
                            symbol: str, start_ms: int, end_ms: int) -> list:
        """Fetch closed klines for one symbol; returns list of (ts_ns, syms, cols)."""
        weight = klines_weight(self.limit)
        results = []
        cursor = start_ms

        async with self._sem:
            while cursor < end_ms:
                await self.rate_limiter.acquire(weight)
                params = {
                    'symbol':    symbol,
                    'interval':  self.interval,
                    'startTime': cursor,
                    'endTime':   end_ms,
                    'limit':     self.limit,
                }
                try:
                    async with session.get(self.klines_url, params=params) as resp:
                        if resp.status in (429, 418):
                            retry = int(resp.headers.get('Retry-After', '60'))
                            logger.warning("Rate limited (%d) on %s, sleeping %ds",
                                           resp.status, symbol, retry)
                            await asyncio.sleep(retry)
                            continue
                        if resp.status != 200:
                            logger.error("Backfill %s failed: HTTP %d",
                                         symbol, resp.status)
                            break
                        data = await resp.json()
                except Exception as e:
                    logger.error("Backfill %s error: %s", symbol, e)
                    break

                if not data:
                    break

                now_ms = int(time.time() * 1000)
                for k in data:
                    close_ms = int(k[6])
                    if close_ms > now_ms:
                        continue  # skip still-forming bar (close_time in the future)
                    results.append((
                        close_ms * 1_000_000,
                        {'symbol': symbol, 'kline_interval': self.interval},
                        {'open':             float(k[1]),
                         'high':             float(k[2]),
                         'low':              float(k[3]),
                         'close':            float(k[4]),
                         'volume':           float(k[5]),
                         'close_time':       int(k[6]),
                         'quote_volume':     float(k[7]),
                         'trades':           int(k[8]),
                         'taker_buy_base':   float(k[9]),
                         'taker_buy_quote':  float(k[10]),
                         },
                    ))

                cursor = int(data[-1][0]) + self.iv_ms
                if len(data) < self.limit:
                    break

        return results


# ─── Channel ──────────────────────────────────────────────────────────────────

class Channel:
    """A Binance stream type: subscription pattern, event matching, row parsing.

    Parsers return (table, symbols_dict, columns_dict, designated_ts) or None.
    """

    def __init__(self, endpoint: str, stream_name: str, parser,
                 interval: str = ''):
        self.endpoint = endpoint            # "public" | "market"
        self.stream_name = stream_name      # e.g. "{symbol}@kline_5m"
        self.parser = parser
        self.interval = interval            # set for kline channels to disambiguate

    def stream_for(self, symbol: str) -> str:
        return self.stream_name.format(symbol=symbol.lower())

    def matches(self, event: dict) -> bool:
        e = event.get('e', '')
        suffix = self.stream_name.split('@')[1]
        if not suffix.startswith(e):
            return False
        if self.interval and event.get('k', {}).get('i') != self.interval:
            return False
        return True


# ─── Parsers ──────────────────────────────────────────────────────────────────

def _kline_parser(event: dict):
    k = event['k']
    if not k['x']:
        return None  # only store closed bars
    return (
        'binance_klines',
        {'symbol': k['s'], 'kline_interval': k['i']},
        {'open':             float(k['o']),
         'high':             float(k['h']),
         'low':              float(k['l']),
         'close':            float(k['c']),
         'volume':           float(k['v']),
         'close_time':       int(k['T']),
         'quote_volume':     float(k['q']),
         'trades':           int(k['n']),
         'taker_buy_base':   float(k['V']),
         'taker_buy_quote':  float(k['Q']),
        },
        TimestampNanos(int(k['T']) * 1_000_000),  # designated ts = close_time
    )


def _agg_trade_parser(event: dict):
    return (
        'binance_agg_trades',
        {'symbol': event['s']},
        {'agg_trade_id':   int(event['a']),
         'price':          float(event['p']),
         'qty':            float(event['q']),
         'first_trade_id': int(event['f']),
         'last_trade_id':  int(event['l']),
         'trade_time':     int(event['T']) * 1_000_000,
         'is_buyer_maker': event['m'],
         },
        TimestampNanos(int(event['T']) * 1_000_000),
    )


def _book_ticker_parser(event: dict):
    return (
        'binance_book_ticker',
        {'symbol': event['s']},
        {'bid_price': float(event['b']),
         'bid_qty':   float(event['B']),
         'ask_price': float(event['a']),
         'ask_qty':   float(event['A']),
         },
        TimestampNanos.now(),
    )


# ─── Channel Definitions ──────────────────────────────────────────────────────

KLINE_1M    = Channel('market', '{symbol}@kline_1m',   _kline_parser, interval='1m')
KLINE_5M    = Channel('market', '{symbol}@kline_5m',   _kline_parser, interval='5m')
KLINE_15M   = Channel('market', '{symbol}@kline_15m',  _kline_parser, interval='15m')
KLINE_1H    = Channel('market', '{symbol}@kline_1h',   _kline_parser, interval='1h')
AGG_TRADE   = Channel('market', '{symbol}@aggTrade',   _agg_trade_parser)
BOOK_TICKER = Channel('public', '{symbol}@bookTicker', _book_ticker_parser)


# ─── Market ───────────────────────────────────────────────────────────────────

class Market:
    """A Binance market (futures or spot)."""

    def __init__(self, name, exchange_url, klines_url, ws_base, channels,
                 symbols='all'):
        self.name = name
        self.exchange_url = exchange_url
        self.klines_url = klines_url
        self.ws_base = ws_base
        self.channels = channels
        self.symbols = symbols
        self._resolved = None

    async def resolve_symbols(self) -> list:
        if self._resolved is not None:
            return self._resolved
        if self.symbols != 'all':
            self._resolved = list(self.symbols)
            return self._resolved
        logger.info("%s: discovering symbols...", self.name)
        async with aiohttp.ClientSession() as session:
            async with session.get(self.exchange_url) as resp:
                if resp.status != 200:
                    raise RuntimeError(
                        f"Exchange info failed: {resp.status}")
                data = await resp.json()
                syms = [s['symbol'] for s in data['symbols']
                        if s['status'] == 'TRADING']
                logger.info("%s: %d active symbols.", self.name, len(syms))
                self._resolved = syms
                return syms


def BinanceFutures(channels, symbols='all'):
    return Market('BinanceFutures', _FUTURES_EXCHANGE, _FUTURES_KLINES,
                  _FUTURES_WS, channels, symbols)


def BinanceSpot(channels, symbols='all'):
    return Market('BinanceSpot', _SPOT_EXCHANGE, _SPOT_KLINES,
                  _SPOT_WS, channels, symbols)


# ─── WebSocket Worker ─────────────────────────────────────────────────────────

_SUB_BATCH = 100          # max streams per SUBSCRIBE message
_MAX_STREAMS = 1000       # Binance hard limit per connection
_RECONNECT_INTERVAL = 23 * 3600  # proactive reconnect before 24 h forced close


async def _ws_worker(worker_id, ws_url, streams, channels):
    conf = _sender_conf()
    rows_ingested = 0

    with Sender.from_conf(conf) as sender:
        while True:
            try:
                async with websockets.connect(ws_url) as ws:
                    for i in range(0, len(streams), _SUB_BATCH):
                        await ws.send(json.dumps({
                            "method": "SUBSCRIBE",
                            "params": streams[i:i + _SUB_BATCH],
                            "id": i,
                        }))
                    logger.info("[%s] Subscribed to %d streams.",
                                worker_id, len(streams))

                    deadline = asyncio.get_event_loop().time() + _RECONNECT_INTERVAL

                    while True:
                        if asyncio.get_event_loop().time() >= deadline:
                            sender.flush()
                            logger.info("[%s] 23h elapsed, reconnecting.", worker_id)
                            break

                        raw = await ws.recv()
                        data = json.loads(raw)
                        event = data.get('data', data)

                        for ch in channels:
                            if ch.matches(event):
                                row = ch.parser(event)
                                if row is not None:
                                    table, syms, cols, at = row
                                    sender.row(table, symbols=syms,
                                               columns=cols, at=at)
                                    rows_ingested += 1
                                    if rows_ingested % 10_000 == 0:
                                        logger.info("[%s] %d rows ingested.",
                                                    worker_id, rows_ingested)
                                break

            except websockets.exceptions.ConnectionClosed as e:
                _flush_quiet(sender)
                logger.warning("[%s] Disconnected (%s). Reconnecting in 5s...",
                               worker_id, e)
                await asyncio.sleep(5)
            except Exception as e:
                _flush_quiet(sender)
                logger.error("[%s] Error: %s. Retrying in 5s...", worker_id, e)
                await asyncio.sleep(5)


def _flush_quiet(sender):
    try:
        sender.flush()
    except Exception:
        pass


# ─── Feed Handler ─────────────────────────────────────────────────────────────

class FeedHandler:
    def __init__(self):
        self._feeds = []

    def add_feed(self, feed) -> 'FeedHandler':
        self._feeds.append(feed)
        return self

    async def run(self):
        tasks = []
        for feed in self._feeds:
            symbols = await feed.resolve_symbols()

            by_endpoint = {}
            for ch in feed.channels:
                by_endpoint.setdefault(ch.endpoint, []).append(ch)

            for endpoint, endpoint_channels in by_endpoint.items():
                ws_url = feed.ws_base[endpoint]
                streams = list(dict.fromkeys(
                    ch.stream_for(sym)
                    for ch in endpoint_channels
                    for sym in symbols
                ))

                for idx, i in enumerate(range(0, len(streams), _MAX_STREAMS), 1):
                    chunk = streams[i:i + _MAX_STREAMS]
                    wid = f"{feed.name}.{endpoint}.{idx}"
                    tasks.append(_ws_worker(wid, ws_url, chunk, endpoint_channels))

        await asyncio.gather(*tasks)


# ─── Entry Point ──────────────────────────────────────────────────────────────

_CHANNELS = {'1m': KLINE_1M, '5m': KLINE_5M, '15m': KLINE_15M, '1h': KLINE_1H}

app = typer.Typer(
    name='binance-live-ingestor',
    help='Subscribe to Binance klines and ingest into QuestDB.',
    no_args_is_help=False,
)


class MarketChoice(str, Enum):
    futures = 'futures'
    spot = 'spot'


class IntervalChoice(str, Enum):
    m1  = '1m'
    m5  = '5m'
    m15 = '15m'
    h1  = '1h'


@app.command()
def run(
    market: MarketChoice = typer.Option(
        MarketChoice(os.environ.get('MARKET', 'futures')),
        '--market', '-m',
        help='Binance market to subscribe to.',
    ),
    symbols: str = typer.Option(
        os.environ.get('SYMBOLS', 'all'),
        '--symbols', '-s',
        help="Comma-separated list (e.g. BTCUSDT,ETHUSDT) or 'all'.",
    ),
    interval: IntervalChoice = typer.Option(
        IntervalChoice(os.environ.get('INTERVAL', '5m')),
        '--interval', '-i',
        help='Kline interval.',
    ),
    backfill_days: int = typer.Option(
        int(os.environ.get('BACKFILL_DAYS', '3')),
        '--backfill-days', '-d',
        help='Days of history to backfill on startup.',
    ),
    backfill_concurrency: int = typer.Option(
        int(os.environ.get('BACKFILL_CONCURRENCY', '5')),
        '--backfill-concurrency',
        help='Parallel REST requests during backfill.',
    ),
    backfill_limit: int = typer.Option(
        int(os.environ.get('BACKFILL_LIMIT', '1000')),
        '--backfill-limit',
        help='Klines per REST request (affects API weight).',
    ),
    backfill: bool = typer.Option(
        True,
        '--backfill/--no-backfill',
        help='Enable or disable historical backfill on startup.',
    ),
    questdb_host: str = typer.Option(QUESTDB_HOST,        '--questdb-host'),
    questdb_port: int = typer.Option(QUESTDB_HTTP_PORT,   '--questdb-port'),
    questdb_user: str = typer.Option(QUESTDB_USER,        '--questdb-user'),
    questdb_password: str = typer.Option(QUESTDB_PASSWORD, '--questdb-password'),
):
    """Start ingesting Binance klines into QuestDB."""
    global QUESTDB_HOST, QUESTDB_HTTP_PORT, QUESTDB_USER, QUESTDB_PASSWORD
    QUESTDB_HOST      = questdb_host
    QUESTDB_HTTP_PORT = questdb_port
    QUESTDB_USER      = questdb_user
    QUESTDB_PASSWORD  = questdb_password

    asyncio.run(_async_run(
        market.value, symbols, interval.value,
        backfill_days, backfill_concurrency, backfill_limit, backfill,
    ))


async def _async_run(market, symbols, interval,
                     backfill_days, backfill_concurrency,
                     backfill_limit, do_backfill):
    await ensure_table()

    sym_list = (symbols.split(',') if symbols != 'all' else 'all')
    channel = _CHANNELS[interval]
    if market == 'futures':
        feed = BinanceFutures(channels=[channel], symbols=sym_list)
    else:
        feed = BinanceSpot(channels=[channel], symbols=sym_list)
    resolved = await feed.resolve_symbols()

    tasks = []
    if do_backfill:
        rate_limiter = RateLimiter(weight_per_minute=2400)
        backfiller = Backfiller(
            klines_url=feed.klines_url,
            interval=interval,
            backfill_days=backfill_days,
            rate_limiter=rate_limiter,
            max_concurrency=backfill_concurrency,
            limit=backfill_limit,
        )
        tasks.append(backfiller.run(resolved))

    handler = FeedHandler().add_feed(feed)
    tasks.append(handler.run())

    await asyncio.gather(*tasks)


def cli():
    """Console-script entry point."""
    try:
        app()
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
