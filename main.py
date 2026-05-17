import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional
import aiohttp
import websockets
from questdb.ingress import Sender, TimestampNanos

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ─── QuestDB Config ───────────────────────────────────────────────────────────
QUESTDB_HOST      = os.environ.get("QUESTDB_HOST", "127.0.0.1")
QUESTDB_HTTP_PORT = int(os.environ.get("QUESTDB_HTTP_PORT", "9000"))
QUESTDB_USER      = os.environ.get("QUESTDB_USER", "admin")
QUESTDB_PASSWORD  = os.environ.get("QUESTDB_PASSWORD", "quest")

# ─── Channel Definitions ──────────────────────────────────────────────────────
# Each Channel encodes:
#   endpoint   — which Binance WS route handles this stream ("public" | "market")
#   stream_fn  — symbol → stream name, e.g. "btcusdt@kline_5m"
#   match      — event dict → True if this channel owns the event
#   parser     — event dict → (table, symbols_dict, columns_dict) or None to skip

@dataclass(frozen=True)
class Channel:
    endpoint:  str
    stream_fn: Callable[[str], str]
    match:     Callable[[dict], bool]
    parser:    Callable[[dict], Optional[tuple]]


# ── Parsers ───────────────────────────────────────────────────────────────────

def _kline_parser(interval: str) -> Callable:
    def parser(event: dict):
        k = event['k']
        if not k['x']:
            return None  # skip mid-candle ticks; only store the closed bar
        return (
            f'binance_klines_{interval}',
            {'symbol': k['s'], 'interval': k['i']},
            {
                'open':            float(k['o']),
                'close':           float(k['c']),
                'high':            float(k['h']),
                'low':             float(k['l']),
                'volume':          float(k['v']),
                'quote_volume':    float(k['q']),
                'taker_volume':    float(k['V']),
                'taker_quote_volume': float(k['Q']),
                'trades':          int(k['n']),
                'open_time':       int(k['t']) * 1_000,  # ms → us (QuestDB TIMESTAMP is microseconds)
                'close_time':      int(k['T']) * 1_000,  # ms → us
            },
        )
    return parser


def _agg_trade_parser(event: dict):
    return (
        'binance_agg_trades',
        {'symbol': event['s']},
        {
            'agg_trade_id':   int(event['a']),
            'price':          float(event['p']),
            'qty':            float(event['q']),
            'normal_qty':     float(event['nq']),
            'first_trade_id': int(event['f']),
            'last_trade_id':  int(event['l']),
            'trade_time':     datetime.fromtimestamp(int(event['T']) / 1000, tz=timezone.utc),
            'is_buyer_maker': event['m'],
        },
    )


def _book_ticker_parser(event: dict):
    return (
        'binance_book_ticker',
        {'symbol': event['s']},
        {
            'bid_price': float(event['b']),
            'bid_qty':   float(event['B']),
            'ask_price': float(event['a']),
            'ask_qty':   float(event['A']),
        },
    )


# ── Channel constants ─────────────────────────────────────────────────────────

KLINE_1M  = Channel('market', lambda s: f'{s.lower()}@kline_1m',
                    lambda e: e.get('e') == 'kline' and e.get('k', {}).get('i') == '1m',
                    _kline_parser('1m'))

KLINE_5M  = Channel('market', lambda s: f'{s.lower()}@kline_5m',
                    lambda e: e.get('e') == 'kline' and e.get('k', {}).get('i') == '5m',
                    _kline_parser('5m'))

KLINE_15M = Channel('market', lambda s: f'{s.lower()}@kline_15m',
                    lambda e: e.get('e') == 'kline' and e.get('k', {}).get('i') == '15m',
                    _kline_parser('15m'))

KLINE_1H  = Channel('market', lambda s: f'{s.lower()}@kline_1h',
                    lambda e: e.get('e') == 'kline' and e.get('k', {}).get('i') == '1h',
                    _kline_parser('1h'))

AGG_TRADE = Channel('market', lambda s: f'{s.lower()}@aggTrade',
                    lambda e: e.get('e') == 'aggTrade',
                    _agg_trade_parser)

BOOK_TICKER = Channel('public', lambda s: f'{s.lower()}@bookTicker',
                      lambda e: e.get('e') == 'bookTicker',
                      _book_ticker_parser)


# ─── Market Feed Classes ──────────────────────────────────────────────────────

class BinanceFutures:
    """USD-S Margined Futures streams."""
    REST_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    WS_BASE  = {
        'public': 'wss://fstream.binance.com/public/ws',
        'market': 'wss://fstream.binance.com/market/ws',
    }

    def __init__(self, channels: list, symbols='all'):
        self.channels = channels if isinstance(channels, list) else [channels]
        self.symbols  = symbols  # 'all' or explicit list, e.g. ['BTCUSDT', 'ETHUSDT']

    async def resolve_symbols(self) -> list:
        if self.symbols != 'all':
            return self.symbols
        logger.info(f"{self.__class__.__name__}: Fetching active symbols...")
        async with aiohttp.ClientSession() as session:
            async with session.get(self.REST_URL) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Failed to fetch exchange info: {resp.status}")
                data = await resp.json()
                syms = [s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING']
                logger.info(f"{self.__class__.__name__}: {len(syms)} active symbols.")
                return syms


class BinanceSpot:
    """Spot streams."""
    REST_URL = "https://api.binance.com/api/v3/exchangeInfo"
    WS_BASE  = {
        'public': 'wss://stream.binance.com:9443/public/ws',
        'market': 'wss://stream.binance.com:9443/market/ws',
    }

    def __init__(self, channels: list, symbols='all'):
        self.channels = channels if isinstance(channels, list) else [channels]
        self.symbols  = symbols

    async def resolve_symbols(self) -> list:
        if self.symbols != 'all':
            return self.symbols
        logger.info(f"{self.__class__.__name__}: Fetching active symbols...")
        async with aiohttp.ClientSession() as session:
            async with session.get(self.REST_URL) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Failed to fetch exchange info: {resp.status}")
                data = await resp.json()
                syms = [s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING']
                logger.info(f"{self.__class__.__name__}: {len(syms)} active symbols.")
                return syms


# ─── WebSocket Worker ─────────────────────────────────────────────────────────

_SUB_BATCH          = 100       # max streams per SUBSCRIBE message (~4 KB limit)
_MAX_STREAMS        = 1000      # Binance hard limit per connection
_RECONNECT_INTERVAL = 23 * 3600  # proactive reconnect before 24 h forced close


async def _ws_worker(worker_id: str, ws_url: str, streams: list,
                     channels: list, qdb_config: tuple):
    host, port, user, password = qdb_config
    rows_ingested = 0

    # auto_flush_rows=200: flush every 200 rows; interval-based flushing disabled.
    conf = (f"http::addr={host}:{port};username={user};password={password};"
            f"auto_flush_rows=200;auto_flush_interval=off;")
    with Sender.from_conf(conf) as sender:
        while True:
            try:
                async with websockets.connect(ws_url) as ws:
                    for i in range(0, len(streams), _SUB_BATCH):
                        sub_batch = streams[i:i + _SUB_BATCH]
                        await ws.send(json.dumps({
                            "method": "SUBSCRIBE",
                            "params": sub_batch,
                            "id":     abs(hash(worker_id)) % 10_000 + i,
                        }))
                    logger.info(f"[{worker_id}] Subscribed to {len(streams)} streams.")

                    deadline = asyncio.get_event_loop().time() + _RECONNECT_INTERVAL

                    while True:
                        if asyncio.get_event_loop().time() >= deadline:
                            sender.flush()  # flush partial batch before reconnect
                            logger.info(f"[{worker_id}] 23 h elapsed, reconnecting.")
                            break

                        raw   = await ws.recv()
                        data  = json.loads(raw)
                        event = data.get('data', data)  # unwrap combined-stream envelope

                        for ch in channels:
                            if ch.match(event):
                                row = ch.parser(event)
                                if row is not None:
                                    table, syms, cols = row
                                    sender.row(table, symbols=syms, columns=cols,
                                               at=TimestampNanos.now())
                                    rows_ingested += 1
                                    if rows_ingested % 10_000 == 0:
                                        logger.info(f"[{worker_id}] {rows_ingested} rows ingested.")
                                break  # each event belongs to at most one channel

            except websockets.exceptions.ConnectionClosed as e:
                try:
                    sender.flush()
                except Exception:
                    pass
                logger.warning(f"[{worker_id}] Connection lost ({e}). Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                try:
                    sender.flush()
                except Exception:
                    pass
                logger.error(f"[{worker_id}] Unexpected error: {e}. Retrying in 5s...")
                await asyncio.sleep(5)


# ─── Feed Handler ─────────────────────────────────────────────────────────────

class FeedHandler:
    def __init__(self):
        self._feeds = []

    def add_feed(self, feed) -> 'FeedHandler':
        self._feeds.append(feed)
        return self  # allow chaining

    async def run(self):
        qdb_config = (QUESTDB_HOST, QUESTDB_HTTP_PORT, QUESTDB_USER, QUESTDB_PASSWORD)
        tasks = []

        for feed in self._feeds:
            symbols = await feed.resolve_symbols()

            # Group channels by their Binance endpoint (public / market)
            by_endpoint: dict = {}
            for ch in feed.channels:
                by_endpoint.setdefault(ch.endpoint, []).append(ch)

            for endpoint, endpoint_channels in by_endpoint.items():
                ws_url  = feed.WS_BASE[endpoint]
                # Build flat stream list: all channels x all symbols on this endpoint
                streams = list(dict.fromkeys(        # preserve order, deduplicate
                    ch.stream_fn(sym)
                    for ch in endpoint_channels
                    for sym in symbols
                ))

                # Split into connections of <= 1000 streams each
                for idx, i in enumerate(range(0, len(streams), _MAX_STREAMS), start=1):
                    chunk     = streams[i:i + _MAX_STREAMS]
                    worker_id = f"{feed.__class__.__name__}.{endpoint}.{idx}"
                    tasks.append(_ws_worker(worker_id, ws_url, chunk,
                                            endpoint_channels, qdb_config))

        await asyncio.gather(*tasks)


# ─── Entry Point ──────────────────────────────────────────────────────────────

async def main():
    handler = FeedHandler()

    # ── one line per subscription ──────────────────────────────────────────
    # handler.add_feed(BinanceFutures(channels=[KLINE_5M], symbols='all'))
    handler.add_feed(BinanceFutures(channels=[KLINE_1M, KLINE_5M, AGG_TRADE], symbols='all'))
    # handler.add_feed(BinanceFutures(channels=[BOOK_TICKER], symbols=['BTCUSDT', 'ETHUSDT']))
    # handler.add_feed(BinanceSpot(channels=[KLINE_5M], symbols=['BTCUSDT', 'ETHUSDT']))
    # ──────────────────────────────────────────────────────────────────────

    await handler.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user.")
