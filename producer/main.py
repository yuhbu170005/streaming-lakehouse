import asyncio
import json
import logging
import os
import random
import signal
import uuid
from pathlib import Path

import aiohttp
import yaml
from prometheus_client import Counter, Gauge, start_http_server

from producer.contracts import DLQ, GAP, KLINE, RAW, decode_stream, now_ms, trade
from producer.publish import create_publisher
from producer.state import State

LOG = logging.getLogger("producer")
RECONNECTS = Counter("producer_reconnects_total", "WebSocket reconnects")
GAPS = Counter("producer_gaps_detected_total", "Detected ID ranges")
DUPLICATES = Counter(
    "producer_duplicate_ids_total", "Replayed or out-of-order trade IDs"
)
DEPTH = Gauge("producer_queue_depth", "Messages waiting for delivery")
PRESSURE = Counter("producer_backpressure_total", "Full queue observations")
OPEN_GAPS = Gauge("producer_open_gaps", "Ranges with incomplete backfill")
REST_ERRORS = Counter(
    "producer_backfill_errors_total", "Backfill request or continuity failures"
)


async def read_stream(http, queue, symbols, ws_base, producer_id, stop):
    streams = "/".join(
        f"{s.lower()}@{channel}"
        for s in symbols
        for channel in ("aggTrade", "kline_1m")
    )
    if len(symbols) > 512:
        raise ValueError("combined stream exceeds 1,024 subscriptions")
    delay = 1
    url = f"{ws_base}/stream?streams={streams}"
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        connection = uuid.uuid4().hex
        started = loop.time()
        immediate = False
        try:
            async with asyncio.timeout(23 * 3600 + 55 * 60):
                async with http.ws_connect(
                    url, timeout=aiohttp.ClientWSTimeout(ws_receive=60)
                ) as ws:
                    async for message in ws:
                        if message.type is not aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            item = decode_stream(
                                message.data, connection, producer_id, symbols
                            )
                        except ValueError as error:
                            item = (
                                DLQ,
                                "unknown",
                                {"payload": message.data, "error_reason": str(error)},
                            )
                        if item[0] == "shutdown":
                            immediate = True
                            break
                        if item[0] is None:
                            continue
                        if queue.full():
                            PRESSURE.inc()
                        await queue.put(item)
                        DEPTH.set(queue.qsize())
        except (aiohttp.ClientError, TimeoutError) as error:
            LOG.warning("WebSocket ended: %s", error)
        if stop.is_set():
            break
        RECONNECTS.inc()
        if loop.time() - started >= 60:
            delay = 1
        await asyncio.sleep(1 if immediate else max(1, random.uniform(0, delay)))
        delay = min(60, delay * 2)


async def publish_live(queue, publisher, state):
    while True:
        batch = [await queue.get()]
        await asyncio.sleep(0.02)
        while len(batch) < 512 and not queue.empty():
            batch.append(queue.get_nowait())
        DEPTH.set(queue.qsize())
        await publisher.send(batch)
        gaps, duplicates = state.acknowledge(
            [event for topic, _, event in batch if topic == RAW]
        )
        GAPS.inc(gaps)
        DUPLICATES.inc(duplicates)
        for _ in batch:
            queue.task_done()


async def recover_page(http, publisher, state, gap, rest_base, producer_id):
    symbol, expected, upper = gap["symbol"], gap["next_id"], gap["to_id"]
    rows = await get_market_data(
        http,
        f"{rest_base}/api/v3/aggTrades",
        {
            "symbol": symbol,
            "fromId": expected,
            "limit": min(1000, upper - expected + 1),
        },
    )
    batch = []
    for row in rows:
        if row.get("a") != expected or expected > upper:
            break
        batch.append(
            (RAW, symbol, trade(row, symbol, "rest", producer_id, backfill=True))
        )
        expected += 1
    if not batch:
        raise ValueError(f"{symbol}: REST did not return expected ID {expected}")
    await publisher.send(batch)
    state.advance(
        gap, expected, {event["T"] // 60_000 * 60_000 for _, _, event in batch}
    )


async def get_market_data(http, url, params):
    async with http.get(url, params=params) as response:
        if response.status in (418, 429):
            try:
                retry_after = int(response.headers.get("Retry-After", "60"))
            except ValueError:
                retry_after = 60
            await asyncio.sleep(max(1, retry_after))
        response.raise_for_status()
        return await response.json()


async def recover_reference(http, publisher, state, request, rest_base):
    symbol, minute = request["symbol"], request["minute_ms"]
    rows = await get_market_data(
        http,
        f"{rest_base}/api/v3/klines",
        {
            "symbol": symbol,
            "interval": "1m",
            "startTime": minute,
            "endTime": minute + 59_999,
            "limit": 1,
        },
    )
    if not rows or len(rows[0]) < 9 or rows[0][0] != minute:
        raise ValueError(f"{symbol}: reference minute {minute} unavailable")
    row = rows[0]
    candle = dict(zip(("t", "o", "h", "l", "c", "v"), row[:6], strict=True))
    candle.update(i="1m", s=symbol, x=True, n=row[8])
    await publisher.send(
        [(KLINE, symbol, {"symbol": symbol, "kline": candle, "ingest_ts": now_ms()})]
    )
    state.finish_reference(symbol, minute)


async def backfill(http, publisher, state, rest_base, producer_id):
    while True:
        notifications = state.notifications()
        if notifications:
            events = [json.loads(row["event"]) for row in notifications]
            await publisher.send([(GAP, event["symbol"], event) for event in events])
            state.clear_notifications(notifications[-1]["id"])
        pending = state.pending()
        OPEN_GAPS.set(len(pending))
        for gap in pending:
            try:
                await recover_page(http, publisher, state, gap, rest_base, producer_id)
            except (
                aiohttp.ClientError,
                TimeoutError,
                ValueError,
                TypeError,
                AttributeError,
            ) as error:
                REST_ERRORS.inc()
                LOG.warning("Backfill pending for %s: %s", gap["symbol"], error)
            # Weight 4 per request, with a single serial backfill worker.
            await asyncio.sleep(0.1)
        for request in state.closed_reference_requests():
            try:
                await recover_reference(http, publisher, state, request, rest_base)
            except (
                aiohttp.ClientError,
                TimeoutError,
                ValueError,
                TypeError,
                KeyError,
            ) as error:
                REST_ERRORS.inc()
                LOG.warning(
                    "Reference backfill pending for %s: %s", request["symbol"], error
                )
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)


async def run():
    symbols_file = Path(os.getenv("SYMBOLS_FILE", "config/symbols.yaml"))
    symbols = frozenset(yaml.safe_load(symbols_file.read_text())["symbols"])
    queue = asyncio.Queue(maxsize=int(os.getenv("PRODUCER_QUEUE_SIZE", "4096")))
    state_path = Path(os.getenv("PRODUCER_STATE", "data/producer.sqlite"))
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state = State(state_path)
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    start_http_server(int(os.getenv("PRODUCER_METRICS_PORT", "9108")))
    producer_id = os.getenv("PRODUCER_ID", "local-spot")
    ws = os.getenv("BINANCE_WS_URL", "wss://data-stream.binance.vision")
    rest = os.getenv("BINANCE_REST_URL", "https://data-api.binance.vision")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as http:
        publisher = await create_publisher(http)
        async with asyncio.TaskGroup() as tasks:
            reader = tasks.create_task(
                read_stream(http, queue, symbols, ws, producer_id, stop)
            )
            sender = tasks.create_task(publish_live(queue, publisher, state))
            filler = tasks.create_task(
                backfill(http, publisher, state, rest, producer_id)
            )
            await stop.wait()
            reader.cancel()
            filler.cancel()
            try:
                await asyncio.wait_for(queue.join(), timeout=20)
            finally:
                sender.cancel()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
