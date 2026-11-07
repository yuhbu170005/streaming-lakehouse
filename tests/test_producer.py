import asyncio
import io
import json
import os
import struct
import uuid
from pathlib import Path

import aiohttp
from aiohttp import web
from confluent_kafka import Consumer
from fastavro import schemaless_reader, schemaless_writer

from producer.contracts import KLINE, RAW, decode_stream
from producer.main import backfill, publish_live, read_stream, recover_page
from producer.publish import create_publisher
from producer.state import State

S = "BTCUSDT"


def envelope(trade_id):
    return {
        "stream": "btcusdt@aggTrade",
        "data": {
            "a": trade_id,
            "e": "aggTrade",
            "E": 1730000000001,
            "s": S,
            "p": "10.12345678",
            "q": "0.3",
            "f": 100,
            "l": 102,
            "T": 1730000000000,
            "m": False,
        },
    }


class RecordingPublisher:
    def __init__(self):
        self.batches = []
        self.kafka = None

    async def send(self, batch):
        if self.kafka:
            await self.kafka.send(batch)
        self.batches.extend(batch)


def test_wire_schema_evolution():
    v1, v2 = [
        json.loads(Path(f"schemas/aggtrade-v{version}.avsc").read_text())
        for version in (1, 2)
    ]
    topic, key, event = decode_stream(json.dumps(envelope(42)), "test", "test", {S})
    assert (topic, key, event["p"]) == (RAW, S, "10.12345678")
    stream = io.BytesIO()
    stream.write(struct.pack(">BI", 0, 29))
    schemaless_writer(stream, v2, {**event, "source_region": "fixture"})
    stream.seek(5)
    assert schemaless_reader(stream, v1) == event


def test_only_closed_klines():
    data = {
        "stream": "btcusdt@kline_1m",
        "data": {"k": {"i": "1m", "s": S, "x": False, "t": 1730000040000}},
    }
    assert decode_stream(json.dumps(data), "test", "test", {S})[0] is None
    data["data"]["k"]["x"] = True
    assert decode_stream(json.dumps(data), "test", "test", {S})[0] == KLINE


def test_websocket_reconnect_rest_backfill_and_replay(tmp_path):
    async def verify():
        state_path = tmp_path / "state.db"
        state = State(state_path)
        published = RecordingPublisher()
        producer_id = uuid.uuid4().hex
        connections = 0

        async def websocket(request):
            nonlocal connections
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            connections += 1
            ids = [10] if connections == 1 else [13, 13, 12]
            for trade_id in ids:
                await ws.send_json(envelope(trade_id))
            await ws.close()
            return ws

        async def rest(request):
            assert request.query["symbol"] == S
            assert request.query["fromId"] == "11"
            assert request.query["limit"] == "2"
            return web.json_response([envelope(i)["data"] for i in (11, 12)])

        app = web.Application()
        app.router.add_get("/stream", websocket)
        app.router.add_get("/api/v3/aggTrades", rest)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        address, port = runner.addresses[0]
        queue = asyncio.Queue(8)
        stop = asyncio.Event()
        try:
            async with aiohttp.ClientSession() as http:
                if os.getenv("RUN_KAFKA_INTEGRATION") == "1":
                    published.kafka = await create_publisher(http)
                tasks = [
                    asyncio.create_task(
                        read_stream(
                            http,
                            queue,
                            {S},
                            f"ws://{address}:{port}",
                            producer_id,
                            stop,
                        )
                    ),
                    asyncio.create_task(publish_live(queue, published, state)),
                    asyncio.create_task(
                        backfill(
                            http,
                            published,
                            state,
                            f"http://{address}:{port}",
                            producer_id,
                        )
                    ),
                ]
                try:
                    async with asyncio.timeout(8):
                        while not any(
                            event.get("backfilled_at")
                            for _, _, event in published.batches
                        ):
                            await asyncio.sleep(0.02)
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await runner.cleanup()
            state.db.close()
        reopened = State(state_path)
        assert not reopened.pending()
        assert connections >= 2
        assert {
            event["a"] for topic, _, event in published.batches if topic == RAW
        } == {10, 11, 12, 13}
        assert [
            event["a"] for _, _, event in published.batches if event.get("is_backfill")
        ] == [11, 12]
        reopened.db.close()
        if published.kafka:
            assert await asyncio.to_thread(kafka_trade_ids, producer_id) == {
                10,
                11,
                12,
                13,
            }

    asyncio.run(verify())


def test_backfill_does_not_skip_missing_first_id(tmp_path):
    async def verify():
        state = State(tmp_path / "state.db")
        state.acknowledge([{"s": S, "a": 10}, {"s": S, "a": 14}])

        async def rest(_):
            return web.json_response([envelope(12)["data"]])

        app = web.Application()
        app.router.add_get("/api/v3/aggTrades", rest)
        runner = web.AppRunner(app)
        await runner.setup()
        try:
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            address, port = runner.addresses[0]
            published = RecordingPublisher()
            async with aiohttp.ClientSession() as http:
                try:
                    await recover_page(
                        http,
                        published,
                        state,
                        state.pending()[0],
                        f"http://{address}:{port}",
                        "t",
                    )
                except ValueError as error:
                    assert "expected ID 11" in str(error)
                else:
                    raise AssertionError("an incomplete page must fail")
            assert published.batches == []
            assert state.pending()[0]["next_id"] == 11
        finally:
            await runner.cleanup()
            state.db.close()

    asyncio.run(verify())


def kafka_trade_ids(producer_id):
    schema = json.loads(Path("schemas/aggtrade-v1.avsc").read_text())
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:19092",
            "group.id": producer_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    ids = set()
    try:
        consumer.subscribe([RAW])
        for _ in range(100):
            message = consumer.poll(0.1)
            if message is None or message.error():
                continue
            stream = io.BytesIO(message.value())
            magic, schema_id = struct.unpack(">BI", stream.read(5))
            assert magic == 0 and schema_id > 0
            event = schemaless_reader(stream, schema)
            if event["producer_id"] == producer_id:
                ids.add(event["a"])
            if ids == {10, 11, 12, 13}:
                break
    finally:
        consumer.close()
    return ids


def test_reference_fixture_matches_closed_market_minute(tmp_path, monkeypatch):
    from producer.contracts import now_ms
    from producer.fixture_server import Market, app_for

    monkeypatch.setenv("FIXTURE_STATE", str(tmp_path / "fixture-epoch"))
    market = Market()
    minute = (now_ms() // 60_000 - 2) * 60_000
    market.started = minute

    async def verify():
        runner = web.AppRunner(app_for(market))
        await runner.setup()
        try:
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            _, port = runner.addresses[0]
            async with aiohttp.ClientSession() as http:
                async with http.get(
                    f"http://127.0.0.1:{port}/api/v3/klines",
                    params={"symbol": "DEMOUSDT", "startTime": minute},
                ) as response:
                    response.raise_for_status()
                    candle = (await response.json())[0]
            assert (candle[0], candle[5], candle[8]) == (minute, "75.000", 1200)
        finally:
            await runner.cleanup()

    asyncio.run(verify())


def test_reference_cursor_recovers_skipped_minutes_without_a_trade_gap(tmp_path):
    path = tmp_path / "state.db"
    state = State(path)

    def candle(minute):
        return {"symbol": S, "kline": {"t": minute}}

    state.acknowledge_references([candle(0), candle(180_000), candle(0)])
    state.db.close()
    reopened = State(path)
    assert not reopened.pending()
    assert [row["minute_ms"] for row in reopened.closed_reference_requests()] == [
        60_000,
        120_000,
    ]
    reopened.acknowledge_references([candle(240_000)])
    assert [row["minute_ms"] for row in reopened.closed_reference_requests()] == [
        60_000,
        120_000,
    ]
    reopened.db.close()
