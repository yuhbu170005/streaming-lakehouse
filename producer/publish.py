import asyncio
import io
import json
import os
import struct
from pathlib import Path

from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer
from prometheus_client import Counter

from producer.contracts import RAW

SENT = Counter("producer_messages_total", "Acknowledged Kafka messages", ["topic"])
ERRORS = Counter("producer_send_errors_total", "Kafka delivery errors")


class Publisher:
    def __init__(self, broker, schema_id, schema):
        self.client = Producer(
            {
                "bootstrap.servers": broker,
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "lz4",
                "linger.ms": 25,
                "delivery.timeout.ms": 15000,
                "queue.buffering.max.messages": 10000,
            }
        )
        self.schema = parse_schema(schema)
        self.header = struct.pack(">BI", 0, schema_id)
        self.lock = asyncio.Lock()

    def encode(self, topic, event):
        if topic != RAW:
            return json.dumps(event, separators=(",", ":")).encode()
        stream = io.BytesIO()
        stream.write(self.header)
        schemaless_writer(stream, self.schema, event)
        return stream.getvalue()

    def _send(self, batch):
        failures = []

        def delivery(error, message):
            if error is None:
                SENT.labels(message.topic()).inc()
            else:
                failures.append(str(error))
                ERRORS.inc()

        try:
            for topic, symbol, event in batch:
                value = self.encode(topic, event)
                self.client.produce(
                    topic, key=symbol, value=value, on_delivery=delivery
                )
        finally:
            pending = self.client.flush(17)
        if pending or failures:
            raise RuntimeError(f"Kafka delivery incomplete: {failures[:1]}")

    async def send(self, batch):
        async with self.lock:
            # Retrying a partially acknowledged batch can replay records.
            # The persisted cursor and the Silver key handle that replay.
            while True:
                try:
                    await asyncio.to_thread(self._send, batch)
                    return
                except (BufferError, RuntimeError):
                    await asyncio.sleep(1)


async def create_publisher(http):
    version = os.getenv("AGGTRADE_SCHEMA_VERSION", "1")
    schema = json.loads(Path(f"schemas/aggtrade-v{version}.avsc").read_text())
    registry = os.getenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:8081")
    subject = f"{registry}/subjects/{RAW}-value"
    async with http.put(
        f"{registry}/config/{RAW}-value", json={"compatibility": "BACKWARD"}
    ) as response:
        response.raise_for_status()
    async with http.post(
        f"{subject}/versions", json={"schema": json.dumps(schema)}
    ) as response:
        response.raise_for_status()
        schema_id = (await response.json())["id"]
    return Publisher(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092"), schema_id, schema
    )
