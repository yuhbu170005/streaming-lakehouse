"""Run the real REST backfill code against the local Postman simulation."""

import asyncio
import os

import aiohttp

from producer.main import recover_page
from producer.state import State


class Recorder:
    def __init__(self):
        self.ids = []

    async def send(self, batch):
        self.ids.extend(event["a"] for _, _, event in batch)


async def verify():
    base = os.getenv("SIMULATED_BINANCE_REST_URL", "http://127.0.0.1:28777/spot")
    async with aiohttp.ClientSession() as http:
        for start, expected in ((11, [11, 12]), (10, [])):
            state = State(":memory:")
            state.acknowledge(
                [{"s": "BTCUSDT", "a": start - 1}, {"s": "BTCUSDT", "a": 13}]
            )
            publisher = Recorder()
            try:
                await recover_page(
                    http, publisher, state, state.pending()[0], base, "simulation"
                )
            except ValueError:
                assert not expected and state.pending()[0]["next_id"] == start
            else:
                assert not state.pending()
            assert publisher.ids == expected
            print(
                f"fromId={start}: published {publisher.ids}; pending={len(state.pending())}"
            )
            state.db.close()


if __name__ == "__main__":
    asyncio.run(verify())
