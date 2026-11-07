"""Deterministic, paced Spot fixtures for local integration and chaos tests."""

import argparse
import asyncio
import os
import time
from decimal import Decimal
from pathlib import Path

from aiohttp import web

BASE_PRICE = Decimal("100")
QUANTITY = Decimal("0.125")
TRADE_MS = 100
BASE_ID = 1_000_000


class Market:
    def __init__(self):
        start_file = Path(os.getenv("FIXTURE_STATE", "data/fixture-start-ms"))
        start_file.parent.mkdir(parents=True, exist_ok=True)
        if start_file.exists():
            self.started = int(start_file.read_text())
        else:
            self.started = time.time_ns() // 1_000_000
            start_file.write_text(str(self.started))

    def head(self):
        return BASE_ID + (time.time_ns() // 1_000_000 - self.started) // TRADE_MS

    def timestamp(self, trade_id):
        return self.started + (trade_id - BASE_ID) * TRADE_MS

    def trade(self, trade_id):
        return {
            "a": trade_id,
            "p": str(BASE_PRICE + Decimal(trade_id % 13) / 100),
            "q": str(QUANTITY),
            "f": trade_id * 2,
            "l": trade_id * 2 + 1,
            "T": self.timestamp(trade_id),
            "m": trade_id % 2 == 0,
        }

    def kline(self, symbol, minute):
        first = max(
            BASE_ID, BASE_ID + (minute - self.started + TRADE_MS - 1) // TRADE_MS
        )
        end = BASE_ID + (minute + 60_000 - self.started + TRADE_MS - 1) // TRADE_MS
        prices = [Decimal(self.trade(trade_id)["p"]) for trade_id in range(first, end)]
        if not prices:
            return None
        return {
            "i": "1m",
            "s": symbol,
            "t": minute,
            "x": True,
            "o": str(prices[0]),
            "h": str(max(prices)),
            "l": str(min(prices)),
            "c": str(prices[-1]),
            "v": str(QUANTITY * len(prices)),
            "n": 2 * len(prices),
        }

    async def websocket(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        subscriptions = [
            stream.partition("@")
            for stream in request.query["streams"].split("/")
            if not stream.startswith("lateusdt@")
        ]
        next_id = self.head()
        minute = self.timestamp(next_id) // 60_000 * 60_000
        try:
            while not ws.closed:
                head = self.head()
                for trade_id in range(next_id, head + 1):
                    for sym, _, channel in subscriptions:
                        if channel == "aggTrade":
                            event = self.trade(trade_id)
                            await ws.send_json(
                                {
                                    "stream": f"{sym}@{channel}",
                                    "data": {
                                        **event,
                                        "e": "aggTrade",
                                        "s": sym.upper(),
                                        "E": event["T"],
                                    },
                                }
                            )
                next_id = head + 1
                current = self.timestamp(head) // 60_000 * 60_000
                if current > minute:
                    for sym, _, channel in subscriptions:
                        if channel == "kline_1m":
                            kline = self.kline(sym.upper(), minute)
                            if kline:
                                await ws.send_json(
                                    {"stream": f"{sym}@{channel}", "data": {"k": kline}}
                                )
                    minute = current
                await asyncio.sleep(0.05)
        except (ConnectionError, asyncio.CancelledError):
            pass
        return ws

    async def rest(self, request):
        start = max(BASE_ID, int(request.query["fromId"]))
        limit = min(1000, int(request.query.get("limit", "500")))
        end = min(start + limit, self.head() + 1)
        return web.json_response(
            [self.trade(trade_id) for trade_id in range(start, end)]
        )

    async def rest_klines(self, request):
        minute = int(request.query["startTime"])
        candle = self.kline(request.query["symbol"], minute)
        if candle is None or minute + 60_000 > self.timestamp(self.head()):
            return web.json_response([])
        return web.json_response(
            [
                [
                    minute,
                    candle["o"],
                    candle["h"],
                    candle["l"],
                    candle["c"],
                    candle["v"],
                    minute + 59_999,
                    "0",
                    candle["n"],
                    "0",
                    "0",
                    "0",
                ]
            ]
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8777)
    args = parser.parse_args()
    market = Market()
    app = web.Application()
    app.add_routes(
        [
            web.get("/stream", market.websocket),
            web.get("/api/v3/aggTrades", market.rest),
        ]
    )
    web.run_app(app, port=args.port)


if __name__ == "__main__":
    main()
