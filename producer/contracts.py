import json
import time

RAW = "binance.aggtrade.raw.v1"
DLQ = "binance.aggtrade.dlq.v1"
KLINE = "binance.kline.ref.v1"
GAP = "binance.gap.events.v1"


def now_ms():
    return time.time_ns() // 1_000_000


def trade(data, symbol, connection, producer_id, *, backfill=False):
    fields = {"a": int, "p": str, "q": str, "f": int, "l": int, "T": int, "m": bool}
    if not backfill:
        fields.update(e=str, E=int, s=str)
    result = {}
    for key, kind in fields.items():
        value = data.get(key)
        if type(value) is not kind:
            raise ValueError(f"invalid aggTrade field {key}")
        if kind is int and not 0 <= value < 2**63:
            raise ValueError(f"out of range aggTrade field {key}")
        result[key] = value
    if not backfill and (result["e"] != "aggTrade" or result["s"] != symbol):
        raise ValueError("aggTrade envelope does not match the subscription")
    return {
        **result,
        "s": symbol,
        "e": "aggTrade",
        "E": data["T"] if backfill else data["E"],
        "conn_id": connection,
        "producer_id": producer_id,
        "ingest_ts": now_ms(),
        "is_backfill": backfill,
    }


def decode_stream(message, connection, producer_id, allowlist):
    envelope = json.loads(message)
    if not isinstance(envelope, dict):
        raise ValueError("expected object envelope")
    data = envelope.get("data")
    if not isinstance(data, dict):
        raise ValueError("missing combined-stream data object")
    if data.get("e") == "serverShutdown":
        return "shutdown", None, None
    symbol, _, channel = str(envelope.get("stream", "")).partition("@")
    symbol = symbol.upper()
    if symbol not in allowlist:
        raise ValueError("unexpected stream symbol")
    if channel == "aggTrade":
        return RAW, symbol, trade(data, symbol, connection, producer_id)
    if channel == "kline_1m":
        candle = data.get("k")
        if (
            not isinstance(candle, dict)
            or candle.get("i") != "1m"
            or candle.get("s") != symbol
        ):
            raise ValueError("invalid kline")
        if candle.get("x") is True:
            minute = candle.get("t")
            if (
                type(minute) is not int
                or not 0 <= minute <= now_ms()
                or minute % 60_000
            ):
                raise ValueError("invalid kline minute")
            return (
                KLINE,
                symbol,
                {"symbol": symbol, "kline": candle, "ingest_ts": now_ms()},
            )
        return None, None, None
    raise ValueError("unexpected stream channel")
