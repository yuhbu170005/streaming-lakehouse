# Binance REST backfill simulation

The static local mock models Binance Spot's public inclusive `GET /api/v3/aggTrades` page. The fixture returns IDs 11 and 12, allowing the caller to be tested with a contiguous page and with a page that begins after the requested ID.

```sh
postman simulation run postman/lakehouse.sim.yaml --port 28777 --output ndjson
# In a separate terminal:
uv run --locked python -m scripts.validate_backfill_fixture
```

Verified on 2026-11-08 with Postman CLI 1.80.0. The request log showed `fromId=11&limit=2` and `fromId=10&limit=3`. The first closed the range after IDs 11 and 12 were published. The second published zero rows and kept the cursor on ID 10. The local fake WebSocket integration and the isolated Redpanda demo cover the other ingest paths. The CLI was signed out, so no workspace mock was queried. [Binance REST contract](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#compressedaggregate-trades-list)
