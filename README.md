# Streaming Lakehouse

A local data engineering portfolio project for processing public Binance Spot market data. The repository currently contains the development foundation.

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and run:

```sh
uv python install
make sync
make lint
make test
uv run --locked pre-commit install
```

See [SPEC](docs/SPEC.md), [PLAN](docs/PLAN.md), and the [version matrix](docs/VERSIONS.md).

Public market data only. This project does not provide financial advice.
