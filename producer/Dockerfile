FROM ghcr.io/astral-sh/uv:0.13.6 AS uv
FROM python:3.12.16-slim
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev
COPY producer producer
COPY schemas schemas
COPY config/symbols.yaml config/symbols.yaml
ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "-m", "producer.main"]
