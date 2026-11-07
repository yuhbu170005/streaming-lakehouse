FROM ghcr.io/astral-sh/uv:0.13.6 AS uv
FROM python:3.12.16-slim
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --group operations
COPY orchestration orchestration
COPY dbt_project dbt_project
ENV PATH="/app/.venv/bin:$PATH" DAGSTER_HOME=/dagster TRINO_HOST=trino TRINO_PORT=8080 PYTHONPATH=/app
RUN mkdir /dagster
COPY orchestration/dagster.yaml /dagster/dagster.yaml
CMD ["dagster", "dev", "-h", "0.0.0.0", "-w", "orchestration/workspace.yaml"]
