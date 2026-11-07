#!/bin/sh
set -eu
count=${1:?Expected symbol count, such as 100}
duration=${2:-1800}
case "$count" in
    20|100|200) ;;
    *) echo "Supported benchmark sizes: 20, 100, 200" >&2; exit 2 ;;
esac
mkdir -p data
project="streaming-lakehouse-scale-$count-$(date -u +%Y%m%d%H%M%S)-$$"
export SYMBOLS_FILE="./data/symbols-$count.yaml"
uv run --locked --group operations python -m chaos.benchmark symbols --count "$count" \
    --require-recent > "$SYMBOLS_FILE"
compose() {
    docker compose -p "$project" -f docker-compose.yml \
        -f compose.scale.yml --profile core --profile query "$@"
}
cleanup() {
    compose down --volumes
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
compose up -d --wait redpanda catalog trino
compose exec -T redpanda sh /opt/lakehouse/topics.sh
compose up -d --wait producer bronze silver
TRINO_PORT=38080 uv run --locked --group operations python -m chaos.benchmark run \
    --mode live --project "$project" --duration "$duration" \
    --warmup 120 --settle 20 --expected-symbols "$count" --ingest-only \
    --output "data/benchmark-scale-$count.json"
