#!/bin/sh
set -eu
create() {
    name=$1 partitions=$2 retention=$3
    if ! rpk topic describe "$name" -X brokers=redpanda:9092 >/dev/null 2>&1; then
        rpk topic create "$name" -p "$partitions" -r 1 -c "retention.ms=$retention" \
            -X brokers=redpanda:9092
    fi
}
create binance.aggtrade.raw.v1 6 259200000
create binance.aggtrade.dlq.v1 1 1209600000
create binance.kline.ref.v1 3 259200000
create binance.gap.events.v1 3 259200000
