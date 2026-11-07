with gaps as (
    select symbol, date_trunc('hour', detected_at) as hour_start,
        count(*) as gaps_detected, count_if(backfilled_at is null) as open_gaps
    from {{ source('ops', 'gap_events') }}
    group by 1, 2
),
recon as (
    select symbol, date_trunc('hour', window_start) as hour_start,
        count_if(status = 'match') as matches,
        count_if(status in ('match', 'mismatch')) as candles_compared,
        count_if(status = 'mismatch') as mismatches,
        count_if(status in ('missing_gold', 'missing_reference')) as missing_inputs
    from {{ source('recon', 'candle_vs_kline') }}
    group by 1, 2
),
arrival as (
    select symbol, date_trunc('hour', trade_time) as hour_start,
        count_if(date_diff('millisecond', trade_time, ingest_ts) > 30000)
            as arrival_delay_over_30s
    from {{ source('silver', 'aggtrade') }}
    group by 1, 2
)
select
    concat(symbol, ':', cast(hour_start as varchar)) as health_key,
    symbol, hour_start, coalesce(gaps_detected, 0) as gaps_detected,
    coalesce(open_gaps, 0) as open_gaps,
    coalesce(arrival_delay_over_30s, 0) as arrival_delay_over_30s,
    coalesce(candles_compared, 0) as candles_compared,
    coalesce(mismatches, 0) as mismatches,
    coalesce(missing_inputs, 0) as missing_inputs,
    cast(matches as double) / nullif(candles_compared, 0) as match_rate
from gaps
full join recon using (symbol, hour_start)
full join arrival using (symbol, hour_start)
