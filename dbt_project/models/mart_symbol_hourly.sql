select
    concat(symbol, ':', cast(date_trunc('hour', window_start) as varchar)) as hourly_key,
    symbol,
    date_trunc('hour', window_start) as hour_start,
    min_by(open, window_start) as open,
    max(high) as high,
    min(low) as low,
    max_by(close, window_start) as close,
    sum(volume) as volume,
    sum(quote_volume) as quote_volume,
    sum(trade_count) as trade_count,
    stddev_pop(cast(close as double)) / nullif(avg(cast(close as double)), 0) as volatility_proxy
from {{ ref('stg_candles') }}
group by 2, 3
