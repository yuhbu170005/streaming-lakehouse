select
    hourly_key, symbol, hour_start, close, quote_volume,
    cast(close as double) / nullif(cast(open as double), 0) - 1 as return_fraction,
    row_number() over (
        partition by hour_start order by quote_volume desc, symbol
    ) as volume_rank,
    row_number() over (
        partition by hour_start
        order by abs(cast(close as double) / nullif(cast(open as double), 0) - 1) desc, symbol
    ) as mover_rank
from {{ ref('mart_symbol_hourly') }}
