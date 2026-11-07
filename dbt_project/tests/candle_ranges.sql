select *
from {{ ref('stg_candles') }}
where high < low
    or volume < 0
    or open not between low and high
    or close not between low and high
    or trade_count < 1
