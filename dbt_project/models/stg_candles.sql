select
    concat(symbol, ':', cast(window_start as varchar)) as candle_key,
    symbol, window_start, window_end, open, high, low, close, volume, quote_volume,
    vwap, trade_count, buy_volume, sell_volume, updated_at, repaired
from {{ source('gold', 'candles_1m') }}
