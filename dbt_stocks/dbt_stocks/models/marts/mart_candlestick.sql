with enriched as (
    select
        company_symbol
        , cast(market_timestamp as date) as trade_date
        , low_price_of_day
        , high_price_of_day
        , current_price
        , first_value(current_price) over (
            partition by company_symbol, cast(market_timestamp as date)
            order by market_timestamp
        ) as candle_open
        , last_value(current_price) over (
            partition by company_symbol, cast(market_timestamp as date)
            order by market_timestamp
            rows between unbounded preceding and unbounded following
        ) as candle_close
   
    from {{ ref('int_stock_quotes') }}
),

candles as (
    select
        company_symbol
        , trade_date as candle_time
        , min(low_price_of_day) as candle_low
        , max(high_price_of_day) as candle_high
        , min(candle_open) as candle_open    -- all rows in group share the same value (window fn)
        , min(candle_close) as candle_close  -- all rows in group share the same value (window fn)
        , avg(current_price) as trend_line
    from enriched
    group by company_symbol, trade_date
),

ranked as (
    select
        c.*
        , row_number() over (
            partition by company_symbol
            order by candle_time desc
        ) as rn
    from candles c
)

select
    company_symbol
    , candle_time
    , candle_low
    , candle_high
    , candle_open
    , candle_close
    , trend_line
from ranked
where rn <= 12
order by company_symbol, candle_time