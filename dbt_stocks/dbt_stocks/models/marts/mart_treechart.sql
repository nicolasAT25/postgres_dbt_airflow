with source as (
    select
        company_symbol
        , current_price
        , market_timestamp
    from {{ ref('int_stock_quotes') }}
    -- optionally filter invalid rows:
    where current_price is not null
),

latest_day as (
    -- if market_timestamp is epoch seconds (number/int):
    select
        cast(max(market_timestamp) as date) as max_day
    from source
),

latest_prices as (
  select
    company_symbol
    , avg(current_price) as avg_price
  from source
  join latest_day ld
    on cast(market_timestamp as date) = ld.max_day
  group by company_symbol
),

all_time_volatility as (
  select
    company_symbol,
    stddev_pop(current_price) as volatility,             
    case
      when avg(current_price) = 0 then null
      else stddev_pop(current_price) / nullif(avg(current_price), 0)
    end as relative_volatility
  from source
  group by company_symbol
)

select
  lp.company_symbol,
  lp.avg_price,
  v.volatility,
  v.relative_volatility
from latest_prices lp
join all_time_volatility v on lp.company_symbol = v.company_symbol
order by lp.company_symbol