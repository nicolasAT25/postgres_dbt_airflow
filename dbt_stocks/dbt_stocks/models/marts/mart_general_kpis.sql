with most_recent_stock_quotes as (
    select
        *
    from (
        select
            *
            , row_number() over (partition by company_symbol order by fetched_at desc) as rn
        from {{ ref('int_stock_quotes') }}
        ) sub_q
    where rn = 1
)
select
    stock_quote_id
    , company_symbol
    , current_price
    , previous_close_price
    , change
    , percent_change
    , fetched_at
from most_recent_stock_quotes