with stock_quotes as (
    select
        *
    from {{ ref('stg_stock_quotes') }}
)
select 
    stock_quote_id
	, company_symbol
	, round(current_price, 2) as current_price
	, round(open_price_of_day, 2) as open_price_of_day
	, round(high_price_of_day, 2) as high_price_of_day
	, round(low_price_of_day, 2) as low_price_of_day
	, round(previous_close_price, 2) as previous_close_price
	, round(change, 2) as change
	, round(percent_change, 2) as percent_change
	, market_timestamp
	, fetched_at
from stock_quotes
where current_price is not null