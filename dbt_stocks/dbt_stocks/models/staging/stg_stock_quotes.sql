select
    id as stock_quote_id
	, raw_data ->> 'symbol' as company_symbol
	, (raw_data ->> 'c')::numeric as current_price
	, (raw_data ->> 'o')::numeric as open_price_of_day
	, (raw_data ->> 'h')::numeric as high_price_of_day
	, (raw_data ->> 'l')::numeric as low_price_of_day
	, (raw_data ->> 'pc')::numeric as previous_close_price
	, (raw_data ->> 'd')::numeric as change
	, (raw_data ->> 'dp')::numeric as percent_change
	, to_timestamp((raw_data ->> 't')::bigint) as market_timestamp
	, to_timestamp((raw_data ->> 'fetched_at')::bigint) as fetched_at
from {{ source('raw', 'stock_quotes_raw') }}