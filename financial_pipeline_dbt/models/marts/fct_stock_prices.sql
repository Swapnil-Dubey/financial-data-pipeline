{{ config(materialized='table') }}

with stg as (
    select * from {{ref('stg_stock_prices')}}
)

select date,
ticker,
open,
high,
low,
close,
volume,
round(daily_return, 4) as daily_return,
round(price_range, 4) as price_range,
ingestion_timestamp
from stg
where date is not null and close is not null and volume>0
order by ticker, date