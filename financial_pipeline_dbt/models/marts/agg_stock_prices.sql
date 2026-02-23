{{ config(materialized='table') }}

with fct as (
    select * from {{ref('fct_stock_prices')}}
)

select
    ticker,
    count(*) as total_trading_days,
    round(avg(daily_return), 4) as avg_daily_return,
    round(max(daily_return), 4) as max_daily_return,
    round(min(daily_return), 4) as min_daily_return,
    round(avg(volume), 0) as avg_volume,
    round(max(close), 2) as all_time_high,
    round(min(close), 2) as all_time_low
from fct
group by ticker
order by ticker