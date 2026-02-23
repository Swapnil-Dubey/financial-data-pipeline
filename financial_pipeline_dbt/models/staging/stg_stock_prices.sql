with source as (
    select * from {{ source('financial_pipeline','raw_stock_prices')}}
)

select Date as date,
    Ticker as ticker,
    Open as open,
    High as high,
    Low as low,
    Close as close,
    Volume as volume,
    daily_return,
    price_range,
    ingestion_timestamp
from source