select *
from {{ ref('fct_stock_prices') }}
where high < low