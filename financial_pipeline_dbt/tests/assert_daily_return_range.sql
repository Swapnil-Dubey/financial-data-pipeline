select *
from {{ref('fct_stock_prices')}}
where daily_return<-50 or daily_return>50