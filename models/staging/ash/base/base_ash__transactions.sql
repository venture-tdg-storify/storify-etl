{{
    config(
        materialized = 'table',
    )
}}

select
    sales_order_number_by_market as order_id,
    trim(item_id) as product_id,
    trim(location_key) as store_id,
    sales::number(38, 2) as amount,
    qty::integer as quantity,
    to_date(date_key, 'YYYYMMDD') as transaction_date
from {{ source('ash', 'raw_transactions') }}
where
    qty <> 'NULL'
    and item_id not like '*%'
