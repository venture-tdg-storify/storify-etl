{{
    config(
        materialized = 'table',
    )
}}

select
    order_id,
    product_id,
    store_id,
    category_id,
    subcategory_id,
    group_id,
    amount,
    quantity,
    transaction_date,
    is_in_inventory,
    is_on_floor,
    on_floor_days_count
from {{ ref('stg_ash__transactions') }}
