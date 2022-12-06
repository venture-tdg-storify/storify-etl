{{
    config(
        materialized = 'table',
    )
}}

select
    product_id,
    store_id,
    true as is_on_floor,
    on_floor_days_count,
    quantity,
    inventory_date
from {{ ref('stg_ash__inventory') }}
