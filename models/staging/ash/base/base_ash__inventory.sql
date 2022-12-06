{{
    config(
        materialized = 'table',
    )
}}

select
    trim(item_id) as product_id,
    trim(location_key) as store_id,
    -- one of these is off by 1 day / where item_id = '5930247' and location_key = '723_303'
    to_date(floored_date, 'MM/DD/YY') as floor_start_date,
    snapshot_date as inventory_date
from {{ source('ash', 'raw_inventory') }}
