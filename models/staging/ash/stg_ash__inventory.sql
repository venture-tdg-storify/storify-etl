{{
    config(
        materialized = 'table',
    )
}}

select
    product_id,
    store_id,
    count(*) as quantity,
    conditional_true_event(true) over (
        partition by product_id, store_id order by inventory_date
    ) as on_floor_days_count,
    max(floor_start_date) as floor_start_date,
    inventory_date
from {{ ref('base_ash__inventory') }}
group by
    product_id,
    store_id,
    inventory_date
