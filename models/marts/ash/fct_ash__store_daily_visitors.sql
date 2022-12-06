{{
    config(
        materialized = 'view',
    )
}}

select
    store_id,
    created_date,
    visitor_count
from {{ ref('stg_ash__store_daily_visitors') }}
