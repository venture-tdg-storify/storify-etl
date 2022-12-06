{{
    config(
        materialized = 'table',
    )
}}

select
    store_id,
    created_date,
    visitor_count
from {{ ref('base_ash__store_daily_visitors') }}
