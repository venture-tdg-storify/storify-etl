{{
    config(
        materialized = 'table',
    )
}}

select
    id,
    category_id,
    subcategory_id,
    group_id,
    status,
    name,
    code,
    color,
    is_exclusive,
    is_commodity,
    is_new
from {{ ref('stg_ash__products') }}
