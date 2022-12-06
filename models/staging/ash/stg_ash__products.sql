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
    subgroup_id,
    status,
    null as purchase_status,
    name,
    'Item' as type,
    code,
    color,
    is_exclusive,
    false as is_commodity,
    is_new,
    null as image_url,
    true as is_available_in_app
from {{ ref('base_ash__products') }}
