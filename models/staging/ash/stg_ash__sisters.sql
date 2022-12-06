{{
    config(
        materialized = 'table',
    )
}}

select
    sisters.product_id,
    products.id,
    products.status,
    sisters.group_id,
    sisters.subgroup_id,
    sisters.category_id,
from {{ ref('base_ash__sisters') }} as sisters
left outer join {{ ref('base_ash__products') }} as products
    on sisters.product_id = products.id
