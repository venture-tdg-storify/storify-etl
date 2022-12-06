{{
    config(
        materialized = 'table',
    )
}}

select distinct
    subcategory_id as id,
    subcategory_name as name,
    category_id
from {{ ref('base_ash__products') }}
