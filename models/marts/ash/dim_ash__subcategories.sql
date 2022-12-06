{{
    config(
        materialized = 'table',
    )
}}

select
    id,
    name,
    category_id
from {{ ref('stg_ash__subcategories') }}
