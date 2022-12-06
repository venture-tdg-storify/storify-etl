{{
    config(
        materialized = 'table',
    )
}}

select distinct
    category_id as id,
    category_id as name,
    null as description,
    true as is_available_in_app
from {{ ref('base_ash__products') }}
