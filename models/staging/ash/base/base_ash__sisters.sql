{{
    config(
        materialized = 'table',
    )
}}

select
    item as product_id,
    group_name as group_id,
    series_number as subgroup_id,
    category as category_id
from {{ source('ash', 'raw_sister') }}
