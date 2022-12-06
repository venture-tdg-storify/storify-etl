{{
    config(
        materialized = 'table',
    )
}}

select distinct
    group_id as id,
    group_id as name
from {{ ref('base_ash__products') }}
