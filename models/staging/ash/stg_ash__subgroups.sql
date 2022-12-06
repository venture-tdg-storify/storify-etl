{{
    config(
        materialized = 'table',
    )
}}

select distinct
    subgroup_id as id,
    subgroup_id as name
from {{ ref('base_ash__products') }}
