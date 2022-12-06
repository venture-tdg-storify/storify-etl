{{
    config(
        materialized = 'table',
    )
}}

select
    tenant_id,
    category_id,
    group_id,
    subgroup_id,
    user_job_timestamp
from {{ source('core', 'sister_subgroups') }}
