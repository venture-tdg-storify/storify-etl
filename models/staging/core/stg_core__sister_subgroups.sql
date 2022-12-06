{{
    config(
        materialized = 'table',
    )
}}

with
latest as (
    select
        tenant_id,
        category_id,
        group_id,
        subgroup_id,
        user_job_timestamp,
        rank() over (
            partition by tenant_id
            order by user_job_timestamp desc
        ) as rank
    from {{ ref('base_core__sister_subgroups') }}
)

select * exclude(rank)
from latest
where rank = 1
