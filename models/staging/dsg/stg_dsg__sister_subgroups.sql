{{
    config(
        materialized = 'table',
    )
}}

select
    category_id,
    group_id,
    subgroup_id
from {{ ref('stg_core__sister_subgroups') }}
inner join {{ ref('tenants') }} as tenants
    on tenant_id = tenants.id
where tenants.codebase_name in ('dsg', 'dsg_demo')
