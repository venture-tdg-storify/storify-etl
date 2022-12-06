{{
    config(
        materialized = 'table',
    )
}}

select
    id,
    internal_id,
    name,
    codebase_name
from {{ ref('seed_core__%s_tenants' % target.name) }}
