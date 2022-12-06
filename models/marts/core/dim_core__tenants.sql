{{
    config(
        materialized = 'table',
        alias = 'tenants',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
all_predictions_info as (
    {% for tenant_name in tenant_names %}
        select
            generated_at,
            '{{ tenant_name }}' as tenant_name
        from {{ ref('base_%s__predictions_info' % tenant_name) }}
        {% if not loop.last %}union all {% endif %}
    {% endfor %}
),

all_tenants as (
    select
        id,
        internal_id,
        name,
        codebase_name
    from {{ ref('tenants') }}
)

select
    all_tenants.id,
    all_tenants.internal_id,
    all_tenants.name,
    all_predictions_info.generated_at as predictions_timestamp
from all_tenants
left outer join all_predictions_info
    on all_predictions_info.tenant_name = all_tenants.codebase_name
