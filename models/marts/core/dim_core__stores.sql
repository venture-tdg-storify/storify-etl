{{
    config(
        materialized = 'table',
        alias = 'stores',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_stores as (
        select
            tenants.id as tenant_id,
            stores.id,
            stores.name,
            stores.is_active
        from {{ ref('stg_%s__stores' % tenant_name) }} as stores
        inner join {{ ref('tenants') }} as tenants
        where tenants.codebase_name = '{{ tenant_name }}'
    )
    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select *
    from {{ tenant_name }}_stores
    {% if not loop.last %}union all{% endif %}
{% endfor %}
