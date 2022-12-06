{{
    config(
        materialized = 'table',
        alias = 'subgroups',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_subgroups as (
        select
            tenants.id as tenant_id,
            subgroups.id,
            subgroups.name
        from {{ ref('stg_%s__subgroups' % tenant_name) }} as subgroups
        inner join {{ ref('tenants') }} as tenants
        where tenants.codebase_name = '{{ tenant_name }}'
    )
    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select *
    from {{ tenant_name }}_subgroups
    {% if not loop.last %}union all{% endif %}
{% endfor %}
