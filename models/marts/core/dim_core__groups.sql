{{
    config(
        materialized = 'table',
        alias = 'groups',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_groups as (
        select
            tenants.id as tenant_id,
            groups.id,
            groups.name
        from {{ ref('stg_%s__groups' % tenant_name) }} as groups
        inner join {{ ref('tenants') }} as tenants
        where tenants.codebase_name = '{{ tenant_name }}'
    )
    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select *
    from {{ tenant_name }}_groups
    {% if not loop.last %}union all{% endif %}
{% endfor %}
