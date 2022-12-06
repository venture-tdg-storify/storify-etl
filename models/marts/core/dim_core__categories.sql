{{
    config(
        materialized = 'table',
        alias = 'categories',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_categories as (
        select
            tenants.id as tenant_id,
            categories.id,
            categories.name,
            categories.description
        from {{ ref('stg_%s__categories' % tenant_name) }} as categories
        inner join {{ ref('tenants') }} as tenants
        where
            tenants.codebase_name = '{{ tenant_name }}'
            and categories.is_available_in_app
    )
    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select *
    from {{ tenant_name }}_categories
    {% if not loop.last %}union all{% endif %}
{% endfor %}
