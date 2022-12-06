{{
    config(
        materialized = 'table',
        alias = 'subcategories',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_subcategories as (
        select
            tenants.id as tenant_id,
            subcategories.id,
            subcategories.name,
            subcategories.category_id
        from {{ ref('stg_%s__subcategories' % tenant_name) }} as subcategories
        inner join {{ ref('tenants') }} as tenants
        inner join {{ ref('stg_%s__categories' % tenant_name) }} as categories
            on categories.id = subcategories.category_id
        where
            tenants.codebase_name = '{{ tenant_name }}'
            and categories.is_available_in_app
    )
    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select *
    from {{ tenant_name }}_subcategories
    {% if not loop.last %}union all{% endif %}
{% endfor %}
