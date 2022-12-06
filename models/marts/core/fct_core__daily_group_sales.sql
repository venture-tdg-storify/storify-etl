{{
    config(
        materialized = 'table',
        alias = 'daily_group_sales',
    )
}}

{% set tenant_names = get_tenant_codebase_names('ash') %}

{% for tenant_name in tenant_names %}
    select
        tenants.id as tenant_id,
        daily_group_sales.category_id,
        daily_group_sales.group_id,
        daily_group_sales.store_id,
        daily_group_sales.date,
        daily_group_sales.sales,
        daily_group_sales.type
    from {{ ref('stg_%s__daily_group_sales' % tenant_name) }} as daily_group_sales
    inner join {{ ref('tenants') }} as tenants
        on tenants.codebase_name = '{{ tenant_name }}'
    {% if not loop.last %}union all{% endif %}
{% endfor %}
