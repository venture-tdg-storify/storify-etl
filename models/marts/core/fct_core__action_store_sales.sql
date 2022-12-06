{{
    config(
        materialized = 'table',
        alias = 'action_store_sales',
    )
}}

{% set tenant_names = get_tenant_codebase_names('ash') %}

{% for tenant_name in tenant_names %}
    select
        tenants.id as tenant_id,
        action_store_sales.action_store_id,
        action_store_sales.date,
        action_store_sales.sales
    from {{ ref('stg_%s__action_store_sales' % tenant_name) }} as action_store_sales
    inner join {{ ref('tenants') }} as tenants
        on tenants.codebase_name = '{{ tenant_name }}'
    {% if not loop.last %}union all{% endif %}
{% endfor %}
