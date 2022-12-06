{{
    config(
        materialized = 'table',
        alias = 'ai_action_stores',
    )
}}

{% set tenant_names = ['dsg'] %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_ai_action_stores as (
        select
            tenants.id as tenant_id,
            ai_action_stores.store_id,
            ai_action_stores.category_id,
            ai_action_stores.subcategory_id,
            ai_action_stores.product_code,
            ai_action_stores.remove_group_id,
            ai_action_stores.remove_predicted_sales,
            ai_action_stores.add_group_id,
            ai_action_stores.add_predicted_sales
        from {{ ref('base_%s__ai_action_stores' % tenant_name) }} as ai_action_stores
        inner join {{ ref('tenants') }} as tenants
        where
            tenants.codebase_name = '{{ tenant_name }}'
    )
    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select * from {{ tenant_name }}_ai_action_stores

    {% if not loop.last %}union all{% endif %}
{% endfor %}
