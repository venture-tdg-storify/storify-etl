{{
    config(
        materialized = 'table',
    )
}}

{% set tenant_names = get_tenant_codebase_names('ash') %}

with
base as (
    {% for tenant_name in tenant_names %}
        select
            tenants.name as tenant_name,
            categories.name as category_name,
            subcategories.name as subcategory_name,
            groups.name as group_name,
            action_stores.is_manual as action_store_is_manual,
            action_store_sales.date,
            action_store_sales.sales
        from {{ ref('stg_%s__action_store_sales' % tenant_name) }} as action_store_sales
        inner join {{ ref('tenants') }} as tenants
            on tenants.codebase_name = '{{ tenant_name }}'
        inner join {{ ref('stg_%s__action_stores' % tenant_name) }} as action_stores
            on action_store_sales.action_store_id = action_stores.id
        inner join {{ ref('stg_%s__categories' % tenant_name) }} as categories
            on categories.id = action_stores.category_id
        left outer join {{ ref('stg_%s__subcategories' % tenant_name) }} as subcategories
            on subcategories.id = action_stores.subcategory_id
        inner join {{ ref('stg_%s__groups' % tenant_name) }} as groups
            on groups.id = action_stores.group_id
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select
    tenant_name,
    category_name,
    subcategory_name,
    group_name,
    action_store_is_manual,
    date,
    sum(sales) as sales
from base
group by
    tenant_name,
    category_name,
    subcategory_name,
    group_name,
    action_store_is_manual,
    date
