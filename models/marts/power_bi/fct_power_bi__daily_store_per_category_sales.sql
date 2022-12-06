{{
    config(
        materialized = 'table',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_store_quarter_sales as (
        select
            transactions.transaction_date,
            stores.id || ' ' || stores.name as store_name,
            categories.name as category_name,
            sum(transactions.amount) as amount
        from {{ ref('stg_%s__transactions' % tenant_name) }} as transactions
        inner join {{ ref('stg_%s__stores' % tenant_name) }} as stores
            on transactions.store_id = stores.id
        inner join {{ ref('stg_%s__products' % tenant_name) }} as products
            on products.id = transactions.product_id
        inner join {{ ref('stg_%s__categories' % tenant_name) }} as categories
            on categories.id = products.category_id
        group by
            transactions.transaction_date, stores.id, stores.name, categories.name
    )

    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select
        tenants.name as tenant_name,
        store_quarter_sales.store_name,
        store_quarter_sales.category_name,
        store_quarter_sales.transaction_date,
        store_quarter_sales.amount
    from {{ tenant_name }}_store_quarter_sales as store_quarter_sales
    inner join {{ ref('tenants') }} as tenants
        on tenants.codebase_name = '{{ tenant_name }}'
    {% if not loop.last %}union all{% endif %}
{% endfor %}
