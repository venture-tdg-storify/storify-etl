{{
    config(
        materialized = 'table',
    )
}}

{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_category_quarter_sales as (
        select
            transactions.transaction_date,
            products.category_id,
            sum(transactions.amount) as amount
        from {{ ref('stg_%s__transactions' % tenant_name) }} as transactions
        inner join {{ ref('stg_%s__products' % tenant_name) }} as products
            on transactions.product_id = products.id
        group by
            transactions.transaction_date, products.category_id
    )

    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select
        tenants.name as tenant_name,
        category_quarter_sales.category_id,
        category_quarter_sales.transaction_date,
        category_quarter_sales.amount
    from {{ tenant_name }}_category_quarter_sales as category_quarter_sales
    inner join {{ ref('tenants') }} as tenants
        on tenants.codebase_name = '{{ tenant_name }}'
    {% if not loop.last %}union all{% endif %}
{% endfor %}
