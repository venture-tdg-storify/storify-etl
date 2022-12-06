{% set tenant_names = get_tenant_codebase_names() %}

with
{% for tenant_name in tenant_names %}
    {{ tenant_name }}_recommendations as (
        with
        inactive_products as (
            select
                *
            from {{ ref('stg_%s__products' % tenant_name) }}
            where status = 'inactive'
        ),

        product_inventory as (
            select distinct
                inventory.product_id,
                inventory.store_id
            from {{ ref('stg_%s__inventory' % tenant_name) }} as inventory
            inner join {{ ref('stg_%s__stores' % tenant_name) }} as stores
                on stores.id = inventory.store_id and stores.is_active
            where
                inventory.inventory_date = (select max(inventory_date) from {{ ref('stg_%s__inventory' % tenant_name) }})
                {% if tenant_name != 'ash' %}
                    and inventory.is_on_floor
                {% endif %}
        ),

        product_transactions as (
            select distinct
                transactions.product_id,
                transactions.store_id,
            from {{ ref('stg_%s__transactions' % tenant_name) }} as transactions
            inner join {{ ref('stg_%s__stores' % tenant_name) }} as stores
                on stores.id = transactions.store_id and stores.is_active
            where
                transactions.transaction_date > (select max(transaction_date) - interval '90 days' from {{ ref('stg_%s__transactions' % tenant_name) }})
        ),

        expected_product_recommendations_count as (
            select
                product_id,
                count(distinct store_id) as count
            from (
                select * from product_inventory
                union all
                select * from product_transactions
            )
            group by
                product_id
        ),

        product_recommendations_count as (
            select
                product_id,
                count(*) as count
            from {{ ref('fct_core__recommendations') }} as recommendations
            inner join {{ ref('tenants') }} as tenants
                on tenants.id = recommendations.tenant_id and tenants.codebase_name = '{{ tenant_name }}'
            inner join inactive_products as products
                on products.id = recommendations.product_id
            group by
                product_id
        )

        select
            product_recommendations_count.*,
            expected_product_recommendations_count.*
        from product_recommendations_count
        left outer join expected_product_recommendations_count
            on expected_product_recommendations_count.product_id = product_recommendations_count.product_id
        where
            product_recommendations_count.count <> expected_product_recommendations_count.count
    )
    {% if not loop.last %}, {% endif %}
{% endfor %}

{% for tenant_name in tenant_names %}
    select *
    from {{ tenant_name }}_recommendations
    {% if not loop.last %}union all{% endif %}
{% endfor %}
