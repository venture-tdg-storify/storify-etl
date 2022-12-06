{% macro stg_afi__transactions(tenant_name) %}
    select
        transactions.order_id,
        transactions.product_id,
        transactions.store_id,
        products.group_id,
        transactions.amount,
        transactions.quantity,
        transactions.transaction_date,
        coalesce(inventory.product_id is not null, false) as is_in_inventory,
        coalesce(inventory.is_on_floor, false) as is_on_floor,
        coalesce(inventory.on_floor_days_count, 0) as on_floor_days_count
    from {{ ref('base_%s__transactions' % tenant_name) }} as transactions
    left outer join {{ ref('stg_%s__products' % tenant_name) }} as products
        on transactions.product_id = products.id
    left outer join {{ ref('stg_%s__inventory' % tenant_name) }} as inventory
        on transactions.product_id = inventory.product_id
        and transactions.store_id = inventory.store_id
        and transactions.transaction_date = inventory.inventory_date
{% endmacro %}
