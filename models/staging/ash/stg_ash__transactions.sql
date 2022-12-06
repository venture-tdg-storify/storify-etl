{{
    config(
        materialized = 'table',
    )
}}

select
    transactions.order_id,
    transactions.product_id,
    transactions.store_id,
    products.category_id,
    products.subcategory_id,
    products.group_id,
    transactions.amount,
    transactions.quantity,
    transactions.transaction_date,
    coalesce(inventory.product_id is not null, false) as is_in_inventory,
    coalesce(inventory.product_id is not null, false) as is_on_floor,
    coalesce(inventory.on_floor_days_count, 0) as on_floor_days_count
from {{ ref('base_ash__transactions') }} as transactions
left outer join {{ ref('stg_ash__products') }} as products
    on transactions.product_id = products.id
left outer join {{ ref('stg_ash__inventory') }} as inventory
    on transactions.product_id = inventory.product_id
    and transactions.store_id = inventory.store_id
    and transactions.transaction_date = inventory.inventory_date
