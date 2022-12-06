{% macro stg_afi__action_store_sales(tenant_name) %}
    with
    action_stores as (
        select
            id,
            completion_date,
            store_id,
            category_id,
            subcategory_id,
            group_id
        from {{ ref('stg_%s__action_stores' % tenant_name) }}
        where
            is_add = true
            and completion_status_str = 'Completed'
    ),

    category_inventory as (
        with
        base as (
            select
                inventory.store_id,
                products.category_id,
                products.group_id,
                inventory.inventory_date
            from {{ ref('stg_%s__inventory' % tenant_name) }} as inventory
            inner join {{ ref('stg_%s__products' % tenant_name) }} as products
                on inventory.product_id = products.id
            where
                inventory.is_on_floor
            group by
                inventory.store_id,
                products.category_id,
                products.group_id,
                inventory.inventory_date
        )

        select
            *,
            coalesce(
                lead(inventory_date) over (
                    partition by store_id, category_id, group_id
                    order by inventory_date
                ) - inventory_date > 1,
                true
            ) is_last_in_seq
        from base
    ),

    subcategory_inventory as (
        with
        base as (
            select
                inventory.store_id,
                products.category_id,
                products.subcategory_id,
                products.group_id,
                inventory.inventory_date
            from {{ ref('stg_%s__inventory' % tenant_name) }} as inventory
            inner join {{ ref('stg_%s__products' % tenant_name) }} as products
                on inventory.product_id = products.id
            where
                inventory.is_on_floor
            group by
                inventory.store_id,
                products.category_id,
                products.subcategory_id,
                products.group_id,
                inventory.inventory_date
        )

        select
            *,
            coalesce(
                lead(inventory_date) over (
                    partition by store_id, category_id, subcategory_id, group_id
                    order by inventory_date
                ) - inventory_date > 1,
                true
            ) is_last_in_seq
        from base
    ),

    category_action_stores_with_last_on_floor_date as (
        select
            action_stores.id,
            action_stores.completion_date,
            action_stores.store_id,
            action_stores.category_id,
            action_stores.group_id,
            min(inventory.inventory_date) + interval '1 year' as last_on_floor_date
        from action_stores
        left outer join category_inventory as inventory
            on inventory.store_id = action_stores.store_id
            and inventory.category_id = action_stores.category_id
            and inventory.group_id = action_stores.group_id
            and inventory.inventory_date >= action_stores.completion_date
            and inventory.is_last_in_seq
        where
            action_stores.subcategory_id is null
        group by
            action_stores.id,
            action_stores.completion_date,
            action_stores.store_id,
            action_stores.category_id,
            action_stores.group_id
    ),

    subcategory_action_stores_with_last_on_floor_date as (
        select
            action_stores.id,
            action_stores.completion_date,
            action_stores.store_id,
            action_stores.category_id,
            action_stores.subcategory_id,
            action_stores.group_id,
            min(inventory.inventory_date) + interval '1 year' as last_on_floor_date
        from action_stores
        left outer join subcategory_inventory as inventory
            on inventory.store_id = action_stores.store_id
            and inventory.category_id = action_stores.category_id
            and inventory.subcategory_id = action_stores.subcategory_id
            and inventory.group_id = action_stores.group_id
            and inventory.inventory_date >= action_stores.completion_date
            and inventory.is_last_in_seq
        where
            action_stores.subcategory_id is not null
        group by
            action_stores.id,
            action_stores.completion_date,
            action_stores.store_id,
            action_stores.category_id,
            action_stores.subcategory_id,
            action_stores.group_id
    ),

    category_action_stores_with_sales_per_day as (
        with
        action_store_on_floor_dates as (
            select
                action_stores.id,
                action_stores.completion_date,
                action_stores.store_id,
                action_stores.category_id,
                action_stores.group_id,
                inventory.inventory_date
            from category_action_stores_with_last_on_floor_date as action_stores
            inner join category_inventory as inventory
                on inventory.store_id = action_stores.store_id
                and inventory.category_id = action_stores.category_id
                and inventory.group_id = action_stores.group_id
                and inventory.inventory_date >= action_stores.completion_date
                and inventory.inventory_date <= action_stores.last_on_floor_date
        ),

        category_transactions as (
            select
                transactions.store_id,
                products.category_id,
                products.group_id,
                transactions.transaction_date,
                sum(transactions.amount) as amount
            from {{ ref('stg_%s__transactions' % tenant_name) }} as transactions
            inner join {{ ref('stg_%s__products' % tenant_name) }} as products
                on transactions.product_id = products.id
            group by
                transactions.store_id,
                products.category_id,
                products.group_id,
                transactions.transaction_date
        )

        select
            action_stores.id as action_store_id,
            action_stores.inventory_date as date,
            coalesce(transactions.amount, 0) as sales
        from action_store_on_floor_dates as action_stores
        left outer join category_transactions as transactions
            on transactions.store_id = action_stores.store_id
            and transactions.category_id = action_stores.category_id
            and transactions.group_id = action_stores.group_id
            and transactions.transaction_date = action_stores.inventory_date
    ),

    subcategory_action_stores_with_sales_per_day as (
        with
        action_store_on_floor_dates as (
            select
                action_stores.id,
                action_stores.completion_date,
                action_stores.store_id,
                action_stores.category_id,
                action_stores.subcategory_id,
                action_stores.group_id,
                inventory.inventory_date
            from subcategory_action_stores_with_last_on_floor_date as action_stores
            inner join subcategory_inventory as inventory
                on inventory.store_id = action_stores.store_id
                and inventory.category_id = action_stores.category_id
                and inventory.subcategory_id = action_stores.subcategory_id
                and inventory.group_id = action_stores.group_id
                and inventory.inventory_date >= action_stores.completion_date
                and inventory.inventory_date <= action_stores.last_on_floor_date
        ),

        subcategory_transactions as (
            select
                transactions.store_id,
                products.category_id,
                products.subcategory_id,
                products.group_id,
                transactions.transaction_date,
                sum(transactions.amount) as amount
            from {{ ref('stg_%s__transactions' % tenant_name) }} as transactions
            inner join {{ ref('stg_%s__products' % tenant_name) }} as products
                on transactions.product_id = products.id
            group by
                transactions.store_id,
                products.category_id,
                products.subcategory_id,
                products.group_id,
                transactions.transaction_date
        )

        select
            action_stores.id as action_store_id,
            action_stores.inventory_date as date,
            coalesce(transactions.amount, 0) as sales
        from action_store_on_floor_dates as action_stores
        left outer join subcategory_transactions as transactions
            on transactions.store_id = action_stores.store_id
            and transactions.category_id = action_stores.category_id
            and transactions.subcategory_id = action_stores.subcategory_id
            and transactions.group_id = action_stores.group_id
            and transactions.transaction_date = action_stores.inventory_date
    ),

    all_action_stores_with_sales_per_day as (
        select * from category_action_stores_with_sales_per_day
        union all
        select * from subcategory_action_stores_with_sales_per_day
    ),

    ranked_all_action_stores_with_sales_per_day as (
        select
            *,
            row_number() over(
                partition by
                    action_stores.category_id,
                    action_stores.subcategory_id,
                    action_stores.group_id,
                    action_stores.store_id,
                    action_stores.is_manual,
                    all_action_stores_with_sales_per_day.date
                order by
                    action_stores.completion_date
            ) as row_number
        from all_action_stores_with_sales_per_day
        inner join {{ ref('stg_%s__action_stores' % tenant_name) }} as action_stores
            on action_stores.id = all_action_stores_with_sales_per_day.action_store_id
    )

    select
        action_store_id,
        date,
        sales
    from ranked_all_action_stores_with_sales_per_day
    where
        row_number = 1
{% endmacro %}
