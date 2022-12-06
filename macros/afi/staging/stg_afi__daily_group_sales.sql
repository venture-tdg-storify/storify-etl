{% macro stg_afi__daily_group_sales(tenant_name) %}
    with
    group_start_newness_date as (
        with
            minimum_transaction_date as (
                select
                    products.category_id,
                    products.group_id,
                    min(transactions.transaction_date) as min_date
                from {{ ref('stg_%s__products' % tenant_name) }} as products
                left outer join {{ ref('stg_%s__transactions' % tenant_name) }} as transactions
                    on products.id = transactions.product_id
                group by
                    products.category_id,
                    products.group_id
            ),

            minimum_inventory_date as (
                select
                    products.category_id,
                    products.group_id,
                    min(inventory.inventory_date) as min_date
                from {{ ref('stg_%s__products' % tenant_name) }} as products
                left outer join {{ ref('stg_%s__inventory' % tenant_name) }} as inventory
                    on products.id = inventory.product_id
                    and inventory.is_on_floor
                group by
                    products.category_id,
                    products.group_id
            ),

            all_dates as (
                select * from minimum_transaction_date
                union all
                select * from minimum_inventory_date
            )

            select
                category_id,
                group_id,
                min(min_date) as date
            from all_dates
            group by
                category_id,
                group_id
    ),

    category_inventory as (
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
    ),

    category_inventory_with_sales as (
        select
            inventory.category_id,
            inventory.group_id,
            inventory.store_id,
            inventory.inventory_date as date,
            coalesce(transactions.amount, 0) as sales
        from category_inventory as inventory
        left outer join category_transactions as transactions
            on transactions.store_id = inventory.store_id
            and transactions.category_id = inventory.category_id
            and transactions.group_id = inventory.group_id
            and transactions.transaction_date = inventory.inventory_date
    ),

    arteli_sales as (
        with
        base as (
            select
                action_stores.category_id,
                action_stores.subcategory_id,
                action_stores.group_id,
                action_stores.store_id,
                action_store_sales.date,
                action_store_sales.sales
            from {{ ref('stg_%s__action_stores' % tenant_name) }} as action_stores
            inner join {{ ref('stg_%s__action_store_sales' % tenant_name) }} as action_store_sales
                on action_store_sales.action_store_id = action_stores.id
            where action_stores.is_manual = false
        )

        select
            category_id,
            group_id,
            store_id,
            date,
            'arteli' as type,
            sum(sales) as sales,
            any_value(subcategory_id) is null as is_full_category
        from base
        group by
            category_id,
            group_id,
            store_id,
            date
    ),

    all_sales as (
        select
            inventory_with_sales.category_id,
            inventory_with_sales.group_id,
            inventory_with_sales.store_id,
            inventory_with_sales.date,
            inventory_with_sales.sales,
            case
                when group_start_newness_date.date + interval '6 months' > inventory_with_sales.date then
                    'new'
                else
                    'manual'
            end as type
        from category_inventory_with_sales as inventory_with_sales
        left outer join group_start_newness_date
            on group_start_newness_date.category_id = inventory_with_sales.category_id
            and group_start_newness_date.group_id = inventory_with_sales.group_id
    ),

    other_sales as (
        select
            all_sales.category_id,
            all_sales.group_id,
            all_sales.store_id,
            all_sales.date,
            all_sales.type,
            all_sales.sales - coalesce(arteli_sales.sales, 0) as sales
        from all_sales
        left outer join arteli_sales
            on arteli_sales.category_id = all_sales.category_id
            and arteli_sales.group_id = all_sales.group_id
            and arteli_sales.store_id = all_sales.store_id
            and arteli_sales.date = all_sales.date
        where
            arteli_sales.category_id is null
            or arteli_sales.is_full_category = false
    ),

    daily_group_sales as (
        select
            category_id,
            group_id,
            store_id,
            date,
            type,
            sales
        from arteli_sales
        union all
        select
            category_id,
            group_id,
            store_id,
            date,
            type,
            sales
        from other_sales
    )

    select
        *
    from daily_group_sales
    where
        category_id != 'ACCESS'
{% endmacro %}
