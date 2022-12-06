{% macro fct_afi__swap_performance(tenant_name) %}
    with
    category_inventory as (
        select
            inventory.store_id,
            products.category_id,
            products.group_id,
            inventory.inventory_date
        from {{ ref('stg_%s__inventory' % tenant_name) }} as inventory
        inner join {{ ref('stg_%s__products' % tenant_name) }} as products
            on inventory.product_id = products.id
        where inventory.is_on_floor
        group by
            inventory.store_id,
            products.category_id,
            products.group_id,
            inventory.inventory_date
    ),

    subcategory_inventory as (
        select
            inventory.store_id,
            products.subcategory_id,
            products.group_id,
            inventory.inventory_date
        from {{ ref('stg_%s__inventory' % tenant_name) }} as inventory
        inner join {{ ref('stg_%s__products' % tenant_name) }} as products
            on inventory.product_id = products.id
        where inventory.is_on_floor
        group by
            inventory.store_id,
            products.subcategory_id,
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
        inner join category_inventory
            on
                transactions.store_id = category_inventory.store_id
                and products.category_id = category_inventory.category_id
                and products.group_id = category_inventory.group_id
                and transactions.transaction_date = category_inventory.inventory_date
        group by
            transactions.store_id,
            products.category_id,
            products.group_id,
            transactions.transaction_date
    ),

    subcategory_transactions as (
        select
            transactions.store_id,
            products.subcategory_id,
            products.group_id,
            transactions.transaction_date,
            sum(transactions.amount) as amount
        from {{ ref('stg_%s__transactions' % tenant_name) }} as transactions
        inner join {{ ref('stg_%s__products' % tenant_name) }} as products
            on transactions.product_id = products.id
        inner join subcategory_inventory
            on
                transactions.store_id = subcategory_inventory.store_id
                and products.subcategory_id = subcategory_inventory.subcategory_id
                and products.group_id = subcategory_inventory.group_id
                and transactions.transaction_date = subcategory_inventory.inventory_date
        group by
            transactions.store_id,
            products.subcategory_id,
            products.group_id,
            transactions.transaction_date
    ),

    principal_action_stores as (
        with
        recommendations as (
            select
                recommendations.store_id,
                products.category_id,
                products.subcategory_id,
                products.group_id,
                sum(recommendations.predicted_sales) as predicted_sales
            from {{ ref('stg_%s__recommendations' % tenant_name) }} as recommendations
            inner join {{ ref('stg_%s__products' % tenant_name) }} as products
                on recommendations.product_id = products.id
            group by
                recommendations.store_id,
                products.category_id,
                products.subcategory_id,
                products.group_id
        ),

        action_stores as (
            select
                action_stores.id,
                action_stores.action_set_finalized_at,
                action_stores.store_id,
                action_stores.category_id,
                action_stores.subcategory_id,
                action_stores.group_id,
                action_stores.completion_date,
                action_stores.created_at,
                action_stores.predicted_sales as predicted_sales_when_created,
                sum(recommendations.predicted_sales) as predicted_sales_now
            from {{ ref('stg_%s__action_stores' % tenant_name) }} as action_stores
            left outer join recommendations
                on
                    action_stores.store_id = recommendations.store_id
                    and action_stores.category_id = recommendations.category_id
                    and (
                        action_stores.subcategory_id is null
                        or action_stores.subcategory_id = recommendations.subcategory_id
                    )
                    and action_stores.group_id = recommendations.group_id
            where
                principal_action_store_id is null
                and completion_status_str = 'Completed'
            group by
                action_stores.id,
                action_stores.action_set_finalized_at,
                action_stores.store_id,
                action_stores.category_id,
                action_stores.subcategory_id,
                action_stores.group_id,
                action_stores.completion_date,
                action_stores.created_at,
                action_stores.predicted_sales
        ),

        action_stores_with_amount as (
            select
                action_stores.*,
                (
                    coalesce(sum(category_transactions.amount), 0) +
                    coalesce(sum(subcategory_transactions.amount), 0)
                ) as amount
            from action_stores
            left outer join category_transactions
                on
                    action_stores.subcategory_id is null
                    and (
                        action_stores.store_id = category_transactions.store_id
                        and action_stores.category_id = category_transactions.category_id
                        and action_stores.group_id = category_transactions.group_id
                        and category_transactions.transaction_date >= action_stores.completion_date
                    )
            left outer join subcategory_transactions
                on
                    action_stores.subcategory_id is not null
                    and (
                        action_stores.store_id = subcategory_transactions.store_id
                        and action_stores.subcategory_id = subcategory_transactions.subcategory_id
                        and action_stores.group_id = subcategory_transactions.group_id
                        and subcategory_transactions.transaction_date >= action_stores.completion_date
                    )
            group by
                action_stores.id,
                action_stores.action_set_finalized_at,
                action_stores.store_id,
                action_stores.category_id,
                action_stores.subcategory_id,
                action_stores.group_id,
                action_stores.completion_date,
                action_stores.created_at,
                action_stores.predicted_sales_when_created,
                action_stores.predicted_sales_now
        )

        select
            action_stores.*,
            (
                count(distinct category_inventory.inventory_date) +
                count(distinct subcategory_inventory.inventory_date)
            ) as on_floor_days
        from action_stores_with_amount as action_stores
        left outer join category_inventory
            on
                action_stores.subcategory_id is null
                and (
                    action_stores.store_id = category_inventory.store_id
                    and action_stores.category_id = category_inventory.category_id
                    and action_stores.group_id = category_inventory.group_id
                    and category_inventory.inventory_date >= action_stores.completion_date
                )
        left outer join subcategory_inventory
            on
                action_stores.subcategory_id is not null
                and (
                    action_stores.store_id = subcategory_inventory.store_id
                    and action_stores.subcategory_id = subcategory_inventory.subcategory_id
                    and action_stores.group_id = subcategory_inventory.group_id
                    and subcategory_inventory.inventory_date >= action_stores.completion_date
                )
        group by
            action_stores.id,
            action_stores.action_set_finalized_at,
            action_stores.store_id,
            action_stores.category_id,
            action_stores.subcategory_id,
            action_stores.group_id,
            action_stores.completion_date,
            action_stores.created_at,
            action_stores.predicted_sales_when_created,
            action_stores.predicted_sales_now,
            action_stores.amount
    ),

    dependent_action_stores as (
        with
        action_stores_with_amount as (
            select
                action_stores.id,
                action_stores.store_id,
                action_stores.principal_action_store_id,
                principal_action_stores.completion_date as principal_completion_date,
                action_stores.category_id,
                action_stores.subcategory_id,
                action_stores.group_id,
                action_stores.completion_date,
                action_stores.created_at,
                (
                    coalesce(sum(category_transactions.amount), 0) +
                    coalesce(sum(subcategory_transactions.amount), 0)
                ) as amount
            from {{ ref('stg_%s__action_stores' % tenant_name) }} as action_stores
            inner join principal_action_stores
                on action_stores.principal_action_store_id = principal_action_stores.id
            left outer join category_transactions
                on
                    action_stores.subcategory_id is null
                    and (
                        action_stores.store_id <> category_transactions.store_id
                        and action_stores.category_id = category_transactions.category_id
                        and action_stores.group_id = category_transactions.group_id
                        and category_transactions.transaction_date >= principal_action_stores.completion_date
                    )
            left outer join subcategory_transactions
                on
                    action_stores.subcategory_id is not null
                    and (
                        action_stores.store_id <> subcategory_transactions.store_id
                        and action_stores.subcategory_id = subcategory_transactions.subcategory_id
                        and action_stores.group_id = subcategory_transactions.group_id
                        and subcategory_transactions.transaction_date >= principal_action_stores.completion_date
                    )
            where
                principal_action_store_id is not null
                and completion_status_str = 'Completed'
            group by
                action_stores.id,
                action_stores.store_id,
                action_stores.principal_action_store_id,
                principal_action_stores.completion_date,
                action_stores.category_id,
                action_stores.subcategory_id,
                action_stores.group_id,
                action_stores.completion_date,
                action_stores.created_at
        )

        select
            action_stores.*,
            (
                count(distinct category_inventory.store_id, category_inventory.inventory_date) +
                count(distinct subcategory_inventory.store_id, subcategory_inventory.inventory_date)
            ) as on_floor_days
        from action_stores_with_amount as action_stores
        left outer join category_inventory
            on
                action_stores.subcategory_id is null
                and (
                    action_stores.store_id <> category_inventory.store_id
                    and action_stores.category_id = category_inventory.category_id
                    and action_stores.group_id = category_inventory.group_id
                    and category_inventory.inventory_date >= principal_completion_date
                )
        left outer join subcategory_inventory
            on
                action_stores.subcategory_id is not null
                and (
                    action_stores.store_id <> subcategory_inventory.store_id
                    and action_stores.subcategory_id = subcategory_inventory.subcategory_id
                    and action_stores.group_id = subcategory_inventory.group_id
                    and subcategory_inventory.inventory_date >= principal_completion_date
                )
        group by
            action_stores.id,
            action_stores.store_id,
            action_stores.principal_action_store_id,
            action_stores.principal_completion_date,
            action_stores.category_id,
            action_stores.subcategory_id,
            action_stores.group_id,
            action_stores.completion_date,
            action_stores.created_at,
            action_stores.amount
    ),

    swaps as (
        select
            principal_action_stores.store_id,
            principal_action_stores.id as principal_action_store_id,
            principal_action_stores.action_set_finalized_at,
            principal_action_stores.category_id as principal_category_id,
            principal_action_stores.subcategory_id as principal_subcategory_id,
            principal_action_stores.group_id as principal_group_id,
            principal_action_stores.completion_date as principal_completion_date,
            principal_action_stores.created_at as principal_created_at,
            principal_action_stores.predicted_sales_when_created as principal_predicted_sales_when_created,
            principal_action_stores.predicted_sales_now as principal_predicted_sales_now,
            principal_action_stores.amount as principal_amount,
            principal_action_stores.on_floor_days as principal_on_floor_days,
            dependent_action_stores.id as dependent_action_store_id,
            dependent_action_stores.category_id as dependent_category_id,
            dependent_action_stores.subcategory_id as dependent_subcategory_id,
            dependent_action_stores.group_id as dependent_group_id,
            dependent_action_stores.completion_date as dependent_completion_date,
            dependent_action_stores.created_at as dependent_created_at,
            dependent_action_stores.amount as dependent_amount,
            dependent_action_stores.on_floor_days as dependent_on_floor_days
        from principal_action_stores
        left outer join dependent_action_stores
            on principal_action_stores.id = dependent_action_stores.principal_action_store_id
    )

    select
        principal_action_store_id as swap_id,
        dependent_action_store_id as dependent_swap_id,
        action_set_finalized_at as plan_finalized_at,
        store_id,
        principal_category_id as add_category_id,
        principal_subcategory_id as add_subcategory_id,
        principal_group_id as add_group_id,
        principal_completion_date as add_date,
        principal_created_at as add_created_at,
        principal_predicted_sales_when_created as predicted_sales_when_created,
        principal_predicted_sales_now as predicted_sales_now,
        principal_on_floor_days as add_on_floor_days_after_add_date,
        principal_amount as add_amount_after_add_date,
        case
            when add_on_floor_days_after_add_date = 0 then null
            else add_amount_after_add_date / add_on_floor_days_after_add_date
        end as add_amount_per_day_after_add_date,
        predicted_sales_when_created / 90 as predicted_sales_when_created_per_day,
        dependent_category_id as drop_category_id,
        dependent_subcategory_id as drop_subcategory_id,
        dependent_group_id as drop_group_id,
        dependent_completion_date as drop_date,
        dependent_created_at as drop_created_at,
        dependent_on_floor_days as drop_on_floor_days_in_other_stores_after_add_date,
        dependent_amount as drop_amount_in_other_stores_after_add_date
    from swaps
{% endmacro %}
