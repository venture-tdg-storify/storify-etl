{% macro stg_afi__action_store_completion_dates(tenant_name) %}
    with
    completed_action_stores as (
        select
            action_stores.id,
            action_stores.store_id,
            action_stores.category_id,
            action_stores.subcategory_id,
            action_stores.group_id,
            action_stores.is_add,
            action_stores.created_at,
            action_stores.completion_date
        from {{ ref('stg_%s__action_stores' % tenant_name) }} as action_stores
        left outer join {{ ref('base_ops__action_store_completion_date_acks') }} as action_store_completion_date_acks
            on action_store_completion_date_acks.action_store_id = action_stores.id
        where
            action_store_completion_date_acks.action_store_id is null
            and action_stores.completion_status_str = 'Completed'
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

    add_action_stores as (
        select
            action_stores.id,
            coalesce(
                min(category_inventory.inventory_date),
                min(subcategory_inventory.inventory_date)
            ) as completion_date
        from completed_action_stores as action_stores
        left outer join category_inventory
            on action_stores.subcategory_id is null
            and action_stores.store_id = category_inventory.store_id
            and action_stores.category_id = category_inventory.category_id
            and action_stores.group_id = category_inventory.group_id
            and action_stores.created_at::date <= category_inventory.inventory_date
        left outer join subcategory_inventory
            on action_stores.subcategory_id is not null
            and action_stores.store_id = subcategory_inventory.store_id
            and action_stores.category_id = subcategory_inventory.category_id
            and action_stores.subcategory_id = subcategory_inventory.subcategory_id
            and action_stores.group_id = subcategory_inventory.group_id
            and action_stores.created_at::date <= subcategory_inventory.inventory_date
        where
            action_stores.is_add
        group by
            action_stores.id
    ),

    remove_action_stores as (
        select
            action_stores.id,
            coalesce(
                min(category_inventory.inventory_date),
                min(subcategory_inventory.inventory_date)
            ) + interval '1 day' as completion_date
        from completed_action_stores as action_stores
        left outer join category_inventory
            on action_stores.subcategory_id is null
            and action_stores.store_id = category_inventory.store_id
            and action_stores.category_id = category_inventory.category_id
            and action_stores.group_id = category_inventory.group_id
            and action_stores.created_at::date <= category_inventory.inventory_date
            and category_inventory.is_last_in_seq
        left outer join subcategory_inventory
            on action_stores.subcategory_id is not null
            and action_stores.store_id = subcategory_inventory.store_id
            and action_stores.category_id = subcategory_inventory.category_id
            and action_stores.subcategory_id = subcategory_inventory.subcategory_id
            and action_stores.group_id = subcategory_inventory.group_id
            and action_stores.created_at::date <= subcategory_inventory.inventory_date
            and subcategory_inventory.is_last_in_seq
        where
            action_stores.is_add = false
        group by
            action_stores.id
    ),

    all_action_stores as (
        select * from add_action_stores
        union all
        select * from remove_action_stores
    )

    select
        all_action_stores.id as action_store_id,
        all_action_stores.completion_date
    from all_action_stores
    inner join completed_action_stores
        on completed_action_stores.id = all_action_stores.id
    where
        completed_action_stores.completion_date <> all_action_stores.completion_date
{% endmacro %}
