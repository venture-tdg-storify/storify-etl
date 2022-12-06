{% macro stg_afi__recommendations(tenant_name) %}
    with
    inventory as (
        select
            product_id,
            store_id,
            is_on_floor,
            floor_start_date,
            -- Ideally, we would have `is_clearance` as a boolean in base inventory
            -- But, building incremental inventory models with additional fields increases complexity
            -- So, we might come back to it eventually
            case
                when regexp_like(reason_codes, '^CL.*') or regexp_like(reason_codes, '.*,CL.*')
                    then true
                else
                    false
            end as is_clearance
        from {{ ref('stg_%s__inventory' % tenant_name) }}
        where inventory_date = (
            select max(inventory_date)
            from {{ ref('stg_%s__inventory' % tenant_name) }}
        )
    ),

    transactions as (
        with
        days_on_floor as (
            select
                product_id,
                store_id,
                count(*) as value
            from {{ ref('stg_%s__inventory' % tenant_name) }}
            where
                is_on_floor = true
                and inventory_date > (
                    select max(transaction_date) - interval '90 days'
                    from {{ ref('stg_%s__transactions' % tenant_name) }}
                )
            group by
                product_id,
                store_id
        ),

        past_90_days as (
            select
                product_id,
                store_id,
                transaction_date,
                sum(amount) as daily_amount,
                any_value(is_on_floor) as is_on_floor
            from {{ ref('stg_%s__transactions' % tenant_name) }}
            where transaction_date > (
                select max(transaction_date) - interval '90 days'
                from {{ ref('stg_%s__transactions' % tenant_name) }}
            )
            group by
                product_id,
                store_id,
                transaction_date
        ),

        on_floor_sales as (
            select
                past_90_days.product_id,
                past_90_days.store_id,
                sum(past_90_days.daily_amount) as on_floor_sales,
                (on_floor_sales / any_value(days_on_floor.value)) as avg_daily_amount
            from past_90_days
            left outer join days_on_floor
                on past_90_days.product_id = days_on_floor.product_id
                and past_90_days.store_id = days_on_floor.store_id
            where
                past_90_days.is_on_floor = true
                -- and days_on_floor.value >= 30
            group by
                past_90_days.product_id,
                past_90_days.store_id
        )

        select
            past_90_days.product_id,
            past_90_days.store_id,
            sum(past_90_days.daily_amount) as past_sales,
            any_value(on_floor_sales.avg_daily_amount) as avg_daily_on_floor_sales
        from past_90_days
        left outer join on_floor_sales
            on past_90_days.product_id = on_floor_sales.product_id
            and past_90_days.store_id = on_floor_sales.store_id
        group by
            past_90_days.product_id,
            past_90_days.store_id
    ),

    all_predictions as (
        select
            products.id as product_id,
            stores.id as store_id,
            predictions.predicted_sales
        from {{ ref('stg_%s__products' % tenant_name) }} as products
        inner join {{ ref('stg_%s__stores' % tenant_name) }} as stores
            on stores.is_active = true
        left outer join {{ ref('base_%s__predictions' % tenant_name) }} as predictions
            on
                predictions.product_id = products.id
                and predictions.store_id = stores.id
    ),

    predictions_with_floor_last_date as (
        with
        base as (
            select
                predictions.product_id,
                predictions.store_id,
                max(inventory.inventory_date) over (
                    partition by predictions.product_id, predictions.store_id
                    order by inventory.inventory_date desc
                ) as floor_last_date
            from all_predictions as predictions
            left outer join {{ ref('stg_%s__inventory' % tenant_name) }} as inventory
                on
                    predictions.product_id = inventory.product_id
                    and predictions.store_id = inventory.store_id
                    and inventory.is_on_floor
        )

        select
            product_id,
            store_id,
            floor_last_date
        from base
        group by
            product_id,
            store_id,
            floor_last_date
    ),

    recommendations as (
        select
            products.id as product_id,
            predictions.store_id as store_id,
            coalesce(inventory.is_on_floor, false) as is_on_floor,
            inventory.floor_start_date,
            predictions_with_floor_last_date.floor_last_date + interval '1 day' as floor_last_date,
            coalesce(transactions.past_sales, 0) as past_sales,
            coalesce(transactions.avg_daily_on_floor_sales, 0) as avg_daily_on_floor_sales,
            predictions.predicted_sales,
            products.is_available_in_app,
            array_construct_compact(
                case
                    when coalesce(inventory.is_clearance, false) then 'clearance'
                    else null
                end
            ) as flags,
            products.type,
            products.subgroup_id,
            products.group_id,
            products.subcategory_id
        from all_predictions as predictions
        inner join {{ ref('stg_%s__products' % tenant_name) }} as products
            on
                predictions.product_id = products.id
        left outer join predictions_with_floor_last_date
            on
                predictions.product_id = predictions_with_floor_last_date.product_id
                and predictions.store_id = predictions_with_floor_last_date.store_id
        left outer join inventory
            on predictions.product_id = inventory.product_id
            and predictions.store_id = inventory.store_id
        left outer join transactions
            on predictions.product_id = transactions.product_id
            and predictions.store_id = transactions.store_id
        where
            products.status = 'active'
            or (
                products.status = 'inactive'
                and inventory.is_on_floor = true
            )
            -- There are transactions for this recommendation
            or transactions.product_id is not null
    )

    select
        product_id,
        store_id,
        is_on_floor,
        case
            when is_on_floor then floor_start_date
            else floor_last_date
        end as floor_date,
        case
            when type = 'Kit' then null
            else predicted_sales
        end as predicted_sales,
        case
            when type = 'Kit' then null
            else past_sales
        end as past_sales,
        case
            when type = 'Kit' then null
            else avg_daily_on_floor_sales
        end as avg_daily_on_floor_sales,
        flags,
        is_available_in_app
    from recommendations
{% endmacro %}
