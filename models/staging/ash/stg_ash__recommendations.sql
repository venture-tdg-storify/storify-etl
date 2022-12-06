{{
    config(
        materialized = 'table',
    )
}}

with
inventory as (
    select
        product_id,
        store_id,
        true as is_on_floor,
        floor_start_date,
        false as is_clearance
    from {{ ref('stg_ash__inventory') }}
    where inventory_date = (
        select max(inventory_date)
        from {{ ref('stg_ash__inventory') }}
    )
),

transactions as (
    with
    days_on_floor as (
        select
            product_id,
            store_id,
            count(*) as value
        from {{ ref('stg_ash__inventory') }}
        where
            inventory_date > (
                select max(transaction_date) - interval ' 90 days'
                from {{ ref('stg_ash__transactions') }}
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
        from {{ ref('stg_ash__transactions') }}
        where transaction_date > (
            select max(transaction_date) - interval ' 90 days'
            from {{ ref('stg_ash__transactions') }}
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
            on days_on_floor.product_id = past_90_days.product_id
            and days_on_floor.store_id = past_90_days.store_id
        where
            past_90_days.is_on_floor
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
        on on_floor_sales.product_id = past_90_days.product_id
        and on_floor_sales.store_id = past_90_days.store_id
    group by
        past_90_days.product_id,
        past_90_days.store_id
),

all_predictions as (
    select
        products.id as product_id,
        stores.id as store_id,
        predictions.predicted_sales
    from {{ ref('stg_ash__products') }} as products
    inner join {{ ref('stg_ash__stores') }} as stores
        on stores.is_active
    left outer join {{ ref('base_ash__predictions') }} as predictions
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
        left outer join {{ ref('stg_ash__inventory') }} as inventory
            on
                inventory.product_id = predictions.product_id
                and inventory.store_id = predictions.store_id
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
        predictions.product_id,
        predictions.store_id,
        coalesce(inventory.is_on_floor, false) as is_on_floor,
        inventory.floor_start_date,
        predictions_with_floor_last_date.floor_last_date + interval '1 day' as floor_last_date,
        coalesce(transactions.past_sales, 0) as past_sales,
        coalesce(transactions.avg_daily_on_floor_sales, 0) as avg_daily_on_floor_sales,
        predictions.predicted_sales,
        true as is_available_in_app,
        array_construct_compact(
            case
                when coalesce(inventory.is_clearance, false) then 'clearance'
                else null
            end
        ) as flags
    from all_predictions as predictions
    inner join {{ ref('stg_ash__products') }} as products
        on products.id = predictions.product_id
    left outer join predictions_with_floor_last_date
        on
            predictions_with_floor_last_date.product_id = predictions.product_id
            and predictions_with_floor_last_date.store_id = predictions.store_id
    left outer join inventory
        on
            inventory.product_id = predictions.product_id
            and inventory.store_id = predictions.store_id
    left outer join transactions
        on
            transactions.product_id = predictions.product_id
            and transactions.store_id = predictions.store_id
    where
        products.status = 'active'
        or (
            products.status = 'inactive'
            and inventory.is_on_floor
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
    past_sales,
    avg_daily_on_floor_sales,
    predicted_sales,
    flags,
    is_available_in_app
from recommendations
