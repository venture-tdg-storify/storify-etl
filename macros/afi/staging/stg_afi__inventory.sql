{% macro stg_afi__inventory(tenant_name) %}
    with
    -- Grouped inventory
    grouped_inventory as (
        select
            product_id,
            store_id,
            boolor_agg(is_on_floor) as is_on_floor,
            sum(quantity) as quantity,
            array_to_string(array_unique_agg(reason_code), ',') as reason_codes,
            inventory_date
        from {{ ref('base_%s__inventory' % tenant_name) }}
        {% if is_incremental() %}
            where inventory_date > (
                select max(inventory_date) from {{ this }}
            )
        {% endif %}
        group by
            product_id,
            store_id,
            inventory_date
    ),

    -- Inventory with lag
    {% if is_incremental() %}
        inventory_with_lag as (
            with
            day_before as (
                select
                    product_id,
                    store_id,
                    is_on_floor,
                    quantity,
                    reason_codes,
                    inventory_date
                from {{ this }}
                where inventory_date = (select max(inventory_date) from {{ this }})
            ),

            grouped_with_day_before as (
                select *
                from grouped_inventory
                union all
                select *
                from day_before
            ),

            -- Have to calculate lag without where statement
            with_lag as (
                select
                    *,
                    lag(is_on_floor, 1, false) over (
                        partition by product_id, store_id
                        order by inventory_date
                    ) as lag_is_on_floor,
                    lag(inventory_date, 1, '2019-01-01'::date) over (
                        partition by product_id, store_id
                        order by inventory_date
                    ) as lag_inventory_date
                from grouped_with_day_before
            )

            select *
            from with_lag
            where inventory_date > (select max(inventory_date) from {{ this }})
        ),
    {% else %}
        inventory_with_lag as (
            select
                *,
                lag(is_on_floor, 1, false) over (
                    partition by product_id, store_id
                    order by inventory_date
                ) as lag_is_on_floor,
                lag(inventory_date, 1, '2019-01-01'::date) over (
                    partition by product_id, store_id
                    order by inventory_date
                ) as lag_inventory_date
            from grouped_inventory
        ),
    {% endif %}

    -- Inventory with floor start date
    inventory_with_floor_start_date as (
        select
            product_id,
            store_id,
            is_on_floor,
            quantity,
            reason_codes,
            inventory_date,
            case
                when
                    is_on_floor = true and
                    (
                        lag_is_on_floor = false or
                        inventory_date - lag_inventory_date > 1
                    )
                    then inventory_date
                else
                    null
            end as temp_floor_start_date
        from inventory_with_lag
    )

    -- Final
    {% if is_incremental() %}
        ,
        maximum as (
            select
                product_id,
                store_id,
                max(on_floor_days_count) as on_floor_days_count,
                max(temp_floor_start_date) as temp_floor_start_date
            from {{ this }}
            group by
                product_id,
                store_id
        )

        select
            product_id,
            store_id,
            is_on_floor,
            -- Count total days on floor (for ML)
            coalesce(maximum.on_floor_days_count, 0) +
                conditional_true_event(is_on_floor = true) over (
                    partition by product_id, store_id order by inventory_date
                ) as on_floor_days_count,
            quantity,
            reason_codes,
            inventory_date,
            -- Date when product was last put on floor
            -- If it is on the floor
            -- (for Webapp)
            case
                when is_on_floor = false
                    then null
                else
                    greatest_ignore_nulls(
                        max(temp_floor_start_date) over (
                            partition by product_id, store_id
                            order by inventory_date
                        ),
                        maximum.temp_floor_start_date
                    )
            end as floor_start_date,
            temp_floor_start_date -- For incremental
        from inventory_with_floor_start_date
        left outer join maximum
            using (product_id, store_id)
    {% else %}
        select
            product_id,
            store_id,
            is_on_floor,
            -- Count total days on floor (for ML)
            conditional_true_event(is_on_floor = true) over (
                partition by product_id, store_id order by inventory_date
            ) as on_floor_days_count,
            quantity,
            reason_codes,
            inventory_date,
            -- Date when product was last put on floor
            -- If it is on the floor
            -- (for Webapp)
            case
                when is_on_floor = false
                    then null
                else
                    max(temp_floor_start_date) over (
                        partition by product_id, store_id
                        order by inventory_date
                    )
            end as floor_start_date,
            temp_floor_start_date -- For incremental
        from inventory_with_floor_start_date
    {% endif %}
{% endmacro %}
