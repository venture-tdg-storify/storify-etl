{{
    config(
        materialized = 'table',
    )
}}

with
recursive all_dates as (
    select '2019-04-01'::date as date
    union all
    select dateadd('day', 1, date) as date
    from all_dates
    where date <= current_date()
),

all_raw_data as (
    select
        product_id,
        location_id as store_id,
        trans_date as inventory_date,
        case
            when category_id is null or category_id = '<No Value>'
                then 'no-category'
            else category_id
        end as category_id,
        case
            when group_id is null or group_id = '<No Value>'
                then case
                    when category_id is null or category_id = '<No Value>'
                        then 'no-category-no-subcategory'
                    else category_id || '-no-subcategory'
                end
            else group_id
        end as subcategory_id,
        case
            when as_is_reason_code_id in ('FLR', '\0') or as_is_reason_code_id is null
                then true
            else false
        end as is_on_floor,
        qty_on_hand as quantity,
        case
            when is_on_floor then 'FLR'
            else as_is_reason_code_id
        end as reason_code
    from {{ source('dsg', 'raw_inventory') }}
    union all
    select
        product_id,
        location_id as store_id,
        inventory_date,
        case
            when category_id is null or category_id = '<No Value>'
                then 'no-category'
            else category_id
        end as category_id,
        case
            when group_id is null or group_id = '<No Value>'
                then case
                    when category_id is null or category_id = '<No Value>'
                        then 'no-category-no-subcategory'
                    else category_id || '-no-subcategory'
                end
            else group_id
        end as subcategory_id,
        case
            when as_is_reason_code_id in ('FLR', '\0') or as_is_reason_code_id is null
                then true
            else false
        end as is_on_floor,
        qty_on_hand as quantity,
        case
            when is_on_floor then 'FLR'
            else as_is_reason_code_id
        end as reason_code
    from {{ source('dsg', 'raw_old_inventory') }}
),

dates_with_inventory as (
    select distinct inventory_date
    from all_raw_data
),

all_dates_with_pointers as (
    select
        all_dates.date,
        (
            select max(inventory_date)
            from dates_with_inventory
            where inventory_date <= all_dates.date
        ) as data_pointer_date
    from all_dates
    left outer join dates_with_inventory
        on all_dates.date = dates_with_inventory.inventory_date
    where
        all_dates.date >= (
            select min(inventory_date)
            from dates_with_inventory
        )
        and all_dates.date <= (
            select max(inventory_date)
            from dates_with_inventory
        )
)

select
    all_raw_data.product_id,
    all_raw_data.store_id,
    all_dates_with_pointers.date as inventory_date,
    all_raw_data.category_id,
    all_raw_data.subcategory_id,
    all_raw_data.is_on_floor,
    all_raw_data.quantity::integer as quantity,
    all_raw_data.reason_code
from all_dates_with_pointers
left outer join all_raw_data
    on all_dates_with_pointers.data_pointer_date = all_raw_data.inventory_date
where reason_code = 'FLR' or reason_code like 'CL%'
