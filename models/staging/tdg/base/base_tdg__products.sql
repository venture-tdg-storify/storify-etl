{{
    config(
        materialized = 'table',
    )
}}

with
raw_products as (
    select
        sku as id,
        item_series_name as group_id,
        regexp_replace(series_id, '^S-') as subgroup_id,
        case
            when status in ('New', 'Current')
                then 'active'
            else
                'inactive'
        end as status,
        item_type as type,
        item_name as name,
        item_class as class,
        item_code as code,
        coalesce(
            is_exclusive::boolean
            and item_exclusive_comment = 'HomeStore'
            and series_exclusive_comment = 'Homestore',
            false
        ) as is_exclusive,
        is_commodity::boolean as is_commodity,
        coalesce(status = 'New', false) as is_new,
        color,
        retail_type,
        medium_image_url as image_url,
        date(
            replace(
                replace(_file, 'products/Products_'),
                '.csv'
            ),
            'YYYYMMDD'
        ) as imported_date
    from {{ source('tdg', 'raw_products') }}
    where item_name is not null
),

non_tariff_series as (
    select series as group_id
    from {{ ref('seed_tdg__non_tariff_series') }}
),

tariff_products as (
    select
        regexp_replace(id, 'C$') as id,
        group_id,
        subgroup_id,
        status,
        type,
        name,
        class,
        code,
        is_exclusive,
        is_commodity,
        is_new,
        color,
        retail_type,
        image_url,
        imported_date
    from raw_products
    where
        id like '%C'
        and id not like '%CC'
        and group_id not in (select group_id from non_tariff_series)
),

non_tariff as (
    select
        id,
        group_id,
        subgroup_id,
        status,
        type,
        name,
        class,
        code,
        is_exclusive,
        is_commodity,
        is_new,
        color,
        retail_type,
        image_url,
        imported_date
    from raw_products
    where
        id like '%CC'
        or id not like '%C'
        or group_id in (select group_id from non_tariff_series)
),

all_products as (
    select * from tariff_products
    union all
    select * from non_tariff
),

ranked as (
    select
        *,
        row_number() over(
            partition by id
            order by imported_date desc, status
        ) as rank
    from all_products
)

select * exclude rank
from ranked
where rank = 1
