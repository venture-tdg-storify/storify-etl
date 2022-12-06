{{
    config(
        materialized = 'table',
    )
}}

with
ranked as (
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
        ) as imported_date,
        row_number() over(
            partition by sku
            order by imported_date desc, status
        ) as rank
    from {{ source('dsg', 'raw_products') }}
    where item_name is not null
)
select * exclude rank
from ranked
where rank = 1
