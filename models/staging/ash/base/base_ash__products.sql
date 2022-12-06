{{
    config(
        materialized = 'table',
    )
}}

select
    trim(item_id) as id,
    trim(product_line) as category_id,
    trim(product_line) || ' - ' || trim(item_grouping) as subcategory_id,
    trim(item_grouping) as subcategory_name,
    trim(series_name) as group_id,
    trim(series_number) as subgroup_id,
    case
        when trim(manufacturing_status) in ('Current', 'New') then 'active'
        else 'inactive'
    end as status,
    trim(item_description) as name,
    case
        when trim(item_code) = 'NULL' then null
        else trim(item_code)
    end as code,
    case
        when trim(colors) = 'N/A' then null
        else trim(colors)
    end as color,
    trim(exclusive_flag) = 'Exclusive' as is_exclusive,
    trim(manufacturing_status) = 'New' as is_new
from {{ source('ash', 'raw_products') }}
where
    item_grouping <> 'N/A'
    and series_name <> 'N/A'
    and item_id is not null
