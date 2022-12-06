{{
    config(
        materialized = 'table',
    )
}}

select
    ai_action_stores.store_id,
    ai_action_stores.category_id,
    subcategories.id as subcategory_id,
    ai_action_stores.code as product_code,
    remove_group.id as remove_group_id,
    ai_action_stores.predicted_sales_remove::number(38, 2) as remove_predicted_sales,
    add_group.id as add_group_id,
    ai_action_stores.predicted_sales_add::number(38, 2) as add_predicted_sales
from {{ source('dsg', 'raw_ai_action_stores') }} ai_action_stores
inner join {{ ref('stg_dsg__stores') }} as stores
    on stores.id = ai_action_stores.store_id
inner join {{ ref('stg_dsg__categories') }} as categories
    on categories.id = ai_action_stores.category_id
left outer join {{ ref('stg_dsg__subcategories') }} as subcategories
    on subcategories.id = ai_action_stores.subcategory_id
inner join {{ ref('stg_dsg__groups') }} as remove_group
    on remove_group.name = ai_action_stores.group_name_remove
inner join {{ ref('stg_dsg__groups') }} as add_group
    on add_group.name = ai_action_stores.group_name_add
