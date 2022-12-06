{{
    config(
        materialized = 'table',
    )
}}

with
sister_subgroups as (
    select
        category_id,
        group_id,
        subgroup_id
    from {{ ref('stg_tdg__sister_subgroups') }}
),

inventory_products as (
    with
    inventory as (
        select
            product_id,
            subcategory_id,
            row_number() over (
                partition by product_id
                order by inventory_date desc
            ) as rank
        from {{ ref('base_tdg__inventory') }}
    )

    select
        product_id,
        subcategory_id
    from inventory
    where rank = 1
),

on_floor_products as (
    select distinct product_id as id
    from {{ ref('base_tdg__inventory') }}
    where
        inventory_date = (
            select max(inventory_date)
            from {{ ref('base_tdg__inventory') }}
        )
        and is_on_floor
),

products as (
    select
        base_products.id,
        base_products.group_id,
        base_products.subgroup_id,
        case
            when on_floor_products.id is not null then 'active'
            else base_products.status
        end as status,
        base_products.type,
        base_products.name,
        base_products.class,
        base_products.code,
        base_products.is_exclusive,
        base_products.is_commodity,
        base_products.is_new,
        base_products.color,
        base_products.retail_type,
        base_products.image_url,
        subcategories.id as subcategory_id
    from {{ ref('base_tdg__products') }} as base_products
    left outer join {{ ref('base_tdg__storis_products') }} as storis_products
        on base_products.id = storis_products.manufacturer_id
    left outer join {{ ref('stg_tdg__subcategories') }} as subcategories
        on storis_products.subcategory_id = subcategories.id
    left outer join on_floor_products
        on base_products.id = on_floor_products.id
),

categorized_products as (
    select *
    from products
    where subcategory_id is not null
),

uncategorized_products as (
    select *
    from products
    where subcategory_id is null
),

item_and_component_products as (
    select
        uncategorized_products.* exclude subcategory_id,
        coalesce(
            inventory_products.subcategory_id,
            'no-category-no-subcategory'
        ) as subcategory_id
    from uncategorized_products
    left outer join inventory_products
        on uncategorized_products.id = inventory_products.product_id
    where type in ('Item', 'Component')
),

kit_products as (
    with
    expanded_kits as (
        select
            kits.product_id,
            kits.kit_product_id,
            inventory_products.subcategory_id
        from {{ ref('stg_tdg__kits') }} as kits
        left outer join inventory_products
            on kits.product_id = inventory_products.product_id
        where kits.kit_product_id in (
            select id from uncategorized_products
        )
    ),

    grouped_kits as (
        select
            kit_product_id,
            array_unique_agg(subcategory_id) as subcategory_ids
        from expanded_kits
        group by kit_product_id
    ),

    kits_with_subcategory_id as (
        select
            kit_product_id,
            case
                when array_size(subcategory_ids) = 1 then subcategory_ids[0]
                else 'no-category-no-subcategory'
            end as subcategory_id
        from grouped_kits
    )

    select
        uncategorized_products.* exclude subcategory_id,
        coalesce(
            kits_with_subcategory_id.subcategory_id,
            'no-category-no-subcategory'
        ) as subcategory_id
    from uncategorized_products
    left outer join kits_with_subcategory_id
        on uncategorized_products.id = kits_with_subcategory_id.kit_product_id
    where type = 'Kit'
),

union_products as (
    select * from categorized_products
    union all
    select * from item_and_component_products
    union all
    select * from kit_products
)

select
    union_products.id,
    subcategories.category_id,
    union_products.subcategory_id,
    coalesce(sister_subgroups.group_id, union_products.group_id) as group_id,
    union_products.subgroup_id,
    union_products.status,
    null as purchase_status,
    union_products.type,
    union_products.name,
    union_products.class,
    union_products.code,
    union_products.is_exclusive,
    union_products.is_commodity,
    union_products.is_new,
    union_products.color,
    union_products.retail_type,
    union_products.image_url,
    case
        when categories.is_available_in_app = false then false
        else true
    end as is_available_in_app
from union_products
left outer join {{ ref('stg_tdg__subcategories') }} as subcategories
    on union_products.subcategory_id = subcategories.id
left outer join sister_subgroups
    on union_products.subgroup_id = sister_subgroups.subgroup_id
    and subcategories.category_id = sister_subgroups.category_id
inner join {{ ref('stg_tdg__categories') }} as categories
    on categories.id = subcategories.category_id
