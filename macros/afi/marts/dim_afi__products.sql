{% macro dim_afi__products(tenant_name) %}
    select
        products.id,
        products.group_id,
        groups.name as group_name,
        products.subgroup_id,
        subgroups.name as subgroup_name,
        products.category_id,
        categories.name as category_name,
        products.subcategory_id,
        subcategories.name as subcategory_name,
        products.status,
        products.type,
        products.name,
        products.class,
        products.code,
        products.is_exclusive,
        products.is_commodity,
        products.is_new,
        products.color,
        products.retail_type,
        products.image_url
    from {{ ref('stg_%s__products' % tenant_name) }} as products
    left outer join {{ ref('stg_%s__groups' % tenant_name) }} as groups
        on products.group_id = groups.id
    left outer join {{ ref('stg_%s__subgroups' % tenant_name) }} as subgroups
        on products.subgroup_id = subgroups.id
    left outer join {{ ref('stg_%s__subcategories' % tenant_name) }} as subcategories
        on products.subcategory_id = subcategories.id
    left outer join {{ ref('stg_%s__categories' % tenant_name) }} as categories
        on subcategories.category_id = categories.id
{% endmacro %}
