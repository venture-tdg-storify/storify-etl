{% macro stg_afi__kits(tenant_name) %}
    with
    products as (
        select distinct id
        from {{ ref('base_%s__products' % tenant_name) }}
    )

    select
        kits.product_id,
        product_map.product_id as kit_product_id,
        kits.quantity
    from {{ ref('base_%s__kits' % tenant_name) }} as kits
    inner join {{ ref('base_%s__product_map' % tenant_name) }} as product_map
        on kits.alt_kit_product_id = product_map.alt_product_id
    where
        kits.product_id in (select id from products)
        and product_map.product_id in (select id from products)
{% endmacro %}
