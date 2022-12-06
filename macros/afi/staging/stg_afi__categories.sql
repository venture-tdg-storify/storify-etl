{% macro stg_afi__categories(tenant_name) %}
    with
    inventory_categories as (
        select distinct category_id as id,
        from {{ ref('base_%s__inventory' % tenant_name) }}
    ),

    category_data as (
        select
            id,
            name,
            description
        from {{ ref('seed_%s__category_data' % tenant_name) }}
    )

    select
        inventory_categories.id,
        coalesce(
            category_data.name,
            inventory_categories.id
        ) as name,
        category_data.description,
        inventory_categories.id != 'no-category' as is_available_in_app
    from inventory_categories
    left outer join category_data
        on inventory_categories.id = category_data.id
{% endmacro %}
