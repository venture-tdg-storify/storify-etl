{% macro dim_afi__subcategories(tenant_name) %}
    select
        id,
        name,
        category_id
    from {{ ref('stg_%s__subcategories' % tenant_name) }}
{% endmacro %}
