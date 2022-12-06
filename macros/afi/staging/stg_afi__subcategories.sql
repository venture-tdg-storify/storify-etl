{% macro stg_afi__subcategories(tenant_name) %}
    select distinct
        subcategory_id as id,
        case
            when subcategory_id like '%-no-subcategory' then '<No Subcategory>'
            else subcategory_id
        end as name,
        category_id
    from {{ ref('base_%s__inventory' % tenant_name) }}
{% endmacro %}
