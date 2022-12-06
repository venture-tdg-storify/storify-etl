{% macro fct_afi__store_monthly_sales(tenant_name) %}
    select
        store_id,
        year,
        month,
        total_amount,
        total_visitor_count,
        amount_per_visitor,
        amount_per_visitor_per_sqft
    from {{ ref('stg_%s__store_monthly_sales' % tenant_name) }}
{% endmacro %}
