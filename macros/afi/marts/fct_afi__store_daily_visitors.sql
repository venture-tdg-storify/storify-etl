{% macro fct_afi__store_daily_visitors(tenant_name) %}
    select
        store_id,
        created_date,
        visitor_count
    from {{ ref('stg_%s__store_daily_visitors' % tenant_name) }}
{% endmacro %}
