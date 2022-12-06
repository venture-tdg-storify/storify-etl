{% macro stg_afi__store_daily_visitors(tenant_name) %}
    select
        store_id,
        created_date,
        visitor_count
    from {{ ref('base_%s__store_daily_visitors' % tenant_name) }}
{% endmacro %}
