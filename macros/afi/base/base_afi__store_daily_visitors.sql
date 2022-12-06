{% macro base_afi__store_daily_visitors(tenant_name) %}
    select
        location_id as store_id,
        trans_date as created_date,
        sum(traffic)::number(38,2) as visitor_count
    from {{ source(tenant_name, 'raw_traffic') }}
    group by
        store_id,
        created_date
{% endmacro %}
