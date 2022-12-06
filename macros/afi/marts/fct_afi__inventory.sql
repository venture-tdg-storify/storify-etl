{% macro fct_afi__inventory(tenant_name) %}
    select
        product_id,
        store_id,
        is_on_floor,
        on_floor_days_count,
        quantity,
        reason_codes,
        inventory_date
    from {{ ref('stg_%s__inventory' % tenant_name) }}
{% endmacro %}
