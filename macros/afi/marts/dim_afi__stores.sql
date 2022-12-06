{% macro dim_afi__stores(tenant_name) %}
    select
        id,
        distribution_center_id,
        store_type,
        name,
        address_country,
        address_state,
        address_city,
        address_street_1,
        address_street_2,
        address_postal_code,
        brand,
        total_sqft,
        floor_sqft,
        other_sqft,
        floor_perc,
        total_occupancy_costs,
        cost_per_sqft,
        is_active
    from {{ ref('stg_%s__stores' % tenant_name) }}
{% endmacro %}
