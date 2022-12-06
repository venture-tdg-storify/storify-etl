{% macro stg_afi__stores(tenant_name) %}
    with
    store_data as (
        select
            id,
            is_active,
            brand,
            total_sqft,
            floor_sqft,
            other_sqft,
            floor_perc,
            cost_per_sqft,
            total_occupancy_costs
        from {{ ref('seed_%s__store_data' % tenant_name) }}
    )

    select
        stores.id,
        stores.distribution_center_id,
        stores.store_type,
        stores.name,
        {% if tenant_name == 'tdg' %}
            'Canada' as address_country,
        {% else %}
            'United States' as address_country,
        {% endif %}
        null as address_state,
        stores.address_city,
        stores.address_street_1,
        null as address_street_2,
        null as address_postal_code,
        store_data.brand,
        store_data.total_sqft,
        store_data.floor_sqft,
        store_data.other_sqft,
        store_data.floor_perc::number(38, 2) as floor_perc,
        store_data.total_occupancy_costs,
        store_data.cost_per_sqft::number(38, 2) as cost_per_sqft,
        coalesce(store_data.is_active, false) as is_active
    from {{ ref('base_%s__stores' % tenant_name) }} as stores
    left outer join store_data
        on stores.id = store_data.id
{% endmacro %}
