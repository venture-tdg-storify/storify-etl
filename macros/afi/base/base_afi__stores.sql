{% macro base_afi__stores(tenant_name) %}
    with
    ranked_locations as (
        select
            location_id as id,
            stock_location_id as distribution_center_id,
            location_type as store_type,
            location_name as name,
            city as address_city,
            address_1 as address_street_1,
            date(
                replace(
                    replace(_file, 'locations/Locations_'),
                    '.csv'
                ),
                'YYYYMMDD'
            ) as imported_date,
            -- We want only the latest version of the store
            row_number() over (
                partition by id
                order by imported_date desc
            ) as rank
        from {{ source(tenant_name, 'raw_locations') }}
    )

    select * exclude rank
    from ranked_locations
    where rank = 1
{% endmacro %}
