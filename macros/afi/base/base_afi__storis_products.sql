{% macro base_afi__storis_products(tenant_name) %}
    with
    ranked_by_id as (
        select
            product_id as id,
            trim(vendor_model_nbr, '"') as manufacturer_id,
            group_id as subcategory_id,
            trim(description, '"') as description,
            trim(description_2, '"') as description_2,
            status,
            purchase_status_code_id as purchase_status,
            case
                {% if tenant_name == 'tdg' %}
                    when _file = 'storis_products/storis-products-historical.csv'
                        then date('2024-10-10')
                {% elif tenant_name == 'dsg' %}
                    when _file = 'storis_products/dsg-storis-products-historical.csv'
                        then date('2024-10-10')
                {% endif %}
                else
                    date(
                        replace(
                            replace(_file, 'storis_products/storis_products_')
                            , '.csv'
                        ),
                        'YYYYMMDD'
                    )
            end as imported_date,
            row_number() over(
                partition by product_id
                order by imported_date desc
            ) as rank
        from {{ source(tenant_name, 'raw_storis_products') }}
        where
            manufacturer_id is not null
            and manufacturer_id <> ''
            {% if tenant_name == 'tdg' %}
                and vendor_id = 'AFHS'
            {% elif tenant_name == 'dsg' %}
                and vendor_id = 'AFI'
            {% endif %}
    ),

    effective_by_id as (
        select * exclude rank
        from ranked_by_id
        where rank = 1
    ),

    ranked_by_manufacturer_id as (
        select
            *,
            row_number() over(
                partition by manufacturer_id
                order by imported_date desc
            ) as rank
        from effective_by_id
    )

    select * exclude rank
    from ranked_by_manufacturer_id
    where rank = 1
{% endmacro %}
