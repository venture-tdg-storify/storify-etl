{% macro base_afi__product_map(tenant_name) %}
    select
        product_id as alt_product_id,
        vendor_model_nbr as product_id,
        group_id as subcategory_id
    from {{ source(tenant_name, 'raw_product_map') }}
    where
        vendor_model_nbr != 'NULL'
        and vendor_id = 'AFHS'
        and product_id != 'NJATEST31' -- this is a test product
{% endmacro %}
