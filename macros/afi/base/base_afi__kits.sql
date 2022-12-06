{% macro base_afi__kits(tenant_name) %}
    select
        regexp_replace(product_id, '^AFHS-') as product_id,
        kit_product_id as alt_kit_product_id,
        qty as quantity
    from {{ source(tenant_name, 'raw_kits') }}
{% endmacro %}
