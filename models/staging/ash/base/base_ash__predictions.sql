{{
    config(
        materialized = 'table',
    )
}}

select
    store_id,
    product_id,
    predicted_sales::number(38, 2) as predicted_sales
{% if target.name == 'prod' %}
    from {{ source('ash', 'raw_predictions_prod') }}
{% else %}
    from {{ source('ash', 'raw_predictions') }}
{% endif %}
