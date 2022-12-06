{% macro base_afi__predictions(tenant_name) %}
    with
    {% if tenant_name == 'tdg' %}
        todays_currency_exchange_rate as (
            select value
            from {{ ref('currency_exchange_rates') }}
            where currency = 'CAD'
            order by date
            desc limit 1
        ),
    {% endif %}

    ranked_predictions as (
        select
            lpad(store_id, 3, '0') as store_id,
            product_id,
            {% if tenant_name == 'tdg' %}
                (predicted_sales * todays_currency_exchange_rate.value)::number(38, 2) as predicted_sales,
            {% else %}
                predicted_sales::number(38, 2) as predicted_sales,
            {% endif %}
            row_number() over (
                partition by store_id, product_id
                order by predicted_sales desc
            ) as rank
        {% if target.name in ['prod', 'demo'] %}
            from {{ source(tenant_name, 'raw_predictions_prod') }}
        {% else %}
            from {{ source(tenant_name, 'raw_predictions') }}
        {% endif %}
        {% if tenant_name == 'tdg' %}
            left outer join todays_currency_exchange_rate
        {% endif %}
    )

    select * exclude rank
    from ranked_predictions
    where rank = 1
{% endmacro %}
