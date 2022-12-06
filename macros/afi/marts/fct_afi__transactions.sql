{% macro fct_afi__transactions(tenant_name) %}
    select
        transactions.order_id,
        transactions.product_id,
        transactions.store_id,
        transactions.group_id,
        products.category_id,
        {% if tenant_name == 'tdg' %}
            (transactions.amount / currency_exchange_rates.value)::number(38, 2) as amount,
            transactions.amount as amount_cad,
        {% else %}
            transactions.amount as amount,
        {% endif %}
        transactions.quantity,
        transactions.transaction_date,
        transactions.is_in_inventory,
        transactions.is_on_floor,
        transactions.on_floor_days_count
    from {{ ref('stg_%s__transactions' % tenant_name) }} as transactions
    left outer join {{ ref('stg_%s__products' % tenant_name) }} as products
        on transactions.product_id = products.id
    {% if tenant_name == 'tdg' %}
        left outer join {{ ref('currency_exchange_rates') }} as currency_exchange_rates
            on transactions.transaction_date = currency_exchange_rates.date
            and currency_exchange_rates.currency = 'CAD'
    {% endif %}
{% endmacro %}
