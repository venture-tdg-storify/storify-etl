{% macro create_alerts() %}
    {% if target.name in ['prod', 'stage', 'dev'] %}
        {{ create_enqueue_alert() }}

        {% for tenant_name in ['tdg', 'dsg'] %}
            {{ create_raw_inventory_alert(tenant_name) }}
            {{ create_raw_locations_alert(tenant_name) }}
            {{ create_raw_products_alert(tenant_name) }}
            {{ create_raw_trafic_alert(tenant_name) }}
            {{ create_raw_transactions_alert(tenant_name) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
