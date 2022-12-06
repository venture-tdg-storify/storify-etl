{% macro generate_schema_name(custom_schema_name, node) %}

    {% set default_schema = target.schema %}

    {% if custom_schema_name is none %}
        {{ default_schema }}
    {% else %}
        {% if node.database.startswith('arteli_app_db_') and default_schema.startswith('dbt_cloud_') %}
            dbt_cloud_{{ custom_schema_name | trim }}
        {% else %}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {% endif %}
    {% endif %}

{% endmacro %}
