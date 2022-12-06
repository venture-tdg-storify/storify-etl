{% macro get_tenant_codebase_names(ignore_tenant_name = None) %}
    {% if target.name == 'demo' %}
        {% set tenant_names = ['dsg', 'dsg_demo'] %}
    {% else %}
        {% set tenant_names = ['tdg', 'dsg', 'ash'] %}
    {% endif %}

    {% if ignore_tenant_name != None and ignore_tenant_name in tenant_names %}
        {% do tenant_names.remove(ignore_tenant_name) %}
    {% endif %}

    {{ return(tenant_names) }}
{% endmacro %}
