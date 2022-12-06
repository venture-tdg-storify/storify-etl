{% macro base_afi__predictions_info(tenant_name) %}
    select
        generated_at
    from
    {% if target.name in ['prod', 'demo'] %}
        {{ source(tenant_name, 'raw_predictions_info_prod') }}
    {% elif target.name in ['dev', 'stage'] %}
        {{ source(tenant_name, 'raw_predictions_info') }}
    {% else %}
        {{ exceptions.raise_compiler_error("Not supported target.name: " ~ target.name) }}
    {% endif %}
{% endmacro %}
