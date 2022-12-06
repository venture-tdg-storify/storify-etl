{% macro dim_afi__categories(tenant_name) %}
    select
        id,
        name,
        description
    from {{ ref('stg_%s__categories' % tenant_name) }}
{% endmacro %}
