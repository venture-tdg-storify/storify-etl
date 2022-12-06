{% macro id_to_demo_id(id) %}

    round({{ id }}::int * 3 - 1, 0)::text

{% endmacro %}
