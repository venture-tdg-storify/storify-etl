{% macro obfuscate_sales(field) %}

    round({{ field }} * (0.5 + uniform(0::number(38,2), 1::number(38,2), random())), 2)

{% endmacro %}
