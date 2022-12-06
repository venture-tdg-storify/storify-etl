{% macro import_raw_predictions(tenant_name) %}
    {% set effective_target_name = "_" ~ target.name if target.name != 'stage' else '' %}

    {% set create_predictions_info_query %}
        create or replace table arteli_db_ingestion.{{ tenant_name }}_ingestion.raw_predictions_info{{ effective_target_name }} as
        select sysdate() as generated_at;
    {% endset %}

    {% if target.name == 'prod' %}
        {% set query %}
            begin;

            delete from {{ source(tenant_name, 'raw_predictions_prod') }};

            insert into {{ source(tenant_name, 'raw_predictions_prod') }}
            select * from {{ source(tenant_name, 'raw_predictions') }};

            {{ create_predictions_info_query }}

            commit;
        {% endset %}

        {% do run_query(query) %}
    {% elif target.name == 'stage' %}
        {% set query %}
            begin;

            delete from {{ source(tenant_name, 'raw_predictions') }};

            insert into {{ source(tenant_name, 'raw_predictions') }}
            select * from arteli_analytics_dev.test_schema."{{ tenant_name.upper() }}_predictions";

            {{ create_predictions_info_query }}

            commit;
        {% endset %}

        {% do run_query(query) %}
    {% elif target.name == 'dev' %}
        {% do run_query(create_predictions_info_query) %}
    {% else %}
        {{ exceptions.raise_compiler_error("Not supported target.name: " ~ target.name) }}
    {% endif %}
{% endmacro %}
