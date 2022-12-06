{% macro create_enqueue_alert() %}
    create or replace procedure arteli_db_ingestion.ops.enqueue_alert_{{ target.name }}(subject string, body string)
        returns boolean
        language sql
        as
        $$
        begin
            call system$send_snowflake_notification(
                snowflake.notification.application_json(to_json(object_construct('subject', :subject, 'body', :body))),
                snowflake.notification.integration('azure_event_grid_integration_{{ target.name }}')
            );
            return true;
        end;
        $$;
{% endmacro %}
