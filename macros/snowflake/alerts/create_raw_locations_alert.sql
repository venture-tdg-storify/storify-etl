{% macro create_raw_locations_alert(tenant_name) %}
    {% set alert_name = 'arteli_db_ingestion.%s_ingestion.raw_locations_alert_%s' % (tenant_name, target.name) %}

    create or replace alert {{ alert_name }}
        warehouse = compute_wh
        schedule = 'USING CRON 30 6 * * * UTC'
        if (
            exists (
                select
                    case
                        when (
                            select (
                                select count(*)
                                from {{ source(tenant_name, 'raw_locations') }}
                                where date(replace(replace(_file, 'locations/Locations_'), '.csv'), 'YYYYMMDD') > current_date() - interval '3 days'
                            ) = 0
                        )
                            then 1
                        else
                            null
                    end
            )
        )
        then call arteli_db_ingestion.ops.enqueue_alert_{{ target.name }}(
            '{{ tenant_name.upper() }} Raw Locations Alert',
            'Last ingested date: ' || (
                select max(date(replace(replace(_file, 'locations/Locations_'), '.csv'), 'YYYYMMDD'))
                from {{ source(tenant_name, 'raw_locations') }}
            )
        );

    alter alert {{ alert_name }} resume;
{% endmacro %}
