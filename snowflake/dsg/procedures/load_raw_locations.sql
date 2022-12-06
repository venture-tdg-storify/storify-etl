create or replace procedure arteli_db_ingestion.dsg_ingestion.load_raw_locations()
returns varchar(16777216)
language javascript
strict
execute as owner
as
$$
var sql_command = `insert into arteli_db_ingestion.dsg_ingestion.raw_locations (
    _file,
    _line,
    _modified,
    location_id,
    stock_location_id,
    location_type,
    location_name,
    city,
    address_1
)

with base_files as(
    select distinct _file
    from arteli_db_ingestion.dsg_ingestion.raw_locations
)

select
    metadata$filename as _file,
    metadata$file_row_number as _line,
    metadata$file_last_modified as _modified,
    t.$1 as location_id,
    t.$2 as stock_location_id,
    t.$3 as location_type,
    t.$4 as location_name,
    t.$5 as city,
    t.$6 as address_1
from  @dsg_stage/locations (file_format => s3_csv) t
left join base_files
    on metadata$filename = base_files._file
where base_files._file is null
`;

try {
    snowflake.execute ({sqlText: sql_command});
    return "Success";
} catch (err) {
    var error_message = "Error: " + err;
    error_message = error_message.replaceAll("'", "");
    error_message = error_message.replaceAll('"', '');

    var email_command = "call arteli_db_ingestion.ops.enqueue_alert_stage('Task Failure - DSG load_raw_locations','" + error_message + "')";
    var stmt = snowflake.createStatement({ sqlText: email_command });
    stmt.execute();

    throw err;
}
$$;
