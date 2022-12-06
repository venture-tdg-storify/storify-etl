create or replace procedure arteli_db_ingestion.dsg_ingestion.load_raw_predictions()
returns varchar(16777216)
language javascript
strict
execute as owner
as '
var sql_command = `insert into arteli_db_ingestion.dsg_ingestion.raw_predictions (
    store_id,
    product_id,
    predicted_sales
)
select
    $1 as store_id,
    $2 as product_id,
    $3 as predicted_sales
from @dsg_predictions_stage/predictions/obm_predictions_dsg.csv (file_format => s3_csv)
`;
try {
    snowflake.execute ({ sqlText: sql_command });
    return "succeeded.";
}
catch (err) {
    return "failed: " + err;
}
';
