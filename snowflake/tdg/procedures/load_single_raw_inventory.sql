create or replace procedure arteli_db_ingestion.tdg_ingestion.load_single_raw_inventory()
returns varchar(16777216)
language javascript
strict
execute as owner
as '
var sql_command = `insert into arteli_db_ingestion.tdg_ingestion.raw_inventory (
    _file,
    _line,
    _modified,
    location_id,
    store,
    category_id,
    product_id,
    common_description,
    vendor_model_number,
    vendor_name,
    vendor_style,
    serial_number,
    reason_code,
    as_is_reason_code_id,
    inv_sub_bucket_id,
    product_name,
    product_status,
    price_point_segmentation,
    group_id,
    replacement_cost,
    cubic_feet,
    actual_inv_cost,
    qty_on_hand,
    trans_date
)

select
    metadata$filename as _file,
    metadata$file_row_number as _row_number,
    metadata$file_last_modified as last_modified_at,
    t.$1 as location_id,
    t.$2 as store,
    t.$3 as category_id,
    t.$4 as product_id,
    t.$5 as common_description,
    t.$6 as vendor_model_number,
    t.$7 as vendor_name,
    t.$8 as vendor_style,
    t.$9 as serial_number,
    t.$10 as reason_code,
    t.$11 as as_is_reason_code_id,
    t.$12 as inv_sub_bucket_id,
    t.$13 as product_name,
    t.$14 as product_status,
    t.$15 as price_point_segmentation,
    t.$16 as group_id,
    t.$17 as replacement_cost,
    t.$18 as cubic_feet,
    t.$19 as actual_inv_cost,
    t.$20 as qty_on_hand,
    t.$21 as trans_date
from @tdg_stage/inventory/Inventory_20240101 (file_format => s3_csv_inventory) t
`;

snowflake.execute ({sqlText: sql_command});
return "success.";
';
