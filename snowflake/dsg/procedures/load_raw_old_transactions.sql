create or replace procedure arteli_db_ingestion.dsg_ingestion.load_raw_old_transactions()
returns varchar(16777216)
language javascript
strict
execute as owner
as '
    var sql_command = `insert into arteli_db_ingestion.dsg_ingestion.raw_old_transactions (
        _file,
        _line,
        _modified,
        location_id,
        order_id,
        written_date,
        category_id,
        group_id,
        series_id,
        vendor_style,
        product_id,
        product_name,
        reason_code,
        drop_down_used,
        qty,
        other_discount,
        retail_price,
        promo_price,
        sell_price,
        sell_vs_retail_disc,
        sell_vs_promo_disc,
        promo_price_degredation,
        gm_impact_by_disc,
        gm_sell_price,
        gm_retail_price,
        salesperson_name,
        price_override_staff_id,
        override_staff_name,
        price_exception_comment,
        kit_product_id,
        vendor_model_number
    )

    with
    new_files as (
        with
        loaded_files as(
            select distinct _file
            from arteli_db_ingestion.dsg_ingestion.raw_old_transactions
        )

        select
            metadata$filename as _file,
            metadata$file_row_number as _row_number,
            metadata$file_last_modified as last_modified_at,
            t.$1 as location_id,
            t.$2 as order_id,
            t.$3 as written_date,
            t.$4 as category_id,
            t.$5 as group_id,
            t.$6 as series_id,
            t.$7 as vendor_style,
            t.$8 as product_id,
            t.$9 as product_name,
            t.$10 as reason_code,
            t.$11 as drop_down_used,
            t.$12 as qty,
            t.$13 as other_discount,
            t.$14 as retail_price,
            t.$15 as promo_price,
            t.$16 as sell_price,
            t.$17 as sell_vs_retail_disc,
            t.$18 as sell_vs_promo_disc,
            t.$19 as promo_price_degredation,
            t.$20 as gm_impact_by_disc,
            t.$21 as gm_sell_price,
            t.$22 as gm_retail_price,
            t.$23 as salesperson_name,
            t.$24 as price_override_staff_id,
            t.$25 as override_staff_name,
            t.$26 as price_exception_comment,
            t.$27 as kit_product_id,
            t.$28 as vendor_model_number
        from @dsg_stage/old_transactions/ (file_format => s3_csv_old_transactions) t
        left join loaded_files
        on metadata$filename = loaded_files._file
        where loaded_files._file is null
    )

    select * from new_files
    `;

    try {
        snowflake.execute ({ sqlText: sql_command });
        return "Succeeded.";
    } catch (err) {
        return "Failed: " + err;   // Return a success/error indicator.
    }
';
