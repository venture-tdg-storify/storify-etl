create or replace task arteli_db_ingestion.tdg_ingestion.task_load_raw_inventory
    warehouse=compute_wh
    schedule='USING CRON 0 1 * * * UTC'
    as call load_raw_inventory();
alter task arteli_db_ingestion.tdg_ingestion.task_load_raw_inventory resume;
