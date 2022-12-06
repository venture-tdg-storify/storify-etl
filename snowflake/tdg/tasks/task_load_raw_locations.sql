create or replace task arteli_db_ingestion.tdg_ingestion.task_load_raw_locations
    warehouse=compute_wh
    schedule='USING CRON 0 1 * * * UTC'
    as call load_raw_locations();
alter task arteli_db_ingestion.tdg_ingestion.task_load_raw_locations resume;
