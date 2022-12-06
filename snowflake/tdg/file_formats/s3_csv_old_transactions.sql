create file format arteli_db_ingestion.tdg_ingestion.s3_csv_old_transactions
    type = CSV
    skip_header = 1
    null_if = ('NULL', 'null', '\0')
    encoding = 'ISO88591';
