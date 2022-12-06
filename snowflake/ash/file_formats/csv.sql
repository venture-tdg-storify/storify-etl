create file format arteli_db_ingestion.ash_ingestion.csv
    type = CSV
    null_if = ('NULL', 'null', '\0', 'N/A')
    parse_header = true
