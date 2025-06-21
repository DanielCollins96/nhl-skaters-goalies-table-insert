# skaters-table-insert
## Run ETL and log results:
```sql
CALL sync_skaters_from_staging();
-- or
SELECT * FROM insert_skaters_from_staging_with_logging();
```
## View ETL history:
```sql
SELECT * FROM newapi.skaters_etl_summary;
```
