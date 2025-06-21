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
# goalies-table-insert
## Run ETL and log results:
```sql
CALL sync_goalies_from_staging();
-- or
SELECT * FROM insert_goalies_from_staging_with_logging();
```
## View ETL history:
```sql
SELECT * FROM newapi.goalies_etl_summary;
```

# Background
This script uses my schema naming convention of `staging1.<skaters/goalies>` as the source table. I generated the Skaters/Goalies source tables using my nhlscraper (python package), which gave me dataframes I wrote to SQL.