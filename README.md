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

# rosters-table-insert
## Run ETL and log results:
```sql
CALL sync_rosters_from_staging();
-- or
SELECT * FROM insert_rosters_from_staging_with_logging();
```
## View ETL history:
```sql
SELECT * FROM newapi.rosters_etl_summary;
```
## View active roster players:
```sql
SELECT * FROM newapi.rosters_active;
```
## View team roster summary:
```sql
SELECT * FROM newapi.team_roster_summary;
```
## View players no longer on rosters:
```sql
SELECT * FROM newapi.current_rosters WHERE active = FALSE;
```

# Background
This script uses my schema naming convention of `staging1.<skaters/goalies>` as the source table. I generated the Skaters/Goalies source tables using my nhlscraper (python package), which gave me dataframes I wrote to SQL.