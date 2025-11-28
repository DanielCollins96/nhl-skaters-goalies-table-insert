
## players-table-insert

### Run ETL and log results

```sql
CALL sync_players_from_staging();
-- or
SELECT * FROM insert_players_from_staging_with_logging();
```

### View ETL history

```sql
SELECT * FROM newapi.players_etl_summary;
```

### View current player info (latest occurrence)

```sql
SELECT * FROM newapi.players_current;
```

## skaters-table-insert

### Run ETL and log results

```sql
CALL sync_skaters_from_staging();
-- or
SELECT * FROM insert_skaters_from_staging_with_logging();
```

### View ETL history

```sql
SELECT * FROM newapi.skaters_etl_summary;
```
## goalies-table-insert

### Run ETL and log results

```sql
CALL sync_goalies_from_staging();
-- or
SELECT * FROM insert_goalies_from_staging_with_logging();
```

### View ETL history

```sql
SELECT * FROM newapi.goalies_etl_summary;
```

## active-rosters-table-insert

### Run ETL and log results

```sql
CALL sync_rosters_from_staging();
-- or
SELECT * FROM insert_rosters_from_staging_with_logging();
```

### View ETL history

```sql
SELECT * FROM newapi.rosters_etl_summary;
```

### View active roster players

```sql
SELECT * FROM newapi.rosters_active;
```

### View team roster summary

```sql
SELECT * FROM newapi.team_roster_summary;
```

### View players no longer on rosters

```sql
SELECT * FROM newapi.current_rosters WHERE active = FALSE;
```

## season-skaters-table-insert

### Run ETL and log results

```sql
CALL sync_season_skaters_from_staging();
-- or
SELECT * FROM insert_season_skaters_from_staging_with_logging();
```

### View ETL history

```sql
SELECT * FROM newapi.season_skater_etl_summary;
```

### View current season skater stats (only active records)

```sql
SELECT * FROM newapi.season_skater_current;
```

### View skaters with multiple occurrences (stat progression)

```sql
SELECT * FROM newapi.season_skater_multiple_stints;
```

### View occurrence statistics

```sql
SELECT * FROM get_season_skaters_occurrence_stats();
```

## season-goalies-table-insert

### Run ETL and log results

```sql
CALL sync_season_goalies_from_staging();
-- or
SELECT * FROM insert_season_goalies_from_staging_with_logging();
```

### View ETL history

```sql
SELECT * FROM newapi.season_goalie_etl_summary;
```

### View current season goalie stats (only active records)

```sql
SELECT * FROM newapi.season_goalie_current;
```

### View goalies with multiple occurrences (stat progression)

```sql
SELECT * FROM newapi.season_goalie_multiple_stints;
```

### View occurrence statistics

```sql
SELECT * FROM get_season_goalies_occurrence_stats();
```

## Background
This script uses my schema naming convention of `staging1.<players/skaters/goalies>` as the source table. I generated the Skaters/Goalies source tables using my nhlscraper (python package), which gave me dataframes I wrote to SQL.

Todo:

- Awards
- Standings
