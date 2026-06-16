
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

## gamecenter-table-insert

Load one raw play-by-play response per game into `staging1.gamecenter_raw`, using the game id from:

```text
https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play
```

Then run:

```sql
CALL sync_gamecenter_from_staging();
-- or
SELECT * FROM upsert_gamecenter_from_staging_with_logging();
```

The production table is `newapi.gamecenter`, one row per play/event. Scoring fields, assists, period time, play type, coordinates, descriptions, scores, shot details, penalties, and common player ids are flattened. The original play payload is still stored in `raw_play`, and `details` is indexed as JSONB for less common fields.

### Useful gamecenter views

```sql
SELECT * FROM newapi.gamecenter_goals;
SELECT * FROM newapi.gamecenter_player_points;
SELECT * FROM newapi.gamecenter_play_timeline;
SELECT * FROM newapi.gamecenter_etl_summary;
```

## player-contracts-table-insert

Load the PuckPedia scrape output into the contract staging tables, then run:

```sql
CALL sync_player_contracts_from_staging();
```

`player_contracts.sql` creates the typed contract tables, scrape status table, safe cast helpers for dirty staging values, and the sync procedure. Run the full file when the schema or helpers change:

```bash
psql "$DATABASE_URL" -f player_contracts.sql
```

## readmodel-views

After the ETL syncs have completed, refresh the app-facing read-model views:

```bash
psql "$DATABASE_URL" -f readmodel_views.sql
psql "$DATABASE_URL" -f readmodel_s3_export_views.sql
```

`readmodel_views.sql` creates the row-level views used by the Next.js API fallback queries. `readmodel_s3_export_views.sql` creates endpoint-shaped S3 payloads in `readmodel.s3_objects`, including player contracts and selected-season team contract payloads:

```text
contracts/players/{player_id}.json
contracts/teams/{team_id}/{season}.json
```

## Background
This script uses my schema naming convention of `staging1.<players/skaters/goalies>` as the source table. I generated the Skaters/Goalies source tables using my nhlscraper (python package), which gave me dataframes I wrote to SQL.

Todo:

- Awards
- Standings
