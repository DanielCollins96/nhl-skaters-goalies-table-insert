## Applying SQL on live RDS

These scripts mix **one-time table bootstrap** with **safe-to-rerun** `CREATE OR REPLACE` functions/procedures.

- **(a) First-time / wipe only**: files under `bootstrap/`. They `DROP TABLE … CASCADE` and will erase production data. They refuse to run unless you opt in in the same session:

```sql
SET app.allow_bootstrap = 'on';
```

Never run `bootstrap/` against populated production. Bootstrap files set `\set ON_ERROR_STOP on` and keep the opt-in check and `DROP` in one transaction/`DO` block so a failed check cannot reach DROP.

- **(b) Live re-apply** (cast fixes, sync logic, `ALTER TABLE … ADD COLUMN IF NOT EXISTS`): run the matching `*_upsert.sql` / `player_contracts.sql` / `sync_season_from_club_stats.sql` file. Those files do **not** `DROP TABLE`. You can apply the whole file; you do not need to skip a DROP block.

Details and the table inventory are in `bootstrap/README.md`.

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

## season-from-club-stats (every ETL after club-stats)

This is a **normal ETL step**, not a one-off backfill. When club/team skater and goalie stats upsert into `newapi.skaters` / `newapi.goalies`, also upsert the same current-season NHL rows into `newapi.season_skater` / `newapi.season_goalie`. Player pages read season history; they stay current without scraping every landing page and without calling the NHL API at request time.

`sync_season_from_club_stats.sql` never `DROP`s production tables. It maps every club-stats field that already exists on `season_*` into `staging1.season_*` (`sequence = 1`, same `NULLIF+TRIM` / `::double precision::bigint` key normalize as the season upserts) and then calls `sync_season_*_from_staging()`. Historical landing rows in `newapi.season_*` stay; only keys present in club-stats are inserted or hash-updated.

Prerequisite: `season_skater_table_upsert.sql` and `season_goalie_table_upsert.sql` are already applied (so `sync_season_*_from_staging()` exist). `newapi.teams` must already have rows (true after the first full ETL).

### Install views and procedures

```bash
psql "$DATABASE_URL" -f sync_season_from_club_stats.sql
```

### Exact CALL order (`run_etl` / operators)

`NHL-ETL` `PIPELINE_ORDER` is `rosters`, `players`, `season_stats`, `teams`, …. Club-stats season upsert belongs in **`season_stats`**, after club-stats have landed in `newapi.skaters` / `newapi.goalies`. If `players` also ran, landing season sync must finish first (it already does).

```sql
-- players pipeline (landing; skip if that pipeline is off)
CALL sync_players_from_staging();
CALL sync_season_skaters_from_staging();
CALL sync_season_goalies_from_staging();

-- season_stats pipeline (club-stats, then season history)
CALL sync_skaters_from_staging();
CALL sync_goalies_from_staging();
CALL sync_season_skaters_from_club_stats();
CALL sync_season_goalies_from_club_stats();
```

Python hook for `run_etl.py` immediately after the existing club-stats CALLs:

```python
conn.execute(text("CALL sync_skaters_from_staging()"))
conn.execute(text("CALL sync_goalies_from_staging()"))
conn.execute(text("CALL sync_season_skaters_from_club_stats()"))
conn.execute(text("CALL sync_season_goalies_from_club_stats()"))
```

`CALL sync_season_*_from_club_stats()` truncates **staging only** (`staging1.season_skater` / `staging1.season_goalie`). It does **not** truncate `newapi.season_*`. Do not run it while a landing scrape is still writing those staging tables.

### Field map (club-stats → season_*)

Only columns that exist on both sides. Club-stats-only fields (`avgShiftsPerGame`, goalie `saves` / `points`) are not invented on `season_*`. Season-only fields club-stats does not send (`powerPlayPoints`, `shorthandedPoints`) stay NULL.

| Club-stats (`newapi.skaters`) | `season_skater` |
| --- | --- |
| `playerId`, `gameType`, `season` | `playerId`, `gameTypeId`, `season` (`sequence` = 1, `leagueAbbrev` = `NHL`) |
| `gamesPlayed`, `goals`, `assists`, `points` | same |
| `penaltyMinutes` | `pim` |
| `plusMinus`, `shots`, `shootingPctg` | same |
| `powerPlayGoals`, `shorthandedGoals`, `gameWinningGoals` | same |
| `overtimeGoals` | `otGoals` |
| `faceoffWinPctg` | `faceoffWinningPctg` |
| `avgTimeOnIcePerGame` (seconds per game) | `avgToi` |
| `triCode` → `teams.fullName` / `franchises.teamCommonName` | `teamName.default` / `teamCommonName.default` |

| Club-stats (`newapi.goalies`) | `season_goalie` |
| --- | --- |
| `playerId`, `gameType`, `season` | `playerId`, `gameTypeId`, `season` (`sequence` = 1, `leagueAbbrev` = `NHL`) |
| `gamesPlayed`, `gamesStarted`, `wins`, `losses`, `ties`, `shutouts` | same |
| `overtimeLosses` | `otLosses` |
| `goalsAgainst`, `goalsAgainstAverage`, `savePercentage`, `shotsAgainst` | `goalsAgainst`, `goalsAgainstAvg`, `savePctg`, `shotsAgainst` |
| `goals`, `assists`, `penaltyMinutes` | `goals`, `assists`, `pim` |
| `timeOnIce` (seconds) | `timeOnIce` (MM:SS text, landing style) |
| `team` → `teams.fullName` / `franchises.teamCommonName` | `teamName.default` / `teamCommonName.default` |

### Preview / verify after a run

```sql
SELECT * FROM newapi.season_skater_from_club_stats
WHERE season = 20262027
LIMIT 20;

SELECT * FROM newapi.season_skater_missing_from_club_stats
WHERE season = 20262027;

SELECT * FROM newapi.season_goalie_missing_from_club_stats
WHERE season = 20262027;

SELECT * FROM newapi.season_skater_etl_summary LIMIT 5;
SELECT * FROM newapi.season_goalie_etl_summary LIMIT 5;
```

After ETL, republish player read models (`readmodel_views.sql` / `readmodel_s3_export_views.sql`, or the usual S3 publish job) so `/api/players` picks up the new season rows and TOI. A Next.js deploy alone does not write these rows.

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

`player_contracts.sql` has no `DROP TABLE`; it uses `CREATE TABLE IF NOT EXISTS` for the typed contract tables, scrape status table, safe cast helpers for dirty staging values, and the sync procedure. Run the full file when the schema or helpers change:

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
