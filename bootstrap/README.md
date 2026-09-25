# Bootstrap (first-time / wipe only)

These scripts **DROP production tables**. They are not part of a live RDS re-apply.

## (a) First-time bootstrap

Only on an empty database, or when you intentionally want to wipe and recreate a table:

```sql
SET app.allow_bootstrap = 'on';
```

Then run the matching file in this folder, then the safe upsert file in the repo root so functions/procedures/views are installed.

Example:

```bash
psql "$DATABASE_URL" -c "SET app.allow_bootstrap = 'on';" -f bootstrap/season_goalie_table_bootstrap.sql
psql "$DATABASE_URL" -f season_goalie_table_upsert.sql
```

`SET` is session-scoped. If you run the bootstrap file in a new `psql` invocation without `-c "SET …"`, it **refuses** to drop anything. Each bootstrap file sets `\set ON_ERROR_STOP on` and keeps the opt-in check and `DROP TABLE` in one `DO` block inside one transaction, so a failed guard cannot reach DROP even if the caller omitted `-v ON_ERROR_STOP=1`.

`DROP … CASCADE` also drops views that depend on the table. After bootstrap, re-run the matching upsert file (it recreates the `newapi.*` views defined in that file). If the dropped table is used by the app read models, also re-run `readmodel_views.sql` and `readmodel_s3_export_views.sql`. Those files already recreate the views that read `newapi.players`, `newapi.season_skater`, `newapi.season_goalie`, `newapi.skaters`, `newapi.goalies`, and `newapi.rosters_active` (from `current_rosters`).

## (b) Live CREATE OR REPLACE re-apply

Run the upsert file only. Do **not** run anything in `bootstrap/`.

```bash
psql "$DATABASE_URL" -f season_goalie_table_upsert.sql
```

## Inventory

| Production table | Bootstrap (DROP + CREATE) | Safe default-run file |
| --- | --- | --- |
| `newapi.season_goalie` | `season_goalie_table_bootstrap.sql` | `season_goalie_table_upsert.sql` |
| `newapi.season_skater` | `season_skater_table_bootstrap.sql` | `season_skater_table_upsert.sql` |
| `newapi.players` | `players_table_bootstrap.sql` | `players_table_upsert.sql` |
| `newapi.skaters` | `skaters_table_bootstrap.sql` | `skaters_table_upsert.sql` |
| `newapi.goalies` | `goalies_table_bootstrap.sql` | `goalies_table_upsert.sql` |
| `newapi.current_rosters` | `rosters_table_bootstrap.sql` | `rosters_table_upsert.sql` |
| `newapi.gamecenter` (+ etl_log + dependent views) | `gamecenter_table_bootstrap.sql` | `gamecenter_table_upsert.sql` |

These upsert files already used `CREATE TABLE IF NOT EXISTS` and never had `DROP TABLE` against production data: `awards_table_upsert.sql`, `games_table_upsert.sql`, `teams_table_upsert.sql`, `draft_upsert.sql`, `player_contracts.sql`, `daily_game_rosters_upsert.sql`. `CREATE TABLE IF NOT EXISTS` for `*_etl_log` tables is safe and stays in the upsert files.

`DROP FUNCTION` / `DROP PROCEDURE` in upsert files only replace routines (needed when return types change). They do not drop production tables.
