-- One-time data fix for an existing database. Safe to re-run.
--
-- Run AFTER the updated files:
--   players_table_upsert.sql
--   rosters_table_upsert.sql
--   readmodel_views.sql
--   migrations/2026-11-26_players_rosters_one_row.sql   (this file)
-- Pause the ETL first; the VACUUM FULLs take exclusive locks.
--
-- newapi.players and newapi.current_rosters inserted a new version on every
-- change and never retired the old ones, so a player had several rows and
-- nothing said which was current. The upserts now update one row in place
-- (one per player; one per player/team for rosters). Keep the current row
-- for each key and delete the rest.
--
-- Current = the row the last ETL run touched. The old upserts compared
-- incoming data against every version and bumped updated_at on whichever
-- matched, so a player whose bio went back to an older version had that
-- older row as current, not the highest occurrence_number.

BEGIN;

-- ── players: newest-touched row per player ─────────────────────────────────
DELETE FROM newapi.players p
WHERE p.id NOT IN (
    SELECT DISTINCT ON ("playerId") id
    FROM newapi.players
    ORDER BY "playerId", updated_at DESC NULLS LAST, occurrence_number DESC
);

-- ── current_rosters: one row per player/team ───────────────────────────────
-- The active row if there is one, else the most recently touched.
DELETE FROM newapi.current_rosters r
WHERE r.id NOT IN (
    SELECT DISTINCT ON ("playerId", "teamAbbreviation") id
    FROM newapi.current_rosters
    ORDER BY "playerId", "teamAbbreviation",
             active DESC NULLS LAST, updated_at DESC NULLS LAST, occurrence_number DESC
);

-- Same columns as the UNIQUE keys, or a prefix of them.
DROP INDEX IF EXISTS newapi.idx_players_player_id;
DROP INDEX IF EXISTS newapi.idx_players_occurrence;
DROP INDEX IF EXISTS newapi.idx_rosters_player_id;
DROP INDEX IF EXISTS newapi.idx_rosters_occurrence;

COMMIT;

VACUUM (FULL, ANALYZE) newapi.players;
VACUUM (FULL, ANALYZE) newapi.current_rosters;
