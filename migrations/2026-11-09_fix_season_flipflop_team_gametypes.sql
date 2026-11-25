-- One-time data fix for an existing database. Safe to re-run.
--
-- Run AFTER the updated files, in this order:
--   season_skater_table_upsert.sql
--   season_goalie_table_upsert.sql
--   skaters_table_upsert.sql
--   goalies_table_upsert.sql
--   sync_season_from_club_stats.sql
--   teams_table_upsert.sql
--   migrations/2026-11-09_fix_season_flipflop_team_gametypes.sql   (this file)
-- Pause the ETL first; part 4 takes exclusive locks.
--
-- 1. team_gametypes: every ETL run inserted all ~1,791 rows again, because the
--    old key (season, "teamId") had a NULL teamId and NULLs never conflict.
--    Keep the newest row per (season, triCode) and key the table on that.
-- 2. season_skater / season_goalie: landing and club-stats kept replacing each
--    other's row (each leaves out fields the other has). Fill the gaps in each
--    active row from the old versions, then delete the old versions.
-- 3. skaters: club-stats is its only source, so the newest version is
--    complete; delete the old versions. goalies already updated in place;
--    any old versions it has go too.
-- 4. Drop indexes the upsert files no longer create (duplicates of the UNIQUE
--    keys, and is_active partial indexes that are redundant with one row per
--    key), drop a stray staging table, then VACUUM FULL to return the space.

BEGIN;

-- ── 1. team_gametypes ───────────────────────────────────────────────────────
DELETE FROM newapi.team_gametypes
WHERE id NOT IN (
    SELECT MAX(id) FROM newapi.team_gametypes GROUP BY season, "triCode"
);

ALTER TABLE newapi.team_gametypes DROP CONSTRAINT IF EXISTS "team_gametypes_season_teamId_key";
DROP INDEX IF EXISTS newapi.idx_team_gametypes_natural_key;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'newapi.team_gametypes'::regclass
          AND conname = 'team_gametypes_season_triCode_key'
    ) THEN
        ALTER TABLE newapi.team_gametypes
            ADD CONSTRAINT "team_gametypes_season_triCode_key" UNIQUE (season, "triCode");
    END IF;
END $$;

UPDATE newapi.team_gametypes tg
SET "teamId" = (
    SELECT t.id FROM newapi.teams t
    WHERE t."triCode" = tg."triCode"
    ORDER BY t.active DESC NULLS LAST, t.id DESC
    LIMIT 1
)
WHERE tg."teamId" IS NULL;

-- ── 2a. season_skater: fill gaps from history, then delete history ──────────
-- For each key, the newest non-NULL value of each stat among the old versions.
WITH h AS (
    SELECT "playerId", season, sequence, "teamName.default", "gameTypeId", "leagueAbbrev",
        (ARRAY_AGG("plusMinus"          ORDER BY occurrence_number DESC) FILTER (WHERE "plusMinus" IS NOT NULL))[1]          AS "plusMinus",
        (ARRAY_AGG("avgToi"             ORDER BY occurrence_number DESC) FILTER (WHERE "avgToi" IS NOT NULL))[1]             AS "avgToi",
        (ARRAY_AGG("powerPlayPoints"    ORDER BY occurrence_number DESC) FILTER (WHERE "powerPlayPoints" IS NOT NULL))[1]    AS "powerPlayPoints",
        (ARRAY_AGG("shorthandedPoints"  ORDER BY occurrence_number DESC) FILTER (WHERE "shorthandedPoints" IS NOT NULL))[1]  AS "shorthandedPoints",
        (ARRAY_AGG("otGoals"            ORDER BY occurrence_number DESC) FILTER (WHERE "otGoals" IS NOT NULL))[1]            AS "otGoals",
        (ARRAY_AGG("faceoffWinningPctg" ORDER BY occurrence_number DESC) FILTER (WHERE "faceoffWinningPctg" IS NOT NULL))[1] AS "faceoffWinningPctg",
        (ARRAY_AGG("shootingPctg"       ORDER BY occurrence_number DESC) FILTER (WHERE "shootingPctg" IS NOT NULL))[1]       AS "shootingPctg",
        (ARRAY_AGG(shots                ORDER BY occurrence_number DESC) FILTER (WHERE shots IS NOT NULL))[1]                AS shots
    FROM newapi.season_skater
    WHERE is_active = FALSE
    GROUP BY 1, 2, 3, 4, 5, 6
)
UPDATE newapi.season_skater a SET
    "plusMinus"          = COALESCE(a."plusMinus", h."plusMinus"),
    "avgToi"             = COALESCE(a."avgToi", h."avgToi"),
    "powerPlayPoints"    = COALESCE(a."powerPlayPoints", h."powerPlayPoints"),
    "shorthandedPoints"  = COALESCE(a."shorthandedPoints", h."shorthandedPoints"),
    "otGoals"            = COALESCE(a."otGoals", h."otGoals"),
    "faceoffWinningPctg" = COALESCE(a."faceoffWinningPctg", h."faceoffWinningPctg"),
    "shootingPctg"       = COALESCE(a."shootingPctg", h."shootingPctg"),
    shots                = COALESCE(a.shots, h.shots)
FROM h
WHERE a.is_active = TRUE
  AND a."playerId" = h."playerId" AND a.season = h.season AND a.sequence = h.sequence
  AND a."teamName.default" = h."teamName.default" AND a."gameTypeId" = h."gameTypeId"
  AND a."leagueAbbrev" = h."leagueAbbrev"
  AND (a."plusMinus" IS NULL OR a."avgToi" IS NULL OR a."powerPlayPoints" IS NULL
       OR a."shorthandedPoints" IS NULL OR a."otGoals" IS NULL OR a."faceoffWinningPctg" IS NULL
       OR a."shootingPctg" IS NULL OR a.shots IS NULL);

DELETE FROM newapi.season_skater WHERE is_active = FALSE;

-- ── 2b. season_goalie: same ─────────────────────────────────────────────────
WITH h AS (
    SELECT "playerId", season, sequence, "teamName.default", "gameTypeId", "leagueAbbrev",
        (ARRAY_AGG("timeOnIce"       ORDER BY occurrence_number DESC) FILTER (WHERE NULLIF("timeOnIce", '') IS NOT NULL))[1] AS "timeOnIce",
        (ARRAY_AGG("savePctg"        ORDER BY occurrence_number DESC) FILTER (WHERE "savePctg" IS NOT NULL))[1]        AS "savePctg",
        (ARRAY_AGG("goalsAgainstAvg" ORDER BY occurrence_number DESC) FILTER (WHERE "goalsAgainstAvg" IS NOT NULL))[1] AS "goalsAgainstAvg",
        (ARRAY_AGG("shotsAgainst"    ORDER BY occurrence_number DESC) FILTER (WHERE "shotsAgainst" IS NOT NULL))[1]    AS "shotsAgainst",
        (ARRAY_AGG("gamesStarted"    ORDER BY occurrence_number DESC) FILTER (WHERE "gamesStarted" IS NOT NULL))[1]    AS "gamesStarted",
        (ARRAY_AGG("otLosses"        ORDER BY occurrence_number DESC) FILTER (WHERE "otLosses" IS NOT NULL))[1]        AS "otLosses",
        (ARRAY_AGG(ties              ORDER BY occurrence_number DESC) FILTER (WHERE ties IS NOT NULL))[1]              AS ties
    FROM newapi.season_goalie
    WHERE is_active = FALSE
    GROUP BY 1, 2, 3, 4, 5, 6
)
UPDATE newapi.season_goalie a SET
    "timeOnIce"       = COALESCE(NULLIF(a."timeOnIce", ''), h."timeOnIce"),
    "savePctg"        = COALESCE(a."savePctg", h."savePctg"),
    "goalsAgainstAvg" = COALESCE(a."goalsAgainstAvg", h."goalsAgainstAvg"),
    "shotsAgainst"    = COALESCE(a."shotsAgainst", h."shotsAgainst"),
    "gamesStarted"    = COALESCE(a."gamesStarted", h."gamesStarted"),
    "otLosses"        = COALESCE(a."otLosses", h."otLosses"),
    ties              = COALESCE(a.ties, h.ties)
FROM h
WHERE a.is_active = TRUE
  AND a."playerId" = h."playerId" AND a.season = h.season AND a.sequence = h.sequence
  AND a."teamName.default" = h."teamName.default" AND a."gameTypeId" = h."gameTypeId"
  AND a."leagueAbbrev" = h."leagueAbbrev"
  AND (NULLIF(a."timeOnIce", '') IS NULL OR a."savePctg" IS NULL OR a."goalsAgainstAvg" IS NULL
       OR a."shotsAgainst" IS NULL OR a."gamesStarted" IS NULL OR a."otLosses" IS NULL OR a.ties IS NULL);

DELETE FROM newapi.season_goalie WHERE is_active = FALSE;

-- ── 3. skaters: delete history ──────────────────────────────────────────────
DELETE FROM newapi.skaters WHERE is_active = FALSE;
DELETE FROM newapi.goalies WHERE is_active = FALSE;

-- ── 4. indexes and stray tables ─────────────────────────────────────────────
-- Same columns as the UNIQUE key, or a prefix of it.
DROP INDEX IF EXISTS newapi.idx_season_skater_occurrence;
DROP INDEX IF EXISTS newapi.idx_season_goalie_occurrence;
DROP INDEX IF EXISTS newapi.idx_skaters_occurrence;
DROP INDEX IF EXISTS newapi.idx_goalies_occurrence;
DROP INDEX IF EXISTS newapi.idx_season_skater_player_id;
DROP INDEX IF EXISTS newapi.idx_season_goalie_player_id;
DROP INDEX IF EXISTS newapi.idx_skaters_player_id;
DROP INDEX IF EXISTS newapi.idx_goalies_player_id;
-- Never or barely used.
DROP INDEX IF EXISTS newapi.idx_skaters_team;
DROP INDEX IF EXISTS newapi.idx_skaters_season;
DROP INDEX IF EXISTS newapi.idx_goalies_team;
DROP INDEX IF EXISTS newapi.idx_goalies_season;
-- One row per key now, so the UNIQUE key serves the active-row lookup.
DROP INDEX IF EXISTS newapi.idx_season_skater_active;
DROP INDEX IF EXISTS newapi.idx_season_goalie_active;
DROP INDEX IF EXISTS newapi.idx_skaters_active;
DROP INDEX IF EXISTS newapi.idx_goalies_active;

-- Left over from an old scrape that wrote to "team.gametypes"; nothing reads it.
DROP TABLE IF EXISTS staging1."team.gametypes";

COMMIT;

-- VACUUM can't run inside a transaction. FULL rewrites the tables so the
-- deleted rows' space goes back to the OS.
VACUUM (FULL, ANALYZE) newapi.team_gametypes;
VACUUM (FULL, ANALYZE) newapi.season_skater;
VACUUM (FULL, ANALYZE) newapi.season_goalie;
VACUUM (FULL, ANALYZE) newapi.skaters;
VACUUM (FULL, ANALYZE) newapi.goalies;
