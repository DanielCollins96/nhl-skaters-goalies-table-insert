-- SAFE TO RE-RUN against populated production (live RDS).
-- Additive: DROP/CREATE views and procedures only. No DROP TABLE.
-- Depends on sync_season_skaters_from_staging() /
-- sync_season_goalies_from_staging() from season_*_table_upsert.sql.
--
-- Normal ETL step (not a one-off backfill). After club-stats upsert
-- into newapi.skaters / newapi.goalies, map those rows into
-- staging1.season_* and CALL the existing season hash upserts so
-- player pages stay current without scraping every landing page.
--
-- Install (does not write season rows):
--   psql "$DATABASE_URL" -f sync_season_from_club_stats.sql
--
-- run_etl / operator CALL order — season_stats pipeline:
--   CALL sync_skaters_from_staging();
--   CALL sync_goalies_from_staging();
--   CALL sync_season_skaters_from_club_stats();
--   CALL sync_season_goalies_from_club_stats();
--
-- If the players/landing pipeline also ran in the same job, it must
-- finish first (PIPELINE_ORDER already has players before season_stats):
--   CALL sync_players_from_staging();
--   CALL sync_season_skaters_from_staging();
--   CALL sync_season_goalies_from_staging();
-- then the four season_stats CALLs above. Club-stats is the last
-- writer of current-season NHL season_* rows.
--
-- Keys use the same NULLIF+TRIM then ::double precision::bigint
-- normalize as season_*_table_upsert.sql. Staging reuses the active
-- season_* sequence + teamName when one already exists for that
-- player/season/gameType/team so sync_season_*_from_staging hash-
-- updates GP/G/A/P/TOI instead of inserting a parallel sequence=1
-- row and leaving the landing row stale.
--
-- Stale existing rows (hash upsert should overwrite these on CALL):
--   SELECT * FROM newapi.season_skater_stale_from_club_stats
--   WHERE season = 20262027;
--
-- Field map (only season_* columns that club-stats provides):
--   skaters.playerId/gameType/season     → playerId/gameTypeId/season
--   skaters.gamesPlayed/goals/assists/points → same
--   skaters.penaltyMinutes               → pim
--   skaters.plusMinus/shots/shootingPctg → plusMinus/shots/shootingPctg
--   skaters.powerPlayGoals/shorthandedGoals/gameWinningGoals
--                                        → same names
--   skaters.overtimeGoals                → otGoals
--   skaters.faceoffWinPctg               → faceoffWinningPctg
--   skaters.avgTimeOnIcePerGame          → avgToi
--       (NHL club-stats seconds per game; Next.js parseToiSeconds
--        treats a numeric avgToi as seconds)
--   triCode → teams.fullName             → teamName.default
--   triCode → franchises.teamCommonName  → teamCommonName.default
--   not on club-stats                    → powerPlayPoints, shorthandedPoints
--   club-stats only (no season_* col)    → avgShiftsPerGame, names, headshot
--
--   goalies.playerId/gameType/season     → playerId/gameTypeId/season
--   goalies.gamesPlayed/gamesStarted     → same
--   goalies.wins/losses/ties/shutouts    → same
--   goalies.overtimeLosses               → otLosses
--   goalies.goalsAgainst                 → goalsAgainst
--   goalies.goalsAgainstAverage          → goalsAgainstAvg
--   goalies.savePercentage               → savePctg
--   goalies.shotsAgainst/goals/assists   → same / pim from penaltyMinutes
--   goalies.timeOnIce                    → timeOnIce (seconds → MM:SS text)
--   team → teams.fullName                → teamName.default
--   club-stats only (no season_* col)    → saves, points, names, headshot

CREATE SCHEMA IF NOT EXISTS newapi;
CREATE SCHEMA IF NOT EXISTS staging1;

DROP PROCEDURE IF EXISTS sync_season_skaters_from_club_stats() CASCADE;
DROP PROCEDURE IF EXISTS sync_season_goalies_from_club_stats() CASCADE;
DROP PROCEDURE IF EXISTS load_season_skater_staging_from_club_stats() CASCADE;
DROP PROCEDURE IF EXISTS load_season_goalie_staging_from_club_stats() CASCADE;

DROP VIEW IF EXISTS newapi.season_skater_stale_from_club_stats CASCADE;
DROP VIEW IF EXISTS newapi.season_goalie_stale_from_club_stats CASCADE;
DROP VIEW IF EXISTS newapi.season_skater_missing_from_club_stats CASCADE;
DROP VIEW IF EXISTS newapi.season_goalie_missing_from_club_stats CASCADE;
DROP VIEW IF EXISTS newapi.season_skater_from_club_stats CASCADE;
DROP VIEW IF EXISTS newapi.season_goalie_from_club_stats CASCADE;
DROP VIEW IF EXISTS newapi.team_fullname_by_tricode CASCADE;
DROP FUNCTION IF EXISTS newapi.club_stats_bigint(text) CASCADE;
DROP FUNCTION IF EXISTS newapi.club_stats_float(text) CASCADE;

-- Dirty club-stats / pandas text must not abort the staging INSERT.
CREATE OR REPLACE FUNCTION newapi.club_stats_bigint(p_value text)
RETURNS bigint
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN NULLIF(TRIM(p_value), '') ~ '^\d+(\.\d+)?$'
        THEN TRIM(p_value)::double precision::bigint
    END;
$$;

CREATE OR REPLACE FUNCTION newapi.club_stats_float(p_value text)
RETURNS double precision
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN NULLIF(TRIM(p_value), '') ~ '^\d+(\.\d+)?$'
        THEN TRIM(p_value)::double precision
        WHEN NULLIF(TRIM(p_value), '') ~ '^\d+:[0-5]?\d(\.\d+)?$'
        THEN SPLIT_PART(TRIM(p_value), ':', 1)::double precision * 60
           + SPLIT_PART(TRIM(p_value), ':', 2)::double precision
    END;
$$;

-- Active team names by club-stats tricode (rawTricode or triCode).
CREATE OR REPLACE VIEW newapi.team_fullname_by_tricode AS
SELECT DISTINCT ON (abbrev)
    abbrev AS tricode,
    t."fullName",
    f."teamCommonName"
FROM (
    SELECT UPPER(TRIM(t."rawTricode")) AS abbrev, t."fullName", t."franchiseId", t.active
    FROM newapi.teams t
    UNION ALL
    SELECT UPPER(TRIM(t."triCode")) AS abbrev, t."fullName", t."franchiseId", t.active
    FROM newapi.teams t
) t
LEFT JOIN newapi.franchises f ON f.id = t."franchiseId"
WHERE NULLIF(abbrev, '') IS NOT NULL
  AND NULLIF(TRIM(t."fullName"), '') IS NOT NULL
ORDER BY abbrev, t.active DESC NULLS LAST;

-- Club-stats (newapi.skaters) → season_skater staging shape.
-- Reuse the live season_* key (sequence + teamName) so
-- sync_season_skaters_from_staging() hash-updates that row.
CREATE OR REPLACE VIEW newapi.season_skater_from_club_stats AS
WITH club AS (
    SELECT
        newapi.club_stats_bigint(s."playerId"::text) AS "playerId",
        newapi.club_stats_float(s.assists::text) AS assists,
        newapi.club_stats_bigint(s."gameType"::text) AS "gameTypeId",
        newapi.club_stats_float(s."gamesPlayed"::text) AS "gamesPlayed",
        newapi.club_stats_float(s.goals::text) AS goals,
        'NHL'::text AS "leagueAbbrev",
        newapi.club_stats_float(s."penaltyMinutes"::text) AS pim,
        newapi.club_stats_float(s."plusMinus"::text) AS "plusMinus",
        newapi.club_stats_float(s.points::text) AS points,
        newapi.club_stats_bigint(s.season::text) AS season,
        newapi.club_stats_float(s."faceoffWinPctg"::text) AS "faceoffWinningPctg",
        newapi.club_stats_float(s."shootingPctg"::text) AS "shootingPctg",
        newapi.club_stats_float(s.shots::text) AS shots,
        newapi.club_stats_float(s."powerPlayGoals"::text) AS "powerPlayGoals",
        newapi.club_stats_float(s."shorthandedGoals"::text) AS "shorthandedGoals",
        newapi.club_stats_float(s."gameWinningGoals"::text) AS "gameWinningGoals",
        newapi.club_stats_float(s."avgTimeOnIcePerGame"::text) AS "avgToi",
        newapi.club_stats_float(s."overtimeGoals"::text) AS "otGoals",
        UPPER(TRIM(s."triCode")) AS tricode
    FROM newapi.skaters s
    WHERE s.is_active = TRUE
      AND newapi.club_stats_bigint(s."playerId"::text) IS NOT NULL
      AND newapi.club_stats_bigint(s."gameType"::text) IS NOT NULL
      AND newapi.club_stats_bigint(s.season::text) IS NOT NULL
      AND NULLIF(TRIM(s."triCode"), '') IS NOT NULL
)
SELECT
    c."playerId",
    c.assists,
    c."gameTypeId",
    c."gamesPlayed",
    c.goals,
    c."leagueAbbrev",
    c.pim,
    c."plusMinus",
    c.points,
    c.season,
    COALESCE(existing.sequence, 1::bigint) AS sequence,
    COALESCE(existing."teamName.default", t."fullName") AS "teamName.default",
    COALESCE(existing."teamCommonName.default", t."teamCommonName") AS "teamCommonName.default",
    c."faceoffWinningPctg",
    c."shootingPctg",
    c.shots,
    c."powerPlayGoals",
    c."shorthandedGoals",
    c."gameWinningGoals",
    c."avgToi",
    c."otGoals",
    NULL::double precision AS "powerPlayPoints",
    NULL::double precision AS "shorthandedPoints"
FROM club c
LEFT JOIN newapi.team_fullname_by_tricode t ON t.tricode = c.tricode
LEFT JOIN LATERAL (
    SELECT
        ss.sequence,
        ss."teamName.default",
        ss."teamCommonName.default"
    FROM newapi.season_skater ss
    LEFT JOIN newapi.teams et ON et."fullName" = ss."teamName.default"
    WHERE ss.is_active = TRUE
      AND ss."playerId" = c."playerId"
      AND ss.season = c.season
      AND ss."gameTypeId" = c."gameTypeId"
      AND ss."leagueAbbrev" = 'NHL'
      AND (
          ss."teamName.default" = t."fullName"
          OR ss."teamName.default" = t."teamCommonName"
          OR UPPER(TRIM(et."rawTricode")) = c.tricode
          OR UPPER(TRIM(et."triCode")) = c.tricode
          OR NOT EXISTS (
              SELECT 1
              FROM newapi.season_skater other
              WHERE other.is_active = TRUE
                AND other."playerId" = c."playerId"
                AND other.season = c.season
                AND other."gameTypeId" = c."gameTypeId"
                AND other."leagueAbbrev" = 'NHL'
                AND other."teamName.default" IS DISTINCT FROM ss."teamName.default"
          )
      )
    ORDER BY
        CASE
            WHEN ss."teamName.default" = t."fullName" THEN 0
            WHEN UPPER(TRIM(COALESCE(et."rawTricode", et."triCode"))) = c.tricode THEN 1
            ELSE 2
        END,
        ss.sequence
    LIMIT 1
) existing ON TRUE
WHERE COALESCE(existing."teamName.default", t."fullName") IS NOT NULL;

CREATE OR REPLACE VIEW newapi.season_skater_missing_from_club_stats AS
SELECT
    c."playerId",
    c.season,
    c.sequence,
    c."gameTypeId",
    c."teamName.default",
    c."gamesPlayed",
    c.goals,
    c.assists,
    c.points,
    c.pim,
    c."plusMinus",
    c.shots,
    c."powerPlayGoals",
    c."avgToi"
FROM newapi.season_skater_from_club_stats c
WHERE NOT EXISTS (
    SELECT 1
    FROM newapi.season_skater ss
    WHERE ss.is_active = TRUE
      AND ss."playerId" = c."playerId"
      AND ss.season = c.season
      AND ss.sequence = c.sequence
      AND ss."gameTypeId" = c."gameTypeId"
      AND ss."leagueAbbrev" = 'NHL'
      AND ss."teamName.default" = c."teamName.default"
);

CREATE OR REPLACE VIEW newapi.season_skater_stale_from_club_stats AS
SELECT
    c."playerId",
    c.season,
    c.sequence,
    c."gameTypeId",
    c."teamName.default",
    ss."gamesPlayed" AS season_games,
    c."gamesPlayed" AS club_games,
    ss.goals AS season_goals,
    c.goals AS club_goals,
    ss.assists AS season_assists,
    c.assists AS club_assists,
    ss.points AS season_points,
    c.points AS club_points,
    ss."avgToi" AS season_avg_toi,
    c."avgToi" AS club_avg_toi
FROM newapi.season_skater_from_club_stats c
JOIN newapi.season_skater ss
    ON ss.is_active = TRUE
   AND ss."playerId" = c."playerId"
   AND ss.season = c.season
   AND ss.sequence = c.sequence
   AND ss."gameTypeId" = c."gameTypeId"
   AND ss."leagueAbbrev" = 'NHL'
   AND ss."teamName.default" = c."teamName.default"
WHERE ss."gamesPlayed" IS DISTINCT FROM c."gamesPlayed"::double precision::bigint
   OR ss.goals IS DISTINCT FROM c.goals::double precision::bigint
   OR ss.assists IS DISTINCT FROM c.assists::double precision::bigint
   OR ss.points IS DISTINCT FROM c.points::double precision::bigint
   OR ss."avgToi" IS DISTINCT FROM c."avgToi";

CREATE OR REPLACE PROCEDURE load_season_skater_staging_from_club_stats()
LANGUAGE plpgsql AS $$
BEGIN
    CREATE SCHEMA IF NOT EXISTS staging1;

    CREATE TABLE IF NOT EXISTS staging1.season_skater (
        "playerId" BIGINT,
        assists DOUBLE PRECISION,
        "gameTypeId" BIGINT,
        "gamesPlayed" DOUBLE PRECISION,
        goals DOUBLE PRECISION,
        "leagueAbbrev" TEXT,
        pim DOUBLE PRECISION,
        "plusMinus" DOUBLE PRECISION,
        points DOUBLE PRECISION,
        season BIGINT,
        sequence BIGINT,
        "teamName.default" TEXT,
        "teamCommonName.default" TEXT,
        "faceoffWinningPctg" DOUBLE PRECISION,
        "shootingPctg" DOUBLE PRECISION,
        shots DOUBLE PRECISION,
        "powerPlayGoals" DOUBLE PRECISION,
        "shorthandedGoals" DOUBLE PRECISION,
        "gameWinningGoals" DOUBLE PRECISION,
        "avgToi" DOUBLE PRECISION,
        "otGoals" DOUBLE PRECISION,
        "powerPlayPoints" DOUBLE PRECISION,
        "shorthandedPoints" DOUBLE PRECISION
    );

    -- Landing scrapes may have already created staging with a subset of
    -- columns. ADD COLUMN IF NOT EXISTS matches season_goalie_table_upsert.sql.
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "playerId" BIGINT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS assists DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "gameTypeId" BIGINT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "gamesPlayed" DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS goals DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "leagueAbbrev" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS pim DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "plusMinus" DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS points DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS season BIGINT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS sequence BIGINT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamName.default" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamCommonName.default" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "faceoffWinningPctg" DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "shootingPctg" DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS shots DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "powerPlayGoals" DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "shorthandedGoals" DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "gameWinningGoals" DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "avgToi" DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "otGoals" DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "powerPlayPoints" DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "shorthandedPoints" DOUBLE PRECISION;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamCommonName.cs" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamCommonName.de" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamCommonName.es" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamCommonName.fi" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamCommonName.sk" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamCommonName.sv" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamCommonName.fr" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamName.cs" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamName.de" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamName.fi" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamName.sk" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamName.sv" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamName.fr" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.default" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.fr" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.cs" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.es" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.fi" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.sk" TEXT;
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.sv" TEXT;

    -- Staging only. Production newapi.season_skater is not truncated.
    -- Wipes any pending landing-scrape rows in staging1.season_skater,
    -- so this CALL must run after landing season sync in the same job.
    TRUNCATE staging1.season_skater;

    INSERT INTO staging1.season_skater (
        "playerId", assists, "gameTypeId", "gamesPlayed", goals, "leagueAbbrev",
        pim, "plusMinus", points, season, sequence, "teamName.default",
        "teamCommonName.default",
        "faceoffWinningPctg", "shootingPctg", shots, "powerPlayGoals",
        "shorthandedGoals", "gameWinningGoals", "avgToi", "otGoals",
        "powerPlayPoints", "shorthandedPoints"
    )
    SELECT
        "playerId", assists, "gameTypeId", "gamesPlayed", goals, "leagueAbbrev",
        pim, "plusMinus", points, season, sequence, "teamName.default",
        "teamCommonName.default",
        "faceoffWinningPctg", "shootingPctg", shots, "powerPlayGoals",
        "shorthandedGoals", "gameWinningGoals", "avgToi", "otGoals",
        "powerPlayPoints", "shorthandedPoints"
    FROM newapi.season_skater_from_club_stats;
END;
$$;

CREATE OR REPLACE PROCEDURE sync_season_skaters_from_club_stats()
LANGUAGE plpgsql AS $$
BEGIN
    CALL load_season_skater_staging_from_club_stats();
    CALL sync_season_skaters_from_staging();
END;
$$;

-- Club-stats (newapi.goalies) → season_goalie staging shape.
-- Reuse the live season_* key so hash upsert overwrites W-L / TOI / GAA.
CREATE OR REPLACE VIEW newapi.season_goalie_from_club_stats AS
WITH club AS (
    SELECT
        newapi.club_stats_bigint(g."playerId"::text) AS "playerId",
        newapi.club_stats_bigint(g."gameType"::text) AS "gameTypeId",
        newapi.club_stats_float(g."gamesPlayed"::text) AS "gamesPlayed",
        newapi.club_stats_float(g."goalsAgainst"::text) AS "goalsAgainst",
        newapi.club_stats_float(g."goalsAgainstAverage"::text) AS "goalsAgainstAvg",
        'NHL'::text AS "leagueAbbrev",
        newapi.club_stats_float(g.losses::text) AS losses,
        newapi.club_stats_bigint(g.season::text) AS season,
        newapi.club_stats_float(g.shutouts::text) AS shutouts,
        newapi.club_stats_float(g.ties::text) AS ties,
        CASE
            WHEN NULLIF(TRIM(g."timeOnIce"::text), '') IS NULL THEN NULL
            WHEN TRIM(g."timeOnIce"::text) ~ '^\d+:[0-5]?\d$' THEN TRIM(g."timeOnIce"::text)
            WHEN TRIM(g."timeOnIce"::text) ~ '^\d+(\.\d+)?$' THEN
                (FLOOR(TRIM(g."timeOnIce"::text)::double precision / 60)::bigint)::text
                || ':'
                || LPAD(
                    (ROUND(TRIM(g."timeOnIce"::text)::double precision)::bigint % 60)::text,
                    2,
                    '0'
                )
            ELSE NULL
        END AS "timeOnIce",
        newapi.club_stats_float(g.wins::text) AS wins,
        newapi.club_stats_float(g.assists::text) AS assists,
        newapi.club_stats_float(g."gamesStarted"::text) AS "gamesStarted",
        newapi.club_stats_float(g.goals::text) AS goals,
        newapi.club_stats_float(g."penaltyMinutes"::text) AS pim,
        newapi.club_stats_float(g."savePercentage"::text) AS "savePctg",
        newapi.club_stats_float(g."shotsAgainst"::text) AS "shotsAgainst",
        newapi.club_stats_float(g."overtimeLosses"::text) AS "otLosses",
        UPPER(TRIM(g.team)) AS tricode
    FROM newapi.goalies g
    WHERE g.is_active = TRUE
      AND newapi.club_stats_bigint(g."playerId"::text) IS NOT NULL
      AND newapi.club_stats_bigint(g."gameType"::text) IS NOT NULL
      AND newapi.club_stats_bigint(g.season::text) IS NOT NULL
      AND NULLIF(TRIM(g.team), '') IS NOT NULL
)
SELECT
    c."playerId",
    c."gameTypeId",
    c."gamesPlayed",
    c."goalsAgainst",
    c."goalsAgainstAvg",
    c."leagueAbbrev",
    c.losses,
    c.season,
    COALESCE(existing.sequence, 1::bigint) AS sequence,
    c.shutouts,
    c.ties,
    c."timeOnIce",
    c.wins,
    COALESCE(existing."teamName.default", t."fullName") AS "teamName.default",
    COALESCE(existing."teamCommonName.default", t."teamCommonName") AS "teamCommonName.default",
    c.assists,
    c."gamesStarted",
    c.goals,
    c.pim,
    c."savePctg",
    c."shotsAgainst",
    c."otLosses"
FROM club c
LEFT JOIN newapi.team_fullname_by_tricode t ON t.tricode = c.tricode
LEFT JOIN LATERAL (
    SELECT
        sg.sequence,
        sg."teamName.default",
        sg."teamCommonName.default"
    FROM newapi.season_goalie sg
    LEFT JOIN newapi.teams et ON et."fullName" = sg."teamName.default"
    WHERE sg.is_active = TRUE
      AND sg."playerId" = c."playerId"
      AND sg.season = c.season
      AND sg."gameTypeId" = c."gameTypeId"
      AND sg."leagueAbbrev" = 'NHL'
      AND (
          sg."teamName.default" = t."fullName"
          OR sg."teamName.default" = t."teamCommonName"
          OR UPPER(TRIM(et."rawTricode")) = c.tricode
          OR UPPER(TRIM(et."triCode")) = c.tricode
          OR NOT EXISTS (
              SELECT 1
              FROM newapi.season_goalie other
              WHERE other.is_active = TRUE
                AND other."playerId" = c."playerId"
                AND other.season = c.season
                AND other."gameTypeId" = c."gameTypeId"
                AND other."leagueAbbrev" = 'NHL'
                AND other."teamName.default" IS DISTINCT FROM sg."teamName.default"
          )
      )
    ORDER BY
        CASE
            WHEN sg."teamName.default" = t."fullName" THEN 0
            WHEN UPPER(TRIM(COALESCE(et."rawTricode", et."triCode"))) = c.tricode THEN 1
            ELSE 2
        END,
        sg.sequence
    LIMIT 1
) existing ON TRUE
WHERE COALESCE(existing."teamName.default", t."fullName") IS NOT NULL;

CREATE OR REPLACE VIEW newapi.season_goalie_missing_from_club_stats AS
SELECT
    c."playerId",
    c.season,
    c.sequence,
    c."gameTypeId",
    c."teamName.default",
    c."gamesPlayed",
    c.wins,
    c.losses,
    c."savePctg",
    c."timeOnIce"
FROM newapi.season_goalie_from_club_stats c
WHERE NOT EXISTS (
    SELECT 1
    FROM newapi.season_goalie sg
    WHERE sg.is_active = TRUE
      AND sg."playerId" = c."playerId"
      AND sg.season = c.season
      AND sg.sequence = c.sequence
      AND sg."gameTypeId" = c."gameTypeId"
      AND sg."leagueAbbrev" = 'NHL'
      AND sg."teamName.default" = c."teamName.default"
);

CREATE OR REPLACE VIEW newapi.season_goalie_stale_from_club_stats AS
SELECT
    c."playerId",
    c.season,
    c.sequence,
    c."gameTypeId",
    c."teamName.default",
    sg."gamesPlayed" AS season_games,
    c."gamesPlayed" AS club_games,
    sg.wins AS season_wins,
    c.wins AS club_wins,
    sg.losses AS season_losses,
    c.losses AS club_losses,
    sg."savePctg" AS season_save_pctg,
    c."savePctg" AS club_save_pctg
FROM newapi.season_goalie_from_club_stats c
JOIN newapi.season_goalie sg
    ON sg.is_active = TRUE
   AND sg."playerId" = c."playerId"
   AND sg.season = c.season
   AND sg.sequence = c.sequence
   AND sg."gameTypeId" = c."gameTypeId"
   AND sg."leagueAbbrev" = 'NHL'
   AND sg."teamName.default" = c."teamName.default"
WHERE sg."gamesPlayed" IS DISTINCT FROM c."gamesPlayed"
   OR sg.wins IS DISTINCT FROM c.wins
   OR sg.losses IS DISTINCT FROM c.losses
   OR sg."savePctg" IS DISTINCT FROM c."savePctg";

CREATE OR REPLACE PROCEDURE load_season_goalie_staging_from_club_stats()
LANGUAGE plpgsql AS $$
BEGIN
    CREATE SCHEMA IF NOT EXISTS staging1;

    CREATE TABLE IF NOT EXISTS staging1.season_goalie (
        "playerId" BIGINT,
        "gameTypeId" BIGINT,
        "gamesPlayed" DOUBLE PRECISION,
        "goalsAgainst" DOUBLE PRECISION,
        "goalsAgainstAvg" DOUBLE PRECISION,
        "leagueAbbrev" TEXT,
        losses DOUBLE PRECISION,
        season BIGINT,
        sequence BIGINT,
        shutouts DOUBLE PRECISION,
        ties DOUBLE PRECISION,
        "timeOnIce" TEXT,
        wins DOUBLE PRECISION,
        "teamName.default" TEXT,
        "teamCommonName.default" TEXT,
        assists DOUBLE PRECISION,
        "gamesStarted" DOUBLE PRECISION,
        goals DOUBLE PRECISION,
        pim DOUBLE PRECISION,
        "savePctg" DOUBLE PRECISION,
        "shotsAgainst" DOUBLE PRECISION,
        "otLosses" DOUBLE PRECISION
    );

    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "playerId" BIGINT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "gameTypeId" BIGINT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "gamesPlayed" DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "goalsAgainst" DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "goalsAgainstAvg" DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "leagueAbbrev" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS losses DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS season BIGINT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS sequence BIGINT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS shutouts DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS ties DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "timeOnIce" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS wins DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.default" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.default" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS assists DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "gamesStarted" DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS goals DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS pim DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "savePctg" DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "shotsAgainst" DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "otLosses" DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.cs" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.de" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.es" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.fi" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.sk" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.sv" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.fr" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.cs" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.de" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.fi" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.sk" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.sv" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.fr" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.default" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.fr" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.cs" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.es" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.fi" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.sk" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.sv" TEXT;

    -- Staging only. Production newapi.season_goalie is not truncated.
    TRUNCATE staging1.season_goalie;

    INSERT INTO staging1.season_goalie (
        "playerId", "gameTypeId", "gamesPlayed", "goalsAgainst", "goalsAgainstAvg",
        "leagueAbbrev", losses, season, sequence, shutouts, ties, "timeOnIce",
        wins, "teamName.default", "teamCommonName.default",
        assists, "gamesStarted", goals, pim,
        "savePctg", "shotsAgainst", "otLosses"
    )
    SELECT
        "playerId", "gameTypeId", "gamesPlayed", "goalsAgainst", "goalsAgainstAvg",
        "leagueAbbrev", losses, season, sequence, shutouts, ties, "timeOnIce",
        wins, "teamName.default", "teamCommonName.default",
        assists, "gamesStarted", goals, pim,
        "savePctg", "shotsAgainst", "otLosses"
    FROM newapi.season_goalie_from_club_stats;
END;
$$;

CREATE OR REPLACE PROCEDURE sync_season_goalies_from_club_stats()
LANGUAGE plpgsql AS $$
BEGIN
    CALL load_season_goalie_staging_from_club_stats();
    CALL sync_season_goalies_from_staging();
END;
$$;

-- Live RDS: this whole file is safe to re-apply. It does not DROP tables.
-- After CALL sync_skaters_from_staging() / sync_goalies_from_staging():
-- CALL sync_season_skaters_from_club_stats();
-- CALL sync_season_goalies_from_club_stats();
