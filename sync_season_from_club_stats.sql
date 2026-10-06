-- SAFE TO RE-RUN against populated production (live RDS).
-- Additive only: CREATE OR REPLACE views/procedures. No DROP TABLE.
-- Depends on sync_season_skaters_from_staging() /
-- sync_season_goalies_from_staging() from season_*_table_upsert.sql.
--
-- Why: daily club-stats land in newapi.skaters / newapi.goalies, but
-- player pages and leaders read newapi.season_skater / season_goalie
-- (landing scrape). Playing-scope landing only scrapes new call-ups, so
-- rostered players can appear on team pages and still miss the new
-- season (e.g. 20262027) on /api/players/:id. Do not fill that gap
-- from the NHL API at request time — run this backfill on RDS instead.
--
-- Apply (install views + procedures only; does not write season rows):
--   psql "$DATABASE_URL" -f sync_season_from_club_stats.sql
--
-- Preview missing player-history rows that club-stats already have:
--   SELECT * FROM newapi.season_skater_missing_from_club_stats
--   WHERE season = 20262027;
--   SELECT * FROM newapi.season_goalie_missing_from_club_stats
--   WHERE season = 20262027;
--
-- Backfill / daily hook (truncates staging1.season_* only, then upserts):
--   CALL sync_season_skaters_from_club_stats();
--   CALL sync_season_goalies_from_club_stats();
--
-- Keys use the same NULLIF+TRIM then ::double precision::bigint
-- normalize as season_*_table_upsert.sql so '' and '2024.0' do not
-- abort the batch. sequence defaults to 1 to match NHL landing NHL rows.

CREATE SCHEMA IF NOT EXISTS newapi;
CREATE SCHEMA IF NOT EXISTS staging1;

DROP PROCEDURE IF EXISTS sync_season_skaters_from_club_stats() CASCADE;
DROP PROCEDURE IF EXISTS sync_season_goalies_from_club_stats() CASCADE;
DROP PROCEDURE IF EXISTS load_season_skater_staging_from_club_stats() CASCADE;
DROP PROCEDURE IF EXISTS load_season_goalie_staging_from_club_stats() CASCADE;

-- Active team fullName by club-stats tricode (rawTricode or triCode).
CREATE OR REPLACE VIEW newapi.team_fullname_by_tricode AS
SELECT DISTINCT ON (abbrev)
    abbrev AS tricode,
    "fullName"
FROM (
    SELECT TRIM(t."rawTricode") AS abbrev, t."fullName", t.active
    FROM newapi.teams t
    UNION ALL
    SELECT TRIM(t."triCode") AS abbrev, t."fullName", t.active
    FROM newapi.teams t
) t
WHERE NULLIF(abbrev, '') IS NOT NULL
  AND NULLIF(TRIM(t."fullName"), '') IS NOT NULL
ORDER BY abbrev, t.active DESC NULLS LAST;

-- Club-stats (newapi.skaters) → player season history shape.
CREATE OR REPLACE VIEW newapi.season_skater_from_club_stats AS
SELECT
    NULLIF(TRIM(s."playerId"::text), '')::double precision::bigint AS "playerId",
    s.assists::double precision AS assists,
    NULLIF(TRIM(s."gameType"::text), '')::double precision::bigint AS "gameTypeId",
    s."gamesPlayed"::double precision AS "gamesPlayed",
    s.goals::double precision AS goals,
    'NHL'::text AS "leagueAbbrev",
    s."penaltyMinutes"::double precision AS pim,
    s."plusMinus"::double precision AS "plusMinus",
    s.points::double precision AS points,
    NULLIF(TRIM(s.season::text), '')::double precision::bigint AS season,
    1::bigint AS sequence,
    t."fullName" AS "teamName.default",
    s."faceoffWinPctg"::double precision AS "faceoffWinningPctg",
    s."shootingPctg"::double precision AS "shootingPctg",
    s.shots::double precision AS shots,
    s."powerPlayGoals"::double precision AS "powerPlayGoals",
    s."shorthandedGoals"::double precision AS "shorthandedGoals",
    s."gameWinningGoals"::double precision AS "gameWinningGoals",
    s."avgTimeOnIcePerGame"::double precision AS "avgToi",
    s."overtimeGoals"::double precision AS "otGoals",
    NULL::double precision AS "powerPlayPoints",
    NULL::double precision AS "shorthandedPoints"
FROM newapi.skaters s
JOIN newapi.team_fullname_by_tricode t ON t.tricode = TRIM(s."triCode")
WHERE s.is_active = TRUE
  AND NULLIF(TRIM(s."playerId"::text), '') IS NOT NULL
  AND NULLIF(TRIM(s."gameType"::text), '') IS NOT NULL
  AND NULLIF(TRIM(s.season::text), '') IS NOT NULL;

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
    c.points
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
    ALTER TABLE staging1.season_skater ADD COLUMN IF NOT EXISTS "teamCommonName.default" TEXT;
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
    -- Wipes any pending landing-scrape rows in staging1.season_skater.
    TRUNCATE staging1.season_skater;

    INSERT INTO staging1.season_skater (
        "playerId", assists, "gameTypeId", "gamesPlayed", goals, "leagueAbbrev",
        pim, "plusMinus", points, season, sequence, "teamName.default",
        "faceoffWinningPctg", "shootingPctg", shots, "powerPlayGoals",
        "shorthandedGoals", "gameWinningGoals", "avgToi", "otGoals",
        "powerPlayPoints", "shorthandedPoints"
    )
    SELECT
        "playerId", assists, "gameTypeId", "gamesPlayed", goals, "leagueAbbrev",
        pim, "plusMinus", points, season, sequence, "teamName.default",
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

-- Club-stats (newapi.goalies) → player season history. Same gap as skaters:
-- daily club-stats update team rollups, landing scrape only covers call-ups.
-- sequence defaults to 1. team on newapi.goalies is the NHL triCode.

CREATE OR REPLACE VIEW newapi.season_goalie_from_club_stats AS
SELECT
    NULLIF(TRIM(g."playerId"::text), '')::double precision::bigint AS "playerId",
    NULLIF(TRIM(g."gameType"::text), '')::double precision::bigint AS "gameTypeId",
    g."gamesPlayed"::double precision AS "gamesPlayed",
    g."goalsAgainst"::double precision AS "goalsAgainst",
    g."goalsAgainstAverage"::double precision AS "goalsAgainstAvg",
    'NHL'::text AS "leagueAbbrev",
    g.losses::double precision AS losses,
    NULLIF(TRIM(g.season::text), '')::double precision::bigint AS season,
    1::bigint AS sequence,
    g.shutouts::double precision AS shutouts,
    g.ties::double precision AS ties,
    g."timeOnIce"::text AS "timeOnIce",
    g.wins::double precision AS wins,
    t."fullName" AS "teamName.default",
    g.assists::double precision AS assists,
    g."gamesStarted"::double precision AS "gamesStarted",
    g.goals::double precision AS goals,
    g."penaltyMinutes"::double precision AS pim,
    g."savePercentage"::double precision AS "savePctg",
    g."shotsAgainst"::double precision AS "shotsAgainst",
    g."overtimeLosses"::double precision AS "otLosses"
FROM newapi.goalies g
JOIN newapi.team_fullname_by_tricode t ON t.tricode = TRIM(g.team)
WHERE g.is_active = TRUE
  AND NULLIF(TRIM(g."playerId"::text), '') IS NOT NULL
  AND NULLIF(TRIM(g."gameType"::text), '') IS NOT NULL
  AND NULLIF(TRIM(g.season::text), '') IS NOT NULL;

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
    c."savePctg"
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
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS assists DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "gamesStarted" DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS goals DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS pim DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "savePctg" DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "shotsAgainst" DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "otLosses" DOUBLE PRECISION;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.default" TEXT;
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
    -- Wipes any pending landing-scrape rows in staging1.season_goalie.
    TRUNCATE staging1.season_goalie;

    INSERT INTO staging1.season_goalie (
        "playerId", "gameTypeId", "gamesPlayed", "goalsAgainst", "goalsAgainstAvg",
        "leagueAbbrev", losses, season, sequence, shutouts, ties, "timeOnIce",
        wins, "teamName.default", assists, "gamesStarted", goals, pim,
        "savePctg", "shotsAgainst", "otLosses"
    )
    SELECT
        "playerId", "gameTypeId", "gamesPlayed", "goalsAgainst", "goalsAgainstAvg",
        "leagueAbbrev", losses, season, sequence, shutouts, ties, "timeOnIce",
        wins, "teamName.default", assists, "gamesStarted", goals, pim,
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
-- CALL sync_season_skaters_from_club_stats();
-- CALL sync_season_goalies_from_club_stats();
