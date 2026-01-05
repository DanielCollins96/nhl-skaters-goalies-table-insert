-- Ensure the schema exists
CREATE SCHEMA IF NOT EXISTS newapi;
CREATE SCHEMA IF NOT EXISTS staging1;


-- ============================================================================
-- FRANCHISES TABLE (newapi.franchises)
-- ============================================================================

-- Drop existing functions/procedures for franchises
DROP FUNCTION IF EXISTS upsert_franchises_from_staging_with_logging() CASCADE;
DROP PROCEDURE IF EXISTS sync_franchises_from_staging() CASCADE;

-- Create the production franchises table
CREATE TABLE IF NOT EXISTS newapi."franchises" (
    id BIGINT PRIMARY KEY,
    "fullName" TEXT NOT NULL,
    "teamCommonName" TEXT,
    "teamPlaceName" TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_franchises_common_name
    ON newapi."franchises" ("teamCommonName");

-- Create ETL log table for franchises
CREATE TABLE IF NOT EXISTS newapi.franchises_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_franchises_etl_log_timestamp
    ON newapi.franchises_etl_log (run_timestamp);

-- Function: Upsert franchises with logging
CREATE OR REPLACE FUNCTION upsert_franchises_from_staging_with_logging()
RETURNS TABLE(
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL
) AS $$
DECLARE
    v_total INTEGER := 0;
    v_inserted INTEGER := 0;
    v_updated INTEGER := 0;
    v_unchanged INTEGER := 0;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN
    start_time := CURRENT_TIMESTAMP;

    WITH src AS (
        SELECT
            NULLIF(f.id::text, '')::BIGINT AS id,
            f."fullName" AS "fullName",
            f."teamCommonName" AS "teamCommonName",
            f."teamPlaceName" AS "teamPlaceName"
        FROM staging1."franchises" f
        WHERE NULLIF(f.id::text, '')::BIGINT IS NOT NULL
    ), upsert AS (
        INSERT INTO newapi."franchises" (
            id, "fullName", "teamCommonName", "teamPlaceName"
        )
        SELECT
            s.id, s."fullName", s."teamCommonName", s."teamPlaceName"
        FROM src s
        ON CONFLICT (id) DO UPDATE
        SET
            "fullName" = EXCLUDED."fullName",
            "teamCommonName" = EXCLUDED."teamCommonName",
            "teamPlaceName" = EXCLUDED."teamPlaceName",
            updated_at = CURRENT_TIMESTAMP
        WHERE (
            EXCLUDED."fullName" IS DISTINCT FROM newapi."franchises"."fullName"
            OR EXCLUDED."teamCommonName" IS DISTINCT FROM newapi."franchises"."teamCommonName"
            OR EXCLUDED."teamPlaceName" IS DISTINCT FROM newapi."franchises"."teamPlaceName"
        )
        RETURNING xmax
    ), counts AS (
        SELECT
            (SELECT COUNT(*) FROM src) AS total_src,
            COUNT(*) FILTER (WHERE xmax::text::bigint = 0) AS inserted_cnt,
            COUNT(*) FILTER (WHERE xmax::text::bigint <> 0) AS updated_cnt
        FROM upsert
    )
    SELECT total_src, inserted_cnt, updated_cnt
    INTO v_total, v_inserted, v_updated
    FROM counts;

    v_unchanged := COALESCE(v_total, 0) - COALESCE(v_inserted, 0) - COALESCE(v_updated, 0);

    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;

    INSERT INTO newapi.franchises_etl_log (
        total_processed, inserted_records, updated_records, unchanged_records, run_duration
    ) VALUES (
        COALESCE(v_total, 0), COALESCE(v_inserted, 0), COALESCE(v_updated, 0), COALESCE(v_unchanged, 0), duration
    );

    RETURN QUERY SELECT v_total, v_inserted, v_updated, v_unchanged, duration;
END;
$$ LANGUAGE plpgsql;

-- Procedure to run the franchises upsert
CREATE OR REPLACE PROCEDURE sync_franchises_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM upsert_franchises_from_staging_with_logging() INTO result_record;

    RAISE NOTICE 'Franchises sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  Inserted records: %', result_record.inserted_records;
    RAISE NOTICE '  Updated records: %', result_record.updated_records;
    RAISE NOTICE '  Unchanged records: %', result_record.unchanged_records;
END;
$$;

-- Summary view for franchises ETL runs
CREATE OR REPLACE VIEW newapi.franchises_etl_summary AS
SELECT 
    id,
    run_timestamp,
    total_processed,
    inserted_records,
    updated_records,
    unchanged_records,
    run_duration,
    ROUND(EXTRACT(EPOCH FROM run_duration)::NUMERIC, 2) AS duration_seconds,
    CASE 
        WHEN total_processed > 0 THEN ROUND((inserted_records + updated_records)::NUMERIC / total_processed * 100, 1)
        ELSE 0 
    END AS change_percentage
FROM newapi.franchises_etl_log
ORDER BY run_timestamp DESC;


-- ============================================================================
-- TEAM SEASON TABLE (newapi.team_season)
-- ============================================================================

-- Drop existing functions/procedures for team_season
DROP FUNCTION IF EXISTS upsert_team_season_from_staging_with_logging() CASCADE;
DROP PROCEDURE IF EXISTS sync_team_season_from_staging() CASCADE;

-- Create the production team_season table
CREATE TABLE IF NOT EXISTS newapi.team_season (
    id SERIAL PRIMARY KEY,
    "seasonId" TEXT NOT NULL,
    "teamId" BIGINT NOT NULL,
    "teamFullName" TEXT,
    "faceoffWinPct" DOUBLE PRECISION,
    "gamesPlayed" INTEGER,
    "goalsAgainst" INTEGER,
    "goalsAgainstPerGame" DOUBLE PRECISION,
    "goalsFor" INTEGER,
    "goalsForPerGame" DOUBLE PRECISION,
    losses INTEGER,
    "otLosses" INTEGER,
    "penaltyKillNetPct" DOUBLE PRECISION,
    "penaltyKillPct" DOUBLE PRECISION,
    "pointPct" DOUBLE PRECISION,
    points INTEGER,
    "powerPlayNetPct" DOUBLE PRECISION,
    "powerPlayPct" DOUBLE PRECISION,
    "regulationAndOtWins" INTEGER,
    "shotsAgainstPerGame" DOUBLE PRECISION,
    "shotsForPerGame" DOUBLE PRECISION,
    "teamShutouts" INTEGER,
    ties INTEGER,
    wins INTEGER,
    "winsInRegulation" INTEGER,
    "winsInShootout" INTEGER,
    "gameTypeId" TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique natural key for a team season
    UNIQUE ("seasonId", "teamId", "gameTypeId")
);

CREATE INDEX IF NOT EXISTS idx_team_season_natural_key
    ON newapi.team_season ("seasonId", "teamId", "gameTypeId");

CREATE INDEX IF NOT EXISTS idx_team_season_team_id
    ON newapi.team_season ("teamId");

-- Create ETL log table for team_season
CREATE TABLE IF NOT EXISTS newapi.team_season_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_team_season_etl_log_timestamp
    ON newapi.team_season_etl_log (run_timestamp);

-- Function: Upsert team_season with logging
CREATE OR REPLACE FUNCTION upsert_team_season_from_staging_with_logging()
RETURNS TABLE(
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL
) AS $$
DECLARE
    v_total INTEGER := 0;
    v_inserted INTEGER := 0;
    v_updated INTEGER := 0;
    v_unchanged INTEGER := 0;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN
    start_time := CURRENT_TIMESTAMP;

    WITH src_raw AS (
        SELECT
            ts."seasonId" AS "seasonId",
            NULLIF(ts."teamId"::text, '')::BIGINT AS "teamId",
            ts."teamFullName" AS "teamFullName",
            NULLIF(ts."faceoffWinPct"::text, '')::DOUBLE PRECISION AS "faceoffWinPct",
            NULLIF(ts."gamesPlayed"::text, '')::INTEGER AS "gamesPlayed",
            NULLIF(ts."goalsAgainst"::text, '')::INTEGER AS "goalsAgainst",
            NULLIF(ts."goalsAgainstPerGame"::text, '')::DOUBLE PRECISION AS "goalsAgainstPerGame",
            NULLIF(ts."goalsFor"::text, '')::INTEGER AS "goalsFor",
            NULLIF(ts."goalsForPerGame"::text, '')::DOUBLE PRECISION AS "goalsForPerGame",
            NULLIF(ts.losses::text, '')::INTEGER AS losses,
            NULLIF(ts."otLosses"::text, '')::INTEGER AS "otLosses",
            NULLIF(ts."penaltyKillNetPct"::text, '')::DOUBLE PRECISION AS "penaltyKillNetPct",
            NULLIF(ts."penaltyKillPct"::text, '')::DOUBLE PRECISION AS "penaltyKillPct",
            NULLIF(ts."pointPct"::text, '')::DOUBLE PRECISION AS "pointPct",
            NULLIF(ts.points::text, '')::INTEGER AS points,
            NULLIF(ts."powerPlayNetPct"::text, '')::DOUBLE PRECISION AS "powerPlayNetPct",
            NULLIF(ts."powerPlayPct"::text, '')::DOUBLE PRECISION AS "powerPlayPct",
            NULLIF(ts."regulationAndOtWins"::text, '')::INTEGER AS "regulationAndOtWins",
            NULLIF(ts."shotsAgainstPerGame"::text, '')::DOUBLE PRECISION AS "shotsAgainstPerGame",
            NULLIF(ts."shotsForPerGame"::text, '')::DOUBLE PRECISION AS "shotsForPerGame",
            NULLIF(ts."teamShutouts"::text, '')::INTEGER AS "teamShutouts",
            NULLIF(ts.ties::text, '')::INTEGER AS ties,
            NULLIF(ts.wins::text, '')::INTEGER AS wins,
            NULLIF(ts."winsInRegulation"::text, '')::INTEGER AS "winsInRegulation",
            NULLIF(ts."winsInShootout"::text, '')::INTEGER AS "winsInShootout",
            ts."gameTypeId" AS "gameTypeId"
        FROM staging1.team_season ts
        WHERE NULLIF(ts."seasonId"::text, '') IS NOT NULL
            AND NULLIF(ts."teamId"::text, '')::BIGINT IS NOT NULL
    ), src AS (
        SELECT DISTINCT ON ("seasonId", "teamId", "gameTypeId") *
        FROM src_raw
        ORDER BY "seasonId", "teamId", "gameTypeId"
    ), upsert AS (
        INSERT INTO newapi.team_season (
            "seasonId", "teamId", "teamFullName", "faceoffWinPct", "gamesPlayed",
            "goalsAgainst", "goalsAgainstPerGame", "goalsFor", "goalsForPerGame",
            losses, "otLosses", "penaltyKillNetPct", "penaltyKillPct", "pointPct",
            points, "powerPlayNetPct", "powerPlayPct", "regulationAndOtWins",
            "shotsAgainstPerGame", "shotsForPerGame", "teamShutouts", ties, wins,
            "winsInRegulation", "winsInShootout", "gameTypeId"
        )
        SELECT
            s."seasonId", s."teamId", s."teamFullName", s."faceoffWinPct", s."gamesPlayed",
            s."goalsAgainst", s."goalsAgainstPerGame", s."goalsFor", s."goalsForPerGame",
            s.losses, s."otLosses", s."penaltyKillNetPct", s."penaltyKillPct", s."pointPct",
            s.points, s."powerPlayNetPct", s."powerPlayPct", s."regulationAndOtWins",
            s."shotsAgainstPerGame", s."shotsForPerGame", s."teamShutouts", s.ties, s.wins,
            s."winsInRegulation", s."winsInShootout", s."gameTypeId"
        FROM src s
        ON CONFLICT ("seasonId", "teamId", "gameTypeId") DO UPDATE
        SET
            "teamFullName" = EXCLUDED."teamFullName",
            "faceoffWinPct" = EXCLUDED."faceoffWinPct",
            "gamesPlayed" = EXCLUDED."gamesPlayed",
            "goalsAgainst" = EXCLUDED."goalsAgainst",
            "goalsAgainstPerGame" = EXCLUDED."goalsAgainstPerGame",
            "goalsFor" = EXCLUDED."goalsFor",
            "goalsForPerGame" = EXCLUDED."goalsForPerGame",
            losses = EXCLUDED.losses,
            "otLosses" = EXCLUDED."otLosses",
            "penaltyKillNetPct" = EXCLUDED."penaltyKillNetPct",
            "penaltyKillPct" = EXCLUDED."penaltyKillPct",
            "pointPct" = EXCLUDED."pointPct",
            points = EXCLUDED.points,
            "powerPlayNetPct" = EXCLUDED."powerPlayNetPct",
            "powerPlayPct" = EXCLUDED."powerPlayPct",
            "regulationAndOtWins" = EXCLUDED."regulationAndOtWins",
            "shotsAgainstPerGame" = EXCLUDED."shotsAgainstPerGame",
            "shotsForPerGame" = EXCLUDED."shotsForPerGame",
            "teamShutouts" = EXCLUDED."teamShutouts",
            ties = EXCLUDED.ties,
            wins = EXCLUDED.wins,
            "winsInRegulation" = EXCLUDED."winsInRegulation",
            "winsInShootout" = EXCLUDED."winsInShootout",
            updated_at = CURRENT_TIMESTAMP
        WHERE (
            EXCLUDED."teamFullName" IS DISTINCT FROM newapi.team_season."teamFullName"
            OR EXCLUDED."faceoffWinPct" IS DISTINCT FROM newapi.team_season."faceoffWinPct"
            OR EXCLUDED."gamesPlayed" IS DISTINCT FROM newapi.team_season."gamesPlayed"
            OR EXCLUDED."goalsAgainst" IS DISTINCT FROM newapi.team_season."goalsAgainst"
            OR EXCLUDED."goalsAgainstPerGame" IS DISTINCT FROM newapi.team_season."goalsAgainstPerGame"
            OR EXCLUDED."goalsFor" IS DISTINCT FROM newapi.team_season."goalsFor"
            OR EXCLUDED."goalsForPerGame" IS DISTINCT FROM newapi.team_season."goalsForPerGame"
            OR EXCLUDED.losses IS DISTINCT FROM newapi.team_season.losses
            OR EXCLUDED."otLosses" IS DISTINCT FROM newapi.team_season."otLosses"
            OR EXCLUDED."penaltyKillNetPct" IS DISTINCT FROM newapi.team_season."penaltyKillNetPct"
            OR EXCLUDED."penaltyKillPct" IS DISTINCT FROM newapi.team_season."penaltyKillPct"
            OR EXCLUDED."pointPct" IS DISTINCT FROM newapi.team_season."pointPct"
            OR EXCLUDED.points IS DISTINCT FROM newapi.team_season.points
            OR EXCLUDED."powerPlayNetPct" IS DISTINCT FROM newapi.team_season."powerPlayNetPct"
            OR EXCLUDED."powerPlayPct" IS DISTINCT FROM newapi.team_season."powerPlayPct"
            OR EXCLUDED."regulationAndOtWins" IS DISTINCT FROM newapi.team_season."regulationAndOtWins"
            OR EXCLUDED."shotsAgainstPerGame" IS DISTINCT FROM newapi.team_season."shotsAgainstPerGame"
            OR EXCLUDED."shotsForPerGame" IS DISTINCT FROM newapi.team_season."shotsForPerGame"
            OR EXCLUDED."teamShutouts" IS DISTINCT FROM newapi.team_season."teamShutouts"
            OR EXCLUDED.ties IS DISTINCT FROM newapi.team_season.ties
            OR EXCLUDED.wins IS DISTINCT FROM newapi.team_season.wins
            OR EXCLUDED."winsInRegulation" IS DISTINCT FROM newapi.team_season."winsInRegulation"
            OR EXCLUDED."winsInShootout" IS DISTINCT FROM newapi.team_season."winsInShootout"
        )
        RETURNING xmax
    ), counts AS (
        SELECT
            (SELECT COUNT(*) FROM src) AS total_src,
            COUNT(*) FILTER (WHERE xmax::text::bigint = 0) AS inserted_cnt,
            COUNT(*) FILTER (WHERE xmax::text::bigint <> 0) AS updated_cnt
        FROM upsert
    )
    SELECT total_src, inserted_cnt, updated_cnt
    INTO v_total, v_inserted, v_updated
    FROM counts;

    v_unchanged := COALESCE(v_total, 0) - COALESCE(v_inserted, 0) - COALESCE(v_updated, 0);

    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;

    INSERT INTO newapi.team_season_etl_log (
        total_processed, inserted_records, updated_records, unchanged_records, run_duration
    ) VALUES (
        COALESCE(v_total, 0), COALESCE(v_inserted, 0), COALESCE(v_updated, 0), COALESCE(v_unchanged, 0), duration
    );

    RETURN QUERY SELECT v_total, v_inserted, v_updated, v_unchanged, duration;
END;
$$ LANGUAGE plpgsql;

-- Procedure to run the team_season upsert
CREATE OR REPLACE PROCEDURE sync_team_season_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM upsert_team_season_from_staging_with_logging() INTO result_record;

    RAISE NOTICE 'Team season sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  Inserted records: %', result_record.inserted_records;
    RAISE NOTICE '  Updated records: %', result_record.updated_records;
    RAISE NOTICE '  Unchanged records: %', result_record.unchanged_records;
END;
$$;

-- Summary view for team_season ETL runs
CREATE OR REPLACE VIEW newapi.team_season_etl_summary AS
SELECT 
    id,
    run_timestamp,
    total_processed,
    inserted_records,
    updated_records,
    unchanged_records,
    run_duration,
    ROUND(EXTRACT(EPOCH FROM run_duration)::NUMERIC, 2) AS duration_seconds,
    CASE 
        WHEN total_processed > 0 THEN ROUND((inserted_records + updated_records)::NUMERIC / total_processed * 100, 1)
        ELSE 0 
    END AS change_percentage
FROM newapi.team_season_etl_log
ORDER BY run_timestamp DESC;


-- ============================================================================
-- TEAM GAME TYPES TABLE (newapi.team_gametypes)
-- ============================================================================

-- Drop existing functions/procedures for team_gametypes
DROP FUNCTION IF EXISTS upsert_team_gametypes_from_staging_with_logging() CASCADE;
DROP PROCEDURE IF EXISTS sync_team_gametypes_from_staging() CASCADE;

-- Create the production team_gametypes table
CREATE TABLE IF NOT EXISTS newapi.team_gametypes (
    id SERIAL PRIMARY KEY,
    season TEXT NOT NULL,
    "gameTypes" TEXT,
    "triCode" TEXT,
    "teamId" BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique natural key for team game types per season
    UNIQUE (season, "teamId")
);

CREATE INDEX IF NOT EXISTS idx_team_gametypes_natural_key
    ON newapi.team_gametypes (season, "teamId");

CREATE INDEX IF NOT EXISTS idx_team_gametypes_tri_code
    ON newapi.team_gametypes ("triCode");

-- Create ETL log table for team_gametypes
CREATE TABLE IF NOT EXISTS newapi.team_gametypes_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_team_gametypes_etl_log_timestamp
    ON newapi.team_gametypes_etl_log (run_timestamp);

-- Function: Upsert team_gametypes with logging
CREATE OR REPLACE FUNCTION upsert_team_gametypes_from_staging_with_logging()
RETURNS TABLE(
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL
) AS $$
DECLARE
    v_total INTEGER := 0;
    v_inserted INTEGER := 0;
    v_updated INTEGER := 0;
    v_unchanged INTEGER := 0;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN
    start_time := CURRENT_TIMESTAMP;

    WITH src AS (
        SELECT
            tg.season AS season,
            tg."gameTypes" AS "gameTypes",
            tg."triCode" AS "triCode",
            f.id AS "teamId"
        FROM staging1.team_gametypes tg
        LEFT JOIN newapi."franchises" f ON f."teamCommonName" = tg."triCode"
        WHERE NULLIF(tg.season::text, '') IS NOT NULL
    ), upsert AS (
        INSERT INTO newapi.team_gametypes (
            season, "gameTypes", "triCode", "teamId"
        )
        SELECT
            s.season, s."gameTypes", s."triCode", s."teamId"
        FROM src s
        ON CONFLICT (season, "teamId") DO UPDATE
        SET
            "gameTypes" = EXCLUDED."gameTypes",
            "triCode" = EXCLUDED."triCode",
            updated_at = CURRENT_TIMESTAMP
        WHERE (
            EXCLUDED."gameTypes" IS DISTINCT FROM newapi.team_gametypes."gameTypes"
            OR EXCLUDED."triCode" IS DISTINCT FROM newapi.team_gametypes."triCode"
        )
        RETURNING xmax
    ), counts AS (
        SELECT
            (SELECT COUNT(*) FROM src) AS total_src,
            COUNT(*) FILTER (WHERE xmax::text::bigint = 0) AS inserted_cnt,
            COUNT(*) FILTER (WHERE xmax::text::bigint <> 0) AS updated_cnt
        FROM upsert
    )
    SELECT total_src, inserted_cnt, updated_cnt
    INTO v_total, v_inserted, v_updated
    FROM counts;

    v_unchanged := COALESCE(v_total, 0) - COALESCE(v_inserted, 0) - COALESCE(v_updated, 0);

    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;

    INSERT INTO newapi.team_gametypes_etl_log (
        total_processed, inserted_records, updated_records, unchanged_records, run_duration
    ) VALUES (
        COALESCE(v_total, 0), COALESCE(v_inserted, 0), COALESCE(v_updated, 0), COALESCE(v_unchanged, 0), duration
    );

    RETURN QUERY SELECT v_total, v_inserted, v_updated, v_unchanged, duration;
END;
$$ LANGUAGE plpgsql;

-- Procedure to run the team_gametypes upsert
CREATE OR REPLACE PROCEDURE sync_team_gametypes_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM upsert_team_gametypes_from_staging_with_logging() INTO result_record;

    RAISE NOTICE 'Team gametypes sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  Inserted records: %', result_record.inserted_records;
    RAISE NOTICE '  Updated records: %', result_record.updated_records;
    RAISE NOTICE '  Unchanged records: %', result_record.unchanged_records;
END;
$$;

-- Summary view for team_gametypes ETL runs
CREATE OR REPLACE VIEW newapi.team_gametypes_etl_summary AS
SELECT 
    id,
    run_timestamp,
    total_processed,
    inserted_records,
    updated_records,
    unchanged_records,
    run_duration,
    ROUND(EXTRACT(EPOCH FROM run_duration)::NUMERIC, 2) AS duration_seconds,
    CASE 
        WHEN total_processed > 0 THEN ROUND((inserted_records + updated_records)::NUMERIC / total_processed * 100, 1)
        ELSE 0 
    END AS change_percentage
FROM newapi.team_gametypes_etl_log
ORDER BY run_timestamp DESC;


-- ============================================================================
-- TEAMS TABLE (newapi.teams)
-- ============================================================================

-- Drop existing functions/procedures for teams
DROP FUNCTION IF EXISTS upsert_teams_from_staging_with_logging() CASCADE;
DROP PROCEDURE IF EXISTS sync_teams_from_staging() CASCADE;

-- Create the production teams table
CREATE TABLE IF NOT EXISTS newapi.teams (
    id BIGINT PRIMARY KEY,
    "franchiseId" BIGINT,
    "fullName" TEXT,
    "leagueId" TEXT,
    "rawTricode" TEXT,
    "triCode" TEXT,
    "active" BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_teams_tri_code
    ON newapi.teams ("triCode");

CREATE INDEX IF NOT EXISTS idx_teams_franchise_id
    ON newapi.teams ("franchiseId");

-- Create ETL log table for teams
CREATE TABLE IF NOT EXISTS newapi.teams_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_teams_etl_log_timestamp
    ON newapi.teams_etl_log (run_timestamp);

-- Function: Upsert teams with logging
CREATE OR REPLACE FUNCTION upsert_teams_from_staging_with_logging()
RETURNS TABLE(
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL
) AS $$
DECLARE
    v_total INTEGER := 0;
    v_inserted INTEGER := 0;
    v_updated INTEGER := 0;
    v_unchanged INTEGER := 0;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN
    start_time := CURRENT_TIMESTAMP;

    WITH src AS (
        SELECT
            NULLIF(t.id::text, '')::BIGINT AS id,
            NULLIF(t."franchiseId"::text, '')::BIGINT AS "franchiseId",
            t."fullName" AS "fullName",
            t."leagueId" AS "leagueId",
            t."rawTricode" AS "rawTricode",
            t."triCode" AS "triCode"
        FROM staging1.teams t
        WHERE NULLIF(t.id::text, '')::BIGINT IS NOT NULL
    ), upsert AS (
        INSERT INTO newapi.teams (
            id, "franchiseId", "fullName", "leagueId", "rawTricode", "triCode"
        )
        SELECT
            s.id, s."franchiseId", s."fullName", s."leagueId", s."rawTricode", s."triCode"
        FROM src s
        ON CONFLICT (id) DO UPDATE
        SET
            "franchiseId" = EXCLUDED."franchiseId",
            "fullName" = EXCLUDED."fullName",
            "leagueId" = EXCLUDED."leagueId",
            "rawTricode" = EXCLUDED."rawTricode",
            "triCode" = EXCLUDED."triCode",
            updated_at = CURRENT_TIMESTAMP
        WHERE (
            EXCLUDED."franchiseId" IS DISTINCT FROM newapi.teams."franchiseId"
            OR EXCLUDED."fullName" IS DISTINCT FROM newapi.teams."fullName"
            OR EXCLUDED."leagueId" IS DISTINCT FROM newapi.teams."leagueId"
            OR EXCLUDED."rawTricode" IS DISTINCT FROM newapi.teams."rawTricode"
            OR EXCLUDED."triCode" IS DISTINCT FROM newapi.teams."triCode"
        )
        RETURNING xmax
    ), counts AS (
        SELECT
            (SELECT COUNT(*) FROM src) AS total_src,
            COUNT(*) FILTER (WHERE xmax::text::bigint = 0) AS inserted_cnt,
            COUNT(*) FILTER (WHERE xmax::text::bigint <> 0) AS updated_cnt
        FROM upsert
    )
    SELECT total_src, inserted_cnt, updated_cnt
    INTO v_total, v_inserted, v_updated
    FROM counts;

    v_unchanged := COALESCE(v_total, 0) - COALESCE(v_inserted, 0) - COALESCE(v_updated, 0);

    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;

    INSERT INTO newapi.teams_etl_log (
        total_processed, inserted_records, updated_records, unchanged_records, run_duration
    ) VALUES (
        COALESCE(v_total, 0), COALESCE(v_inserted, 0), COALESCE(v_updated, 0), COALESCE(v_unchanged, 0), duration
    );

    RETURN QUERY SELECT v_total, v_inserted, v_updated, v_unchanged, duration;
END;
$$ LANGUAGE plpgsql;

-- Procedure to run the teams upsert
CREATE OR REPLACE PROCEDURE sync_teams_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM upsert_teams_from_staging_with_logging() INTO result_record;

    RAISE NOTICE 'Teams sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  Inserted records: %', result_record.inserted_records;
    RAISE NOTICE '  Updated records: %', result_record.updated_records;
    RAISE NOTICE '  Unchanged records: %', result_record.unchanged_records;
END;
$$;

-- Summary view for teams ETL runs
CREATE OR REPLACE VIEW newapi.teams_etl_summary AS
SELECT 
    id,
    run_timestamp,
    total_processed,
    inserted_records,
    updated_records,
    unchanged_records,
    run_duration,
    ROUND(EXTRACT(EPOCH FROM run_duration)::NUMERIC, 2) AS duration_seconds,
    CASE 
        WHEN total_processed > 0 THEN ROUND((inserted_records + updated_records)::NUMERIC / total_processed * 100, 1)
        ELSE 0 
    END AS change_percentage
FROM newapi.teams_etl_log
ORDER BY run_timestamp DESC;


-- ============================================================================
-- MASTER SYNC PROCEDURE
-- ============================================================================

-- Create a master procedure to sync all team-related tables
CREATE OR REPLACE PROCEDURE sync_all_teams_from_staging()
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'Starting team data synchronization...';
    RAISE NOTICE '';
    
    RAISE NOTICE '--- Syncing teams ---';
    CALL sync_teams_from_staging();
    RAISE NOTICE '';
    
    RAISE NOTICE '--- Syncing franchises ---';
    CALL sync_franchises_from_staging();
    RAISE NOTICE '';
    
    RAISE NOTICE '--- Syncing team season stats ---';
    CALL sync_team_season_from_staging();
    RAISE NOTICE '';
    
    RAISE NOTICE '--- Syncing team game types ---';
    CALL sync_team_gametypes_from_staging();
    RAISE NOTICE '';
    
    RAISE NOTICE 'All team data synchronization completed successfully!';
END;
$$;

-- Optional: Execute all syncs immediately
-- CALL sync_all_teams_from_staging();
