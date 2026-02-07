-- Ensure the schema exists
CREATE SCHEMA IF NOT EXISTS newapi;
CREATE SCHEMA IF NOT EXISTS staging1;

-- Drop existing functions/procedures for games
DROP FUNCTION IF EXISTS upsert_games_from_staging_with_logging() CASCADE;
DROP PROCEDURE IF EXISTS sync_games_from_staging() CASCADE;

-- Create the production games table (simplified)
CREATE TABLE IF NOT EXISTS newapi.games (
    id BIGINT PRIMARY KEY,
    season INTEGER,
    "gameType" INTEGER,
    "gameDate" DATE,
    "gameState" TEXT,
    "gameScheduleState" TEXT,
    "startTimeUTC" TIMESTAMP WITH TIME ZONE,
    "venueTimezone" TEXT,
    "neutralSite" BOOLEAN,
    "venue_default" TEXT,
    "tvBroadcasts" TEXT,
    -- Away team columns
    "awayTeam_id" INTEGER,
    "awayTeam_abbrev" TEXT,
    "awayTeam_score" INTEGER,
    -- Home team columns
    "homeTeam_id" INTEGER,
    "homeTeam_abbrev" TEXT,
    "homeTeam_score" INTEGER,
    -- Period/outcome columns
    "periodDescriptor_periodType" TEXT,
    "gameOutcome_lastPeriodType" TEXT,
    -- Winner columns
    "winningGoalie_playerId" BIGINT,
    "winningGoalScorer_playerId" BIGINT,
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_games_game_date
    ON newapi.games ("gameDate");

CREATE INDEX IF NOT EXISTS idx_games_season
    ON newapi.games (season);

CREATE INDEX IF NOT EXISTS idx_games_away_team
    ON newapi.games ("awayTeam_id");

CREATE INDEX IF NOT EXISTS idx_games_home_team
    ON newapi.games ("homeTeam_id");

CREATE INDEX IF NOT EXISTS idx_games_game_state
    ON newapi.games ("gameState");

CREATE INDEX IF NOT EXISTS idx_games_season_type
    ON newapi.games (season, "gameType");

-- Create ETL log table for games
CREATE TABLE IF NOT EXISTS newapi.games_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_games_etl_log_timestamp
    ON newapi.games_etl_log (run_timestamp);

-- Function: Upsert games with logging (simplified - only essential columns)
CREATE OR REPLACE FUNCTION upsert_games_from_staging_with_logging()
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
            NULLIF(g.id::text, '')::BIGINT AS id,
            NULLIF(g.season::text, '')::INTEGER AS season,
            NULLIF(g."gameType"::text, '')::INTEGER AS "gameType",
            NULLIF(g."gameDate"::text, '')::DATE AS "gameDate",
            g."gameState" AS "gameState",
            g."gameScheduleState" AS "gameScheduleState",
            NULLIF(g."startTimeUTC"::text, '')::TIMESTAMP WITH TIME ZONE AS "startTimeUTC",
            g."venueTimezone" AS "venueTimezone",
            g."neutralSite"::BOOLEAN AS "neutralSite",
            g."venue_default" AS "venue_default",
            g."tvBroadcasts" AS "tvBroadcasts",
            -- Away team
            NULLIF(g."awayTeam_id"::text, '')::INTEGER AS "awayTeam_id",
            g."awayTeam_abbrev" AS "awayTeam_abbrev",
            NULLIF(g."awayTeam_score"::text, '')::INTEGER AS "awayTeam_score",
            -- Home team
            NULLIF(g."homeTeam_id"::text, '')::INTEGER AS "homeTeam_id",
            g."homeTeam_abbrev" AS "homeTeam_abbrev",
            NULLIF(g."homeTeam_score"::text, '')::INTEGER AS "homeTeam_score",
            -- Period/outcome
            g."periodDescriptor_periodType" AS "periodDescriptor_periodType",
            g."gameOutcome_lastPeriodType" AS "gameOutcome_lastPeriodType",
            -- Winners
            NULLIF(g."winningGoalie_playerId"::text, '')::BIGINT AS "winningGoalie_playerId",
            NULLIF(g."winningGoalScorer_playerId"::text, '')::BIGINT AS "winningGoalScorer_playerId"
        FROM staging1.games g
        WHERE NULLIF(g.id::text, '')::BIGINT IS NOT NULL
    ), upsert AS (
        INSERT INTO newapi.games (
            id, season, "gameType", "gameDate", "gameState", "gameScheduleState",
            "startTimeUTC", "venueTimezone", "neutralSite", "venue_default", "tvBroadcasts",
            "awayTeam_id", "awayTeam_abbrev", "awayTeam_score",
            "homeTeam_id", "homeTeam_abbrev", "homeTeam_score",
            "periodDescriptor_periodType", "gameOutcome_lastPeriodType",
            "winningGoalie_playerId", "winningGoalScorer_playerId"
        )
        SELECT
            s.id, s.season, s."gameType", s."gameDate", s."gameState", s."gameScheduleState",
            s."startTimeUTC", s."venueTimezone", s."neutralSite", s."venue_default", s."tvBroadcasts",
            s."awayTeam_id", s."awayTeam_abbrev", s."awayTeam_score",
            s."homeTeam_id", s."homeTeam_abbrev", s."homeTeam_score",
            s."periodDescriptor_periodType", s."gameOutcome_lastPeriodType",
            s."winningGoalie_playerId", s."winningGoalScorer_playerId"
        FROM src s
        ON CONFLICT (id) DO UPDATE
        SET
            "gameState" = EXCLUDED."gameState",
            "gameScheduleState" = EXCLUDED."gameScheduleState",
            "awayTeam_score" = EXCLUDED."awayTeam_score",
            "homeTeam_score" = EXCLUDED."homeTeam_score",
            "periodDescriptor_periodType" = EXCLUDED."periodDescriptor_periodType",
            "gameOutcome_lastPeriodType" = EXCLUDED."gameOutcome_lastPeriodType",
            "winningGoalie_playerId" = EXCLUDED."winningGoalie_playerId",
            "winningGoalScorer_playerId" = EXCLUDED."winningGoalScorer_playerId",
            "tvBroadcasts" = EXCLUDED."tvBroadcasts",
            updated_at = CURRENT_TIMESTAMP
        WHERE (
            EXCLUDED."gameState" IS DISTINCT FROM newapi.games."gameState"
            OR EXCLUDED."gameScheduleState" IS DISTINCT FROM newapi.games."gameScheduleState"
            OR EXCLUDED."awayTeam_score" IS DISTINCT FROM newapi.games."awayTeam_score"
            OR EXCLUDED."homeTeam_score" IS DISTINCT FROM newapi.games."homeTeam_score"
            OR EXCLUDED."periodDescriptor_periodType" IS DISTINCT FROM newapi.games."periodDescriptor_periodType"
            OR EXCLUDED."gameOutcome_lastPeriodType" IS DISTINCT FROM newapi.games."gameOutcome_lastPeriodType"
            OR EXCLUDED."winningGoalie_playerId" IS DISTINCT FROM newapi.games."winningGoalie_playerId"
            OR EXCLUDED."winningGoalScorer_playerId" IS DISTINCT FROM newapi.games."winningGoalScorer_playerId"
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

    INSERT INTO newapi.games_etl_log (
        total_processed, inserted_records, updated_records, unchanged_records, run_duration
    ) VALUES (
        COALESCE(v_total, 0), COALESCE(v_inserted, 0), COALESCE(v_updated, 0), COALESCE(v_unchanged, 0), duration
    );

    RETURN QUERY SELECT v_total, v_inserted, v_updated, v_unchanged, duration;
END;
$$ LANGUAGE plpgsql;

-- Procedure to run the games upsert
CREATE OR REPLACE PROCEDURE sync_games_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM upsert_games_from_staging_with_logging() INTO result_record;

    RAISE NOTICE 'Games sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  Inserted records: %', result_record.inserted_records;
    RAISE NOTICE '  Updated records: %', result_record.updated_records;
    RAISE NOTICE '  Unchanged records: %', result_record.unchanged_records;
END;
$$;

-- Summary view for games ETL runs
CREATE OR REPLACE VIEW newapi.games_etl_summary AS
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
FROM newapi.games_etl_log
ORDER BY run_timestamp DESC;

-- Helper view: Get games for a specific date with team info
CREATE OR REPLACE VIEW newapi.games_by_date AS
SELECT
    g.id,
    g."gameDate",
    g."startTimeUTC",
    g."gameState",
    g."awayTeam_abbrev" AS away_team,
    g."awayTeam_score" AS away_score,
    g."homeTeam_abbrev" AS home_team,
    g."homeTeam_score" AS home_score,
    g."venue_default" AS venue,
    g."gameOutcome_lastPeriodType" AS outcome_period,
    g."tvBroadcasts"
FROM newapi.games g
ORDER BY g."gameDate", g."startTimeUTC";

-- Optional: Execute sync immediately
-- CALL sync_games_from_staging();
