-- Ensure the schema exists
CREATE SCHEMA IF NOT EXISTS newapi;
CREATE SCHEMA IF NOT EXISTS staging1;

-- Drop existing functions/procedures for games
DROP FUNCTION IF EXISTS upsert_games_from_staging_with_logging() CASCADE;
DROP PROCEDURE IF EXISTS sync_games_from_staging() CASCADE;

-- Create the production games table
CREATE TABLE IF NOT EXISTS newapi.games (
    id BIGINT PRIMARY KEY,
    season INTEGER,
    "gameType" INTEGER,
    "gameDate" DATE,
    "gameState" TEXT,
    "gameScheduleState" TEXT,
    "startTimeUTC" TIMESTAMP WITH TIME ZONE,
    "venueTimezone" TEXT,
    "venueUTCOffset" TEXT,
    "easternUTCOffset" TEXT,
    "neutralSite" BOOLEAN,
    "venue_default" TEXT,
    "tvBroadcasts" TEXT,
    "gameCenterLink" TEXT,
    "threeMinRecap" TEXT,
    "threeMinRecapFr" TEXT,
    "condensedGame" TEXT,
    "condensedGameFr" TEXT,
    -- Away team columns
    "awayTeam_id" INTEGER,
    "awayTeam_abbrev" TEXT,
    "awayTeam_score" INTEGER,
    "awayTeam_commonName_default" TEXT,
    "awayTeam_placeName_default" TEXT,
    "awayTeam_logo" TEXT,
    "awayTeam_darkLogo" TEXT,
    -- Home team columns
    "homeTeam_id" INTEGER,
    "homeTeam_abbrev" TEXT,
    "homeTeam_score" INTEGER,
    "homeTeam_commonName_default" TEXT,
    "homeTeam_placeName_default" TEXT,
    "homeTeam_logo" TEXT,
    "homeTeam_darkLogo" TEXT,
    -- Period/outcome columns
    "periodDescriptor_periodType" TEXT,
    "periodDescriptor_maxRegulationPeriods" INTEGER,
    "gameOutcome_lastPeriodType" TEXT,
    -- Winner columns
    "winningGoalie_playerId" BIGINT,
    "winningGoalie_firstInitial_default" TEXT,
    "winningGoalie_lastName_default" TEXT,
    "winningGoalScorer_playerId" BIGINT,
    "winningGoalScorer_firstInitial_default" TEXT,
    "winningGoalScorer_lastName_default" TEXT,
    -- Tickets/links
    "ticketsLink" TEXT,
    "ticketsLinkFr" TEXT,
    "specialEvent" TEXT,
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

-- Add missing columns to staging table (pandas may not create all columns)
DO $$
BEGIN
    -- Core columns
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'specialEvent') THEN
        ALTER TABLE staging1.games ADD COLUMN "specialEvent" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'ticketsLink') THEN
        ALTER TABLE staging1.games ADD COLUMN "ticketsLink" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'ticketsLinkFr') THEN
        ALTER TABLE staging1.games ADD COLUMN "ticketsLinkFr" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'threeMinRecap') THEN
        ALTER TABLE staging1.games ADD COLUMN "threeMinRecap" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'threeMinRecapFr') THEN
        ALTER TABLE staging1.games ADD COLUMN "threeMinRecapFr" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'condensedGame') THEN
        ALTER TABLE staging1.games ADD COLUMN "condensedGame" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'condensedGameFr') THEN
        ALTER TABLE staging1.games ADD COLUMN "condensedGameFr" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'gameCenterLink') THEN
        ALTER TABLE staging1.games ADD COLUMN "gameCenterLink" TEXT;
    END IF;
    -- Winner columns (only present for finished games)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'winningGoalie_playerId') THEN
        ALTER TABLE staging1.games ADD COLUMN "winningGoalie_playerId" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'winningGoalie_firstInitial_default') THEN
        ALTER TABLE staging1.games ADD COLUMN "winningGoalie_firstInitial_default" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'winningGoalie_lastName_default') THEN
        ALTER TABLE staging1.games ADD COLUMN "winningGoalie_lastName_default" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'winningGoalScorer_playerId') THEN
        ALTER TABLE staging1.games ADD COLUMN "winningGoalScorer_playerId" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'winningGoalScorer_firstInitial_default') THEN
        ALTER TABLE staging1.games ADD COLUMN "winningGoalScorer_firstInitial_default" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'winningGoalScorer_lastName_default') THEN
        ALTER TABLE staging1.games ADD COLUMN "winningGoalScorer_lastName_default" TEXT;
    END IF;
    -- Outcome columns (only present for finished games)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'gameOutcome_lastPeriodType') THEN
        ALTER TABLE staging1.games ADD COLUMN "gameOutcome_lastPeriodType" TEXT;
    END IF;
    -- Team columns that may vary
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'awayTeam_logo') THEN
        ALTER TABLE staging1.games ADD COLUMN "awayTeam_logo" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'awayTeam_darkLogo') THEN
        ALTER TABLE staging1.games ADD COLUMN "awayTeam_darkLogo" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'homeTeam_logo') THEN
        ALTER TABLE staging1.games ADD COLUMN "homeTeam_logo" TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'staging1' AND table_name = 'games' AND column_name = 'homeTeam_darkLogo') THEN
        ALTER TABLE staging1.games ADD COLUMN "homeTeam_darkLogo" TEXT;
    END IF;
EXCEPTION
    WHEN undefined_table THEN
        RAISE NOTICE 'staging1.games table does not exist yet - will be created by pandas';
END;
$$;

-- Function: Upsert games with logging
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
            g."venueUTCOffset" AS "venueUTCOffset",
            g."easternUTCOffset" AS "easternUTCOffset",
            g."neutralSite"::BOOLEAN AS "neutralSite",
            g."venue_default" AS "venue_default",
            g."tvBroadcasts" AS "tvBroadcasts",
            g."gameCenterLink" AS "gameCenterLink",
            g."threeMinRecap" AS "threeMinRecap",
            g."threeMinRecapFr" AS "threeMinRecapFr",
            g."condensedGame" AS "condensedGame",
            g."condensedGameFr" AS "condensedGameFr",
            -- Away team
            NULLIF(g."awayTeam_id"::text, '')::INTEGER AS "awayTeam_id",
            g."awayTeam_abbrev" AS "awayTeam_abbrev",
            NULLIF(g."awayTeam_score"::text, '')::INTEGER AS "awayTeam_score",
            g."awayTeam_commonName_default" AS "awayTeam_commonName_default",
            g."awayTeam_placeName_default" AS "awayTeam_placeName_default",
            g."awayTeam_logo" AS "awayTeam_logo",
            g."awayTeam_darkLogo" AS "awayTeam_darkLogo",
            -- Home team
            NULLIF(g."homeTeam_id"::text, '')::INTEGER AS "homeTeam_id",
            g."homeTeam_abbrev" AS "homeTeam_abbrev",
            NULLIF(g."homeTeam_score"::text, '')::INTEGER AS "homeTeam_score",
            g."homeTeam_commonName_default" AS "homeTeam_commonName_default",
            g."homeTeam_placeName_default" AS "homeTeam_placeName_default",
            g."homeTeam_logo" AS "homeTeam_logo",
            g."homeTeam_darkLogo" AS "homeTeam_darkLogo",
            -- Period/outcome
            g."periodDescriptor_periodType" AS "periodDescriptor_periodType",
            NULLIF(g."periodDescriptor_maxRegulationPeriods"::text, '')::INTEGER AS "periodDescriptor_maxRegulationPeriods",
            g."gameOutcome_lastPeriodType" AS "gameOutcome_lastPeriodType",
            -- Winners
            NULLIF(g."winningGoalie_playerId"::text, '')::BIGINT AS "winningGoalie_playerId",
            g."winningGoalie_firstInitial_default" AS "winningGoalie_firstInitial_default",
            g."winningGoalie_lastName_default" AS "winningGoalie_lastName_default",
            NULLIF(g."winningGoalScorer_playerId"::text, '')::BIGINT AS "winningGoalScorer_playerId",
            g."winningGoalScorer_firstInitial_default" AS "winningGoalScorer_firstInitial_default",
            g."winningGoalScorer_lastName_default" AS "winningGoalScorer_lastName_default",
            -- Tickets/links
            g."ticketsLink" AS "ticketsLink",
            g."ticketsLinkFr" AS "ticketsLinkFr",
            g."specialEvent" AS "specialEvent"
        FROM staging1.games g
        WHERE NULLIF(g.id::text, '')::BIGINT IS NOT NULL
    ), upsert AS (
        INSERT INTO newapi.games (
            id, season, "gameType", "gameDate", "gameState", "gameScheduleState",
            "startTimeUTC", "venueTimezone", "venueUTCOffset", "easternUTCOffset",
            "neutralSite", "venue_default", "tvBroadcasts", "gameCenterLink",
            "threeMinRecap", "threeMinRecapFr", "condensedGame", "condensedGameFr",
            "awayTeam_id", "awayTeam_abbrev", "awayTeam_score",
            "awayTeam_commonName_default", "awayTeam_placeName_default",
            "awayTeam_logo", "awayTeam_darkLogo",
            "homeTeam_id", "homeTeam_abbrev", "homeTeam_score",
            "homeTeam_commonName_default", "homeTeam_placeName_default",
            "homeTeam_logo", "homeTeam_darkLogo",
            "periodDescriptor_periodType", "periodDescriptor_maxRegulationPeriods",
            "gameOutcome_lastPeriodType",
            "winningGoalie_playerId", "winningGoalie_firstInitial_default", "winningGoalie_lastName_default",
            "winningGoalScorer_playerId", "winningGoalScorer_firstInitial_default", "winningGoalScorer_lastName_default",
            "ticketsLink", "ticketsLinkFr", "specialEvent"
        )
        SELECT
            s.id, s.season, s."gameType", s."gameDate", s."gameState", s."gameScheduleState",
            s."startTimeUTC", s."venueTimezone", s."venueUTCOffset", s."easternUTCOffset",
            s."neutralSite", s."venue_default", s."tvBroadcasts", s."gameCenterLink",
            s."threeMinRecap", s."threeMinRecapFr", s."condensedGame", s."condensedGameFr",
            s."awayTeam_id", s."awayTeam_abbrev", s."awayTeam_score",
            s."awayTeam_commonName_default", s."awayTeam_placeName_default",
            s."awayTeam_logo", s."awayTeam_darkLogo",
            s."homeTeam_id", s."homeTeam_abbrev", s."homeTeam_score",
            s."homeTeam_commonName_default", s."homeTeam_placeName_default",
            s."homeTeam_logo", s."homeTeam_darkLogo",
            s."periodDescriptor_periodType", s."periodDescriptor_maxRegulationPeriods",
            s."gameOutcome_lastPeriodType",
            s."winningGoalie_playerId", s."winningGoalie_firstInitial_default", s."winningGoalie_lastName_default",
            s."winningGoalScorer_playerId", s."winningGoalScorer_firstInitial_default", s."winningGoalScorer_lastName_default",
            s."ticketsLink", s."ticketsLinkFr", s."specialEvent"
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
            "winningGoalie_firstInitial_default" = EXCLUDED."winningGoalie_firstInitial_default",
            "winningGoalie_lastName_default" = EXCLUDED."winningGoalie_lastName_default",
            "winningGoalScorer_playerId" = EXCLUDED."winningGoalScorer_playerId",
            "winningGoalScorer_firstInitial_default" = EXCLUDED."winningGoalScorer_firstInitial_default",
            "winningGoalScorer_lastName_default" = EXCLUDED."winningGoalScorer_lastName_default",
            "threeMinRecap" = EXCLUDED."threeMinRecap",
            "threeMinRecapFr" = EXCLUDED."threeMinRecapFr",
            "condensedGame" = EXCLUDED."condensedGame",
            "condensedGameFr" = EXCLUDED."condensedGameFr",
            "gameCenterLink" = EXCLUDED."gameCenterLink",
            "tvBroadcasts" = EXCLUDED."tvBroadcasts",
            "ticketsLink" = EXCLUDED."ticketsLink",
            "ticketsLinkFr" = EXCLUDED."ticketsLinkFr",
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
            OR EXCLUDED."threeMinRecap" IS DISTINCT FROM newapi.games."threeMinRecap"
            OR EXCLUDED."condensedGame" IS DISTINCT FROM newapi.games."condensedGame"
            OR EXCLUDED."gameCenterLink" IS DISTINCT FROM newapi.games."gameCenterLink"
            OR EXCLUDED."tvBroadcasts" IS DISTINCT FROM newapi.games."tvBroadcasts"
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
