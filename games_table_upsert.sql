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
    "easternUTCOffset" TEXT,
    "venueUTCOffset" TEXT,
    "venueTimezone" TEXT,
    "neutralSite" BOOLEAN,
    "venue_default" TEXT,
    "venueLocation_default" TEXT,
    "tvBroadcasts" TEXT,
    "limitedScoring" BOOLEAN,
    "shootoutInUse" BOOLEAN,
    "regPeriods" INTEGER,
    "otInUse" BOOLEAN,
    "tiesInUse" BOOLEAN,
    -- Away team columns
    "awayTeam_id" INTEGER,
    "awayTeam_abbrev" TEXT,
    "awayTeam_commonName_default" TEXT,
    "awayTeam_placeName_default" TEXT,
    "awayTeam_score" INTEGER,
    "awayTeam_sog" INTEGER,
    "awayTeam_logo" TEXT,
    "awayTeam_darkLogo" TEXT,
    -- Home team columns
    "homeTeam_id" INTEGER,
    "homeTeam_abbrev" TEXT,
    "homeTeam_commonName_default" TEXT,
    "homeTeam_placeName_default" TEXT,
    "homeTeam_score" INTEGER,
    "homeTeam_sog" INTEGER,
    "homeTeam_logo" TEXT,
    "homeTeam_darkLogo" TEXT,
    -- Period/outcome columns
    "periodDescriptor_number" INTEGER,
    "periodDescriptor_periodType" TEXT,
    "periodDescriptor_otPeriods" INTEGER,
    "periodDescriptor_maxRegulationPeriods" INTEGER,
    "gameOutcome_lastPeriodType" TEXT,
    -- Winner columns
    "winningGoalie_playerId" BIGINT,
    "winningGoalScorer_playerId" BIGINT,
    -- Clock columns
    "clock_timeRemaining" TEXT,
    "clock_secondsRemaining" INTEGER,
    "clock_running" BOOLEAN,
    "clock_inIntermission" BOOLEAN,
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add landing-enrichment columns when the table already existed.
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "easternUTCOffset" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "venueUTCOffset" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "venueLocation_default" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "limitedScoring" BOOLEAN;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "shootoutInUse" BOOLEAN;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "regPeriods" INTEGER;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "otInUse" BOOLEAN;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "tiesInUse" BOOLEAN;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "awayTeam_commonName_default" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "awayTeam_placeName_default" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "awayTeam_sog" INTEGER;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "awayTeam_logo" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "awayTeam_darkLogo" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "homeTeam_commonName_default" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "homeTeam_placeName_default" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "homeTeam_sog" INTEGER;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "homeTeam_logo" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "homeTeam_darkLogo" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "periodDescriptor_number" INTEGER;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "periodDescriptor_otPeriods" INTEGER;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "periodDescriptor_maxRegulationPeriods" INTEGER;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "clock_timeRemaining" TEXT;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "clock_secondsRemaining" INTEGER;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "clock_running" BOOLEAN;
ALTER TABLE newapi.games ADD COLUMN IF NOT EXISTS "clock_inIntermission" BOOLEAN;

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

CREATE TABLE IF NOT EXISTS newapi.game_goals (
    game_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    season INTEGER,
    game_type INTEGER,
    game_date DATE,
    away_team_abbrev TEXT,
    home_team_abbrev TEXT,
    period_number INTEGER,
    period_type TEXT,
    time_in_period TEXT,
    team_abbrev TEXT,
    is_home BOOLEAN,
    strength TEXT,
    situation_code TEXT,
    scoring_player_id BIGINT,
    scoring_player_name TEXT,
    assist1_player_id BIGINT,
    assist1_player_name TEXT,
    assist2_player_id BIGINT,
    assist2_player_name TEXT,
    away_score INTEGER,
    home_score INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_id, event_id)
);

CREATE TABLE IF NOT EXISTS newapi.game_penalties (
    game_id BIGINT NOT NULL,
    penalty_index INTEGER NOT NULL,
    season INTEGER,
    game_type INTEGER,
    game_date DATE,
    away_team_abbrev TEXT,
    home_team_abbrev TEXT,
    period_number INTEGER,
    period_type TEXT,
    time_in_period TEXT,
    team_abbrev TEXT,
    penalty_type TEXT,
    duration INTEGER,
    desc_key TEXT,
    committed_by_player_name TEXT,
    committed_by_sweater_number INTEGER,
    drawn_by_player_name TEXT,
    drawn_by_sweater_number INTEGER,
    served_by_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_id, period_number, penalty_index)
);

CREATE TABLE IF NOT EXISTS newapi.game_three_stars (
    game_id BIGINT NOT NULL,
    star INTEGER NOT NULL,
    season INTEGER,
    game_type INTEGER,
    game_date DATE,
    player_id BIGINT,
    player_name TEXT,
    team_abbrev TEXT,
    sweater_number INTEGER,
    position TEXT,
    goals INTEGER,
    assists INTEGER,
    points INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_id, star)
);

CREATE INDEX IF NOT EXISTS idx_game_goals_player
    ON newapi.game_goals (scoring_player_id);

CREATE INDEX IF NOT EXISTS idx_game_goals_game_period
    ON newapi.game_goals (game_id, period_number);

CREATE INDEX IF NOT EXISTS idx_game_penalties_game_period
    ON newapi.game_penalties (game_id, period_number);

CREATE INDEX IF NOT EXISTS idx_game_three_stars_player
    ON newapi.game_three_stars (player_id);

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
            g."easternUTCOffset" AS "easternUTCOffset",
            g."venueUTCOffset" AS "venueUTCOffset",
            g."venueTimezone" AS "venueTimezone",
            g."neutralSite"::BOOLEAN AS "neutralSite",
            g."venue_default" AS "venue_default",
            g."venueLocation_default" AS "venueLocation_default",
            g."tvBroadcasts" AS "tvBroadcasts",
            NULLIF(g."limitedScoring"::text, '')::BOOLEAN AS "limitedScoring",
            NULLIF(g."shootoutInUse"::text, '')::BOOLEAN AS "shootoutInUse",
            NULLIF(g."regPeriods"::text, '')::INTEGER AS "regPeriods",
            NULLIF(g."otInUse"::text, '')::BOOLEAN AS "otInUse",
            NULLIF(g."tiesInUse"::text, '')::BOOLEAN AS "tiesInUse",
            -- Away team
            NULLIF(g."awayTeam_id"::text, '')::INTEGER AS "awayTeam_id",
            g."awayTeam_abbrev" AS "awayTeam_abbrev",
            g."awayTeam_commonName_default" AS "awayTeam_commonName_default",
            g."awayTeam_placeName_default" AS "awayTeam_placeName_default",
            NULLIF(g."awayTeam_score"::text, '')::INTEGER AS "awayTeam_score",
            NULLIF(g."awayTeam_sog"::text, '')::INTEGER AS "awayTeam_sog",
            g."awayTeam_logo" AS "awayTeam_logo",
            g."awayTeam_darkLogo" AS "awayTeam_darkLogo",
            -- Home team
            NULLIF(g."homeTeam_id"::text, '')::INTEGER AS "homeTeam_id",
            g."homeTeam_abbrev" AS "homeTeam_abbrev",
            g."homeTeam_commonName_default" AS "homeTeam_commonName_default",
            g."homeTeam_placeName_default" AS "homeTeam_placeName_default",
            NULLIF(g."homeTeam_score"::text, '')::INTEGER AS "homeTeam_score",
            NULLIF(g."homeTeam_sog"::text, '')::INTEGER AS "homeTeam_sog",
            g."homeTeam_logo" AS "homeTeam_logo",
            g."homeTeam_darkLogo" AS "homeTeam_darkLogo",
            -- Period/outcome
            NULLIF(g."periodDescriptor_number"::text, '')::INTEGER AS "periodDescriptor_number",
            g."periodDescriptor_periodType" AS "periodDescriptor_periodType",
            NULLIF(g."periodDescriptor_otPeriods"::text, '')::INTEGER AS "periodDescriptor_otPeriods",
            NULLIF(g."periodDescriptor_maxRegulationPeriods"::text, '')::INTEGER AS "periodDescriptor_maxRegulationPeriods",
            g."gameOutcome_lastPeriodType" AS "gameOutcome_lastPeriodType",
            -- Winners
            NULLIF(g."winningGoalie_playerId"::text, '')::BIGINT AS "winningGoalie_playerId",
            NULLIF(g."winningGoalScorer_playerId"::text, '')::BIGINT AS "winningGoalScorer_playerId",
            -- Clock
            g."clock_timeRemaining" AS "clock_timeRemaining",
            NULLIF(g."clock_secondsRemaining"::text, '')::INTEGER AS "clock_secondsRemaining",
            NULLIF(g."clock_running"::text, '')::BOOLEAN AS "clock_running",
            NULLIF(g."clock_inIntermission"::text, '')::BOOLEAN AS "clock_inIntermission"
        FROM staging1.games g
        WHERE NULLIF(g.id::text, '')::BIGINT IS NOT NULL
    ), upsert AS (
        INSERT INTO newapi.games (
            id, season, "gameType", "gameDate", "gameState", "gameScheduleState",
            "startTimeUTC", "easternUTCOffset", "venueUTCOffset", "venueTimezone",
            "neutralSite", "venue_default", "venueLocation_default", "tvBroadcasts",
            "limitedScoring", "shootoutInUse", "regPeriods", "otInUse", "tiesInUse",
            "awayTeam_id", "awayTeam_abbrev", "awayTeam_commonName_default",
            "awayTeam_placeName_default", "awayTeam_score", "awayTeam_sog",
            "awayTeam_logo", "awayTeam_darkLogo",
            "homeTeam_id", "homeTeam_abbrev", "homeTeam_commonName_default",
            "homeTeam_placeName_default", "homeTeam_score", "homeTeam_sog",
            "homeTeam_logo", "homeTeam_darkLogo",
            "periodDescriptor_number", "periodDescriptor_periodType",
            "periodDescriptor_otPeriods", "periodDescriptor_maxRegulationPeriods",
            "gameOutcome_lastPeriodType", "winningGoalie_playerId",
            "winningGoalScorer_playerId", "clock_timeRemaining",
            "clock_secondsRemaining", "clock_running", "clock_inIntermission"
        )
        SELECT
            s.id, s.season, s."gameType", s."gameDate", s."gameState", s."gameScheduleState",
            s."startTimeUTC", s."easternUTCOffset", s."venueUTCOffset", s."venueTimezone",
            s."neutralSite", s."venue_default", s."venueLocation_default", s."tvBroadcasts",
            s."limitedScoring", s."shootoutInUse", s."regPeriods", s."otInUse", s."tiesInUse",
            s."awayTeam_id", s."awayTeam_abbrev", s."awayTeam_commonName_default",
            s."awayTeam_placeName_default", s."awayTeam_score", s."awayTeam_sog",
            s."awayTeam_logo", s."awayTeam_darkLogo",
            s."homeTeam_id", s."homeTeam_abbrev", s."homeTeam_commonName_default",
            s."homeTeam_placeName_default", s."homeTeam_score", s."homeTeam_sog",
            s."homeTeam_logo", s."homeTeam_darkLogo",
            s."periodDescriptor_number", s."periodDescriptor_periodType",
            s."periodDescriptor_otPeriods", s."periodDescriptor_maxRegulationPeriods",
            s."gameOutcome_lastPeriodType", s."winningGoalie_playerId",
            s."winningGoalScorer_playerId", s."clock_timeRemaining",
            s."clock_secondsRemaining", s."clock_running", s."clock_inIntermission"
        FROM src s
        ON CONFLICT (id) DO UPDATE
        SET
            "gameState" = EXCLUDED."gameState",
            "gameScheduleState" = EXCLUDED."gameScheduleState",
            "startTimeUTC" = EXCLUDED."startTimeUTC",
            "easternUTCOffset" = EXCLUDED."easternUTCOffset",
            "venueUTCOffset" = EXCLUDED."venueUTCOffset",
            "venueTimezone" = EXCLUDED."venueTimezone",
            "neutralSite" = EXCLUDED."neutralSite",
            "venue_default" = EXCLUDED."venue_default",
            "venueLocation_default" = EXCLUDED."venueLocation_default",
            "limitedScoring" = EXCLUDED."limitedScoring",
            "shootoutInUse" = EXCLUDED."shootoutInUse",
            "regPeriods" = EXCLUDED."regPeriods",
            "otInUse" = EXCLUDED."otInUse",
            "tiesInUse" = EXCLUDED."tiesInUse",
            "awayTeam_id" = EXCLUDED."awayTeam_id",
            "awayTeam_abbrev" = EXCLUDED."awayTeam_abbrev",
            "awayTeam_commonName_default" = EXCLUDED."awayTeam_commonName_default",
            "awayTeam_placeName_default" = EXCLUDED."awayTeam_placeName_default",
            "awayTeam_score" = EXCLUDED."awayTeam_score",
            "awayTeam_sog" = EXCLUDED."awayTeam_sog",
            "awayTeam_logo" = EXCLUDED."awayTeam_logo",
            "awayTeam_darkLogo" = EXCLUDED."awayTeam_darkLogo",
            "homeTeam_id" = EXCLUDED."homeTeam_id",
            "homeTeam_abbrev" = EXCLUDED."homeTeam_abbrev",
            "homeTeam_commonName_default" = EXCLUDED."homeTeam_commonName_default",
            "homeTeam_placeName_default" = EXCLUDED."homeTeam_placeName_default",
            "homeTeam_score" = EXCLUDED."homeTeam_score",
            "homeTeam_sog" = EXCLUDED."homeTeam_sog",
            "homeTeam_logo" = EXCLUDED."homeTeam_logo",
            "homeTeam_darkLogo" = EXCLUDED."homeTeam_darkLogo",
            "periodDescriptor_number" = EXCLUDED."periodDescriptor_number",
            "periodDescriptor_periodType" = EXCLUDED."periodDescriptor_periodType",
            "periodDescriptor_otPeriods" = EXCLUDED."periodDescriptor_otPeriods",
            "periodDescriptor_maxRegulationPeriods" = EXCLUDED."periodDescriptor_maxRegulationPeriods",
            "gameOutcome_lastPeriodType" = EXCLUDED."gameOutcome_lastPeriodType",
            "winningGoalie_playerId" = EXCLUDED."winningGoalie_playerId",
            "winningGoalScorer_playerId" = EXCLUDED."winningGoalScorer_playerId",
            "clock_timeRemaining" = EXCLUDED."clock_timeRemaining",
            "clock_secondsRemaining" = EXCLUDED."clock_secondsRemaining",
            "clock_running" = EXCLUDED."clock_running",
            "clock_inIntermission" = EXCLUDED."clock_inIntermission",
            "tvBroadcasts" = EXCLUDED."tvBroadcasts",
            updated_at = CURRENT_TIMESTAMP
        WHERE (
            EXCLUDED."gameState" IS DISTINCT FROM newapi.games."gameState"
            OR EXCLUDED."gameScheduleState" IS DISTINCT FROM newapi.games."gameScheduleState"
            OR EXCLUDED."startTimeUTC" IS DISTINCT FROM newapi.games."startTimeUTC"
            OR EXCLUDED."easternUTCOffset" IS DISTINCT FROM newapi.games."easternUTCOffset"
            OR EXCLUDED."venueUTCOffset" IS DISTINCT FROM newapi.games."venueUTCOffset"
            OR EXCLUDED."venueTimezone" IS DISTINCT FROM newapi.games."venueTimezone"
            OR EXCLUDED."neutralSite" IS DISTINCT FROM newapi.games."neutralSite"
            OR EXCLUDED."venue_default" IS DISTINCT FROM newapi.games."venue_default"
            OR EXCLUDED."venueLocation_default" IS DISTINCT FROM newapi.games."venueLocation_default"
            OR EXCLUDED."tvBroadcasts" IS DISTINCT FROM newapi.games."tvBroadcasts"
            OR EXCLUDED."limitedScoring" IS DISTINCT FROM newapi.games."limitedScoring"
            OR EXCLUDED."shootoutInUse" IS DISTINCT FROM newapi.games."shootoutInUse"
            OR EXCLUDED."regPeriods" IS DISTINCT FROM newapi.games."regPeriods"
            OR EXCLUDED."otInUse" IS DISTINCT FROM newapi.games."otInUse"
            OR EXCLUDED."tiesInUse" IS DISTINCT FROM newapi.games."tiesInUse"
            OR EXCLUDED."awayTeam_id" IS DISTINCT FROM newapi.games."awayTeam_id"
            OR EXCLUDED."awayTeam_abbrev" IS DISTINCT FROM newapi.games."awayTeam_abbrev"
            OR EXCLUDED."awayTeam_commonName_default" IS DISTINCT FROM newapi.games."awayTeam_commonName_default"
            OR EXCLUDED."awayTeam_placeName_default" IS DISTINCT FROM newapi.games."awayTeam_placeName_default"
            OR EXCLUDED."awayTeam_score" IS DISTINCT FROM newapi.games."awayTeam_score"
            OR EXCLUDED."awayTeam_sog" IS DISTINCT FROM newapi.games."awayTeam_sog"
            OR EXCLUDED."awayTeam_logo" IS DISTINCT FROM newapi.games."awayTeam_logo"
            OR EXCLUDED."awayTeam_darkLogo" IS DISTINCT FROM newapi.games."awayTeam_darkLogo"
            OR EXCLUDED."homeTeam_id" IS DISTINCT FROM newapi.games."homeTeam_id"
            OR EXCLUDED."homeTeam_abbrev" IS DISTINCT FROM newapi.games."homeTeam_abbrev"
            OR EXCLUDED."homeTeam_commonName_default" IS DISTINCT FROM newapi.games."homeTeam_commonName_default"
            OR EXCLUDED."homeTeam_placeName_default" IS DISTINCT FROM newapi.games."homeTeam_placeName_default"
            OR EXCLUDED."homeTeam_score" IS DISTINCT FROM newapi.games."homeTeam_score"
            OR EXCLUDED."homeTeam_sog" IS DISTINCT FROM newapi.games."homeTeam_sog"
            OR EXCLUDED."homeTeam_logo" IS DISTINCT FROM newapi.games."homeTeam_logo"
            OR EXCLUDED."homeTeam_darkLogo" IS DISTINCT FROM newapi.games."homeTeam_darkLogo"
            OR EXCLUDED."periodDescriptor_number" IS DISTINCT FROM newapi.games."periodDescriptor_number"
            OR EXCLUDED."periodDescriptor_periodType" IS DISTINCT FROM newapi.games."periodDescriptor_periodType"
            OR EXCLUDED."periodDescriptor_otPeriods" IS DISTINCT FROM newapi.games."periodDescriptor_otPeriods"
            OR EXCLUDED."periodDescriptor_maxRegulationPeriods" IS DISTINCT FROM newapi.games."periodDescriptor_maxRegulationPeriods"
            OR EXCLUDED."gameOutcome_lastPeriodType" IS DISTINCT FROM newapi.games."gameOutcome_lastPeriodType"
            OR EXCLUDED."winningGoalie_playerId" IS DISTINCT FROM newapi.games."winningGoalie_playerId"
            OR EXCLUDED."winningGoalScorer_playerId" IS DISTINCT FROM newapi.games."winningGoalScorer_playerId"
            OR EXCLUDED."clock_timeRemaining" IS DISTINCT FROM newapi.games."clock_timeRemaining"
            OR EXCLUDED."clock_secondsRemaining" IS DISTINCT FROM newapi.games."clock_secondsRemaining"
            OR EXCLUDED."clock_running" IS DISTINCT FROM newapi.games."clock_running"
            OR EXCLUDED."clock_inIntermission" IS DISTINCT FROM newapi.games."clock_inIntermission"
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

    IF to_regclass('staging1.game_goals') IS NOT NULL THEN
        INSERT INTO newapi.game_goals (
            game_id, event_id, season, game_type, game_date, away_team_abbrev,
            home_team_abbrev, period_number, period_type, time_in_period,
            team_abbrev, is_home, strength, situation_code, scoring_player_id,
            scoring_player_name, assist1_player_id, assist1_player_name,
            assist2_player_id, assist2_player_name, away_score, home_score
        )
        SELECT
            game_id::BIGINT, event_id::BIGINT, season::INTEGER, game_type::INTEGER,
            game_date::DATE, away_team_abbrev, home_team_abbrev,
            period_number::INTEGER, period_type, time_in_period, team_abbrev,
            is_home::BOOLEAN, strength, situation_code, scoring_player_id::BIGINT,
            scoring_player_name, assist1_player_id::BIGINT, assist1_player_name,
            assist2_player_id::BIGINT, assist2_player_name, away_score::INTEGER,
            home_score::INTEGER
        FROM staging1.game_goals
        WHERE game_id IS NOT NULL AND event_id IS NOT NULL
        ON CONFLICT (game_id, event_id) DO UPDATE
        SET
            season = EXCLUDED.season,
            game_type = EXCLUDED.game_type,
            game_date = EXCLUDED.game_date,
            away_team_abbrev = EXCLUDED.away_team_abbrev,
            home_team_abbrev = EXCLUDED.home_team_abbrev,
            period_number = EXCLUDED.period_number,
            period_type = EXCLUDED.period_type,
            time_in_period = EXCLUDED.time_in_period,
            team_abbrev = EXCLUDED.team_abbrev,
            is_home = EXCLUDED.is_home,
            strength = EXCLUDED.strength,
            situation_code = EXCLUDED.situation_code,
            scoring_player_id = EXCLUDED.scoring_player_id,
            scoring_player_name = EXCLUDED.scoring_player_name,
            assist1_player_id = EXCLUDED.assist1_player_id,
            assist1_player_name = EXCLUDED.assist1_player_name,
            assist2_player_id = EXCLUDED.assist2_player_id,
            assist2_player_name = EXCLUDED.assist2_player_name,
            away_score = EXCLUDED.away_score,
            home_score = EXCLUDED.home_score,
            updated_at = CURRENT_TIMESTAMP;
    END IF;

    IF to_regclass('staging1.game_penalties') IS NOT NULL THEN
        DELETE FROM newapi.game_penalties gp
        USING (SELECT DISTINCT game_id::BIGINT AS game_id FROM staging1.game_penalties) s
        WHERE gp.game_id = s.game_id;

        INSERT INTO newapi.game_penalties (
            game_id, penalty_index, season, game_type, game_date, away_team_abbrev,
            home_team_abbrev, period_number, period_type, time_in_period,
            team_abbrev, penalty_type, duration, desc_key, committed_by_player_name,
            committed_by_sweater_number, drawn_by_player_name, drawn_by_sweater_number,
            served_by_name
        )
        SELECT
            game_id::BIGINT, penalty_index::INTEGER, season::INTEGER, game_type::INTEGER,
            game_date::DATE, away_team_abbrev, home_team_abbrev,
            period_number::INTEGER, period_type, time_in_period, team_abbrev,
            penalty_type, duration::INTEGER, desc_key, committed_by_player_name,
            committed_by_sweater_number::INTEGER, drawn_by_player_name,
            drawn_by_sweater_number::INTEGER, served_by_name
        FROM staging1.game_penalties
        WHERE game_id IS NOT NULL;
    END IF;

    IF to_regclass('staging1.game_three_stars') IS NOT NULL THEN
        INSERT INTO newapi.game_three_stars (
            game_id, star, season, game_type, game_date, player_id, player_name,
            team_abbrev, sweater_number, position, goals, assists, points
        )
        SELECT
            game_id::BIGINT, star::INTEGER, season::INTEGER, game_type::INTEGER,
            game_date::DATE, player_id::BIGINT, player_name, team_abbrev,
            sweater_number::INTEGER, position, goals::INTEGER, assists::INTEGER,
            points::INTEGER
        FROM staging1.game_three_stars
        WHERE game_id IS NOT NULL AND star IS NOT NULL
        ON CONFLICT (game_id, star) DO UPDATE
        SET
            season = EXCLUDED.season,
            game_type = EXCLUDED.game_type,
            game_date = EXCLUDED.game_date,
            player_id = EXCLUDED.player_id,
            player_name = EXCLUDED.player_name,
            team_abbrev = EXCLUDED.team_abbrev,
            sweater_number = EXCLUDED.sweater_number,
            position = EXCLUDED.position,
            goals = EXCLUDED.goals,
            assists = EXCLUDED.assists,
            points = EXCLUDED.points,
            updated_at = CURRENT_TIMESTAMP;
    END IF;

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
