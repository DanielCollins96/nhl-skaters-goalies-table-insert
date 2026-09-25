-- ============================================================================
-- FIRST-TIME / GREENFIELD BOOTSTRAP ONLY — DO NOT RUN ON POPULATED PRODUCTION
-- ============================================================================
-- This script DROPS and recreates newapi.goalies. Running it on live RDS
-- wipes production rows and cascaded dependents (views, FKs).
--
-- (a) First-time / wipe:
--     SET app.allow_bootstrap = 'on';
--     then run this file, then goalies_table_upsert.sql
--
-- (b) Live RDS re-apply (cast/function fixes, sync logic):
--     run goalies_table_upsert.sql only — never this file
--
-- This file sets ON_ERROR_STOP itself. Do not rely on the caller passing
-- -v ON_ERROR_STOP=1. The opt-in check and DROP share one DO block inside
-- one transaction: a failed guard cannot reach DROP.
-- ============================================================================

\set ON_ERROR_STOP on

BEGIN;

DO $$
BEGIN
  IF current_setting('app.allow_bootstrap', true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION
      'Refusing to DROP newapi.goalies. Bootstrap is opt-in. For live RDS run goalies_table_upsert.sql. To bootstrap: SET app.allow_bootstrap = ''on'';';
  END IF;

  EXECUTE 'CREATE SCHEMA IF NOT EXISTS newapi';
  EXECUTE 'DROP TABLE IF EXISTS newapi.goalies CASCADE';
END $$;

-- Create the production goalies table with occurrence tracking
CREATE TABLE newapi.goalies (
    id SERIAL PRIMARY KEY,
    "playerId" BIGINT,
    headshot TEXT,
    "firstName" TEXT,
    "lastName" TEXT,
    "fullName" TEXT,
    "gamesPlayed" BIGINT,
    "gamesStarted" BIGINT,
    wins BIGINT,
    losses BIGINT,
    "overtimeLosses" DOUBLE PRECISION,
    "goalsAgainstAverage" DOUBLE PRECISION,
    "savePercentage" DOUBLE PRECISION,
    "shotsAgainst" DOUBLE PRECISION,
    saves DOUBLE PRECISION,
    "goalsAgainst" BIGINT,
    shutouts BIGINT,
    goals BIGINT,
    assists BIGINT,
    points BIGINT,
    "penaltyMinutes" BIGINT,
    "timeOnIce" BIGINT,
    ties DOUBLE PRECISION,
    season BIGINT,
    "gameType" BIGINT,
    team TEXT,
    occurrence_number INTEGER DEFAULT 1,  -- 1st, 2nd, 3rd time with same team in season
    data_hash TEXT,                       -- Hash of key data fields to detect actual changes
    is_active BOOLEAN DEFAULT TRUE,       -- Only one active record per player/season/gameType/team
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent exact duplicates
    UNIQUE("playerId", season, "gameType", team, occurrence_number)
);

-- Create indexes for better performance
CREATE INDEX idx_goalies_player_id ON newapi.goalies("playerId");
CREATE INDEX idx_goalies_season ON newapi.goalies(season);
CREATE INDEX idx_goalies_team ON newapi.goalies(team);
CREATE INDEX idx_goalies_occurrence ON newapi.goalies("playerId", season, "gameType", team, occurrence_number);
CREATE INDEX idx_goalies_active ON newapi.goalies("playerId", season, "gameType", team, is_active) WHERE is_active = TRUE;

COMMIT;
