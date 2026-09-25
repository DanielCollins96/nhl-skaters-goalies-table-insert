-- ============================================================================
-- FIRST-TIME / GREENFIELD BOOTSTRAP ONLY — DO NOT RUN ON POPULATED PRODUCTION
-- ============================================================================
-- This script DROPS and recreates newapi.skaters. Running it on live RDS
-- wipes production rows and cascaded dependents (views, FKs).
--
-- (a) First-time / wipe:
--     SET app.allow_bootstrap = 'on';
--     then run this file, then skaters_table_upsert.sql
--
-- (b) Live RDS re-apply (cast/function fixes, sync logic):
--     run skaters_table_upsert.sql only — never this file
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
      'Refusing to DROP newapi.skaters. Bootstrap is opt-in. For live RDS run skaters_table_upsert.sql. To bootstrap: SET app.allow_bootstrap = ''on'';';
  END IF;

  EXECUTE 'CREATE SCHEMA IF NOT EXISTS newapi';
  EXECUTE 'DROP TABLE IF EXISTS newapi.skaters CASCADE';
END $$;

-- Create the production skaters table with occurrence tracking
CREATE TABLE newapi.skaters (
    id SERIAL PRIMARY KEY,
    url_index BIGINT,
    "playerId" BIGINT,
    headshot TEXT,
    "positionCode" TEXT,
    "gamesPlayed" BIGINT,
    goals BIGINT,
    assists BIGINT,
    points BIGINT,
    "plusMinus" DOUBLE PRECISION,
    "penaltyMinutes" BIGINT,
    "powerPlayGoals" DOUBLE PRECISION,
    "shorthandedGoals" DOUBLE PRECISION,
    "gameWinningGoals" DOUBLE PRECISION,
    "overtimeGoals" DOUBLE PRECISION,
    shots DOUBLE PRECISION,
    "shootingPctg" DOUBLE PRECISION,
    "avgTimeOnIcePerGame" DOUBLE PRECISION,
    "avgShiftsPerGame" DOUBLE PRECISION,
    "faceoffWinPctg" DOUBLE PRECISION,
    "firstName" TEXT,  -- Simplified from localized fields
    "lastName" TEXT,   -- Simplified from localized fields
    "fullName" TEXT,
    season BIGINT,
    "gameType" BIGINT,
    "triCode" TEXT,    -- Changed from abbreviation to match staging
    occurrence_number INTEGER DEFAULT 1,  -- 1st, 2nd, 3rd time with same team in season
    data_hash TEXT,                       -- Hash of key data fields to detect actual changes
    is_active BOOLEAN DEFAULT TRUE,       -- Only one active record per player/season/gameType/team
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent exact duplicates
    UNIQUE("playerId", season, "gameType", "triCode", occurrence_number)
);

-- Create indexes for better performance
CREATE INDEX idx_skaters_player_id ON newapi.skaters("playerId");
CREATE INDEX idx_skaters_season ON newapi.skaters(season);
CREATE INDEX idx_skaters_team ON newapi.skaters("triCode");
CREATE INDEX idx_skaters_occurrence ON newapi.skaters("playerId", season, "gameType", "triCode", occurrence_number);
CREATE INDEX idx_skaters_active ON newapi.skaters("playerId", season, "gameType", "triCode", is_active) WHERE is_active = TRUE;

COMMIT;
