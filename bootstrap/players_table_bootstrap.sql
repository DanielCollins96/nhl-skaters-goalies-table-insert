-- ============================================================================
-- FIRST-TIME / GREENFIELD BOOTSTRAP ONLY — DO NOT RUN ON POPULATED PRODUCTION
-- ============================================================================
-- This script DROPS and recreates newapi.players. Running it on live RDS
-- wipes production rows and cascaded dependents (views, FKs).
--
-- (a) First-time / wipe:
--     SET app.allow_bootstrap = 'on';
--     then run this file, then players_table_upsert.sql
--
-- (b) Live RDS re-apply (cast/function fixes, sync logic):
--     run players_table_upsert.sql only — never this file
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
      'Refusing to DROP newapi.players. Bootstrap is opt-in. For live RDS run players_table_upsert.sql. To bootstrap: SET app.allow_bootstrap = ''on'';';
  END IF;

  EXECUTE 'CREATE SCHEMA IF NOT EXISTS newapi';
  EXECUTE 'DROP TABLE IF EXISTS newapi.players CASCADE';
END $$;

-- Create the production players table with occurrence tracking
CREATE TABLE newapi.players (
    id SERIAL PRIMARY KEY,
    "playerId" BIGINT,
    "isActive" BOOLEAN,
    "currentTeamId" BIGINT,
    "currentTeamAbbrev" TEXT,
    "fullTeamName" TEXT,
    "firstName" TEXT,
    "lastName" TEXT,
    "sweaterNumber" INTEGER,
    "position" TEXT,
    headshot TEXT,
    "heroImage" TEXT,
    "heightInInches" DOUBLE PRECISION,
    "heightInCentimeters" DOUBLE PRECISION,
    "weightInPounds" DOUBLE PRECISION,
    "weightInKilograms" DOUBLE PRECISION,
    "birthDate" DATE,
    "birthCity" TEXT,
    "birthStateProvince" TEXT,
    "birthCountry" TEXT,
    "shootsCatches" TEXT,
    "playerSlug" TEXT,
    "inTop100AllTime" BOOLEAN,
    "inHHOF" BOOLEAN,
    "draftYear" INTEGER,
    "draftTeamAbbrev" TEXT,
    "draftRound" INTEGER,
    "draftPickInRound" INTEGER,
    "draftOverallPick" INTEGER,
    
    occurrence_number INTEGER DEFAULT 1,
    data_hash TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent exact duplicates
    UNIQUE("playerId", occurrence_number)
);

-- Create indexes for better performance
CREATE INDEX idx_players_player_id ON newapi.players("playerId");
CREATE INDEX idx_players_current_team ON newapi.players("currentTeamAbbrev");
CREATE INDEX idx_players_occurrence ON newapi.players("playerId", occurrence_number);

COMMIT;
