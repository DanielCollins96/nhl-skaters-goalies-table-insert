-- ============================================================================
-- FIRST-TIME / GREENFIELD BOOTSTRAP ONLY — DO NOT RUN ON POPULATED PRODUCTION
-- ============================================================================
-- This script DROPS and recreates newapi.current_rosters. Running it on live RDS
-- wipes production rows and cascaded dependents (views, FKs).
--
-- (a) First-time / wipe:
--     SET app.allow_bootstrap = 'on';
--     then run this file, then rosters_table_upsert.sql
--
-- (b) Live RDS re-apply (cast/function fixes, sync logic):
--     run rosters_table_upsert.sql only — never this file
-- ============================================================================

DO $$
BEGIN
  IF current_setting('app.allow_bootstrap', true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION
      'Refusing to DROP newapi.current_rosters. Bootstrap is opt-in. For live RDS run rosters_table_upsert.sql. To bootstrap: SET app.allow_bootstrap = ''on'';';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS newapi;

DROP TABLE IF EXISTS newapi.current_rosters CASCADE;

-- Create the production rosters table with occurrence tracking and active flag
CREATE TABLE newapi.current_rosters (
    id SERIAL PRIMARY KEY,
    "teamAbbreviation" TEXT,
    "positionGroup" TEXT,
    "playerId" BIGINT,
    headshot TEXT,
    "firstName" TEXT,
    "lastName" TEXT,
    "sweaterNumber" DOUBLE PRECISION,
    "positionCode" TEXT,
    "shootsCatches" TEXT,
    "heightInInches" BIGINT,
    "weightInPounds" BIGINT,
    "heightInCentimeters" BIGINT,
    "weightInKilograms" BIGINT,
    "birthDate" TEXT,
    "birthCity" TEXT,
    "birthCountry" TEXT,
    "birthStateProvince" TEXT,
    active BOOLEAN DEFAULT TRUE,          -- TRUE if player is in most recent staging batch
    occurrence_number INTEGER DEFAULT 1,  -- 1st, 2nd, 3rd time with same team
    data_hash TEXT,                       -- Hash of key data fields to detect actual changes
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent exact duplicates
    UNIQUE("playerId", "teamAbbreviation", occurrence_number)
);

-- Create indexes for better performance
CREATE INDEX idx_rosters_player_id ON newapi.current_rosters("playerId");
CREATE INDEX idx_rosters_team ON newapi.current_rosters("teamAbbreviation");
CREATE INDEX idx_rosters_active ON newapi.current_rosters(active);
CREATE INDEX idx_rosters_occurrence ON newapi.current_rosters("playerId", "teamAbbreviation", occurrence_number);
CREATE INDEX idx_rosters_position ON newapi.current_rosters("positionGroup", "positionCode");
