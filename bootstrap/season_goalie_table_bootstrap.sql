-- ============================================================================
-- FIRST-TIME / GREENFIELD BOOTSTRAP ONLY — DO NOT RUN ON POPULATED PRODUCTION
-- ============================================================================
-- This script DROPS and recreates newapi.season_goalie. Running it on live RDS
-- wipes production rows and cascaded dependents (views, FKs).
--
-- (a) First-time / wipe:
--     SET app.allow_bootstrap = 'on';
--     then run this file, then season_goalie_table_upsert.sql
--
-- (b) Live RDS re-apply (cast/function fixes, sync logic):
--     run season_goalie_table_upsert.sql only — never this file
-- ============================================================================

DO $$
BEGIN
  IF current_setting('app.allow_bootstrap', true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION
      'Refusing to DROP newapi.season_goalie. Bootstrap is opt-in. For live RDS run season_goalie_table_upsert.sql. To bootstrap: SET app.allow_bootstrap = ''on'';';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS newapi;

DROP TABLE IF EXISTS newapi.season_goalie CASCADE;

-- Create the production season_goalie table with occurrence tracking
CREATE TABLE newapi.season_goalie (
    id SERIAL PRIMARY KEY,
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
    "teamCommonName.default" TEXT,
    "teamName.fr" TEXT,
    "teamPlaceNameWithPreposition.default" TEXT,
    "teamPlaceNameWithPreposition.fr" TEXT,
    "teamCommonName.cs" TEXT,
    "teamCommonName.de" TEXT,
    "teamCommonName.es" TEXT,
    "teamCommonName.fi" TEXT,
    "teamCommonName.sk" TEXT,
    "teamCommonName.sv" TEXT,
    "teamName.cs" TEXT,
    "teamName.de" TEXT,
    "teamName.fi" TEXT,
    "teamName.sk" TEXT,
    "teamName.sv" TEXT,
    "otLosses" DOUBLE PRECISION,
    "teamCommonName.fr" TEXT,
    "teamPlaceNameWithPreposition.cs" TEXT,
    "teamPlaceNameWithPreposition.es" TEXT,
    "teamPlaceNameWithPreposition.fi" TEXT,
    "teamPlaceNameWithPreposition.sk" TEXT,
    "teamPlaceNameWithPreposition.sv" TEXT,
    occurrence_number INTEGER DEFAULT 1,  -- 1st, 2nd, 3rd time with same combination
    data_hash TEXT,                       -- Hash of key data fields to detect actual changes
    is_active BOOLEAN DEFAULT TRUE,       -- Only one active record per player/season/team combination
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent exact duplicates
    -- Using all 6 key columns from the GROUP BY
    UNIQUE("playerId", season, sequence, "teamName.default", "gameTypeId", "leagueAbbrev", occurrence_number)
);

-- Create indexes for better performance
CREATE INDEX idx_season_goalie_player_id ON newapi.season_goalie("playerId");
CREATE INDEX idx_season_goalie_season ON newapi.season_goalie(season);
CREATE INDEX idx_season_goalie_team ON newapi.season_goalie("teamName.default");
CREATE INDEX idx_season_goalie_league ON newapi.season_goalie("leagueAbbrev");
CREATE INDEX idx_season_goalie_occurrence ON newapi.season_goalie("playerId", season, sequence, "teamName.default", "gameTypeId", "leagueAbbrev", occurrence_number);
CREATE INDEX idx_season_goalie_active ON newapi.season_goalie("playerId", season, sequence, "teamName.default", "gameTypeId", "leagueAbbrev", is_active) WHERE is_active = TRUE;
