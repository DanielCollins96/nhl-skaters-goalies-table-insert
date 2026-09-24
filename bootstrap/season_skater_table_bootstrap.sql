-- ============================================================================
-- FIRST-TIME / GREENFIELD BOOTSTRAP ONLY — DO NOT RUN ON POPULATED PRODUCTION
-- ============================================================================
-- This script DROPS and recreates newapi.season_skater. Running it on live RDS
-- wipes production rows and cascaded dependents (views, FKs).
--
-- (a) First-time / wipe:
--     SET app.allow_bootstrap = 'on';
--     then run this file, then season_skater_table_upsert.sql
--
-- (b) Live RDS re-apply (cast/function fixes, sync logic):
--     run season_skater_table_upsert.sql only — never this file
-- ============================================================================

DO $$
BEGIN
  IF current_setting('app.allow_bootstrap', true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION
      'Refusing to DROP newapi.season_skater. Bootstrap is opt-in. For live RDS run season_skater_table_upsert.sql. To bootstrap: SET app.allow_bootstrap = ''on'';';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS newapi;

DROP TABLE IF EXISTS newapi.season_skater CASCADE;

-- Create the production season_skater table with occurrence tracking
CREATE TABLE newapi.season_skater (
    id SERIAL PRIMARY KEY,
    "playerId" BIGINT,
    assists BIGINT,
    "gameTypeId" BIGINT,
    "gamesPlayed" BIGINT,
    goals BIGINT,
    "leagueAbbrev" TEXT,
    pim BIGINT,
    "plusMinus" BIGINT,
    points BIGINT,
    season BIGINT,
    sequence BIGINT,
    "teamName.default" TEXT,
    "faceoffWinningPctg" DOUBLE PRECISION,
    "shootingPctg" DOUBLE PRECISION,
    shots DOUBLE PRECISION,
    "powerPlayGoals" DOUBLE PRECISION,
    "shorthandedGoals" DOUBLE PRECISION,
    "gameWinningGoals" DOUBLE PRECISION,
    "teamCommonName.default" TEXT,
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
    "teamPlaceNameWithPreposition.default" TEXT,
    "avgToi" DOUBLE PRECISION,
    "otGoals" DOUBLE PRECISION,
    "powerPlayPoints" DOUBLE PRECISION,
    "shorthandedPoints" DOUBLE PRECISION,
    "teamName.fr" TEXT,
    "teamPlaceNameWithPreposition.fr" TEXT,
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
CREATE INDEX idx_season_skater_player_id ON newapi.season_skater("playerId");
CREATE INDEX idx_season_skater_season ON newapi.season_skater(season);
CREATE INDEX idx_season_skater_team ON newapi.season_skater("teamName.default");
CREATE INDEX idx_season_skater_league ON newapi.season_skater("leagueAbbrev");
CREATE INDEX idx_season_skater_occurrence ON newapi.season_skater("playerId", season, sequence, "teamName.default", "gameTypeId", "leagueAbbrev", occurrence_number);
CREATE INDEX idx_season_skater_active ON newapi.season_skater("playerId", season, sequence, "teamName.default", "gameTypeId", "leagueAbbrev", is_active) WHERE is_active = TRUE;
