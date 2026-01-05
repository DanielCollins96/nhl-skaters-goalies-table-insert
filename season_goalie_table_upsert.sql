-- Drop existing functions first to avoid return type conflicts
DROP FUNCTION IF EXISTS insert_season_goalies_from_staging() CASCADE;
DROP FUNCTION IF EXISTS insert_season_goalies_from_staging_with_logging() CASCADE;
DROP FUNCTION IF EXISTS get_season_goalies_occurrence_stats() CASCADE;
DROP FUNCTION IF EXISTS generate_season_goalie_data_hash(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION) CASCADE;
DROP PROCEDURE IF EXISTS sync_season_goalies_from_staging() CASCADE;

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

-- Create a table to store ETL run statistics
CREATE TABLE IF NOT EXISTS newapi.season_goalie_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_processed INTEGER,
    new_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    new_occurrences INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

-- Create index for querying recent runs
CREATE INDEX IF NOT EXISTS idx_season_goalie_etl_log_timestamp ON newapi.season_goalie_etl_log(run_timestamp);

-- Function to generate a hash of the important season_goalie data fields
CREATE OR REPLACE FUNCTION generate_season_goalie_data_hash(
    p_games_played DOUBLE PRECISION,
    p_goals_against DOUBLE PRECISION,
    p_goals_against_avg DOUBLE PRECISION,
    p_losses DOUBLE PRECISION,
    p_shutouts DOUBLE PRECISION,
    p_ties DOUBLE PRECISION,
    p_wins DOUBLE PRECISION,
    p_assists DOUBLE PRECISION,
    p_games_started DOUBLE PRECISION,
    p_goals DOUBLE PRECISION,
    p_pim DOUBLE PRECISION,
    p_save_pctg DOUBLE PRECISION,
    p_shots_against DOUBLE PRECISION,
    p_ot_losses DOUBLE PRECISION,
    p_time_on_ice TEXT
) RETURNS TEXT AS $$
BEGIN
    RETURN md5(
        COALESCE(p_games_played::TEXT, '') || '|' ||
        COALESCE(p_goals_against::TEXT, '') || '|' ||
        COALESCE(p_goals_against_avg::TEXT, '') || '|' ||
        COALESCE(p_losses::TEXT, '') || '|' ||
        COALESCE(p_shutouts::TEXT, '') || '|' ||
        COALESCE(p_ties::TEXT, '') || '|' ||
        COALESCE(p_wins::TEXT, '') || '|' ||
        COALESCE(p_assists::TEXT, '') || '|' ||
        COALESCE(p_games_started::TEXT, '') || '|' ||
        COALESCE(p_goals::TEXT, '') || '|' ||
        COALESCE(p_pim::TEXT, '') || '|' ||
        COALESCE(p_save_pctg::TEXT, '') || '|' ||
        COALESCE(p_shots_against::TEXT, '') || '|' ||
        COALESCE(p_ot_losses::TEXT, '') || '|' ||
        COALESCE(p_time_on_ice, '')
    );
END;
$$ LANGUAGE plpgsql;

-- Enhanced function that logs results
CREATE OR REPLACE FUNCTION insert_season_goalies_from_staging_with_logging()
RETURNS TABLE(
    total_processed INTEGER,
    new_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    new_occurrences INTEGER,
    run_duration INTERVAL
) AS $$
DECLARE
    rows_processed INTEGER := 0;
    new_count INTEGER := 0;
    updated_count INTEGER := 0;
    unchanged_count INTEGER := 0;
    occurrence_count INTEGER := 0;
    rec RECORD;
    matching_record RECORD;
    new_hash TEXT;
    next_occurrence INTEGER;
    found_match BOOLEAN;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN
    start_time := CURRENT_TIMESTAMP;
    
    -- Ensure missing columns exist in staging
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.cs" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.de" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.es" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.fi" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.sk" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.sv" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamCommonName.fr" TEXT;
    
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.cs" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.de" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.fi" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.sk" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamName.sv" TEXT;
    
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.cs" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.es" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.fi" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.sk" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.sv" TEXT;
    ALTER TABLE staging1.season_goalie ADD COLUMN IF NOT EXISTS "teamPlaceNameWithPreposition.fr" TEXT;

    -- Loop through each record in staging
    FOR rec IN 
        SELECT 
            "playerId", "gameTypeId", "gamesPlayed", "goalsAgainst", "goalsAgainstAvg",
            "leagueAbbrev", losses, season, sequence, shutouts, ties, "timeOnIce",
            wins, "teamName.default", assists, "gamesStarted", goals, pim, "savePctg",
            "shotsAgainst", "teamCommonName.default", "teamName.fr",
            "teamPlaceNameWithPreposition.default", "teamPlaceNameWithPreposition.fr",
            "teamCommonName.cs", "teamCommonName.de", "teamCommonName.es",
            "teamCommonName.fi", "teamCommonName.sk", "teamCommonName.sv",
            "teamName.cs", "teamName.de", "teamName.fi", "teamName.sk", "teamName.sv",
            "otLosses", "teamCommonName.fr", "teamPlaceNameWithPreposition.cs",
            "teamPlaceNameWithPreposition.es", "teamPlaceNameWithPreposition.fi",
            "teamPlaceNameWithPreposition.sk", "teamPlaceNameWithPreposition.sv"
        FROM staging1.season_goalie
    LOOP
        -- Generate hash for the new data
        new_hash := generate_season_goalie_data_hash(
            rec."gamesPlayed", rec."goalsAgainst", rec."goalsAgainstAvg",
            rec.losses, rec.shutouts, rec.ties, rec.wins,
            rec.assists, rec."gamesStarted", rec.goals, rec.pim,
            rec."savePctg", rec."shotsAgainst", rec."otLosses", rec."timeOnIce"
        );
        
        found_match := FALSE;
        
        -- Check the active record for this player/season/sequence/team/gameType/league combination
        SELECT * INTO matching_record FROM newapi.season_goalie 
        WHERE "playerId" = rec."playerId" 
        AND season = rec.season 
        AND sequence = rec.sequence
        AND "teamName.default" = rec."teamName.default"
        AND "gameTypeId" = rec."gameTypeId" 
        AND "leagueAbbrev" = rec."leagueAbbrev"
        AND is_active = TRUE
        LIMIT 1;
        
        IF FOUND THEN
            -- Check if this record has the same data (hash match)
            IF matching_record.data_hash = new_hash THEN
                -- Data hasn't changed - just update the timestamp
                UPDATE newapi.season_goalie 
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = matching_record.id;
                
                unchanged_count := unchanged_count + 1;
                found_match := TRUE;
            ELSE
                -- Data has changed - deactivate the old record and mark for new insert
                UPDATE newapi.season_goalie 
                SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
                WHERE id = matching_record.id;
                
                updated_count := updated_count + 1;
                found_match := FALSE; -- Will trigger insert of new active record
            END IF;
        END IF;
        
        -- If no matching hash was found, we need to insert
        IF NOT found_match THEN
            -- Get the next occurrence number for this combination
            SELECT COALESCE(MAX(occurrence_number), 0) + 1 INTO next_occurrence
            FROM newapi.season_goalie 
            WHERE "playerId" = rec."playerId" 
            AND season = rec.season 
            AND sequence = rec.sequence
            AND "teamName.default" = rec."teamName.default"
            AND "gameTypeId" = rec."gameTypeId" 
            AND "leagueAbbrev" = rec."leagueAbbrev";
            
            -- Insert new record with the next occurrence number (active by default)
            INSERT INTO newapi.season_goalie (
                "playerId", "gameTypeId", "gamesPlayed", "goalsAgainst", "goalsAgainstAvg",
                "leagueAbbrev", losses, season, sequence, shutouts, ties, "timeOnIce",
                wins, "teamName.default", assists, "gamesStarted", goals, pim, "savePctg",
                "shotsAgainst", "teamCommonName.default", "teamName.fr",
                "teamPlaceNameWithPreposition.default", "teamPlaceNameWithPreposition.fr",
                "teamCommonName.cs", "teamCommonName.de", "teamCommonName.es",
                "teamCommonName.fi", "teamCommonName.sk", "teamCommonName.sv",
                "teamName.cs", "teamName.de", "teamName.fi", "teamName.sk", "teamName.sv",
                "otLosses", "teamCommonName.fr", "teamPlaceNameWithPreposition.cs",
                "teamPlaceNameWithPreposition.es", "teamPlaceNameWithPreposition.fi",
                "teamPlaceNameWithPreposition.sk", "teamPlaceNameWithPreposition.sv",
                occurrence_number, data_hash, is_active
            ) VALUES (
                rec."playerId", rec."gameTypeId", rec."gamesPlayed", rec."goalsAgainst",
                rec."goalsAgainstAvg", rec."leagueAbbrev", rec.losses, rec.season,
                rec.sequence, rec.shutouts, rec.ties, rec."timeOnIce", rec.wins,
                rec."teamName.default", rec.assists, rec."gamesStarted", rec.goals,
                rec.pim, rec."savePctg", rec."shotsAgainst", rec."teamCommonName.default",
                rec."teamName.fr", rec."teamPlaceNameWithPreposition.default",
                rec."teamPlaceNameWithPreposition.fr", rec."teamCommonName.cs",
                rec."teamCommonName.de", rec."teamCommonName.es", rec."teamCommonName.fi",
                rec."teamCommonName.sk", rec."teamCommonName.sv", rec."teamName.cs",
                rec."teamName.de", rec."teamName.fi", rec."teamName.sk", rec."teamName.sv",
                rec."otLosses", rec."teamCommonName.fr", rec."teamPlaceNameWithPreposition.cs",
                rec."teamPlaceNameWithPreposition.es", rec."teamPlaceNameWithPreposition.fi",
                rec."teamPlaceNameWithPreposition.sk", rec."teamPlaceNameWithPreposition.sv",
                next_occurrence, new_hash, TRUE
            );
            
            IF next_occurrence = 1 THEN
                new_count := new_count + 1;
            ELSE
                occurrence_count := occurrence_count + 1;
            END IF;
        END IF;
        
        rows_processed := rows_processed + 1;
    END LOOP;
    
    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;
    
    -- Log the results
    INSERT INTO newapi.season_goalie_etl_log (
        total_processed, new_records, updated_records, unchanged_records, 
        new_occurrences, run_duration
    ) VALUES (
        rows_processed, new_count, updated_count, unchanged_count, 
        occurrence_count, duration
    );
    
    RETURN QUERY SELECT rows_processed, new_count, updated_count, unchanged_count, occurrence_count, duration;
END;
$$ LANGUAGE plpgsql;

-- Updated procedure to use the logging function
CREATE OR REPLACE PROCEDURE sync_season_goalies_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM insert_season_goalies_from_staging_with_logging() INTO result_record;
    
    RAISE NOTICE 'Season goalies sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  New player/team combinations: %', result_record.new_records;
    RAISE NOTICE '  Unchanged records (same data): %', result_record.unchanged_records;
    RAISE NOTICE '  New occurrences (same team, different stats): %', result_record.new_occurrences;
END;
$$;

-- View to show current/latest season_goalie stats (only active records)
CREATE OR REPLACE VIEW newapi.season_goalie_current AS
SELECT * FROM newapi.season_goalie WHERE is_active = TRUE;

-- View to show season_goalies with multiple occurrences
CREATE OR REPLACE VIEW newapi.season_goalie_multiple_stints AS
SELECT 
    s."playerId",
    s.season,
    s.sequence,
    s."teamName.default",
    s."gameTypeId",
    s."leagueAbbrev",
    s.occurrence_number,
    s.created_at,
    s.updated_at,
    s.wins,
    s.losses,
    s."goalsAgainstAvg",
    s."savePctg",
    s."gamesPlayed",
    -- Show progression between occurrences
    LAG(s.wins) OVER (PARTITION BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev" ORDER BY s.occurrence_number) as prev_wins,
    LAG(s.losses) OVER (PARTITION BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev" ORDER BY s.occurrence_number) as prev_losses,
    LAG(s."goalsAgainstAvg") OVER (PARTITION BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev" ORDER BY s.occurrence_number) as prev_gaa,
    LAG(s."gamesPlayed") OVER (PARTITION BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev" ORDER BY s.occurrence_number) as prev_games
FROM newapi.season_goalie s
WHERE (s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev") IN (
    SELECT "playerId", season, sequence, "teamName.default", "gameTypeId", "leagueAbbrev"
    FROM newapi.season_goalie 
    GROUP BY "playerId", season, sequence, "teamName.default", "gameTypeId", "leagueAbbrev"
    HAVING COUNT(*) > 1
)
ORDER BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev", s.occurrence_number;

-- Function to show statistics about multiple occurrences
CREATE OR REPLACE FUNCTION get_season_goalies_occurrence_stats()
RETURNS TABLE(
    player_id BIGINT,
    season_val BIGINT,
    sequence_val BIGINT,
    team_name TEXT,
    game_type BIGINT,
    league TEXT,
    total_occurrences INTEGER,
    date_range TEXT,
    stat_progression TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s."playerId",
        s.season,
        s.sequence,
        s."teamName.default",
        s."gameTypeId",
        s."leagueAbbrev",
        COUNT(*)::INTEGER as total_occurrences,
        (MIN(s.created_at)::DATE || ' to ' || MAX(s.updated_at)::DATE) as date_range,
        'Games: ' || MIN(s."gamesPlayed") || '->' || MAX(s."gamesPlayed") || 
        ', Wins: ' || MIN(s.wins) || '->' || MAX(s.wins) || 
        ', GAA: ' || MIN(s."goalsAgainstAvg") || '->' || MAX(s."goalsAgainstAvg") as stat_progression
    FROM newapi.season_goalie s
    GROUP BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev"
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC, s."playerId", s.season;
END;
$$ LANGUAGE plpgsql;

-- View to see recent ETL runs
CREATE OR REPLACE VIEW newapi.season_goalie_etl_summary AS
SELECT 
    id,
    run_timestamp,
    total_processed,
    new_records,
    unchanged_records,
    new_occurrences,
    run_duration,
    ROUND(EXTRACT(EPOCH FROM run_duration)::NUMERIC, 2) as duration_seconds,
    CASE 
        WHEN total_processed > 0 THEN ROUND((new_records + new_occurrences)::NUMERIC / total_processed * 100, 1)
        ELSE 0 
    END as change_percentage
FROM newapi.season_goalie_etl_log
ORDER BY run_timestamp DESC;

-- Execute the sync AFTER you've loaded staging1.season_goalie with pandas to_sql
-- CALL sync_season_goalies_from_staging();
