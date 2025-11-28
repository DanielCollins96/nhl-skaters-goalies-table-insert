-- Drop existing functions first to avoid return type conflicts
DROP FUNCTION IF EXISTS insert_season_skaters_from_staging() CASCADE;
DROP FUNCTION IF EXISTS insert_season_skaters_from_staging_with_logging() CASCADE;
DROP FUNCTION IF EXISTS get_season_skaters_occurrence_stats() CASCADE;
DROP FUNCTION IF EXISTS generate_season_skater_data_hash(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION) CASCADE;
DROP PROCEDURE IF EXISTS sync_season_skaters_from_staging() CASCADE;

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

-- Create a table to store ETL run statistics
CREATE TABLE IF NOT EXISTS newapi.season_skater_etl_log (
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
CREATE INDEX IF NOT EXISTS idx_season_skater_etl_log_timestamp ON newapi.season_skater_etl_log(run_timestamp);

-- Function to generate a hash of the important season_skater data fields
CREATE OR REPLACE FUNCTION generate_season_skater_data_hash(
    p_assists DOUBLE PRECISION,
    p_games_played DOUBLE PRECISION,
    p_goals DOUBLE PRECISION,
    p_pim DOUBLE PRECISION,
    p_plus_minus DOUBLE PRECISION,
    p_points DOUBLE PRECISION,
    p_shots DOUBLE PRECISION,
    p_faceoff_winning_pctg DOUBLE PRECISION,
    p_shooting_pctg DOUBLE PRECISION,
    p_power_play_goals DOUBLE PRECISION,
    p_shorthanded_goals DOUBLE PRECISION,
    p_game_winning_goals DOUBLE PRECISION,
    p_avg_toi DOUBLE PRECISION,
    p_ot_goals DOUBLE PRECISION,
    p_power_play_points DOUBLE PRECISION,
    p_shorthanded_points DOUBLE PRECISION
) RETURNS TEXT AS $$
BEGIN
    RETURN md5(
        COALESCE(p_assists::TEXT, '') || '|' ||
        COALESCE(p_games_played::TEXT, '') || '|' ||
        COALESCE(p_goals::TEXT, '') || '|' ||
        COALESCE(p_pim::TEXT, '') || '|' ||
        COALESCE(p_plus_minus::TEXT, '') || '|' ||
        COALESCE(p_points::TEXT, '') || '|' ||
        COALESCE(p_shots::TEXT, '') || '|' ||
        COALESCE(p_faceoff_winning_pctg::TEXT, '') || '|' ||
        COALESCE(p_shooting_pctg::TEXT, '') || '|' ||
        COALESCE(p_power_play_goals::TEXT, '') || '|' ||
        COALESCE(p_shorthanded_goals::TEXT, '') || '|' ||
        COALESCE(p_game_winning_goals::TEXT, '') || '|' ||
        COALESCE(p_avg_toi::TEXT, '') || '|' ||
        COALESCE(p_ot_goals::TEXT, '') || '|' ||
        COALESCE(p_power_play_points::TEXT, '') || '|' ||
        COALESCE(p_shorthanded_points::TEXT, '')
    );
END;
$$ LANGUAGE plpgsql;

-- Enhanced function that logs results
CREATE OR REPLACE FUNCTION insert_season_skaters_from_staging_with_logging()
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
    
    -- Loop through each record in staging
    FOR rec IN 
        SELECT 
            "playerId", assists, "gameTypeId", "gamesPlayed", goals, "leagueAbbrev", 
            pim, "plusMinus", points, season, sequence, "teamName.default", 
            "faceoffWinningPctg", "shootingPctg", shots, "powerPlayGoals", 
            "shorthandedGoals", "gameWinningGoals", "teamCommonName.default", 
            "teamCommonName.cs", "teamCommonName.de", "teamCommonName.es", 
            "teamCommonName.fi", "teamCommonName.sk", "teamCommonName.sv", 
            "teamName.cs", "teamName.de", "teamName.fi", "teamName.sk", "teamName.sv", 
            "teamPlaceNameWithPreposition.default", "avgToi", "otGoals", 
            "powerPlayPoints", "shorthandedPoints", "teamName.fr", 
            "teamPlaceNameWithPreposition.fr", "teamCommonName.fr", 
            "teamPlaceNameWithPreposition.cs", "teamPlaceNameWithPreposition.es", 
            "teamPlaceNameWithPreposition.fi", "teamPlaceNameWithPreposition.sk", 
            "teamPlaceNameWithPreposition.sv"
        FROM staging1.season_skater
    LOOP
        -- Generate hash for the new data
        new_hash := generate_season_skater_data_hash(
            rec.assists::DOUBLE PRECISION, rec."gamesPlayed"::DOUBLE PRECISION, rec.goals::DOUBLE PRECISION, 
            rec.pim::DOUBLE PRECISION, rec."plusMinus"::DOUBLE PRECISION, rec.points::DOUBLE PRECISION, 
            rec.shots::DOUBLE PRECISION, rec."faceoffWinningPctg"::DOUBLE PRECISION, rec."shootingPctg"::DOUBLE PRECISION,
            rec."powerPlayGoals"::DOUBLE PRECISION, rec."shorthandedGoals"::DOUBLE PRECISION, rec."gameWinningGoals"::DOUBLE PRECISION,
            CASE WHEN rec."avgToi" ~ '^\d+(\.\d+)?$' THEN rec."avgToi"::DOUBLE PRECISION ELSE 0::DOUBLE PRECISION END, 
            0::DOUBLE PRECISION, 0::DOUBLE PRECISION, 0::DOUBLE PRECISION
        );
        
        found_match := FALSE;
        
        -- Check the active record for this player/season/sequence/team/gameType/league combination
        SELECT * INTO matching_record FROM newapi.season_skater 
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
                UPDATE newapi.season_skater 
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = matching_record.id;
                
                unchanged_count := unchanged_count + 1;
                found_match := TRUE;
            ELSE
                -- Data has changed - deactivate the old record and mark for new insert
                UPDATE newapi.season_skater 
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
            FROM newapi.season_skater 
            WHERE "playerId" = rec."playerId" 
            AND season = rec.season 
            AND sequence = rec.sequence
            AND "teamName.default" = rec."teamName.default"
            AND "gameTypeId" = rec."gameTypeId" 
            AND "leagueAbbrev" = rec."leagueAbbrev";
            
            -- Insert new record with the next occurrence number (active by default)
            INSERT INTO newapi.season_skater (
                "playerId", assists, "gameTypeId", "gamesPlayed", goals, "leagueAbbrev", 
                pim, "plusMinus", points, season, sequence, "teamName.default", 
                "faceoffWinningPctg", "shootingPctg", shots, "powerPlayGoals", 
                "shorthandedGoals", "gameWinningGoals", "teamCommonName.default", 
                "teamCommonName.cs", "teamCommonName.de", "teamCommonName.es", 
                "teamCommonName.fi", "teamCommonName.sk", "teamCommonName.sv", 
                "teamName.cs", "teamName.de", "teamName.fi", "teamName.sk", "teamName.sv", 
                "teamPlaceNameWithPreposition.default", "avgToi", "otGoals", 
                "powerPlayPoints", "shorthandedPoints", "teamName.fr", 
                "teamPlaceNameWithPreposition.fr", "teamCommonName.fr", 
                "teamPlaceNameWithPreposition.cs", "teamPlaceNameWithPreposition.es", 
                "teamPlaceNameWithPreposition.fi", "teamPlaceNameWithPreposition.sk", 
                "teamPlaceNameWithPreposition.sv",
                occurrence_number, data_hash, is_active
            ) VALUES (
                rec."playerId", rec.assists, rec."gameTypeId", rec."gamesPlayed", rec.goals, 
                rec."leagueAbbrev", rec.pim, rec."plusMinus", rec.points, rec.season, 
                rec.sequence, rec."teamName.default", rec."faceoffWinningPctg", 
                rec."shootingPctg", rec.shots, rec."powerPlayGoals", rec."shorthandedGoals", 
                rec."gameWinningGoals", rec."teamCommonName.default", rec."teamCommonName.cs", 
                rec."teamCommonName.de", rec."teamCommonName.es", rec."teamCommonName.fi", 
                rec."teamCommonName.sk", rec."teamCommonName.sv", rec."teamName.cs", 
                rec."teamName.de", rec."teamName.fi", rec."teamName.sk", rec."teamName.sv", 
                rec."teamPlaceNameWithPreposition.default", CASE WHEN rec."avgToi" ~ '^\d+(\.\d+)?$' THEN rec."avgToi"::DOUBLE PRECISION ELSE NULL END, rec."otGoals", 
                rec."powerPlayPoints", rec."shorthandedPoints", rec."teamName.fr", 
                rec."teamPlaceNameWithPreposition.fr", rec."teamCommonName.fr", 
                rec."teamPlaceNameWithPreposition.cs", rec."teamPlaceNameWithPreposition.es", 
                rec."teamPlaceNameWithPreposition.fi", rec."teamPlaceNameWithPreposition.sk", 
                rec."teamPlaceNameWithPreposition.sv",
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
    INSERT INTO newapi.season_skater_etl_log (
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
CREATE OR REPLACE PROCEDURE sync_season_skaters_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM insert_season_skaters_from_staging_with_logging() INTO result_record;
    
    RAISE NOTICE 'Season skaters sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  New player/team combinations: %', result_record.new_records;
    RAISE NOTICE '  Unchanged records (same data): %', result_record.unchanged_records;
    RAISE NOTICE '  New occurrences (same team, different stats): %', result_record.new_occurrences;
END;
$$;

-- View to show current/latest season_skater stats (only active records)
CREATE OR REPLACE VIEW newapi.season_skater_current AS
SELECT * FROM newapi.season_skater WHERE is_active = TRUE;

-- View to show season_skaters with multiple occurrences
CREATE OR REPLACE VIEW newapi.season_skater_multiple_stints AS
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
    s.goals,
    s.assists,
    s.points,
    s."gamesPlayed",
    -- Show progression between occurrences
    LAG(s.goals) OVER (PARTITION BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev" ORDER BY s.occurrence_number) as prev_goals,
    LAG(s.assists) OVER (PARTITION BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev" ORDER BY s.occurrence_number) as prev_assists,
    LAG(s.points) OVER (PARTITION BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev" ORDER BY s.occurrence_number) as prev_points,
    LAG(s."gamesPlayed") OVER (PARTITION BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev" ORDER BY s.occurrence_number) as prev_games
FROM newapi.season_skater s
WHERE (s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev") IN (
    SELECT "playerId", season, sequence, "teamName.default", "gameTypeId", "leagueAbbrev"
    FROM newapi.season_skater 
    GROUP BY "playerId", season, sequence, "teamName.default", "gameTypeId", "leagueAbbrev"
    HAVING COUNT(*) > 1
)
ORDER BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev", s.occurrence_number;

-- Function to show statistics about multiple occurrences
CREATE OR REPLACE FUNCTION get_season_skaters_occurrence_stats()
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
        ', Goals: ' || MIN(s.goals) || '->' || MAX(s.goals) || 
        ', Points: ' || MIN(s.points) || '->' || MAX(s.points) as stat_progression
    FROM newapi.season_skater s
    GROUP BY s."playerId", s.season, s.sequence, s."teamName.default", s."gameTypeId", s."leagueAbbrev"
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC, s."playerId", s.season;
END;
$$ LANGUAGE plpgsql;

-- View to see recent ETL runs
CREATE OR REPLACE VIEW newapi.season_skater_etl_summary AS
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
FROM newapi.season_skater_etl_log
ORDER BY run_timestamp DESC;

-- Execute the sync
CALL sync_season_skaters_from_staging();
