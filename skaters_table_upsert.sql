-- Drop existing functions first to avoid return type conflicts
DROP FUNCTION IF EXISTS insert_skaters_from_staging() CASCADE;
DROP FUNCTION IF EXISTS get_occurrence_stats() CASCADE;
DROP FUNCTION IF EXISTS generate_skater_data_hash(BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE PRECISION, BIGINT, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION) CASCADE;
DROP PROCEDURE IF EXISTS sync_skaters_from_staging() CASCADE;

DROP TABLE IF EXISTS newapi.skaters CASCADE;
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

-- Function to generate a hash of the important data fields
CREATE OR REPLACE FUNCTION generate_skater_data_hash(
    p_games_played BIGINT,
    p_goals BIGINT,
    p_assists BIGINT,
    p_points BIGINT,
    p_plus_minus DOUBLE PRECISION,
    p_penalty_minutes BIGINT,
    p_power_play_goals DOUBLE PRECISION,
    p_shorthanded_goals DOUBLE PRECISION,
    p_game_winning_goals DOUBLE PRECISION,
    p_overtime_goals DOUBLE PRECISION,
    p_shots DOUBLE PRECISION,
    p_shooting_pctg DOUBLE PRECISION,
    p_avg_time_on_ice DOUBLE PRECISION,
    p_avg_shifts DOUBLE PRECISION,
    p_faceoff_win_pctg DOUBLE PRECISION
) RETURNS TEXT AS $$
BEGIN
    RETURN md5(
        COALESCE(p_games_played::TEXT, '') || '|' ||
        COALESCE(p_goals::TEXT, '') || '|' ||
        COALESCE(p_assists::TEXT, '') || '|' ||
        COALESCE(p_points::TEXT, '') || '|' ||
        COALESCE(p_plus_minus::TEXT, '') || '|' ||
        COALESCE(p_penalty_minutes::TEXT, '') || '|' ||
        COALESCE(p_power_play_goals::TEXT, '') || '|' ||
        COALESCE(p_shorthanded_goals::TEXT, '') || '|' ||
        COALESCE(p_game_winning_goals::TEXT, '') || '|' ||
        COALESCE(p_overtime_goals::TEXT, '') || '|' ||
        COALESCE(p_shots::TEXT, '') || '|' ||
        COALESCE(p_shooting_pctg::TEXT, '') || '|' ||
        COALESCE(p_avg_time_on_ice::TEXT, '') || '|' ||
        COALESCE(p_avg_shifts::TEXT, '') || '|' ||
        COALESCE(p_faceoff_win_pctg::TEXT, '')
    );
END;
$$ LANGUAGE plpgsql;

-- Function to insert with occurrence tracking
CREATE OR REPLACE FUNCTION insert_skaters_from_staging()
RETURNS TABLE(
    total_processed INTEGER,
    new_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    new_occurrences INTEGER
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
BEGIN
    -- Loop through each record in staging
    FOR rec IN 
        SELECT 
            "playerId", headshot, "firstName", "lastName", "positionCode", 
            "gamesPlayed", goals, assists, points, "plusMinus", "penaltyMinutes", 
            "powerPlayGoals", "shorthandedGoals", "gameWinningGoals", 
            "overtimeGoals", shots, "shootingPctg", "avgTimeOnIcePerGame", 
            "avgShiftsPerGame", "faceoffWinPctg", season, "gameType", "triCode"
        FROM staging1.skaters
    LOOP
        -- Generate hash for the new data
        new_hash := generate_skater_data_hash(
            rec."gamesPlayed", rec.goals, rec.assists, rec.points, rec."plusMinus",
            rec."penaltyMinutes", rec."powerPlayGoals", rec."shorthandedGoals",
            rec."gameWinningGoals", rec."overtimeGoals", rec.shots, rec."shootingPctg",
            rec."avgTimeOnIcePerGame", rec."avgShiftsPerGame", rec."faceoffWinPctg"
        );
        
        found_match := FALSE;
        
        -- Check the active record for this player/season/gameType/team combination
        SELECT * INTO matching_record FROM newapi.skaters 
        WHERE "playerId" = rec."playerId" 
        AND season = rec.season 
        AND "gameType" = rec."gameType" 
        AND "triCode" = rec."triCode"
        AND is_active = TRUE
        LIMIT 1;
        
        IF FOUND THEN
            -- Check if this record has the same data (hash match)
            IF matching_record.data_hash = new_hash THEN
                -- Data hasn't changed - just update the timestamp
                UPDATE newapi.skaters 
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = matching_record.id;
                
                unchanged_count := unchanged_count + 1;
                found_match := TRUE;
            ELSE
                -- Data has changed - deactivate the old record and mark for new insert
                UPDATE newapi.skaters 
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
            FROM newapi.skaters 
            WHERE "playerId" = rec."playerId" 
            AND season = rec.season 
            AND "gameType" = rec."gameType" 
            AND "triCode" = rec."triCode";
            
            -- Insert new record with the next occurrence number (active by default)
            INSERT INTO newapi.skaters (
                "playerId", headshot, "firstName", "lastName", "positionCode", 
                "gamesPlayed", goals, assists, points, "plusMinus", "penaltyMinutes", 
                "powerPlayGoals", "shorthandedGoals", "gameWinningGoals", 
                "overtimeGoals", shots, "shootingPctg", "avgTimeOnIcePerGame", 
                "avgShiftsPerGame", "faceoffWinPctg", season, "gameType", "triCode",
                occurrence_number, data_hash, is_active
            ) VALUES (
                rec."playerId", rec.headshot, rec."firstName", rec."lastName", rec."positionCode",
                rec."gamesPlayed", rec.goals, rec.assists, rec.points, rec."plusMinus", 
                rec."penaltyMinutes", rec."powerPlayGoals", rec."shorthandedGoals", 
                rec."gameWinningGoals", rec."overtimeGoals", rec.shots, rec."shootingPctg", 
                rec."avgTimeOnIcePerGame", rec."avgShiftsPerGame", rec."faceoffWinPctg", 
                rec.season, rec."gameType", rec."triCode", next_occurrence, new_hash, TRUE
            );
            
            IF next_occurrence = 1 THEN
                new_count := new_count + 1;
            ELSE
                occurrence_count := occurrence_count + 1;
            END IF;
        END IF;
        
        rows_processed := rows_processed + 1;
    END LOOP;
    
    RETURN QUERY SELECT rows_processed, new_count, updated_count, unchanged_count, occurrence_count;
END;
$$ LANGUAGE plpgsql;

-- Enhanced procedure with better reporting
CREATE OR REPLACE PROCEDURE sync_skaters_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM insert_skaters_from_staging() INTO result_record;
    
    RAISE NOTICE 'Sync completed:';
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  New player/team combinations: %', result_record.new_records;
    RAISE NOTICE '  Unchanged records (same data): %', result_record.unchanged_records;
    RAISE NOTICE '  New occurrences (same team, different stats): %', result_record.new_occurrences;
END;
$$;

-- View to show current/latest stats (only active records)
CREATE OR REPLACE VIEW newapi.skaters_current AS
SELECT * FROM newapi.skaters WHERE is_active = TRUE;

-- View to show players with multiple stints on same team
CREATE OR REPLACE VIEW newapi.skaters_multiple_stints AS
SELECT 
    s."playerId",
    s."firstName",
    s."lastName",
    s.season,
    s."gameType",
    s."triCode",
    s.occurrence_number,
    s.created_at,
    s.updated_at,
    s.goals,
    s.assists,
    s.points,
    s."gamesPlayed",
    -- Show progression between occurrences
    LAG(s.goals) OVER (PARTITION BY s."playerId", s.season, s."gameType", s."triCode" ORDER BY s.occurrence_number) as prev_goals,
    LAG(s.assists) OVER (PARTITION BY s."playerId", s.season, s."gameType", s."triCode" ORDER BY s.occurrence_number) as prev_assists,
    LAG(s.points) OVER (PARTITION BY s."playerId", s.season, s."gameType", s."triCode" ORDER BY s.occurrence_number) as prev_points,
    LAG(s."gamesPlayed") OVER (PARTITION BY s."playerId", s.season, s."gameType", s."triCode" ORDER BY s.occurrence_number) as prev_games
FROM newapi.skaters s
WHERE s."playerId" IN (
    SELECT "playerId" 
    FROM newapi.skaters 
    GROUP BY "playerId", season, "gameType", "triCode" 
    HAVING COUNT(*) > 1
)
ORDER BY s."playerId", s.season, s."gameType", s."triCode", s.occurrence_number;

-- Function to show statistics about multiple occurrences
CREATE OR REPLACE FUNCTION get_occurrence_stats()
RETURNS TABLE(
    player_id BIGINT,
    first_name TEXT,
    last_name TEXT,
    season_val BIGINT,
    game_type BIGINT,
    team TEXT,
    total_occurrences INTEGER,
    date_range TEXT,
    stat_progression TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s."playerId",
        s."firstName",
        s."lastName",
        s.season,
        s."gameType",
        s."triCode",
        COUNT(*)::INTEGER as total_occurrences,
        (MIN(s.created_at)::DATE || ' to ' || MAX(s.updated_at)::DATE) as date_range,
        'Games: ' || MIN(s."gamesPlayed") || '->' || MAX(s."gamesPlayed") || 
        ', Goals: ' || MIN(s.goals) || '->' || MAX(s.goals) || 
        ', Points: ' || MIN(s.points) || '->' || MAX(s.points) as stat_progression
    FROM newapi.skaters s
    GROUP BY s."playerId", s."firstName", s."lastName", s.season, s."gameType", s."triCode"
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC, s."playerId", s.season;
END;
$$ LANGUAGE plpgsql;

-- Create a table to store ETL run statistics
CREATE TABLE IF NOT EXISTS newapi.skaters_etl_log (
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
CREATE INDEX IF NOT EXISTS idx_skaters_etl_log_timestamp ON newapi.skaters_etl_log(run_timestamp);

-- Enhanced function that logs results
CREATE OR REPLACE FUNCTION insert_skaters_from_staging_with_logging()
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
            "playerId", headshot, "firstName", "lastName", "positionCode", 
            "gamesPlayed", goals, assists, points, "plusMinus", "penaltyMinutes", 
            "powerPlayGoals", "shorthandedGoals", "gameWinningGoals", 
            "overtimeGoals", shots, "shootingPctg", "avgTimeOnIcePerGame", 
            "avgShiftsPerGame", "faceoffWinPctg", season, "gameType", "triCode"
        FROM staging1.skaters
    LOOP
        -- Generate hash for the new data
        new_hash := generate_skater_data_hash(
            rec."gamesPlayed", rec.goals, rec.assists, rec.points, rec."plusMinus",
            rec."penaltyMinutes", rec."powerPlayGoals", rec."shorthandedGoals",
            rec."gameWinningGoals", rec."overtimeGoals", rec.shots, rec."shootingPctg",
            rec."avgTimeOnIcePerGame", rec."avgShiftsPerGame", rec."faceoffWinPctg"
        );
        
        found_match := FALSE;
        
        -- Check the active record for this player/season/gameType/team combination
        SELECT * INTO matching_record FROM newapi.skaters 
        WHERE "playerId" = rec."playerId" 
        AND season = rec.season 
        AND "gameType" = rec."gameType" 
        AND "triCode" = rec."triCode"
        AND is_active = TRUE
        LIMIT 1;
        
        IF FOUND THEN
            -- Check if this record has the same data (hash match)
            IF matching_record.data_hash = new_hash THEN
                -- Data hasn't changed - just update the timestamp
                UPDATE newapi.skaters 
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = matching_record.id;
                
                unchanged_count := unchanged_count + 1;
                found_match := TRUE;
            ELSE
                -- Data has changed - deactivate the old record and mark for new insert
                UPDATE newapi.skaters 
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
            FROM newapi.skaters 
            WHERE "playerId" = rec."playerId" 
            AND season = rec.season 
            AND "gameType" = rec."gameType" 
            AND "triCode" = rec."triCode";
            
            -- Insert new record with the next occurrence number (active by default)
            INSERT INTO newapi.skaters (
                "playerId", headshot, "firstName", "lastName", "positionCode", 
                "gamesPlayed", goals, assists, points, "plusMinus", "penaltyMinutes", 
                "powerPlayGoals", "shorthandedGoals", "gameWinningGoals", 
                "overtimeGoals", shots, "shootingPctg", "avgTimeOnIcePerGame", 
                "avgShiftsPerGame", "faceoffWinPctg", season, "gameType", "triCode",
                occurrence_number, data_hash, is_active
            ) VALUES (
                rec."playerId", rec.headshot, rec."firstName", rec."lastName", rec."positionCode",
                rec."gamesPlayed", rec.goals, rec.assists, rec.points, rec."plusMinus", 
                rec."penaltyMinutes", rec."powerPlayGoals", rec."shorthandedGoals", 
                rec."gameWinningGoals", rec."overtimeGoals", rec.shots, rec."shootingPctg", 
                rec."avgTimeOnIcePerGame", rec."avgShiftsPerGame", rec."faceoffWinPctg", 
                rec.season, rec."gameType", rec."triCode", next_occurrence, new_hash, TRUE
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
    INSERT INTO newapi.skaters_etl_log (
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
CREATE OR REPLACE PROCEDURE sync_skaters_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM insert_skaters_from_staging_with_logging() INTO result_record;
    
    RAISE NOTICE 'Sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  New player/team combinations: %', result_record.new_records;
    RAISE NOTICE '  Unchanged records (same data): %', result_record.unchanged_records;
    RAISE NOTICE '  New occurrences (same team, different stats): %', result_record.new_occurrences;
END;
$$;

-- View to see recent ETL runs
CREATE OR REPLACE VIEW newapi.skaters_etl_summary AS
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
FROM newapi.skaters_etl_log
ORDER BY run_timestamp DESC;

-- Execute the sync
CALL sync_skaters_from_staging();