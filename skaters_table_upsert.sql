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
    "firstName.default" TEXT,
    "lastName.default" TEXT,
    "firstName.cs" TEXT,
    "firstName.sk" TEXT,
    "lastName.cs" TEXT,
    "lastName.sk" TEXT,
    "firstName.de" TEXT,
    "firstName.es" TEXT,
    "firstName.fi" TEXT,
    "firstName.sv" TEXT,
    "lastName.de" TEXT,
    "lastName.fi" TEXT,
    "lastName.sv" TEXT,
    "lastName.es" TEXT,
    season BIGINT,
    "gameType" BIGINT,
    abbreviation TEXT,
    occurrence_number INTEGER DEFAULT 1,  -- 1st, 2nd, 3rd time with same team in season
    data_hash TEXT,                       -- Hash of key data fields to detect actual changes
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent exact duplicates
    UNIQUE("playerId", season, "gameType", abbreviation, occurrence_number)
);

-- Create indexes for better performance
CREATE INDEX idx_skaters_player_id ON newapi.skaters("playerId");
CREATE INDEX idx_skaters_season ON newapi.skaters(season);
CREATE INDEX idx_skaters_team ON newapi.skaters(abbreviation);
CREATE INDEX idx_skaters_occurrence ON newapi.skaters("playerId", season, "gameType", abbreviation, occurrence_number);

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
            "playerId", headshot, "positionCode", "gamesPlayed", 
            goals, assists, points, "plusMinus", "penaltyMinutes", 
            "powerPlayGoals", "shorthandedGoals", "gameWinningGoals", 
            "overtimeGoals", shots, "shootingPctg", "avgTimeOnIcePerGame", 
            "avgShiftsPerGame", "faceoffWinPctg", "firstName.default", 
            "lastName.default", "firstName.cs", "firstName.sk", 
            "lastName.cs", "lastName.sk", "firstName.de", "firstName.es", 
            "firstName.fi", "firstName.sv", "lastName.de", "lastName.fi", 
            "lastName.sv", "lastName.es", season, "gameType", abbreviation
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
        
        -- Check all existing records for this player/season/gameType/team combination
        FOR matching_record IN
            SELECT * FROM newapi.skaters 
            WHERE "playerId" = rec."playerId" 
            AND season = rec.season 
            AND "gameType" = rec."gameType" 
            AND abbreviation = rec.abbreviation
            ORDER BY occurrence_number
        LOOP
            -- Check if this record has the same data (hash match)
            IF matching_record.data_hash = new_hash THEN
                -- Data hasn't changed - just update the timestamp
                UPDATE newapi.skaters 
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = matching_record.id;
                
                unchanged_count := unchanged_count + 1;
                found_match := TRUE;
                EXIT; -- Break out of the loop
            END IF;
        END LOOP;
        
        -- If no matching hash was found, we need to insert
        IF NOT found_match THEN
            -- Get the next occurrence number for this combination
            SELECT COALESCE(MAX(occurrence_number), 0) + 1 INTO next_occurrence
            FROM newapi.skaters 
            WHERE "playerId" = rec."playerId" 
            AND season = rec.season 
            AND "gameType" = rec."gameType" 
            AND abbreviation = rec.abbreviation;
            
            -- Insert new record with the next occurrence number
            INSERT INTO newapi.skaters (
                "playerId", headshot, "positionCode", "gamesPlayed", 
                goals, assists, points, "plusMinus", "penaltyMinutes", 
                "powerPlayGoals", "shorthandedGoals", "gameWinningGoals", 
                "overtimeGoals", shots, "shootingPctg", "avgTimeOnIcePerGame", 
                "avgShiftsPerGame", "faceoffWinPctg", "firstName.default", 
                "lastName.default", "firstName.cs", "firstName.sk", 
                "lastName.cs", "lastName.sk", "firstName.de", "firstName.es", 
                "firstName.fi", "firstName.sv", "lastName.de", "lastName.fi", 
                "lastName.sv", "lastName.es", season, "gameType", abbreviation,
                occurrence_number, data_hash
            ) VALUES (
                rec."playerId", rec.headshot, rec."positionCode", rec."gamesPlayed",
                rec.goals, rec.assists, rec.points, rec."plusMinus", rec."penaltyMinutes",
                rec."powerPlayGoals", rec."shorthandedGoals", rec."gameWinningGoals",
                rec."overtimeGoals", rec.shots, rec."shootingPctg", rec."avgTimeOnIcePerGame",
                rec."avgShiftsPerGame", rec."faceoffWinPctg", rec."firstName.default",
                rec."lastName.default", rec."firstName.cs", rec."firstName.sk",
                rec."lastName.cs", rec."lastName.sk", rec."firstName.de", rec."firstName.es",
                rec."firstName.fi", rec."firstName.sv", rec."lastName.de", rec."lastName.fi",
                rec."lastName.sv", rec."lastName.es", rec.season, rec."gameType", rec.abbreviation,
                next_occurrence,
                new_hash
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

-- View to show current/latest stats (highest occurrence number for each combination)
CREATE OR REPLACE VIEW newapi.skaters_current AS
SELECT s.* FROM newapi.skaters s
INNER JOIN (
    SELECT "playerId", season, "gameType", abbreviation, MAX(occurrence_number) as max_occurrence
    FROM newapi.skaters
    GROUP BY "playerId", season, "gameType", abbreviation
) latest ON s."playerId" = latest."playerId" 
    AND s.season = latest.season 
    AND s."gameType" = latest."gameType" 
    AND s.abbreviation = latest.abbreviation
    AND s.occurrence_number = latest.max_occurrence;

-- View to show players with multiple stints on same team
CREATE OR REPLACE VIEW newapi.skaters_multiple_stints AS
SELECT 
    s."playerId",
    s.season,
    s."gameType",
    s.abbreviation,
    s.occurrence_number,
    s.created_at,
    s.updated_at,
    s.goals,
    s.assists,
    s.points,
    s."gamesPlayed",
    -- Show progression between occurrences
    LAG(s.goals) OVER (PARTITION BY s."playerId", s.season, s."gameType", s.abbreviation ORDER BY s.occurrence_number) as prev_goals,
    LAG(s.assists) OVER (PARTITION BY s."playerId", s.season, s."gameType", s.abbreviation ORDER BY s.occurrence_number) as prev_assists,
    LAG(s.points) OVER (PARTITION BY s."playerId", s.season, s."gameType", s.abbreviation ORDER BY s.occurrence_number) as prev_points,
    LAG(s."gamesPlayed") OVER (PARTITION BY s."playerId", s.season, s."gameType", s.abbreviation ORDER BY s.occurrence_number) as prev_games
FROM newapi.skaters s
WHERE s."playerId" IN (
    SELECT "playerId" 
    FROM newapi.skaters 
    GROUP BY "playerId", season, "gameType", abbreviation 
    HAVING COUNT(*) > 1
)
ORDER BY s."playerId", s.season, s."gameType", s.abbreviation, s.occurrence_number;

-- Function to show statistics about multiple occurrences
CREATE OR REPLACE FUNCTION get_occurrence_stats()
RETURNS TABLE(
    player_id BIGINT,
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
        s.season,
        s."gameType",
        s.abbreviation,
        COUNT(*)::INTEGER as total_occurrences,
        (MIN(s.created_at)::DATE || ' to ' || MAX(s.updated_at)::DATE) as date_range,
        'Games: ' || MIN(s."gamesPlayed") || '->' || MAX(s."gamesPlayed") || 
        ', Goals: ' || MIN(s.goals) || '->' || MAX(s.goals) || 
        ', Points: ' || MIN(s.points) || '->' || MAX(s.points) as stat_progression
    FROM newapi.skaters s
    GROUP BY s."playerId", s.season, s."gameType", s.abbreviation
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC, s."playerId", s.season;
END;
$$ LANGUAGE plpgsql;

-- Execute the sync
CALL sync_skaters_from_staging();