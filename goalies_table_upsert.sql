-- Drop existing functions first to avoid return type conflicts
DROP FUNCTION IF EXISTS insert_goalies_from_staging() CASCADE;
DROP FUNCTION IF EXISTS insert_goalies_from_staging_with_logging() CASCADE;
DROP FUNCTION IF EXISTS get_goalies_occurrence_stats() CASCADE;
DROP FUNCTION IF EXISTS generate_goalie_data_hash(BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE PRECISION) CASCADE;
DROP PROCEDURE IF EXISTS sync_goalies_from_staging() CASCADE;

DROP TABLE IF EXISTS newapi.goalies CASCADE;

-- Create the production goalies table with occurrence tracking
CREATE TABLE newapi.goalies (
    id SERIAL PRIMARY KEY,
    "playerId" BIGINT,
    headshot TEXT,
    "firstName" TEXT,
    "lastName" TEXT,
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

-- Create a table to store ETL run statistics
CREATE TABLE IF NOT EXISTS newapi.goalies_etl_log (
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
CREATE INDEX IF NOT EXISTS idx_goalies_etl_log_timestamp ON newapi.goalies_etl_log(run_timestamp);

-- Function to generate a hash of the important goalie data fields
CREATE OR REPLACE FUNCTION generate_goalie_data_hash(
    p_games_played BIGINT,
    p_games_started BIGINT,
    p_wins BIGINT,
    p_losses BIGINT,
    p_overtime_losses DOUBLE PRECISION,
    p_goals_against_avg DOUBLE PRECISION,
    p_save_percentage DOUBLE PRECISION,
    p_shots_against DOUBLE PRECISION,
    p_saves DOUBLE PRECISION,
    p_goals_against BIGINT,
    p_shutouts BIGINT,
    p_goals BIGINT,
    p_assists BIGINT,
    p_points BIGINT,
    p_penalty_minutes BIGINT,
    p_time_on_ice BIGINT,
    p_ties DOUBLE PRECISION
) RETURNS TEXT AS $$
BEGIN
    RETURN md5(
        COALESCE(p_games_played::TEXT, '') || '|' ||
        COALESCE(p_games_started::TEXT, '') || '|' ||
        COALESCE(p_wins::TEXT, '') || '|' ||
        COALESCE(p_losses::TEXT, '') || '|' ||
        COALESCE(p_overtime_losses::TEXT, '') || '|' ||
        COALESCE(p_goals_against_avg::TEXT, '') || '|' ||
        COALESCE(p_save_percentage::TEXT, '') || '|' ||
        COALESCE(p_shots_against::TEXT, '') || '|' ||
        COALESCE(p_saves::TEXT, '') || '|' ||
        COALESCE(p_goals_against::TEXT, '') || '|' ||
        COALESCE(p_shutouts::TEXT, '') || '|' ||
        COALESCE(p_goals::TEXT, '') || '|' ||
        COALESCE(p_assists::TEXT, '') || '|' ||
        COALESCE(p_points::TEXT, '') || '|' ||
        COALESCE(p_penalty_minutes::TEXT, '') || '|' ||
        COALESCE(p_time_on_ice::TEXT, '') || '|' ||
        COALESCE(p_ties::TEXT, '')
    );
END;
$$ LANGUAGE plpgsql;

-- Enhanced function that logs results
CREATE OR REPLACE FUNCTION insert_goalies_from_staging_with_logging()
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
            "playerId", headshot, "firstName", "lastName", "gamesPlayed", 
            "gamesStarted", wins, losses, "overtimeLosses", "goalsAgainstAverage", 
            "savePercentage", "shotsAgainst", saves, "goalsAgainst", shutouts, 
            goals, assists, points, "penaltyMinutes", "timeOnIce", ties,
            season, "gameType", team
        FROM staging1.goalies
    LOOP
        -- Generate hash for the new data
        new_hash := generate_goalie_data_hash(
            rec."gamesPlayed", rec."gamesStarted", rec.wins, rec.losses, rec."overtimeLosses",
            rec."goalsAgainstAverage", rec."savePercentage", rec."shotsAgainst", rec.saves,
            rec."goalsAgainst", rec.shutouts, rec.goals, rec.assists, rec.points,
            rec."penaltyMinutes", rec."timeOnIce", rec.ties
        );
        
        found_match := FALSE;
        
        -- Check the active record for this player/season/gameType/team combination
        SELECT * INTO matching_record FROM newapi.goalies 
        WHERE "playerId" = rec."playerId" 
        AND season = rec.season 
        AND "gameType" = rec."gameType" 
        AND team = rec.team
        AND is_active = TRUE
        LIMIT 1;
        
        IF FOUND THEN
            -- Check if this record has the same data (hash match)
            IF matching_record.data_hash = new_hash THEN
                -- Data hasn't changed - just update the timestamp
                UPDATE newapi.goalies 
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = matching_record.id;
                
                unchanged_count := unchanged_count + 1;
                found_match := TRUE;
            ELSE
                -- Data has changed - deactivate the old record and mark for new insert
                UPDATE newapi.goalies 
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
            FROM newapi.goalies 
            WHERE "playerId" = rec."playerId" 
            AND season = rec.season 
            AND "gameType" = rec."gameType" 
            AND team = rec.team;
            
            -- Insert new record with the next occurrence number (active by default)
            INSERT INTO newapi.goalies (
                "playerId", headshot, "firstName", "lastName", "gamesPlayed", 
                "gamesStarted", wins, losses, "overtimeLosses", "goalsAgainstAverage", 
                "savePercentage", "shotsAgainst", saves, "goalsAgainst", shutouts, 
                goals, assists, points, "penaltyMinutes", "timeOnIce", ties,
                season, "gameType", team, occurrence_number, data_hash, is_active
            ) VALUES (
                rec."playerId", rec.headshot, rec."firstName", rec."lastName", rec."gamesPlayed",
                rec."gamesStarted", rec.wins, rec.losses, rec."overtimeLosses", rec."goalsAgainstAverage",
                rec."savePercentage", rec."shotsAgainst", rec.saves, rec."goalsAgainst", rec.shutouts,
                rec.goals, rec.assists, rec.points, rec."penaltyMinutes", rec."timeOnIce", rec.ties,
                rec.season, rec."gameType", rec.team, next_occurrence, new_hash, TRUE
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
    INSERT INTO newapi.goalies_etl_log (
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
CREATE OR REPLACE PROCEDURE sync_goalies_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM insert_goalies_from_staging_with_logging() INTO result_record;
    
    RAISE NOTICE 'Goalies sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  New player/team combinations: %', result_record.new_records;
    RAISE NOTICE '  Unchanged records (same data): %', result_record.unchanged_records;
    RAISE NOTICE '  New occurrences (same team, different stats): %', result_record.new_occurrences;
END;
$$;

-- View to show current/latest goalie stats (only active records)
CREATE OR REPLACE VIEW newapi.goalies_current AS
SELECT * FROM newapi.goalies WHERE is_active = TRUE;

-- View to show goalies with multiple stints on same team
CREATE OR REPLACE VIEW newapi.goalies_multiple_stints AS
SELECT 
    g."playerId",
    g."firstName",
    g."lastName",
    g.season,
    g."gameType",
    g.team,
    g.occurrence_number,
    g.created_at,
    g.updated_at,
    g.wins,
    g.losses,
    g."gamesPlayed",
    g."savePercentage",
    g."goalsAgainstAverage",
    -- Show progression between occurrences
    LAG(g.wins) OVER (PARTITION BY g."playerId", g.season, g."gameType", g.team ORDER BY g.occurrence_number) as prev_wins,
    LAG(g.losses) OVER (PARTITION BY g."playerId", g.season, g."gameType", g.team ORDER BY g.occurrence_number) as prev_losses,
    LAG(g."gamesPlayed") OVER (PARTITION BY g."playerId", g.season, g."gameType", g.team ORDER BY g.occurrence_number) as prev_games,
    LAG(g."savePercentage") OVER (PARTITION BY g."playerId", g.season, g."gameType", g.team ORDER BY g.occurrence_number) as prev_save_pct
FROM newapi.goalies g
WHERE g."playerId" IN (
    SELECT "playerId" 
    FROM newapi.goalies 
    GROUP BY "playerId", season, "gameType", team 
    HAVING COUNT(*) > 1
)
ORDER BY g."playerId", g.season, g."gameType", g.team, g.occurrence_number;

-- Function to show statistics about multiple occurrences
CREATE OR REPLACE FUNCTION get_goalies_occurrence_stats()
RETURNS TABLE(
    player_id BIGINT,
    first_name TEXT,
    last_name TEXT,
    season_val BIGINT,
    game_type BIGINT,
    team_name TEXT,
    total_occurrences INTEGER,
    date_range TEXT,
    stat_progression TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        g."playerId",
        g."firstName",
        g."lastName",
        g.season,
        g."gameType",
        g.team,
        COUNT(*)::INTEGER as total_occurrences,
        (MIN(g.created_at)::DATE || ' to ' || MAX(g.updated_at)::DATE) as date_range,
        'Games: ' || MIN(g."gamesPlayed") || '->' || MAX(g."gamesPlayed") || 
        ', Wins: ' || MIN(g.wins) || '->' || MAX(g.wins) || 
        ', Save%: ' || ROUND(MIN(g."savePercentage")::NUMERIC, 3) || '->' || ROUND(MAX(g."savePercentage")::NUMERIC, 3) as stat_progression
    FROM newapi.goalies g
    GROUP BY g."playerId", g."firstName", g."lastName", g.season, g."gameType", g.team
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC, g."playerId", g.season;
END;
$$ LANGUAGE plpgsql;

-- View to see recent ETL runs
CREATE OR REPLACE VIEW newapi.goalies_etl_summary AS
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
FROM newapi.goalies_etl_log
ORDER BY run_timestamp DESC;

-- Execute the sync
CALL sync_goalies_from_staging();