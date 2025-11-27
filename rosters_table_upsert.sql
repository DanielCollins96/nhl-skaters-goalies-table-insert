-- Drop existing functions first to avoid return type conflicts
DROP FUNCTION IF EXISTS insert_rosters_from_staging() CASCADE;
DROP FUNCTION IF EXISTS insert_rosters_from_staging_with_logging() CASCADE;
DROP FUNCTION IF EXISTS get_rosters_occurrence_stats() CASCADE;
DROP FUNCTION IF EXISTS generate_roster_data_hash(TEXT, TEXT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, BIGINT, BIGINT, TEXT, TEXT, TEXT, TEXT) CASCADE;
DROP PROCEDURE IF EXISTS sync_rosters_from_staging() CASCADE;

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

-- Create a table to store ETL run statistics
CREATE TABLE IF NOT EXISTS newapi.rosters_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_processed INTEGER,
    new_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    new_occurrences INTEGER,
    deactivated_records INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

-- Create index for querying recent runs
CREATE INDEX IF NOT EXISTS idx_rosters_etl_log_timestamp ON newapi.rosters_etl_log(run_timestamp);

-- Function to generate a hash of the important roster data fields
CREATE OR REPLACE FUNCTION generate_roster_data_hash(
    p_team_abbreviation TEXT,
    p_position_group TEXT,
    p_player_id BIGINT,
    p_headshot TEXT,
    p_first_name TEXT,
    p_last_name TEXT,
    p_sweater_number TEXT,
    p_position_code TEXT,
    p_shoots_catches TEXT,
    p_height_inches BIGINT,
    p_weight_pounds BIGINT,
    p_height_cm BIGINT,
    p_weight_kg BIGINT,
    p_birth_date TEXT,
    p_birth_city TEXT,
    p_birth_country TEXT,
    p_birth_state_province TEXT
) RETURNS TEXT AS $$
BEGIN
    RETURN md5(
        COALESCE(p_team_abbreviation, '') || '|' ||
        COALESCE(p_position_group, '') || '|' ||
        COALESCE(p_player_id::TEXT, '') || '|' ||
        COALESCE(p_headshot, '') || '|' ||
        COALESCE(p_first_name, '') || '|' ||
        COALESCE(p_last_name, '') || '|' ||
        COALESCE(p_sweater_number, '') || '|' ||
        COALESCE(p_position_code, '') || '|' ||
        COALESCE(p_shoots_catches, '') || '|' ||
        COALESCE(p_height_inches::TEXT, '') || '|' ||
        COALESCE(p_weight_pounds::TEXT, '') || '|' ||
        COALESCE(p_height_cm::TEXT, '') || '|' ||
        COALESCE(p_weight_kg::TEXT, '') || '|' ||
        COALESCE(p_birth_date, '') || '|' ||
        COALESCE(p_birth_city, '') || '|' ||
        COALESCE(p_birth_country, '') || '|' ||
        COALESCE(p_birth_state_province, '')
    );
END;
$$ LANGUAGE plpgsql;

-- Enhanced function that logs results and manages active status
CREATE OR REPLACE FUNCTION insert_rosters_from_staging_with_logging()
RETURNS TABLE(
    total_processed INTEGER,
    new_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    new_occurrences INTEGER,
    deactivated_records INTEGER,
    run_duration INTERVAL
) AS $$
DECLARE
    rows_processed INTEGER := 0;
    new_count INTEGER := 0;
    updated_count INTEGER := 0;
    unchanged_count INTEGER := 0;
    occurrence_count INTEGER := 0;
    deactivated_count INTEGER := 0;
    rec RECORD;
    matching_record RECORD;
    new_hash TEXT;
    next_occurrence INTEGER;
    found_match BOOLEAN;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
    staging_player_ids BIGINT[];
BEGIN
    start_time := CURRENT_TIMESTAMP;
    
    -- Collect all player IDs from the current staging batch
    SELECT ARRAY_AGG(DISTINCT "playerId") INTO staging_player_ids
    FROM staging1.current_rosters;
    
    -- First, mark all existing records as inactive
    -- They will be reactivated if they appear in the current staging batch
    UPDATE newapi.current_rosters 
    SET active = FALSE, updated_at = CURRENT_TIMESTAMP
    WHERE active = TRUE;
    
    -- Loop through each record in staging
    FOR rec IN 
        SELECT 
            "teamAbbreviation", "positionGroup", "playerId", headshot,
            "firstName", "lastName", "sweaterNumber", "positionCode",
            "shootsCatches", "heightInInches", "weightInPounds",
            "heightInCentimeters", "weightInKilograms", "birthDate",
            "birthCity", "birthCountry", "birthStateProvince"
        FROM staging1.current_rosters
    LOOP
        -- Generate hash for the new data
        new_hash := generate_roster_data_hash(
            rec."teamAbbreviation", rec."positionGroup", rec."playerId", rec.headshot,
            rec."firstName", rec."lastName", rec."sweaterNumber"::TEXT, rec."positionCode",
            rec."shootsCatches", rec."heightInInches", rec."weightInPounds",
            rec."heightInCentimeters", rec."weightInKilograms", rec."birthDate",
            rec."birthCity", rec."birthCountry", rec."birthStateProvince"
        );
        
        found_match := FALSE;
        
        -- Check all existing records for this player/team combination
        FOR matching_record IN
            SELECT * FROM newapi.current_rosters 
            WHERE "playerId" = rec."playerId" 
            AND "teamAbbreviation" = rec."teamAbbreviation"
            ORDER BY occurrence_number
        LOOP
            -- Check if this record has the same data (hash match)
            IF matching_record.data_hash = new_hash THEN
                -- Data hasn't changed - reactivate and update timestamp
                UPDATE newapi.current_rosters 
                SET active = TRUE, updated_at = CURRENT_TIMESTAMP
                WHERE id = matching_record.id;
                
                unchanged_count := unchanged_count + 1;
                found_match := TRUE;
                EXIT; -- Break out of the loop
            END IF;
        END LOOP;
        
        -- If no matching hash was found, we need to insert a new record
        IF NOT found_match THEN
            -- Get the next occurrence number for this combination
            SELECT COALESCE(MAX(occurrence_number), 0) + 1 INTO next_occurrence
            FROM newapi.current_rosters 
            WHERE "playerId" = rec."playerId" 
            AND "teamAbbreviation" = rec."teamAbbreviation";
            
            -- Insert new record with the next occurrence number
            INSERT INTO newapi.current_rosters (
                "teamAbbreviation", "positionGroup", "playerId", headshot,
                "firstName", "lastName", "sweaterNumber", "positionCode",
                "shootsCatches", "heightInInches", "weightInPounds",
                "heightInCentimeters", "weightInKilograms", "birthDate",
                "birthCity", "birthCountry", "birthStateProvince",
                active, occurrence_number, data_hash
            ) VALUES (
                rec."teamAbbreviation", rec."positionGroup", rec."playerId", rec.headshot,
                rec."firstName", rec."lastName", rec."sweaterNumber", rec."positionCode",
                rec."shootsCatches", rec."heightInInches", rec."weightInPounds",
                rec."heightInCentimeters", rec."weightInKilograms", rec."birthDate",
                rec."birthCity", rec."birthCountry", rec."birthStateProvince",
                TRUE, next_occurrence, new_hash
            );
            
            IF next_occurrence = 1 THEN
                new_count := new_count + 1;
            ELSE
                occurrence_count := occurrence_count + 1;
            END IF;
        END IF;
        
        rows_processed := rows_processed + 1;
    END LOOP;
    
    -- Count how many records remain inactive (no longer in staging)
    SELECT COUNT(*) INTO deactivated_count
    FROM newapi.current_rosters
    WHERE active = FALSE;
    
    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;
    
    -- Log the results
    INSERT INTO newapi.rosters_etl_log (
        total_processed, new_records, updated_records, unchanged_records, 
        new_occurrences, deactivated_records, run_duration
    ) VALUES (
        rows_processed, new_count, updated_count, unchanged_count, 
        occurrence_count, deactivated_count, duration
    );
    
    RETURN QUERY SELECT rows_processed, new_count, updated_count, unchanged_count, occurrence_count, deactivated_count, duration;
END;
$$ LANGUAGE plpgsql;

-- Updated procedure to use the logging function
CREATE OR REPLACE PROCEDURE sync_rosters_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM insert_rosters_from_staging_with_logging() INTO result_record;
    
    RAISE NOTICE 'Rosters sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  New player/team combinations: %', result_record.new_records;
    RAISE NOTICE '  Unchanged records (same data): %', result_record.unchanged_records;
    RAISE NOTICE '  New occurrences (same team, different data): %', result_record.new_occurrences;
    RAISE NOTICE '  Deactivated records (not in current batch): %', result_record.deactivated_records;
END;
$$;

-- View to show only active roster players (latest occurrence for each player/team)
CREATE OR REPLACE VIEW newapi.rosters_active AS
SELECT r.* FROM newapi.current_rosters r
WHERE r.active = TRUE;

-- View to show current/latest roster entry (highest occurrence number for each player/team)
CREATE OR REPLACE VIEW newapi.rosters_latest AS
SELECT r.* FROM newapi.current_rosters r
INNER JOIN (
    SELECT "playerId", "teamAbbreviation", MAX(occurrence_number) as max_occurrence
    FROM newapi.current_rosters
    GROUP BY "playerId", "teamAbbreviation"
) latest ON r."playerId" = latest."playerId" 
    AND r."teamAbbreviation" = latest."teamAbbreviation"
    AND r.occurrence_number = latest.max_occurrence;

-- View to show players with multiple roster entries for same team (e.g., traded and came back)
CREATE OR REPLACE VIEW newapi.rosters_multiple_stints AS
SELECT 
    r."playerId",
    r."firstName",
    r."lastName",
    r."teamAbbreviation",
    r."positionCode",
    r."sweaterNumber",
    r.occurrence_number,
    r.active,
    r.created_at,
    r.updated_at,
    -- Show progression between occurrences
    LAG(r."sweaterNumber") OVER (PARTITION BY r."playerId", r."teamAbbreviation" ORDER BY r.occurrence_number) as prev_sweater_number,
    LAG(r."positionCode") OVER (PARTITION BY r."playerId", r."teamAbbreviation" ORDER BY r.occurrence_number) as prev_position
FROM newapi.current_rosters r
WHERE r."playerId" IN (
    SELECT "playerId" 
    FROM newapi.current_rosters 
    GROUP BY "playerId", "teamAbbreviation" 
    HAVING COUNT(*) > 1
)
ORDER BY r."playerId", r."teamAbbreviation", r.occurrence_number;

-- Function to show statistics about multiple occurrences
CREATE OR REPLACE FUNCTION get_rosters_occurrence_stats()
RETURNS TABLE(
    player_id BIGINT,
    first_name TEXT,
    last_name TEXT,
    team TEXT,
    position_code TEXT,
    total_occurrences INTEGER,
    is_active BOOLEAN,
    date_range TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        r."playerId",
        r."firstName",
        r."lastName",
        r."teamAbbreviation",
        r."positionCode",
        COUNT(*)::INTEGER as total_occurrences,
        BOOL_OR(r.active) as is_active,
        (MIN(r.created_at)::DATE || ' to ' || MAX(r.updated_at)::DATE) as date_range
    FROM newapi.current_rosters r
    GROUP BY r."playerId", r."firstName", r."lastName", r."teamAbbreviation", r."positionCode"
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC, r."playerId";
END;
$$ LANGUAGE plpgsql;

-- View to see team roster summaries
CREATE OR REPLACE VIEW newapi.team_roster_summary AS
SELECT 
    "teamAbbreviation",
    COUNT(*) FILTER (WHERE active = TRUE) as active_players,
    COUNT(*) FILTER (WHERE active = FALSE) as inactive_players,
    COUNT(*) FILTER (WHERE active = TRUE AND "positionGroup" = 'forwards') as active_forwards,
    COUNT(*) FILTER (WHERE active = TRUE AND "positionGroup" = 'defensemen') as active_defensemen,
    COUNT(*) FILTER (WHERE active = TRUE AND "positionGroup" = 'goalies') as active_goalies
FROM newapi.current_rosters
GROUP BY "teamAbbreviation"
ORDER BY "teamAbbreviation";

-- View to see recent ETL runs
CREATE OR REPLACE VIEW newapi.rosters_etl_summary AS
SELECT 
    id,
    run_timestamp,
    total_processed,
    new_records,
    unchanged_records,
    new_occurrences,
    deactivated_records,
    run_duration,
    ROUND(EXTRACT(EPOCH FROM run_duration)::NUMERIC, 2) as duration_seconds,
    CASE 
        WHEN total_processed > 0 THEN ROUND((new_records + new_occurrences)::NUMERIC / total_processed * 100, 1)
        ELSE 0 
    END as change_percentage
FROM newapi.rosters_etl_log
ORDER BY run_timestamp DESC;

-- Execute the sync
CALL sync_rosters_from_staging();
