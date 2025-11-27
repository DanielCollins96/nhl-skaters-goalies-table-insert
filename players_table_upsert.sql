-- Drop existing functions first to avoid return type conflicts
DROP FUNCTION IF EXISTS insert_players_from_staging() CASCADE;
DROP FUNCTION IF EXISTS insert_players_from_staging_with_logging() CASCADE;
DROP FUNCTION IF EXISTS generate_player_data_hash(BOOLEAN, DOUBLE PRECISION, TEXT, TEXT, DOUBLE PRECISION, TEXT, TEXT, TEXT, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, DOUBLE PRECISION, TEXT, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION) CASCADE;
DROP PROCEDURE IF EXISTS sync_players_from_staging() CASCADE;

DROP TABLE IF EXISTS newapi.players CASCADE;

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

-- Create a table to store ETL run statistics
CREATE TABLE IF NOT EXISTS newapi.players_etl_log (
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
CREATE INDEX IF NOT EXISTS idx_players_etl_log_timestamp ON newapi.players_etl_log(run_timestamp);

-- Function to generate a hash of the important player data fields
CREATE OR REPLACE FUNCTION generate_player_data_hash(
    p_is_active BOOLEAN,
    p_current_team_id DOUBLE PRECISION,
    p_current_team_abbrev TEXT,
    p_full_team_name TEXT,
    p_sweater_number DOUBLE PRECISION,
    p_position TEXT,
    p_headshot TEXT,
    p_hero_image TEXT,
    p_height_in_inches DOUBLE PRECISION,
    p_height_in_centimeters DOUBLE PRECISION,
    p_weight_in_pounds DOUBLE PRECISION,
    p_weight_in_kilograms DOUBLE PRECISION,
    p_birth_date TEXT,
    p_birth_city TEXT,
    p_birth_state_province TEXT,
    p_birth_country TEXT,
    p_shoots_catches TEXT,
    p_in_top_100_all_time BIGINT,
    p_in_hhof BIGINT,
    p_draft_year DOUBLE PRECISION,
    p_draft_team_abbrev TEXT,
    p_draft_round DOUBLE PRECISION,
    p_draft_pick_in_round DOUBLE PRECISION,
    p_draft_overall_pick DOUBLE PRECISION
) RETURNS TEXT AS $$
BEGIN
    RETURN md5(
        COALESCE(p_is_active::TEXT, '') || '|' ||
        COALESCE(p_current_team_id::TEXT, '') || '|' ||
        COALESCE(p_current_team_abbrev, '') || '|' ||
        COALESCE(p_full_team_name, '') || '|' ||
        COALESCE(p_sweater_number::TEXT, '') || '|' ||
        COALESCE(p_position, '') || '|' ||
        COALESCE(p_headshot, '') || '|' ||
        COALESCE(p_hero_image, '') || '|' ||
        COALESCE(p_height_in_inches::TEXT, '') || '|' ||
        COALESCE(p_height_in_centimeters::TEXT, '') || '|' ||
        COALESCE(p_weight_in_pounds::TEXT, '') || '|' ||
        COALESCE(p_weight_in_kilograms::TEXT, '') || '|' ||
        COALESCE(p_birth_date, '') || '|' ||
        COALESCE(p_birth_city, '') || '|' ||
        COALESCE(p_birth_state_province, '') || '|' ||
        COALESCE(p_birth_country, '') || '|' ||
        COALESCE(p_shoots_catches, '') || '|' ||
        COALESCE(p_in_top_100_all_time::TEXT, '') || '|' ||
        COALESCE(p_in_hhof::TEXT, '') || '|' ||
        COALESCE(p_draft_year::TEXT, '') || '|' ||
        COALESCE(p_draft_team_abbrev, '') || '|' ||
        COALESCE(p_draft_round::TEXT, '') || '|' ||
        COALESCE(p_draft_pick_in_round::TEXT, '') || '|' ||
        COALESCE(p_draft_overall_pick::TEXT, '')
    );
END;
$$ LANGUAGE plpgsql;

-- Enhanced function that logs results
CREATE OR REPLACE FUNCTION insert_players_from_staging_with_logging()
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
            "playerId", "isActive", "currentTeamId", "currentTeamAbbrev", 
            "fullTeamName.default" as fullTeamName, 
            "firstName.default" as firstName, 
            "lastName.default" as lastName, 
            "sweaterNumber", "position", headshot, "heroImage", 
            "heightInInches", "heightInCentimeters", "weightInPounds", "weightInKilograms", 
            "birthDate", "birthCity.default" as birthCity, 
            "birthStateProvince.default" as birthStateProvince, 
            "birthCountry", "shootsCatches", "playerSlug", 
            "inTop100AllTime", "inHHOF", 
            "draftDetails.year" as draftYear, 
            "draftDetails.teamAbbrev" as draftTeamAbbrev, 
            "draftDetails.round" as draftRound, 
            "draftDetails.pickInRound" as draftPickInRound, 
            "draftDetails.overallPick" as draftOverallPick
        FROM staging1.player
    LOOP
        -- Generate hash for the new data
        new_hash := generate_player_data_hash(
            rec."isActive", rec."currentTeamId", rec."currentTeamAbbrev", rec.fullTeamName,
            rec."sweaterNumber", rec."position", rec.headshot, rec."heroImage",
            rec."heightInInches", rec."heightInCentimeters", rec."weightInPounds", rec."weightInKilograms",
            rec."birthDate", rec.birthCity, rec.birthStateProvince, rec."birthCountry",
            rec."shootsCatches", rec."inTop100AllTime", rec."inHHOF",
            rec.draftYear, rec.draftTeamAbbrev, rec.draftRound, rec.draftPickInRound, rec.draftOverallPick
        );
        
        found_match := FALSE;
        
        -- Check all existing records for this player
        FOR matching_record IN
            SELECT * FROM newapi.players 
            WHERE "playerId" = rec."playerId"
            ORDER BY occurrence_number
        LOOP
            -- Check if this record has the same data (hash match)
            IF matching_record.data_hash = new_hash THEN
                -- Data hasn't changed - just update the timestamp
                UPDATE newapi.players 
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = matching_record.id;
                
                unchanged_count := unchanged_count + 1;
                found_match := TRUE;
                EXIT; -- Break out of the loop
            END IF;
        END LOOP;
        
        -- If no matching hash was found, we need to insert
        IF NOT found_match THEN
            -- Get the next occurrence number for this player
            SELECT COALESCE(MAX(occurrence_number), 0) + 1 INTO next_occurrence
            FROM newapi.players 
            WHERE "playerId" = rec."playerId";
            
            -- Insert new record with the next occurrence number
            INSERT INTO newapi.players (
                "playerId", "isActive", "currentTeamId", "currentTeamAbbrev", "fullTeamName",
                "firstName", "lastName", "sweaterNumber", "position", headshot, "heroImage",
                "heightInInches", "heightInCentimeters", "weightInPounds", "weightInKilograms",
                "birthDate", "birthCity", "birthStateProvince", "birthCountry",
                "shootsCatches", "playerSlug", "inTop100AllTime", "inHHOF",
                "draftYear", "draftTeamAbbrev", "draftRound", "draftPickInRound", "draftOverallPick",
                occurrence_number, data_hash
            ) VALUES (
                rec."playerId", rec."isActive", rec."currentTeamId"::BIGINT, rec."currentTeamAbbrev", rec.fullTeamName,
                rec.firstName, rec.lastName, rec."sweaterNumber"::INTEGER, rec."position", rec.headshot, rec."heroImage",
                rec."heightInInches", rec."heightInCentimeters", rec."weightInPounds", rec."weightInKilograms",
                CASE WHEN rec."birthDate" IS NOT NULL AND rec."birthDate" != '' THEN rec."birthDate"::DATE ELSE NULL END, 
                rec.birthCity, rec.birthStateProvince, rec."birthCountry",
                rec."shootsCatches", rec."playerSlug", (rec."inTop100AllTime" = 1), (rec."inHHOF" = 1),
                rec.draftYear::INTEGER, rec.draftTeamAbbrev, rec.draftRound::INTEGER, rec.draftPickInRound::INTEGER, rec.draftOverallPick::INTEGER,
                next_occurrence, new_hash
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
    INSERT INTO newapi.players_etl_log (
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
CREATE OR REPLACE PROCEDURE sync_players_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM insert_players_from_staging_with_logging() INTO result_record;
    
    RAISE NOTICE 'Players sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  New players: %', result_record.new_records;
    RAISE NOTICE '  Unchanged records (same data): %', result_record.unchanged_records;
    RAISE NOTICE '  New occurrences (updates): %', result_record.new_occurrences;
END;
$$;

-- View to show current/latest player info (highest occurrence number)
CREATE OR REPLACE VIEW newapi.players_current AS
SELECT p.* FROM newapi.players p
INNER JOIN (
    SELECT "playerId", MAX(occurrence_number) as max_occurrence
    FROM newapi.players
    GROUP BY "playerId"
) latest ON p."playerId" = latest."playerId" 
    AND p.occurrence_number = latest.max_occurrence;

-- View to see recent ETL runs
CREATE OR REPLACE VIEW newapi.players_etl_summary AS
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
FROM newapi.players_etl_log
ORDER BY run_timestamp DESC;

-- Execute the sync
CALL sync_players_from_staging();
