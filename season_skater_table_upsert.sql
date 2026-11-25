-- SAFE TO RE-RUN against populated production (live RDS).
-- This file never DROPs production tables. Schema uses CREATE TABLE IF NOT EXISTS
-- and CREATE INDEX IF NOT EXISTS; routines use CREATE OR REPLACE.
-- Greenfield wipe/recreate: bootstrap/season_skater_table_bootstrap.sql
-- (requires SET app.allow_bootstrap = 'on' in the same session).

CREATE SCHEMA IF NOT EXISTS newapi;

-- Drop existing functions first to avoid return type conflicts
DROP FUNCTION IF EXISTS insert_season_skaters_from_staging() CASCADE;
DROP FUNCTION IF EXISTS insert_season_skaters_from_staging_with_logging() CASCADE;
DROP FUNCTION IF EXISTS get_season_skaters_occurrence_stats() CASCADE;
DROP FUNCTION IF EXISTS generate_season_skater_data_hash(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION) CASCADE;
DROP PROCEDURE IF EXISTS sync_season_skaters_from_staging() CASCADE;

-- Create the production season_skater table with occurrence tracking
CREATE TABLE IF NOT EXISTS newapi.season_skater (
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

-- Create indexes for better performance.
-- No separate ("playerId"), occurrence, or is_active index: the UNIQUE key above already
-- starts with "playerId" and covers the same columns, and with in-place updates
-- there is one row per key, so it serves the active-row lookup too.
CREATE INDEX IF NOT EXISTS idx_season_skater_season ON newapi.season_skater(season);
CREATE INDEX IF NOT EXISTS idx_season_skater_team ON newapi.season_skater("teamName.default");
CREATE INDEX IF NOT EXISTS idx_season_skater_league ON newapi.season_skater("leagueAbbrev");

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

-- Average TOI in whole seconds. Landing sends 'MM:SS' ("23:40"); club-stats
-- sends fractional seconds ("858.89"). Rounding makes the two agree, so they
-- don't keep overwriting each other by a fraction of a second. Anything else
-- is NULL.
CREATE OR REPLACE FUNCTION toi_seconds(p_value TEXT)
RETURNS DOUBLE PRECISION
LANGUAGE sql IMMUTABLE AS $$
    SELECT ROUND(CASE
        WHEN TRIM(p_value) ~ '^\d+(\.\d+)?$' THEN TRIM(p_value)::double precision
        WHEN TRIM(p_value) ~ '^\d+:\d{1,2}$' THEN
            split_part(TRIM(p_value), ':', 1)::double precision * 60
            + split_part(TRIM(p_value), ':', 2)::double precision
    END)
$$;

-- Upsert landing or club-stats rows into newapi.season_skater.
-- One row per key, updated in place. The two sources each leave some stats
-- out (landing: none now that TOI parses; club-stats: powerPlayPoints,
-- shorthandedPoints, sometimes plusMinus), so a NULL coming in keeps the
-- stored value instead of wiping it. Only the old rows from before this
-- change have is_active = FALSE; nothing new is deactivated.
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
    v_toi DOUBLE PRECISION;
    next_occurrence INTEGER;
    found_match BOOLEAN;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN
    start_time := clock_timestamp();

    -- Loop through each record in staging.
    -- Keys: NULLIF+TRIM then ::double precision::bigint so '' and '2024.0'
    -- do not abort the batch. Skip rows whose required keys are unusable.
    FOR rec IN 
        SELECT 
            NULLIF(TRIM("playerId"::text), '')::double precision::bigint AS "playerId",
            assists,
            NULLIF(TRIM("gameTypeId"::text), '')::double precision::bigint AS "gameTypeId",
            "gamesPlayed", goals, "leagueAbbrev", 
            pim, "plusMinus", points,
            NULLIF(TRIM(season::text), '')::double precision::bigint AS season,
            NULLIF(TRIM(sequence::text), '')::double precision::bigint AS sequence,
            "teamName.default", 
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
        WHERE NULLIF(TRIM("playerId"::text), '') IS NOT NULL
          AND NULLIF(TRIM("gameTypeId"::text), '') IS NOT NULL
          AND NULLIF(TRIM(season::text), '') IS NOT NULL
          AND NULLIF(TRIM(sequence::text), '') IS NOT NULL
    LOOP
        -- Staging types from pandas can be bigint, float or text, so every
        -- value is cast explicitly.
        v_toi := toi_seconds(rec."avgToi"::text);
        found_match := FALSE;

        -- The active row for this player/season/sequence/team/gameType/league.
        SELECT * INTO matching_record FROM newapi.season_skater
        WHERE "playerId" = rec."playerId"
        AND season = rec.season
        AND sequence = rec.sequence
        AND "teamName.default" = rec."teamName.default"
        AND "gameTypeId" = rec."gameTypeId"
        AND "leagueAbbrev" = rec."leagueAbbrev"
        AND is_active = TRUE
        LIMIT 1;
        found_match := FOUND;

        -- Same team under another sequence: club-stats added this stint after
        -- a trade and guessed its number before landing had it. Take this
        -- row's number instead of adding a second row for the same stint,
        -- unless this staging batch also has that other sequence (a player
        -- who returned to a team has two real stints with it).
        IF NOT found_match THEN
            SELECT ss.* INTO matching_record FROM newapi.season_skater ss
            WHERE ss."playerId" = rec."playerId"
            AND ss.season = rec.season
            AND ss."teamName.default" = rec."teamName.default"
            AND ss."gameTypeId" = rec."gameTypeId"
            AND ss."leagueAbbrev" = rec."leagueAbbrev"
            AND ss.is_active = TRUE
            AND NOT EXISTS (
                SELECT 1 FROM staging1.season_skater st
                WHERE NULLIF(TRIM(st."playerId"::text), '')::double precision::bigint = ss."playerId"
                AND NULLIF(TRIM(st.season::text), '')::double precision::bigint = ss.season
                AND NULLIF(TRIM(st.sequence::text), '')::double precision::bigint = ss.sequence
                AND NULLIF(TRIM(st."gameTypeId"::text), '')::double precision::bigint = ss."gameTypeId"
                AND st."teamName.default" = ss."teamName.default"
                AND st."leagueAbbrev" = ss."leagueAbbrev"
            )
            ORDER BY ss.sequence
            LIMIT 1;

            IF FOUND THEN
                UPDATE newapi.season_skater SET sequence = rec.sequence, updated_at = CURRENT_TIMESTAMP
                WHERE id = matching_record.id;
                matching_record.sequence := rec.sequence;
                found_match := TRUE;
            END IF;
        END IF;

        IF found_match THEN
            -- Landing's MM:SS and club-stats' fractional seconds can round a
            -- second apart. Keep the stored TOI inside that, or the two
            -- sources rewrite the row every run.
            IF v_toi IS NOT NULL AND matching_record."avgToi" IS NOT NULL
               AND ABS(v_toi - matching_record."avgToi") <= 1 THEN
                v_toi := matching_record."avgToi";
            END IF;

            -- Hash the row as it will look after the merge: the incoming value,
            -- or the stored one when the incoming value is NULL.
            new_hash := generate_season_skater_data_hash(
                COALESCE(rec.assists::double precision, matching_record.assists),
                COALESCE(rec."gamesPlayed"::double precision, matching_record."gamesPlayed"),
                COALESCE(rec.goals::double precision, matching_record.goals),
                COALESCE(rec.pim::double precision, matching_record.pim),
                COALESCE(rec."plusMinus"::double precision, matching_record."plusMinus"),
                COALESCE(rec.points::double precision, matching_record.points),
                COALESCE(rec.shots::double precision, matching_record.shots),
                COALESCE(rec."faceoffWinningPctg"::double precision, matching_record."faceoffWinningPctg"),
                COALESCE(rec."shootingPctg"::double precision, matching_record."shootingPctg"),
                COALESCE(rec."powerPlayGoals"::double precision, matching_record."powerPlayGoals"),
                COALESCE(rec."shorthandedGoals"::double precision, matching_record."shorthandedGoals"),
                COALESCE(rec."gameWinningGoals"::double precision, matching_record."gameWinningGoals"),
                COALESCE(v_toi, matching_record."avgToi"),
                COALESCE(rec."otGoals"::double precision, matching_record."otGoals"),
                COALESCE(rec."powerPlayPoints"::double precision, matching_record."powerPlayPoints"),
                COALESCE(rec."shorthandedPoints"::double precision, matching_record."shorthandedPoints")
            );

            IF matching_record.data_hash IS NOT DISTINCT FROM new_hash THEN
                unchanged_count := unchanged_count + 1;
            ELSE
                UPDATE newapi.season_skater SET
                    assists = COALESCE(rec.assists::double precision::bigint, assists),
                    "gamesPlayed" = COALESCE(rec."gamesPlayed"::double precision::bigint, "gamesPlayed"),
                    goals = COALESCE(rec.goals::double precision::bigint, goals),
                    pim = COALESCE(rec.pim::double precision::bigint, pim),
                    "plusMinus" = COALESCE(rec."plusMinus"::double precision::bigint, "plusMinus"),
                    points = COALESCE(rec.points::double precision::bigint, points),
                    "faceoffWinningPctg" = COALESCE(rec."faceoffWinningPctg"::double precision, "faceoffWinningPctg"),
                    "shootingPctg" = COALESCE(rec."shootingPctg"::double precision, "shootingPctg"),
                    shots = COALESCE(rec.shots::double precision, shots),
                    "powerPlayGoals" = COALESCE(rec."powerPlayGoals"::double precision, "powerPlayGoals"),
                    "shorthandedGoals" = COALESCE(rec."shorthandedGoals"::double precision, "shorthandedGoals"),
                    "gameWinningGoals" = COALESCE(rec."gameWinningGoals"::double precision, "gameWinningGoals"),
                    "avgToi" = COALESCE(v_toi, "avgToi"),
                    "otGoals" = COALESCE(rec."otGoals"::double precision, "otGoals"),
                    "powerPlayPoints" = COALESCE(rec."powerPlayPoints"::double precision, "powerPlayPoints"),
                    "shorthandedPoints" = COALESCE(rec."shorthandedPoints"::double precision, "shorthandedPoints"),
                    "teamCommonName.default" = COALESCE(rec."teamCommonName.default", "teamCommonName.default"),
                    "teamPlaceNameWithPreposition.default" = COALESCE(rec."teamPlaceNameWithPreposition.default", "teamPlaceNameWithPreposition.default"),
                    data_hash = new_hash,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = matching_record.id;

                updated_count := updated_count + 1;
            END IF;
        END IF;

        -- No active row yet: insert one.
        IF NOT found_match THEN
            new_hash := generate_season_skater_data_hash(
                rec.assists::double precision,
                rec."gamesPlayed"::double precision,
                rec.goals::double precision,
                rec.pim::double precision,
                rec."plusMinus"::double precision,
                rec.points::double precision,
                rec.shots::double precision,
                rec."faceoffWinningPctg"::double precision,
                rec."shootingPctg"::double precision,
                rec."powerPlayGoals"::double precision,
                rec."shorthandedGoals"::double precision,
                rec."gameWinningGoals"::double precision,
                v_toi,
                rec."otGoals"::double precision,
                rec."powerPlayPoints"::double precision,
                rec."shorthandedPoints"::double precision
            );

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
                rec."playerId", rec.assists::double precision::bigint, rec."gameTypeId",
                rec."gamesPlayed"::double precision::bigint, rec.goals::double precision::bigint,
                rec."leagueAbbrev", rec.pim::double precision::bigint,
                rec."plusMinus"::double precision::bigint, rec.points::double precision::bigint,
                rec.season, rec.sequence, rec."teamName.default",
                rec."faceoffWinningPctg"::double precision, rec."shootingPctg"::double precision,
                rec.shots::double precision, rec."powerPlayGoals"::double precision,
                rec."shorthandedGoals"::double precision, rec."gameWinningGoals"::double precision,
                rec."teamCommonName.default", rec."teamCommonName.cs",
                rec."teamCommonName.de", rec."teamCommonName.es", rec."teamCommonName.fi",
                rec."teamCommonName.sk", rec."teamCommonName.sv", rec."teamName.cs",
                rec."teamName.de", rec."teamName.fi", rec."teamName.sk", rec."teamName.sv",
                rec."teamPlaceNameWithPreposition.default",
                v_toi,
                rec."otGoals"::double precision, rec."powerPlayPoints"::double precision,
                rec."shorthandedPoints"::double precision, rec."teamName.fr",
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
    
    end_time := clock_timestamp();
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

-- Live RDS: this whole file is safe to re-apply. Do not run
-- bootstrap/season_skater_table_bootstrap.sql against populated production.
-- CALL sync_season_skaters_from_staging();
-- After club-stats CALL sync_skaters_from_staging(), also:
--   CALL sync_season_skaters_from_club_stats();
-- (sync_season_from_club_stats.sql — every ETL, not a one-off backfill)
