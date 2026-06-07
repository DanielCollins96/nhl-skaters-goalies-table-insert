CREATE SCHEMA IF NOT EXISTS staging1;
CREATE SCHEMA IF NOT EXISTS newapi;

DROP FUNCTION IF EXISTS upsert_daily_game_rosters_from_staging_with_logging() CASCADE;
DROP PROCEDURE IF EXISTS sync_daily_game_rosters_from_staging() CASCADE;

CREATE TABLE IF NOT EXISTS newapi.daily_game_rosters (
    schedule_date DATE NOT NULL,
    game_id BIGINT NOT NULL,
    team_side TEXT NOT NULL,
    team_abbreviation TEXT NOT NULL,
    opponent_abbreviation TEXT,
    position_group TEXT,
    player_id BIGINT NOT NULL,
    headshot TEXT,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT,
    sweater_number DOUBLE PRECISION,
    position_code TEXT,
    shoots_catches TEXT,
    height_in_inches BIGINT,
    weight_in_pounds BIGINT,
    height_in_centimeters BIGINT,
    weight_in_kilograms BIGINT,
    birth_date TEXT,
    birth_city TEXT,
    birth_country TEXT,
    birth_state_province TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (schedule_date, game_id, team_abbreviation, player_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_game_rosters_game
    ON newapi.daily_game_rosters (game_id);

CREATE INDEX IF NOT EXISTS idx_daily_game_rosters_team
    ON newapi.daily_game_rosters (team_abbreviation);

CREATE TABLE IF NOT EXISTS newapi.daily_game_rosters_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    roster_rows_processed INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

CREATE OR REPLACE FUNCTION upsert_daily_game_rosters_from_staging_with_logging()
RETURNS TABLE(
    roster_rows_processed INTEGER,
    run_duration INTERVAL
) AS $$
DECLARE
    v_roster_rows_processed INTEGER := 0;
    start_time TIMESTAMP;
    duration INTERVAL;
BEGIN
    start_time := CURRENT_TIMESTAMP;

    WITH roster_src AS (
        SELECT
            NULLIF(r."scheduleDate"::TEXT, '')::DATE AS schedule_date,
            NULLIF(r."gameId"::TEXT, '')::BIGINT AS game_id,
            r."teamSide" AS team_side,
            r."teamAbbreviation" AS team_abbreviation,
            r."opponentAbbreviation" AS opponent_abbreviation,
            r."positionGroup" AS position_group,
            NULLIF(r."playerId"::TEXT, '')::BIGINT AS player_id,
            r.headshot,
            r."firstName" AS first_name,
            r."lastName" AS last_name,
            r."fullName" AS full_name,
            NULLIF(r."sweaterNumber"::TEXT, '')::DOUBLE PRECISION AS sweater_number,
            r."positionCode" AS position_code,
            r."shootsCatches" AS shoots_catches,
            NULLIF(r."heightInInches"::TEXT, '')::BIGINT AS height_in_inches,
            NULLIF(r."weightInPounds"::TEXT, '')::BIGINT AS weight_in_pounds,
            NULLIF(r."heightInCentimeters"::TEXT, '')::BIGINT AS height_in_centimeters,
            NULLIF(r."weightInKilograms"::TEXT, '')::BIGINT AS weight_in_kilograms,
            r."birthDate" AS birth_date,
            r."birthCity" AS birth_city,
            r."birthCountry" AS birth_country,
            r."birthStateProvince" AS birth_state_province
        FROM staging1.daily_game_rosters r
        WHERE NULLIF(r."gameId"::TEXT, '')::BIGINT IS NOT NULL
          AND NULLIF(r."playerId"::TEXT, '')::BIGINT IS NOT NULL
          AND r."teamAbbreviation" IS NOT NULL
    ), roster_upsert AS (
        INSERT INTO newapi.daily_game_rosters (
            schedule_date,
            game_id,
            team_side,
            team_abbreviation,
            opponent_abbreviation,
            position_group,
            player_id,
            headshot,
            first_name,
            last_name,
            full_name,
            sweater_number,
            position_code,
            shoots_catches,
            height_in_inches,
            weight_in_pounds,
            height_in_centimeters,
            weight_in_kilograms,
            birth_date,
            birth_city,
            birth_country,
            birth_state_province
        )
        SELECT
            schedule_date,
            game_id,
            team_side,
            team_abbreviation,
            opponent_abbreviation,
            position_group,
            player_id,
            headshot,
            first_name,
            last_name,
            full_name,
            sweater_number,
            position_code,
            shoots_catches,
            height_in_inches,
            weight_in_pounds,
            height_in_centimeters,
            weight_in_kilograms,
            birth_date,
            birth_city,
            birth_country,
            birth_state_province
        FROM roster_src
        ON CONFLICT (schedule_date, game_id, team_abbreviation, player_id) DO UPDATE
        SET
            team_side = EXCLUDED.team_side,
            opponent_abbreviation = EXCLUDED.opponent_abbreviation,
            position_group = EXCLUDED.position_group,
            headshot = EXCLUDED.headshot,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            full_name = EXCLUDED.full_name,
            sweater_number = EXCLUDED.sweater_number,
            position_code = EXCLUDED.position_code,
            shoots_catches = EXCLUDED.shoots_catches,
            height_in_inches = EXCLUDED.height_in_inches,
            weight_in_pounds = EXCLUDED.weight_in_pounds,
            height_in_centimeters = EXCLUDED.height_in_centimeters,
            weight_in_kilograms = EXCLUDED.weight_in_kilograms,
            birth_date = EXCLUDED.birth_date,
            birth_city = EXCLUDED.birth_city,
            birth_country = EXCLUDED.birth_country,
            birth_state_province = EXCLUDED.birth_state_province,
            updated_at = CURRENT_TIMESTAMP
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_roster_rows_processed
    FROM roster_upsert;

    duration := CURRENT_TIMESTAMP - start_time;

    INSERT INTO newapi.daily_game_rosters_etl_log (
        roster_rows_processed,
        run_duration
    ) VALUES (
        v_roster_rows_processed,
        duration
    );

    RETURN QUERY SELECT v_roster_rows_processed, duration;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE sync_daily_game_rosters_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM upsert_daily_game_rosters_from_staging_with_logging() INTO result_record;

    RAISE NOTICE 'Daily game roster sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Roster rows processed: %', result_record.roster_rows_processed;
END;
$$;
