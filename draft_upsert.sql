-- Ensure the schema exists
CREATE SCHEMA IF NOT EXISTS newapi;
CREATE SCHEMA IF NOT EXISTS staging1;

-- Drop existing objects to avoid conflicts when rerun
DROP FUNCTION IF EXISTS upsert_drafts_from_staging_with_logging() CASCADE;
DROP PROCEDURE IF EXISTS sync_drafts_from_staging() CASCADE;

-- Create the production drafts table if it doesn't exist
CREATE TABLE IF NOT EXISTS newapi."drafts" (
    "draftYear" INTEGER NOT NULL,
    "teamAbbrev" TEXT NOT NULL,
    "round" INTEGER NOT NULL,
    "pickInRound" INTEGER NOT NULL,
    "overallPick" INTEGER,
    "ordinalPick" TEXT,
    "teamId" BIGINT,
    "teamName" TEXT,
    "teamCommonName" TEXT,
    "teamPlaceNameWithPreposition" TEXT,
    "displayAbbrev" TEXT,
    "teamLogoLight" TEXT,
    "teamLogoDark" TEXT,
    "firstName" TEXT,
    "lastName" TEXT,
    "positionCode" TEXT,
    "countryCode" TEXT,
    "height" DOUBLE PRECISION,
    "weight" DOUBLE PRECISION,
    "amateurLeague" TEXT,
    "amateurClubName" TEXT,
    "playerId" BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique natural key for a draft pick
    UNIQUE ("draftYear", "teamAbbrev", "round", "pickInRound")
);

CREATE INDEX IF NOT EXISTS idx_drafts_natural_key
    ON newapi."drafts" ("draftYear", "teamAbbrev", "round", "pickInRound");

-- Create a table to store ETL run statistics for drafts
CREATE TABLE IF NOT EXISTS newapi.drafts_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_playerid_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_drafts_etl_log_timestamp
    ON newapi.drafts_etl_log (run_timestamp);

-- Function: Upsert drafts with playerId lookup and log results
CREATE OR REPLACE FUNCTION upsert_drafts_from_staging_with_logging()
RETURNS TABLE(
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_playerid_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL
) AS $$
DECLARE
    v_total INTEGER := 0;
    v_inserted INTEGER := 0;
    v_updated INTEGER := 0;
    v_unchanged INTEGER := 0;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN
    start_time := CURRENT_TIMESTAMP;

    WITH src_raw AS (
        SELECT
            NULLIF(d."draftYear"::text, '')::INTEGER AS "draftYear",
            d."teamAbbrev" AS "teamAbbrev",
            NULLIF(d."round"::text, '')::INTEGER AS "round",
            NULLIF(d."pickInRound"::text, '')::INTEGER AS "pickInRound",
            NULLIF(d."overallPick"::text, '')::INTEGER AS "overallPick",
            -- Compute ordinal pick (1st, 2nd, 3rd, 4th, etc.) mirroring the JS getOrdinalSuffix logic
            CASE
                WHEN NULLIF(d."overallPick"::text, '')::INTEGER IS NULL THEN NULL
                ELSE NULLIF(d."overallPick"::text, '')::INTEGER::text ||
                    CASE
                        WHEN (NULLIF(d."overallPick"::text, '')::INTEGER % 10 = 1
                              AND NULLIF(d."overallPick"::text, '')::INTEGER % 100 <> 11)
                            THEN 'st'
                        WHEN (NULLIF(d."overallPick"::text, '')::INTEGER % 10 = 2
                              AND NULLIF(d."overallPick"::text, '')::INTEGER % 100 <> 12)
                            THEN 'nd'
                        WHEN (NULLIF(d."overallPick"::text, '')::INTEGER % 10 = 3
                              AND NULLIF(d."overallPick"::text, '')::INTEGER % 100 <> 13)
                            THEN 'rd'
                        ELSE 'th'
                    END
            END AS "ordinalPick",
            NULLIF(d."teamId"::text, '')::BIGINT AS "teamId",
            d."teamName" AS "teamName",
            d."teamCommonName" AS "teamCommonName",
            d."teamPlaceNameWithPreposition" AS "teamPlaceNameWithPreposition",
            d."displayAbbrev" AS "displayAbbrev",
            d."teamLogoLight" AS "teamLogoLight",
            d."teamLogoDark" AS "teamLogoDark",
            d."firstName" AS "firstName",
            d."lastName" AS "lastName",
            d."positionCode" AS "positionCode",
            d."countryCode" AS "countryCode",
            NULLIF(d."height"::text, '')::DOUBLE PRECISION AS "height",
            NULLIF(d."weight"::text, '')::DOUBLE PRECISION AS "weight",
            d."amateurLeague" AS "amateurLeague",
            d."amateurClubName" AS "amateurClubName",
            p."playerId" AS "playerId"
        FROM staging1."drafts" d
        LEFT JOIN newapi."players" p
            ON NULLIF(d."draftYear"::text, '')::INTEGER = p."draftYear"
           AND d."teamAbbrev" = p."draftTeamAbbrev"
           AND NULLIF(d."round"::text, '')::INTEGER = p."draftRound"
           AND NULLIF(d."pickInRound"::text, '')::INTEGER = p."draftPickInRound"
    ), src AS (
        SELECT DISTINCT ON ("draftYear", "teamAbbrev", "round", "pickInRound") *
        FROM src_raw
        ORDER BY "draftYear", "teamAbbrev", "round", "pickInRound", "playerId" DESC NULLS LAST
    ), upsert AS (
        INSERT INTO newapi."drafts" (
            "draftYear", "teamAbbrev", "round", "pickInRound",
            "overallPick", "teamId", "teamName", "teamCommonName",
            "teamPlaceNameWithPreposition", "displayAbbrev", "teamLogoLight", "teamLogoDark",
            "firstName", "lastName", "positionCode", "countryCode",
            "height", "weight", "amateurLeague", "amateurClubName",
            "ordinalPick", "playerId"
        )
        SELECT
            s."draftYear", s."teamAbbrev", s."round", s."pickInRound",
            s."overallPick", s."teamId", s."teamName", s."teamCommonName",
            s."teamPlaceNameWithPreposition", s."displayAbbrev", s."teamLogoLight", s."teamLogoDark",
            s."firstName", s."lastName", s."positionCode", s."countryCode",
            s."height", s."weight", s."amateurLeague", s."amateurClubName",
            s."ordinalPick", s."playerId"
        FROM src s
        ON CONFLICT ("draftYear", "teamAbbrev", "round", "pickInRound") DO UPDATE
        SET
            "playerId" = COALESCE(EXCLUDED."playerId", newapi."drafts"."playerId"),
            "ordinalPick" = COALESCE(EXCLUDED."ordinalPick", newapi."drafts"."ordinalPick"),
            updated_at = CURRENT_TIMESTAMP
        WHERE (
            EXCLUDED."playerId" IS NOT NULL
            AND newapi."drafts"."playerId" IS DISTINCT FROM EXCLUDED."playerId"
        ) OR (
            EXCLUDED."ordinalPick" IS NOT NULL
            AND newapi."drafts"."ordinalPick" IS DISTINCT FROM EXCLUDED."ordinalPick"
        )
        RETURNING xmax
    ), counts AS (
        SELECT
            (SELECT COUNT(*) FROM src) AS total_src,
            COUNT(*) FILTER (WHERE xmax::text::bigint = 0) AS inserted_cnt,
            COUNT(*) FILTER (WHERE xmax::text::bigint <> 0) AS updated_cnt
        FROM upsert
    )
    SELECT total_src, inserted_cnt, updated_cnt
    INTO v_total, v_inserted, v_updated
    FROM counts;

    v_unchanged := COALESCE(v_total, 0) - COALESCE(v_inserted, 0) - COALESCE(v_updated, 0);

    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;

    INSERT INTO newapi.drafts_etl_log (
        total_processed, inserted_records, updated_playerid_records, unchanged_records, run_duration
    ) VALUES (
        COALESCE(v_total, 0), COALESCE(v_inserted, 0), COALESCE(v_updated, 0), COALESCE(v_unchanged, 0), duration
    );

    RETURN QUERY SELECT v_total, v_inserted, v_updated, v_unchanged, duration;
END;
$$ LANGUAGE plpgsql;

-- Procedure to run the upsert and echo results
CREATE OR REPLACE PROCEDURE sync_drafts_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM upsert_drafts_from_staging_with_logging() INTO result_record;

    RAISE NOTICE 'Drafts sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  Inserted records: %', result_record.inserted_records;
    RAISE NOTICE '  Updated playerId records: %', result_record.updated_playerid_records;
    RAISE NOTICE '  Unchanged records: %', result_record.unchanged_records;
END;
$$;

-- Summary view of recent drafts ETL runs
CREATE OR REPLACE VIEW newapi.drafts_etl_summary AS
SELECT 
    id,
    run_timestamp,
    total_processed,
    inserted_records,
    updated_playerid_records,
    unchanged_records,
    run_duration,
    ROUND(EXTRACT(EPOCH FROM run_duration)::NUMERIC, 2) AS duration_seconds,
    CASE 
        WHEN total_processed > 0 THEN ROUND((inserted_records + updated_playerid_records)::NUMERIC / total_processed * 100, 1)
        ELSE 0 
    END AS change_percentage
FROM newapi.drafts_etl_log
ORDER BY run_timestamp DESC;

-- If drafts_etl_log already existed before unchanged_records was added above,
-- apply a forward-compatible ALTER (safe to rerun).
ALTER TABLE IF EXISTS newapi.drafts_etl_log
    ADD COLUMN IF NOT EXISTS unchanged_records INTEGER;

-- Ensure `ordinalPick` column exists for forward-compatibility when re-running this script
ALTER TABLE IF EXISTS newapi."drafts"
    ADD COLUMN IF NOT EXISTS "ordinalPick" TEXT;

-- Optional immediate execution
-- CALL sync_drafts_from_staging();
