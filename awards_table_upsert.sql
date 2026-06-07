CREATE SCHEMA IF NOT EXISTS newapi;
CREATE SCHEMA IF NOT EXISTS staging1;

DROP FUNCTION IF EXISTS upsert_awards_from_staging_with_logging() CASCADE;
DROP PROCEDURE IF EXISTS sync_awards_from_staging() CASCADE;

CREATE TABLE IF NOT EXISTS newapi.awards (
    id SERIAL PRIMARY KEY,
    "playerId" BIGINT NOT NULL,
    trophy_default TEXT NOT NULL,
    trophy_fr TEXT,
    "seasonId" BIGINT NOT NULL,
    "gamesPlayed" DOUBLE PRECISION,
    goals DOUBLE PRECISION,
    assists DOUBLE PRECISION,
    points DOUBLE PRECISION,
    "plusMinus" DOUBLE PRECISION,
    pim DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE ("playerId", trophy_default, "seasonId")
);

ALTER TABLE newapi.awards
    ALTER COLUMN "seasonId" TYPE BIGINT USING "seasonId"::BIGINT,
    ALTER COLUMN "gamesPlayed" TYPE DOUBLE PRECISION USING "gamesPlayed"::DOUBLE PRECISION,
    ALTER COLUMN goals TYPE DOUBLE PRECISION USING goals::DOUBLE PRECISION,
    ALTER COLUMN assists TYPE DOUBLE PRECISION USING assists::DOUBLE PRECISION,
    ALTER COLUMN points TYPE DOUBLE PRECISION USING points::DOUBLE PRECISION,
    ALTER COLUMN "plusMinus" TYPE DOUBLE PRECISION USING "plusMinus"::DOUBLE PRECISION,
    ALTER COLUMN pim TYPE DOUBLE PRECISION USING pim::DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_awards_player_id
    ON newapi.awards ("playerId");

CREATE INDEX IF NOT EXISTS idx_awards_trophy
    ON newapi.awards (trophy_default);

CREATE INDEX IF NOT EXISTS idx_awards_season
    ON newapi.awards ("seasonId");

CREATE TABLE IF NOT EXISTS newapi.awards_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_awards_etl_log_timestamp
    ON newapi.awards_etl_log (run_timestamp);

CREATE OR REPLACE FUNCTION upsert_awards_from_staging_with_logging()
RETURNS TABLE(
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
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

    IF to_regclass('staging1.award') IS NULL THEN
        end_time := CURRENT_TIMESTAMP;
        duration := end_time - start_time;

        INSERT INTO newapi.awards_etl_log (
            total_processed, inserted_records, updated_records, unchanged_records, run_duration, notes
        ) VALUES (
            0, 0, 0, 0, duration, 'staging1.award did not exist'
        );

        RETURN QUERY SELECT 0::INTEGER, 0::INTEGER, 0::INTEGER, 0::INTEGER, duration;
        RETURN;
    END IF;

    WITH src AS (
        SELECT DISTINCT ON (
            NULLIF(a."playerId"::text, '')::DOUBLE PRECISION::BIGINT,
            NULLIF(a.trophy_default::text, ''),
            NULLIF(a."seasonId"::text, '')::DOUBLE PRECISION::BIGINT
        )
            NULLIF(a."playerId"::text, '')::DOUBLE PRECISION::BIGINT AS "playerId",
            NULLIF(a.trophy_default::text, '') AS trophy_default,
            a.trophy_fr AS trophy_fr,
            NULLIF(a."seasonId"::text, '')::DOUBLE PRECISION::BIGINT AS "seasonId",
            NULLIF(a."gamesPlayed"::text, '')::DOUBLE PRECISION AS "gamesPlayed",
            NULLIF(a.goals::text, '')::DOUBLE PRECISION AS goals,
            NULLIF(a.assists::text, '')::DOUBLE PRECISION AS assists,
            NULLIF(a.points::text, '')::DOUBLE PRECISION AS points,
            NULLIF(a."plusMinus"::text, '')::DOUBLE PRECISION AS "plusMinus",
            NULLIF(a.pim::text, '')::DOUBLE PRECISION AS pim
        FROM staging1.award a
        WHERE NULLIF(a."playerId"::text, '') IS NOT NULL
          AND NULLIF(a.trophy_default::text, '') IS NOT NULL
          AND NULLIF(a."seasonId"::text, '') IS NOT NULL
        ORDER BY
            NULLIF(a."playerId"::text, '')::DOUBLE PRECISION::BIGINT,
            NULLIF(a.trophy_default::text, ''),
            NULLIF(a."seasonId"::text, '')::DOUBLE PRECISION::BIGINT
    ), upsert AS (
        INSERT INTO newapi.awards (
            "playerId", trophy_default, trophy_fr, "seasonId", "gamesPlayed",
            goals, assists, points, "plusMinus", pim
        )
        SELECT
            s."playerId", s.trophy_default, s.trophy_fr, s."seasonId", s."gamesPlayed",
            s.goals, s.assists, s.points, s."plusMinus", s.pim
        FROM src s
        ON CONFLICT ("playerId", trophy_default, "seasonId") DO UPDATE
        SET
            trophy_fr = EXCLUDED.trophy_fr,
            "gamesPlayed" = EXCLUDED."gamesPlayed",
            goals = EXCLUDED.goals,
            assists = EXCLUDED.assists,
            points = EXCLUDED.points,
            "plusMinus" = EXCLUDED."plusMinus",
            pim = EXCLUDED.pim,
            updated_at = CURRENT_TIMESTAMP
        WHERE (
            EXCLUDED.trophy_fr IS DISTINCT FROM newapi.awards.trophy_fr
            OR EXCLUDED."gamesPlayed" IS DISTINCT FROM newapi.awards."gamesPlayed"
            OR EXCLUDED.goals IS DISTINCT FROM newapi.awards.goals
            OR EXCLUDED.assists IS DISTINCT FROM newapi.awards.assists
            OR EXCLUDED.points IS DISTINCT FROM newapi.awards.points
            OR EXCLUDED."plusMinus" IS DISTINCT FROM newapi.awards."plusMinus"
            OR EXCLUDED.pim IS DISTINCT FROM newapi.awards.pim
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

    INSERT INTO newapi.awards_etl_log (
        total_processed, inserted_records, updated_records, unchanged_records, run_duration
    ) VALUES (
        COALESCE(v_total, 0), COALESCE(v_inserted, 0), COALESCE(v_updated, 0), COALESCE(v_unchanged, 0), duration
    );

    RETURN QUERY SELECT v_total, v_inserted, v_updated, v_unchanged, duration;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE sync_awards_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM upsert_awards_from_staging_with_logging() INTO result_record;

    RAISE NOTICE 'Awards sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  Inserted records: %', result_record.inserted_records;
    RAISE NOTICE '  Updated records: %', result_record.updated_records;
    RAISE NOTICE '  Unchanged records: %', result_record.unchanged_records;
END;
$$;

CREATE OR REPLACE VIEW newapi.awards_etl_summary AS
SELECT
    id,
    run_timestamp,
    total_processed,
    inserted_records,
    updated_records,
    unchanged_records,
    run_duration,
    ROUND(EXTRACT(EPOCH FROM run_duration)::NUMERIC, 2) AS duration_seconds,
    CASE
        WHEN total_processed > 0 THEN ROUND((inserted_records + updated_records)::NUMERIC / total_processed * 100, 1)
        ELSE 0
    END AS change_percentage
FROM newapi.awards_etl_log
ORDER BY run_timestamp DESC;
