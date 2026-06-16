CREATE SCHEMA IF NOT EXISTS staging1;
CREATE SCHEMA IF NOT EXISTS newapi;
CREATE SCHEMA IF NOT EXISTS readmodel;

-- Drop existing routines first to avoid return type conflicts
DROP FUNCTION IF EXISTS insert_player_contracts_from_staging_with_logging() CASCADE;
DROP PROCEDURE IF EXISTS sync_player_contracts_from_staging() CASCADE;

CREATE TABLE IF NOT EXISTS newapi.player_external_ids (
    "playerId" BIGINT NOT NULL,
    source TEXT NOT NULL,
    source_slug TEXT NOT NULL,
    source_url TEXT,
    matched_by TEXT,
    confidence NUMERIC(5, 4),
    last_verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("playerId", source)
);

CREATE TABLE IF NOT EXISTS newapi.player_contracts (
    contract_id TEXT PRIMARY KEY,
    "playerId" BIGINT NOT NULL,
    source TEXT NOT NULL DEFAULT 'puckpedia',
    start_season TEXT,
    end_season TEXT,
    signing_date DATE,
    signing_team TEXT,
    contract_type TEXT,
    term_years INTEGER,
    total_value BIGINT,
    cap_hit BIGINT,
    aav BIGINT,
    signing_status TEXT,
    expiry_status TEXT,
    expiry_year INTEGER,
    expiry_age INTEGER,
    signing_age INTEGER,
    signing_gm TEXT,
    signing_agent TEXT,
    qualifying_offer BIGINT,
    arbitration_eligible BOOLEAN,
    notes TEXT,
    source_url TEXT,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_player_contracts_player_id
    ON newapi.player_contracts ("playerId");
CREATE INDEX IF NOT EXISTS idx_player_contracts_player_season
    ON newapi.player_contracts ("playerId", start_season DESC);
CREATE INDEX IF NOT EXISTS idx_player_contracts_source
    ON newapi.player_contracts (source);

CREATE TABLE IF NOT EXISTS newapi.player_contract_seasons (
    contract_id TEXT NOT NULL REFERENCES newapi.player_contracts(contract_id) ON DELETE CASCADE,
    "playerId" BIGINT NOT NULL,
    season TEXT NOT NULL,
    team_id BIGINT,
    cap_hit BIGINT,
    aav BIGINT,
    base_salary BIGINT,
    performance_bonus BIGINT,
    signing_bonus BIGINT,
    total_salary BIGINT,
    minors_salary BIGINT,
    clause TEXT,
    is_slide_year BOOLEAN NOT NULL DEFAULT FALSE,
    is_ufa_year BOOLEAN NOT NULL DEFAULT FALSE,
    buyout_url TEXT,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (contract_id, season)
);

CREATE INDEX IF NOT EXISTS idx_player_contract_seasons_player_id
    ON newapi.player_contract_seasons ("playerId");
CREATE INDEX IF NOT EXISTS idx_player_contract_seasons_player_season
    ON newapi.player_contract_seasons ("playerId", season DESC);

-- Create a table to store ETL run statistics
CREATE TABLE IF NOT EXISTS newapi.player_contracts_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    total_contracts_processed INTEGER,
    contracts_inserted INTEGER,
    contracts_updated INTEGER,
    total_seasons_processed INTEGER,
    seasons_inserted INTEGER,
    seasons_updated INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_player_contracts_etl_log_timestamp
    ON newapi.player_contracts_etl_log(run_timestamp);

CREATE TABLE IF NOT EXISTS newapi.player_contract_scrape_status (
    "playerId" BIGINT NOT NULL,
    source TEXT NOT NULL,
    source_slug TEXT,
    status TEXT NOT NULL,
    status_code INTEGER,
    error TEXT,
    last_attempted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("playerId", source)
);

CREATE INDEX IF NOT EXISTS idx_player_contract_scrape_status_status
    ON newapi.player_contract_scrape_status (source, status);

CREATE OR REPLACE FUNCTION newapi.is_nullish_text(value TEXT)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT lower(coalesce(value, '')) IN ('', 'nan', 'none', 'nat')
$$;

CREATE OR REPLACE FUNCTION newapi.safe_bigint(value TEXT)
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE WHEN newapi.is_nullish_text(value) THEN NULL ELSE value::NUMERIC::BIGINT END
$$;

CREATE OR REPLACE FUNCTION newapi.safe_integer(value TEXT)
RETURNS INTEGER
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE WHEN newapi.is_nullish_text(value) THEN NULL ELSE value::NUMERIC::INTEGER END
$$;

CREATE OR REPLACE FUNCTION newapi.safe_numeric_5_4(value TEXT)
RETURNS NUMERIC(5, 4)
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE WHEN newapi.is_nullish_text(value) THEN NULL ELSE value::NUMERIC(5, 4) END
$$;

CREATE OR REPLACE FUNCTION newapi.safe_date(value TEXT)
RETURNS DATE
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE WHEN newapi.is_nullish_text(value) THEN NULL ELSE value::DATE END
$$;

CREATE OR REPLACE FUNCTION newapi.safe_timestamptz(value TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE SQL
STABLE
AS $$
    SELECT CASE WHEN newapi.is_nullish_text(value) THEN NULL ELSE value::TIMESTAMPTZ END
$$;

CREATE OR REPLACE FUNCTION newapi.safe_boolean(value TEXT)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE WHEN newapi.is_nullish_text(value) THEN NULL ELSE value::BOOLEAN END
$$;

-- Enhanced function that logs results
CREATE OR REPLACE FUNCTION insert_player_contracts_from_staging_with_logging()
RETURNS TABLE(
    total_contracts_processed INTEGER,
    contracts_inserted INTEGER,
    contracts_updated INTEGER,
    total_seasons_processed INTEGER,
    seasons_inserted INTEGER,
    seasons_updated INTEGER,
    run_duration INTERVAL
) AS $$
DECLARE
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    duration INTERVAL;
    contract_count INTEGER := 0;
    contract_insert_count INTEGER := 0;
    contract_update_count INTEGER := 0;
    season_count INTEGER := 0;
    season_insert_count INTEGER := 0;
    season_update_count INTEGER := 0;
BEGIN
    start_time := CURRENT_TIMESTAMP;

    IF to_regclass('staging1.player_external_ids') IS NOT NULL THEN
        INSERT INTO newapi.player_external_ids (
            "playerId", source, source_slug, source_url, matched_by, confidence,
            last_verified_at, updated_at
        )
        SELECT
            e."playerId"::BIGINT,
            e.source,
            e.source_slug,
            e.source_url,
            e.matched_by,
            newapi.safe_numeric_5_4(e.confidence::TEXT),
            newapi.safe_timestamptz(e.last_verified_at::TEXT),
            CURRENT_TIMESTAMP
        FROM staging1.player_external_ids e
        ON CONFLICT ("playerId", source) DO UPDATE SET
            source_slug = EXCLUDED.source_slug,
            source_url = EXCLUDED.source_url,
            matched_by = EXCLUDED.matched_by,
            confidence = EXCLUDED.confidence,
            last_verified_at = EXCLUDED.last_verified_at,
            updated_at = CURRENT_TIMESTAMP;
    END IF;

    IF to_regclass('staging1.player_contract_failures') IS NOT NULL THEN
        ALTER TABLE staging1.player_contract_failures
            ADD COLUMN IF NOT EXISTS status_code INTEGER;
        ALTER TABLE staging1.player_contract_failures
            ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ;

        INSERT INTO newapi.player_contract_scrape_status (
            "playerId", source, source_slug, status, status_code, error,
            last_attempted_at, updated_at
        )
        SELECT DISTINCT ON (f."playerId")
            f."playerId"::BIGINT,
            'puckpedia',
            f.slug,
            CASE
                WHEN f.status_code::TEXT = '404' THEN 'not_found'
                ELSE 'failed'
            END,
            newapi.safe_integer(f.status_code::TEXT),
            f.error,
            COALESCE(newapi.safe_timestamptz(f.failed_at::TEXT), CURRENT_TIMESTAMP),
            CURRENT_TIMESTAMP
        FROM staging1.player_contract_failures f
        WHERE f."playerId" IS NOT NULL
        ORDER BY f."playerId", f.failed_at DESC NULLS LAST
        ON CONFLICT ("playerId", source) DO UPDATE SET
            source_slug = EXCLUDED.source_slug,
            status = EXCLUDED.status,
            status_code = EXCLUDED.status_code,
            error = EXCLUDED.error,
            last_attempted_at = EXCLUDED.last_attempted_at,
            updated_at = CURRENT_TIMESTAMP;
    END IF;

    IF to_regclass('staging1.player_contracts') IS NULL THEN
        end_time := CURRENT_TIMESTAMP;
        duration := end_time - start_time;

        INSERT INTO newapi.player_contracts_etl_log (
            total_contracts_processed, contracts_inserted, contracts_updated,
            total_seasons_processed, seasons_inserted, seasons_updated,
            run_duration, notes
        ) VALUES (
            0, 0, 0, 0, 0, 0, duration, 'staging1.player_contracts did not exist'
        );

        RETURN QUERY SELECT 0, 0, 0, 0, 0, 0, duration;
        RETURN;
    END IF;

    SELECT COUNT(*) INTO contract_count
    FROM staging1.player_contracts;

    SELECT COUNT(*) INTO contract_update_count
    FROM staging1.player_contracts s
    JOIN newapi.player_contracts c
        ON c.contract_id = s.contract_id;

    contract_insert_count := contract_count - contract_update_count;

    INSERT INTO newapi.player_contracts (
        contract_id, "playerId", source, start_season, end_season, signing_date,
        signing_team, contract_type, term_years, total_value, cap_hit, aav,
        signing_status, expiry_status, expiry_year, expiry_age, signing_age,
        signing_gm, signing_agent, qualifying_offer, arbitration_eligible,
        notes, source_url, scraped_at, updated_at
    )
    SELECT
        s.contract_id,
        s."playerId"::BIGINT,
        COALESCE(s.source, 'puckpedia'),
        s.start_season,
        s.end_season,
        newapi.safe_date(s.signing_date::TEXT),
        s.signing_team,
        s.contract_type,
        newapi.safe_integer(s.term_years::TEXT),
        newapi.safe_bigint(s.total_value::TEXT),
        newapi.safe_bigint(s.cap_hit::TEXT),
        newapi.safe_bigint(s.aav::TEXT),
        s.signing_status,
        s.expiry_status,
        newapi.safe_integer(s.expiry_year::TEXT),
        newapi.safe_integer(s.expiry_age::TEXT),
        newapi.safe_integer(s.signing_age::TEXT),
        s.signing_gm,
        s.signing_agent,
        newapi.safe_bigint(s.qualifying_offer::TEXT),
        newapi.safe_boolean(s.arbitration_eligible::TEXT),
        s.notes,
        s.source_url,
        COALESCE(newapi.safe_timestamptz(s.scraped_at::TEXT), CURRENT_TIMESTAMP),
        CURRENT_TIMESTAMP
    FROM staging1.player_contracts s
    ON CONFLICT (contract_id) DO UPDATE SET
        "playerId" = EXCLUDED."playerId",
        source = EXCLUDED.source,
        start_season = EXCLUDED.start_season,
        end_season = EXCLUDED.end_season,
        signing_date = EXCLUDED.signing_date,
        signing_team = EXCLUDED.signing_team,
        contract_type = EXCLUDED.contract_type,
        term_years = EXCLUDED.term_years,
        total_value = EXCLUDED.total_value,
        cap_hit = EXCLUDED.cap_hit,
        aav = EXCLUDED.aav,
        signing_status = EXCLUDED.signing_status,
        expiry_status = EXCLUDED.expiry_status,
        expiry_year = EXCLUDED.expiry_year,
        expiry_age = EXCLUDED.expiry_age,
        signing_age = EXCLUDED.signing_age,
        signing_gm = EXCLUDED.signing_gm,
        signing_agent = EXCLUDED.signing_agent,
        qualifying_offer = EXCLUDED.qualifying_offer,
        arbitration_eligible = EXCLUDED.arbitration_eligible,
        notes = EXCLUDED.notes,
        source_url = EXCLUDED.source_url,
        scraped_at = EXCLUDED.scraped_at,
        updated_at = CURRENT_TIMESTAMP;

    IF to_regclass('staging1.player_contract_seasons') IS NOT NULL THEN
        SELECT COUNT(*) INTO season_count
        FROM staging1.player_contract_seasons;

        SELECT COUNT(*) INTO season_update_count
        FROM staging1.player_contract_seasons s
        JOIN newapi.player_contract_seasons cs
            ON cs.contract_id = s.contract_id
           AND cs.season = s.season;

        season_insert_count := season_count - season_update_count;

        INSERT INTO newapi.player_contract_seasons (
            contract_id, "playerId", season, team_id, cap_hit, aav, base_salary,
            performance_bonus, signing_bonus, total_salary, minors_salary, clause,
            is_slide_year, is_ufa_year, buyout_url, scraped_at, updated_at
        )
        SELECT
            s.contract_id,
            s."playerId"::BIGINT,
            s.season,
            newapi.safe_bigint(s.team_id::TEXT),
            newapi.safe_bigint(s.cap_hit::TEXT),
            newapi.safe_bigint(s.aav::TEXT),
            newapi.safe_bigint(s.base_salary::TEXT),
            newapi.safe_bigint(s.performance_bonus::TEXT),
            newapi.safe_bigint(s.signing_bonus::TEXT),
            newapi.safe_bigint(s.total_salary::TEXT),
            newapi.safe_bigint(s.minors_salary::TEXT),
            s.clause,
            COALESCE(newapi.safe_boolean(s.is_slide_year::TEXT), FALSE),
            COALESCE(newapi.safe_boolean(s.is_ufa_year::TEXT), FALSE),
            s.buyout_url,
            COALESCE(newapi.safe_timestamptz(s.scraped_at::TEXT), CURRENT_TIMESTAMP),
            CURRENT_TIMESTAMP
        FROM staging1.player_contract_seasons s
        ON CONFLICT (contract_id, season) DO UPDATE SET
            "playerId" = EXCLUDED."playerId",
            team_id = EXCLUDED.team_id,
            cap_hit = EXCLUDED.cap_hit,
            aav = EXCLUDED.aav,
            base_salary = EXCLUDED.base_salary,
            performance_bonus = EXCLUDED.performance_bonus,
            signing_bonus = EXCLUDED.signing_bonus,
            total_salary = EXCLUDED.total_salary,
            minors_salary = EXCLUDED.minors_salary,
            clause = EXCLUDED.clause,
            is_slide_year = EXCLUDED.is_slide_year,
            is_ufa_year = EXCLUDED.is_ufa_year,
            buyout_url = EXCLUDED.buyout_url,
            scraped_at = EXCLUDED.scraped_at,
            updated_at = CURRENT_TIMESTAMP;
    END IF;

    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;

    INSERT INTO newapi.player_contracts_etl_log (
        total_contracts_processed, contracts_inserted, contracts_updated,
        total_seasons_processed, seasons_inserted, seasons_updated,
        run_duration
    ) VALUES (
        contract_count, contract_insert_count, contract_update_count,
        season_count, season_insert_count, season_update_count,
        duration
    );

    RETURN QUERY SELECT
        contract_count, contract_insert_count, contract_update_count,
        season_count, season_insert_count, season_update_count,
        duration;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE sync_player_contracts_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM insert_player_contracts_from_staging_with_logging() INTO result_record;

    RAISE NOTICE 'Player contracts sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Contracts processed: %', result_record.total_contracts_processed;
    RAISE NOTICE '  Contracts inserted: %', result_record.contracts_inserted;
    RAISE NOTICE '  Contracts updated: %', result_record.contracts_updated;
    RAISE NOTICE '  Seasons processed: %', result_record.total_seasons_processed;
    RAISE NOTICE '  Seasons inserted: %', result_record.seasons_inserted;
    RAISE NOTICE '  Seasons updated: %', result_record.seasons_updated;
END;
$$;

CREATE OR REPLACE VIEW readmodel.player_contracts AS
SELECT
    c.contract_id,
    c."playerId",
    c.source,
    c.start_season,
    c.end_season,
    c.signing_date,
    c.signing_team,
    c.contract_type,
    c.term_years,
    c.total_value,
    c.cap_hit,
    c.aav,
    c.signing_status,
    c.expiry_status,
    c.expiry_year,
    c.expiry_age,
    c.signing_age,
    c.signing_gm,
    c.signing_agent,
    c.qualifying_offer,
    c.arbitration_eligible,
    c.notes,
    c.source_url,
    c.scraped_at,
    COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'contract_id', s.contract_id,
                'playerId', s."playerId",
                'season', s.season,
                'team_id', s.team_id,
                'cap_hit', s.cap_hit,
                'aav', s.aav,
                'base_salary', s.base_salary,
                'performance_bonus', s.performance_bonus,
                'signing_bonus', s.signing_bonus,
                'total_salary', s.total_salary,
                'minors_salary', s.minors_salary,
                'clause', s.clause,
                'is_slide_year', s.is_slide_year,
                'is_ufa_year', s.is_ufa_year,
                'buyout_url', s.buyout_url,
                'scraped_at', s.scraped_at
            )
            ORDER BY s.season DESC
        ) FILTER (WHERE s.season IS NOT NULL),
        '[]'::jsonb
    ) AS seasons
FROM newapi.player_contracts c
LEFT JOIN newapi.player_contract_seasons s
    ON s.contract_id = c.contract_id
GROUP BY c.contract_id;

CREATE OR REPLACE VIEW readmodel.player_current_contract AS
WITH current_nhl_season AS (
    SELECT CASE
        WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 7 THEN
            EXTRACT(YEAR FROM CURRENT_DATE)::INT::TEXT ||
            (EXTRACT(YEAR FROM CURRENT_DATE)::INT + 1)::TEXT
        ELSE
            (EXTRACT(YEAR FROM CURRENT_DATE)::INT - 1)::TEXT ||
            EXTRACT(YEAR FROM CURRENT_DATE)::INT::TEXT
    END AS season
),
current_rows AS (
    SELECT DISTINCT ON (c."playerId")
        c.contract_id,
        c."playerId",
        c.source,
        c.start_season,
        c.end_season,
        c.signing_team,
        c.contract_type,
        c.term_years,
        c.total_value,
        c.cap_hit,
        c.aav,
        c.expiry_status,
        c.expiry_year,
        c.expiry_age,
        s.season AS current_season,
        s.total_salary AS current_total_salary,
        s.base_salary AS current_base_salary,
        s.signing_bonus AS current_signing_bonus,
        s.performance_bonus AS current_performance_bonus,
        s.minors_salary AS current_minors_salary,
        s.clause AS current_clause,
        c.source_url,
        GREATEST(c.scraped_at, COALESCE(s.scraped_at, c.scraped_at)) AS scraped_at
    FROM newapi.player_contracts c
    CROSS JOIN current_nhl_season current_season
    LEFT JOIN newapi.player_contract_seasons s
        ON s.contract_id = c.contract_id
       AND s.season = current_season.season
    WHERE current_season.season BETWEEN c.start_season AND c.end_season
    ORDER BY c."playerId", s.season DESC NULLS LAST, c.end_season DESC NULLS LAST
)
SELECT * FROM current_rows;

CREATE OR REPLACE VIEW newapi.player_contracts_etl_summary AS
SELECT
    id,
    run_timestamp,
    total_contracts_processed,
    contracts_inserted,
    contracts_updated,
    total_seasons_processed,
    seasons_inserted,
    seasons_updated,
    run_duration,
    ROUND(EXTRACT(EPOCH FROM run_duration)::NUMERIC, 2) AS duration_seconds,
    notes
FROM newapi.player_contracts_etl_log
ORDER BY run_timestamp DESC;

-- Execute the sync AFTER you've loaded staging1.player_contracts and staging1.player_contract_seasons
CALL sync_player_contracts_from_staging();
