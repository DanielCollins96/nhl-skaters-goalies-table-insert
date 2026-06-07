-- Ensure expected schemas exist and provide a helper to reset the gamecenter objects.
CREATE SCHEMA IF NOT EXISTS staging1;
CREATE SCHEMA IF NOT EXISTS newapi;

-- Helper: optionally drop existing gamecenter objects and let this script recreate them.
CREATE OR REPLACE FUNCTION newapi.reset_gamecenter_schema(recreate BOOLEAN DEFAULT FALSE)
RETURNS VOID AS $$
BEGIN
    IF recreate THEN
        DROP VIEW IF EXISTS newapi.gamecenter_player_points CASCADE;
        DROP VIEW IF EXISTS newapi.gamecenter_goals CASCADE;
        DROP VIEW IF EXISTS newapi.gamecenter_etl_summary CASCADE;
        DROP TABLE IF EXISTS newapi.gamecenter_etl_log CASCADE;
        DROP TABLE IF EXISTS newapi.gamecenter CASCADE;
    END IF;
    -- Ensure schemas exist
    PERFORM 1;
END;
$$ LANGUAGE plpgsql;

-- Drop existing functions/procs to avoid return-type conflicts.
-- Do not drop gamecenter tables here; this script is safe to rerun against loaded data.
DROP FUNCTION IF EXISTS upsert_gamecenter_from_staging_with_logging() CASCADE;
DROP PROCEDURE IF EXISTS sync_gamecenter_from_staging() CASCADE;

CREATE TABLE IF NOT EXISTS newapi.gamecenter (
    game_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    season INTEGER,
    game_type INTEGER,
    game_date DATE,
    away_team_id BIGINT,
    away_team_abbrev TEXT,
    home_team_id BIGINT,
    home_team_abbrev TEXT,
    period_number INTEGER,
    period_type TEXT,
    max_regulation_periods INTEGER,
    time_in_period TEXT,
    time_remaining TEXT,
    elapsed_seconds INTEGER,
    remaining_seconds INTEGER,
    situation_code TEXT,
    home_team_defending_side TEXT,
    type_code INTEGER,
    type_desc_key TEXT,
    sort_order INTEGER,
    event_owner_team_id BIGINT,
    x_coord INTEGER,
    y_coord INTEGER,
    zone_code TEXT,
    desc_key TEXT,
    secondary_reason TEXT,
    reason TEXT,
    shot_type TEXT,
    scoring_player_id BIGINT,
    scoring_player_total INTEGER,
    assist1_player_id BIGINT,
    assist1_player_total INTEGER,
    assist2_player_id BIGINT,
    assist2_player_total INTEGER,
    goalie_in_net_id BIGINT,
    shooting_player_id BIGINT,
    blocking_player_id BIGINT,
    winning_player_id BIGINT,
    losing_player_id BIGINT,
    hitting_player_id BIGINT,
    hittee_player_id BIGINT,
    committed_by_player_id BIGINT,
    drawn_by_player_id BIGINT,
    player_id BIGINT,
    away_score INTEGER,
    home_score INTEGER,
    away_sog INTEGER,
    home_sog INTEGER,
    penalty_type_code TEXT,
    penalty_duration INTEGER,
    highlight_clip BIGINT,
    highlight_clip_sharing_url TEXT,
    ppt_replay_url TEXT,
    details JSONB,
    raw_play JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_id, event_id)
);

-- Query-oriented indexes. The partial goal index keeps scoring lookups small.
CREATE INDEX IF NOT EXISTS idx_gamecenter_game_sort
    ON newapi.gamecenter (game_id, sort_order);

CREATE INDEX IF NOT EXISTS idx_gamecenter_game_period_time
    ON newapi.gamecenter (game_id, period_number, elapsed_seconds);

CREATE INDEX IF NOT EXISTS idx_gamecenter_type
    ON newapi.gamecenter (type_desc_key);

CREATE INDEX IF NOT EXISTS idx_gamecenter_scoring_player
    ON newapi.gamecenter (scoring_player_id)
    WHERE scoring_player_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_gamecenter_assist1_player
    ON newapi.gamecenter (assist1_player_id)
    WHERE assist1_player_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_gamecenter_assist2_player
    ON newapi.gamecenter (assist2_player_id)
    WHERE assist2_player_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_gamecenter_owner_team
    ON newapi.gamecenter (event_owner_team_id);

CREATE INDEX IF NOT EXISTS idx_gamecenter_details_gin
    ON newapi.gamecenter USING GIN (details);

-- Create ETL log table for gamecenter
CREATE TABLE IF NOT EXISTS newapi.gamecenter_etl_log (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_processed INTEGER,
    inserted_records INTEGER,
    updated_records INTEGER,
    unchanged_records INTEGER,
    run_duration INTERVAL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_gamecenter_etl_log_timestamp
    ON newapi.gamecenter_etl_log (run_timestamp);

-- Function: Upsert gamecenter plays from staging with logging.
CREATE OR REPLACE FUNCTION upsert_gamecenter_from_staging_with_logging()
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

    WITH src AS (
        SELECT
            game_id::BIGINT AS fallback_game_id,
            event_id::BIGINT AS fallback_event_id,
            game_payload::JSONB AS game_payload,
            raw_play::JSONB AS raw_play
        FROM staging1.gamecenter
    ), shaped AS (
        SELECT
            COALESCE((game_payload ->> 'id')::BIGINT, fallback_game_id) AS game_id,
            COALESCE((raw_play ->> 'eventId')::BIGINT, fallback_event_id) AS event_id,
            (game_payload ->> 'season')::INTEGER AS season,
            (game_payload ->> 'gameType')::INTEGER AS game_type,
            (game_payload ->> 'gameDate')::DATE AS game_date,
            (game_payload #>> '{awayTeam,id}')::BIGINT AS away_team_id,
            game_payload #>> '{awayTeam,abbrev}' AS away_team_abbrev,
            (game_payload #>> '{homeTeam,id}')::BIGINT AS home_team_id,
            game_payload #>> '{homeTeam,abbrev}' AS home_team_abbrev,
            (raw_play #>> '{periodDescriptor,number}')::INTEGER AS period_number,
            raw_play #>> '{periodDescriptor,periodType}' AS period_type,
            (raw_play #>> '{periodDescriptor,maxRegulationPeriods}')::INTEGER AS max_regulation_periods,
            raw_play ->> 'timeInPeriod' AS time_in_period,
            raw_play ->> 'timeRemaining' AS time_remaining,
            CASE
                WHEN raw_play ->> 'timeInPeriod' ~ '^[0-9]+:[0-9]{2}$'
                     AND raw_play #>> '{periodDescriptor,number}' IS NOT NULL
                THEN (((raw_play #>> '{periodDescriptor,number}')::INTEGER - 1) * 1200)
                    + split_part(raw_play ->> 'timeInPeriod', ':', 1)::INTEGER * 60
                    + split_part(raw_play ->> 'timeInPeriod', ':', 2)::INTEGER
                ELSE NULL
            END AS elapsed_seconds,
            CASE
                WHEN raw_play ->> 'timeRemaining' ~ '^[0-9]+:[0-9]{2}$'
                THEN split_part(raw_play ->> 'timeRemaining', ':', 1)::INTEGER * 60
                    + split_part(raw_play ->> 'timeRemaining', ':', 2)::INTEGER
                ELSE NULL
            END AS remaining_seconds,
            raw_play ->> 'situationCode' AS situation_code,
            raw_play ->> 'homeTeamDefendingSide' AS home_team_defending_side,
            (raw_play ->> 'typeCode')::INTEGER AS type_code,
            raw_play ->> 'typeDescKey' AS type_desc_key,
            (raw_play ->> 'sortOrder')::INTEGER AS sort_order,
            (raw_play #>> '{details,eventOwnerTeamId}')::BIGINT AS event_owner_team_id,
            (raw_play #>> '{details,xCoord}')::INTEGER AS x_coord,
            (raw_play #>> '{details,yCoord}')::INTEGER AS y_coord,
            raw_play #>> '{details,zoneCode}' AS zone_code,
            raw_play #>> '{details,descKey}' AS desc_key,
            raw_play #>> '{details,secondaryReason}' AS secondary_reason,
            raw_play #>> '{details,reason}' AS reason,
            raw_play #>> '{details,shotType}' AS shot_type,
            (raw_play #>> '{details,scoringPlayerId}')::BIGINT AS scoring_player_id,
            (raw_play #>> '{details,scoringPlayerTotal}')::INTEGER AS scoring_player_total,
            (raw_play #>> '{details,assist1PlayerId}')::BIGINT AS assist1_player_id,
            (raw_play #>> '{details,assist1PlayerTotal}')::INTEGER AS assist1_player_total,
            (raw_play #>> '{details,assist2PlayerId}')::BIGINT AS assist2_player_id,
            (raw_play #>> '{details,assist2PlayerTotal}')::INTEGER AS assist2_player_total,
            (raw_play #>> '{details,goalieInNetId}')::BIGINT AS goalie_in_net_id,
            (raw_play #>> '{details,shootingPlayerId}')::BIGINT AS shooting_player_id,
            (raw_play #>> '{details,blockingPlayerId}')::BIGINT AS blocking_player_id,
            (raw_play #>> '{details,winningPlayerId}')::BIGINT AS winning_player_id,
            (raw_play #>> '{details,losingPlayerId}')::BIGINT AS losing_player_id,
            (raw_play #>> '{details,hittingPlayerId}')::BIGINT AS hitting_player_id,
            (raw_play #>> '{details,hitteePlayerId}')::BIGINT AS hittee_player_id,
            (raw_play #>> '{details,committedByPlayerId}')::BIGINT AS committed_by_player_id,
            (raw_play #>> '{details,drawnByPlayerId}')::BIGINT AS drawn_by_player_id,
            (raw_play #>> '{details,playerId}')::BIGINT AS player_id,
            (raw_play #>> '{details,awayScore}')::INTEGER AS away_score,
            (raw_play #>> '{details,homeScore}')::INTEGER AS home_score,
            (raw_play #>> '{details,awaySOG}')::INTEGER AS away_sog,
            (raw_play #>> '{details,homeSOG}')::INTEGER AS home_sog,
            raw_play #>> '{details,typeCode}' AS penalty_type_code,
            (raw_play #>> '{details,duration}')::INTEGER AS penalty_duration,
            (raw_play #>> '{details,highlightClip}')::BIGINT AS highlight_clip,
            raw_play #>> '{details,highlightClipSharingUrl}' AS highlight_clip_sharing_url,
            raw_play ->> 'pptReplayUrl' AS ppt_replay_url,
            raw_play -> 'details' AS details,
            raw_play
        FROM src
    ), upsert AS (
        INSERT INTO newapi.gamecenter (
            game_id,
            event_id,
            season,
            game_type,
            game_date,
            away_team_id,
            away_team_abbrev,
            home_team_id,
            home_team_abbrev,
            period_number,
            period_type,
            max_regulation_periods,
            time_in_period,
            time_remaining,
            elapsed_seconds,
            remaining_seconds,
            situation_code,
            home_team_defending_side,
            type_code,
            type_desc_key,
            sort_order,
            event_owner_team_id,
            x_coord,
            y_coord,
            zone_code,
            desc_key,
            secondary_reason,
            reason,
            shot_type,
            scoring_player_id,
            scoring_player_total,
            assist1_player_id,
            assist1_player_total,
            assist2_player_id,
            assist2_player_total,
            goalie_in_net_id,
            shooting_player_id,
            blocking_player_id,
            winning_player_id,
            losing_player_id,
            hitting_player_id,
            hittee_player_id,
            committed_by_player_id,
            drawn_by_player_id,
            player_id,
            away_score,
            home_score,
            away_sog,
            home_sog,
            penalty_type_code,
            penalty_duration,
            highlight_clip,
            highlight_clip_sharing_url,
            ppt_replay_url,
            details,
            raw_play
        )
        SELECT
            s.game_id,
            s.event_id,
            s.season,
            s.game_type,
            s.game_date,
            s.away_team_id,
            s.away_team_abbrev,
            s.home_team_id,
            s.home_team_abbrev,
            s.period_number,
            s.period_type,
            s.max_regulation_periods,
            s.time_in_period,
            s.time_remaining,
            s.elapsed_seconds,
            s.remaining_seconds,
            s.situation_code,
            s.home_team_defending_side,
            s.type_code,
            s.type_desc_key,
            s.sort_order,
            s.event_owner_team_id,
            s.x_coord,
            s.y_coord,
            s.zone_code,
            s.desc_key,
            s.secondary_reason,
            s.reason,
            s.shot_type,
            s.scoring_player_id,
            s.scoring_player_total,
            s.assist1_player_id,
            s.assist1_player_total,
            s.assist2_player_id,
            s.assist2_player_total,
            s.goalie_in_net_id,
            s.shooting_player_id,
            s.blocking_player_id,
            s.winning_player_id,
            s.losing_player_id,
            s.hitting_player_id,
            s.hittee_player_id,
            s.committed_by_player_id,
            s.drawn_by_player_id,
            s.player_id,
            s.away_score,
            s.home_score,
            s.away_sog,
            s.home_sog,
            s.penalty_type_code,
            s.penalty_duration,
            s.highlight_clip,
            s.highlight_clip_sharing_url,
            s.ppt_replay_url,
            s.details,
            s.raw_play
        FROM shaped s
        ON CONFLICT (game_id, event_id) DO UPDATE
        SET
            season = EXCLUDED.season,
            game_type = EXCLUDED.game_type,
            game_date = EXCLUDED.game_date,
            away_team_id = EXCLUDED.away_team_id,
            away_team_abbrev = EXCLUDED.away_team_abbrev,
            home_team_id = EXCLUDED.home_team_id,
            home_team_abbrev = EXCLUDED.home_team_abbrev,
            period_number = EXCLUDED.period_number,
            period_type = EXCLUDED.period_type,
            max_regulation_periods = EXCLUDED.max_regulation_periods,
            time_in_period = EXCLUDED.time_in_period,
            time_remaining = EXCLUDED.time_remaining,
            elapsed_seconds = EXCLUDED.elapsed_seconds,
            remaining_seconds = EXCLUDED.remaining_seconds,
            situation_code = EXCLUDED.situation_code,
            home_team_defending_side = EXCLUDED.home_team_defending_side,
            type_code = EXCLUDED.type_code,
            type_desc_key = EXCLUDED.type_desc_key,
            sort_order = EXCLUDED.sort_order,
            event_owner_team_id = EXCLUDED.event_owner_team_id,
            x_coord = EXCLUDED.x_coord,
            y_coord = EXCLUDED.y_coord,
            zone_code = EXCLUDED.zone_code,
            desc_key = EXCLUDED.desc_key,
            secondary_reason = EXCLUDED.secondary_reason,
            reason = EXCLUDED.reason,
            shot_type = EXCLUDED.shot_type,
            scoring_player_id = EXCLUDED.scoring_player_id,
            scoring_player_total = EXCLUDED.scoring_player_total,
            assist1_player_id = EXCLUDED.assist1_player_id,
            assist1_player_total = EXCLUDED.assist1_player_total,
            assist2_player_id = EXCLUDED.assist2_player_id,
            assist2_player_total = EXCLUDED.assist2_player_total,
            goalie_in_net_id = EXCLUDED.goalie_in_net_id,
            shooting_player_id = EXCLUDED.shooting_player_id,
            blocking_player_id = EXCLUDED.blocking_player_id,
            winning_player_id = EXCLUDED.winning_player_id,
            losing_player_id = EXCLUDED.losing_player_id,
            hitting_player_id = EXCLUDED.hitting_player_id,
            hittee_player_id = EXCLUDED.hittee_player_id,
            committed_by_player_id = EXCLUDED.committed_by_player_id,
            drawn_by_player_id = EXCLUDED.drawn_by_player_id,
            player_id = EXCLUDED.player_id,
            away_score = EXCLUDED.away_score,
            home_score = EXCLUDED.home_score,
            away_sog = EXCLUDED.away_sog,
            home_sog = EXCLUDED.home_sog,
            penalty_type_code = EXCLUDED.penalty_type_code,
            penalty_duration = EXCLUDED.penalty_duration,
            highlight_clip = EXCLUDED.highlight_clip,
            highlight_clip_sharing_url = EXCLUDED.highlight_clip_sharing_url,
            ppt_replay_url = EXCLUDED.ppt_replay_url,
            details = EXCLUDED.details,
            raw_play = EXCLUDED.raw_play,
            updated_at = CURRENT_TIMESTAMP
        WHERE (EXCLUDED.raw_play IS DISTINCT FROM newapi.gamecenter.raw_play)
        RETURNING xmax
    ), counts AS (
        SELECT
            (SELECT COUNT(*) FROM shaped) AS total_src,
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

    INSERT INTO newapi.gamecenter_etl_log (
        total_processed, inserted_records, updated_records, unchanged_records, run_duration
    ) VALUES (
        COALESCE(v_total, 0), COALESCE(v_inserted, 0), COALESCE(v_updated, 0), COALESCE(v_unchanged, 0), duration
    );

    RETURN QUERY SELECT v_total, v_inserted, v_updated, v_unchanged, duration;
END;
$$ LANGUAGE plpgsql;

-- Procedure to run the gamecenter upsert
CREATE OR REPLACE PROCEDURE sync_gamecenter_from_staging()
LANGUAGE plpgsql AS $$
DECLARE
    result_record RECORD;
BEGIN
    SELECT * FROM upsert_gamecenter_from_staging_with_logging() INTO result_record;

    RAISE NOTICE 'Gamecenter sync completed in %:', result_record.run_duration;
    RAISE NOTICE '  Total processed: %', result_record.total_processed;
    RAISE NOTICE '  Inserted records: %', result_record.inserted_records;
    RAISE NOTICE '  Updated records: %', result_record.updated_records;
    RAISE NOTICE '  Unchanged records: %', result_record.unchanged_records;
END;
$$;

-- Summary view for gamecenter ETL runs
CREATE OR REPLACE VIEW newapi.gamecenter_etl_summary AS
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
FROM newapi.gamecenter_etl_log
ORDER BY run_timestamp DESC;

-- Helper view: goals with scorer and assist IDs.
CREATE OR REPLACE VIEW newapi.gamecenter_goals AS
SELECT
    game_id,
    event_id,
    season,
    game_date,
    period_number,
    period_type,
    time_in_period,
    elapsed_seconds,
    event_owner_team_id,
    CASE
        WHEN event_owner_team_id = away_team_id THEN away_team_abbrev
        WHEN event_owner_team_id = home_team_id THEN home_team_abbrev
        ELSE NULL
    END AS scoring_team,
    scoring_player_id,
    scoring_player_total,
    assist1_player_id,
    assist1_player_total,
    assist2_player_id,
    assist2_player_total,
    goalie_in_net_id,
    shot_type,
    x_coord,
    y_coord,
    zone_code,
    away_score,
    home_score,
    highlight_clip_sharing_url
FROM newapi.gamecenter
WHERE type_desc_key = 'goal'
ORDER BY game_id, sort_order;

-- Helper view: one row per scoring point, covering goals and assists.
CREATE OR REPLACE VIEW newapi.gamecenter_player_points AS
SELECT
    game_id,
    event_id,
    season,
    game_date,
    period_number,
    period_type,
    time_in_period,
    elapsed_seconds,
    event_owner_team_id,
    CASE
        WHEN event_owner_team_id = away_team_id THEN away_team_abbrev
        WHEN event_owner_team_id = home_team_id THEN home_team_abbrev
        ELSE NULL
    END AS team_abbrev,
    scoring_player_id AS player_id,
    'goal'::TEXT AS point_type,
    1 AS point_order,
    scoring_player_total AS player_season_total,
    away_score,
    home_score,
    sort_order
FROM newapi.gamecenter
WHERE type_desc_key = 'goal'
  AND scoring_player_id IS NOT NULL

UNION ALL

SELECT
    game_id,
    event_id,
    season,
    game_date,
    period_number,
    period_type,
    time_in_period,
    elapsed_seconds,
    event_owner_team_id,
    CASE
        WHEN event_owner_team_id = away_team_id THEN away_team_abbrev
        WHEN event_owner_team_id = home_team_id THEN home_team_abbrev
        ELSE NULL
    END AS team_abbrev,
    assist1_player_id AS player_id,
    'assist'::TEXT AS point_type,
    2 AS point_order,
    assist1_player_total AS player_season_total,
    away_score,
    home_score,
    sort_order
FROM newapi.gamecenter
WHERE type_desc_key = 'goal'
  AND assist1_player_id IS NOT NULL

UNION ALL

SELECT
    game_id,
    event_id,
    season,
    game_date,
    period_number,
    period_type,
    time_in_period,
    elapsed_seconds,
    event_owner_team_id,
    CASE
        WHEN event_owner_team_id = away_team_id THEN away_team_abbrev
        WHEN event_owner_team_id = home_team_id THEN home_team_abbrev
        ELSE NULL
    END AS team_abbrev,
    assist2_player_id AS player_id,
    'assist'::TEXT AS point_type,
    3 AS point_order,
    assist2_player_total AS player_season_total,
    away_score,
    home_score,
    sort_order
FROM newapi.gamecenter
WHERE type_desc_key = 'goal'
  AND assist2_player_id IS NOT NULL
ORDER BY game_id, sort_order, point_order;

-- Helper view: all plays ordered for replay/analysis.
CREATE OR REPLACE VIEW newapi.gamecenter_play_timeline AS
SELECT
    game_id,
    event_id,
    period_number,
    period_type,
    time_in_period,
    time_remaining,
    elapsed_seconds,
    type_desc_key,
    event_owner_team_id,
    x_coord,
    y_coord,
    zone_code,
    desc_key,
    reason,
    secondary_reason,
    shot_type,
    scoring_player_id,
    assist1_player_id,
    assist2_player_id,
    shooting_player_id,
    goalie_in_net_id,
    away_score,
    home_score,
    sort_order
FROM newapi.gamecenter
ORDER BY game_id, sort_order;

-- Example staging load for the provided sample JSON:
-- INSERT INTO staging1.gamecenter_raw (game_id, source_url, response)
-- VALUES (
--     2023020204,
--     'https://api-web.nhle.com/v1/gamecenter/2023020204/play-by-play',
--     '<paste json here>'::jsonb
-- )
-- ON CONFLICT (game_id) DO UPDATE
-- SET response = EXCLUDED.response,
--     source_url = EXCLUDED.source_url,
--     fetched_at = CURRENT_TIMESTAMP;
--
-- CALL sync_gamecenter_from_staging();
