-- S3 export views for the Next.js hockey app.
--
-- These views depend on sql/readmodel_views.sql.
-- They return endpoint-shaped JSON payloads plus the S3 key each payload should
-- be uploaded to.
--
-- Typical ETL order:
--   1. Load/transform staging and newapi tables.
--   2. Run sql/readmodel_views.sql.
--   3. Run this file.
--   4. SELECT s3_key, payload FROM readmodel.s3_objects and upload each row.

CREATE SCHEMA IF NOT EXISTS readmodel;

CREATE OR REPLACE VIEW readmodel.s3_player_payloads AS
WITH player_ids AS (
    SELECT DISTINCT "playerId"
    FROM readmodel.players
),
player_rows AS (
    SELECT
        p."playerId",
        JSONB_AGG(
            TO_JSONB(p) - 'birthDate' || JSONB_BUILD_OBJECT(
                'birthdate', p."birthDate",
                'birthcountry', p."birthCountry"
            )
            ORDER BY p.player_name
        ) AS player
    FROM readmodel.players p
    GROUP BY p."playerId"
),
player_positions AS (
    SELECT DISTINCT ON ("playerId")
        "playerId",
        "position"
    FROM readmodel.players
    ORDER BY "playerId", "position"
),
skater_stats AS (
    SELECT
        "playerId",
        JSONB_AGG(
            TO_JSONB(s) - 'playerId' || JSONB_BUILD_OBJECT(
                'birthdate', s."birthDate",
                'birthcountry', s."birthCountry",
                'age', s.age
            )
            ORDER BY s."season" DESC, s."team.name"
        ) AS stats
    FROM readmodel.player_skater_stats s
    GROUP BY "playerId"
),
goalie_stats AS (
    SELECT
        "playerId",
        JSONB_AGG(
            TO_JSONB(g) - 'playerId' || JSONB_BUILD_OBJECT(
                'birthdate', g."birthDate",
                'birthcountry', g."birthCountry",
                'age', g.age
            )
            ORDER BY g."season" DESC, g."team.name"
        ) AS stats
    FROM readmodel.player_goalie_stats g
    GROUP BY "playerId"
),
awards AS (
    SELECT
        "playerId",
        JSONB_AGG(TO_JSONB(a) ORDER BY a."seasonId" DESC) AS awards
    FROM readmodel.player_awards a
    GROUP BY "playerId"
),
contracts AS (
    SELECT
        "playerId",
        JSONB_AGG(
            TO_JSONB(c) - 'playerId'
            ORDER BY c.start_season DESC NULLS LAST, c.end_season DESC NULLS LAST
        ) AS contracts
    FROM readmodel.player_contracts c
    GROUP BY "playerId"
),
current_contracts AS (
    SELECT
        "playerId",
        TO_JSONB(c) - 'playerId' AS current_contract
    FROM readmodel.player_current_contract c
)
SELECT
    CONCAT('players/', ids."playerId", '.json') AS s3_key,
    JSONB_BUILD_OBJECT(
        'player', COALESCE(pr.player, '[]'::JSONB),
        'playerStats', COALESCE(
            CASE
                WHEN pp."position" = 'G' THEN gs.stats
                ELSE ss.stats
            END,
            '[]'::JSONB
        ),
        'awards', COALESCE(a.awards, '[]'::JSONB),
        'contracts', COALESCE(c.contracts, '[]'::JSONB),
        'currentContract', cc.current_contract
    ) AS payload
FROM player_ids ids
LEFT JOIN player_rows pr ON ids."playerId" = pr."playerId"
LEFT JOIN player_positions pp ON ids."playerId" = pp."playerId"
LEFT JOIN skater_stats ss ON ids."playerId" = ss."playerId"
LEFT JOIN goalie_stats gs ON ids."playerId" = gs."playerId"
LEFT JOIN awards a ON ids."playerId" = a."playerId"
LEFT JOIN contracts c ON ids."playerId" = c."playerId"
LEFT JOIN current_contracts cc ON ids."playerId" = cc."playerId";

CREATE OR REPLACE VIEW readmodel.s3_team_payloads AS
WITH teams AS (
    SELECT
        ti.id,
        TO_JSONB(ti) - 'id' AS team
    FROM readmodel.team_info ti
),
team_records AS (
    SELECT
        "teamId" AS id,
        JSONB_AGG(
            TO_JSONB(ts) - 'teamId' - 'season_rank'
            ORDER BY ts."seasonId" DESC
        ) AS team_records
    FROM readmodel.team_seasons ts
    -- WHERE ts.season_rank <= 8
    GROUP BY ts."teamId"
),
skaters AS (
    SELECT
        id,
        JSONB_AGG(
            TO_JSONB(s) || JSONB_BUILD_OBJECT(
                'birthdate', s."birthDate",
                'birthcountry', s."birthCountry",
                'age', s.age
            )
            ORDER BY s.season DESC, s."fullName"
        ) AS skaters
    FROM readmodel.team_skaters s
    GROUP BY id
),
goalies AS (
    SELECT
        id,
        JSONB_AGG(
            TO_JSONB(g) || JSONB_BUILD_OBJECT(
                'birthdate', g."birthDate",
                'birthcountry', g."birthCountry",
                'age', g.age
            )
            ORDER BY g.season DESC, g."fullName"
        ) AS goalies
    FROM readmodel.team_goalies g
    GROUP BY id
),
playoffs AS (
    SELECT
        ti.id,
        JSONB_AGG(tpy.season ORDER BY tpy.season DESC) AS playoff_seasons
    FROM readmodel.team_info ti
    JOIN readmodel.team_playoff_years tpy ON ti.abbreviation = tpy.abbreviation
    GROUP BY ti.id
)
SELECT
    CONCAT('teams/', t.id, '.json') AS s3_key,
    JSONB_BUILD_OBJECT(
        'team', t.team,
        'teamRecords', COALESCE(tr.team_records, '[]'::JSONB),
        'skaters', COALESCE(s.skaters, '[]'::JSONB),
        'goalies', COALESCE(g.goalies, '[]'::JSONB),
        'playoffSeasons', COALESCE(p.playoff_seasons, '[]'::JSONB)
    ) AS payload
FROM teams t
LEFT JOIN team_records tr ON t.id = tr.id
LEFT JOIN skaters s ON t.id = s.id
LEFT JOIN goalies g ON t.id = g.id
LEFT JOIN playoffs p ON t.id = p.id;

CREATE OR REPLACE VIEW readmodel.s3_season_payloads AS
WITH seasons AS (
    SELECT season
    FROM readmodel.available_seasons
),
available AS (
    SELECT JSONB_AGG(season ORDER BY season DESC) AS seasons
    FROM readmodel.available_seasons
),
players AS (
    SELECT
        season,
        JSONB_AGG(TO_JSONB(p) ORDER BY p.row_number ASC) AS players
    FROM readmodel.season_point_leaders p
    GROUP BY season
),
goalies AS (
    SELECT
        season,
        JSONB_AGG(TO_JSONB(g) ORDER BY g.row_number ASC) AS goalies
    FROM readmodel.season_goalie_leaders g
    GROUP BY season
)
SELECT
    CONCAT('seasons/', s.season, '.json') AS s3_key,
    JSONB_BUILD_OBJECT(
        'season', s.season,
        'players', COALESCE(p.players, '[]'::JSONB),
        'goalies', COALESCE(g.goalies, '[]'::JSONB),
        'availableSeasons', COALESCE(a.seasons, '[]'::JSONB)
    ) AS payload
FROM seasons s
CROSS JOIN available a
LEFT JOIN players p ON s.season = p.season
LEFT JOIN goalies g ON s.season = g.season;

CREATE OR REPLACE VIEW readmodel.s3_draft_payloads AS
SELECT
    CONCAT('drafts/', "draftYear", '.json') AS s3_key,
    JSONB_BUILD_OBJECT(
        'draft',
        JSONB_AGG(TO_JSONB(dp) - 'draftYear' ORDER BY dp."overallPick" ASC)
    ) AS payload
FROM readmodel.draft_picks dp
GROUP BY "draftYear";

CREATE OR REPLACE VIEW readmodel.s3_contract_payloads AS
SELECT
    CONCAT('contracts/players/', "playerId", '.json') AS s3_key,
    JSONB_BUILD_OBJECT(
        'playerId', "playerId",
        'contracts',
        JSONB_AGG(
            TO_JSONB(pc) - 'playerId'
            ORDER BY pc.start_season DESC NULLS LAST, pc.end_season DESC NULLS LAST
        )
    ) AS payload
FROM readmodel.player_contracts pc
GROUP BY "playerId";

CREATE OR REPLACE VIEW readmodel.s3_team_contract_payloads AS
WITH roster_players AS (
    SELECT DISTINCT
        id AS team_id,
        season,
        "playerId",
        "fullName",
        "positionCode"
    FROM readmodel.team_skaters
    WHERE season >= '20052006'

    UNION

    SELECT DISTINCT
        id AS team_id,
        season,
        "playerId",
        "fullName",
        'G' AS "positionCode"
    FROM readmodel.team_goalies
    WHERE season >= '20052006'
),
contract_seasons AS (
    SELECT
        rp.team_id,
        rp.season,
        rp."playerId",
        rp."fullName",
        rp."positionCode",
        pc.contract_id,
        TO_JSONB(pc) - 'playerId' - 'seasons' AS contract,
        season_row.season_payload
    FROM roster_players rp
    JOIN readmodel.player_contracts pc
      ON pc."playerId" = rp."playerId"
    CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
        COALESCE((TO_JSONB(pc) -> 'seasons'), '[]'::JSONB)
    ) AS season_row(season_payload)
    WHERE season_row.season_payload ->> 'season' = rp.season::TEXT
),
player_contracts AS (
    SELECT
        team_id,
        season,
        "playerId",
        "fullName",
        "positionCode",
        JSONB_AGG(
            contract || JSONB_BUILD_OBJECT(
                'seasons',
                JSONB_BUILD_ARRAY(season_payload)
            )
            ORDER BY
                contract ->> 'start_season' DESC NULLS LAST,
                contract ->> 'end_season' DESC NULLS LAST,
                contract_id
        ) AS contracts
    FROM contract_seasons
    GROUP BY
        team_id,
        season,
        "playerId",
        "fullName",
        "positionCode"
)
SELECT
    CONCAT('contracts/teams/', team_id, '/', season, '.json') AS s3_key,
    JSONB_BUILD_OBJECT(
        'teamId', team_id,
        'season', season,
        'teamContracts',
        COALESCE(
            JSONB_AGG(
                JSONB_BUILD_OBJECT(
                    'playerId', "playerId",
                    'fullName', "fullName",
                    'positionCode', "positionCode",
                    'rosterSeasons', JSONB_BUILD_ARRAY(season),
                    'currentContract', NULL,
                    'contracts', contracts
                )
                ORDER BY "fullName"
            ),
            '[]'::JSONB
        )
    ) AS payload
FROM player_contracts
GROUP BY team_id, season;

CREATE OR REPLACE VIEW readmodel.s3_index_payloads AS
WITH teams_payload AS (
    SELECT
        'indexes/teams.json' AS s3_key,
        JSONB_BUILD_OBJECT(
            'teams',
            COALESCE(JSONB_AGG(TO_JSONB(t) ORDER BY t.name), '[]'::JSONB)
        ) AS payload
    FROM readmodel.teams t
),
team_ids_payload AS (
    SELECT
        'indexes/team-ids.json' AS s3_key,
        JSONB_BUILD_OBJECT(
            'teamIds',
            COALESCE(JSONB_AGG(TO_JSONB(t) ORDER BY t.id), '[]'::JSONB)
        ) AS payload
    FROM readmodel.team_ids t
),
player_ids_payload AS (
    SELECT
        'indexes/player-ids.json' AS s3_key,
        JSONB_BUILD_OBJECT(
            'playerIds',
            COALESCE(JSONB_AGG(TO_JSONB(p) ORDER BY p."playerId"), '[]'::JSONB)
        ) AS payload
    FROM readmodel.player_ids p
),
contract_player_ids_payload AS (
    SELECT
        'indexes/contract-player-ids.json' AS s3_key,
        JSONB_BUILD_OBJECT(
            'playerIds',
            COALESCE(JSONB_AGG(DISTINCT "playerId" ORDER BY "playerId"), '[]'::JSONB)
        ) AS payload
    FROM readmodel.player_contracts
),
draft_years_payload AS (
    SELECT
        'indexes/draft-years.json' AS s3_key,
        JSONB_BUILD_OBJECT(
            'years',
            COALESCE(JSONB_AGG(TO_JSONB(d) ORDER BY d."draftYear" DESC), '[]'::JSONB)
        ) AS payload
    FROM readmodel.draft_years d
),
game_date_range_payload AS (
    SELECT
        'indexes/game-date-range.json' AS s3_key,
        JSONB_BUILD_OBJECT(
            'minDate', TO_CHAR(MIN("gameDate"), 'YYYY-MM-DD'),
            'maxDate', TO_CHAR(MAX("gameDate"), 'YYYY-MM-DD')
        ) AS payload
    FROM readmodel.games
),
team_rosters_payload AS (
    WITH roster_players AS (
        SELECT
            "teamAbbreviation",
            "positionGroup",
            JSONB_AGG(
                JSONB_BUILD_OBJECT(
                    'position', "positionGroup",
                    'id', "playerId",
                    'sweaterNumber', "sweaterNumber",
                    'firstName', "firstName",
                    'lastName', "lastName"
                )
                ORDER BY "firstName", "lastName"
            ) AS players
        FROM readmodel.active_rosters
        GROUP BY "teamAbbreviation", "positionGroup"
    ),
    roster_by_team AS (
        SELECT
            team_abbrevs."teamAbbreviation",
            JSONB_BUILD_OBJECT(
                'forwards', COALESCE(forwards.players, '[]'::JSONB),
                'defensemen', COALESCE(defensemen.players, '[]'::JSONB),
                'goalies', COALESCE(goalies.players, '[]'::JSONB)
            ) AS roster
        FROM (
            SELECT DISTINCT "teamAbbreviation"
            FROM roster_players
        ) team_abbrevs
        LEFT JOIN roster_players forwards
            ON team_abbrevs."teamAbbreviation" = forwards."teamAbbreviation"
           AND forwards."positionGroup" = 'forwards'
        LEFT JOIN roster_players defensemen
            ON team_abbrevs."teamAbbreviation" = defensemen."teamAbbreviation"
           AND defensemen."positionGroup" = 'defensemen'
        LEFT JOIN roster_players goalies
            ON team_abbrevs."teamAbbreviation" = goalies."teamAbbreviation"
           AND goalies."positionGroup" = 'goalies'
    )
    SELECT
        'indexes/team-rosters.json' AS s3_key,
        JSONB_BUILD_OBJECT(
            'rosters',
            COALESCE(
                JSONB_AGG(
                    JSONB_BUILD_OBJECT(
                        'team', TO_JSONB(t),
                        'roster', r.roster
                    )
                    ORDER BY t.name
                ),
                '[]'::JSONB
            )
        ) AS payload
    FROM readmodel.teams t
    JOIN roster_by_team r ON t.abbreviation = r."teamAbbreviation"
)
SELECT s3_key, payload FROM teams_payload
UNION ALL
SELECT s3_key, payload FROM team_ids_payload
UNION ALL
SELECT s3_key, payload FROM player_ids_payload
UNION ALL
SELECT s3_key, payload FROM contract_player_ids_payload
UNION ALL
SELECT s3_key, payload FROM draft_years_payload
UNION ALL
SELECT s3_key, payload FROM game_date_range_payload
UNION ALL
SELECT s3_key, payload FROM team_rosters_payload;

CREATE OR REPLACE VIEW readmodel.s3_player_search_payloads AS
WITH player_terms AS (
    SELECT
        ps.*,
        LOWER(REGEXP_REPLACE(COALESCE(ps.player_name, ''), '[^a-zA-Z0-9]', '', 'g')) AS name_key,
        LOWER(REGEXP_REPLACE(COALESCE(SPLIT_PART(ps.player_name, ' ', 1), ''), '[^a-zA-Z0-9]', '', 'g')) AS first_name_key,
        LOWER(REGEXP_REPLACE(COALESCE(NULLIF(SPLIT_PART(ps.player_name, ' ', 2), ''), ps.player_name, ''), '[^a-zA-Z0-9]', '', 'g')) AS last_name_key,
        LOWER(REGEXP_REPLACE(COALESCE(ps.team_abbrev, ''), '[^a-zA-Z0-9]', '', 'g')) AS team_abbrev_key,
        LOWER(REGEXP_REPLACE(COALESCE(ps.team_name, ''), '[^a-zA-Z0-9]', '', 'g')) AS team_name_key
    FROM readmodel.player_search ps
),
bucketed_players AS (
    SELECT DISTINCT
        bucket,
        TO_JSONB(pt) - 'name_key' - 'first_name_key' - 'last_name_key' - 'team_abbrev_key' - 'team_name_key' AS player
    FROM player_terms pt
    CROSS JOIN LATERAL (
        VALUES
            (LEFT(pt.name_key, 1)),
            (LEFT(pt.first_name_key, 1)),
            (LEFT(pt.last_name_key, 1)),
            (LEFT(pt.team_abbrev_key, 1)),
            (LEFT(pt.team_name_key, 1))
    ) AS buckets(bucket)
    WHERE bucket ~ '^[a-z]$'
)
SELECT
    CONCAT('indexes/player-search/', bucket, '.json') AS s3_key,
    JSONB_BUILD_OBJECT(
        'players',
        JSONB_AGG(
            player
            ORDER BY
                COALESCE((player->>'games')::INTEGER, 0) DESC,
                COALESCE((player->>'points')::INTEGER, 0) DESC,
                COALESCE((player->>'goals')::INTEGER, 0) DESC
        )
    ) AS payload
FROM bucketed_players
GROUP BY bucket;

CREATE OR REPLACE VIEW readmodel.s3_game_date_payloads AS
SELECT
    CONCAT('games/dates/', TO_CHAR("gameDate", 'YYYY-MM-DD'), '.json') AS s3_key,
    JSONB_BUILD_OBJECT(
        'games',
        JSONB_AGG(TO_JSONB(g) ORDER BY g."startTimeUTC" ASC)
    ) AS payload
FROM readmodel.games g
GROUP BY "gameDate";

CREATE OR REPLACE VIEW readmodel.s3_game_payloads AS
WITH goals AS (
    SELECT
        game_id,
        JSONB_AGG(
            TO_JSONB(g) - 'game_id' - 'season' - 'game_type' - 'game_date'
            ORDER BY period_number ASC, time_in_period ASC, event_id ASC
        ) AS goals
    FROM readmodel.game_goals g
    GROUP BY game_id
),
penalties AS (
    SELECT
        game_id,
        JSONB_AGG(
            TO_JSONB(p) - 'game_id' - 'season' - 'game_type' - 'game_date'
            ORDER BY period_number ASC, time_in_period ASC, penalty_index ASC
        ) AS penalties
    FROM readmodel.game_penalties p
    GROUP BY game_id
),
three_stars AS (
    SELECT
        game_id,
        JSONB_AGG(
            TO_JSONB(s) - 'game_id' - 'season' - 'game_type' - 'game_date'
            ORDER BY star ASC
        ) AS three_stars
    FROM readmodel.game_three_stars s
    GROUP BY game_id
)
SELECT
    CONCAT('games/', g.id, '.json') AS s3_key,
    JSONB_BUILD_OBJECT(
        'game', TO_JSONB(g),
        'goals', COALESCE(goals.goals, '[]'::JSONB),
        'penalties', COALESCE(penalties.penalties, '[]'::JSONB),
        'threeStars', COALESCE(three_stars.three_stars, '[]'::JSONB)
    ) AS payload
FROM readmodel.games g
LEFT JOIN goals ON g.id = goals.game_id
LEFT JOIN penalties ON g.id = penalties.game_id
LEFT JOIN three_stars ON g.id = three_stars.game_id;

CREATE OR REPLACE VIEW readmodel.s3_objects AS
SELECT s3_key, payload FROM readmodel.s3_player_payloads
UNION ALL
SELECT s3_key, payload FROM readmodel.s3_team_payloads
UNION ALL
SELECT s3_key, payload FROM readmodel.s3_season_payloads
UNION ALL
SELECT s3_key, payload FROM readmodel.s3_draft_payloads
UNION ALL
SELECT s3_key, payload FROM readmodel.s3_contract_payloads
UNION ALL
SELECT s3_key, payload FROM readmodel.s3_team_contract_payloads
UNION ALL
SELECT s3_key, payload FROM readmodel.s3_index_payloads
UNION ALL
SELECT s3_key, payload FROM readmodel.s3_player_search_payloads
UNION ALL
SELECT s3_key, payload FROM readmodel.s3_game_date_payloads
UNION ALL
SELECT s3_key, payload FROM readmodel.s3_game_payloads;
