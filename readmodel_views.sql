-- Read-model views for the Next.js hockey app.
--
-- These views are additive: they do not modify newapi/staging tables.
-- Run this after your ETL tables/procedures exist:
--
--   psql "$DATABASE_URL" -f sql/readmodel_views.sql
--
-- The views intentionally keep the same column aliases currently returned by
-- lib/queries.js so the app can migrate from complex SQL to simple selects.

CREATE SCHEMA IF NOT EXISTS readmodel;

CREATE OR REPLACE VIEW readmodel.players AS
WITH unique_players AS (
    SELECT DISTINCT ON (p."playerId")
        p."playerId",
        p."firstName",
        p."lastName",
        p."birthDate",
        p."birthCountry",
        p."position",
        p."sweaterNumber",
        p."shootsCatches"
    FROM newapi.players p
    ORDER BY
        p."playerId",
        p."isActive" DESC NULLS LAST,
        p."birthCountry" ASC NULLS LAST
)
SELECT
    p."playerId",
    CONCAT(p."firstName", ' ', p."lastName") AS player_name,
    TO_CHAR(p."birthDate", 'YYYY-MM-DD') AS "birthDate",
    p."birthCountry",
    p."position",
    p."sweaterNumber",
    p."shootsCatches",
    d."displayAbbrev",
    d."ordinalPick",
    ARRAY_AGG(DISTINCT d."draftYear") AS draft_seasons,
    ARRAY_AGG(DISTINCT d."overallPick") AS draft_position
FROM unique_players p
LEFT JOIN newapi.drafts d ON p."playerId" = d."playerId"
GROUP BY
    p."playerId",
    CONCAT(p."firstName", ' ', p."lastName"),
    TO_CHAR(p."birthDate", 'YYYY-MM-DD'),
    p."birthCountry",
    p."position",
    p."sweaterNumber",
    p."shootsCatches",
    d."displayAbbrev",
    d."ordinalPick";

CREATE OR REPLACE VIEW readmodel.player_skater_stats AS
SELECT
    s."playerId",
    s."season",
    s."leagueAbbrev" AS "league.name",
    t.id AS "team.id",
    s."teamName.default" AS "team.name",
    s."gamesPlayed" AS "stat.games",
    s."goals" AS "stat.goals",
    s."pim" AS "stat.pim",
    s."plusMinus" AS "stat.plusMinus",
    s."points" AS "stat.points",
    s."assists" AS "stat.assists"
FROM newapi.season_skater s
LEFT JOIN newapi.teams t ON s."teamName.default" = t."fullName"
WHERE s.is_active = true
  AND s."gameTypeId" = 2;

CREATE OR REPLACE VIEW readmodel.player_goalie_stats AS
SELECT
    g."playerId",
    g."season",
    g."leagueAbbrev" AS "league.name",
    t.id AS "team.id",
    g."teamName.default" AS "team.name",
    g."gamesPlayed" AS "stat.games",
    g."wins" AS "stat.wins",
    g."losses" AS "stat.losses",
    g."goals" AS "stat.goals",
    g."savePctg" AS "stat.savePercentage",
    g."goalsAgainstAvg" AS "stat.goalAgainstAverage",
    g."shutouts" AS "stat.shutouts",
    g."pim" AS "stat.pim",
    g."otLosses" AS "stat.otl",
    g."assists" AS "stat.assists"
FROM newapi.season_goalie g
LEFT JOIN newapi.teams t ON g."teamName.default" = t."fullName"
WHERE g.is_active = true
  AND g."gameTypeId" = 2;

CREATE OR REPLACE VIEW readmodel.player_awards AS
SELECT
    "playerId",
    trophy_default,
    "seasonId"::BIGINT AS "seasonId",
    "gamesPlayed"::DOUBLE PRECISION AS "gamesPlayed",
    goals::DOUBLE PRECISION AS goals,
    assists::DOUBLE PRECISION AS assists,
    points::DOUBLE PRECISION AS points,
    "plusMinus"::DOUBLE PRECISION AS "plusMinus",
    pim::DOUBLE PRECISION AS pim
FROM newapi.awards;

CREATE OR REPLACE VIEW readmodel.player_ids AS
SELECT DISTINCT
    p."playerId"
FROM newapi.players p
WHERE p."isActive" = true;

CREATE OR REPLACE VIEW readmodel.teams AS
SELECT
    "rawTricode" AS abbreviation,
    "fullName" AS name,
    id
FROM newapi.teams
WHERE "active" = true;

CREATE OR REPLACE VIEW readmodel.team_ids AS
SELECT id
FROM newapi.teams;

CREATE OR REPLACE VIEW readmodel.team_info AS
SELECT DISTINCT
    id,
    "rawTricode" AS abbreviation,
    "fullName"
FROM newapi.teams;

CREATE OR REPLACE VIEW readmodel.team_seasons AS
SELECT
    ts."teamId",
    ts."seasonId",
    ts."wins",
    ts."losses",
    ts."points",
    ts."goalsAgainstPerGame",
    ts."goalsForPerGame",
    ts."regulationAndOtWins" AS "row",
    ts."pointPct",
    ts."winsInShootout",
    ts."otLosses",
    ROW_NUMBER() OVER (
        PARTITION BY ts."teamId"
        ORDER BY ts."seasonId" DESC
    ) AS season_rank
FROM newapi.team_season ts;

CREATE OR REPLACE VIEW readmodel.team_skaters AS
WITH unique_players AS (
    SELECT DISTINCT ON (p."playerId")
        p."playerId",
        p."firstName",
        p."lastName",
        p."position"
    FROM newapi.players p
    ORDER BY
        p."playerId",
        p."isActive" DESC NULLS LAST,
        p."birthCountry" ASC NULLS LAST
),
combined_data AS (
    SELECT
        t.id,
        s."playerId",
        s.season,
        t."rawTricode" AS "triCode",
        CONCAT(p."firstName", ' ', p."lastName") AS "fullName",
        SUM(CASE WHEN s."gameTypeId" = 2 THEN s."gamesPlayed" ELSE 0 END) AS "gamesPlayed",
        SUM(CASE WHEN s."gameTypeId" = 3 THEN s."gamesPlayed" ELSE 0 END) AS "playoffGamesPlayed",
        SUM(CASE WHEN s."gameTypeId" = 2 THEN s."goals" ELSE 0 END) AS "goals",
        SUM(CASE WHEN s."gameTypeId" = 3 THEN s."goals" ELSE 0 END) AS "playoffGoals",
        SUM(CASE WHEN s."gameTypeId" = 2 THEN s."assists" ELSE 0 END) AS "assists",
        SUM(CASE WHEN s."gameTypeId" = 3 THEN s."assists" ELSE 0 END) AS "playoffAssists",
        SUM(CASE WHEN s."gameTypeId" = 2 THEN s."points" ELSE 0 END) AS "points",
        SUM(CASE WHEN s."gameTypeId" = 3 THEN s."points" ELSE 0 END) AS "playoffPoints",
        SUM(CASE WHEN s."gameTypeId" = 2 THEN s."pim" ELSE 0 END) AS "penaltyMinutes",
        SUM(CASE WHEN s."gameTypeId" = 3 THEN s."pim" ELSE 0 END) AS "playoffPenaltyMinutes",
        SUM(CASE WHEN s."gameTypeId" = 2 THEN s."plusMinus" ELSE 0 END)::DOUBLE PRECISION AS "plusMinus",
        SUM(CASE WHEN s."gameTypeId" = 3 THEN s."plusMinus" ELSE 0 END)::DOUBLE PRECISION AS "playoffPlusMinus",
        p."position" AS "positionCode"
    FROM newapi.season_skater s
    JOIN newapi.teams t ON s."teamName.default" = t."fullName"
    LEFT JOIN unique_players p ON s."playerId" = p."playerId"
    WHERE s.is_active = true
      AND s."leagueAbbrev" = 'NHL'
      AND s."gameTypeId" IN (2, 3)
    GROUP BY
        t.id,
        s."playerId",
        s.season,
        t."rawTricode",
        CONCAT(p."firstName", ' ', p."lastName"),
        p."position"
)
SELECT DISTINCT
    id,
    "playerId",
    season,
    "triCode",
    "fullName",
    "gamesPlayed",
    "playoffGamesPlayed",
    "goals",
    "playoffGoals",
    "assists",
    "playoffAssists",
    "points",
    "playoffPoints",
    "penaltyMinutes",
    "playoffPenaltyMinutes",
    "plusMinus",
    "playoffPlusMinus",
    "positionCode"
FROM combined_data;

CREATE OR REPLACE VIEW readmodel.team_goalies AS
WITH unique_players AS (
    SELECT DISTINCT ON (p."playerId")
        p."playerId",
        p."firstName",
        p."lastName"
    FROM newapi.players p
    ORDER BY
        p."playerId",
        p."isActive" DESC NULLS LAST,
        p."birthCountry" ASC NULLS LAST
),
combined_goalie_data AS (
    SELECT
        t.id,
        g."playerId",
        g.season,
        t."rawTricode" AS "team",
        CONCAT(p."firstName", ' ', p."lastName") AS "fullName",
        SUM(CASE WHEN g."gameTypeId" = 2 THEN g."gamesPlayed" ELSE 0 END)::NUMERIC AS "gamesPlayed",
        SUM(CASE WHEN g."gameTypeId" = 3 THEN g."gamesPlayed" ELSE 0 END)::NUMERIC AS "playoffGamesPlayed",
        SUM(CASE WHEN g."gameTypeId" = 2 THEN g."goals" ELSE 0 END)::NUMERIC AS "goals",
        SUM(CASE WHEN g."gameTypeId" = 3 THEN g."goals" ELSE 0 END)::NUMERIC AS "playoffGoals",
        SUM(CASE WHEN g."gameTypeId" = 2 THEN g."assists" ELSE 0 END)::NUMERIC AS "assists",
        SUM(CASE WHEN g."gameTypeId" = 3 THEN g."assists" ELSE 0 END)::NUMERIC AS "playoffAssists",
        SUM(CASE WHEN g."gameTypeId" = 2 THEN g."goals" + g."assists" ELSE 0 END)::NUMERIC AS "points",
        SUM(CASE WHEN g."gameTypeId" = 3 THEN g."goals" + g."assists" ELSE 0 END)::NUMERIC AS "playoffPoints",
        SUM(CASE WHEN g."gameTypeId" = 2 THEN g."wins" ELSE 0 END)::NUMERIC AS "wins",
        SUM(CASE WHEN g."gameTypeId" = 3 THEN g."wins" ELSE 0 END)::NUMERIC AS "playoffWins",
        SUM(CASE WHEN g."gameTypeId" = 2 THEN g."losses" ELSE 0 END)::NUMERIC AS "losses",
        SUM(CASE WHEN g."gameTypeId" = 3 THEN g."losses" ELSE 0 END)::NUMERIC AS "playoffLosses",
        SUM(CASE WHEN g."gameTypeId" = 2 THEN g."goalsAgainstAvg" * g."gamesPlayed" ELSE 0 END)
            / NULLIF(SUM(CASE WHEN g."gameTypeId" = 2 THEN g."gamesPlayed" ELSE 0 END), 0) AS "goalsAgainstAverage",
        SUM(CASE WHEN g."gameTypeId" = 3 THEN g."goalsAgainstAvg" * g."gamesPlayed" ELSE 0 END)
            / NULLIF(SUM(CASE WHEN g."gameTypeId" = 3 THEN g."gamesPlayed" ELSE 0 END), 0) AS "playoffGoalsAgainstAverage",
        SUM(CASE WHEN g."gameTypeId" = 2 THEN g."savePctg" * g."gamesPlayed" ELSE 0 END)
            / NULLIF(SUM(CASE WHEN g."gameTypeId" = 2 THEN g."gamesPlayed" ELSE 0 END), 0) AS "savePercentage",
        SUM(CASE WHEN g."gameTypeId" = 3 THEN g."savePctg" * g."gamesPlayed" ELSE 0 END)
            / NULLIF(SUM(CASE WHEN g."gameTypeId" = 3 THEN g."gamesPlayed" ELSE 0 END), 0) AS "playoffSavePercentage",
        SUM(CASE WHEN g."gameTypeId" = 2 THEN g."pim" ELSE 0 END)::NUMERIC AS "penaltyMinutes",
        SUM(CASE WHEN g."gameTypeId" = 3 THEN g."pim" ELSE 0 END)::NUMERIC AS "playoffPenaltyMinutes"
    FROM newapi.season_goalie g
    JOIN newapi.teams t ON g."teamName.default" = t."fullName"
    LEFT JOIN unique_players p ON g."playerId" = p."playerId"
    WHERE g.is_active = true
      AND g."leagueAbbrev" = 'NHL'
      AND g."gameTypeId" IN (2, 3)
    GROUP BY
        t.id,
        g."playerId",
        g.season,
        t."rawTricode",
        CONCAT(p."firstName", ' ', p."lastName")
)
SELECT DISTINCT
    id,
    "playerId",
    season,
    "team",
    "fullName",
    "gamesPlayed",
    "playoffGamesPlayed",
    "goals",
    "playoffGoals",
    "assists",
    "playoffAssists",
    "points",
    "playoffPoints",
    "wins",
    "playoffWins",
    "losses",
    "playoffLosses",
    "goalsAgainstAverage",
    "playoffGoalsAgainstAverage",
    "savePercentage",
    "playoffSavePercentage",
    "penaltyMinutes",
    "playoffPenaltyMinutes"
FROM combined_goalie_data;

CREATE OR REPLACE VIEW readmodel.team_playoff_years AS
SELECT
    "triCode" AS abbreviation,
    season::BIGINT AS season
FROM newapi.team_gametypes
WHERE COALESCE("gameTypes", '') ~ '(^|[^0-9])3([^0-9]|$)';

CREATE OR REPLACE VIEW readmodel.active_rosters AS
SELECT
    id,
    "teamAbbreviation",
    "positionGroup",
    "playerId",
    headshot,
    "firstName",
    "lastName",
    "sweaterNumber",
    "positionCode",
    "shootsCatches",
    "heightInInches",
    "weightInPounds",
    "heightInCentimeters",
    "weightInKilograms",
    "birthDate",
    "birthCity",
    "birthCountry",
    "birthStateProvince",
    active,
    occurrence_number,
    data_hash,
    created_at,
    updated_at
FROM newapi.rosters_active;

CREATE OR REPLACE VIEW readmodel.available_seasons AS
SELECT DISTINCT season::INTEGER AS season
FROM newapi.season_skater
WHERE "leagueAbbrev" = 'NHL'
UNION
SELECT DISTINCT season::INTEGER AS season
FROM newapi.season_goalie
WHERE "leagueAbbrev" = 'NHL';

CREATE OR REPLACE VIEW readmodel.season_point_leaders AS
WITH skater_totals AS (
    SELECT
        s."playerId",
        s.season,
        SUM(s.goals) AS goals,
        SUM(s."gamesPlayed") AS "gamesPlayed",
        SUM(s.assists) AS assists,
        SUM(s.points) AS points,
        (ARRAY_AGG(s."teamName.default" ORDER BY s."gamesPlayed" DESC))[1] AS "teamName"
    FROM newapi.season_skater s
    WHERE s."leagueAbbrev" = 'NHL'
      AND s.is_active = true
    GROUP BY s."playerId", s.season
),
goalie_totals AS (
    SELECT
        g."playerId",
        g.season,
        SUM(g.goals) AS goals,
        SUM(g."gamesPlayed") AS "gamesPlayed",
        SUM(g.assists) AS assists,
        (ARRAY_AGG(g."teamName.default" ORDER BY g."gamesPlayed" DESC))[1] AS "teamName"
    FROM newapi.season_goalie g
    WHERE g."leagueAbbrev" = 'NHL'
      AND g.is_active = true
    GROUP BY g."playerId", g.season
),
leader_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(s.season, g.season)
            ORDER BY
                COALESCE(s.points, (g.goals + g.assists), 0) DESC NULLS LAST,
                COALESCE(s.goals, g.goals, 0) DESC NULLS LAST
        ) AS row_number,
        CONCAT(p."firstName", ' ', p."lastName") AS player_name,
        p."playerId",
        p."position",
        COALESCE(s.season, g.season) AS season,
        COALESCE(s."teamName", g."teamName") AS "team.name",
        COALESCE(s.goals, g.goals, 0) AS "stat.goals",
        COALESCE(s."gamesPlayed", g."gamesPlayed", 0) AS "stat.games",
        COALESCE(s.assists, g.assists, 0) AS "stat.assists",
        COALESCE(s.points, (g.goals + g.assists), 0) AS "stat.points",
        COALESCE(ts.id, tg.id) AS "team.id"
    FROM (
        SELECT DISTINCT ON ("playerId")
            "playerId",
            "firstName",
            "lastName",
            "position"
        FROM newapi.players
    ) p
    LEFT JOIN skater_totals s ON p."playerId" = s."playerId"
    LEFT JOIN goalie_totals g ON p."playerId" = g."playerId"
    LEFT JOIN newapi.teams ts ON ts."fullName" = s."teamName" AND ts.active = true
    LEFT JOIN newapi.teams tg ON tg."fullName" = g."teamName" AND tg.active = true
    WHERE s."playerId" IS NOT NULL
       OR g."playerId" IS NOT NULL
)
SELECT *
FROM leader_rows
WHERE row_number <= 200;

CREATE OR REPLACE VIEW readmodel.season_goalie_leaders AS
WITH goalie_totals AS (
    SELECT
        g."playerId",
        g.season,
        SUM(g."gamesPlayed") AS "gamesPlayed",
        SUM(g.wins) AS wins,
        SUM(g.losses) AS losses,
        SUM(g."otLosses") AS "otLosses",
        SUM(g."goalsAgainstAvg" * g."gamesPlayed") / NULLIF(SUM(g."gamesPlayed"), 0) AS "goalsAgainstAvg",
        SUM(g."savePctg" * g."gamesPlayed") / NULLIF(SUM(g."gamesPlayed"), 0) AS "savePctg",
        SUM(g.shutouts) AS shutouts,
        (ARRAY_AGG(g."teamName.default" ORDER BY g."gamesPlayed" DESC))[1] AS "teamName"
    FROM newapi.season_goalie g
    WHERE g."leagueAbbrev" = 'NHL'
      AND g.is_active = true
    GROUP BY g."playerId", g.season
),
leader_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY g.season
            ORDER BY g.wins DESC NULLS LAST, g."savePctg" DESC NULLS LAST
        ) AS row_number,
        CONCAT(p."firstName", ' ', p."lastName") AS player_name,
        p."playerId",
        g.season,
        g."teamName" AS "team.name",
        g."gamesPlayed" AS "stat.games",
        g.wins AS "stat.wins",
        g.losses AS "stat.losses",
        g."otLosses" AS "stat.otl",
        g."goalsAgainstAvg" AS "stat.gaa",
        g."savePctg" AS "stat.savePct",
        g.shutouts AS "stat.shutouts",
        t.id AS "team.id"
    FROM (
        SELECT DISTINCT ON ("playerId")
            "playerId",
            "firstName",
            "lastName"
        FROM newapi.players
    ) p
    JOIN goalie_totals g ON p."playerId" = g."playerId"
    LEFT JOIN newapi.teams t ON t."fullName" = g."teamName" AND t.active = true
)
SELECT *
FROM leader_rows
WHERE row_number <= 100;

CREATE OR REPLACE VIEW readmodel.draft_years AS
SELECT DISTINCT
    "draftYear"
FROM newapi.drafts;

CREATE OR REPLACE VIEW readmodel.draft_picks AS
SELECT
    d."draftYear",
    d."playerId",
    d."overallPick",
    d."pickInRound",
    d."round",
    CONCAT(d."firstName", ' ', d."lastName") AS "playerName",
    d."positionCode",
    d."amateurLeague",
    d."amateurClubName",
    d."teamAbbrev",
    d."teamId",
    d."teamId" AS "draftedByTeamId",
    SUM(COALESCE(s."gamesPlayed", g."gamesPlayed", NULL)) AS games,
    SUM(COALESCE(s.goals, g.goals, NULL)) AS goals,
    SUM(COALESCE(s.assists, g.assists, NULL)) AS assists,
    SUM(COALESCE(s.points, g.points, NULL)) AS points,
    SUM(COALESCE(s."penaltyMinutes", g."penaltyMinutes", NULL)) AS pim,
    CASE
        WHEN MAX(COALESCE(s.season, g.season)) IS NOT NULL THEN
            CONCAT(
                SUBSTRING(CAST(MAX(COALESCE(s.season, g.season)) AS text), 1, 4),
                '-',
                SUBSTRING(CAST(MAX(COALESCE(s.season, g.season)) AS text), 5)
            )
        ELSE CAST(MAX(COALESCE(s.season, g.season)) AS text)
    END AS last_season
FROM newapi.drafts d
LEFT JOIN newapi.skaters s
    ON d."playerId" = s."playerId"
   AND s."gameType" = 2
   AND s.is_active = true
LEFT JOIN newapi.goalies g
    ON d."playerId" = g."playerId"
   AND g."gameType" = 2
   AND g.is_active = true
GROUP BY
    d."draftYear",
    d."playerId",
    d."overallPick",
    d."pickInRound",
    d."round",
    d."positionCode",
    d."amateurLeague",
    d."amateurClubName",
    d."teamAbbrev",
    d."teamId",
    d."firstName",
    d."lastName";

CREATE OR REPLACE VIEW readmodel.player_search AS
WITH skater_stats AS (
    SELECT
        "playerId",
        SUM(CASE WHEN "gameType" = 2 THEN "gamesPlayed" ELSE 0 END) AS games,
        SUM(CASE WHEN "gameType" = 2 THEN goals ELSE 0 END) AS goals,
        SUM(CASE WHEN "gameType" = 2 THEN assists ELSE 0 END) AS assists,
        SUM(CASE WHEN "gameType" = 2 THEN points ELSE 0 END) AS points,
        MAX(season) AS last_season
    FROM newapi.skaters
    WHERE is_active = true
    GROUP BY "playerId"
),
goalie_stats AS (
    SELECT
        "playerId",
        SUM(CASE WHEN "gameType" = 2 THEN "gamesPlayed" ELSE 0 END) AS games,
        SUM(CASE WHEN "gameType" = 2 THEN wins ELSE 0 END) AS wins,
        SUM(CASE WHEN "gameType" = 2 THEN losses ELSE 0 END) AS losses,
        MAX(season) AS last_season
    FROM newapi.goalies
    WHERE is_active = true
    GROUP BY "playerId"
),
unique_players AS (
    SELECT DISTINCT ON (p."playerId")
        p."playerId",
        CONCAT(p."firstName", ' ', p."lastName") AS player_name,
        p."position",
        p."birthCountry",
        r."teamAbbreviation" AS team_abbrev,
        t.id AS team_id,
        t."fullName" AS team_name,
        COALESCE(sk.games, g.games, 0) AS games,
        COALESCE(sk.goals, 0) AS goals,
        COALESCE(sk.assists, 0) AS assists,
        COALESCE(sk.points, 0) AS points,
        COALESCE(g.wins, 0) AS wins,
        COALESCE(g.losses, 0) AS losses,
        COALESCE(sk.last_season, g.last_season) AS last_season
    FROM newapi.players p
    LEFT JOIN newapi.rosters_active r ON p."playerId" = r."playerId"
    LEFT JOIN newapi.teams t ON r."teamAbbreviation" = t."rawTricode"
    LEFT JOIN skater_stats sk ON p."playerId" = sk."playerId"
    LEFT JOIN goalie_stats g ON p."playerId" = g."playerId"
    ORDER BY p."playerId"
)
SELECT
    *,
    CONCAT_WS(
        ' ',
        player_name,
        team_abbrev,
        team_name,
        "playerId"::TEXT
    ) AS "searchText"
FROM unique_players;

CREATE OR REPLACE VIEW readmodel.games AS
SELECT
    g.id,
    g."gameDate",
    g."gameState",
    g."awayTeam_id",
    g."awayTeam_abbrev",
    g."awayTeam_score",
    g."awayTeam_logo",
    g."awayTeam_darkLogo",
    g."homeTeam_id",
    g."homeTeam_abbrev",
    g."homeTeam_score",
    g."homeTeam_logo",
    g."homeTeam_darkLogo",
    g."periodDescriptor_periodType",
    g."gameOutcome_lastPeriodType",
    g."startTimeUTC",
    g."gameCenterLink",
    ta.id AS "awayTeam_dbId",
    th.id AS "homeTeam_dbId",
    g."awayTeam_commonName_default",
    g."awayTeam_placeName_default",
    g."awayTeam_sog",
    g."homeTeam_commonName_default",
    g."homeTeam_placeName_default",
    g."homeTeam_sog",
    g."venue_default",
    g."venueLocation_default",
    g."venueTimezone",
    g."easternUTCOffset",
    g."venueUTCOffset",
    g."limitedScoring",
    g."shootoutInUse",
    g."regPeriods",
    g."otInUse",
    g."tiesInUse",
    g."periodDescriptor_number",
    g."periodDescriptor_otPeriods",
    g."periodDescriptor_maxRegulationPeriods",
    g."clock_timeRemaining",
    g."clock_secondsRemaining",
    g."clock_running",
    g."clock_inIntermission"
FROM newapi.games g
LEFT JOIN newapi.teams ta ON g."awayTeam_abbrev" = ta."rawTricode" AND ta.active = true
LEFT JOIN newapi.teams th ON g."homeTeam_abbrev" = th."rawTricode" AND th.active = true;

CREATE OR REPLACE VIEW readmodel.game_goals AS
SELECT
    game_id,
    event_id,
    season,
    game_type,
    game_date,
    away_team_abbrev,
    home_team_abbrev,
    period_number,
    period_type,
    time_in_period,
    team_abbrev,
    is_home,
    strength,
    situation_code,
    scoring_player_id,
    scoring_player_name,
    assist1_player_id,
    assist1_player_name,
    assist2_player_id,
    assist2_player_name,
    away_score,
    home_score
FROM newapi.game_goals;

CREATE OR REPLACE VIEW readmodel.game_penalties AS
SELECT
    game_id,
    penalty_index,
    season,
    game_type,
    game_date,
    away_team_abbrev,
    home_team_abbrev,
    period_number,
    period_type,
    time_in_period,
    team_abbrev,
    penalty_type,
    duration,
    desc_key,
    committed_by_player_name,
    committed_by_sweater_number,
    drawn_by_player_name,
    drawn_by_sweater_number,
    served_by_name
FROM newapi.game_penalties;

CREATE OR REPLACE VIEW readmodel.game_three_stars AS
SELECT
    game_id,
    star,
    season,
    game_type,
    game_date,
    player_id,
    player_name,
    team_abbrev,
    sweater_number,
    position,
    goals,
    assists,
    points
FROM newapi.game_three_stars;
