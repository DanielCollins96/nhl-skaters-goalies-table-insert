-- ============================================================================
-- FIRST-TIME / GREENFIELD BOOTSTRAP ONLY — DO NOT RUN ON POPULATED PRODUCTION
-- ============================================================================
-- This script DROPS newapi.gamecenter, its etl_log, and dependent views.
-- Running it on live RDS wipes production rows and cascaded dependents.
--
-- (a) First-time / wipe:
--     SET app.allow_bootstrap = 'on';
--     then run this file, then gamecenter_table_upsert.sql
--
-- (b) Live RDS re-apply (cast/function fixes, sync logic):
--     run gamecenter_table_upsert.sql only — never this file
--
-- Replaces the old in-file helper newapi.reset_gamecenter_schema(true).
--
-- This file sets ON_ERROR_STOP itself. Do not rely on the caller passing
-- -v ON_ERROR_STOP=1. The opt-in check and DROP share one DO block inside
-- one transaction: a failed guard cannot reach DROP.
-- ============================================================================

\set ON_ERROR_STOP on

BEGIN;

DO $$
BEGIN
  IF current_setting('app.allow_bootstrap', true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION
      'Refusing to DROP newapi.gamecenter. Bootstrap is opt-in. For live RDS run gamecenter_table_upsert.sql. To bootstrap: SET app.allow_bootstrap = ''on'';';
  END IF;

  EXECUTE 'CREATE SCHEMA IF NOT EXISTS newapi';
  EXECUTE 'DROP VIEW IF EXISTS newapi.gamecenter_player_points CASCADE';
  EXECUTE 'DROP VIEW IF EXISTS newapi.gamecenter_goals CASCADE';
  EXECUTE 'DROP VIEW IF EXISTS newapi.gamecenter_etl_summary CASCADE';
  EXECUTE 'DROP TABLE IF EXISTS newapi.gamecenter_etl_log CASCADE';
  EXECUTE 'DROP TABLE IF EXISTS newapi.gamecenter CASCADE';
END $$;

COMMIT;
