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
-- ============================================================================

DO $$
BEGIN
  IF current_setting('app.allow_bootstrap', true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION
      'Refusing to DROP newapi.gamecenter. Bootstrap is opt-in. For live RDS run gamecenter_table_upsert.sql. To bootstrap: SET app.allow_bootstrap = ''on'';';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS newapi;

DROP VIEW IF EXISTS newapi.gamecenter_player_points CASCADE;
DROP VIEW IF EXISTS newapi.gamecenter_goals CASCADE;
DROP VIEW IF EXISTS newapi.gamecenter_etl_summary CASCADE;
DROP TABLE IF EXISTS newapi.gamecenter_etl_log CASCADE;
DROP TABLE IF EXISTS newapi.gamecenter CASCADE;
