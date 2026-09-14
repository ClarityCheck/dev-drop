-- =====================================================================
-- Cron C (drop-reports-cleanup) — Supabase setup.
-- Run in the Supabase SQL editor. The ClickHouse half is in
-- clickhouse-cron-c.sql. Everything here is additive; nothing drops.
--
-- Cron C writes public.ca_drop_work_item_match. The original setup granted
-- drop_workflow nothing on that table at all — only on ca_drop_work_item —
-- so every match write would fail with "permission denied for table
-- ca_drop_work_item_match".
-- =====================================================================

-- The insert itself, and the bigserial behind it.
GRANT SELECT, INSERT ON public.ca_drop_work_item_match             TO drop_workflow;
GRANT USAGE, SELECT   ON SEQUENCE public.ca_drop_work_item_match_id_seq TO drop_workflow;

-- SELECT on ca_drop_work_item is already granted and is still required: the
-- insert resolves DROP's (list_type, work_item_id) to the bigint id by
-- joining against it.


-- ---------------------------------------------------------------------
-- Row-level security — worth a look while you are in here.
--
-- The existing setup creates a policy on ca_drop_work_item but never runs
-- ALTER TABLE ... ENABLE ROW LEVEL SECURITY, so that policy is inert and
-- the grants above are what actually governs access. That is a perfectly
-- reasonable posture for a role granted on two tables, but it is not what
-- the policy suggests is happening. Decide which one you meant.
--
-- If you do enable RLS, the match table needs a policy too, or the insert
-- starts silently writing nothing:
--
--   ALTER TABLE public.ca_drop_work_item       ENABLE ROW LEVEL SECURITY;
--   ALTER TABLE public.ca_drop_work_item_match ENABLE ROW LEVEL SECURITY;
--
--   CREATE POLICY drop_workflow_full_access
--     ON public.ca_drop_work_item_match
--     FOR ALL TO drop_workflow
--     USING (true) WITH CHECK (true);
-- ---------------------------------------------------------------------


-- ---------------------------------------------------------------------
-- Also no longer needed, if you want to tidy up.
--
-- GRANT drop_workflow TO authenticator was there so PostgREST could assume
-- the role. The Worker reaches Postgres through Hyperdrive now and does not
-- use PostgREST, so that grant is an unused path into the role:
--
--   REVOKE drop_workflow FROM authenticator;
--
-- Leave it if anything else still goes through PostgREST as this role.
-- ---------------------------------------------------------------------


-- ---------------------------------------------------------------------
-- Verify — expect ca_drop_work_item (SELECT/INSERT/UPDATE) and
-- ca_drop_work_item_match (SELECT/INSERT), and nothing else.
-- ---------------------------------------------------------------------
SELECT table_name,
       string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privs
FROM information_schema.table_privileges
WHERE grantee = 'drop_workflow'
GROUP BY table_name
ORDER BY table_name;
