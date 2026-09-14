-- =====================================================================
-- Cron C (drop-reports-cleanup) — ClickHouse setup.
-- Service: ClarityCheck - DEV  (cu7iy7dd3r.eu-central-1.aws.clickhouse.cloud)
--
-- Run as an admin user, TOP TO BOTTOM. Safe to re-run in full: every
-- statement is either IF NOT EXISTS, a REVOKE, or an idempotent GRANT.
--
-- Do not cherry-pick the REVOKE in section 1. It is written to run BEFORE
-- the grants in section 2, and on its own it takes those grants away with
-- everything else. To clear the dangling grants without disturbing the live
-- ones, revoke them by name instead:
--     REVOKE SELECT, INSERT ON default.ca_drop_match_run FROM drop_workflow_role;
--     REVOKE SELECT, INSERT, ALTER DELETE
--            ON default.ca_drop_work_items FROM drop_workflow_role;
--
-- No ON CLUSTER anywhere. Access entities in ClickHouse Cloud are stored
-- replicated (system.users.storage = 'replicated'), so users, roles and
-- grants propagate on their own.
--
-- Cron C touches exactly three things. Anything granted beyond them is
-- surface area with no user:
--   SELECT  default.ca_drop_combined_search_result   the candidate keys
--   SYSTEM REFRESH VIEW on that view                 step ①
--   SELECT  system.view_refreshes                    waiting for step ①
--   SELECT + ALTER DELETE on entity_search_results   the expire
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. The role
-- ---------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS drop_workflow_role;

-- Start from nothing. This is what clears the grants left behind by
-- ca_drop_work_items and ca_drop_match_run: dropping those tables did NOT
-- remove their privileges, because ClickHouse records privileges against the
-- NAME rather than the object. They are still in system.grants today,
-- pointing at nothing — and would silently apply again to any future table
-- that reuses either name.
REVOKE ALL ON *.* FROM drop_workflow_role;


-- ---------------------------------------------------------------------
-- 2. The three grants Cron C actually needs
-- ---------------------------------------------------------------------

-- The candidate keys.
GRANT SELECT ON default.ca_drop_combined_search_result TO drop_workflow_role;

-- Step ① runs  SYSTEM REFRESH VIEW default.ca_drop_combined_search_result.
-- SELECT does not cover it. Without this line the first step of every run
-- fails with ACCESS_DENIED — which is the real reason the workflow's
-- refreshView parameter had to default to false.
GRANT SYSTEM VIEWS ON default.ca_drop_combined_search_result TO drop_workflow_role;

-- REQUIRED, and confirmed the hard way. Having asked for a refresh, the
-- workflow waits for it by polling here. Reads of system tables are implicit
-- and row-filtered for many tables, which is why this looked optional when
-- this file was first written. It is not. Without it, the first run fails:
--     Code: 497. drop_workflow: Not enough privileges. To execute this
--     query, it's necessary to have the grant SELECT ON system.view_refreshes
GRANT SELECT ON system.view_refreshes TO drop_workflow_role;

-- The expire. Cron C now erases the matched records themselves, which is what
-- makes status = 'deleted' a true statement rather than a claim.
--
--   ALTER DELETE  runs ALTER TABLE ... DELETE, a real mutation: the parts are
--                 rewritten without the rows. NOT the lightweight DELETE FROM,
--                 which only marks rows and leaves the data on disk until some
--                 later merge -- not good enough for a statutory deletion.
--   SELECT        the predicate reads the columns, and the workflow counts the
--                 matching rows before and after so it can verify the delete
--                 instead of trusting it.
--
-- This is the one real widening of the role. entity_search_results holds the
-- raw provider payloads, so SELECT on it is broad -- it is the table this whole
-- pipeline exists to protect. Granted because the alternative is a workflow
-- that reports deletions it cannot confirm.
GRANT SELECT, ALTER DELETE ON default.entity_search_results TO drop_workflow_role;

-- Note the asymmetry: reading through ca_drop_combined_search_result never
-- needed this. That view is declared SQL SECURITY DEFINER, so a refresh reads
-- the source as its definer (sql-console:access@claritycheck.com), never as
-- drop_workflow. The definer is load-bearing: if that account is removed,
-- refreshes break and no grant in this file will fix it.


-- ---------------------------------------------------------------------
-- 3. The user
--
-- It already exists, so CREATE USER IF NOT EXISTS is a no-op and the
-- placeholder password below is never read — leave it as is. IF NOT EXISTS
-- skips the whole statement, so this cannot clobber the password that is in
-- the CH_PASSWORD secret.
--
-- To rotate the password deliberately, run this instead, then
-- `npx wrangler secret put CH_PASSWORD`:
--     ALTER USER drop_workflow IDENTIFIED WITH sha256_password BY '<new>';
-- ---------------------------------------------------------------------
CREATE USER IF NOT EXISTS drop_workflow
  IDENTIFIED WITH sha256_password BY 'REPLACE_ONLY_IF_CREATING_A_NEW_USER';

-- Everything reaches the user through the role and nothing directly.
-- This revokes privileges only; role membership is separate and survives.
REVOKE ALL ON *.* FROM drop_workflow;

GRANT drop_workflow_role TO drop_workflow;
ALTER USER drop_workflow DEFAULT ROLE drop_workflow_role;


-- ---------------------------------------------------------------------
-- 4. Settings
--
-- ALTER USER ... SETTINGS REPLACES the whole list, so all four go in one
-- statement or the omitted ones are dropped.
--
-- readonly = 0 is already the default and changes nothing — keep it as
-- documentation, because it is not obviously the default and SYSTEM REFRESH
-- VIEW is blocked under readonly 1 or 2.
--
-- max_execution_time = 900 is fine as is. Every query Cron C sends is either
-- the one-row view_refreshes poll or a batch query that runs in well under a
-- second. The refresh itself is asynchronous and is not bounded by this.
--
-- max_result_rows = 200000 does almost nothing for this workload. A batch
-- query returns at most `rowScan` rows — 200 by default — so the row cap is
-- never approached. But each of those rows carries a whole key array, which
-- makes rows a poor proxy for size. max_result_bytes is the cap that bites:
-- 64 MiB is about 45x a default batch (25,000 keys is roughly 1.4 MB), so it
-- will not trip in normal use, and it turns "batchSize was raised to
-- something absurd" into a clean ClickHouse error rather than a Worker that
-- runs out of memory part-way through a batch.
-- ---------------------------------------------------------------------
ALTER USER drop_workflow SETTINGS
  max_execution_time = 900,
  max_result_rows    = 200000,
  max_result_bytes   = 67108864,
  readonly           = 0;


-- =====================================================================
-- 5. Verify — run as admin
-- =====================================================================

-- Expect EXACTLY these five rows. Anything naming ca_drop_work_items or
-- ca_drop_match_run means the REVOKE in section 1 did not run.
--
--   SELECT        default   ca_drop_combined_search_result
--   SYSTEM VIEWS  default   ca_drop_combined_search_result
--   SELECT        default   entity_search_results
--   ALTER DELETE  default   entity_search_results
--   SELECT        system    view_refreshes
SELECT access_type, database, table
FROM system.grants
WHERE role_name = 'drop_workflow_role'
ORDER BY database, table, access_type;

-- Expect: default_roles_list = ['drop_workflow_role'], default_roles_all = 0
SELECT name, auth_type, default_roles_all, default_roles_list
FROM system.users
WHERE name = 'drop_workflow';

-- Expect the four settings from section 4.
SELECT setting_name, value
FROM system.settings_profile_elements
WHERE user_name = 'drop_workflow'
ORDER BY setting_name;

-- Expect no rows: the user must hold no privileges of its own.
SELECT access_type, database, table
FROM system.grants
WHERE user_name = 'drop_workflow';
