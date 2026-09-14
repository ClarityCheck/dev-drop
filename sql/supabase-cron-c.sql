-- =====================================================================
-- Cron C (drop-reports-cleanup) — Supabase setup.
-- Run in the Supabase SQL editor. The ClickHouse half is in
-- clickhouse-cron-c.sql.
--
-- Sections 1 and 2 must be applied together with the matching Worker
-- code; section 3 is independent and can go in on its own.
-- =====================================================================


-- =====================================================================
-- 1. GRANTS — what the workflow needs to write a match
--
-- The original setup granted drop_workflow nothing at all on
-- ca_drop_work_item_match, so every match write failed with
-- "permission denied for table ca_drop_work_item_match".
-- =====================================================================

GRANT SELECT, INSERT ON public.ca_drop_work_item_match                  TO drop_workflow;
GRANT USAGE, SELECT  ON SEQUENCE public.ca_drop_work_item_match_id_seq  TO drop_workflow;

-- SELECT on ca_drop_work_item is already granted and is still required:
-- the insert resolves DROP's (list_type, work_item_id) to the bigint id by
-- joining against it.


-- =====================================================================
-- 2. One work item can match more than one identifier
--
-- UNIQUE (ca_drop_work_item_id) allows a single match row per work item.
-- That is wrong for NDZ and NameVIN, and quietly so. Those hashes stand
-- for a PERSON -- a name with a date of birth and a ZIP, or a name with a
-- VIN -- not for one contact detail. One DROP work item therefore matches
-- every e-mail address and phone number that person appears under, and
-- keyed on the work item alone only the first survives: ON CONFLICT DO
-- NOTHING discards the others without a word.
--
-- They cannot be recovered later. The expire in entity_search_results
-- destroys the matched rows, and this table is the only place the link
-- from a work_item_id to what it matched exists. Whatever is not written
-- here is gone.
--
-- Widening the key to the (work item, identifier) pair keeps every
-- distinct match and stays idempotent across re-runs. Cron B is
-- unaffected -- it asks whether a work item matched at all, which is
-- still one EXISTS.
--
-- Volume: this can write many rows for one work item where the old key
-- wrote one. That is the real shape of the data, but it is worth knowing
-- before a production run.
-- =====================================================================

-- The constraint is the auto-named one from UNIQUE (ca_drop_work_item_id).
-- Confirm the name first if this project was built by hand:
--   SELECT conname FROM pg_constraint
--   WHERE conrelid = 'public.ca_drop_work_item_match'::regclass AND contype = 'u';

ALTER TABLE public.ca_drop_work_item_match
  DROP CONSTRAINT IF EXISTS ca_drop_work_item_match_ca_drop_work_item_id_key;

ALTER TABLE public.ca_drop_work_item_match
  ADD CONSTRAINT ca_drop_work_item_match_item_value_key
  UNIQUE (ca_drop_work_item_id, matched_normalized_value);

-- Cron B's lookup, and the cascade delete, both want this.
CREATE INDEX IF NOT EXISTS ix_cadwim_work_item
  ON public.ca_drop_work_item_match (ca_drop_work_item_id);


-- =====================================================================
-- 3. SECURITY — check this before the next run
--
-- Both tables live in `public`, which Supabase exposes through PostgREST,
-- and Supabase's default privileges grant anon / authenticated on tables
-- created there. With RLS off, grants alone decide, so if those roles
-- hold a grant then anyone with the project's anon key can read both
-- tables over HTTP.
--
-- That matters here more than usual. ca_drop_work_item_match holds
-- matched_normalized_value, which is the consumer's actual e-mail
-- address or phone number -- the plaintext the rest of this pipeline
-- goes out of its way never to log.
--
-- The existing CREATE POLICY on ca_drop_work_item does NOT protect
-- anything: a policy is inert until RLS is enabled on the table, and
-- ENABLE ROW LEVEL SECURITY was never run. It reads as protection
-- without being any.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 3a. Look first. Any row for anon or authenticated is the problem.
-- ---------------------------------------------------------------------
SELECT grantee,
       table_name,
       string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privs
FROM information_schema.table_privileges
WHERE table_schema = 'public'
  AND table_name IN ('ca_drop_work_item', 'ca_drop_work_item_match')
GROUP BY grantee, table_name
ORDER BY grantee, table_name;

-- Is RLS on? Expect rowsecurity = true for both once section 3c has run.
SELECT relname, relrowsecurity AS rls_enabled
FROM pg_class
WHERE oid IN ('public.ca_drop_work_item'::regclass,
              'public.ca_drop_work_item_match'::regclass);


-- ---------------------------------------------------------------------
-- 3b. Take the PostgREST roles off both tables.
--
-- Nothing of ours reads them as anon or authenticated -- the Worker
-- connects as drop_workflow through Hyperdrive -- so this costs nothing.
-- ---------------------------------------------------------------------
REVOKE ALL ON public.ca_drop_work_item       FROM anon, authenticated;
REVOKE ALL ON public.ca_drop_work_item_match FROM anon, authenticated;


-- ---------------------------------------------------------------------
-- 3c. Turn RLS on, so a future default-privileges grant cannot undo 3b.
--
-- drop_workflow does not own these tables, so RLS applies to it too and
-- it needs a policy or the workflow stops working. The one on
-- ca_drop_work_item already exists; the match table has none.
-- ---------------------------------------------------------------------
ALTER TABLE public.ca_drop_work_item       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ca_drop_work_item_match ENABLE ROW LEVEL SECURITY;

CREATE POLICY drop_workflow_full_access
  ON public.ca_drop_work_item_match
  FOR ALL
  TO drop_workflow
  USING (true)
  WITH CHECK (true);

-- Expect two rows, one per table, both for drop_workflow.
SELECT tablename, policyname, roles, cmd
FROM pg_policies
WHERE tablename IN ('ca_drop_work_item', 'ca_drop_work_item_match')
ORDER BY tablename;


-- ---------------------------------------------------------------------
-- 3d. Optional tidy-up.
--
-- GRANT drop_workflow TO authenticator existed so PostgREST could assume
-- the role. The Worker goes through Hyperdrive and does not use
-- PostgREST, so that is an unused path into the role:
--
--   REVOKE drop_workflow FROM authenticator;
--
-- Leave it if anything else still reaches this role through PostgREST.
-- ---------------------------------------------------------------------


-- =====================================================================
-- 4. Verify — expect ca_drop_work_item (SELECT, INSERT, UPDATE) and
-- ca_drop_work_item_match (SELECT, INSERT), for drop_workflow only.
-- =====================================================================
SELECT table_name,
       string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privs
FROM information_schema.table_privileges
WHERE grantee = 'drop_workflow'
GROUP BY table_name
ORDER BY table_name;
