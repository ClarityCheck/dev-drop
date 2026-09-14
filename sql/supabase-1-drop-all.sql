-- =====================================================================
-- STEP 1 of 2 — remove everything the CA DROP pipeline owns in Supabase.
--
--   >>> DEV ONLY. This is destructive and there is no undo. <<<
--
-- Check you are on the dev project before running a single line:
--     SELECT current_database(), inet_server_addr();
-- The Hyperdrive config points at db.vsqxnmrvvjsgcrudpruy.supabase.co.
--
-- RUN IT A SECTION AT A TIME. The Supabase SQL editor wraps whatever you
-- submit in a single transaction, so one failing statement rolls back
-- everything that ran before it in the same submission -- which looks
-- exactly like the script having done nothing at all, because it has.
--
-- WHAT YOU LOSE, and it does not come back on its own:
-- ca_drop_work_item holds the DROP work items Cron A loaded, and Cron C
-- resolves a match through it -- KV gives a (list_type, work_item_id),
-- and that pair has to find a row here to become a match. An empty table
-- means every match reports as unlinked and NOTHING is recorded.
--
-- So after step 2, re-run Cron A before Cron C:
--     POST /api/downloader/start
-- KV itself is untouched by this file, so the hash set survives.
--
-- Nothing here touches the OTP tables that share the schema.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1a. Look before you leap. Run this on its own first.
--     Everything it lists is what the rest of the file destroys.
-- ---------------------------------------------------------------------
SELECT 'table' AS kind, tablename AS name, NULL::text AS detail
FROM pg_tables
WHERE schemaname = 'public' AND tablename LIKE 'ca\_drop\_%'
UNION ALL
SELECT 'policy', policyname, tablename
FROM pg_policies
WHERE schemaname = 'public' AND tablename LIKE 'ca\_drop\_%'
UNION ALL
SELECT 'sequence', sequencename, NULL::text
FROM pg_sequences
WHERE schemaname = 'public' AND sequencename LIKE 'ca\_drop\_%'
UNION ALL
SELECT 'grant', grantee, table_name
FROM information_schema.table_privileges
WHERE grantee = 'drop_workflow'
UNION ALL
SELECT 'role', rolname, NULL::text
FROM pg_roles
WHERE rolname = 'drop_workflow'
ORDER BY 1, 2, 3;

-- How much is actually in there, so the re-load is a known quantity.
SELECT
  (SELECT count(*) FROM public.ca_drop_work_item)       AS work_items,
  (SELECT count(*) FROM public.ca_drop_work_item_match) AS matches;


-- ---------------------------------------------------------------------
-- 1b. The tables.
--
-- Child first: ca_drop_work_item_match has a foreign key into
-- ca_drop_work_item, so this order needs no CASCADE. Dropping a table
-- takes its indexes, its policies, its constraints and the sequences
-- behind its bigserial columns with it.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS public.ca_drop_work_item_match;
DROP TABLE IF EXISTS public.ca_drop_work_item;


-- ---------------------------------------------------------------------
-- 1c. The role.
--
-- A role cannot be dropped while anything still refers to it. After 1b
-- the table and sequence grants are gone with the tables, so what is
-- left is the schema grant and the membership.
--
-- Note what is NOT here. The obvious tool for this is
--     DROP OWNED BY drop_workflow;
-- and on Supabase it fails:
--     ERROR: 42501: permission denied to drop objects
--     DETAIL: Only roles with privileges of role "drop_workflow" may
--             drop objects owned by it.
-- Supabase's `postgres` is not a real superuser, and DROP OWNED BY wants
-- either superuser or membership in the role being cleared. Revoking the
-- specific grants needs neither, and there are only two of them.
--
-- REVOKE ... FROM authenticator undoes the PostgREST membership from the
-- original setup. Step 2 does not recreate it: the Worker reaches
-- Postgres through Hyperdrive and has no use for PostgREST.
-- ---------------------------------------------------------------------
REVOKE ALL ON SCHEMA public FROM drop_workflow;
REVOKE drop_workflow FROM authenticator;
DROP ROLE IF EXISTS drop_workflow;

-- If DROP ROLE still reports that objects depend on the role, something
-- granted it a privilege this file does not know about. Find it:
--
--   SELECT table_schema, table_name, privilege_type
--   FROM information_schema.table_privileges WHERE grantee = 'drop_workflow'
--   UNION ALL
--   SELECT 'schema', nspname, 'USAGE/CREATE'
--   FROM pg_namespace WHERE has_schema_privilege('drop_workflow', nspname, 'USAGE');
--
-- Or take the blunt route, which works because granting yourself
-- membership is what DROP OWNED BY was missing:
--
--   GRANT drop_workflow TO current_user;
--   DROP OWNED BY drop_workflow;
--   DROP ROLE drop_workflow;


-- ---------------------------------------------------------------------
-- 1d. Confirm it is all gone. Every query should return no rows.
-- ---------------------------------------------------------------------
SELECT tablename FROM pg_tables
WHERE schemaname = 'public' AND tablename LIKE 'ca\_drop\_%';

SELECT sequencename FROM pg_sequences
WHERE schemaname = 'public' AND sequencename LIKE 'ca\_drop\_%';

SELECT policyname, tablename FROM pg_policies
WHERE schemaname = 'public' AND tablename LIKE 'ca\_drop\_%';

SELECT rolname FROM pg_roles WHERE rolname = 'drop_workflow';

SELECT grantee, table_name FROM information_schema.table_privileges
WHERE grantee = 'drop_workflow';
