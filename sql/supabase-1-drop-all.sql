-- =====================================================================
-- STEP 1 of 2 — remove everything the CA DROP pipeline owns in Supabase.
--
--   >>> DEV ONLY. This is destructive and there is no undo. <<<
--
-- Check you are on the dev project before running a single line:
--     SELECT current_database(), inet_server_addr();
-- The Hyperdrive config points at db.vsqxnmrvvjsgcrudpruy.supabase.co.
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
-- A role cannot be dropped while anything in the database still refers
-- to it. DROP OWNED BY clears both halves of that: objects the role owns
-- (none are expected -- the tables belong to postgres) and every
-- privilege granted TO it, including ones on objects outside this
-- pipeline that would otherwise block the DROP with a bare
-- "role cannot be dropped because some objects depend on it".
--
-- REVOKE ... FROM authenticator undoes the PostgREST membership from the
-- original setup. Step 2 does not recreate it: the Worker reaches
-- Postgres through Hyperdrive and has no use for PostgREST.
-- ---------------------------------------------------------------------
REVOKE drop_workflow FROM authenticator;
DROP OWNED BY drop_workflow;
DROP ROLE IF EXISTS drop_workflow;


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
