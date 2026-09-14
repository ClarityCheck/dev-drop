-- =====================================================================
-- STEP 2 of 2 — the CA DROP pipeline in Supabase, from nothing.
-- Run after supabase-1-drop-all.sql, in the Supabase SQL editor.
--
-- Everything the build taught us is folded in, so this is the whole
-- schema rather than a patch on the old one:
--   * grants on ca_drop_work_item_match, which the first version missed
--     entirely -- every match write failed on it
--   * UNIQUE on the (work item, identifier) PAIR, not the work item
--   * RLS actually enabled, and anon / authenticated revoked
--
-- The ClickHouse half is in clickhouse-cron-c.sql and is unaffected.
--
-- AFTERWARDS, and it matters: ca_drop_work_item is empty, so Cron C has
-- nothing to resolve a match against and would record none. Re-run
-- Cron A first --  POST /api/downloader/start  -- then Cron C.
-- =====================================================================


-- =====================================================================
-- 1. Tables
-- =====================================================================

CREATE TABLE public.ca_drop_work_item
(
    id            bigserial   PRIMARY KEY,
    list_type     text        NOT NULL
                              CHECK (list_type IN ('email', 'phone', 'ndz', 'namevin')),
    work_item_id  text        NOT NULL,          -- DROP's Id, case-sensitive
    hash          text        NOT NULL,          -- the hashed value, Base64 as published
    request_date  date,
    added_at      timestamptz NOT NULL DEFAULT now(),
    status        text        CHECK (status IN ('exempted', 'deleted', 'opted_out', 'not_found')),
    status_set_at timestamptz,

    UNIQUE (list_type, work_item_id)             -- lets the sync upsert
);
-- Status as DROP names it. Codes 0 and 1 are not used by the spec:
--   2 Exempted   3 Deleted   4 Opted out   5 Not found
-- status NULL = nothing has looked at this item yet. Cron B reports NULL
-- as not_found; exempted / deleted / opted_out are set explicitly.

CREATE INDEX ca_drop_work_item_pending_idx
    ON public.ca_drop_work_item (list_type, added_at) WHERE status IS NULL;

CREATE INDEX ca_drop_work_item_hash_idx
    ON public.ca_drop_work_item (list_type, hash);


CREATE TABLE public.ca_drop_work_item_match
(
    id                       bigserial   PRIMARY KEY,
    ca_drop_work_item_id     bigint      NOT NULL
                                         REFERENCES public.ca_drop_work_item (id)
                                         ON DELETE CASCADE,
    matched_normalized_value text        NOT NULL
                                         CHECK (matched_normalized_value <> ''),
    matched_at               timestamptz NOT NULL DEFAULT now(),

    -- The PAIR, not the work item alone.
    --
    -- NDZ and NameVIN hashes stand for a PERSON -- a name with a date of
    -- birth and a ZIP, or a name with a VIN -- not for one contact
    -- detail. So a single DROP work item legitimately matches every
    -- e-mail address and phone number that person appears under. Keyed
    -- on the work item alone, the first match won and ON CONFLICT DO
    -- NOTHING discarded the rest without a word -- permanently, because
    -- the expire destroys the matched rows and this table is the only
    -- place the link from a work_item_id to what it matched survives.
    --
    -- Cron B is indifferent: it asks whether a work item matched at all,
    -- which is still one EXISTS.
    UNIQUE (ca_drop_work_item_id, matched_normalized_value)
);

CREATE INDEX ix_cadwim_normalized_value
    ON public.ca_drop_work_item_match (matched_normalized_value);

-- Cron B's lookup and the ON DELETE CASCADE both want this.
CREATE INDEX ix_cadwim_work_item
    ON public.ca_drop_work_item_match (ca_drop_work_item_id);


-- =====================================================================
-- 2. The role the Workflow connects as
--
-- Granted on these two tables only, so it cannot reach the OTP tables
-- that share the schema. No DELETE, no DDL.
--
-- Put the same password into the Hyperdrive config, which is where the
-- Worker's copy of it lives:
--     npx wrangler hyperdrive update df29c50286574600a5f37d7c2490589c \
--         --origin-password '<password>'
-- =====================================================================

CREATE ROLE drop_workflow LOGIN PASSWORD 'PUT_THE_REAL_PASSWORD_HERE' NOINHERIT;

GRANT USAGE ON SCHEMA public TO drop_workflow;

-- Cron A upserts work items; Cron B will update status.
GRANT SELECT, INSERT, UPDATE ON public.ca_drop_work_item                 TO drop_workflow;
GRANT USAGE, SELECT  ON SEQUENCE public.ca_drop_work_item_id_seq         TO drop_workflow;

-- Cron C writes matches. SELECT on ca_drop_work_item above is also
-- required by the same statement: it resolves DROP's
-- (list_type, work_item_id) to the bigint id by joining against it.
GRANT SELECT, INSERT ON public.ca_drop_work_item_match                   TO drop_workflow;
GRANT USAGE, SELECT  ON SEQUENCE public.ca_drop_work_item_match_id_seq   TO drop_workflow;

-- Deliberately NOT recreated: GRANT drop_workflow TO authenticator.
-- That existed so PostgREST could assume the role. The Worker goes
-- through Hyperdrive, so it would be an unused way into the role.


-- =====================================================================
-- 3. Keep the PostgREST roles out
--
-- Both tables are in `public`, which Supabase exposes through PostgREST,
-- and Supabase's default privileges grant anon / authenticated on tables
-- created there -- including the two just created above. With RLS off,
-- grants alone decide, so the project's anon key would read both tables
-- over HTTP.
--
-- ca_drop_work_item_match holds matched_normalized_value: the consumer's
-- actual e-mail address or phone number, the plaintext the rest of this
-- pipeline goes out of its way never to log.
-- =====================================================================

REVOKE ALL ON public.ca_drop_work_item       FROM anon, authenticated;
REVOKE ALL ON public.ca_drop_work_item_match FROM anon, authenticated;

-- RLS is the backstop, so a later default-privileges grant cannot quietly
-- undo the revokes above. drop_workflow does not own these tables, so RLS
-- applies to it too and the policies below are required, not decorative --
-- without them the workflow stops working.
ALTER TABLE public.ca_drop_work_item       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ca_drop_work_item_match ENABLE ROW LEVEL SECURITY;

CREATE POLICY drop_workflow_full_access
  ON public.ca_drop_work_item
  FOR ALL TO drop_workflow
  USING (true) WITH CHECK (true);

CREATE POLICY drop_workflow_full_access
  ON public.ca_drop_work_item_match
  FOR ALL TO drop_workflow
  USING (true) WITH CHECK (true);


-- =====================================================================
-- 4. Verify
-- =====================================================================

-- Expect exactly two rows, and the grantee column should say
-- drop_workflow and nothing else:
--   ca_drop_work_item        INSERT, SELECT, UPDATE
--   ca_drop_work_item_match  INSERT, SELECT
SELECT grantee, table_name,
       string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privs
FROM information_schema.table_privileges
WHERE table_schema = 'public'
  AND table_name IN ('ca_drop_work_item', 'ca_drop_work_item_match')
GROUP BY grantee, table_name
ORDER BY grantee, table_name;

-- Both true.
SELECT relname, relrowsecurity AS rls_enabled
FROM pg_class
WHERE oid IN ('public.ca_drop_work_item'::regclass,
              'public.ca_drop_work_item_match'::regclass);

-- One policy per table, both for drop_workflow.
SELECT tablename, policyname, roles, cmd
FROM pg_policies
WHERE schemaname = 'public' AND tablename LIKE 'ca\_drop\_%'
ORDER BY tablename;

-- The pair, not the work item alone.
SELECT conname, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = 'public.ca_drop_work_item_match'::regclass AND contype = 'u';


-- =====================================================================
-- 5. Then, in order
--
--   1. Set the password in the Hyperdrive config (see section 2).
--   2. POST /api/downloader/start        Cron A, refills work items
--   3. POST /api/workflow/start          Cron C
--
-- Cron C against an empty ca_drop_work_item finds matches and links none
-- of them -- watch matchesLinked and matchesUnlinked in the summary.
-- =====================================================================
