-- =====================================================================
-- CA DROP — the whole Supabase side, from nothing.
--
-- One file, run once, on an empty database. There are no DROPs, no
-- ALTERs and no IF NOT EXISTS: every statement creates. If one fails
-- saying something already exists, this is not a fresh database — stop
-- and find out what is there before going further, because the rest of
-- the file assumes it built everything it depends on.
--
-- Run it in the Supabase SQL editor A SECTION AT A TIME. The editor
-- submits as one transaction, so a failure late in the script rolls back
-- everything before it and looks like nothing ran.
--
-- ─── EDIT ONE THING FIRST ────────────────────────────────────────────
--
-- Section 4 creates the role the Worker connects as, with a placeholder
-- password. Replace it, and put the same value into the Hyperdrive
-- config, which is where the Worker's copy lives:
--
--     npx wrangler hyperdrive update <config-id> --origin-password '<password>'
--
-- ─── WHAT THIS BUILDS ────────────────────────────────────────────────
--
--   ca_drop_work_item         one row per identifier DROP gave us, its
--                             status, and whether it has been revoked
--   ca_drop_work_item_match   what each work item matched, which is the
--                             only evidence that survives an erase
--   ca_drop_suppressed_value  values whose reports must be suppressed
--                             although the value itself is not listed
--
-- The ClickHouse side is sql/clickhouse.sql. The rules these tables
-- serve are in BUSINESS-LOGIC.md.
-- =====================================================================


-- =====================================================================
-- 1. ca_drop_work_item — the identifiers, and what we did about them
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

    -- Status as DROP names it. Codes 0 and 1 are not used by the spec:
    --   2 Exempted   3 Deleted   4 Opted out   5 Not found
    -- NULL means nothing has looked at this item yet. Cron B reports NULL
    -- as not_found; the other three are set explicitly.
    status        text        CHECK (status IN ('exempted', 'deleted', 'opted_out', 'not_found')),
    status_set_at timestamptz,

    -- Set when the identifier appears in a DROP removals file: the
    -- consumer withdrew, or the state revoked the entry. The row is kept
    -- rather than deleted, because Cron B has to tell "never given to us"
    -- apart from "given and then withdrawn".
    --
    -- Everything that reads work items reads live ones only. A revoked
    -- item is deleted from KV by design, so counting it would make every
    -- revocation look like a key KV had lost — and the KV repair would
    -- put the hash back and suppress a consumer who asked to be released.
    revoked_at    timestamptz,

    UNIQUE (list_type, work_item_id)             -- lets Cron A upsert
);

CREATE INDEX ca_drop_work_item_pending_idx
    ON public.ca_drop_work_item (list_type, added_at) WHERE status IS NULL;

CREATE INDEX ca_drop_work_item_hash_idx
    ON public.ca_drop_work_item (list_type, hash);

-- Every reader filters revoked_at IS NULL, so this is the index that
-- serves them.
CREATE INDEX ca_drop_work_item_live_idx
    ON public.ca_drop_work_item (id) WHERE revoked_at IS NULL;


-- =====================================================================
-- 2. ca_drop_work_item_match — what each work item matched
-- =====================================================================

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
    -- NDZ and NameVIN hashes stand for a PERSON — a name with a date of
    -- birth and a ZIP, or a name with a VIN — not for one contact detail.
    -- So a single DROP work item legitimately matches every e-mail
    -- address and phone number that person appears under. Keyed on the
    -- work item alone, the first match would win and ON CONFLICT DO
    -- NOTHING would discard the rest without a word — permanently,
    -- because the erase destroys the matched rows and this table is the
    -- only place the link from a work_item_id to what it matched survives.
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
-- 3. ca_drop_suppressed_value — searches that must be short-circuited
--
-- A phone number or e-mail can be absent from every DROP list and still
-- produce a report that cannot be served, because the aggregated data
-- carries a person who IS listed. The subject is not suppressed; the
-- report is. Without a record of that, every later search for the value
-- calls the providers, spends a credit, builds the report and has it
-- suppressed again.
--
-- One row per value, written the first time a report matches. No
-- counters, no clearing and no expiry: once a value is here it is not
-- processed again.
--
-- NOT a compliance record. "Searching this yields data we must suppress"
-- is ours and derived; "this identifier belongs to a consumer who asked
-- to be deleted" is California's and lives in ca_drop_work_item. Cron B
-- reports from that one and must never report from this one, which is
-- why /api/drop/check returns `onDropList` separately from `listed`.
--
-- `value` is the DROP-normalized identifier, in the clear — the same
-- exposure class as matched_normalized_value above, and for the same
-- reason: a record that cannot be read cannot be audited or explained.
-- =====================================================================

CREATE TABLE public.ca_drop_suppressed_value
(
    id          bigserial   PRIMARY KEY,

    -- 'email' or 'phone'. A people search is a name, which is not a DROP
    -- key and which /api/drop/check does not accept, so nothing writes
    -- 'people' here today; the CHECK allows it for when something can.
    search_type text        NOT NULL
                            CHECK (search_type IN ('email', 'phone', 'people')),

    -- Normalized rather than raw, so one consumer is one row however the
    -- search was typed, and so the KV key can be derived from it during a
    -- repair without guessing.
    value       text        NOT NULL CHECK (value <> ''),

    added_at    timestamptz NOT NULL DEFAULT now(),

    UNIQUE (search_type, value)
);

-- The repair walks every row, oldest first, paged by id — which the
-- primary key already serves, so there is no second index to keep.


-- =====================================================================
-- 4. The role the Worker connects as
--
-- REPLACE THE PASSWORD, and put the same value into the Hyperdrive
-- config. Granted on these three tables only, so it cannot reach the OTP
-- tables that share the schema. No DELETE anywhere, and no DDL.
-- =====================================================================

CREATE ROLE drop_workflow LOGIN PASSWORD 'PUT_THE_REAL_PASSWORD_HERE' NOINHERIT;

GRANT USAGE ON SCHEMA public TO drop_workflow;

-- Cron A upserts work items and stamps revocations; Cron B sets status.
GRANT SELECT, INSERT, UPDATE ON public.ca_drop_work_item                 TO drop_workflow;
GRANT USAGE, SELECT  ON SEQUENCE public.ca_drop_work_item_id_seq          TO drop_workflow;

-- Cron C and the incident path write matches. SELECT on ca_drop_work_item
-- above is required by the same statement: it resolves DROP's
-- (list_type, work_item_id) to the bigint id by joining against it.
GRANT SELECT, INSERT ON public.ca_drop_work_item_match                   TO drop_workflow;
GRANT USAGE, SELECT  ON SEQUENCE public.ca_drop_work_item_match_id_seq   TO drop_workflow;

-- The suppression hint. No UPDATE: a row is inserted once and never
-- changed, and a repeat match is ON CONFLICT DO NOTHING.
GRANT SELECT, INSERT ON public.ca_drop_suppressed_value                  TO drop_workflow;
GRANT USAGE, SELECT  ON SEQUENCE public.ca_drop_suppressed_value_id_seq  TO drop_workflow;

-- Deliberately NOT granted: drop_workflow TO authenticator. That would
-- exist so PostgREST could assume the role. The Worker goes through
-- Hyperdrive, so it would be an unused way into the role.


-- =====================================================================
-- 5. Keep the PostgREST roles out
--
-- All three tables are in `public`, which Supabase exposes through
-- PostgREST, and Supabase's default privileges grant anon / authenticated
-- on tables created there. With RLS off, grants alone decide, so the
-- project's anon key would read all of them over HTTP.
--
-- ca_drop_work_item_match holds matched_normalized_value and
-- ca_drop_suppressed_value holds value: the consumer's actual e-mail
-- address or phone number, the plaintext the rest of this pipeline goes
-- out of its way never to log.
--
-- RLS is the backstop, so a later default-privileges grant cannot quietly
-- undo the revokes. drop_workflow does not own these tables, so RLS
-- applies to it too and the policies are required, not decorative —
-- without them the Worker stops working.
-- =====================================================================

REVOKE ALL ON public.ca_drop_work_item        FROM anon, authenticated;
REVOKE ALL ON public.ca_drop_work_item_match  FROM anon, authenticated;
REVOKE ALL ON public.ca_drop_suppressed_value FROM anon, authenticated;

ALTER TABLE public.ca_drop_work_item        ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ca_drop_work_item_match  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ca_drop_suppressed_value ENABLE ROW LEVEL SECURITY;

CREATE POLICY drop_workflow_full_access
  ON public.ca_drop_work_item
  FOR ALL TO drop_workflow
  USING (true) WITH CHECK (true);

CREATE POLICY drop_workflow_full_access
  ON public.ca_drop_work_item_match
  FOR ALL TO drop_workflow
  USING (true) WITH CHECK (true);

CREATE POLICY drop_workflow_full_access
  ON public.ca_drop_suppressed_value
  FOR ALL TO drop_workflow
  USING (true) WITH CHECK (true);


-- =====================================================================
-- 6. Verify
-- =====================================================================

-- Expect exactly these three rows, grantee drop_workflow and nobody else:
--   ca_drop_suppressed_value  INSERT, SELECT
--   ca_drop_work_item         INSERT, SELECT, UPDATE
--   ca_drop_work_item_match   INSERT, SELECT
SELECT grantee, table_name,
       string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privs
FROM information_schema.table_privileges
WHERE table_schema = 'public'
  AND table_name IN ('ca_drop_work_item', 'ca_drop_work_item_match',
                     'ca_drop_suppressed_value')
GROUP BY grantee, table_name
ORDER BY grantee, table_name;

-- All three true.
SELECT relname, relrowsecurity AS rls_enabled
FROM pg_class
WHERE oid IN ('public.ca_drop_work_item'::regclass,
              'public.ca_drop_work_item_match'::regclass,
              'public.ca_drop_suppressed_value'::regclass);

-- One policy per table, all for drop_workflow.
SELECT tablename, policyname, roles, cmd
FROM pg_policies
WHERE schemaname = 'public' AND tablename LIKE 'ca\_drop\_%'
ORDER BY tablename;

-- The match uniqueness is on the PAIR, not the work item alone.
SELECT conname, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = 'public.ca_drop_work_item_match'::regclass AND contype = 'u';

-- revoked_at exists and is nullable.
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'ca_drop_work_item'
  AND column_name = 'revoked_at';


-- =====================================================================
-- 7. Then, in order
--
--   1. Put the password from section 4 into the Hyperdrive config.
--   2. Run sql/clickhouse.sql against the ClickHouse service.
--   3. POST /api/downloader/start     Cron A — fills the work items
--   4. POST /api/workflow/start       Cron C — matches and erases
--
-- Cron C against an empty ca_drop_work_item finds matches and links none
-- of them; watch matchesLinked and matchesUnlinked in the run summary.
-- =====================================================================
