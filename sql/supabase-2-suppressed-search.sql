-- =====================================================================
-- public.ca_drop_suppressed_search
--
-- Run in the Supabase SQL editor AFTER supabase-1-create.sql. A section
-- at a time -- the editor submits as one transaction, so a failure late
-- in the script rolls back everything before it and looks like nothing
-- ran.
--
-- WHAT THIS IS FOR
--
-- A phone number or e-mail address can be absent from the DROP list and
-- still produce a report that cannot be served: the aggregated data comes
-- back carrying a person whose name + date of birth + ZIP is on the list,
-- or a secondary contact detail that is. The subject is not suppressed;
-- the report is.
--
-- Nothing recorded that. Every search for such a value called the
-- providers, spent a credit, assembled a report, had it suppressed, and
-- arrived back where it started -- and did it again on the next search,
-- because ClickHouse caches only what was clean enough to store.
--
-- So the outcome is remembered here. /api/drop/check reads it and answers
-- listed: true, which stops the funnel before the first provider call.
--
-- WHAT IT IS NOT
--
-- Not a DROP list. A row here says "searching this yields data we must
-- suppress", which is derived and ours; the DROP list says "this
-- identifier belongs to a consumer who asked to be deleted", which is
-- California's. The two are kept apart deliberately:
--
--   * ca_drop_work_item stays the record of DROP membership. Cron B
--     reports status from THERE and must never report from here.
--   * /api/drop/check returns `source` alongside `listed`, so a caller
--     that needs the statutory fact can tell which one it got.
--
-- NO PLAINTEXT
--
-- The lookup key is the hash, exactly as /api/drop/check already computes
-- it: DROP's normalization for the type, then SHA-256 over the UTF-8
-- bytes, Base64. The gate has the hash in hand and needs no plaintext to
-- answer, and this table is read on ordinary traffic rather than inside a
-- job -- so the e-mail address and phone number stay out of it. The
-- durable link from a DROP work item to the value it matched already
-- exists, once, in ca_drop_work_item_match.
-- =====================================================================


-- =====================================================================
-- 1. Table
-- =====================================================================

CREATE TABLE public.ca_drop_suppressed_search
(
    id            bigserial   PRIMARY KEY,

    -- What was searched, and the hash of it under DROP's rules for that
    -- type. Together they are the lookup: the same (type, hash) pair the
    -- gate computes from the raw value it was given.
    search_type   text        NOT NULL
                              CHECK (search_type IN ('email', 'phone', 'people')),
    hash          text        NOT NULL CHECK (hash <> ''),

    -- Which DROP key families the report matched on, for triage. An
    -- 'ndz' or 'namevin' match means the report carried a listed PERSON;
    -- 'email' or 'phone' means it carried a listed contact detail.
    matched       text[]      NOT NULL DEFAULT '{}',

    -- How many records of the report were suppressed, and how many it
    -- had. For a phone or e-mail report both are 1; the columns exist so
    -- a people report can use the same table without a second shape.
    records_suppressed integer NOT NULL DEFAULT 0,
    records_total      integer NOT NULL DEFAULT 0,

    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now(),
    times_seen    integer     NOT NULL DEFAULT 1,

    -- Set when the finding stops applying -- the consumer came off the
    -- DROP list, or a later search produced a clean report. Kept rather
    -- than deleted so the history of a suppression survives, and read as
    -- "not suppressed any more" by the gate.
    cleared_at    timestamptz,

    UNIQUE (search_type, hash)      -- lets the recorder upsert
);

-- The gate's read: one row by (type, hash), still in force.
CREATE INDEX ca_drop_suppressed_search_active_idx
    ON public.ca_drop_suppressed_search (search_type, hash)
    WHERE cleared_at IS NULL;

-- Rebuilding the KV fast path reads these oldest-first, in pages.
CREATE INDEX ca_drop_suppressed_search_id_idx
    ON public.ca_drop_suppressed_search (id)
    WHERE cleared_at IS NULL;


-- =====================================================================
-- 2. Grants for the Workflow role
--
-- The role already exists -- supabase-1-create.sql section 2 created it.
-- UPDATE is needed because the recorder upserts: a value searched again
-- bumps last_seen_at and times_seen rather than inserting a duplicate.
-- Still no DELETE and no DDL.
-- =====================================================================

GRANT SELECT, INSERT, UPDATE ON public.ca_drop_suppressed_search        TO drop_workflow;
GRANT USAGE, SELECT  ON SEQUENCE public.ca_drop_suppressed_search_id_seq TO drop_workflow;


-- =====================================================================
-- 3. Keep the PostgREST roles out
--
-- Same reasoning as the other two tables. There is no plaintext in here,
-- but a hash of one identifier is still an identifier: anyone holding a
-- candidate e-mail address can hash it and ask whether it is present.
-- =====================================================================

REVOKE ALL ON public.ca_drop_suppressed_search FROM anon, authenticated;

ALTER TABLE public.ca_drop_suppressed_search ENABLE ROW LEVEL SECURITY;

CREATE POLICY drop_workflow_full_access
  ON public.ca_drop_suppressed_search
  FOR ALL TO drop_workflow
  USING (true) WITH CHECK (true);


-- =====================================================================
-- 4. Verify
-- =====================================================================

-- drop_workflow, and INSERT, SELECT, UPDATE -- nothing else, nobody else.
SELECT grantee, table_name,
       string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privs
FROM information_schema.table_privileges
WHERE table_schema = 'public'
  AND table_name = 'ca_drop_suppressed_search'
GROUP BY grantee, table_name
ORDER BY grantee;

-- true.
SELECT relname, relrowsecurity AS rls_enabled
FROM pg_class
WHERE oid = 'public.ca_drop_suppressed_search'::regclass;

-- One policy, for drop_workflow.
SELECT tablename, policyname, roles, cmd
FROM pg_policies
WHERE schemaname = 'public' AND tablename = 'ca_drop_suppressed_search';


-- =====================================================================
-- 5. Afterwards
--
-- Nothing to backfill. The table fills as reports are checked, and an
-- empty one only means the gate answers from the DROP list alone -- which
-- is what it did before this existed.
--
-- The KV fast path is rebuilt from here, not the other way round. Cron A
-- with clearKv wipes the namespace, so run
--     POST /api/kv-repair/start
-- after a clear to put the suppression keys back.
-- =====================================================================
