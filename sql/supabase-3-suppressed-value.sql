-- =====================================================================
-- public.ca_drop_suppressed_value
--
-- Replaces public.ca_drop_suppressed_search from
-- supabase-2-suppressed-search.sql, which was built for a design with
-- per-finding counters, matched key families and a clearing path. None of
-- that earned its keep: the clearing alone put a Hyperdrive connection
-- and an UPDATE on the overwhelming majority of lookups, which have
-- nothing to clear.
--
-- Run this in the Supabase SQL editor, a section at a time.
--
-- IT DROPS THE OLD TABLE. Section 1 discards whatever is in
-- ca_drop_suppressed_search. That is the intent -- the table is days old
-- and holds a cache-warming hint, not a compliance record. If it has rows
-- you want, copy them out first:
--     SELECT search_type, hash, first_seen_at FROM public.ca_drop_suppressed_search;
-- There is no way to recover the values from it, only the hashes, which
-- is one of the reasons it is being replaced.
--
-- WHAT THIS IS FOR
--
-- A phone number or e-mail address can be absent from the DROP list and
-- still produce a report that cannot be served, because the aggregated
-- data carries a person who IS listed. The subject is not suppressed; the
-- report is. Without a record of that, every later search for the value
-- calls the providers, spends a credit, builds the report and has it
-- suppressed again.
--
-- So the value is written here, once, and POST /api/drop/check answers
-- from it as well as from the DROP hash set in KV.
--
-- WHAT IT IS NOT
--
-- Not a DROP list, and not a compliance record. A row here says
-- "searching this yields data we must suppress", which is derived and
-- ours. ca_drop_work_item stays the record of DROP membership and
-- ca_drop_work_item_match the record of what was found and erased; Cron B
-- reports from those and must never report from here. /api/drop/check
-- returns `onDropList` separately for exactly that reason.
--
-- ON PLAINTEXT
--
-- `value` is the normalized e-mail address or phone number, in the clear.
-- That is deliberate and it is a change from the table it replaces, which
-- stored only a hash and so could never be read, audited or explained.
-- ca_drop_work_item_match already stores matched_normalized_value in the
-- clear, so this is the same exposure class as an existing table rather
-- than a new one: reachable only by the drop_workflow role, with RLS on
-- and the PostgREST roles revoked.
-- =====================================================================


-- =====================================================================
-- 1. Out with the old
-- =====================================================================

DROP TABLE IF EXISTS public.ca_drop_suppressed_value;
DROP TABLE IF EXISTS public.ca_drop_suppressed_search;


-- =====================================================================
-- 2. The table
-- =====================================================================

CREATE TABLE public.ca_drop_suppressed_value
(
    id          bigserial   PRIMARY KEY,

    -- 'email' or 'phone'. A people search is a name, which is not a DROP
    -- key and which /api/drop/check does not accept, so nothing writes
    -- 'people' here today; the CHECK allows it for when something can.
    search_type text        NOT NULL
                            CHECK (search_type IN ('email', 'phone', 'people')),

    -- The value as DROP normalizes it for this type: an e-mail with
    -- whitespace removed and lowercased, a phone reduced to its last ten
    -- digits. Normalized rather than raw so that one consumer is one row
    -- however the search was typed, and so that the KV key can be derived
    -- from it without guessing.
    value       text        NOT NULL CHECK (value <> ''),

    -- When it was added. A repeat search for the same value changes
    -- nothing: the row is written once and stays, so this is the date the
    -- value was first found in a matching report.
    added_at    timestamptz NOT NULL DEFAULT now(),

    UNIQUE (search_type, value)
);

-- The repair walks every row, oldest first, paged by id -- which the
-- primary key already serves, so there is no second index to keep.


-- =====================================================================
-- 3. Grants for the Workflow role
--
-- The role already exists -- supabase-1-create.sql section 2 created it.
-- No UPDATE: a row is inserted once and never changed. A repeat match is
-- ON CONFLICT DO NOTHING. No DELETE either -- a value recorded here is
-- not processed again, and nothing revokes that.
-- =====================================================================

GRANT SELECT, INSERT ON public.ca_drop_suppressed_value                 TO drop_workflow;
GRANT USAGE, SELECT  ON SEQUENCE public.ca_drop_suppressed_value_id_seq TO drop_workflow;


-- =====================================================================
-- 4. Keep the PostgREST roles out
-- =====================================================================

REVOKE ALL ON public.ca_drop_suppressed_value FROM anon, authenticated;

ALTER TABLE public.ca_drop_suppressed_value ENABLE ROW LEVEL SECURITY;

CREATE POLICY drop_workflow_full_access
  ON public.ca_drop_suppressed_value
  FOR ALL TO drop_workflow
  USING (true) WITH CHECK (true);


-- =====================================================================
-- 5. Verify
-- =====================================================================

-- drop_workflow, and INSERT, SELECT -- nothing else, nobody else.
SELECT grantee, table_name,
       string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privs
FROM information_schema.table_privileges
WHERE table_schema = 'public'
  AND table_name = 'ca_drop_suppressed_value'
GROUP BY grantee, table_name
ORDER BY grantee;

-- true.
SELECT relname, relrowsecurity AS rls_enabled
FROM pg_class
WHERE oid = 'public.ca_drop_suppressed_value'::regclass;

-- gone.
SELECT count(*) AS old_table_still_there
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'ca_drop_suppressed_search';


-- =====================================================================
-- 6. Afterwards
--
-- Nothing to backfill; it fills as reports are checked. An empty table
-- only means /api/drop/check answers from the DROP list alone, which is
-- what it did before any of this existed.
--
-- Cron A with clearKv wipes the KV namespace, this table's fast path
-- included, so run POST /api/kv-repair/start after a clear. The repair
-- restores every row, because every row still applies.
-- =====================================================================
