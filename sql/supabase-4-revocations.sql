-- =====================================================================
-- Revocations on public.ca_drop_work_item
--
-- Run in the Supabase SQL editor. Additive and idempotent: it adds one
-- column and one index, and re-running it changes nothing.
--
-- WHAT WAS WRONG
--
-- DROP publishes a removals file alongside the four lists: a consumer has
-- withdrawn their request, or the state has revoked the entry. Cron A
-- deleted those hashes from KV, so the gate stopped suppressing them
-- immediately, and that part was right.
--
-- Nothing was written to ca_drop_work_item, because there was nowhere to
-- write it. The row stayed exactly as active as before, and three things
-- read it as though the work item were still live:
--
--   kv-repair      pages ca_drop_work_item and puts every hash back into
--                  KV. A revoked hash was therefore RESURRECTED by the
--                  next repair, and the consumer suppressed again after
--                  asking not to be. This is the one with live effect.
--   /api/kv-health samples ca_drop_work_item and checks each hash is in
--                  KV. A revoked item is absent by design, so the health
--                  check reported it as missing and said the gate was
--                  under-suppressing -- a false alarm on the one signal
--                  that is supposed to mean something.
--   Cron C         compares countWorkItems() against what it read out of
--                  KV and logs an error when Supabase has more. Every
--                  revocation widened that gap permanently.
--
-- And Cron B, when it exists, would report a revoked work item to
-- California as though it were still ours to answer for.
--
-- So the revocation needs to be a fact in the table, not a counter in a
-- log line.
-- =====================================================================


-- =====================================================================
-- 1. The column
-- =====================================================================

ALTER TABLE public.ca_drop_work_item
    ADD COLUMN IF NOT EXISTS revoked_at timestamptz;

COMMENT ON COLUMN public.ca_drop_work_item.revoked_at IS
    'Set when the identifier appeared in a DROP removals file. The row is '
    'kept rather than deleted: it is the record that we were once asked to '
    'suppress this consumer and then released. A revoked item is excluded '
    'from the KV rebuild, from the KV health sample, from the expected-count '
    'comparison, and from anything Cron B reports.';

-- Everything that reads work items now reads only the live ones, so the
-- index that serves them is the partial one.
CREATE INDEX IF NOT EXISTS ca_drop_work_item_live_idx
    ON public.ca_drop_work_item (id) WHERE revoked_at IS NULL;


-- =====================================================================
-- 2. Grants
--
-- Nothing to add. supabase-1-create.sql already granted UPDATE on this
-- table for Cron B's status writes, and that is what the revocation uses.
-- =====================================================================


-- =====================================================================
-- 3. Verify
-- =====================================================================

-- one row: revoked_at, timestamptz, nullable.
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'ca_drop_work_item'
  AND column_name = 'revoked_at';

-- the partial index.
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'ca_drop_work_item'
  AND indexname = 'ca_drop_work_item_live_idx';

-- Nothing is revoked yet unless a run has already applied removals. Both
-- numbers should match the counts you expect, and live should equal the
-- number of keys in KV.
SELECT count(*)                                    AS total,
       count(*) FILTER (WHERE revoked_at IS NULL)  AS live,
       count(*) FILTER (WHERE revoked_at IS NOT NULL) AS revoked
FROM public.ca_drop_work_item;


-- =====================================================================
-- 4. Afterwards
--
-- Past revocations were never recorded, so any hash removed by an earlier
-- run is still marked live here while being absent from KV. There is no
-- way to reconstruct which those were from this table -- the removals
-- files in R2 are the only record. If /api/kv-health reports missing
-- hashes straight after this migration, that backlog is the likely cause;
-- check the removals rows in ca-drop/raw/ before treating it as drift.
-- =====================================================================
