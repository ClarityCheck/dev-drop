-- =====================================================================
-- Cron B — bring an EXISTING database up to sql/supabase.sql
--
-- sql/supabase.sql is create-only and already includes these columns. This
-- file is for a database built before Cron B existed. Run it once, as the
-- table owner, section by section in the Supabase SQL editor.
-- =====================================================================

ALTER TABLE public.ca_drop_work_item
    ADD COLUMN source_file     text,
    ADD COLUMN reported_status smallint CHECK (reported_status IN (2, 3, 4, 5)),
    ADD COLUMN reported_at     timestamptz;

CREATE INDEX ca_drop_work_item_report_idx
    ON public.ca_drop_work_item (source_file, id) WHERE revoked_at IS NULL;

-- drop_workflow already holds table-level SELECT, INSERT, UPDATE on
-- ca_drop_work_item, which covers the new columns. No grant change.

-- Rows loaded before this migration have source_file NULL, and Cron B
-- refuses to report them because it cannot name their response file.
-- Backfill by re-running Cron A on each archived ZIP:
--
--   POST /api/downloader/start   { "r2Key": "ca-drop/raw/<file>.zip" }
--   Authorization: Bearer <DROP_OPERATOR_TOKEN>
--
-- The upsert only fills source_file where it is NULL, so this changes
-- nothing else. Then check:

SELECT count(*) FILTER (WHERE source_file IS NULL) AS missing_source_file,
       count(*)                                    AS live_items
FROM public.ca_drop_work_item
WHERE revoked_at IS NULL;
