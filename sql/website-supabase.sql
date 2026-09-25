-- =====================================================================
-- CA DROP — the Worker's access to the WEBSITE's Supabase project
--
-- A different project from sql/supabase.sql: one the website stores its
-- users' search history in. Run it in EACH website project — on DEV,
-- [ClarityCheck] Product *DEV and [ReverseLookup] Product *DEV — with a
-- different password for each.
-- Cron C deletes the search_history rows of a phone or e-mail whose
-- report it erases, so a DROP-listed consumer's identifier does not stay
-- in anyone's report history.
--
-- Run in that project's SQL editor, a section at a time.
-- =====================================================================


-- 1. The role. REPLACE THE PASSWORD, and use the same value in the
--    Hyperdrive config below. Granted on search_history only: it can read
--    and delete rows there and reach nothing else in the project.

CREATE ROLE drop_workflow LOGIN PASSWORD 'PUT_THE_REAL_PASSWORD_HERE' NOINHERIT;

GRANT USAGE ON SCHEMA public TO drop_workflow;
GRANT SELECT, DELETE ON public.search_history TO drop_workflow;


-- 2. A policy, so the role keeps working once RLS is turned on for this
--    table. RLS is OFF on search_history today (the Supabase advisor flags
--    it), and while it is off the policy has no effect.

CREATE POLICY drop_workflow_delete_search_history
  ON public.search_history
  FOR ALL TO drop_workflow
  USING (true);


-- 3. Verify. Expect one row: drop_workflow · search_history · DELETE, SELECT

SELECT grantee, table_name,
       string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privs
FROM information_schema.table_privileges
WHERE table_schema = 'public' AND grantee = 'drop_workflow'
GROUP BY grantee, table_name;


-- 4. Then, outside SQL — one Hyperdrive config per project, with the
--    supabase-ca certificate (npx wrangler cert list):
--
--    npx wrangler hyperdrive create drop-website-cc-db \
--      --connection-string="postgresql://drop_workflow:<PASSWORD>@db.<ref>.supabase.co:5432/postgres" \
--      --sslmode verify-full --ca-certificate-id <supabase-ca id>
--
--    Answer No when Wrangler offers to add the binding, and add it by hand
--    under "hyperdrive" in wrangler.jsonc:
--      { "binding": "WEBSITE_CC_DB", "id": "<returned id>", "localConnectionString": "…" }
--    (WEBSITE_RL_DB for ReverseLookup). Cron C fails every chunk while any
--    website listed in WEBSITE_DBS (worker/db.ts) has no binding.
