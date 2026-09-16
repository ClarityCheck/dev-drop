-- =====================================================================
-- CA DROP — the whole ClickHouse side, from nothing.
--
-- One file, run once, as an admin user, TOP TO BOTTOM. There are no
-- DROPs and no IF NOT EXISTS: every statement creates. If one fails
-- saying something already exists, this is not a fresh service — stop
-- and find out what is there, because the grants further down assume
-- this file built what they point at.
--
-- ─── TWO THINGS TO DO FIRST ──────────────────────────────────────────
--
-- 1. entity_search_results must already exist. The lookup API creates it
--    itself, from src/modules/db/clickhouse/types/entity-search-result-table.ts,
--    so start the API against this service once before running this file.
--    Section 2's view reads that table and section 4 grants on it; both
--    fail if it is not there.
--
-- 2. THE VIEW'S DEFINER, in section 2, names an account that exists in
--    the DEV service and will not exist in a new one. Change it to an
--    admin account on the target service before running.
--
--    It is load-bearing, not decoration. The view is SQL SECURITY
--    DEFINER, so a refresh reads entity_search_results as the definer
--    rather than as drop_workflow — which is why drop_workflow needs no
--    SELECT on that table for the match, only for the erase. If the
--    definer account is ever removed, refreshes break and no grant in
--    this file will fix it.
--
-- ─── WHAT THIS BUILDS ────────────────────────────────────────────────
--
--   ca_drop_combined_search_result  the candidate keys, derived in SQL
--                                   from entity_search_results
--   ca_drop_work_items              the DROP hash set, borrowed from KV
--                                   only while a run is matching
--   drop_workflow_role / _user      the five privileges Cron C needs
--
-- The Supabase side is sql/supabase.sql. The rules these serve are in
-- BUSINESS-LOGIC.md.
--
-- No ON CLUSTER anywhere: access entities in ClickHouse Cloud are stored
-- replicated, so users, roles and grants propagate on their own.
-- =====================================================================


-- =====================================================================
-- 1. ca_drop_work_items — the DROP set, while a run is using it
--
-- The match is a join between the DROP hash set and the candidate keys,
-- and the only question is which side travels. The DROP set is the small
-- one; the candidate keys run to hundreds of thousands. ClickHouse
-- cannot join against KV, so the small side is copied in here and the
-- join happens next to the data.
--
-- The measured difference is not marginal: comparing the keys one at a
-- time from the Worker took about five hours and exceeded the Worker CPU
-- limit, where the same join inside ClickHouse returns in well under a
-- second.
--
-- It is a working copy, not a store. Truncated at the start of every run
-- and again at the end, so the DROP hashes are resident only while a run
-- is using them. KV remains the source of truth.
-- =====================================================================

CREATE TABLE default.ca_drop_work_items
(
    list_type    LowCardinality(String),
    hash         String,                       -- Base64, exactly as DROP published it
    work_item_id String,                       -- DROP's Id, case-sensitive
    request_date Nullable(Date),
    loaded_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (list_type, hash);
-- ReplacingMergeTree because a retried sync page re-inserts rows it
-- already wrote. Dedup is not immediate, so the match query also groups
-- by (list_type, hash) rather than relying on a merge having happened.


-- =====================================================================
-- 2. ca_drop_combined_search_result — the candidate keys
--
-- CHANGE THE DEFINER BELOW before running. See the note at the top.
-- =====================================================================

CREATE MATERIALIZED VIEW default.ca_drop_combined_search_result
REFRESH EVERY 1 YEAR
(
    `type`              LowCardinality(String),
    `normalized_value`  String,
    `source_rows`       UInt64,
    `record_count`      UInt64,
    `oversized_records` UInt64,
    `last_seen_at`      DateTime64(3, 'UTC'),
    `spec_version`      String,
    `email_keys`        Array(String),
    `phone_keys`        Array(String),
    `ndz_keys`          Array(String),
    `namevin_keys`      Array(String)
)
ENGINE = MergeTree
ORDER BY (type, normalized_value)
DEFINER = `sql-console:access@claritycheck.com`
SQL SECURITY DEFINER
AS
-- ── L7 ── pick the path by type, and drop the composites if the total is
--          past the cut. `oversized_records` now means "composites
--          dropped", which with the caps in place is the only way they can
--          be missing.
SELECT
    type,
    normalized_value,
    source_rows,
    record_count,
    toUInt64(total_width > 20000)                        AS oversized_records,
    last_seen_at,
    'v3'                                                 AS spec_version,
    email_keys,
    phone_keys,
    if(total_width > 20000, [],
       if(type = 'people', people_ndz_keys, merged_ndz_keys))         AS ndz_keys,
    if(total_width > 20000, [],
       if(type = 'people', people_namevin_keys, merged_namevin_keys)) AS namevin_keys
FROM
(
    -- ── L6 ── the merged cross product, and the total the cut applies to
    SELECT
        type,
        normalized_value,
        source_rows,
        record_count,
        last_seen_at,
        email_keys,
        phone_keys,
        people_ndz_keys,
        people_namevin_keys,

        if(type = 'people', people_width, merged_width)
            + length(email_keys) + length(phone_keys)    AS total_width,

        arrayDistinct(arrayFlatten(arrayMap(f ->
            arrayFlatten(arrayMap(l ->
                arrayFlatten(arrayMap(d ->
                    arrayMap(z -> base64Encode(SHA256(concat(f, l, d, z))), mzh),
                mdh)),
            mlh)),
        mfh))) AS merged_ndz_keys,

        arrayDistinct(arrayFlatten(arrayMap(f ->
            arrayFlatten(arrayMap(l ->
                arrayMap(v -> base64Encode(SHA256(concat(f, l, v))), mvh),
            mlh)),
        mfh))) AS merged_namevin_keys
    FROM
    (
        -- ── L5 ── cap the MERGED values, then hash them. Cap after the
        --          merge, never before it.
        SELECT
            type,
            normalized_value,
            source_rows,
            record_count,
            last_seen_at,
            email_keys,
            phone_keys,
            people_ndz_keys,
            people_namevin_keys,
            people_width,

            arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(all_firsts), 1, 10)) AS mfh,
            arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(all_lasts),  1, 10)) AS mlh,
            arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(all_dobs),   1, 5))  AS mdh,
            arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(all_zips),   1, 24)) AS mzh,
            arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(all_vins),   1, 12)) AS mvh,

            (toFloat64(length(arraySlice(all_firsts, 1, 10)))
                * length(arraySlice(all_lasts, 1, 10))
                * length(arraySlice(all_dobs,  1, 5))
                * length(arraySlice(all_zips,  1, 24)))
            + (toFloat64(length(arraySlice(all_firsts, 1, 10)))
                * length(arraySlice(all_lasts, 1, 10))
                * length(arraySlice(all_vins,  1, 12)))                                      AS merged_width
        FROM
        (
            -- ── L4 ── one row per (type, normalized_value). The people
            --          keys are already built per element; the raw fields
            --          come up unaggregated for the merged path.
            SELECT
                type,
                normalized_value,
                uniqExact((provider, service, created_at))                AS source_rows,
                sum(rec_present)                                          AS record_count,
                max(created_at)                                           AS last_seen_at,
                arrayDistinct(arrayFlatten(groupArray(rec_email_keys)))   AS email_keys,
                arrayDistinct(arrayFlatten(groupArray(rec_phone_keys)))   AS phone_keys,
                arrayDistinct(arrayFlatten(groupArray(rec_ndz_keys)))     AS people_ndz_keys,
                arrayDistinct(arrayFlatten(groupArray(rec_namevin_keys))) AS people_namevin_keys,
                sum(rec_ndz_width) + sum(rec_namevin_width)               AS people_width,
                arrayDistinct(arrayFlatten(groupArray(firsts)))           AS all_firsts,
                arrayDistinct(arrayFlatten(groupArray(lasts)))            AS all_lasts,
                arrayDistinct(arrayFlatten(groupArray(dobs)))             AS all_dobs,
                arrayDistinct(arrayFlatten(groupArray(zips)))             AS all_zips,
                arrayDistinct(arrayFlatten(groupArray(vins)))             AS all_vins
            FROM
            (
                -- ── L3 ── per element: cap, hash, and build the element's
                --          own keys. Used by the people path; the raw
                --          fields pass straight through for the other one.
                SELECT
                    type,
                    normalized_value,
                    provider,
                    service,
                    created_at,
                    firsts,
                    lasts,
                    dobs,
                    zips,
                    vins,

                    arrayDistinct(arrayConcat(
                        if(type = 'email',
                           [base64Encode(SHA256(lowerUTF8(replaceRegexpAll(normalized_value, '\\s', ''))))],
                           []),
                        arrayMap(x -> base64Encode(SHA256(x)), emails)
                    )) AS rec_email_keys,

                    arrayDistinct(arrayConcat(
                        if(type = 'phone',
                           [base64Encode(SHA256(right(replaceRegexpAll(normalized_value, '[^0-9]', ''), 10)))],
                           []),
                        arrayMap(x -> base64Encode(SHA256(x)), phones)
                    )) AS rec_phone_keys,

                    arrayDistinct(arrayFlatten(arrayMap(f ->
                        arrayFlatten(arrayMap(l ->
                            arrayFlatten(arrayMap(d ->
                                arrayMap(z -> base64Encode(SHA256(concat(f, l, d, z))), czh),
                            cdh)),
                        clh)),
                    cfh))) AS rec_ndz_keys,

                    arrayDistinct(arrayFlatten(arrayMap(f ->
                        arrayFlatten(arrayMap(l ->
                            arrayMap(v -> base64Encode(SHA256(concat(f, l, v))), cvh),
                        clh)),
                    cfh))) AS rec_namevin_keys,

                    toFloat64(length(cfh)) * length(clh) * length(cdh) * length(czh) AS rec_ndz_width,
                    toFloat64(length(cfh)) * length(clh) * length(cvh)               AS rec_namevin_width,
                    toUInt64(has_record)                                            AS rec_present
                FROM
                (
                    -- ── L2 ── the per-element caps, as hashes. Sorted then
                    --          sliced on the VALUES, matching FIELD_CAPS.
                    SELECT
                        type,
                        normalized_value,
                        provider,
                        service,
                        created_at,
                        has_record,
                        emails,
                        phones,
                        firsts,
                        lasts,
                        dobs,
                        zips,
                        vins,
                        arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(firsts), 1, 10)) AS cfh,
                        arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(lasts),  1, 10)) AS clh,
                        arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(dobs),   1, 5))  AS cdh,
                        arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(zips),   1, 24)) AS czh,
                        arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(vins),   1, 12)) AS cvh
                    FROM
                    (
                        -- ── L1 ── normalization, per DROP's rules. Unchanged
                        --          from v2; the conformance vectors in
                        --          test/drop-report.test.ts pin these.
                        WITH
                            ['α', 'β', 'γ', 'δ', 'ε', 'ζ', 'η', 'θ', 'ι', 'κ', 'λ', 'μ', 'ν', 'ξ', 'ο', 'π', 'ρ', 'σ', 'ς', 'τ', 'υ', 'φ', 'χ', 'ψ', 'ω', 'ά', 'έ', 'ί', 'ή', 'ύ', 'ό', 'ώ', 'ϊ', 'ΐ', 'ϋ', 'ΰ', 'а', 'б', 'в', 'г', 'д', 'е', 'ё', 'ж', 'з', 'и', 'й', 'к', 'л', 'м', 'н', 'о', 'п', 'р', 'с', 'т', 'у', 'ф', 'х', 'ц', 'ч', 'ш', 'щ', 'ъ', 'ы', 'ь', 'э', 'ю', 'я', 'є', 'і', 'ї', 'ґ', 'ў', 'ß', 'æ', 'œ', 'ø', 'ð', 'þ', 'ł', 'đ', 'ħ', 'ŋ', 'ı', 'ĳ', 'ŀ'] AS tr_src,
                            ['a', 'v', 'g', 'd', 'e', 'z', 'i', 'th', 'i', 'k', 'l', 'm', 'n', 'x', 'o', 'p', 'r', 's', 's', 't', 'y', 'f', 'ch', 'ps', 'o', 'a', 'e', 'i', 'i', 'y', 'o', 'o', 'i', 'i', 'y', 'y', 'a', 'b', 'v', 'g', 'd', 'e', 'yo', 'zh', 'z', 'i', 'y', 'k', 'l', 'm', 'n', 'o', 'p', 'r', 's', 't', 'u', 'f', 'kh', 'ts', 'ch', 'sh', 'shch', '', 'y', '', 'e', 'yu', 'ya', 'e', 'i', 'i', 'g', 'u', 'ss', 'ae', 'oe', 'o', 'd', 'th', 'l', 'd', 'h', 'n', 'i', 'ij', 'l'] AS tr_dst,
                            '[^a-z0-9\\p{Han}\\p{Hiragana}\\p{Katakana}\\p{Hangul}\\p{Arabic}\\p{Hebrew}]' AS strip_re
                        SELECT
                            type,
                            normalized_value,
                            provider,
                            service,
                            created_at,
                            (r != '') AS has_record,

                            arrayDistinct(arrayFilter(x -> x != '',
                                arrayMap(x -> lowerUTF8(replaceRegexpAll(x, '\\s', '')),
                                    arrayFilter(x -> (x != '') AND (x != 'null'), arrayMap(x -> trim(BOTH '"' FROM x), arrayConcat(
                                        if(JSONType(r, 'contactInfo', 'email') = 'Array', JSONExtractArrayRaw(r, 'contactInfo', 'email'), [JSONExtractRaw(r, 'contactInfo', 'email')]),
                                        if(JSONType(r, 'contactInfo', 'emails') = 'Array', JSONExtractArrayRaw(r, 'contactInfo', 'emails'), [JSONExtractRaw(r, 'contactInfo', 'emails')]),
                                        arrayMap(o -> JSONExtractRaw(o, 'address'), JSONExtractArrayRaw(r, 'emails'))
                                    )))))) AS emails,

                            arrayDistinct(arrayFilter(x -> x != '',
                                arrayMap(x -> right(replaceRegexpAll(x, '[^0-9]', ''), 10),
                                    arrayFilter(x -> (x != '') AND (x != 'null'), arrayMap(x -> trim(BOTH '"' FROM x), arrayConcat(
                                        if(JSONType(r, 'contactInfo', 'phone') = 'Array', JSONExtractArrayRaw(r, 'contactInfo', 'phone'), [JSONExtractRaw(r, 'contactInfo', 'phone')]),
                                        if(JSONType(r, 'contactInfo', 'phones') = 'Array', JSONExtractArrayRaw(r, 'contactInfo', 'phones'), [JSONExtractRaw(r, 'contactInfo', 'phones')]),
                                        arrayMap(o -> JSONExtractRaw(o, 'number'), JSONExtractArrayRaw(r, 'phones'))
                                    )))))) AS phones,

                            arrayDistinct(arrayFilter(x -> x != '',
                                arrayMap(x -> replaceRegexpAll(normalizeUTF8NFD(arrayStringConcat(arrayMap(c -> transform(c, tr_src, tr_dst, c), extractAll(lowerUTF8(x), '.')), '')), strip_re, ''),
                                    arrayFilter(x -> (x != '') AND (x != 'null'), arrayMap(x -> trim(BOTH '"' FROM x), arrayConcat(
                                        if(JSONType(r, 'personalInfo', 'firstName') = 'Array', JSONExtractArrayRaw(r, 'personalInfo', 'firstName'), [JSONExtractRaw(r, 'personalInfo', 'firstName')]),
                                        if(JSONType(r, 'personalInfo', 'firstNames') = 'Array', JSONExtractArrayRaw(r, 'personalInfo', 'firstNames'), [JSONExtractRaw(r, 'personalInfo', 'firstNames')]),
                                        arrayMap(o -> JSONExtractRaw(o, 'first'), JSONExtractArrayRaw(r, 'names'))
                                    )))))) AS firsts,

                            arrayDistinct(arrayFilter(x -> x != '',
                                arrayMap(x -> replaceRegexpAll(normalizeUTF8NFD(arrayStringConcat(arrayMap(c -> transform(c, tr_src, tr_dst, c), extractAll(lowerUTF8(x), '.')), '')), strip_re, ''),
                                    arrayFilter(x -> (x != '') AND (x != 'null'), arrayMap(x -> trim(BOTH '"' FROM x), arrayConcat(
                                        if(JSONType(r, 'personalInfo', 'lastName') = 'Array', JSONExtractArrayRaw(r, 'personalInfo', 'lastName'), [JSONExtractRaw(r, 'personalInfo', 'lastName')]),
                                        if(JSONType(r, 'personalInfo', 'lastNames') = 'Array', JSONExtractArrayRaw(r, 'personalInfo', 'lastNames'), [JSONExtractRaw(r, 'personalInfo', 'lastNames')]),
                                        arrayMap(o -> JSONExtractRaw(o, 'last'), JSONExtractArrayRaw(r, 'names'))
                                    )))))) AS lasts,

                            arrayDistinct(arrayFilter(x -> length(x) = 8,
                                arrayMap(x -> if(match(x, '^(19|20)\\d{2}'), substring(replaceRegexpAll(x, '[^0-9]', ''), 1, 8), ''),
                                    arrayFilter(x -> (x != '') AND (x != 'null'), arrayMap(x -> trim(BOTH '"' FROM x), arrayConcat(
                                        if(JSONType(r, 'personalInfo', 'birthDate') = 'Array', JSONExtractArrayRaw(r, 'personalInfo', 'birthDate'), [JSONExtractRaw(r, 'personalInfo', 'birthDate')]),
                                        if(JSONType(r, 'personalInfo', 'birthDates') = 'Array', JSONExtractArrayRaw(r, 'personalInfo', 'birthDates'), [JSONExtractRaw(r, 'personalInfo', 'birthDates')]),
                                        [JSONExtractRaw(r, 'dateOfBirth', 'start')]
                                    )))))) AS dobs,

                            arrayDistinct(arrayFilter(x -> x != '',
                                arrayMap(x -> substring(replaceRegexpOne(replaceRegexpAll(lowerUTF8(splitByChar('-', x)[1]), '[^a-z0-9]', ''), '^0+', ''), 1, 5),
                                    arrayFilter(x -> (x != '') AND (x != 'null'), arrayMap(x -> trim(BOTH '"' FROM x), arrayConcat(
                                        arrayMap(o -> JSONExtractRaw(o, 'zip'),     JSONExtractArrayRaw(r, 'contactInfo', 'fullAddresses')),
                                        arrayMap(o -> JSONExtractRaw(o, 'zipCode'), JSONExtractArrayRaw(r, 'contactInfo', 'fullAddresses')),
                                        arrayMap(o -> JSONExtractRaw(o, 'zip'),     JSONExtractArrayRaw(r, 'addresses')),
                                        arrayMap(o -> JSONExtractRaw(o, 'zipCode'), JSONExtractArrayRaw(r, 'addresses')),
                                        [JSONExtractRaw(r, 'contactInfo', 'zip')]
                                    )))))) AS zips,

                            arrayDistinct(arrayFilter(x -> x != '',
                                arrayMap(x -> replaceRegexpAll(lowerUTF8(x), '[^a-z0-9]', ''),
                                    arrayFilter(x -> (x != '') AND (x != 'null'), arrayMap(x -> trim(BOTH '"' FROM x),
                                        arrayMap(o -> JSONExtractRaw(o, 'vin'), JSONExtractArrayRaw(r, 'vehicles'))
                                    ))))) AS vins
                        FROM
                        (
                            -- ── L0 ── one row per array element for people,
                            --          one per provider row otherwise.
                            SELECT
                                type,
                                normalized_value,
                                provider,
                                service,
                                created_at,
                                arrayJoin(if(empty(recs), [''], recs)) AS r
                            FROM
                            (
                                SELECT
                                    type,
                                    normalized_value,
                                    provider,
                                    service,
                                    created_at,
                                    if(JSONType(payload_json) = 'Array', JSONExtractArrayRaw(payload_json), [payload_json]) AS recs
                                FROM default.entity_search_results
                                WHERE type IN ('email', 'phone', 'people')
                            )
                        )
                    )
                )
            )
            GROUP BY
                type,
                normalized_value
        )
    )
);

-- =====================================================================
-- 3. The role
--
-- Cron C touches exactly five things. Anything granted beyond them is
-- surface area with no user.
-- =====================================================================

CREATE ROLE drop_workflow_role;

-- The candidate keys.
GRANT SELECT ON default.ca_drop_combined_search_result TO drop_workflow_role;

-- Step ① runs  SYSTEM REFRESH VIEW default.ca_drop_combined_search_result.
-- SELECT does not cover it, and without this line the first step of every
-- run fails with ACCESS_DENIED.
GRANT SYSTEM VIEWS ON default.ca_drop_combined_search_result TO drop_workflow_role;

-- REQUIRED, and confirmed the hard way. Having asked for a refresh, the
-- workflow waits for it by polling here. Reads of system tables are
-- implicit and row-filtered for many tables, which is why this looks
-- optional. It is not:
--     Code: 497. drop_workflow: Not enough privileges. To execute this
--     query, it's necessary to have the grant SELECT ON system.view_refreshes
GRANT SELECT ON system.view_refreshes TO drop_workflow_role;

-- TRUNCATE is what lets the workflow clear the helper table itself, at
-- the start of a run and again when it finishes.
GRANT SELECT, INSERT, TRUNCATE ON default.ca_drop_work_items TO drop_workflow_role;

-- The erase, and the one real widening of the role.
--
--   ALTER DELETE  runs ALTER TABLE ... DELETE, a real mutation: the parts
--                 are rewritten without the rows. NOT the lightweight
--                 DELETE FROM, which only marks rows and leaves the data
--                 on disk until some later merge — not good enough for a
--                 statutory deletion.
--   SELECT        the predicate reads the columns, and the workflow counts
--                 the matching rows before and after so it can verify the
--                 delete instead of trusting it. The incident endpoint
--                 reads payloads back through the same grant.
--
-- entity_search_results holds the raw provider payloads, so SELECT on it
-- is broad — it is the table this whole pipeline exists to protect.
-- Granted because the alternative is a workflow that reports deletions it
-- cannot confirm.
GRANT SELECT, ALTER DELETE ON default.entity_search_results TO drop_workflow_role;


-- =====================================================================
-- 4. The user
--
-- REPLACE THE PASSWORD, then put the same value into the Worker:
--     npx wrangler secret put CH_PASSWORD
-- =====================================================================

CREATE USER drop_workflow
  IDENTIFIED WITH sha256_password BY 'PUT_THE_REAL_PASSWORD_HERE';

GRANT drop_workflow_role TO drop_workflow;
ALTER USER drop_workflow DEFAULT ROLE drop_workflow_role;

-- Everything reaches the user through the role and nothing directly.
-- This revokes privileges only; role membership is separate and survives.
REVOKE ALL ON *.* FROM drop_workflow;


-- =====================================================================
-- 5. Settings
--
-- ALTER USER ... SETTINGS REPLACES the whole list, so all four go in one
-- statement or the omitted ones are dropped.
--
-- readonly = 0 is already the default and changes nothing — kept as
-- documentation, because it is not obviously the default and SYSTEM
-- REFRESH VIEW is blocked under readonly 1 or 2.
--
-- max_result_bytes is the cap that bites, not max_result_rows: a match
-- row carries a whole key array, which makes rows a poor proxy for size.
-- 64 MiB will not trip in normal use, and it turns "matchChunk was raised
-- to something absurd" into a clean ClickHouse error rather than a Worker
-- that runs out of memory part-way through a chunk.
-- =====================================================================

ALTER USER drop_workflow SETTINGS
  max_execution_time = 900,
  max_result_rows    = 200000,
  max_result_bytes   = 67108864,
  readonly           = 0;


-- =====================================================================
-- 6. Verify — run as admin
-- =====================================================================

-- Expect EXACTLY these eight rows:
--   SELECT        default   ca_drop_combined_search_result
--   SYSTEM VIEWS  default   ca_drop_combined_search_result
--   SELECT        default   ca_drop_work_items
--   INSERT        default   ca_drop_work_items
--   TRUNCATE      default   ca_drop_work_items
--   SELECT        default   entity_search_results
--   ALTER DELETE  default   entity_search_results
--   SELECT        system    view_refreshes
SELECT access_type, database, table
FROM system.grants
WHERE role_name = 'drop_workflow_role'
ORDER BY database, table, access_type;

-- Expect: default_roles_list = ['drop_workflow_role'], default_roles_all = 0
SELECT name, auth_type, default_roles_all, default_roles_list
FROM system.users
WHERE name = 'drop_workflow';

-- Expect the four settings from section 5.
SELECT setting_name, value
FROM system.settings_profile_elements
WHERE user_name = 'drop_workflow'
ORDER BY setting_name;

-- Expect no rows: the user must hold no privileges of its own.
SELECT access_type, database, table
FROM system.grants
WHERE user_name = 'drop_workflow';

-- Expect spec_version v3 on every row once the view has been refreshed.
-- It is empty until then; REFRESH is the next step.
SELECT spec_version, count() AS rows
FROM default.ca_drop_combined_search_result
GROUP BY spec_version;


-- =====================================================================
-- 7. Then, in order
--
--   1. Put the password from section 4 into  npx wrangler secret put CH_PASSWORD
--   2. SYSTEM REFRESH VIEW default.ca_drop_combined_search_result;
--      The view is REFRESH EVERY 1 YEAR, so it is empty until asked. It
--      is also why Cron C polls last_success_time rather than the status:
--      'Scheduled' is this view's resting state as well as its finished
--      state, so a status check can read a queued refresh as a done one
--      and match against the previous contents.
--   3. Run sql/supabase.sql if it has not been run yet.
--   4. POST /api/downloader/start     Cron A
--   5. POST /api/workflow/start       Cron C
-- =====================================================================
