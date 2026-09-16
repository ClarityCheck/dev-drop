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
--
-- ─── THE GRAIN, WHICH IS THE WHOLE DESIGN ────────────────────────────
--
--   people          ONE ROW PER ARRAY ELEMENT of one stored row version,
--                   named by element_digest
--   phone / email   one row per (type, normalized_value), with every
--                   provider merged into it
--
-- That is BUSINESS-LOGIC.md §2 written as a GROUP BY. A people report's
-- elements are different people; a phone or e-mail report's elements are
-- providers describing the one person who was searched for. The grouping
-- key below carries the element locator for people and a constant for the
-- other two, so both rules come out of ONE derivation instead of two that
-- can drift apart.
--
-- It is also what lets Cron C obey Rule 2. v3 flattened every element's
-- keys into one row per identifier, so a match could name only the
-- identifier and the only erase available was the whole row — forty John
-- Smiths deleted because one of them registered. A v4 match names the
-- element, and the sweep edits the array instead of deleting the row.
--
-- WHY A DIGEST AND NOT A POSITION. entity_search_results is a
-- ReplacingMergeTree, so several versions of one key coexist until a merge
-- collapses them — with different array lengths and different people at
-- the same index — and this view is refreshed long before the erase runs.
-- A position is only meaningful against the exact array it was read from.
-- The SHA-256 of the element's raw JSON is meaningful against any of them:
-- an element that moved is still the same element, and every stored copy
-- of it goes in one mutation.
--
-- ─── ON A SERVICE THAT ALREADY CARRIES v3 ────────────────────────────
--
-- DEV does. v4 changes the grain, so ALTER TABLE ... MODIFY QUERY cannot
-- get there. Run, as admin:
--
--     DROP VIEW default.ca_drop_combined_search_result;
--
-- then this statement, then
--
--     SYSTEM REFRESH VIEW default.ca_drop_combined_search_result;
--
-- Nothing else needs redoing. ClickHouse records a privilege against the
-- NAME, so section 3's grants survive the drop and re-apply to the new
-- view, and the view holds no state of its own — every row is derived
-- from entity_search_results.
--
-- DO IT BEFORE DEPLOYING THE WORKER. Cron C selects element_digest, which
-- v3 has no column for, so the match fails outright against the old view
-- rather than matching less. That is the direction Rule 3 asks for, but it
-- does mean the sweep does not run until the view is v4.
-- =====================================================================

CREATE MATERIALIZED VIEW default.ca_drop_combined_search_result
REFRESH EVERY 1 YEAR
(
    `type`              LowCardinality(String),
    `normalized_value`  String,
    `provider`          String,                -- people: the stored row's provider; '' otherwise
    `service`           String,                -- people: the stored row's service;  '' otherwise
    `created_at`        DateTime64(3, 'UTC'),  -- people: the ROW VERSION the element belongs to
    `element_digest`    String,                -- people: base64(SHA256(the element's raw JSON))
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
ORDER BY (type, normalized_value, provider, service, created_at, element_digest)
DEFINER = `sql-console:access@claritycheck.com`
SQL SECURITY DEFINER
AS
-- ── L5 ── drop the composites if the REPORT is past the cut.
--          `oversized_records` means "composites dropped", which with the
--          caps in place is the only way they can be missing.
SELECT
    type,
    normalized_value,
    provider,
    service,
    created_at,
    element_digest,
    source_rows,
    record_count,
    toUInt64(report_width > 20000)             AS oversized_records,
    last_seen_at,
    'v4'                                       AS spec_version,
    email_keys,
    phone_keys,
    if(report_width > 20000, [], ndz_keys)     AS ndz_keys,
    if(report_width > 20000, [], namevin_keys) AS namevin_keys
FROM
(
    -- ── L4 ── the cross product, and the total the cut applies to.
    --
    --          The window is what keeps the cut a REPORT-level decision now
    --          that a people report is many rows. For phone and e-mail the
    --          partition is this one row; for people it sums every element,
    --          which is the same arithmetic as the Worker's
    --          groups.reduce(countReportKeys). Composites are then dropped
    --          for the whole report or for none of it, never per element.
    SELECT
        type,
        normalized_value,
        provider,
        service,
        created_at,
        element_digest,
        source_rows,
        record_count,
        last_seen_at,
        email_keys,
        phone_keys,

        sum(width + length(email_keys) + length(phone_keys))
            OVER (PARTITION BY type, normalized_value) AS report_width,

        arrayDistinct(arrayFlatten(arrayMap(f ->
            arrayFlatten(arrayMap(l ->
                arrayFlatten(arrayMap(d ->
                    arrayMap(z -> base64Encode(SHA256(concat(f, l, d, z))), zh),
                dh)),
            lh)),
        fh))) AS ndz_keys,

        arrayDistinct(arrayFlatten(arrayMap(f ->
            arrayFlatten(arrayMap(l ->
                arrayMap(v -> base64Encode(SHA256(concat(f, l, v))), vh),
            lh)),
        fh))) AS namevin_keys
    FROM
    (
        -- ── L3 ── cap the GROUP's values, then hash them. Sorted and sliced
        --          on the normalized values and before hashing, matching
        --          FIELD_CAPS: the Worker sorts values, and sorting hashes
        --          instead would select a different subset.
        SELECT
            type,
            normalized_value,
            provider,
            service,
            created_at,
            element_digest,
            source_rows,
            record_count,
            last_seen_at,
            email_keys,
            phone_keys,

            arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(all_firsts), 1, 10)) AS fh,
            arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(all_lasts),  1, 10)) AS lh,
            arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(all_dobs),   1, 5))  AS dh,
            arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(all_zips),   1, 24)) AS zh,
            arrayMap(x -> base64Encode(SHA256(x)), arraySlice(arraySort(all_vins),   1, 12)) AS vh,

            (toFloat64(length(arraySlice(all_firsts, 1, 10)))
                * length(arraySlice(all_lasts, 1, 10))
                * length(arraySlice(all_dobs,  1, 5))
                * length(arraySlice(all_zips,  1, 24)))
            + (toFloat64(length(arraySlice(all_firsts, 1, 10)))
                * length(arraySlice(all_lasts, 1, 10))
                * length(arraySlice(all_vins,  1, 12)))                                      AS width
        FROM
        (
            -- ── L2 ── THE GROUP BY IS THE RULE.
            --
            --          One group per people element — so one person's name is
            --          never combined with another's date of birth and ZIP —
            --          and one group per phone or e-mail identifier, so the
            --          name from Veriphone joins the date of birth and ZIP
            --          from Pipl and forms the ndz key that a per-provider
            --          grouping would never derive.
            --
            --          Capping happens after this, never before: taking each
            --          provider's own first ten names and merging those is
            --          not the ten the Worker picks.
            SELECT
                type,
                normalized_value,
                if(type = 'people', row_provider,   '')                        AS provider,
                if(type = 'people', row_service,    '')                        AS service,
                if(type = 'people', row_created_at, toDateTime64(0, 3, 'UTC')) AS created_at,
                if(type = 'people', row_digest,     '')                        AS element_digest,

                uniqExact((row_provider, row_service, row_created_at)) AS source_rows,
                sum(toUInt64(has_record))                              AS record_count,
                max(row_created_at)                                    AS last_seen_at,

                -- The exact keys, hashed once per group rather than once per
                -- element. Hashing the union and unioning the hashes give the
                -- same set, and these are never capped.
                arrayDistinct(arrayConcat(
                    if(type = 'email',
                       [base64Encode(SHA256(lowerUTF8(replaceRegexpAll(normalized_value, '\\s', ''))))],
                       []),
                    arrayMap(x -> base64Encode(SHA256(x)),
                             arrayDistinct(arrayFlatten(groupArray(emails))))
                )) AS email_keys,

                arrayDistinct(arrayConcat(
                    if(type = 'phone',
                       [base64Encode(SHA256(right(replaceRegexpAll(normalized_value, '[^0-9]', ''), 10)))],
                       []),
                    arrayMap(x -> base64Encode(SHA256(x)),
                             arrayDistinct(arrayFlatten(groupArray(phones))))
                )) AS phone_keys,

                arrayDistinct(arrayFlatten(groupArray(firsts))) AS all_firsts,
                arrayDistinct(arrayFlatten(groupArray(lasts)))  AS all_lasts,
                arrayDistinct(arrayFlatten(groupArray(dobs)))   AS all_dobs,
                arrayDistinct(arrayFlatten(groupArray(zips)))   AS all_zips,
                arrayDistinct(arrayFlatten(groupArray(vins)))   AS all_vins
            FROM
            (
                -- ── L1 ── normalization, per DROP's rules, and the digest
                --          that names the element. The conformance vectors in
                --          test/drop-report.test.ts pin the normalization;
                --          row_digest is hashed from `r` exactly as it came
                --          out of JSONExtractArrayRaw, which is what
                --          erasePeopleElements re-derives when it filters the
                --          stored array.
                WITH
                    ['α', 'β', 'γ', 'δ', 'ε', 'ζ', 'η', 'θ', 'ι', 'κ', 'λ', 'μ', 'ν', 'ξ', 'ο', 'π', 'ρ', 'σ', 'ς', 'τ', 'υ', 'φ', 'χ', 'ψ', 'ω', 'ά', 'έ', 'ί', 'ή', 'ύ', 'ό', 'ώ', 'ϊ', 'ΐ', 'ϋ', 'ΰ', 'а', 'б', 'в', 'г', 'д', 'е', 'ё', 'ж', 'з', 'и', 'й', 'к', 'л', 'м', 'н', 'о', 'п', 'р', 'с', 'т', 'у', 'ф', 'х', 'ц', 'ч', 'ш', 'щ', 'ъ', 'ы', 'ь', 'э', 'ю', 'я', 'є', 'і', 'ї', 'ґ', 'ў', 'ß', 'æ', 'œ', 'ø', 'ð', 'þ', 'ł', 'đ', 'ħ', 'ŋ', 'ı', 'ĳ', 'ŀ'] AS tr_src,
                    ['a', 'v', 'g', 'd', 'e', 'z', 'i', 'th', 'i', 'k', 'l', 'm', 'n', 'x', 'o', 'p', 'r', 's', 's', 't', 'y', 'f', 'ch', 'ps', 'o', 'a', 'e', 'i', 'i', 'y', 'o', 'o', 'i', 'i', 'y', 'y', 'a', 'b', 'v', 'g', 'd', 'e', 'yo', 'zh', 'z', 'i', 'y', 'k', 'l', 'm', 'n', 'o', 'p', 'r', 's', 't', 'u', 'f', 'kh', 'ts', 'ch', 'sh', 'shch', '', 'y', '', 'e', 'yu', 'ya', 'e', 'i', 'i', 'g', 'u', 'ss', 'ae', 'oe', 'o', 'd', 'th', 'l', 'd', 'h', 'n', 'i', 'ij', 'l'] AS tr_dst,
                    '[^a-z0-9\\p{Han}\\p{Hiragana}\\p{Katakana}\\p{Hangul}\\p{Arabic}\\p{Hebrew}]' AS strip_re
                SELECT
                    type,
                    normalized_value,
                    row_provider,
                    row_service,
                    row_created_at,
                    (r != '')               AS has_record,
                    base64Encode(SHA256(r)) AS row_digest,

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
                    -- ── L0 ── one row per array element for people, one per
                    --          provider row otherwise. The row's own identity
                    --          travels under row_* so that L2 can group by it
                    --          for people and discard it for the other two.
                    SELECT
                        type,
                        normalized_value,
                        provider   AS row_provider,
                        service    AS row_service,
                        created_at AS row_created_at,
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
            GROUP BY
                type,
                normalized_value,
                provider,
                service,
                created_at,
                element_digest
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

-- Do not drop this line because the file is create-only.
--
-- ClickHouse records a privilege against the NAME, not the object, so a
-- grant outlives the table it referenced and re-applies to any future
-- table that reuses the name. On a service where the role already exists
-- — every service this has run on so far — the grants below are added to
-- whatever was there before rather than replacing it. Dev proved it: the
-- role still carried SELECT and INSERT on ca_drop_match_run, a table
-- deleted long ago, and ALTER DELETE on ca_drop_work_items, which no
-- version of this file grants.
--
-- On a genuinely fresh service this is a no-op. That is the point: it
-- costs nothing there and is the only thing that makes section 6's count
-- of 9 true anywhere else.
REVOKE ALL ON *.* FROM drop_workflow_role;

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
--   ALTER UPDATE  the people half of the erase, and Rule 2 cannot be kept
--                 without it. A matched people report loses the matched
--                 array elements and the row survives, so the payload is
--                 rewritten in place rather than deleted. Also a mutation,
--                 so the old payload is not left on disk either.
--   SELECT        the predicate reads the columns, and the workflow counts
--                 what it matched before and after so it can verify the
--                 erase instead of trusting it — rows for a phone or e-mail
--                 report, listed elements for a people one. The incident
--                 endpoint reads payloads back through the same grant.
--
-- entity_search_results holds the raw provider payloads, so SELECT on it
-- is broad — it is the table this whole pipeline exists to protect.
-- Granted because the alternative is a workflow that reports deletions it
-- cannot confirm.
GRANT SELECT, ALTER DELETE, ALTER UPDATE ON default.entity_search_results TO drop_workflow_role;


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

-- Expect EXACTLY these nine rows:
--   SELECT        default   ca_drop_combined_search_result
--   SYSTEM VIEWS  default   ca_drop_combined_search_result
--   SELECT        default   ca_drop_work_items
--   INSERT        default   ca_drop_work_items
--   TRUNCATE      default   ca_drop_work_items
--   ALTER DELETE  default   entity_search_results
--   ALTER UPDATE  default   entity_search_results
--   SELECT        default   entity_search_results
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

-- Expect spec_version v4 on every row once the view has been refreshed.
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
