-- =====================================================================
-- default.ca_drop_combined_search_result  —  spec_version v3
--
-- v3 changes ONE thing and it is a bug fix: how a phone or e-mail report
-- is grouped before the cross product is taken.
--
-- WHAT WAS WRONG IN v2
--
-- v2 derived keys per SOURCE ROW and unioned them. For a people report a
-- source row is one array element, which is right — the elements are
-- different people and combining one person's name with another's ZIP
-- would invent a key for someone who does not exist.
--
-- For a phone or e-mail report a source row is one PROVIDER, and every
-- provider describes the SAME person: the subject of the search. So when
-- Veriphone returns the name and Pipl returns the date of birth and the
-- ZIP, those four factors belong to one consumer and must form one NDZ
-- key. Deriving them per provider produces NO ndz key at all — each
-- provider is missing a factor — and an NDZ-registered consumer is never
-- matched by Cron C. Silently: no error is raised anywhere.
--
-- worker/drop-report.ts reportGroups() has merged the providers for phone
-- and e-mail since the request path was built, so the sweep has been
-- under-matching exactly the registrations the request path catches. This
-- brings the view into line.
--
-- THE CAPS COME WITH IT, AND HAVE TO
--
-- Merging the providers multiplies the factors. Measured on DEV, a single
-- e-mail provider row already reaches 82 first names x 71 last names x
-- 230 birthDates x 140 ZIPs; merged across eleven providers the product
-- is far larger. v2's rule — drop the composites when the width exceeds
-- 20,000 — would then fire on most e-mail values and derive nothing,
-- which is worse than the bug being fixed.
--
-- So the factors are capped exactly as the Worker caps them, in
-- worker/drop-report.ts FIELD_CAPS:
--
--     firstNames 10   lastNames 10   dobs 5   zips 24   vins 12
--
-- Sorted, then sliced, on the NORMALIZED VALUES and before hashing —
-- because the Worker sorts values, and sorting hashes instead would
-- select a different subset and put the two back out of step.
--
-- Capping bounds one group at 10*10*5*24 + 10*10*12 = 13,200 keys, so for
-- a phone or e-mail report the 20,000 cut is now unreachable and the two
-- sides agree exactly. For a people report the groups are summed, so a
-- very wide report can still cross it and the composites are dropped —
-- which is what the Worker does too.
--
-- ORDER OF OPERATIONS, because it is load-bearing:
--
--     people        cap per element, then keys per element, then union
--     phone/email   union the raw fields across providers, THEN cap,
--                   THEN one cross product
--
-- Capping before merging would select each provider's first ten names and
-- then merge, which is not the same ten names as the Worker picks.
-- =====================================================================

DROP VIEW IF EXISTS default.ca_drop_combined_search_result;


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
