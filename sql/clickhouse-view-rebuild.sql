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
SELECT
    type,
    normalized_value,
    uniqExact((provider, service, created_at))                AS source_rows,
    sum(rec_present)                                          AS record_count,
    sum(rec_oversized)                                        AS oversized_records,
    max(created_at)                                           AS last_seen_at,
    'v2'                                                      AS spec_version,
    arrayDistinct(arrayFlatten(groupArray(rec_email_keys)))   AS email_keys,
    arrayDistinct(arrayFlatten(groupArray(rec_phone_keys)))   AS phone_keys,
    arrayDistinct(arrayFlatten(groupArray(rec_ndz_keys)))     AS ndz_keys,
    arrayDistinct(arrayFlatten(groupArray(rec_namevin_keys))) AS namevin_keys
FROM
(
    SELECT
        type,
        normalized_value,
        provider,
        service,
        created_at,

        arrayDistinct(arrayConcat(
            if(type = 'email',
               [base64Encode(SHA256(lowerUTF8(replaceRegexpAll(normalized_value, '\\s', ''))))],
               []),
            eh
        )) AS rec_email_keys,

        arrayDistinct(arrayConcat(
            if(type = 'phone',
               [base64Encode(SHA256(right(replaceRegexpAll(normalized_value, '[^0-9]', ''), 10)))],
               []),
            ph
        )) AS rec_phone_keys,

        if(ndz_width > 20000, [], arrayDistinct(arrayFlatten(arrayMap(f ->
            arrayFlatten(arrayMap(l ->
                arrayFlatten(arrayMap(d ->
                    arrayMap(z -> base64Encode(SHA256(concat(f, l, d, z))), zh),
                dh)),
            lh)),
        fh)))) AS rec_ndz_keys,

        if(namevin_width > 20000, [], arrayDistinct(arrayFlatten(arrayMap(f ->
            arrayFlatten(arrayMap(l ->
                arrayMap(v -> base64Encode(SHA256(concat(f, l, v))), vh),
            lh)),
        fh)))) AS rec_namevin_keys,

        toUInt64((ndz_width > 20000) OR (namevin_width > 20000)) AS rec_oversized,
        toUInt64(has_record)                                     AS rec_present
    FROM
    (
        SELECT
            type,
            normalized_value,
            provider,
            service,
            created_at,
            has_record,
            arrayMap(x -> base64Encode(SHA256(x)), emails) AS eh,
            arrayMap(x -> base64Encode(SHA256(x)), phones) AS ph,
            arrayMap(x -> base64Encode(SHA256(x)), firsts) AS fh,
            arrayMap(x -> base64Encode(SHA256(x)), lasts)  AS lh,
            arrayMap(x -> base64Encode(SHA256(x)), dobs)   AS dh,
            arrayMap(x -> base64Encode(SHA256(x)), zips)   AS zh,
            arrayMap(x -> base64Encode(SHA256(x)), vins)   AS vh,
            toFloat64(length(firsts)) * length(lasts) * length(dobs) * length(zips) AS ndz_width,
            toFloat64(length(firsts)) * length(lasts) * length(vins)                AS namevin_width
        FROM
        (
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
    normalized_value;
