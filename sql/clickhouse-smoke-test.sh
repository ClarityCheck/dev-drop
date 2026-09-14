#!/usr/bin/env bash
#
# Proves the grants in clickhouse-cron-c.sql from the outside, as the Worker
# sees them — as drop_workflow over HTTPS, not as admin in the SQL console.
# An admin session can read system.grants and still be wrong about what the
# role can actually do.
#
# Every request here is shaped exactly like worker/ch.ts sends it:
# default_format=JSONEachRow, params as param_<name> in the query string, SQL
# as the POST body.
#
#   export CH_PASSWORD='...'          # same value as the wrangler secret
#   ./sql/clickhouse-smoke-test.sh
#
set -uo pipefail

CH_URL="${CH_URL:-https://cu7iy7dd3r.eu-central-1.aws.clickhouse.cloud:8443}"
CH_USER="${CH_USER:-drop_workflow}"
: "${CH_PASSWORD:?set CH_PASSWORD first — the same value as the wrangler secret}"

pass=0
fail=0

# ch <label> <query-string> [sql-on-stdin]
ch() {
	local label="$1" qs="${2:-}"
	local body out code
	body=$(cat)
	out=$(curl -sS -w $'\n%{http_code}' "${CH_URL}/?${qs}" \
		-H "X-ClickHouse-User: ${CH_USER}" \
		-H "X-ClickHouse-Key: ${CH_PASSWORD}" \
		-H "Content-Type: text/plain; charset=utf-8" \
		--data-binary "${body}")
	code="${out##*$'\n'}"
	out="${out%$'\n'*}"

	if [ "$code" = "200" ]; then
		pass=$((pass + 1))
		printf '  ok   %s\n' "$label"
		[ -n "$out" ] && printf '       %s\n' "$(printf '%s' "$out" | head -c 300)"
	else
		fail=$((fail + 1))
		printf '  FAIL %s  (HTTP %s)\n' "$label" "$code"
		printf '       %s\n' "$(printf '%s' "$out" | head -c 300)"
	fi
	return 0
}

echo "ClickHouse smoke test — ${CH_USER} @ ${CH_URL}"
echo

echo "1. SELECT on the view"
ch "read ca_drop_combined_search_result" "default_format=JSONEachRow" <<'SQL'
SELECT count() AS rows FROM default.ca_drop_combined_search_result
SQL

echo
echo "2. SELECT on system.view_refreshes"
ch "read system.view_refreshes" "default_format=JSONEachRow" <<'SQL'
SELECT status, ifNull(toUnixTimestamp(last_success_time), 0) AS last_success
FROM system.view_refreshes WHERE view = 'ca_drop_combined_search_result'
SQL

echo
echo "3. SYSTEM VIEWS — the grant the original script was missing"
before=$(curl -sS "${CH_URL}/?default_format=TSV" \
	-H "X-ClickHouse-User: ${CH_USER}" -H "X-ClickHouse-Key: ${CH_PASSWORD}" \
	--data-binary "SELECT ifNull(toUnixTimestamp(last_success_time),0) FROM system.view_refreshes WHERE view='ca_drop_combined_search_result'" \
	| tr -d '[:space:]')
ch "SYSTEM REFRESH VIEW" "" <<'SQL'
SYSTEM REFRESH VIEW default.ca_drop_combined_search_result
SQL

echo
echo "4. The refresh actually lands (last_success_time moves past ${before:-?})"
echo "   Waiting on the timestamp, not on status: 'Scheduled' is this view's"
echo "   resting state as well as its finished state, so status alone reports"
echo "   success before the refresh has even started."
landed=0
for _ in $(seq 1 60); do
	sleep 5
	now=$(curl -sS "${CH_URL}/?default_format=TSV" \
		-H "X-ClickHouse-User: ${CH_USER}" -H "X-ClickHouse-Key: ${CH_PASSWORD}" \
		--data-binary "SELECT ifNull(toUnixTimestamp(last_success_time),0) FROM system.view_refreshes WHERE view='ca_drop_combined_search_result'" \
		| tr -d '[:space:]')
	if [ -n "$now" ] && [ -n "${before:-}" ] && [ "$now" -gt "$before" ] 2>/dev/null; then
		landed=1
		pass=$((pass + 1))
		echo "  ok   refresh completed (${before} -> ${now})"
		break
	fi
done
[ "$landed" = "1" ] || { fail=$((fail + 1)); echo "  FAIL refresh did not complete within 5 minutes"; }

echo
echo "5. The batch query, with the same parameters the Workflow binds"
ch "batch 1 (budget 50)" \
	"default_format=JSONEachRow&param_cur_type=&param_cur_value=&param_cur_offset=0&param_budget=50&param_row_scan=20" <<'SQL'
WITH
src AS (
    SELECT
        type,
        normalized_value,
        toInt64(length(email_keys) + length(phone_keys)
              + length(ndz_keys)   + length(namevin_keys)) AS total_keys,
        arrayConcat(
            arrayMap(h -> ('email', h),   email_keys),
            arrayMap(h -> ('phone', h),   phone_keys),
            arrayMap(h -> ('ndz', h),     ndz_keys),
            arrayMap(h -> ('namevin', h), namevin_keys)
        ) AS all_keys
    FROM default.ca_drop_combined_search_result
    WHERE {cur_type:String} = ''
       OR (type, normalized_value) >= ({cur_type:String}, {cur_value:String})
    ORDER BY type, normalized_value
    LIMIT {row_scan:UInt64}
),
adj AS (
    SELECT
        type, normalized_value, total_keys,
        toInt64(if(type = {cur_type:String} AND normalized_value = {cur_value:String},
                   {cur_offset:UInt64}, 0)) AS base,
        arraySlice(
            all_keys,
            if(type = {cur_type:String} AND normalized_value = {cur_value:String},
               toInt64({cur_offset:UInt64}) + 1, 1),
            {budget:Int64}
        ) AS keys
    FROM src
),
cum AS (
    SELECT
        type, normalized_value, total_keys, base, keys,
        toInt64(length(keys)) AS n,
        toInt64(sum(length(keys)) OVER (ORDER BY type, normalized_value
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS running
    FROM adj
)
SELECT
    count() AS rows_returned,
    sum(length(arraySlice(keys, 1,
        if(running <= {budget:Int64}, n, {budget:Int64} - (running - n))))) AS keys_emitted
FROM cum
WHERE (running - n) < {budget:Int64}
SQL
echo "   keys_emitted should be exactly 50 — the budget, filled across rows."

echo
echo "6. Nothing beyond the three grants (each of these SHOULD fail)"
ch_expect_denied() {
	local label="$1" body code out
	body=$(cat)
	out=$(curl -sS -w $'\n%{http_code}' "${CH_URL}/" \
		-H "X-ClickHouse-User: ${CH_USER}" -H "X-ClickHouse-Key: ${CH_PASSWORD}" \
		--data-binary "${body}")
	code="${out##*$'\n'}"
	if [ "$code" = "200" ]; then
		fail=$((fail + 1))
		printf '  FAIL %s was ALLOWED — the role is wider than intended\n' "$label"
	else
		pass=$((pass + 1))
		printf '  ok   %s denied\n' "$label"
	fi
	return 0
}
ch_expect_denied "read entity_search_results" <<'SQL'
SELECT count() FROM default.entity_search_results
SQL
ch_expect_denied "write to the view" <<'SQL'
INSERT INTO default.ca_drop_combined_search_result (type, normalized_value) VALUES ('x','y')
SQL

echo
echo "── ${pass} passed, ${fail} failed ──"
[ "$fail" -eq 0 ] || exit 1
