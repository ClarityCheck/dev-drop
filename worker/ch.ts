/**
 * ClickHouse access for the Worker / Workflow.
 *
 * There is no binding and no connection object for ClickHouse — Workers are
 * request-scoped and have nowhere to hold a socket. Every query is an
 * independent HTTPS request to the :8443 interface.
 *
 * Credentials come from vars/secrets:  CH_URL, CH_USER, CH_PASSWORD
 */

/** SELECT. Returns parsed rows (JSONEachRow, one JSON object per line). */
export async function chQuery<T = Record<string, unknown>>(
	env: Env,
	sql: string,
	params: Record<string, string | number> = {},
): Promise<T[]> {
	const url = new URL(env.CH_URL);
	url.searchParams.set("default_format", "JSONEachRow");

	// Server-side parameters: {name:Type} in the SQL, param_name in the query
	// string. Always use these instead of string interpolation — Base64 hashes
	// contain + / = and will eventually break a hand-built statement.
	for (const [k, v] of Object.entries(params)) {
		url.searchParams.set(`param_${k}`, String(v));
	}

	const res = await fetch(url, {
		method: "POST",
		headers: {
			"X-ClickHouse-User": env.CH_USER,
			"X-ClickHouse-Key": env.CH_PASSWORD,
			"Content-Type": "text/plain; charset=utf-8",
		},
		body: sql,
	});

	const text = await res.text();
	if (!res.ok) throw new Error(`ClickHouse ${res.status}: ${text.slice(0, 400)}`);
	if (!text.trim()) return [];
	return text.trim().split("\n").map((line) => JSON.parse(line) as T);
}

/** INSERT. Rows as plain objects; keys must match the column names. */
export async function chInsert(env: Env, table: string, rows: object[]): Promise<number> {
	if (rows.length === 0) return 0;

	const body =
		`INSERT INTO ${table} FORMAT JSONEachRow\n` +
		rows.map((r) => JSON.stringify(r)).join("\n");

	const res = await fetch(env.CH_URL, {
		method: "POST",
		headers: {
			"X-ClickHouse-User": env.CH_USER,
			"X-ClickHouse-Key": env.CH_PASSWORD,
			"Content-Type": "text/plain; charset=utf-8",
		},
		body,
	});

	if (!res.ok) {
		throw new Error(`ClickHouse insert ${res.status}: ${(await res.text()).slice(0, 400)}`);
	}
	return rows.length;
}

/**
 * Base64 -> base64url. The hash is stored in ClickHouse in standard Base64,
 * but it appears in the KV key name as base64url, because + and / are awkward
 * in key names, logs and URLs.
 */
/** base64url (the KV key name) -> standard Base64 (what ClickHouse stores). */
export function fromB64Url(b64url: string): string {
	const b64 = b64url.replace(/-/g, "+").replace(/_/g, "/");
	return b64 + "=".repeat((4 - (b64.length % 4)) % 4);
}

export function toB64Url(b64: string): string {
	return b64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** One KV membership check. Returns the work_item_id, or null if not listed. */
export async function isListed(
	env: Env,
	listType: string,
	hashB64: string,
): Promise<string | null> {
	return env.kv.get(`drop:${listType}:${toB64Url(hashB64)}`);
}

/**
 * Smoke test. Walks the connection one step at a time so a failure points at
 * the exact cause instead of a generic error. Run this before anything else.
 */
export async function chSmokeTest(env: Env): Promise<Response> {
	const out: Record<string, unknown> = {};
	try {
		out.step1_reachable = (await chQuery<{ x: number }>(env, "SELECT 1 AS x"))[0];

		out.step2_identity = (
			await chQuery(env, "SELECT currentUser() AS user, version() AS version, currentDatabase() AS db")
		)[0];

		out.step3_read_combined_view = (
			await chQuery(
				env,
				`SELECT count() AS identifiers,
				        sum(length(email_keys))   AS email_keys,
				        sum(length(phone_keys))   AS phone_keys,
				        sum(length(ndz_keys))     AS ndz_keys,
				        sum(length(namevin_keys)) AS namevin_keys
				 FROM default.ca_drop_combined_search_result`,
			)
		)[0];

		out.step4_read_run_table = (
			await chQuery(env, "SELECT count() AS rows FROM default.ca_drop_match_run")
		)[0];

		// Proves INSERT works. Clean up afterwards with:
		//   ALTER TABLE default.ca_drop_match_run DROP PARTITION 'smoke-test'
		out.step5_write = await chInsert(env, "default.ca_drop_match_run", [
			{
				run_id: "smoke-test",
				list_type: "phone",
				work_item_id: "SMOKETEST000",
				hash: "rkAezKLuIw+Iea+CUbE06yHm0twk0e13KEdY4ptU+6M=",
				type: "phone",
				normalized_value: "smoke-test",
			},
		]);

		out.step6_kv_reachable = {
			probe: "drop:phone:rkAezKLuIw-Iea-CUbE06yHm0twk0e13KEdY4ptU-6M",
			value: await env.kv.get("drop:phone:rkAezKLuIw-Iea-CUbE06yHm0twk0e13KEdY4ptU-6M"),
		};

		out.ok = true;
	} catch (e) {
		out.ok = false;
		out.error = String(e);
	}
	return Response.json(out, { status: out.ok ? 200 : 500 });
}
