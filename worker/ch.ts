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
