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

/** One identifier as the combined view keys it. */
export type EntityKey = { type: string; normalized_value: string };

/**
 * The predicate, shared by the count and the delete so they cannot drift.
 *
 * The pairs travel as ONE String parameter holding JSON, not as an
 * Array(String) and not interpolated. normalized_value is arbitrary consumer
 * input — an apostrophe, a quote or a backslash in an e-mail address would
 * break a hand-built IN list, and getting that wrong here deletes the wrong
 * rows rather than merely erroring.
 */
const ENTITY_MATCH = `(type, normalized_value) IN (
    SELECT tuple(JSONExtractString(p, 1), JSONExtractString(p, 2))
    FROM (SELECT arrayJoin(JSONExtractArrayRaw({pairs:String})) AS p)
)`;

/**
 * Erase every entity_search_results row for the matched identifiers.
 *
 * ALTER TABLE ... DELETE, not the lightweight DELETE FROM. The lightweight
 * form marks rows with the virtual _row_exists column and leaves the data on
 * disk until a merge happens to rewrite the part; this is a mutation, so the
 * parts are rewritten without the rows in them. For a statutory deletion
 * request that difference is the whole point.
 *
 * mutations_sync = 2 waits for the mutation to finish on every replica, so
 * this returns only once the data is actually gone — and the caller can check
 * `after` rather than take the ALTER's word for it.
 */
/**
 * How many entity_search_results rows these identifiers still have.
 *
 * Exported because /api/drop/erase-incident has to verify a deletion it did
 * not perform: the caller says it erased the rows, and "deleted" is a
 * statement to a regulator, so it is checked rather than believed.
 */
export async function countEntityRows(env: Env, keys: EntityKey[]): Promise<number> {
	if (keys.length === 0) return 0;
	const [r] = await chQuery<{ n: string }>(
		env,
		`SELECT count() AS n FROM default.entity_search_results WHERE ${ENTITY_MATCH}`,
		{ pairs: JSON.stringify(keys.map((k) => [k.type, k.normalized_value])) },
	);
	return Number(r?.n ?? 0);
}

export type EntityPayload = { provider: string; service: string; payload_json: string };

/**
 * The stored payloads for these identifiers.
 *
 * No FINAL. entity_search_results is a ReplacingMergeTree, so an older version
 * of a row can still be on disk and still readable until a merge collapses it.
 * A verifier that asked for FINAL would be told about the version it wants to
 * see rather than every version that exists — and an old copy still holding a
 * DROP-listed person is exactly what has to fail the check.
 */
export async function fetchEntityPayloads(
	env: Env,
	keys: EntityKey[],
	limit = 100,
): Promise<EntityPayload[]> {
	if (keys.length === 0) return [];
	return chQuery<EntityPayload>(
		env,
		`SELECT provider, service, payload_json
		 FROM default.entity_search_results
		 WHERE ${ENTITY_MATCH}
		 LIMIT {limit:UInt32}`,
		{ pairs: JSON.stringify(keys.map((k) => [k.type, k.normalized_value])), limit },
	);
}

export async function deleteEntityRows(
	env: Env,
	keys: EntityKey[],
): Promise<{ before: number; after: number }> {
	if (keys.length === 0) return { before: 0, after: 0 };

	const pairs = JSON.stringify(keys.map((k) => [k.type, k.normalized_value]));
	const count = () => countEntityRows(env, keys);

	const before = await count();
	if (before === 0) return { before: 0, after: 0 };

	await chQuery(
		env,
		`ALTER TABLE default.entity_search_results
		 DELETE WHERE ${ENTITY_MATCH}
		 SETTINGS mutations_sync = 2`,
		{ pairs },
	);

	return { before, after: await count() };
}
