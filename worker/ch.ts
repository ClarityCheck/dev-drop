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

/**
 * Erase the website's AI enrichments for the identifiers whose reports were
 * erased.
 *
 * default.ai_enrichment_info holds generated text about a searched phone or
 * e-mail, keyed by the same (type, normalized_value) as entity_search_results —
 * the website normalizes it the way the lookup API does. It is derived from
 * the report, so it goes with it: every user's row, every purpose. Counted
 * before and after, like the report rows, so a survivor fails the chunk.
 *
 * Phone and e-mail only, for the same reason as the whole-row erase: there is
 * no people enrichment, and a people report is not erased whole.
 */
export async function deleteEnrichmentRows(
	env: Env,
	keys: EntityKey[],
): Promise<{ before: number; after: number }> {
	const scoped = keys.filter((k) => k.type === "phone" || k.type === "email");
	if (scoped.length === 0) return { before: 0, after: 0 };

	const pairs = JSON.stringify(scoped.map((k) => [k.type, k.normalized_value]));
	const count = async () => {
		const [r] = await chQuery<{ n: string }>(
			env,
			`SELECT count() AS n FROM default.ai_enrichment_info WHERE ${ENTITY_MATCH}`,
			{ pairs },
		);
		return Number(r?.n ?? 0);
	};

	const before = await count();
	if (before === 0) return { before: 0, after: 0 };

	await chQuery(
		env,
		`ALTER TABLE default.ai_enrichment_info
		 DELETE WHERE ${ENTITY_MATCH}
		 SETTINGS mutations_sync = 2`,
		{ pairs },
	);

	return { before, after: await count() };
}

/**
 * One element of a stored people payload, as the combined view names it.
 *
 * The digest is base64(SHA256(the element's raw JSON)), taken from the same
 * JSONExtractArrayRaw output the erase re-derives it from, and NOT a position.
 * entity_search_results is a ReplacingMergeTree, so several versions of one key
 * coexist with different array lengths and different people at the same index,
 * and the view is refreshed long before the sweep reaches the erase. A position
 * is only meaningful against the array it was read from; the digest is
 * meaningful against every stored copy of that element.
 */
export type ElementKey = { normalized_value: string; element_digest: string };

/**
 * The people predicate, shared by the count and the update so they cannot drift.
 *
 * Two parameters, both JSON in a single String for the reason ENTITY_MATCH
 * gives: a normalized_value is arbitrary consumer input and a hand-built IN
 * list would eventually erase the wrong people rather than merely error.
 *
 * The identifiers and the digests travel as two sets rather than as pairs, so
 * one mutation covers a whole chunk. The consequence is deliberate: a listed
 * element stored under a SECOND matched identifier in the same chunk goes too.
 * Byte-identical JSON is the same record about the same consumer, and it is a
 * consumer who asked to be deleted, so removing it there as well is the rule
 * being applied, not an over-reach.
 */
const PEOPLE_VALUE_MATCH = `type = 'people' AND normalized_value IN (
    SELECT arrayJoin(JSONExtract({values:String}, 'Array(String)'))
)`;

/** The stored array, read exactly as the view's L0 reads it. */
const STORED_ELEMENTS = `if(JSONType(payload_json) = 'Array',
    JSONExtractArrayRaw(payload_json), [payload_json])`;

const LISTED_ELEMENTS = `arrayFilter(
    e -> has(JSONExtract({digests:String}, 'Array(String)'), base64Encode(SHA256(e))),
    ${STORED_ELEMENTS}
)`;

function peopleParams(keys: ElementKey[]): Record<string, string> {
	return {
		values: JSON.stringify([...new Set(keys.map((k) => k.normalized_value))]),
		digests: JSON.stringify([...new Set(keys.map((k) => k.element_digest))]),
	};
}

/**
 * Elements per statement.
 *
 * Both parameters travel in the query string, where ClickHouse's
 * http_max_uri_size ends the request at 1 MiB — and a digest is 44 bytes before
 * the identifier it came with. A chunk of matches can hold thousands, so it is
 * split rather than risk losing the erase to a 400 after the match rows have
 * already been written. Splitting is safe: the batches carry disjoint digests,
 * so removing one batch's elements cannot change what the next one counts.
 */
const ELEMENT_BATCH = 500;

/**
 * How many stored records these digests still account for.
 *
 * Records, not rows. A people row survives its erasure by design, so a row
 * count says nothing about whether it happened — the question is whether the
 * listed elements are still inside the array.
 */
export async function countPeopleElements(env: Env, keys: ElementKey[]): Promise<number> {
	if (keys.length === 0) return 0;
	const [r] = await chQuery<{ n: string }>(
		env,
		`SELECT sum(length(${LISTED_ELEMENTS})) AS n
		 FROM default.entity_search_results
		 WHERE ${PEOPLE_VALUE_MATCH}`,
		peopleParams(keys),
	);
	return Number(r?.n ?? 0);
}

/**
 * Erase the matched elements of a stored people report, and nothing else.
 *
 * BUSINESS-LOGIC.md Rule 2: the report is still served and still stored,
 * without those people. Search "John Smith", get back forty; one of them
 * registered with DROP and the other thirty-nine are strangers whose records
 * stay. deleteEntityRows would take all forty, which is why it is not used for
 * this type.
 *
 * ALTER TABLE ... UPDATE, the same array surgery the lookup API performs in
 * eraseEntitySearchResultRecords, and a mutation for the same reason the delete
 * is one: the part is rewritten, so the old payload does not sit on disk
 * waiting for a merge.
 *
 * ClickHouse does the surgery on its own stored value and only the digests
 * travel. Sending a rewritten payload back would not work at any size worth
 * having — a people payload runs to megabytes.
 *
 * The WHERE keeps the rewrite to rows that really do carry a listed element, so
 * a row whose people are all strangers is not touched at all, and its payload
 * keeps whatever shape it was stored in.
 *
 * Counted before and after rather than inferred from the number of digests
 * asked for: `after` is what tells the caller the erase actually happened.
 */
export async function erasePeopleElements(
	env: Env,
	keys: ElementKey[],
): Promise<{ before: number; after: number }> {
	let before = 0;
	let after = 0;

	for (let i = 0; i < keys.length; i += ELEMENT_BATCH) {
		const batch = keys.slice(i, i + ELEMENT_BATCH);
		const count = () => countPeopleElements(env, batch);

		const found = await count();
		if (found === 0) continue;
		before += found;

		await chQuery(
			env,
			`ALTER TABLE default.entity_search_results
			 UPDATE payload_json = concat('[', arrayStringConcat(
			     arrayFilter(
			         e -> NOT has(JSONExtract({digests:String}, 'Array(String)'), base64Encode(SHA256(e))),
			         ${STORED_ELEMENTS}
			     ), ','), ']')
			 WHERE ${PEOPLE_VALUE_MATCH} AND notEmpty(${LISTED_ELEMENTS})
			 SETTINGS mutations_sync = 2`,
			peopleParams(batch),
		);

		after += await count();
	}

	return { before, after };
}
