/**
 * Supabase access for the Workflows — Postgres via Hyperdrive.
 *
 * Cloudflare's own guidance: a Worker that opens its own TCP connection
 * re-does the TLS handshake on every invocation, so Hyperdrive holds the pool
 * instead. It also allows sslmode=verify-full against an uploaded CA, which a
 * Worker socket cannot do — that reproduces what the Nest API does with
 * rejectUnauthorized + DB_CA_CERT.
 *
 * Not PostgREST: it authenticates with a JWT signed by the project's JWT
 * secret, and that secret can mint a token for any role, service_role
 * included. A role password is worth exactly the grants on that role — here,
 * SELECT / INSERT / UPDATE on public.ca_drop_work_item and nothing else.
 *
 * Setup, once:
 *   npx wrangler cert upload certificate-authority \
 *       --ca-cert supabase-ca.pem --name supabase-ca
 *   npx wrangler hyperdrive create drop-db \
 *       --connection-string="postgresql://drop_workflow:<PASSWORD>@db.<ref>.supabase.co:5432/postgres" \
 *       --sslmode verify-full --ca-certificate-id <UUID>
 * then put the returned id in wrangler.jsonc as the DROP_DB binding.
 *
 * Until that exists, the secret SUPABASE_DB_URL is used instead, so the
 * workflow can be tested before Hyperdrive is configured. That fallback MUST
 * use the shared pooler in SESSION mode, not the direct host:
 *   postgresql://drop_workflow.<project-ref>:<password>@<pooler-host>:5432/postgres
 * The direct host (db.<ref>.supabase.co) resolves to IPv6 only unless the
 * project buys the IPv4 add-on, and a Worker socket is IPv4 — it does not
 * fail, it hangs. The shared pooler is IPv4 on every plan. Note the username:
 * the pooler wants <role>.<project-ref>. Hyperdrive is the opposite case and
 * wants the direct string, because Cloudflare does its own pooling.
 */

import postgres from "postgres";

export type WorkItemRow = {
	list_type: string;
	work_item_id: string;
	hash: string;
	request_date: string | null;
	/** the downloaded CSV this item arrived in; Cron B names its response after it */
	source_file: string;
};

/** One DROP work item found in ClickHouse, as Cron C reports it. */
export type MatchRow = {
	/** DROP list the hash came from, per the KV metadata */
	list_type: string;
	/**
	 * The DROP hash that matched. Work items are linked by this, not by a
	 * work_item_id: two consumers can share a phone or e-mail, which gives two
	 * work items one hash, and KV and ClickHouse each keep only one of them.
	 */
	hash: string;
	/** the identifier the hash matched — e-mail, phone, or the NDZ/NameVIN source value */
	matched_normalized_value: string;
};

/** A live work item a match was linked to, by DROP's own identifiers. */
export type LinkedWorkItem = { list_type: string; work_item_id: string; hash: string };

/**
 * sql.end() waits for the connection to close, and on a half-open TLS socket
 * that wait does not finish — which is why db-test was answering a bare 500:
 * the handler had its result and was stuck in `finally`. Closing is
 * best-effort, and nothing downstream depends on it: Hyperdrive (or the
 * pooler) owns the real connection.
 */
async function closeQuietly(sql: { end: (o?: { timeout?: number }) => Promise<void> } | undefined) {
	if (!sql) return;

	// .catch() goes on FIRST, before the race, and that ordering is the whole
	// point. Promise.race does not cancel the loser: when the timer below wins,
	// this function returns and its try/catch is finished, but end() is still
	// pending. If it then rejects — "write CONNECTION_CLOSED", because the
	// socket to Hyperdrive went away while it was closing — nothing is left
	// handling it, and an unhandled rejection in a Worker does not warn, it
	// tears down the invocation. The step dies with "Network connection lost"
	// some distance from the query that actually ran, which is exactly as
	// confusing as it sounds.
	//
	// Attaching the handler at creation makes the rejection handled whenever it
	// arrives, raced or not.
	const ending = sql.end({ timeout: 1 }).catch(() => {
		// A connection that never opened, or one already gone, has nothing
		// worth reporting on close. Hyperdrive owns the real connection.
	});

	await Promise.race([ending, new Promise<void>((resolve) => setTimeout(resolve, 3000))]);
}

/** Nothing in workerd enforces postgres.js's own connect_timeout, so a socket
 *  that never establishes would hang the whole step until the Workflow's
 *  timeout. This turns that into a real error with a usable message. */
async function withTimeout<T>(work: Promise<T>, ms: number, what: string): Promise<T> {
	// The same trap closeQuietly fell into. Promise.race does not cancel the
	// loser: if the timeout below wins, `work` is still in flight, and when it
	// later rejects — which it will, the query having been abandoned — nothing
	// is handling that rejection. An unhandled rejection in a Worker does not
	// warn, it tears down the invocation, and the failure then surfaces
	// somewhere unrelated as "Network connection lost".
	//
	// This extra handler does not affect the race, which still awaits the
	// original promise. It only guarantees the rejection has somewhere to go.
	void work.catch(() => {});

	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			work,
			new Promise<never>((_, reject) => {
				timer = setTimeout(
					() =>
						reject(
							new Error(
								`${what}: no response in ${ms / 1000}s. If this is the SUPABASE_DB_URL ` +
									`fallback, check it uses the shared pooler in session mode (port 5432, ` +
									`user <role>.<project-ref>) — the direct host is IPv6-only and a Worker ` +
									`socket will hang on it.`,
							),
						),
					ms,
				);
			}),
		]);
	} finally {
		if (timer !== undefined) clearTimeout(timer);
	}
}

/** DROP_DB is declared only when the Hyperdrive binding exists, hence the cast. */
function connectionString(env: Env): string | null {
	const hyperdrive = (env as unknown as { DROP_DB?: { connectionString?: string } }).DROP_DB;
	return hyperdrive?.connectionString ?? env.SUPABASE_DB_URL ?? null;
}

export function hasDb(env: Env): boolean {
	return Boolean(connectionString(env));
}

/** host:port of the target, for logs. Never the credentials. */
function describe(env: Env): string {
	const url = connectionString(env);
	if (!url) return "none";
	try {
		const u = new URL(url);
		return `${u.hostname}:${u.port || "5432"} as ${decodeURIComponent(u.username)}`;
	} catch {
		return "unparseable";
	}
}

/**
 * Everything that can be wrong with the fallback connection string, said
 * plainly. Without this, a placeholder left in the secret surfaces as
 * "TypeError: Invalid URL string." from inside the driver, four retries deep.
 */
function checkUrl(url: string): void {
	if (/[<>]/.test(url)) {
		throw new Error(
			"SUPABASE_DB_URL still contains a placeholder (< or >). Paste the real " +
				"connection string from Supabase → Connect → Session pooler.",
		);
	}

	let u: URL;
	try {
		u = new URL(url);
	} catch {
		throw new Error(
			"SUPABASE_DB_URL is not a valid connection string. Expected " +
				"postgresql://<role>.<project-ref>:<password>@<pooler-host>:5432/postgres",
		);
	}

	if (u.protocol !== "postgres:" && u.protocol !== "postgresql:") {
		throw new Error(`SUPABASE_DB_URL has protocol ${u.protocol}, expected postgresql:`);
	}
	if (/^db\..*\.supabase\.co$/.test(u.hostname)) {
		throw new Error(
			`${u.hostname} is the direct host, which Supabase serves over IPv6 only ` +
				"unless the project has the IPv4 add-on — a Worker socket hangs on it. " +
				"Use the shared pooler in session mode instead. The direct host is what " +
				"the Hyperdrive config wants, not the Worker.",
		);
	}
	if (u.hostname.endsWith(".pooler.supabase.com") && !u.username.includes(".")) {
		throw new Error(
			`the pooler needs the username as <role>.<project-ref>, got "${u.username}"`,
		);
	}
}

/**
 * One client per step, with options that depend on which path we are on —
 * they are not interchangeable.
 *
 * Through Hyperdrive: exactly Cloudflare's documented sample, and no ssl.
 * The hop from the Worker to hyperdrive.local is internal; Hyperdrive does
 * the real TLS to Supabase itself with verify-full. Forcing ssl here makes
 * the driver send an SSLRequest into that internal hop and wait forever for
 * an answer that never comes — which is what made db-test hang at 8s.
 *
 * Direct to the pooler (the SUPABASE_DB_URL fallback): the opposite. TLS is
 * ours to ask for, and `prepare` must be off because Supavisor in
 * transaction mode does not keep prepared statements.
 */
function connect(env: Env) {
	const hyperdrive = (env as unknown as { DROP_DB?: { connectionString?: string } }).DROP_DB;
	return connectVia(
		hyperdrive?.connectionString,
		env.SUPABASE_DB_URL,
		"no database: add the DROP_DB Hyperdrive binding or set SUPABASE_DB_URL",
	);
}

/**
 * The website's Supabase project — a different database from the one above,
 * reached the same way: the WEBSITE_DB Hyperdrive binding, or the
 * WEBSITE_DB_URL secret until that exists. Its role is granted SELECT and
 * DELETE on public.search_history and nothing else (sql/website-supabase.sql).
 */
function connectWebsite(env: Env) {
	const hyperdrive = (env as unknown as { WEBSITE_DB?: { connectionString?: string } }).WEBSITE_DB;
	return connectVia(
		hyperdrive?.connectionString,
		env.WEBSITE_DB_URL,
		"no website database: add the WEBSITE_DB Hyperdrive binding or set WEBSITE_DB_URL",
	);
}

export function hasWebsiteDb(env: Env): boolean {
	const hyperdrive = (env as unknown as { WEBSITE_DB?: { connectionString?: string } }).WEBSITE_DB;
	return Boolean(hyperdrive?.connectionString ?? env.WEBSITE_DB_URL);
}

function connectVia(hyperdriveConnectionString: string | undefined, url: string | undefined, missing: string) {
	if (hyperdriveConnectionString) {
		return postgres(hyperdriveConnectionString, {
			max: 5,
			fetch_types: false,
			// prepare: false, and this is the important one.
			//
			// A named prepared statement is CONNECTION-scoped: Parse names it on
			// one backend, and Bind/Execute must land on that same backend.
			// Hyperdrive pools and reuses connections underneath us, so there is
			// no guarantee the second message reaches the socket the first one
			// named. When it does not, the protocol desynchronises and the
			// connection is torn down — which arrives in the Worker as
			// "Network connection lost", with retryable: true and NO SQLSTATE,
			// because Postgres never got to answer.
			//
			// That matches every symptom: the query runs to completion on the
			// server (its backend sat idle afterwards with the statement as
			// last_query) while the Worker sees the socket die; psql against the
			// same database with the same credentials runs it in 74ms; and it
			// worked once, early on, when the statement was new to a fresh
			// connection.
			//
			// Unnamed statements still get placeholders and parameter binding —
			// no SQL is built by hand, and everything below stays parameterised.
			// The cost is re-planning per execution, which for a handful of
			// statements per run is nothing.
			prepare: false,
		});
	}

	if (!url) throw new Error(missing);
	checkUrl(url);
	return postgres(url, {
		max: 1,
		idle_timeout: 10,
		connect_timeout: 15,
		prepare: false,
		fetch_types: false,
		ssl: "require",
	});
}

/**
 * Upsert on (list_type, work_item_id), so a retried step is a no-op instead of
 * a duplicate. Deliberately does not touch status / matched / status_set_at:
 * those belong to the report and cleanup runs, and re-downloading a list must
 * never reset a status that was already reported to DROP.
 */
export async function upsertWorkItems(env: Env, rows: WorkItemRow[]): Promise<number> {
	if (rows.length === 0) return 0;
	console.log(`db: upserting ${rows.length} rows → ${describe(env)}`);
	const sql = connect(env);
	try {
		await withTimeout(
			sql`
				INSERT INTO public.ca_drop_work_item
					${sql(rows, "list_type", "work_item_id", "hash", "request_date", "source_file")}
				ON CONFLICT (list_type, work_item_id) DO UPDATE
					SET hash         = EXCLUDED.hash,
					    request_date = EXCLUDED.request_date,
					    source_file  = COALESCE(public.ca_drop_work_item.source_file,
					                            EXCLUDED.source_file)
			`,
			20000,
			"upsert ca_drop_work_item",
		);
		console.log(`db: upserted ${rows.length} rows`);
		return rows.length;
	} finally {
		await closeQuietly(sql);
	}
}

export type MatchWriteResult = {
	/** rows Cron C handed over, including any it could not send */
	submitted: number;
	/** dropped before the insert because the CHECK constraint would reject them */
	skippedEmpty: number;
	/** of those sent, the ones whose hash no live work item holds */
	unlinked: number;
	/** (match, work item) pairs — above `submitted` when work items share a hash */
	linked: number;
	/** distinct work items among the linked rows */
	workItems: number;
	/** match rows written — lower than `linked` when a previous attempt wrote them */
	inserted: number;
	/** ids of those work items, for the status write once ClickHouse is clear */
	workItemIds: string[];
	/** the same work items by DROP's identifiers, for the alert and the evidence file */
	linkedWorkItems: LinkedWorkItem[];
};

/**
 * Why the match rows did not all land, or null if they did.
 *
 * Only about the insert. The status is written later and separately, because
 * it may not be written until the rows are actually gone from ClickHouse.
 *
 * `inserted` is deliberately not part of the test. It is lower than `linked`
 * whenever a previous attempt already wrote the rows, which is exactly what a
 * retried step should do.
 */
export function incompleteReason(r: MatchWriteResult): string | null {
	if (r.skippedEmpty > 0) {
		return `${r.skippedEmpty} match(es) had an empty normalized value and could not be stored`;
	}
	if (r.unlinked > 0) {
		return (
			`${r.unlinked} of ${r.submitted} match(es) had no live row in ` +
			`ca_drop_work_item — KV and Supabase have drifted, or Cron A has not run since ` +
			`the table was last rebuilt`
		);
	}
	return null;
}

/**
 * Record the matches Cron C found.
 *
 * ca_drop_work_item_match references ca_drop_work_item by its bigint id, and
 * the join happens here, in one statement, rather than as a lookup per match.
 *
 * It joins on (list_type, hash) against LIVE work items, never on the
 * work_item_id KV carries. Two consumers can share a phone or e-mail, so one
 * hash can belong to several work items, and KV and ClickHouse keep only one
 * id per hash. Joining on that id would link one consumer and leave the other
 * unmatched, to be reported as 5 Not found for data that was erased. Joining
 * on the hash links every one of them.
 *
 * The rows travel as a single jsonb parameter rather than N placeholders or a
 * text[]: it is one bind regardless of batch size, and it does not depend on
 * the driver inferring an array type, which `fetch_types: false` leaves it
 * unable to do.
 *
 * The cast is `::text::jsonb` and the extra hop is load-bearing. Written as
 * `::jsonb`, Postgres describes the parameter as jsonb, and postgres.js then
 * serializes it with its own jsonb encoder — JSON.stringify over a value that
 * JSON.stringify already produced. The result is a JSON *string* rather than
 * a JSON array, and jsonb_to_recordset rejects it with "cannot call
 * jsonb_to_recordset on a non-array". Going through text pins the parameter
 * to the type it actually is, so the driver ships the string verbatim and
 * Postgres does the one parse.
 *
 * Idempotent twice over, because a retried Workflow step re-runs it verbatim:
 * DISTINCT collapses duplicates inside the batch, ON CONFLICT DO NOTHING
 * absorbs whatever a previous attempt already wrote.
 *
 * The conflict target is the (work item, identifier) PAIR, so a work item may
 * match more than one identifier and all of them are kept. That matters most
 * for NDZ and NameVIN: those hashes stand for a person — a name with a date
 * of birth and a ZIP — not for a single contact detail, so one DROP work item
 * legitimately matches every e-mail address and phone number that person
 * appears under. Keyed on the work item alone, the first match would win and
 * the rest would be silently dropped, and since the expire destroys the
 * matched rows there would be nothing left to recover them from.
 *
 * `submitted - linked` is worth watching. It counts hashes that are in KV but
 * whose work item is missing from Supabase, which means the two stores have
 * drifted and Cron B would under-report.
 */
export async function recordMatches(env: Env, rows: MatchRow[]): Promise<MatchWriteResult> {
	// ca_drop_work_item_match has CHECK (matched_normalized_value <> ''), and a
	// failed CHECK aborts the whole statement — so a single empty identifier
	// would throw away every match in the batch alongside it, then burn the
	// step's retries reproducing the same failure. There are none in the view
	// today; this keeps one appearing later a counted skip rather than a run
	// that dies at batch 40 of 100.
	const usable = rows.filter((r) => r.matched_normalized_value !== "");
	const skippedEmpty = rows.length - usable.length;
	if (skippedEmpty > 0) {
		console.warn(`db: skipped ${skippedEmpty} match(es) with an empty normalized value`);
	}

	if (usable.length === 0) {
		return {
			submitted: rows.length,
			skippedEmpty,
			unlinked: 0,
			linked: 0,
			workItems: 0,
			inserted: 0,
			workItemIds: [],
			linkedWorkItems: [],
		};
	}

	console.log(`db: recording ${usable.length} match(es) → ${describe(env)}`);
	const sql = connect(env);
	try {
		const [r] = await withTimeout(
			sql<
				{
					submitted: string;
					unlinked: string;
					linked: string;
					work_items: string;
					inserted: string;
					item_ids: string;
					linked_items: string;
				}[]
			>`
				WITH v AS (
					SELECT *
					FROM jsonb_to_recordset(${JSON.stringify(usable)}::text::jsonb)
						AS v(list_type text, hash text, matched_normalized_value text)
				),
				linked AS (
					SELECT w.id, w.list_type, w.work_item_id, w.hash, v.matched_normalized_value
					FROM v
					JOIN public.ca_drop_work_item w
						ON w.list_type = v.list_type
					   AND w.hash = v.hash
					   AND w.revoked_at IS NULL
				),
				items AS (
					SELECT DISTINCT id, list_type, work_item_id, hash FROM linked
				),
				ins AS (
					INSERT INTO public.ca_drop_work_item_match
						(ca_drop_work_item_id, matched_normalized_value)
					SELECT DISTINCT id, matched_normalized_value
					FROM linked
					ON CONFLICT (ca_drop_work_item_id, matched_normalized_value) DO NOTHING
					RETURNING 1
				)
				SELECT (SELECT count(*) FROM v)      AS submitted,
				       (SELECT count(*) FROM v
				        WHERE NOT EXISTS (
				            SELECT 1 FROM public.ca_drop_work_item w
				            WHERE w.list_type = v.list_type
				              AND w.hash = v.hash
				              AND w.revoked_at IS NULL
				        ))                           AS unlinked,
				       (SELECT count(*) FROM linked) AS linked,
				       (SELECT count(*) FROM items)  AS work_items,
				       (SELECT count(*) FROM ins)    AS inserted,
				       (SELECT coalesce(jsonb_agg(id), '[]'::jsonb) FROM items)::text AS item_ids,
				       (SELECT coalesce(jsonb_agg(jsonb_build_object(
				            'list_type', list_type,
				            'work_item_id', work_item_id,
				            'hash', hash)), '[]'::jsonb) FROM items)::text AS linked_items
			`,
			30000,
			"insert ca_drop_work_item_match",
		);

		return {
			submitted: Number(r?.submitted ?? 0) + skippedEmpty,
			skippedEmpty,
			unlinked: Number(r?.unlinked ?? 0),
			linked: Number(r?.linked ?? 0),
			workItems: Number(r?.work_items ?? 0),
			inserted: Number(r?.inserted ?? 0),
			workItemIds: JSON.parse(r?.item_ids ?? "[]") as string[],
			linkedWorkItems: JSON.parse(r?.linked_items ?? "[]") as LinkedWorkItem[],
		};
	} finally {
		await closeQuietly(sql);
	}
}

/**
 * Mark matched work items as deleted.
 *
 * Separate from recordMatches, and called only once the rows are actually
 * gone from entity_search_results. `status = 'deleted'` is what Cron B
 * reports to California as code 3, so writing it before the data is erased
 * would put a false statement in front of a regulator — the match row is a
 * record that we found the consumer, this is a claim that we acted on it.
 *
 * IS DISTINCT FROM 'deleted' keeps the write to once per work item. A work
 * item is matched again in every later batch that turns up another of its
 * identifiers, which for NDZ and NameVIN is the normal case, and
 * status_set_at should say when the consumer was first cleared rather than
 * when the scan last passed one of their records.
 *
 * Returns how many of the given work items now carry the status, counting
 * the ones a previous attempt set — the test is the end state, not the
 * delta, so a retried step does not read as a failure.
 */
export async function markMatchesDeleted(env: Env, workItemIds: string[]): Promise<number> {
	if (workItemIds.length === 0) return 0;

	const sql = connect(env);
	try {
		const [r] = await withTimeout(
			sql<{ updated: string; already: string }[]>`
				WITH ids AS (
					SELECT (jsonb_array_elements_text(${JSON.stringify(workItemIds)}::text::jsonb))::bigint AS id
				),
				already AS (
					SELECT w.id FROM ids i
					JOIN public.ca_drop_work_item w ON w.id = i.id
					WHERE w.status = 'deleted'
				),
				upd AS (
					UPDATE public.ca_drop_work_item w
					SET status = 'deleted', status_set_at = now()
					FROM ids i
					WHERE w.id = i.id AND w.status IS DISTINCT FROM 'deleted'
					RETURNING w.id
				)
				SELECT (SELECT count(*) FROM upd)     AS updated,
				       (SELECT count(*) FROM already) AS already
			`,
			30000,
			"update ca_drop_work_item.status",
		);
		return Number(r?.updated ?? 0) + Number(r?.already ?? 0);
	} finally {
		await closeQuietly(sql);
	}
}

/**
 * One round trip to Postgres, with whatever went wrong reported verbatim.
 *
 * A failure on the Worker -> Hyperdrive -> Supabase path arrives as
 * "CONNECTION_CLOSED" or "Network connection lost", which says only that the
 * socket went away. The cause -- wrong password, no CONNECT privilege, the
 * project asleep, connection limit reached, a query rejected -- happens on the
 * Hyperdrive -> Supabase hop and never reaches the Worker as itself.
 *
 * So this asks a series of increasingly demanding questions and reports which
 * one stopped answering. Connect, then identity, then each table Cron C needs,
 * separately: a table that errors on permission is a grant problem, and one
 * that returns zero rows where rows are expected is RLS hiding them, which
 * looks identical from inside the workflow.
 */
export async function dbPing(env: Env): Promise<Record<string, unknown>> {
	const out: Record<string, unknown> = { target: describe(env) };
	if (!hasDb(env)) return { ...out, ok: false, stage: "config", error: "no DROP_DB binding and no SUPABASE_DB_URL" };

	let sql: ReturnType<typeof connect> | undefined;
	const fail = (stage: string, e: unknown) => ({
		...out,
		ok: false,
		stage,
		error: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
		// postgres.js puts the SQLSTATE here; its absence is itself a signal,
		// because it means the failure was below the protocol rather than a
		// database saying no.
		code: (e as { code?: string })?.code ?? null,
	});

	try {
		sql = connect(env);
	} catch (e) {
		return fail("connect", e);
	}

	try {
		const [who] = await withTimeout(
			sql`SELECT current_user AS usr, current_database() AS db, version() AS ver`,
			15000,
			"db ping",
		);
		out.current_user = who?.usr;
		out.database = who?.db;
		out.server = String(who?.ver ?? "").split(" on ")[0];
	} catch (e) {
		await closeQuietly(sql);
		return fail("identity", e);
	}

	for (const table of ["ca_drop_work_item", "ca_drop_work_item_match"]) {
		try {
			const [r] = await withTimeout(
				sql`SELECT count(*)::text AS n FROM public.${sql(table)}`,
				15000,
				`count ${table}`,
			);
			out[`${table}_rows`] = r?.n ?? null;
		} catch (e) {
			out[`${table}_rows`] = null;
			out[`${table}_error`] =
				e instanceof Error ? `${e.name}: ${e.message}` : String(e);
		}
	}

	// Visible only when RLS is on AND a policy admits this role. Zero rows in a
	// table that is not empty is the signature of a missing policy.
	try {
		const [r] = await withTimeout(
			sql`SELECT count(*)::text AS n FROM pg_policies
			    WHERE schemaname = 'public' AND tablename LIKE 'ca\\_drop\\_%'`,
			15000,
			"policies",
		);
		out.policies_visible = r?.n ?? null;
	} catch {
		out.policies_visible = null;
	}

	await closeQuietly(sql);
	return { ...out, ok: true };
}

/**
 * How many LIVE work items exist. The number KV is expected to match.
 *
 * Revoked items are excluded because they are deliberately absent from KV —
 * counting them would make every revocation look like a key KV had lost.
 */
export async function countWorkItems(env: Env): Promise<number> {
	const sql = connect(env);
	try {
		const [r] = await withTimeout(
			sql<{ n: string }[]>`
				SELECT count(*)::text AS n
				FROM public.ca_drop_work_item
				WHERE revoked_at IS NULL
			`,
			15000,
			"count ca_drop_work_item",
		);
		return Number(r?.n ?? 0);
	} finally {
		await closeQuietly(sql);
	}
}

/**
 * A page of work items, for rebuilding KV.
 *
 * Keyset pagination on the primary key, not OFFSET: the repair walks the whole
 * table, and OFFSET re-reads everything before the page on every call, so the
 * last pages of a large rebuild cost more than the first. `id > $after` costs
 * the same at row 1 and row 3,000,000.
 */
export async function pageWorkItems(
	env: Env,
	afterId: string,
	limit: number,
): Promise<{ id: string; list_type: string; work_item_id: string; hash: string; request_date: string | null }[]> {
	const sql = connect(env);
	try {
		return await withTimeout(
			sql<
				{ id: string; list_type: string; work_item_id: string; hash: string; request_date: string | null }[]
			>`
				SELECT id::text AS id, list_type, work_item_id, hash,
				       to_char(request_date, 'YYYY-MM-DD') AS request_date
				FROM public.ca_drop_work_item
				WHERE id > ${afterId}::bigint
				  AND revoked_at IS NULL
				ORDER BY id
				LIMIT ${limit}
			`,
			30000,
			"page ca_drop_work_item",
		);
	} finally {
		await closeQuietly(sql);
	}
}

/**
 * A random handful of work items, to spot-check KV against.
 *
 * Counting KV means listing all of it, which is one call per 1,000 keys — fine
 * as part of a run that is listing anyway, far too slow for a health check.
 * Sampling costs one query and N reads, and it catches the failure that
 * matters most: if KV has been emptied, every sampled key misses at once.
 */
export async function sampleWorkItems(
	env: Env,
	n: number,
): Promise<{ list_type: string; work_item_id: string; hash: string }[]> {
	const sql = connect(env);
	try {
		return await withTimeout(
			sql<{ list_type: string; work_item_id: string; hash: string }[]>`
				SELECT list_type, work_item_id, hash
				FROM public.ca_drop_work_item
				WHERE revoked_at IS NULL
				ORDER BY random()
				LIMIT ${n}
			`,
			20000,
			"sample ca_drop_work_item",
		);
	} finally {
		await closeQuietly(sql);
	}
}

/**
 * A value whose report had to be suppressed although the value itself was not
 * on the DROP list.
 *
 * One row per (type, value), written the first time a report matches. No
 * counters, no matched families, no clearing and no expiry: once a value is
 * here it is not processed again, and everything beyond "we have seen this"
 * was cost without a reader.
 *
 * `value` is DROP-normalized, so one consumer is one row however the search was
 * typed, and so its suppressed_kv key can be re-derived during a repair.
 */
export async function recordSuppressedValue(
	env: Env,
	searchType: string,
	value: string,
): Promise<void> {
	const sql = connect(env);
	try {
		await withTimeout(
			sql`
				INSERT INTO public.ca_drop_suppressed_value (search_type, value)
				VALUES (${searchType}, ${value})
				ON CONFLICT (search_type, value) DO NOTHING
			`,
			20000,
			"upsert ca_drop_suppressed_value",
		);
	} finally {
		await closeQuietly(sql);
	}
}


/**
 * Mark work items as revoked, from a DROP removals file.
 *
 * The row is stamped rather than deleted. It is the record that we were once
 * asked to suppress this consumer and then released, and Cron B needs to know
 * the difference between "never given to us" and "given and then withdrawn".
 *
 * Idempotent: `revoked_at IS NULL` in the WHERE means a re-run of the same
 * removals file is a no-op and does not move the timestamp, so the date stays
 * the date we first saw the revocation.
 *
 * Returns how many rows this call actually changed. A removal naming a work
 * item we never held counts nothing, which is normal — DROP does not know
 * which of its identifiers we were given.
 */
export async function markWorkItemsRevoked(
	env: Env,
	rows: { list_type: string; work_item_id: string }[],
): Promise<number> {
	if (rows.length === 0) return 0;

	const sql = connect(env);
	try {
		const [r] = await withTimeout(
			sql<{ revoked: string }[]>`
				WITH v AS (
					SELECT *
					FROM jsonb_to_recordset(${JSON.stringify(rows)}::text::jsonb)
						AS v(list_type text, work_item_id text)
				),
				upd AS (
					UPDATE public.ca_drop_work_item w
					SET revoked_at = now()
					FROM v
					WHERE w.list_type = v.list_type
					  AND w.work_item_id = v.work_item_id
					  AND w.revoked_at IS NULL
					RETURNING w.id
				)
				SELECT count(*)::text AS revoked FROM upd
			`,
			30000,
			"revoke ca_drop_work_item",
		);
		return Number(r?.revoked ?? 0);
	} finally {
		await closeQuietly(sql);
	}
}

/**
 * Suppressed values, oldest first, for rebuilding suppressed_kv.
 *
 * Paged by id rather than OFFSET so a repair running while rows are added
 * cannot skip one: the cursor is a row that exists.
 */
export async function pageSuppressedValues(
	env: Env,
	afterId: string,
	limit: number,
): Promise<{ id: string; search_type: string; value: string }[]> {
	const sql = connect(env);
	try {
		return await withTimeout(
			sql<{ id: string; search_type: string; value: string }[]>`
				SELECT id::text AS id, search_type, value
				FROM public.ca_drop_suppressed_value
				WHERE id > ${afterId}::bigint
				ORDER BY id
				LIMIT ${limit}
			`,
			30000,
			"page ca_drop_suppressed_value",
		);
	} finally {
		await closeQuietly(sql);
	}
}

/** The newest live work item's arrival, epoch ms. Cron C must have started after it. */
export async function latestIngestAt(env: Env): Promise<number | null> {
	const sql = connect(env);
	try {
		const [r] = await withTimeout(
			sql<{ at: string | null }[]>`
				SELECT (extract(epoch FROM max(added_at)) * 1000)::bigint::text AS at
				FROM public.ca_drop_work_item
				WHERE revoked_at IS NULL
			`,
			15000,
			"latest ca_drop_work_item",
		);
		return r?.at == null ? null : Number(r.at);
	} finally {
		await closeQuietly(sql);
	}
}

export type ReportKind = "upload" | "amend";

export type ReportGroup = {
	source_file: string;
	list_type: string;
	kind: ReportKind;
	rows: number;
};

export type ReportPlan = {
	groups: ReportGroup[];
	/** live items Cron B cannot name a response file for */
	missingSourceFile: number;
	/** matched but not yet erased — neither 5 nor 3 would be true today */
	held: number;
	/** days since the oldest never-reported live item arrived; 0 if none */
	oldestUnreportedDays: number;
};

/**
 * What Cron B has to send, grouped by the downloaded file each item came from.
 *
 *   upload  never reported
 *   amend   reported, and the code it would get today is different
 *   held    status not set yet but a match row exists: Cron C has found the
 *           consumer and not finished erasing, so it is skipped this run
 *
 * Revoked items are never reported: DROP asks for no response to removals.
 */
export async function planStatusReport(env: Env): Promise<ReportPlan> {
	const sql = connect(env);
	try {
		const rows = await withTimeout(
			sql<{
				source_file: string | null;
				list_type: string;
				kind: string;
				rows: string;
				oldest_days: string | null;
			}[]>`
				WITH live AS (
					SELECT w.source_file, w.list_type, w.reported_status, w.added_at,
					       CASE w.status
					           WHEN 'exempted'  THEN 2
					           WHEN 'deleted'   THEN 3
					           WHEN 'opted_out' THEN 4
					           ELSE 5
					       END AS code,
					       (w.status IS NULL AND EXISTS (
					           SELECT 1 FROM public.ca_drop_work_item_match m
					           WHERE m.ca_drop_work_item_id = w.id
					       )) AS held
					FROM public.ca_drop_work_item w
					WHERE w.revoked_at IS NULL
				)
				SELECT source_file, list_type,
				       CASE
				           WHEN held                        THEN 'held'
				           WHEN reported_status IS NULL     THEN 'upload'
				           WHEN reported_status <> code     THEN 'amend'
				           ELSE 'done'
				       END AS kind,
				       count(*)::text AS rows,
				       (extract(epoch FROM now() - min(added_at)) / 86400)::int::text AS oldest_days
				FROM live
				GROUP BY 1, 2, 3
			`,
			30000,
			"plan status report",
		);

		const plan: ReportPlan = { groups: [], missingSourceFile: 0, held: 0, oldestUnreportedDays: 0 };
		for (const r of rows) {
			const n = Number(r.rows);
			if (r.kind === "done") continue;
			if (r.kind === "held") {
				plan.held += n;
				continue;
			}
			if (r.kind === "upload") {
				plan.oldestUnreportedDays = Math.max(plan.oldestUnreportedDays, Number(r.oldest_days ?? 0));
			}
			if (r.source_file === null) {
				plan.missingSourceFile += n;
				continue;
			}
			plan.groups.push({
				source_file: r.source_file,
				list_type: r.list_type,
				kind: r.kind as ReportKind,
				rows: n,
			});
		}
		plan.groups.sort((a, b) =>
			`${a.kind}|${a.source_file}`.localeCompare(`${b.kind}|${b.source_file}`),
		);
		return plan;
	} finally {
		await closeQuietly(sql);
	}
}

export type ReportItem = { id: string; work_item_id: string; code: number };

/** One page of a report group, by id, with the code each item gets today. */
export async function pageReportItems(
	env: Env,
	group: { source_file: string; list_type: string; kind: ReportKind },
	afterId: string,
	limit: number,
): Promise<ReportItem[]> {
	const sql = connect(env);
	try {
		return await withTimeout(
			sql<ReportItem[]>`
				WITH live AS (
					SELECT w.id, w.work_item_id, w.reported_status,
					       CASE w.status
					           WHEN 'exempted'  THEN 2
					           WHEN 'deleted'   THEN 3
					           WHEN 'opted_out' THEN 4
					           ELSE 5
					       END AS code,
					       (w.status IS NULL AND EXISTS (
					           SELECT 1 FROM public.ca_drop_work_item_match m
					           WHERE m.ca_drop_work_item_id = w.id
					       )) AS held
					FROM public.ca_drop_work_item w
					WHERE w.revoked_at IS NULL
					  AND w.source_file = ${group.source_file}
					  AND w.list_type = ${group.list_type}
					  AND w.id > ${afterId}::bigint
				)
				SELECT id::text AS id, work_item_id, code
				FROM live
				WHERE NOT held
				  AND ${
						group.kind === "upload"
							? sql`reported_status IS NULL`
							: sql`reported_status IS NOT NULL AND reported_status <> code`
					}
				ORDER BY id
				LIMIT ${limit}
			`,
			30000,
			"page status report",
		);
	} finally {
		await closeQuietly(sql);
	}
}

/**
 * Record what DROP accepted. Only for files the upload answered as accepted,
 * and with the code that was in the file, so a status that changed since the
 * file was built is still amended next run.
 */
export async function markReported(
	env: Env,
	listType: string,
	items: { work_item_id: string; code: number }[],
): Promise<number> {
	if (items.length === 0) return 0;
	const sql = connect(env);
	try {
		const [r] = await withTimeout(
			sql<{ n: string }[]>`
				WITH v AS (
					SELECT *
					FROM jsonb_to_recordset(${JSON.stringify(items)}::text::jsonb)
						AS v(work_item_id text, code int)
				),
				upd AS (
					UPDATE public.ca_drop_work_item w
					SET reported_status = v.code, reported_at = now()
					FROM v
					WHERE w.list_type = ${listType}
					  AND w.work_item_id = v.work_item_id
					RETURNING w.id
				)
				SELECT count(*)::text AS n FROM upd
			`,
			30000,
			"mark ca_drop_work_item reported",
		);
		return Number(r?.n ?? 0);
	} finally {
		await closeQuietly(sql);
	}
}

export type HashHolder = {
	list_type: string;
	hash: string;
	work_item_id: string;
	request_date: string | null;
};

/**
 * For each (list_type, hash), one LIVE work item that still holds it — or
 * nothing, if none does.
 *
 * A removal withdraws one work item, not one hash: a household sharing a phone
 * is two consumers and two work items under one hash. Before a removal takes
 * the hash out of KV, this says whether someone else still needs it there.
 */
export async function liveHoldersOf(
	env: Env,
	pairs: { list_type: string; hash: string }[],
): Promise<HashHolder[]> {
	if (pairs.length === 0) return [];
	const sql = connect(env);
	try {
		return await withTimeout(
			sql<HashHolder[]>`
				WITH v AS (
					SELECT DISTINCT list_type, hash
					FROM jsonb_to_recordset(${JSON.stringify(pairs)}::text::jsonb)
						AS v(list_type text, hash text)
				)
				SELECT DISTINCT ON (w.list_type, w.hash)
				       w.list_type, w.hash, w.work_item_id,
				       to_char(w.request_date, 'YYYY-MM-DD') AS request_date
				FROM v
				JOIN public.ca_drop_work_item w
					ON w.list_type = v.list_type
				   AND w.hash = v.hash
				   AND w.revoked_at IS NULL
				ORDER BY w.list_type, w.hash, w.id DESC
			`,
			30000,
			"live holders of ca_drop_work_item hashes",
		);
	} finally {
		await closeQuietly(sql);
	}
}

/**
 * How many distinct live (list_type, hash) pairs exist — the number of keys KV
 * should hold. Not the number of work items: two sharing a hash are one key.
 */
export async function countLiveHashes(env: Env): Promise<number> {
	const sql = connect(env);
	try {
		const [r] = await withTimeout(
			sql<{ n: string }[]>`
				SELECT count(*)::text AS n
				FROM (
					SELECT DISTINCT list_type, hash
					FROM public.ca_drop_work_item
					WHERE revoked_at IS NULL
				) d
			`,
			15000,
			"count live ca_drop_work_item hashes",
		);
		return Number(r?.n ?? 0);
	} finally {
		await closeQuietly(sql);
	}
}


export type SearchHistoryErase = { matched: number; deleted: number; remaining: number };

/**
 * Delete the website's search_history rows for identifiers Cron C erased.
 *
 * Phone and e-mail only: for those the searched value IS the listed
 * consumer's identifier. A people search is a name shared by many people, and
 * Rule 2 keeps the user's record of searching it.
 *
 * search_history stores the value as typed, so it is normalized here the way
 * the website's own normalizeValue does — digits only for a phone, trimmed
 * and lowercased otherwise — which is also how entity_search_results'
 * normalized_value was made.
 *
 * A matched row's deep-search children go with it: they are the same
 * report's follow-up searches, and the parent key is ON DELETE SET NULL, so
 * deleting only the parent would leave them behind, unlinked. What remains
 * is counted afterwards, so a survivor fails the chunk rather than being
 * assumed gone.
 */
export async function deleteSearchHistory(
	env: Env,
	keys: { type: string; normalized_value: string }[],
): Promise<SearchHistoryErase> {
	const pairs = keys.filter((k) => k.type === "phone" || k.type === "email");
	if (pairs.length === 0) return { matched: 0, deleted: 0, remaining: 0 };

	const sql = connectWebsite(env);
	try {
		const [r] = await withTimeout(
			sql<{ matched: string; deleted: string }[]>`
				WITH v AS (
					SELECT DISTINCT type, normalized_value
					FROM jsonb_to_recordset(${JSON.stringify(pairs)}::text::jsonb)
						AS v(type text, normalized_value text)
				),
				m AS (
					SELECT s.id
					FROM public.search_history s
					JOIN v ON v.type = s.type
					      AND v.normalized_value = CASE s.type
					          WHEN 'phone' THEN regexp_replace(s.value, '\D', '', 'g')
					          ELSE lower(regexp_replace(s.value, '^\s+|\s+$', '', 'g'))
					      END
				),
				del AS (
					DELETE FROM public.search_history
					WHERE id IN (SELECT id FROM m) OR parent_id IN (SELECT id FROM m)
					RETURNING 1
				)
				SELECT (SELECT count(*) FROM m)::text   AS matched,
				       (SELECT count(*) FROM del)::text AS deleted
			`,
			30000,
			"delete website search_history",
		);

		const [left] = await withTimeout(
			sql<{ n: string }[]>`
				WITH v AS (
					SELECT DISTINCT type, normalized_value
					FROM jsonb_to_recordset(${JSON.stringify(pairs)}::text::jsonb)
						AS v(type text, normalized_value text)
				)
				SELECT count(*)::text AS n
				FROM public.search_history s
				JOIN v ON v.type = s.type
				      AND v.normalized_value = CASE s.type
				          WHEN 'phone' THEN regexp_replace(s.value, '\D', '', 'g')
				          ELSE lower(regexp_replace(s.value, '^\s+|\s+$', '', 'g'))
				      END
			`,
			30000,
			"count website search_history",
		);

		return {
			matched: Number(r?.matched ?? 0),
			deleted: Number(r?.deleted ?? 0),
			remaining: Number(left?.n ?? 0),
		};
	} finally {
		await closeQuietly(sql);
	}
}
