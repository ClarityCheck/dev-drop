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
};

/** One DROP work item found in ClickHouse, as Cron C reports it. */
export type MatchRow = {
	/** DROP list the hash came from, per the KV metadata */
	list_type: string;
	/** DROP's own Id, case-sensitive */
	work_item_id: string;
	/** the identifier the hash matched — e-mail, phone, or the NDZ/NameVIN source value */
	matched_normalized_value: string;
};

/**
 * sql.end() waits for the connection to close, and on a half-open TLS socket
 * that wait does not finish — which is why db-test was answering a bare 500:
 * the handler had its result and was stuck in `finally`. Closing is
 * best-effort, and nothing downstream depends on it: Hyperdrive (or the
 * pooler) owns the real connection.
 */
async function closeQuietly(sql: { end: (o?: { timeout?: number }) => Promise<void> } | undefined) {
	if (!sql) return;
	try {
		await Promise.race([
			sql.end({ timeout: 1 }),
			new Promise<void>((resolve) => setTimeout(resolve, 3000)),
		]);
	} catch {
		// a connection that never opened has nothing worth reporting on close
	}
}

/** Nothing in workerd enforces postgres.js's own connect_timeout, so a socket
 *  that never establishes would hang the whole step until the Workflow's
 *  timeout. This turns that into a real error with a usable message. */
async function withTimeout<T>(work: Promise<T>, ms: number, what: string): Promise<T> {
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

	if (hyperdrive?.connectionString) {
		return postgres(hyperdrive.connectionString, {
			max: 5,
			fetch_types: false,
			prepare: true,
		});
	}

	const url = env.SUPABASE_DB_URL;
	if (!url) throw new Error("no database: add the DROP_DB Hyperdrive binding or set SUPABASE_DB_URL");
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
					${sql(rows, "list_type", "work_item_id", "hash", "request_date")}
				ON CONFLICT (list_type, work_item_id) DO UPDATE
					SET hash         = EXCLUDED.hash,
					    request_date = EXCLUDED.request_date
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
	/** rows Cron C handed over */
	submitted: number;
	/** dropped before the insert because the CHECK constraint would reject them */
	skippedEmpty: number;
	/** of those sent, the ones that resolved to a row in ca_drop_work_item */
	linked: number;
	/** of those, the ones that were not already recorded */
	inserted: number;
};

/**
 * Record the matches Cron C found.
 *
 * ca_drop_work_item_match references ca_drop_work_item by its bigint id, but
 * KV only knows DROP's own (list_type, work_item_id) — so the join happens
 * here, in one statement, rather than as a lookup per match.
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
 * DISTINCT ON collapses duplicates inside the batch, ON CONFLICT DO NOTHING
 * absorbs whatever a previous attempt already wrote.
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
		return { submitted: rows.length, skippedEmpty, linked: 0, inserted: 0 };
	}

	console.log(`db: recording ${usable.length} match(es) → ${describe(env)}`);
	const sql = connect(env);
	try {
		const [r] = await withTimeout(
			sql<{ submitted: string; linked: string; inserted: string }[]>`
				WITH v AS (
					SELECT *
					FROM jsonb_to_recordset(${JSON.stringify(usable)}::text::jsonb)
						AS v(list_type text, work_item_id text, matched_normalized_value text)
				),
				linked AS (
					SELECT w.id, v.matched_normalized_value
					FROM v
					JOIN public.ca_drop_work_item w
						ON w.list_type = v.list_type AND w.work_item_id = v.work_item_id
				),
				ins AS (
					INSERT INTO public.ca_drop_work_item_match
						(ca_drop_work_item_id, matched_normalized_value)
					SELECT DISTINCT ON (id) id, matched_normalized_value
					FROM linked
					ORDER BY id
					ON CONFLICT (ca_drop_work_item_id) DO NOTHING
					RETURNING 1
				)
				SELECT (SELECT count(*) FROM v)      AS submitted,
				       (SELECT count(*) FROM linked) AS linked,
				       (SELECT count(*) FROM ins)    AS inserted
			`,
			30000,
			"insert ca_drop_work_item_match",
		);

		return {
			submitted: Number(r?.submitted ?? 0) + skippedEmpty,
			skippedEmpty,
			linked: Number(r?.linked ?? 0),
			inserted: Number(r?.inserted ?? 0),
		};
	} finally {
		await closeQuietly(sql);
	}
}
