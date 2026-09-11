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
 * One client per step. `prepare: false` because the connection is not
 * exclusively ours between statements; `fetch_types: false` saves a round trip
 * on connect. Hyperdrive keeps the real pool, so this is cheap.
 */
/**
 * Everything that can be wrong with the connection string, said plainly.
 * Without this, a placeholder left in the secret surfaces as
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
				"Use the shared pooler in session mode instead (aws-N-<region>.pooler.supabase.com:5432). " +
				"The direct host is what the Hyperdrive config wants, not the Worker.",
		);
	}
	if (u.hostname.endsWith(".pooler.supabase.com") && !u.username.includes(".")) {
		throw new Error(
			`the pooler needs the username as <role>.<project-ref>, got "${u.username}"`,
		);
	}
}

function connect(env: Env) {
	const url = connectionString(env);
	if (!url) throw new Error("no database: add the DROP_DB Hyperdrive binding or set SUPABASE_DB_URL");
	checkUrl(url);
	return postgres(url, {
		max: 1,
		idle_timeout: 10,
		connect_timeout: 15,
		prepare: false,
		fetch_types: false,
		// Supabase refuses a plaintext connection ("ESSLREQUIRED: SSL connection
		// is required"), and postgres.js defaults to no SSL. Forced here rather
		// than left to ?sslmode= in the connection string, so a string pasted
		// without it still works.
		//
		// "require" encrypts but does not verify the server's certificate: a
		// Worker socket cannot be given a CA bundle. That is the gap Hyperdrive
		// closes with --sslmode verify-full, and the reason this driver path is
		// the fallback rather than the destination.
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
		await sql.end({ timeout: 5 });
	}
}

/** Connectivity + privilege check. GET /api/db-test */
export async function dbSmokeTest(env: Env): Promise<Response> {
	const out: Record<string, unknown> = {
		configured: hasDb(env),
		via: (env as unknown as { DROP_DB?: unknown }).DROP_DB ? "hyperdrive" : "SUPABASE_DB_URL",
		target: describe(env),
	};
	out.logging = {
		host: env.LOGS_HOST && !/[<>]/.test(env.LOGS_HOST) ? env.LOGS_HOST : "not configured",
		token: env.LOGS_TOKEN ? "set" : "missing",
	};
	if (!hasDb(env)) {
		out.hint = "create the Hyperdrive config, or set SUPABASE_DB_URL as a secret";
		return Response.json(out);
	}

	const url = connectionString(env);
	// Everything from here is inside one try: connect() and postgres() can both
	// throw synchronously, and a throw that escapes turns this into a bare 500
	// with no body — which is the one thing a diagnostic must never do.
	let sql: ReturnType<typeof connect> | undefined;
	try {
		if (url) checkUrl(url);
		sql = connect(env);

		const [who] = await withTimeout(
			sql`SELECT current_user AS role, current_database() AS db`,
			20000,
			"connect",
		);
		out.identity = who;

		const [rows] = await withTimeout(
			sql`SELECT count(*)::int AS n FROM public.ca_drop_work_item`,
			20000,
			"count ca_drop_work_item",
		);
		out.work_item_rows = rows.n;

		// The grant should reach exactly one table. Anything higher means the
		// role can see tables it has no business seeing.
		const [reach] = await withTimeout(
			sql`
				SELECT count(DISTINCT table_name)::int AS n
				FROM information_schema.table_privileges
				WHERE grantee = current_user AND privilege_type = 'SELECT'
			`,
			20000,
			"privilege check",
		);
		out.selectable_tables = reach.n;

		out.ok = true;
	} catch (e) {
		out.ok = false;
		out.error = e instanceof Error ? `${e.name}: ${e.message}` : String(e);
	} finally {
		try {
			await sql?.end({ timeout: 5 });
		} catch {
			// closing a connection that never opened is not worth reporting
		}
	}
	return Response.json(out);
}
