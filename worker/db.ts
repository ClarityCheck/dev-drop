/**
 * Supabase access for the Workflows, over PostgREST.
 *
 * There is no Postgres driver in this Worker (the npm registry is not
 * reachable from the build environment), so Supabase is reached over HTTPS
 * the same way ClickHouse is.
 *
 * The token is signed here with the project's JWT secret and carries
 * {"role":"drop_workflow"}, so the grants on that role are the actual ceiling.
 * The service_role key is deliberately not used: it bypasses RLS and every
 * grant, which would make the role pointless.
 *
 * Requires, in Supabase: schema ca_drop added to Project settings → API →
 * Exposed schemas, and GRANT drop_workflow TO authenticator.
 */

const SCHEMA = "ca_drop";
const ROLE = "drop_workflow";

export function hasSupabase(env: Env): boolean {
	return Boolean(env.SUPABASE_URL && env.SUPABASE_ANON_KEY && env.SUPABASE_JWT_SECRET);
}

function b64url(bytes: Uint8Array): string {
	let s = "";
	for (const b of bytes) s += String.fromCharCode(b);
	return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** Short-lived HS256 token for the drop_workflow role. */
async function roleToken(env: Env): Promise<string> {
	const now = Math.floor(Date.now() / 1000);
	const header = b64url(new TextEncoder().encode(JSON.stringify({ alg: "HS256", typ: "JWT" })));
	const payload = b64url(
		new TextEncoder().encode(JSON.stringify({ role: ROLE, iat: now, exp: now + 300 })),
	);
	const key = await crypto.subtle.importKey(
		"raw",
		new TextEncoder().encode(env.SUPABASE_JWT_SECRET),
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"],
	);
	const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(`${header}.${payload}`));
	return `${header}.${payload}.${b64url(new Uint8Array(sig))}`;
}

async function headers(env: Env, extra: Record<string, string> = {}): Promise<HeadersInit> {
	return {
		apikey: env.SUPABASE_ANON_KEY,
		Authorization: `Bearer ${await roleToken(env)}`,
		"Content-Type": "application/json",
		"Content-Profile": SCHEMA,
		"Accept-Profile": SCHEMA,
		...extra,
	};
}

/** INSERT ... ON CONFLICT DO UPDATE. Returns the number of rows sent. */
export async function sbUpsert(
	env: Env,
	table: string,
	rows: object[],
	onConflict: string,
): Promise<number> {
	if (rows.length === 0) return 0;
	const url = `${env.SUPABASE_URL}/rest/v1/${table}?on_conflict=${encodeURIComponent(onConflict)}`;
	const res = await fetch(url, {
		method: "POST",
		headers: await headers(env, { Prefer: "resolution=merge-duplicates,return=minimal" }),
		body: JSON.stringify(rows),
	});
	if (!res.ok) {
		throw new Error(`Supabase upsert ${table} ${res.status}: ${(await res.text()).slice(0, 400)}`);
	}
	return rows.length;
}

/** PATCH with a PostgREST filter, e.g. { list_type: "eq.phone" }. */
export async function sbUpdate(
	env: Env,
	table: string,
	filter: Record<string, string>,
	patch: object,
): Promise<void> {
	const qs = Object.entries(filter)
		.map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
		.join("&");
	const res = await fetch(`${env.SUPABASE_URL}/rest/v1/${table}?${qs}`, {
		method: "PATCH",
		headers: await headers(env, { Prefer: "return=minimal" }),
		body: JSON.stringify(patch),
	});
	if (!res.ok) {
		throw new Error(`Supabase update ${table} ${res.status}: ${(await res.text()).slice(0, 400)}`);
	}
}

/** Connectivity + privilege check. GET /api/sb-test */
export async function sbSmokeTest(env: Env): Promise<Response> {
	const out: Record<string, unknown> = { configured: hasSupabase(env) };
	if (!hasSupabase(env)) {
		out.hint = "set SUPABASE_URL, SUPABASE_ANON_KEY and SUPABASE_JWT_SECRET as secrets";
		return Response.json(out, { status: 500 });
	}
	try {
		const res = await fetch(
			`${env.SUPABASE_URL}/rest/v1/work_item?select=id&limit=1`,
			{ headers: await headers(env) },
		);
		out.status = res.status;
		out.body = (await res.text()).slice(0, 300);
		out.ok = res.ok;
	} catch (e) {
		out.ok = false;
		out.error = String(e);
	}
	return Response.json(out, { status: out.ok ? 200 : 500 });
}
