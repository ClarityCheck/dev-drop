/**
 * Who may start Cron A, Cron B and Cron C.
 *
 * The three are started by hand, never on a schedule, and only by an operator
 * holding DROP_OPERATOR_TOKEN:
 *
 *   Authorization: Bearer <DROP_OPERATOR_TOKEN>
 *
 * Its own secret, separate from the token the Lookup API sends: the API has
 * no reason to start a workflow, and a leaked API token should not be able to.
 *
 * Fails closed. With the secret unset nobody can start anything, rather than
 * everybody.
 *
 *   npx wrangler secret put DROP_OPERATOR_TOKEN
 */
export async function requireOperator(request: Request, env: Env): Promise<Response | null> {
	const expected = env.DROP_OPERATOR_TOKEN;
	if (!expected) {
		return Response.json(
			{ error: "starting a workflow is disabled: DROP_OPERATOR_TOKEN is not set" },
			{ status: 503 },
		);
	}

	const header = request.headers.get("Authorization") ?? "";
	const given = header.startsWith("Bearer ") ? header.slice("Bearer ".length).trim() : "";

	if (given === "" || !(await sameSecret(given, expected))) {
		return Response.json(
			{ error: "unauthorized: send Authorization: Bearer <DROP_OPERATOR_TOKEN>" },
			{ status: 401, headers: { "WWW-Authenticate": 'Bearer realm="drop-worker"' } },
		);
	}

	return null;
}

/**
 * Constant-time comparison. timingSafeEqual needs equal lengths, so both
 * sides are hashed first — which also keeps the length of the secret from
 * leaking through an early return.
 */
async function sameSecret(a: string, b: string): Promise<boolean> {
	const encoder = new TextEncoder();
	const [ha, hb] = await Promise.all([
		crypto.subtle.digest("SHA-256", encoder.encode(a)),
		crypto.subtle.digest("SHA-256", encoder.encode(b)),
	]);
	return crypto.subtle.timingSafeEqual(ha, hb);
}
