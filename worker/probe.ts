/**
 * Raw socket probe, to tell a network problem from a driver problem.
 *
 * GET /api/socket-test            uses the host from SUPABASE_DB_URL
 * GET /api/socket-test?host=x&port=5432
 *
 * Three stages, each timed and each with its own timeout, so a hang is
 * attributable instead of showing up as "no response in 20s" from the driver:
 *
 *   1. tcp      — connect() resolves, i.e. the address is reachable at all
 *   2. sslreq   — Postgres SSLRequest is answered with 'S' (yes) or 'N' (no)
 *   3. starttls — the TLS upgrade completes
 *
 * The SSLRequest is eight bytes: Int32 length = 8, Int32 code = 80877103.
 * Nothing is authenticated here, so no credentials are sent and none are
 * needed — this probe cannot leak anything.
 */

type Stage = { stage: string; ms: number; ok: boolean; detail?: string };

function timeout<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	return Promise.race([
		p.finally(() => timer !== undefined && clearTimeout(timer)),
		new Promise<never>((_, reject) => {
			timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms);
		}),
	]);
}

export async function socketProbe(env: Env, url: URL): Promise<Response> {
	let host = url.searchParams.get("host") ?? "";
	const port = Number(url.searchParams.get("port") ?? 5432);

	if (!host) {
		const dsn = env.SUPABASE_DB_URL;
		if (!dsn) {
			return Response.json({
				ok: false,
				error: "pass ?host=… or set SUPABASE_DB_URL",
			});
		}
		try {
			host = new URL(dsn).hostname;
		} catch {
			return Response.json({ ok: false, error: "SUPABASE_DB_URL is not parseable" });
		}
	}

	const stages: Stage[] = [];
	const out: Record<string, unknown> = { host, port, stages };
	let socket: Socket | undefined;

	try {
		const { connect } = await import("cloudflare:sockets");

		// 1. TCP
		let t = Date.now();
		socket = connect(`${host}:${port}`, { secureTransport: "starttls", allowHalfOpen: false });
		const info = await timeout(socket.opened, 8000, "tcp connect");
		stages.push({
			stage: "tcp",
			ms: Date.now() - t,
			ok: true,
			detail: `${info.remoteAddress ?? "?"}`,
		});

		// 2. SSLRequest
		t = Date.now();
		const req = new Uint8Array(8);
		new DataView(req.buffer).setInt32(0, 8);
		new DataView(req.buffer).setInt32(4, 80877103);
		const writer = socket.writable.getWriter();
		await timeout(writer.write(req), 5000, "write SSLRequest");
		writer.releaseLock();

		const reader = socket.readable.getReader();
		const first = await timeout(reader.read(), 8000, "read SSLRequest reply");
		const answer = first.value ? String.fromCharCode(first.value[0]) : "(nothing)";
		reader.releaseLock();
		stages.push({
			stage: "sslreq",
			ms: Date.now() - t,
			ok: answer === "S",
			detail: `server answered "${answer}" (S = TLS accepted, N = refused)`,
		});

		if (answer !== "S") {
			out.ok = false;
			out.conclusion = "the server will not do TLS on this port — wrong port or wrong host";
			return Response.json(out);
		}

		// 3. TLS upgrade
		t = Date.now();
		const secure = socket.startTls();
		await timeout(secure.opened, 8000, "TLS handshake");
		stages.push({ stage: "starttls", ms: Date.now() - t, ok: true });
		socket = secure;

		out.ok = true;
		out.conclusion = "network and TLS are fine — anything failing above this is the driver or the credentials";
	} catch (e) {
		stages.push({
			stage: "failed",
			ms: 0,
			ok: false,
			detail: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
		});
		out.ok = false;
		out.conclusion =
			"stopped at the stage above. A timeout on 'tcp connect' means the address is " +
			"unreachable from a Worker (the usual cause is an IPv6-only host); a timeout " +
			"later means the port speaks something else.";
	} finally {
		try {
			await socket?.close();
		} catch {
			// nothing useful to report from closing a broken socket
		}
	}

	return Response.json(out);
}
