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

import { logEvent, version } from "./logs";

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
	const out: Record<string, unknown> = { version: version(env), host, port, stages };
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

		// 4. Postgres startup, over TLS. A StartupMessage carries the user and
		//    database but no password, so this stays credential-free: we only
		//    read what the server asks for. 'R' = authentication request (the
		//    server is speaking Postgres), 'E' = error (e.g. Supavisor saying
		//    the tenant or user is unknown, which is a useful answer too).
		t = Date.now();
		const user = url.searchParams.get("user") ?? "drop_workflow.vsqxnmrvvjsgcrudpruy";
		const params = new TextEncoder().encode(`user\0${user}\0database\0postgres\0\0`);
		const startup = new Uint8Array(8 + params.length);
		const dv = new DataView(startup.buffer);
		dv.setInt32(0, startup.length);
		dv.setInt32(4, 196608); // protocol 3.0
		startup.set(params, 8);

		const w2 = secure.writable.getWriter();
		await timeout(w2.write(startup), 5000, "write StartupMessage");
		w2.releaseLock();

		const r2 = secure.readable.getReader();
		const reply = await timeout(r2.read(), 8000, "read startup reply");
		const tag = reply.value ? String.fromCharCode(reply.value[0]) : "(nothing)";
		const text = reply.value
			? new TextDecoder().decode(reply.value.subarray(0, 200)).replace(/[^\x20-\x7e]+/g, " ").trim()
			: "";
		r2.releaseLock();
		stages.push({
			stage: "startup",
			ms: Date.now() - t,
			ok: tag === "R",
			detail: `server tag "${tag}" (R = asks to authenticate, E = error) · ${text}`,
		});

		out.ok = tag === "R";
		out.conclusion =
			tag === "R"
				? "the server completes TLS and asks for authentication — the network, the host, the port and the user routing are all fine, so a hang past this point is the driver"
				: "TLS is fine but the server did not ask for authentication: read the tag and text above";
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

	await logEvent(env, "socket-test", out.ok === true, {
		error: stages.find((x) => !x.ok)?.detail,
		result: Object.fromEntries(stages.map((x) => [`${x.stage}_ms`, x.ms])),
	});
	return Response.json(out);
}
