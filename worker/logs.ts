/**
 * Step-level logging to Better Stack.
 *
 * Every step reports three things: that it started, and then either that it
 * completed (with a duration) or that it threw (with the message). A step that
 * Workflows retries logs one started/failed pair per attempt, so the retry
 * history is visible in the logs and not only in the dashboard.
 *
 * Ingest is a plain POST of JSON — no SDK:
 *   https://<ingesting-host>/   with   Authorization: Bearer <source token>
 * The host is not a secret and lives in wrangler.jsonc as LOGS_HOST; the token
 * is a secret:
 *   npx wrangler secret put LOGS_TOKEN
 *
 * Logging never breaks a run: every failure to ship a log is swallowed. A
 * telemetry outage must not fail a compliance job.
 *
 * Nothing identifying is ever shipped. Step return values are reduced to
 * their numeric fields (counts, page numbers), so hashes, e-mails, phone
 * numbers and cursors cannot leak into the log stream.
 */

import type { WorkflowStep, WorkflowStepConfig } from "cloudflare:workers";

type Phase = "started" | "completed" | "failed";

type Context = {
	workflow: string;
	run_id: string;
};

type Entry = Context & {
	dt: string;
	level: "info" | "error";
	message: string;
	step: string;
	phase: Phase;
	attempt_duration_ms?: number;
	result?: Record<string, number>;
	error?: string;
};

function configured(env: Env): boolean {
	// A LOGS_HOST left as the <sXXXX...> placeholder is treated as unset, so it
	// fails quietly instead of throwing on every step.
	return Boolean(env.LOGS_HOST && !/[<>]/.test(env.LOGS_HOST) && env.LOGS_TOKEN);
}

async function ship(env: Env, entry: Entry): Promise<void> {
	if (!configured(env)) return;
	try {
		await fetch(`https://${env.LOGS_HOST}/`, {
			method: "POST",
			headers: {
				Authorization: `Bearer ${env.LOGS_TOKEN}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify(entry),
		});
	} catch {
		// Telemetry is not allowed to fail the run.
	}
}

/** Only numbers survive, so no identifier can reach the log stream. */
function numericOnly(value: unknown): Record<string, number> | undefined {
	if (typeof value === "number") return { value };
	if (!value || typeof value !== "object") return undefined;
	const out: Record<string, number> = {};
	for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
		if (typeof v === "number") out[k] = v;
		else if (v && typeof v === "object") {
			for (const [k2, v2] of Object.entries(v as Record<string, unknown>)) {
				if (typeof v2 === "number") out[`${k}.${k2}`] = v2;
			}
		}
	}
	return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Wraps step.do. Same call shapes as the original — (name, fn) and
 * (name, config, fn) — so it is a drop-in replacement.
 */
export function tracer(env: Env, step: WorkflowStep, ctx: Context) {
	return async function tracedStep<T extends Rpc.Serializable<T>>(
		name: string,
		configOrBody: WorkflowStepConfig | (() => Promise<T>),
		maybeBody?: () => Promise<T>,
	): Promise<T> {
		const config = typeof configOrBody === "function" ? undefined : configOrBody;
		const body = (typeof configOrBody === "function" ? configOrBody : maybeBody) as () => Promise<T>;

		const traced = async (): Promise<T> => {
			const startedAt = Date.now();
			await ship(env, {
				...ctx,
				dt: new Date().toISOString(),
				level: "info",
				message: `step started: ${name}`,
				step: name,
				phase: "started",
			});

			try {
				const result = await body();
				await ship(env, {
					...ctx,
					dt: new Date().toISOString(),
					level: "info",
					message: `step completed: ${name}`,
					step: name,
					phase: "completed",
					attempt_duration_ms: Date.now() - startedAt,
					result: numericOnly(result),
				});
				return result;
			} catch (e) {
				await ship(env, {
					...ctx,
					dt: new Date().toISOString(),
					level: "error",
					message: `step failed: ${name}`,
					step: name,
					phase: "failed",
					attempt_duration_ms: Date.now() - startedAt,
					error: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
				});
				throw e;
			}
		};

		return config ? step.do(name, config, traced) : step.do(name, traced);
	};
}

/** Run-level bookend, so a run that dies between steps is still visible. */
export async function logRun(
	env: Env,
	ctx: Context,
	phase: "started" | "completed" | "failed",
	extra?: { error?: string; result?: unknown; duration_ms?: number },
): Promise<void> {
	await ship(env, {
		...ctx,
		dt: new Date().toISOString(),
		level: phase === "failed" ? "error" : "info",
		message: `run ${phase}: ${ctx.workflow}`,
		step: "(run)",
		phase: phase === "started" ? "started" : phase === "completed" ? "completed" : "failed",
		attempt_duration_ms: extra?.duration_ms,
		result: numericOnly(extra?.result),
		error: extra?.error,
	});
}

/**
 * Ships one entry and reports what Better Stack said. GET /api/logs-test
 * Unlike the workflow path, this one does NOT swallow errors — the point is
 * to see them.
 */
export async function logsSmokeTest(env: Env): Promise<Response> {
	const out: Record<string, unknown> = {
		host: env.LOGS_HOST ?? null,
		token: env.LOGS_TOKEN ? "set" : "missing",
		configured: configured(env),
	};
	if (!configured(env)) {
		out.hint =
			"set LOGS_HOST in wrangler.jsonc to the source's ingesting host " +
			"(s<id>.<region>.betterstackdata.com) and LOGS_TOKEN as a secret, then redeploy";
		return Response.json(out);
	}
	try {
		const res = await fetch(`https://${env.LOGS_HOST}/`, {
			method: "POST",
			headers: {
				Authorization: `Bearer ${env.LOGS_TOKEN}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify({
				dt: new Date().toISOString(),
				level: "info",
				message: "logs-test from the Worker",
				workflow: "(smoke test)",
				step: "(none)",
				phase: "completed",
			}),
		});
		out.status = res.status;
		out.body = (await res.text()).slice(0, 300);
		out.ok = res.ok;
	} catch (e) {
		out.ok = false;
		out.error = String(e);
	}
	return Response.json(out);
}
