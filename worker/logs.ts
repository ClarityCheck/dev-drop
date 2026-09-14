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

type Phase = "started" | "completed" | "failed" | "match";

type Context = {
	workflow: string;
	run_id: string;
};

type Entry = Context & {
	dt: string;
	level: "info" | "warn" | "error";
	message: string;
	step: string;
	phase: Phase;
	attempt_duration_ms?: number;
	result?: Record<string, number>;
	error?: string;
	/** Match alerts only — see logMatches. */
	text?: string;
	matched?: MatchAlert[];
	match_count?: number;
	batch?: number;
};

/**
 * One matched work item, as it is allowed to appear in a log.
 *
 * The matched normalized_value — the e-mail address or phone number itself —
 * is deliberately absent. It is consumer PII, it is what the record is keyed
 * on, and the log stream is not where it belongs; the durable link lives in
 * public.ca_drop_work_item_match instead. work_item_id and hash are DROP's own
 * published identifiers and carry no plaintext, so they may be logged.
 */
export type MatchAlert = {
	list_type: string;
	work_item_id: string;
	hash: string;
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

/** How many matches the text log spells out before it summarises the rest. */
const MATCHES_IN_TEXT = 50;

/**
 * The match alert. A DROP match means a consumer on California's delete list
 * is present in our data, so it is reported at `warn` — it is not an error
 * (the pipeline is working exactly as intended) but it is the one thing in
 * this workflow a human may want to see without going looking for it.
 *
 * Shipped as both a rendered text block (`text`, for reading in the Better
 * Stack UI) and structured fields (`matched`, for querying and alerting on).
 * Returns the text so the caller can put it in the run summary.
 */
export async function logMatches(
	env: Env,
	ctx: Context,
	batch: number,
	matches: MatchAlert[],
): Promise<string> {
	const at = new Date();
	const shown = matches.slice(0, MATCHES_IN_TEXT);
	const lines = [
		`CA DROP suppression match`,
		`at: ${at.toISOString()}`,
		`workflow: ${ctx.workflow}`,
		`run_id: ${ctx.run_id}`,
		`batch: ${batch}`,
		`matches: ${matches.length}`,
		``,
		...shown.map((m) => `  ${m.list_type}\t${m.work_item_id}\t${m.hash}`),
	];
	if (matches.length > shown.length) {
		lines.push(`  ... and ${matches.length - shown.length} more`);
	}
	const text = lines.join("\n");

	await ship(env, {
		...ctx,
		dt: at.toISOString(),
		level: "warn",
		message: `DROP match: ${matches.length} work item(s) found in ClickHouse (batch ${batch})`,
		step: `scan · batch ${batch}`,
		phase: "match",
		text,
		// Capped for the same reason the text block is: a single batch can
		// match thousands of items and a log line is not a bulk transport.
		// The complete set is in public.ca_drop_work_item_match.
		matched: shown,
		match_count: matches.length,
		batch,
	});

	return text;
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
