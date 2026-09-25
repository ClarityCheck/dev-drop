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

type Phase = "started" | "completed" | "failed" | "waiting" | "match";

/**
 * Thrown by a step that is waiting for something, not failing at it.
 *
 * Workflows has no "not ready, ask me again" signal — a step that needs to
 * poll says so by throwing, and the retry policy turns that into the next
 * attempt. So the throw is the mechanism, and on the happy path a step like
 * "await refresh" throws several times before it succeeds. Reporting those
 * as errors means a healthy run fills the log with failures and any alert
 * built on level=error fires on nothing at all.
 *
 * The tracer logs these at info with phase "waiting" instead. A step that
 * exhausts its retries still surfaces: the run-level bookend records the run
 * as failed, at error, with the message that finally got through.
 */
export class Pending extends Error {
	readonly pending = true;
	constructor(message: string) {
		super(message);
		this.name = "Pending";
	}
}

function isPending(e: unknown): boolean {
	return e instanceof Pending || (typeof e === "object" && e !== null && "pending" in e);
}

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
	/** which call inside a step — see phaseTracer */
	sub_phase?: string;
	/** error detail from describeError; shape depends on what threw */
	[key: string]: unknown;
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
				const waiting = isPending(e);
				await ship(env, {
					...ctx,
					dt: new Date().toISOString(),
					level: waiting ? "info" : "error",
					message: waiting ? `step waiting: ${name}` : `step failed: ${name}`,
					step: name,
					phase: waiting ? "waiting" : "failed",
					attempt_duration_ms: Date.now() - startedAt,
					// On a waiting attempt this is why it is still waiting, not a
					// fault — "refresh not finished yet (status Running)". A real
					// failure gets the stack and whatever fields the thrower hung
					// off the error, which is usually where the cause actually is.
					...(waiting
						? { error: e instanceof Error ? `${e.name}: ${e.message}` : String(e) }
						: describeError(e)),
				});
				throw e;
			}
		};

		return config ? step.do(name, config, traced) : step.do(name, traced);
	};
}

/**
 * Everything an error is carrying, not just its message.
 *
 * `${e.name}: ${e.message}` throws away the two things that usually identify
 * the cause: the stack, and the fields the thrower hung off the error.
 * postgres.js puts the SQLSTATE in `code` and the server's own explanation in
 * `severity` / `detail` / `hint` / `constraint_name`; workerd marks a
 * cancelled I/O with `retryable` and `remote`. Without them, a Postgres
 * constraint violation and a dead socket both read as one line of prose.
 *
 * `parameters` is deliberately never copied: for this workflow it holds the
 * matched identifiers, and the log stream is not where those belong. The SQL
 * text is safe — it is parameterised — and is truncated only for length.
 */
export function describeError(e: unknown): Record<string, unknown> {
	if (!(e instanceof Error)) {
		return { error: String(e), error_type: typeof e };
	}

	const out: Record<string, unknown> = {
		error: `${e.name}: ${e.message}`,
		error_name: e.name,
		error_message: e.message,
	};
	if (e.stack) out.error_stack = e.stack.split("\n").slice(0, 12).join("\n");
	if (e.cause) {
		out.error_cause =
			e.cause instanceof Error ? `${e.cause.name}: ${e.cause.message}` : String(e.cause);
	}

	const carried = [
		"code", "errno", "syscall", "severity", "detail", "hint", "routine",
		"where", "schema_name", "table_name", "constraint_name", "column_name",
		"position", "retryable", "remote", "durable",
	];
	for (const k of carried) {
		const v = (e as unknown as Record<string, unknown>)[k];
		if (v !== undefined && v !== null) out[`error_${k}`] = v;
	}
	const q = (e as unknown as Record<string, unknown>).query;
	if (typeof q === "string") out.error_query = q.replace(/\s+/g, " ").slice(0, 300);

	return out;
}

/**
 * Wraps the individual calls inside a step, so a failure names which one.
 *
 * A step like "record and erase" touches ClickHouse twice, Postgres twice,
 * Better Stack and R2. When the Workflow reports that the step failed, it says
 * nothing about which of the six died — and "Network connection lost" at 1.8
 * seconds could be any of them.
 *
 * Both the start and the end of each call are shipped, and the start matters
 * more than it looks. If the isolate is torn down mid-call — which is exactly
 * what a lost connection or an exceeded limit does — the failure log never
 * leaves the Worker. What survives is the last `phase started` with no
 * matching end, and that alone identifies the call.
 */
export function phaseTracer(env: Env, ctx: Context, stepName: string) {
	return async function phase<T>(name: string, fn: () => Promise<T>): Promise<T> {
		const startedAt = Date.now();
		console.log(`${stepName} > ${name}`);
		await ship(env, {
			...ctx,
			dt: new Date().toISOString(),
			level: "info",
			message: `phase started: ${name}`,
			step: stepName,
			phase: "started",
			sub_phase: name,
		});

		try {
			const out = await fn();
			const ms = Date.now() - startedAt;
			console.log(`${stepName} ok ${name} (${ms}ms)`);
			await ship(env, {
				...ctx,
				dt: new Date().toISOString(),
				level: "info",
				message: `phase ok: ${name}`,
				step: stepName,
				phase: "completed",
				sub_phase: name,
				attempt_duration_ms: ms,
			});
			return out;
		} catch (e) {
			const ms = Date.now() - startedAt;
			const detail = describeError(e);
			console.error(`${stepName} FAILED ${name} (${ms}ms)`, detail);
			await ship(env, {
				...ctx,
				dt: new Date().toISOString(),
				level: "error",
				message: `phase failed: ${name}`,
				step: stepName,
				phase: "failed",
				sub_phase: name,
				attempt_duration_ms: ms,
				...detail,
			});
			// Re-thrown, not wrapped: NonRetryableError has to stay one.
			if (e instanceof Error) e.message = `[${name}] ${e.message}`;
			throw e;
		}
	};
}

/** How many matches the Better Stack entry spells out before it summarises. */
const MATCHES_IN_TEXT = 50;

/**
 * The match log, as plain text.
 *
 * `limit` caps how many matches are spelled out — Better Stack gets a capped
 * view because a log line is not a bulk transport, R2 gets all of them
 * because it is the durable record.
 */
export function renderMatchLog(
	ctx: Context,
	batch: number,
	matches: MatchAlert[],
	at: Date,
	outcome: MatchOutcome,
	limit = Number.POSITIVE_INFINITY,
): string {
	const shown = Number.isFinite(limit) ? matches.slice(0, limit) : matches;
	const lines = [
		`CA DROP suppression match`,
		`at: ${at.toISOString()}`,
		`workflow: ${ctx.workflow}`,
		`run_id: ${ctx.run_id}`,
		`batch: ${batch}`,
		`matches: ${matches.length}`,
		`recorded: ${outcome.ok ? "yes" : `NO — ${outcome.reason}`}`,
		``,
		...shown.map((m) => `  ${m.list_type}\t${m.work_item_id}\t${m.hash}`),
	];
	if (matches.length > shown.length) {
		lines.push(`  ... and ${matches.length - shown.length} more`);
	}
	return lines.join("\n");
}

/** Whether the batch's matches actually made it into Supabase. */
export type MatchOutcome = { ok: true } | { ok: false; reason: string };

/**
 * The match alert.
 *
 * A match that WAS recorded goes out at `warn`: the pipeline is working
 * exactly as intended, but it is the one event here someone may want to see
 * without going looking for it.
 *
 * A match that was NOT recorded goes out at `error`, because it is the worst
 * thing this workflow can do. A consumer on California's delete list was
 * found in our data and the fact was then lost — which looks, from every
 * report downstream, identical to never having found them at all.
 *
 * Returns the text so the caller can put it in R2 and in the run summary.
 */
export async function logMatches(
	env: Env,
	ctx: Context,
	batch: number,
	matches: MatchAlert[],
	outcome: MatchOutcome,
): Promise<string> {
	const at = new Date();
	const text = renderMatchLog(ctx, batch, matches, at, outcome, MATCHES_IN_TEXT);

	await ship(env, {
		...ctx,
		dt: at.toISOString(),
		level: outcome.ok ? "warn" : "error",
		message: outcome.ok
			? `DROP match: ${matches.length} work item(s) found and recorded (batch ${batch})`
			: `DROP match NOT RECORDED: ${matches.length} work item(s) found in batch ${batch} — ${outcome.reason}`,
		step: `scan · batch ${batch}`,
		phase: outcome.ok ? "match" : "failed",
		text,
		// Capped for the same reason the text block is: a single batch can
		// match thousands of items and a log line is not a bulk transport.
		// The complete set is in R2, and in public.ca_drop_work_item_match.
		matched: matches.slice(0, MATCHES_IN_TEXT),
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
	extra?: {
		error?: string;
		result?: unknown;
		duration_ms?: number;
		failedAtBatch?: number;
	} & Record<string, unknown>,
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
		batch: extra?.failedAtBatch,
		// Whatever describeError produced — stack, SQLSTATE, the server's own
		// explanation — rather than one line of prose.
		...Object.fromEntries(
			Object.entries(extra ?? {}).filter(
				([k]) => k === "error" || k.startsWith("error_"),
			),
		),
	});
}

/**
 * KV has drifted from Supabase.
 *
 * Its own entry rather than a field on the summary, because the summary is
 * shipped with only its numeric fields and this needs to say what to do about
 * it. Error when KV is short — that is the gate failing to suppress someone
 * who asked to be deleted — and warn when it is long, which only means stale
 * entries suppressing someone who should have got a result.
 */
export async function logUncheckedReports(env: Env, ctx: Context, reports: number): Promise<void> {
	await ship(env, {
		...ctx,
		dt: new Date().toISOString(),
		level: "error",
		message:
			`${reports} stored report(s) yield more than 20,000 DROP keys, so their NDZ and NameVIN ` +
			"keys were not derived and a listed consumer in them cannot be found. Cron B will not " +
			"report Not found while this is non-zero.",
		step: "count unchecked reports",
		phase: "match",
		result: { unchecked_reports: reports },
	});
}

export async function logKvDrift(
	env: Env,
	ctx: Context,
	d: { level: "warn" | "error"; expected: number; inKv: number; message: string },
): Promise<void> {
	await ship(env, {
		...ctx,
		dt: new Date().toISOString(),
		level: d.level,
		message: d.message,
		step: "check DROP set is complete",
		phase: d.level === "error" ? "failed" : "match",
		result: {
			work_items_in_supabase: d.expected,
			hashes_in_kv: d.inKv,
			shortfall: d.expected - d.inKv,
		},
	});
}
