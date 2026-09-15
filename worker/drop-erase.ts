import { writeMatchLog } from "./audit";
import { countEntityRows, deleteEntityRows } from "./ch";
import type { EntityKey } from "./ch";
import { incompleteReason, markMatchesDeleted, recordMatches } from "./db";
import type { MatchRow } from "./db";
import { logMatches, phaseTracer, renderMatchLog } from "./logs";
import type { MatchAlert } from "./logs";
import type { DropHit } from "./drop-report";

export const ERASE_WORKFLOW = "drop-report-check";

const BATCH = 1;

export const DEFAULT_WAIT_MS = 30_000;
export const MAX_WAIT_MS = 60_000;
const POLL_MS = 1_000;

export const ERASE_STAGES = ["record", "alert", "appear", "erase", "status", "audit"] as const;
export type EraseStage = (typeof ERASE_STAGES)[number];

export type EraseResult = {
	matchRowsInserted: number;
	workItemsMarked: number;
	rowsErased: number;
	waitedMs: number;
	auditKey: string;
};

export type Appearance = { rows: number; waitedMs: number; polls: number };

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Wait for the report to exist in ClickHouse before erasing it.
 *
 * The lookup API does not write entity_search_results directly — it pushes onto
 * a Redis list that flushes at 100 rows or on a 10 second interval. A delete
 * issued the moment a match is found can therefore run BEFORE the insert, find
 * nothing, and report a clean erase; the row then lands seconds later and the
 * report is fetchable again from a history endpoint that reads ClickHouse.
 *
 * So the count is polled rather than trusted once. `count` is injected so the
 * loop can be tested without ClickHouse.
 */
export async function waitForEntityRows(
	count: () => Promise<number>,
	timeoutMs: number,
	pollMs: number = POLL_MS,
): Promise<Appearance> {
	const startedAt = Date.now();
	let polls = 0;
	let lastError: unknown;
	let sawZero = false;

	for (;;) {
		polls += 1;
		try {
			const rows = await count();
			if (rows > 0) return { rows, waitedMs: Date.now() - startedAt, polls };
			sawZero = true;
			lastError = undefined;
		} catch (e) {
			// A blip on the way to ClickHouse is not the same as an absent row, so
			// it is tolerated until the deadline and only then reported.
			lastError = e;
		}

		const waitedMs = Date.now() - startedAt;
		if (waitedMs + pollMs > timeoutMs) {
			// Only errors, never a clean answer — we do not know whether the row
			// is there, and must not proceed as though it is not.
			if (lastError !== undefined && !sawZero) throw lastError;
			return { rows: 0, waitedMs, polls };
		}
		await sleep(pollMs);
	}
}

export class EraseFailed extends Error {
	readonly stage: EraseStage;
	readonly rowsErased: number;

	constructor(stage: EraseStage, message: string, rowsErased: number) {
		super(message);
		this.name = "EraseFailed";
		this.stage = stage;
		this.rowsErased = rowsErased;
	}
}

export async function eraseMatchedReport(
	env: Env,
	runId: string,
	entity: EntityKey,
	hits: DropHit[],
	waitMs: number = DEFAULT_WAIT_MS,
): Promise<EraseResult> {
	const ctx = { workflow: ERASE_WORKFLOW, run_id: runId };
	const phase = phaseTracer(env, ctx, "erase matched report");

	const alerts: MatchAlert[] = hits.map((hit) => ({
		list_type: hit.list_type,
		work_item_id: hit.work_item_id,
		hash: hit.hash,
	}));
	const matchRows: MatchRow[] = hits.map((hit) => ({
		list_type: hit.list_type,
		work_item_id: hit.work_item_id,
		matched_normalized_value: entity.normalized_value,
	}));

	let stage: EraseStage = "record";
	let rowsErased = 0;
	let waitedMs = 0;

	try {
		const written = await phase("supabase: insert match rows", () =>
			recordMatches(env, matchRows),
		);
		const unrecorded = incompleteReason(written);
		if (unrecorded) {
			await logMatches(env, ctx, BATCH, alerts, { ok: false, reason: unrecorded });
			throw new EraseFailed(
				"record",
				`found ${matchRows.length} DROP match(es) but could not record them — ${unrecorded}`,
				0,
			);
		}

		stage = "alert";
		await phase("betterstack: match alert", () =>
			logMatches(env, ctx, BATCH, alerts, { ok: true }),
		);

		stage = "appear";
		const appeared = await phase("clickhouse: wait for the report to land", () =>
			waitForEntityRows(() => countEntityRows(env, [entity]), waitMs),
		);
		waitedMs = appeared.waitedMs;
		if (appeared.rows === 0) {
			const why =
				`no entity_search_results row for this identifier after ${appeared.waitedMs}ms ` +
				`(${appeared.polls} polls) — the lookup API's write buffer may not have flushed, ` +
				`so nothing can be confirmed erased`;
			await logMatches(env, ctx, BATCH, alerts, { ok: false, reason: why });
			throw new EraseFailed("appear", why, 0);
		}

		stage = "erase";
		const expired = await phase("clickhouse: erase matched records", () =>
			deleteEntityRows(env, [entity]),
		);
		rowsErased = expired.before - expired.after;
		if (expired.after > 0) {
			const why = `${expired.after} of ${expired.before} entity_search_results row(s) survived the delete`;
			await logMatches(env, ctx, BATCH, alerts, { ok: false, reason: why });
			throw new EraseFailed("erase", why, rowsErased);
		}

		stage = "status";
		const statusSet = await phase("supabase: set status deleted", () =>
			markMatchesDeleted(env, written.workItemIds),
		);
		if (statusSet < written.workItems) {
			const why =
				`${written.workItems - statusSet} of ${written.workItems} work item(s) did not end up ` +
				`with status = 'deleted' after the records were erased`;
			await logMatches(env, ctx, BATCH, alerts, { ok: false, reason: why });
			throw new EraseFailed("status", why, rowsErased);
		}

		stage = "audit";
		const auditKey = await phase("r2: evidence file", () =>
			writeMatchLog(env, {
				run_id: runId,
				batch: BATCH,
				text: renderMatchLog(ctx, BATCH, alerts, new Date(), { ok: true }),
			}),
		);

		return {
			matchRowsInserted: written.inserted,
			workItemsMarked: statusSet,
			rowsErased,
			waitedMs,
			auditKey,
		};
	} catch (e) {
		if (e instanceof EraseFailed) throw e;
		throw new EraseFailed(
			stage,
			e instanceof Error ? `${e.name}: ${e.message}` : String(e),
			rowsErased,
		);
	}
}
