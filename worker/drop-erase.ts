import { writeMatchLog } from "./audit";
import { deleteEntityRows } from "./ch";
import type { EntityKey } from "./ch";
import { incompleteReason, markMatchesDeleted, recordMatches } from "./db";
import type { MatchRow } from "./db";
import { logMatches, phaseTracer, renderMatchLog } from "./logs";
import type { MatchAlert } from "./logs";
import type { DropHit } from "./drop-report";

export const ERASE_WORKFLOW = "drop-report-check";

const BATCH = 1;

export const ERASE_STAGES = ["record", "alert", "erase", "status", "audit"] as const;
export type EraseStage = (typeof ERASE_STAGES)[number];

export type EraseResult = {
	matchRowsInserted: number;
	workItemsMarked: number;
	rowsErased: number;
	auditKey: string;
};

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
			auditKey,
		};
	} catch (e) {
		if (e instanceof EraseFailed) throw e;
		throw new EraseFailed(stage, e instanceof Error ? `${e.name}: ${e.message}` : String(e), rowsErased);
	}
}
