import { writeMatchLog } from "./audit";
import { countEntityRows } from "./ch";
import type { EntityKey } from "./ch";
import { incompleteReason, markMatchesDeleted, recordMatches } from "./db";
import type { MatchRow } from "./db";
import { logMatches, phaseTracer, renderMatchLog } from "./logs";
import type { MatchAlert } from "./logs";
import type { DropHit } from "./drop-report";

export const INCIDENT_WORKFLOW = "drop-erase-incident";

const BATCH = 1;

export const INCIDENT_STAGES = [
	"unverifiable",
	"verify",
	"record",
	"alert",
	"status",
	"audit",
] as const;
export type IncidentStage = (typeof INCIDENT_STAGES)[number];

export type IncidentResult = {
	matchRowsInserted: number;
	workItemsLinked: number;
	workItemsMarked: number;
	rowsRemaining: number;
	auditKey: string;
};

export class IncidentFailed extends Error {
	readonly stage: IncidentStage;
	readonly detail?: Record<string, unknown>;

	constructor(stage: IncidentStage, message: string, detail?: Record<string, unknown>) {
		super(message);
		this.name = "IncidentFailed";
		this.stage = stage;
		this.detail = detail;
	}
}

/**
 * Record a DROP erasure that somebody else performed.
 *
 * Cron C finds a match, erases the rows and records the fact, in that order
 * and for reasons set out in workflow-reports-cleanup.ts. This endpoint covers
 * the case Cron C is too slow for: a cached report, stored when its subject was
 * not on the DROP list, whose subject is on today's list. The lookup API
 * notices on the next search, deletes the rows itself — it owns the write path,
 * so it also owns the delete — and then calls this to leave the same trail Cron
 * C would have left.
 *
 * Nothing here touches ClickHouse except to count. The erase already happened.
 *
 * TWO THINGS ARE CHECKED RATHER THAN BELIEVED
 *
 * The caller asserts both that a match occurred and that the rows are gone, and
 * this writes a compliance record and sets the status Cron B reports to
 * California as code 3 Deleted. So the match is re-derived from the report
 * against KV, and the row count is read back from ClickHouse. A caller that is
 * wrong about either gets a refusal and nothing is written — a false "deleted"
 * is worse than a missing one, because a missing one is still discoverable.
 */
export async function recordEraseIncident(
	env: Env,
	runId: string,
	entity: EntityKey,
	hits: DropHit[],
): Promise<IncidentResult> {
	const ctx = { workflow: INCIDENT_WORKFLOW, run_id: runId };
	const phase = phaseTracer(env, ctx, "record erase incident");

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

	let stage: IncidentStage = "unverifiable";
	let rowsRemaining = 0;

	try {
		// Not knowing the count and knowing it is non-zero mean different things.
		// The first says the erasure cannot be confirmed; the second says it did
		// not happen. Reporting the first as the second would tell a caller that
		// did delete the rows to go and delete them again.
		try {
			rowsRemaining = await phase("clickhouse: confirm the rows are gone", () =>
				countEntityRows(env, [entity]),
			);
		} catch (e) {
			throw new IncidentFailed(
				"unverifiable",
				`could not read the row count from ClickHouse — ${e instanceof Error ? `${e.name}: ${e.message}` : String(e)}`,
			);
		}

		stage = "verify";
		if (rowsRemaining > 0) {
			throw new IncidentFailed(
				"verify",
				`${rowsRemaining} entity_search_results row(s) still exist for this identifier — ` +
					"delete them before recording the erasure",
				{ rowsRemaining },
			);
		}

		stage = "record";
		const written = await phase("supabase: insert match rows", () =>
			recordMatches(env, matchRows),
		);
		const unrecorded = incompleteReason(written);
		if (unrecorded) {
			await logMatches(env, ctx, BATCH, alerts, { ok: false, reason: unrecorded });
			throw new IncidentFailed(
				"record",
				`could not record ${matchRows.length} DROP match(es) — ${unrecorded}`,
			);
		}

		stage = "alert";
		await phase("betterstack: match alert", () =>
			logMatches(env, ctx, BATCH, alerts, { ok: true }),
		);

		stage = "status";
		const statusSet = await phase("supabase: set status deleted", () =>
			markMatchesDeleted(env, written.workItemIds),
		);
		if (statusSet < written.workItems) {
			const why =
				`${written.workItems - statusSet} of ${written.workItems} work item(s) did not end up ` +
				"with status = 'deleted'";
			await logMatches(env, ctx, BATCH, alerts, { ok: false, reason: why });
			throw new IncidentFailed("status", why);
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
			workItemsLinked: written.workItems,
			workItemsMarked: statusSet,
			rowsRemaining,
			auditKey,
		};
	} catch (e) {
		if (e instanceof IncidentFailed) throw e;
		throw new IncidentFailed(
			stage,
			e instanceof Error ? `${e.name}: ${e.message}` : String(e),
		);
	}
}
