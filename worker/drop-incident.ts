import { writeMatchLog } from "./audit";
import { countEntityRows, fetchEntityPayloads } from "./ch";
import type { EntityKey } from "./ch";
import { incompleteReason, markMatchesDeleted, recordMatches } from "./db";
import type { MatchRow } from "./db";
import { logMatches, phaseTracer, renderMatchLog } from "./logs";
import type { MatchAlert } from "./logs";
import {
	MAX_REPORT_KEYS,
	buildReportKeys,
	countReportKeys,
	extractReportRecords,
	lookupDropKeys,
} from "./drop-report";
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
	recordsRemaining: number;
	auditKey: string;
};

export type Erasure = { rowsRemaining: number; recordsRemaining: number };

/**
 * Confirm that the caller's erasure actually happened.
 *
 * The two report shapes are erased differently, so they are verified
 * differently:
 *
 *   email / phone   the whole report is about one consumer, so every
 *                   entity_search_results row for the identifier goes. The
 *                   check is that the count is zero.
 *
 *   people          the report is an array of different people and only the
 *                   matched elements go, so the ROW SURVIVES and a count of
 *                   zero would be wrong. The check is on the contents: read
 *                   the stored payload back and re-run the per-record DROP
 *                   check over it. Nothing listed may remain.
 *
 * The people check is the stronger of the two. It verifies the state that
 * matters rather than a row count, and it catches the caller removing the
 * wrong element — which a count never could.
 */
export async function verifyErasure(env: Env, entity: EntityKey): Promise<Erasure> {
	const rowsRemaining = await countEntityRows(env, [entity]);

	if (entity.type !== "people") return { rowsRemaining, recordsRemaining: 0 };
	if (rowsRemaining === 0) return { rowsRemaining, recordsRemaining: 0 };

	const stored = await fetchEntityPayloads(env, [entity]);
	const recordsRemaining = await listedRecordsIn(
		env.kv,
		stored.map((row) => ({ label: row.provider, payload_json: row.payload_json })),
	);

	return { rowsRemaining, recordsRemaining };
}

/**
 * How many records across these stored payloads are still on the DROP list.
 *
 * Separate from verifyErasure and taking the payloads as an argument, because
 * this is the part with the judgement in it: it decides whether a caller
 * removed the right elements. A caller that deleted the wrong index leaves a
 * listed record behind and a row count that looks exactly right.
 */
export async function listedRecordsIn(
	kv: KVNamespace,
	payloads: { label: string; payload_json: string }[],
): Promise<number> {
	let remaining = 0;

	for (const row of payloads) {
		let payload: unknown;
		try {
			payload = JSON.parse(row.payload_json);
		} catch {
			// A payload that cannot be parsed cannot be cleared either, and
			// guessing would let an unverified row through.
			throw new Error(`payload_json for ${row.label} is not valid JSON`);
		}

		const groups = extractReportRecords(payload).map((record) => record.fields);
		const candidates = groups.reduce((total, group) => total + countReportKeys(group), 0);
		if (candidates === 0) continue;
		if (candidates > MAX_REPORT_KEYS) {
			throw new Error(
				`stored payload for ${row.label} yields ${candidates} candidate keys, over the ` +
					`${MAX_REPORT_KEYS} limit — the erasure cannot be verified`,
			);
		}

		const keys = await Promise.all(groups.map(buildReportKeys));
		const { matched } = await lookupDropKeys(kv, keys);
		remaining += matched.filter((families) => families.length > 0).length;
	}

	return remaining;
}

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
 * notices on the next search, erases the data itself — it owns the write path,
 * so it also owns the delete — and then calls this to leave the same trail Cron
 * C would have left.
 *
 * "Erases" means the whole row set for an e-mail or phone report, and only the
 * matched array elements for a people report, whose other records are other
 * people and stay. verifyErasure checks whichever of those was meant to happen.
 *
 * ClickHouse is only read here. The erasure already happened.
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
	let erasure: Erasure = { rowsRemaining: 0, recordsRemaining: 0 };

	try {
		// Not knowing whether the erasure happened and knowing it did not are
		// different things. The first says it cannot be confirmed; the second says
		// it did not happen. Reporting the first as the second would tell a caller
		// that did erase the data to go and erase it again.
		try {
			erasure = await phase("clickhouse: confirm the erasure", () =>
				verifyErasure(env, entity),
			);
		} catch (e) {
			throw new IncidentFailed(
				"unverifiable",
				`could not confirm the erasure in ClickHouse — ${e instanceof Error ? `${e.name}: ${e.message}` : String(e)}`,
			);
		}

		stage = "verify";
		if (entity.type === "people") {
			if (erasure.recordsRemaining > 0) {
				throw new IncidentFailed(
					"verify",
					`${erasure.recordsRemaining} record(s) in the stored payload still match the DROP ` +
						"list — remove them before recording the erasure",
					{ ...erasure },
				);
			}
		} else if (erasure.rowsRemaining > 0) {
			throw new IncidentFailed(
				"verify",
				`${erasure.rowsRemaining} entity_search_results row(s) still exist for this identifier — ` +
					"delete them before recording the erasure",
				{ ...erasure },
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
			rowsRemaining: erasure.rowsRemaining,
			recordsRemaining: erasure.recordsRemaining,
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
