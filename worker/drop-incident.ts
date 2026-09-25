import { writeMatchLog } from "./audit";
import { countEntityRows, fetchEntityPayloads } from "./ch";
import type { EntityKey } from "./ch";
import { incompleteReason, recordMatches } from "./db";
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

/** Rows whose payload verifyErasure will read back. Beyond this it refuses. */
const PAYLOAD_LIMIT = 100;

export const INCIDENT_STAGES = [
	"unverifiable",
	"verify",
	"record",
	"alert",
	"audit",
] as const;
export type IncidentStage = (typeof INCIDENT_STAGES)[number];

export type IncidentResult = {
	matchRowsInserted: number;
	workItemsLinked: number;
	rowsRemaining: number;
	recordsRemaining: number;
	/** work items whose status only a full Cron C sweep can truthfully set */
	statusLeftToCronC: number;
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

	// A people identifier has one provider row today, so the fetch limit is
	// never reached in practice. But "never in practice" is not a verification:
	// reading the first PAYLOAD_LIMIT rows and reporting recordsRemaining: 0
	// would be a measurement of a subset presented as a measurement of the
	// whole. Anything past the limit is unverifiable, and says so.
	if (rowsRemaining > PAYLOAD_LIMIT) {
		throw new Error(
			`${rowsRemaining} rows hold this identifier, over the ${PAYLOAD_LIMIT} this can read — ` +
				"the erasure cannot be verified by reading the payloads back",
		);
	}

	const stored = await fetchEntityPayloads(env, [entity], PAYLOAD_LIMIT);
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

export type MatchFoundResult = {
	matchRowsInserted: number;
	workItemsLinked: number;
};

/**
 * Record that a DROP match was found, BEFORE the caller erases anything.
 *
 * This is the half of Cron C's sequence that has to come first, and the reason
 * is in workflow-reports-cleanup.ts: "The match row is written first because
 * the erase destroys the only other evidence the consumer was ever in the
 * data."
 *
 * Cron C can do the whole sequence in one function because it performs the
 * erase itself. When the caller performs it, the sequence has to split at the
 * same seam Cron C splits at:
 *
 *   recordMatchFound     the match row, and the alert   -- BEFORE the erase
 *   (the caller erases)
 *   recordEraseIncident  verify it happened, then the R2 evidence -- AFTER
 *
 * The two writes are different claims and that is why they sit on opposite
 * sides of the erase. A match row says "this consumer was in our data", which
 * is true the moment the match is found and stays true afterwards. The evidence
 * file says the erasure is durable, which is a false statement until it is.
 *
 * Neither half sets the work item status -- see recordEraseIncident for why a
 * single report is not enough to make that claim.
 *
 * Calling this and then failing to erase is recoverable: the match row is
 * there and Cron C finds the rows on its next pass because they still exist. Erasing without calling this is NOT recoverable --
 * the rows are gone, so the view has nothing to match, and the only record
 * that the consumer was ever in the data is the one that was never written.
 */
export async function recordMatchFound(
	env: Env,
	runId: string,
	entity: EntityKey,
	hits: DropHit[],
): Promise<MatchFoundResult> {
	const ctx = { workflow: INCIDENT_WORKFLOW, run_id: runId };
	const phase = phaseTracer(env, ctx, "record match found");

	let alerts: MatchAlert[] = hits.map((hit) => ({
		list_type: hit.list_type,
		work_item_id: hit.work_item_id,
		hash: hit.hash,
	}));
	const matchRows: MatchRow[] = hits.map((hit) => ({
		list_type: hit.list_type,
		hash: hit.hash,
		matched_normalized_value: entity.normalized_value,
	}));

	try {
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

		// Every work item the hash belongs to, not only the one KV named.
		alerts = written.linkedWorkItems;

		await phase("betterstack: match alert", () =>
			logMatches(env, ctx, BATCH, alerts, { ok: true }),
		);

		return {
			matchRowsInserted: written.inserted,
			workItemsLinked: written.workItems,
		};
	} catch (e) {
		if (e instanceof IncidentFailed) throw e;
		throw new IncidentFailed(
			"record",
			e instanceof Error ? `${e.name}: ${e.message}` : String(e),
		);
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

	let alerts: MatchAlert[] = hits.map((hit) => ({
		list_type: hit.list_type,
		work_item_id: hit.work_item_id,
		hash: hit.hash,
	}));
	const matchRows: MatchRow[] = hits.map((hit) => ({
		list_type: hit.list_type,
		hash: hit.hash,
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

		// Every work item the hash belongs to, not only the one KV named.
		alerts = written.linkedWorkItems;

		stage = "alert";
		await phase("betterstack: match alert", () =>
			logMatches(env, ctx, BATCH, alerts, { ok: true }),
		);

		// NO STATUS IS SET HERE, deliberately.
		//
		// status = 'deleted' is on the WORK ITEM, and Cron B reports it to
		// California as code 3 Deleted — a statement about the consumer, not
		// about one report. This path erased the rows for exactly one
		// normalized_value. An ndz work item stands for a person, and the same
		// person can appear under dozens of other cached e-mail addresses and
		// phone numbers that this call did not touch. Marking the item deleted
		// would tell a regulator the consumer is gone while most of their data
		// is still there.
		//
		// Cron C can mark it because it sweeps every match in one run, so by the
		// time it writes the status there is nothing left for that work item.
		// This path cannot make that claim and does not try. It writes the match
		// row, which is true and is the thing that must not be lost, and leaves
		// the status to the sweep.
		//
		// Until that sweep, Cron B holds the work item back rather than
		// reporting it: it has a match row and no status, so neither 5 Not found
		// nor 3 Deleted is true yet. The match row is what makes that possible.
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
			rowsRemaining: erasure.rowsRemaining,
			recordsRemaining: erasure.recordsRemaining,
			statusLeftToCronC: written.workItems,
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
