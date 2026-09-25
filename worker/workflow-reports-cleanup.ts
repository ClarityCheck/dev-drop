import { WorkflowEntrypoint } from "cloudflare:workers";
import { NonRetryableError } from "cloudflare:workflows";
import type { WorkflowEvent, WorkflowStep } from "cloudflare:workers";
import { chInsert, chQuery, deleteEntityRows, erasePeopleElements } from "./ch";
import type { ElementKey, EntityKey } from "./ch";
import { countWorkItems, incompleteReason, markMatchesDeleted, recordMatches } from "./db";
import type { MatchRow } from "./db";
import {
	describeError,
	logKvDrift,
	logMatches,
	logRun,
	Pending,
	phaseTracer,
	renderMatchLog,
	tracer,
} from "./logs";
import type { MatchAlert, MatchOutcome } from "./logs";
import { writeMatchLog } from "./audit";

/**
 * Cron C — match and erase  (workflow: drop-reports-cleanup)
 *
 *   ① rebuild ca_drop_combined_search_result and wait for it to settle
 *   ② clear default.ca_drop_work_items
 *   ③ copy the DROP hash set from KV into it, one page per step
 *   ④ match, in SQL, inside ClickHouse
 *   ⑤ per chunk of matches:
 *        the rows in public.ca_drop_work_item_match
 *        the Better Stack alert
 *        erase the matched records from default.entity_search_results —
 *          every row for a phone or e-mail identifier, only the matched
 *          array elements for a people one
 *        status 'deleted' on the work item — only once the erase is verified
 *        the R2 evidence file
 *   ⑥ clear ca_drop_work_items again
 *   ⑦ summary
 *
 * The order inside ⑤ is the design, not an implementation detail. The match
 * row is written first because the erase destroys the only other evidence the
 * consumer was ever in the data. The alert comes before the erase, so a
 * failure afterwards still leaves a record and a warning. The status comes
 * last, because Cron B reports it to California as code 3 Deleted: written
 * before the records are gone it is a false statement to a regulator; written
 * after, it is simply true.
 *
 * ---------------------------------------------------------------------------
 * WHY THE MATCH IS IN SQL
 *
 * A previous build read every candidate key out of ClickHouse and did one
 * kv.get per key. The shape of the data makes that hopeless: ndz_keys is the
 * cross product of first names x last names x dates of birth x ZIPs, so dev's
 * 1,965 view rows carry 2,505,630 keys and one row alone carries 1,209,008.
 * One subrequest per key meant 2.5M subrequests, roughly five hours, and
 * enough CPU per batch to exceed the Worker limit and have the isolate killed
 * mid-write — which is what CONNECTION_CLOSED to hyperdrive.local was.
 *
 * The same match as a join inside ClickHouse returns in 0.33 seconds.
 *
 * The only difference is which side travels. The DROP set is small and the
 * candidate keys are many, so the small side is copied into ClickHouse and
 * the join happens next to the data. ca_drop_work_items is a working copy
 * that exists only for the duration of a run — KV stays the source of truth,
 * and still serves the real-time gate in the fetch handler, where one
 * kv.get per request is exactly the right shape.
 * ---------------------------------------------------------------------------
 */

const LISTS = ["email", "phone", "ndz", "namevin"] as const;
type ListType = (typeof LISTS)[number];

const KEY_COLUMN: Record<ListType, string> = {
	email: "email_keys",
	phone: "phone_keys",
	ndz: "ndz_keys",
	namevin: "namevin_keys",
};

type Params = {
	/** run SYSTEM REFRESH VIEW first — needs the SYSTEM VIEWS privilege */
	refreshView?: boolean;
	/** KV keys listed and inserted per sync step */
	kvPageSize?: number;
	/** safety rail on the sync loop */
	maxKvPages?: number;
	/** match against whatever is already in ca_drop_work_items */
	skipKvSync?: boolean;
	/** matches handled per step. Each chunk is one ClickHouse mutation. */
	matchChunk?: number;
	/** safety rail on the match loop */
	maxChunks?: number;
	/** leave the DROP set in ClickHouse after the run, for inspection */
	keepDropSet?: boolean;
	/** find and report, but write nothing and erase nothing */
	dryRun?: boolean;
};

/** The KV metadata Cron A writes alongside each hash. */
type KvMeta = { work_item_id?: string; list_type?: string; request_date?: string };

/** One row of the match result. */
export type MatchResult = {
	list_type: string;
	work_item_id: string;
	type: string;
	normalized_value: string;
	hash: string;
	/** the stored row the match came from — people only, '' otherwise */
	provider: string;
	service: string;
	created_at: string;
	/** which element of a people report matched — '' for phone and e-mail */
	element_digest: string;
};

/** What a chunk of matches has to erase, split by what a match MEANS. */
export type ErasurePlan = { rows: EntityKey[]; elements: ElementKey[] };

/**
 * Which erase each match earns. This is Rule 2, and it is the only place the
 * two shapes are told apart.
 *
 *   phone / email   every provider row for the identifier goes. All of them
 *                   describe the consumer who is listed.
 *
 *   people          only the matched elements go and the row survives. The
 *                   other elements are other people — search "John Smith",
 *                   get forty, and thirty-nine of them never registered.
 *
 * Deduped on both sides: one identifier can match several work items, and one
 * element can match on its e-mail and its NDZ key at once. It is one erase
 * either way.
 *
 * A people match with no element_digest is refused rather than widened into a
 * whole-row delete. It can only mean the view and this code disagree about the
 * people path — a v3 view, or a people row whose payload is not an array — and
 * under Rule 3 something that cannot be done correctly must not be reported as
 * done. The alternative is the bug this function exists to prevent.
 */
export function planErasures(matches: MatchResult[]): ErasurePlan {
	const rows = new Map<string, EntityKey>();
	const elements = new Map<string, ElementKey>();

	for (const m of matches) {
		if (m.type !== "people") {
			rows.set(`${m.type}::${m.normalized_value}`, {
				type: m.type,
				normalized_value: m.normalized_value,
			});
			continue;
		}
		if (!m.element_digest) {
			// work_item_id and list_type are DROP's own identifiers and carry no
			// plaintext, so they are the only things this may name.
			throw new NonRetryableError(
				`people match on ${m.list_type} work item ${m.work_item_id} carries no ` +
					"element_digest — the view cannot say which element matched, and erasing " +
					"the whole row would delete people who are not on the DROP list. Rebuild " +
					"the view as v4",
			);
		}
		elements.set(`${m.normalized_value}::${m.element_digest}`, {
			normalized_value: m.normalized_value,
			element_digest: m.element_digest,
		});
	}

	return { rows: [...rows.values()], elements: [...elements.values()] };
}

export class DropReportsCleanupWorkflow extends WorkflowEntrypoint<Env, Params> {
	async run(event: WorkflowEvent<Params>, step: WorkflowStep) {
		const runId = event.instanceId;
		const refreshView = event.payload?.refreshView ?? true;
		const kvPageSize = Math.min(1000, Math.max(1, event.payload?.kvPageSize ?? 1000));
		const maxKvPages = event.payload?.maxKvPages ?? 20_000;
		const skipKvSync = event.payload?.skipKvSync ?? false;
		const matchChunk = Math.max(1, event.payload?.matchChunk ?? 5_000);
		const maxChunks = event.payload?.maxChunks ?? 2_000;
		const keepDropSet = event.payload?.keepDropSet ?? false;
		const dryRun = event.payload?.dryRun ?? false;

		const ctx = { workflow: "drop-reports-cleanup", run_id: runId };
		const tracedStep = tracer(this.env, step, ctx);
		const runStartedAt = Date.now();
		await logRun(this.env, ctx, "started");

		// Declared out here so the run-level failure log can say where it got to.
		let dropSetRows = 0;
		let kvKeysWithoutMeta = 0;
		let totalMatches = 0;
		let chunk = 0;
		let matchesLinked = 0;
		let matchRowsInserted = 0;
		let workItemsMarked = 0;
		let entityRowsExpired = 0;
		let peopleRecordsErased = 0;
		let lastMatchLog = "";

		const notifyStep = async (
			stepName: string,
			status: "running" | "completed" | "waiting",
		) => {
			try {
				const doId = this.env.WORKFLOW_STATUS.idFromName(runId);
				const stub = this.env.WORKFLOW_STATUS.get(doId);
				await stub.updateStep(stepName, status);
			} catch {
				// Progress reporting must never break the run.
			}
		};

		try {
			// ---------------------------------------------------------------
			// ① rebuild the view, so the match sees current data
			// ---------------------------------------------------------------
			if (refreshView) {
				await notifyStep("refresh view", "running");

				// The timestamp of the last successful refresh, read BEFORE asking
				// for a new one. Waiting on `status` alone does not work: the view
				// is REFRESH EVERY 1 YEAR, so 'Scheduled' is its resting state as
				// well as its finished state, and SYSTEM REFRESH VIEW returns the
				// moment the refresh is queued. A poll landing in the gap before
				// the status turns 'Running' sees 'Scheduled', calls it done, and
				// the match then runs against last year's data — silently, and
				// looking entirely healthy. Success is this number moving.
				const refreshedAfter = await tracedStep(
					"refresh combined view",
					{ retries: { limit: 2, delay: "10 seconds", backoff: "constant" } },
					async () => {
						const [before] = await chQuery<{ last_success: string }>(
							this.env,
							`SELECT ifNull(toUnixTimestamp(last_success_time), 0) AS last_success
							 FROM system.view_refreshes
							 WHERE view = 'ca_drop_combined_search_result'`,
						);
						if (!before) {
							throw new Error(
								"system.view_refreshes has no row for ca_drop_combined_search_result — " +
									"check SELECT on system.view_refreshes and that the view exists",
							);
						}
						await chQuery(
							this.env,
							"SYSTEM REFRESH VIEW default.ca_drop_combined_search_result",
						);
						return Number(before.last_success);
					},
				);

				await tracedStep(
					"await refresh",
					{ retries: { limit: 90, delay: "10 seconds", backoff: "constant" } },
					async () => {
						const [r] = await chQuery<{
							status: string;
							exception: string;
							last_success: string;
						}>(
							this.env,
							`SELECT status, exception,
							        ifNull(toUnixTimestamp(last_success_time), 0) AS last_success
							 FROM system.view_refreshes
							 WHERE view = 'ca_drop_combined_search_result'`,
						);
						// Real faults, reported from the first attempt.
						if (!r) throw new Error("view_refreshes has no row for the view");
						if (r.exception) throw new Error(`refresh failed: ${r.exception}`);

						// The poll doing its job. Throwing is how a step asks for
						// another attempt, so this happens several times on a
						// healthy run; Pending keeps it out of the error stream.
						const lastSuccess = Number(r.last_success);
						if (lastSuccess <= refreshedAfter) {
							throw new Pending(`refresh not finished yet (status ${r.status})`);
						}
						if (r.status !== "Scheduled") throw new Pending(`refresh still ${r.status}`);
						return lastSuccess;
					},
				);
				await notifyStep("refresh view", "completed");
			}

			// ---------------------------------------------------------------
			// ②③ the DROP set into ClickHouse, so the join has both sides
			// ---------------------------------------------------------------
			if (!skipKvSync) {
				await notifyStep("sync DROP set", "running");

				await tracedStep("clear DROP set", async () => {
					await chQuery(this.env, "TRUNCATE TABLE default.ca_drop_work_items");
				});

				// One pass over the namespace. The key IS the hash, Base64 exactly
				// as DROP published it and with no prefix; what list it belongs to
				// lives in the metadata Cron A wrote, and kv.list returns metadata,
				// so this needs no kv.get at all — 1,000 keys per subrequest.
				let cursor: string | undefined;
				let page = 0;

				for (;;) {
					page += 1;
					if (page > maxKvPages) {
						throw new Error(
							`sync: stopped after ${maxKvPages} pages — raise maxKvPages deliberately`,
						);
					}

					const result: { cursor?: string; inserted: number; ignored: number } =
						await tracedStep(`sync DROP set · page ${page}`, async () => {
							const listed = await this.env.kv.list<KvMeta>({
								limit: kvPageSize,
								cursor,
							});

							const rows: Record<string, string>[] = [];
							for (const k of listed.keys) {
								const meta = k.metadata;
								// Without a list_type there is nothing to match against,
								// so the key is counted and left alone rather than guessed.
								if (!meta?.list_type || !meta.work_item_id) continue;
								const row: Record<string, string> = {
									list_type: meta.list_type,
									hash: k.name,
									work_item_id: meta.work_item_id,
								};
								if (meta.request_date) row.request_date = meta.request_date;
								rows.push(row);
							}

							if (rows.length > 0) {
								await chInsert(this.env, "default.ca_drop_work_items", rows);
							}

							return {
								cursor: listed.list_complete ? undefined : listed.cursor,
								inserted: rows.length,
								ignored: listed.keys.length - rows.length,
							};
						});

					dropSetRows += result.inserted;
					kvKeysWithoutMeta += result.ignored;
					cursor = result.cursor;
					if (!cursor) break;
				}

				await notifyStep("sync DROP set", "completed");
			}

			// ---------------------------------------------------------------
			// ④ the match — one query, entirely inside ClickHouse
			// ---------------------------------------------------------------
			await notifyStep("match", "running");
			totalMatches = await tracedStep(
				"match",
				{ timeout: "30 minutes", retries: { limit: 4, delay: "30 seconds", backoff: "linear" } },
				async () => {
					const [r] = await chQuery<{ n: string }>(
						this.env,
						`SELECT count() AS n FROM (${MATCH_SQL})`,
					);
					return Number(r?.n ?? 0);
				},
			);
			await notifyStep("match", "completed");

			// ---------------------------------------------------------------
			// ⑤ act on them, a chunk at a time
			//
			// Paging with LIMIT/OFFSET over the match stays stable even while the
			// erase is running: the match reads the materialized view, and
			// deleting from entity_search_results does not change the view until
			// it is refreshed again.
			// ---------------------------------------------------------------
			if (totalMatches > 0) {
				await notifyStep("record and erase", "running");

				for (let offset = 0; offset < totalMatches; offset += matchChunk) {
					chunk += 1;
					if (chunk > maxChunks) {
						throw new Error(
							`stopped after ${maxChunks} chunks — raise maxChunks or matchChunk deliberately`,
						);
					}

					const result = await tracedStep(
						`record and erase · chunk ${chunk}`,
						{
							timeout: "30 minutes",
							retries: { limit: 4, delay: "30 seconds", backoff: "linear" },
						},
						async () => this.handleChunk(ctx, chunk, offset, matchChunk, dryRun),
					);

					matchesLinked += result.linked;
					matchRowsInserted += result.inserted;
					workItemsMarked += result.statusSet;
					entityRowsExpired += result.rowsExpired;
					peopleRecordsErased += result.recordsErased;
					if (result.matchLog) lastMatchLog = result.matchLog;
				}

				await notifyStep("record and erase", "completed");
			}

			// ---------------------------------------------------------------
			// ⑥ put the DROP set away again. It is a working copy, and there is
			//    no reason for consumer hashes to sit in ClickHouse between runs.
			// ---------------------------------------------------------------
			if (!keepDropSet && !dryRun) {
				await tracedStep("clear DROP set · after run", async () => {
					await chQuery(this.env, "TRUNCATE TABLE default.ca_drop_work_items");
					const [r] = await chQuery<{ n: string }>(
						this.env,
						"SELECT count() AS n FROM default.ca_drop_work_items",
					);
					const left = Number(r?.n ?? 0);
					if (left > 0) throw new Error(`${left} row(s) left in ca_drop_work_items`);
					return left;
				});
			}

			// ---------------------------------------------------------------
			// ⑦ is KV still complete?
			//
			// Free, because both numbers are already here: dropSetRows is what
			// came out of KV, and Supabase holds what should have. They are the
			// only two places the DROP set exists in a countable form.
			//
			// This matters more than it looks. An empty or partial KV does not
			// make Cron C fail — it makes it match less and report a clean run,
			// which is indistinguishable from there being nothing to delete. The
			// comparison is what turns that silence into a number.
			//
			// KV short is the dangerous direction: the real-time gate stops
			// suppressing people who asked to be deleted. KV long is stale
			// entries — over-suppression, unhelpful but not a breach.
			// ---------------------------------------------------------------
			let workItemsInSupabase = -1;
			if (!skipKvSync) {
				workItemsInSupabase = await tracedStep("check DROP set is complete", async () => {
					const expected = await countWorkItems(this.env);
					const shortfall = expected - dropSetRows;

					if (shortfall > 0) {
						await logKvDrift(this.env, ctx, {
							level: "error",
							expected,
							inKv: dropSetRows,
							message:
								`KV is missing ${shortfall} of ${expected} DROP hashes — the real-time gate ` +
								`is under-suppressing. Rebuild with POST /api/kv-repair/start`,
						});
					} else if (shortfall < 0) {
						await logKvDrift(this.env, ctx, {
							level: "warn",
							expected,
							inKv: dropSetRows,
							message:
								`KV holds ${-shortfall} more hashes than Supabase has work items — stale ` +
								`entries over-suppress. Harmless, but they should not be there.`,
						});
					}
					return expected;
				});
			}

			// ---------------------------------------------------------------
			// ⑧ summary
			// ---------------------------------------------------------------
			const summary = {
				workItemsInSupabase,
				kvShortfall: workItemsInSupabase < 0 ? 0 : workItemsInSupabase - dropSetRows,
				runId,
				dropSetRows,
				kvKeysWithoutMeta,
				matchesFound: totalMatches,
				matchesLinked,
				matchesUnlinked: totalMatches - matchesLinked,
				matchRowsInserted,
				workItemsMarkedDeleted: workItemsMarked,
				// Rows deleted whole, for phone and e-mail identifiers, and
				// records cut out of a surviving people payload. Two numbers
				// because they are two different claims about the data.
				entityRowsExpired,
				peopleRecordsErased,
				chunks: chunk,
				dryRun: dryRun ? 1 : 0,
				// What Cron B's gate reads: a sweep counts only if it synced the
				// whole DROP set from KV, rebuilt the view, and started after the
				// newest work item arrived.
				kvSynced: skipKvSync ? 0 : 1,
				viewRefreshed: refreshView ? 1 : 0,
				startedAt: new Date(runStartedAt).toISOString(),
			};

			console.log("Cron C finished:", summary);
			if (lastMatchLog) console.log(lastMatchLog);
			await logRun(this.env, ctx, "completed", {
				result: summary,
				duration_ms: Date.now() - runStartedAt,
			});
			return summary;
		} catch (e) {
			// The tracer logged the attempt that threw. This says the run as a
			// whole is over and how far it got, which a step-level log cannot.
			await logRun(this.env, ctx, "failed", {
				...describeError(e),
				duration_ms: Date.now() - runStartedAt,
				failedAtBatch: chunk,
				result: {
					dropSetRows,
					matchesFound: totalMatches,
					chunks: chunk,
					matchesLinked,
					matchRowsInserted,
					workItemsMarkedDeleted: workItemsMarked,
					entityRowsExpired,
					peopleRecordsErased,
				},
			});
			throw e;
		}
	}

	/**
	 * One chunk of matches: record, announce, erase, mark, log — in that order.
	 *
	 * Everything it returns is a count. Step return values are persisted and
	 * size-capped, so identifiers never travel between steps; the next chunk
	 * re-runs the match query at its own offset, which costs a fraction of a
	 * second because the join is the thing ClickHouse is good at.
	 */
	private async handleChunk(
		ctx: { workflow: string; run_id: string },
		chunk: number,
		offset: number,
		limit: number,
		dryRun: boolean,
	) {
		// Every call this chunk makes, reported to Better Stack as it happens —
		// start and end, with the full error on a failure. See phaseTracer for
		// why the start entries matter as much as the ends.
		const phase = phaseTracer(this.env, ctx, `record and erase · chunk ${chunk}`);

		// Every projected column is in the ORDER BY, which makes it a total
		// order. LIMIT/OFFSET over a partial order lets ties come back in a
		// different arrangement per page, so a row can be served twice and
		// another never — and the one never served is a deletion that was
		// counted and not performed.
		const matches = await phase("clickhouse: select matches", () =>
			chQuery<MatchResult>(
				this.env,
				`${MATCH_SQL}
				 ORDER BY list_type, work_item_id, type, normalized_value,
				          provider, service, created_at, element_digest, hash
				 LIMIT {limit:UInt64} OFFSET {offset:UInt64}`,
				{ limit, offset },
			),
		);

		if (matches.length === 0) {
			return {
				linked: 0,
				inserted: 0,
				statusSet: 0,
				rowsExpired: 0,
				recordsErased: 0,
				matchLog: "",
			};
		}

		const alerts: MatchAlert[] = matches.map((m) => ({
			list_type: m.list_type,
			work_item_id: m.work_item_id,
			hash: m.hash,
		}));
		const matchRows: MatchRow[] = matches.map((m) => ({
			list_type: m.list_type,
			work_item_id: m.work_item_id,
			matched_normalized_value: m.normalized_value,
		}));

		// What has to go, and what a match means decides which.
		const plan = planErasures(matches);

		if (dryRun) {
			const outcome: MatchOutcome = { ok: false, reason: "dryRun — nothing written or erased" };
			const text = await phase("betterstack: dry-run alert", () =>
				logMatches(this.env, ctx, chunk, alerts, outcome),
			);
			return {
				linked: 0,
				inserted: 0,
				statusSet: 0,
				rowsExpired: 0,
				recordsErased: 0,
				matchLog: text,
			};
		}

		// the match rows first — the erase destroys the only other evidence
		const written = await phase("supabase: insert match rows", () =>
			recordMatches(this.env, matchRows),
		);
		const reason = incompleteReason(written);
		if (reason) {
			await logMatches(this.env, ctx, chunk, alerts, { ok: false, reason });
			throw new NonRetryableError(
				`chunk ${chunk}: found ${matchRows.length} DROP match(es) but could not record them — ${reason}`,
			);
		}

		// announce it, before the data goes
		const matchLog = await phase("betterstack: match alert", () =>
			logMatches(this.env, ctx, chunk, alerts, { ok: true }),
		);

		// erase, and verify rather than assume — one call per shape, because a
		// people row is supposed to survive its erasure and a row count would
		// call that a failure
		const expired = await phase("clickhouse: erase matched rows", () =>
			deleteEntityRows(this.env, plan.rows),
		);
		if (expired.after > 0) {
			const why = `${expired.after} of ${expired.before} entity_search_results row(s) survived the delete`;
			await logMatches(this.env, ctx, chunk, alerts, { ok: false, reason: why });
			throw new Error(`chunk ${chunk}: ${why}`);
		}

		const erased = await phase("clickhouse: erase matched people records", () =>
			erasePeopleElements(this.env, plan.elements),
		);
		if (erased.after > 0) {
			const why =
				`${erased.after} of ${erased.before} DROP-listed record(s) are still in the stored ` +
				"people payloads after the update";
			await logMatches(this.env, ctx, chunk, alerts, { ok: false, reason: why });
			throw new Error(`chunk ${chunk}: ${why}`);
		}

		// only now is 'deleted' a true statement
		const statusSet = await phase("supabase: set status deleted", () =>
			markMatchesDeleted(this.env, written.workItemIds),
		);
		if (statusSet < written.workItems) {
			const why =
				`${written.workItems - statusSet} of ${written.workItems} work item(s) did not end up ` +
				`with status = 'deleted' after the records were erased`;
			await logMatches(this.env, ctx, chunk, alerts, { ok: false, reason: why });
			throw new Error(`chunk ${chunk}: ${why}`);
		}

		await phase("r2: evidence file", () =>
			writeMatchLog(this.env, {
				run_id: ctx.run_id,
				batch: chunk,
				text: renderMatchLog(ctx, chunk, alerts, new Date(), { ok: true }),
			}),
		);

		return {
			linked: written.linked,
			inserted: written.inserted,
			statusSet,
			rowsExpired: expired.before - expired.after,
			recordsErased: erased.before - erased.after,
			matchLog,
		};
	}
}

/**
 * The match.
 *
 * Candidate keys are expanded with arrayJoin and streamed past the DROP set,
 * which ClickHouse hashes into memory as the build side — the cheap direction,
 * because the DROP set is small and the candidate side is millions of rows.
 * Measured at 0.33 seconds over dev's 2,505,630 keys.
 *
 * The GROUP BY on the DROP side is not decoration. ca_drop_work_items is a
 * ReplacingMergeTree and a retried sync page re-inserts rows it already wrote;
 * dedup happens on merge, which may not have run yet, so the query collapses
 * duplicates itself rather than trusting that it has.
 *
 * Joining on (list_type, hash) rather than hash alone keeps a key matched
 * against the list it was derived for. A collision across lists is not a real
 * risk with SHA-256 — this is about saying what is meant.
 *
 * WHAT THE MATCH NAMES is the view's grain, and it decides what may be erased.
 * A phone or e-mail row is one identifier with every provider merged into it,
 * so the match names the identifier. A people row is ONE ARRAY ELEMENT of one
 * stored row version, so the match names that element by its digest and the
 * erase can take it without touching the thirty-nine strangers beside it.
 */
const MATCH_SQL = `
SELECT
    d.list_type        AS list_type,
    d.work_item_id     AS work_item_id,
    c.type             AS type,
    c.normalized_value AS normalized_value,
    c.provider         AS provider,
    c.service          AS service,
    c.created_at       AS created_at,
    c.element_digest   AS element_digest,
    c.hash             AS hash
FROM
(
${LISTS.map(
	(l) => `    SELECT '${l}' AS list_type, type, normalized_value,
           provider, service, created_at, element_digest,
           arrayJoin(${KEY_COLUMN[l]}) AS hash
    FROM default.ca_drop_combined_search_result
    WHERE notEmpty(${KEY_COLUMN[l]})`,
).join("\n    UNION ALL\n")}
) AS c
INNER JOIN
(
    SELECT list_type, hash, argMax(work_item_id, loaded_at) AS work_item_id
    FROM default.ca_drop_work_items
    GROUP BY list_type, hash
) AS d
ON c.list_type = d.list_type AND c.hash = d.hash
`;
