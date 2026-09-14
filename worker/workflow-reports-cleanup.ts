import { WorkflowEntrypoint } from "cloudflare:workers";
import { NonRetryableError } from "cloudflare:workflows";
import type { WorkflowEvent, WorkflowStep } from "cloudflare:workers";
import { chInsert, chQuery, deleteEntityRows } from "./ch";
import type { EntityKey } from "./ch";
import { incompleteReason, markMatchesDeleted, recordMatches } from "./db";
import type { MatchRow } from "./db";
import { logMatches, logRun, Pending, renderMatchLog, tracer } from "./logs";
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
 *        erase the matched records from default.entity_search_results
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
type MatchResult = {
	list_type: string;
	work_item_id: string;
	type: string;
	normalized_value: string;
	hash: string;
};

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
			// ⑦ summary
			// ---------------------------------------------------------------
			const summary = {
				runId,
				dropSetRows,
				kvKeysWithoutMeta,
				matchesFound: totalMatches,
				matchesLinked,
				matchesUnlinked: totalMatches - matchesLinked,
				matchRowsInserted,
				workItemsMarkedDeleted: workItemsMarked,
				entityRowsExpired,
				chunks: chunk,
				dryRun: dryRun ? 1 : 0,
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
				error: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
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
		// A chunk touches ClickHouse twice, Postgres twice, Better Stack and R2,
		// and when one of them dies the Workflow reports only that the step
		// failed. "Network connection lost" at 1.8 seconds could be any of six
		// calls. This names each one as it runs — visible live in
		// `wrangler tail` — and stamps the phase into the error message, so the
		// Better Stack entry says where it died rather than merely that it did.
		//
		// The error object itself is re-thrown, not wrapped: NonRetryableError
		// has to stay a NonRetryableError or a consistency failure would start
		// burning five attempts again.
		const phase = async <T>(name: string, fn: () => Promise<T>): Promise<T> => {
			const startedAt = Date.now();
			console.log(`chunk ${chunk} > ${name}`);
			try {
				const out = await fn();
				console.log(`chunk ${chunk} ok ${name} (${Date.now() - startedAt}ms)`);
				return out;
			} catch (e) {
				const detail = e instanceof Error ? `${e.name}: ${e.message}` : String(e);
				console.error(`chunk ${chunk} FAILED ${name} (${Date.now() - startedAt}ms) - ${detail}`);
				if (e instanceof Error) e.message = `[${name}] ${e.message}`;
				throw e;
			}
		};

		const matches = await phase("clickhouse: select matches", () =>
			chQuery<MatchResult>(
				this.env,
				`${MATCH_SQL}
				 ORDER BY list_type, work_item_id, type, normalized_value
				 LIMIT {limit:UInt64} OFFSET {offset:UInt64}`,
				{ limit, offset },
			),
		);

		if (matches.length === 0) {
			return { linked: 0, inserted: 0, statusSet: 0, rowsExpired: 0, matchLog: "" };
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

		// The identifiers whose records have to go. Deduped: one identifier can
		// match more than one work item, and it is one row's worth of records
		// either way.
		const toExpire = new Map<string, EntityKey>();
		for (const m of matches) {
			toExpire.set(`${m.type}::${m.normalized_value}`, {
				type: m.type,
				normalized_value: m.normalized_value,
			});
		}

		if (dryRun) {
			const outcome: MatchOutcome = { ok: false, reason: "dryRun — nothing written or erased" };
			const text = await phase("betterstack: dry-run alert", () =>
				logMatches(this.env, ctx, chunk, alerts, outcome),
			);
			return { linked: 0, inserted: 0, statusSet: 0, rowsExpired: 0, matchLog: text };
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

		// erase, and verify rather than assume
		const expired = await phase("clickhouse: erase matched records", () =>
			deleteEntityRows(this.env, [...toExpire.values()]),
		);
		if (expired.after > 0) {
			const why = `${expired.after} of ${expired.before} entity_search_results row(s) survived the delete`;
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
 */
const MATCH_SQL = `
SELECT
    d.list_type        AS list_type,
    d.work_item_id     AS work_item_id,
    c.type             AS type,
    c.normalized_value AS normalized_value,
    c.hash             AS hash
FROM
(
${LISTS.map(
	(l) => `    SELECT '${l}' AS list_type, type, normalized_value, arrayJoin(${KEY_COLUMN[l]}) AS hash
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
