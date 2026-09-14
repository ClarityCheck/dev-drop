import { WorkflowEntrypoint } from "cloudflare:workers";
import { NonRetryableError } from "cloudflare:workflows";
import type { WorkflowEvent, WorkflowStep } from "cloudflare:workers";
import { chQuery } from "./ch";
import { incompleteReason, recordMatches } from "./db";
import type { MatchRow } from "./db";
import { logMatches, logRun, Pending, renderMatchLog, tracer } from "./logs";
import type { MatchAlert, MatchOutcome } from "./logs";
import { writeMatchLog } from "./audit";

/**
 * Cron C — ClickHouse check  (workflow: drop-reports-cleanup)
 *
 *   ① rebuild ca_drop_combined_search_result and wait for it to settle
 *   ② pull candidate keys out of the view, one batch at a time
 *   ③ for every key in the batch, kv.get the hash against the DROP set
 *   ④ on a match: a row in public.ca_drop_work_item_match AND status
 *      'deleted' on the work item, then the Better Stack alert
 *   ⑤ the R2 evidence file — only once ④ has fully landed
 *   ⑥ summary
 *
 * A match that is found and not recorded fails the run. It is the worst
 * outcome available here: a consumer on California's delete list was located
 * in our data and the fact was then lost, which is indistinguishable
 * downstream from never having found them. So the batch step throws, the
 * alert goes out at error rather than warn, and no R2 file is written
 * claiming the match was handled.
 *
 * Steps ②–④ are one Workflow step per batch, deliberately. The batch is the
 * unit of progress: the cursor advances only when the keys have been compared
 * AND the matches are in Supabase, so a step that dies anywhere in the middle
 * replays that batch and nothing else. Re-running a batch is harmless — the
 * KV reads have no side effects and the insert is idempotent.
 *
 * Nothing is mirrored into ClickHouse and no run table is written. The only
 * record a run leaves behind is public.ca_drop_work_item_match, which is also
 * the one place the link from a DROP work_item_id to the matched identifier
 * survives.
 *
 * ---------------------------------------------------------------------------
 * SIZING — read this before raising anything.
 *
 * One kv.get per candidate key means one subrequest per candidate key, and
 * the view holds far more keys than rows: ndz_keys is the cross product of
 * first names x last names x dates of birth x ZIPs for an identifier, so a
 * single row can carry over a million keys. In dev today: 1,965 rows and
 * 2,505,630 keys, 98% of them from three rows that are junk e-mail addresses
 * seen alongside hundreds of different people.
 *
 * That is why batching is by KEY and not by row — a row-at-a-time loop would
 * try to pull 1.2M keys into one step — and why wrangler.jsonc has to raise
 * limits.subrequests well above its 10,000 default. The ceiling is 10 million
 * per Workflow instance, which dev fits inside and production will not.
 * ---------------------------------------------------------------------------
 */

/** Where the walk over the view's key stream has got to. */
type Cursor = {
	/** view's `type` — '' before the first batch */
	type: string;
	/** view's `normalized_value` */
	value: string;
	/** keys already consumed from that row */
	offset: number;
};

type Params = {
	/** run SYSTEM REFRESH VIEW first — needs the SYSTEM VIEWS privilege */
	refreshView?: boolean;
	/** candidate keys compared per step. Also the KV subrequests per step. */
	batchSize?: number;
	/** view rows a batch query may examine. See the note on sizing below. */
	rowScan?: number;
	/** safety rail on the batch loop */
	maxBatches?: number;
	/** KV reads in flight at once */
	kvConcurrency?: number;
	/** compare and log, but write nothing to Supabase */
	dryRun?: boolean;
};

/** The KV metadata Cron A writes alongside each hash. */
type KvMeta = { work_item_id?: string; list_type?: string; request_date?: string };

/**
 * One row of a batch query: a slice of one view row's key stream.
 *
 * total_keys and base are Int64 in ClickHouse and arrive as strings, because
 * JSONEachRow quotes 64-bit integers by default so they survive a round trip
 * through a JSON number. Hence the Number() at every use.
 */
type BatchRow = {
	type: string;
	normalized_value: string;
	total_keys: string;
	base: string;
	/** [list_type, hash] pairs — ClickHouse tuples arrive as arrays */
	keys: [string, string][];
};

export class DropReportsCleanupWorkflow extends WorkflowEntrypoint<Env, Params> {
	async run(event: WorkflowEvent<Params>, step: WorkflowStep) {
		const runId = event.instanceId;
		const refreshView = event.payload?.refreshView ?? true;
		const batchSize = Math.max(1, event.payload?.batchSize ?? 25_000);
		// At least two, always. A batch query that can only ever see the cursor
		// row has no way to step past it once its keys are spent, so rowScan = 1
		// walks the first row and then loops on it until maxBatches trips.
		const rowScan = Math.max(2, event.payload?.rowScan ?? 200);
		const maxBatches = event.payload?.maxBatches ?? 2_000;
		const kvConcurrency = event.payload?.kvConcurrency ?? 6;
		const dryRun = event.payload?.dryRun ?? false;

		// Every step below reports started / completed / failed to Better Stack.
		const ctx = { workflow: "drop-reports-cleanup", run_id: runId };
		const tracedStep = tracer(this.env, step, ctx);
		const runStartedAt = Date.now();
		await logRun(this.env, ctx, "started");

		// Progress for the UI. Outside step.do, so it may repeat — updateStep is
		// idempotent, which is why that is safe.
		const notifyStep = async (
			stepName: string,
			status: "running" | "completed" | "waiting",
		) => {
			try {
				const doId = this.env.WORKFLOW_STATUS.idFromName(runId);
				const stub = this.env.WORKFLOW_STATUS.get(doId);
				await stub.updateStep(stepName, status);
			} catch {
				// Silently fail — progress reporting must never break the run.
			}
		};

		// Declared out here so the run-level failure log can say which batch was
		// in flight and how far the run had got. Inside the try they would be
		// out of scope exactly when something has gone wrong.
		let cursor: Cursor = { type: "", value: "", offset: 0 };
		let batch = 0;
		let keysRead = 0;
		let keysCompared = 0;
		let rowsSeen = 0;
		let matchesFound = 0;
		let matchesLinked = 0;
		let matchesInserted = 0;
		let matchesStatusSet = 0;
		let matchesSkippedEmpty = 0;
		let lastMatchLog = "";

		try {
			// ---------------------------------------------------------------
			// ① rebuild the view, so the scan sees current data
			// ---------------------------------------------------------------
			if (refreshView) {
				await notifyStep("refresh view", "running");

				// The timestamp of the last successful refresh, read BEFORE asking
				// for a new one. Waiting on `status` alone does not work: the view
				// is declared REFRESH EVERY 1 YEAR, so 'Scheduled' is its resting
				// state as well as its finished state, and SYSTEM REFRESH VIEW
				// returns the moment the refresh is queued. A poll that lands in
				// the gap before the status turns 'Running' sees 'Scheduled', calls
				// it done, and the whole scan then runs against last year's data —
				// silently, and looking entirely healthy. Success is this number
				// moving, not the status.
				const refreshedAfter = await tracedStep(
					"refresh combined view",
					// A few attempts, so a dropped connection to ClickHouse does
					// not end the run before it starts. A missing privilege still
					// surfaces inside half a minute.
					{ retries: { limit: 2, delay: "10 seconds", backoff: "constant" } },
					async () => {
						const [before] = await chQuery<{ last_success: string }>(
							this.env,
							`SELECT ifNull(toUnixTimestamp(last_success_time), 0) AS last_success
							 FROM system.view_refreshes
							 WHERE view = 'ca_drop_combined_search_result'`,
						);
						// No row here is a permissions or naming problem, not a slow
						// refresh. Caught now it names itself; left to the poll below it
						// spends 15 minutes retrying before saying anything.
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
						// These two are real faults — the view vanished, or the
						// refresh itself threw. Neither improves by waiting, but
						// the retry policy cannot be selective, so they burn the
						// remaining attempts before the run gives up. They are at
						// least logged as errors from the first attempt on.
						if (!r) throw new Error("view_refreshes has no row for the view");
						if (r.exception) throw new Error(`refresh failed: ${r.exception}`);

						// These two are the poll doing its job. Throwing is how a
						// step asks Workflows for another attempt, so on a healthy
						// run this happens several times before the refresh lands;
						// Pending keeps those out of the error stream.
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
			// ②③④ walk the view's keys, compare each against KV, record matches
			// ---------------------------------------------------------------
			await notifyStep("scan", "running");


			for (;;) {
				batch += 1;
				if (batch > maxBatches) {
					throw new Error(
						`scan: stopped after ${maxBatches} batches with the view not exhausted — ` +
							`raise maxBatches or batchSize deliberately`,
					);
				}

				const result = await tracedStep(
					`scan · batch ${batch}`,
					{
						timeout: "30 minutes",
						// limit is RETRIES, not attempts: `attempt` is 1-indexed and
						// is 2 on the first retry. So 4 here is 5 attempts in all.
						// Each one re-reads the batch from ClickHouse and re-does
						// its KV lookups, which is what covers a transient
						// ClickHouse 5xx, a dropped socket or a Hyperdrive blip.
						retries: { limit: 4, delay: "30 seconds", backoff: "linear" },
					},
					async () => this.runBatch(ctx, batch, cursor, {
						batchSize,
						rowScan,
						kvConcurrency,
						dryRun,
					}),
				);

				keysRead += result.keysRead;
				keysCompared += result.keysCompared;
				rowsSeen += result.rowsReturned;
				matchesFound += result.matchesFound;
				matchesLinked += result.linked;
				matchesInserted += result.inserted;
				matchesStatusSet += result.statusSet;
				matchesSkippedEmpty += result.skippedEmpty;
				if (result.matchLog) lastMatchLog = result.matchLog;

				if (result.done) break;
				cursor = { type: result.cursorType, value: result.cursorValue, offset: result.cursorOffset };
			}

			await notifyStep("scan", "completed");

			// ---------------------------------------------------------------
			// ⑤ summary
			// ---------------------------------------------------------------
			const summary = {
				runId,
				batches: batch,
				viewRowSlicesRead: rowsSeen,
				keysRead,
				keysCompared,
				matchesFound,
				matchesLinked,
				matchesUnlinked: matchesFound - matchesLinked,
				matchRowsInserted: matchesInserted,
				workItemsMarkedDeleted: matchesStatusSet,
				matchesSkippedEmpty,
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
			// The tracer logged the individual attempt that threw. This says the
			// run as a whole is over and where it stopped, which a step-level
			// log cannot — a batch that exhausted its retries otherwise leaves
			// five identical step failures and nothing tying them together.
			await logRun(this.env, ctx, "failed", {
				error: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
				duration_ms: Date.now() - runStartedAt,
				failedAtBatch: batch,
				result: {
					batches: batch,
					keysRead,
					keysCompared,
					matchesFound,
					matchesLinked,
					matchRowsInserted: matchesInserted,
					workItemsMarkedDeleted: matchesStatusSet,
					matchesSkippedEmpty,
				},
			});
			throw e;
		}
	}

	/**
	 * One batch: read up to `batchSize` candidate keys from the view starting
	 * at the cursor, ask KV about each of them, and record whatever matched.
	 *
	 * Everything it returns is a count or a cursor. Step return values are
	 * persisted and size-capped, so keys and identifiers never travel between
	 * steps — the next batch re-reads from ClickHouse using the cursor.
	 */
	private async runBatch(
		ctx: { workflow: string; run_id: string },
		batch: number,
		cursor: Cursor,
		opts: { batchSize: number; rowScan: number; kvConcurrency: number; dryRun: boolean },
	) {
		const rows = await chQuery<BatchRow>(this.env, BATCH_SQL, {
			cur_type: cursor.type,
			cur_value: cursor.value,
			cur_offset: cursor.offset,
			budget: opts.batchSize,
			row_scan: opts.rowScan,
		});

		// Flatten to the keys this batch is responsible for, remembering which
		// identifier each came from so a match can name it.
		type Candidate = { listType: string; hash: string; normalizedValue: string };
		const candidates: Candidate[] = [];
		for (const r of rows) {
			for (const [listType, hash] of r.keys) {
				candidates.push({ listType, hash, normalizedValue: r.normalized_value });
			}
		}

		const last = rows[rows.length - 1];
		const emittedFromLast = last ? last.keys.length : 0;

		// Nothing left to emit and the scan reached the end of the view.
		const done = candidates.length === 0 && rows.length < opts.rowScan;

		if (candidates.length === 0) {
			return {
				keysRead: 0,
				keysCompared: 0,
				rowsReturned: rows.length,
				matchesFound: 0,
				linked: 0,
				inserted: 0,
				statusSet: 0,
				skippedEmpty: 0,
				matchLog: "",
				matchLogKey: "",
				done,
				// Still advance, or a run of key-less rows would be rescanned forever.
				cursorType: last?.type ?? cursor.type,
				cursorValue: last?.normalized_value ?? cursor.value,
				cursorOffset: last ? Number(last.base) + emittedFromLast : cursor.offset,
			};
		}

		// The same hash can be reached from several identifiers in one batch;
		// asking KV once per distinct hash is the only free saving available
		// here, and subrequests are this workflow's scarcest resource.
		const byHash = new Map<string, Candidate[]>();
		for (const c of candidates) {
			const seen = byHash.get(c.hash);
			if (seen) seen.push(c);
			else byHash.set(c.hash, [c]);
		}
		const distinctHashes = [...byHash.keys()];

		// ③ the comparison — one kv.get per distinct hash. The key IS the hash,
		// Base64 exactly as DROP published it and with no prefix, so a hit is a
		// hit with no parsing. What the hash belongs to rides in the metadata,
		// which getWithMetadata returns in the same read.
		const hits = await mapPool(distinctHashes, opts.kvConcurrency, async (hash) => {
			const { value, metadata } = await this.env.kv.getWithMetadata<KvMeta>(hash);
			if (value === null && !metadata) return null;
			const workItemId = metadata?.work_item_id ?? value ?? "";
			if (!workItemId) return null;
			return { hash, workItemId, listType: metadata?.list_type ?? "" };
		});

		// ④ what matched
		const alerts: MatchAlert[] = [];
		const matchRows: MatchRow[] = [];
		for (const hit of hits) {
			if (!hit) continue;
			for (const c of byHash.get(hit.hash) ?? []) {
				// KV's metadata is authoritative for the list: ca_drop_work_item is
				// keyed on (list_type, work_item_id) as DROP published it, and the
				// key column the hash came out of is only our own derivation.
				const listType = hit.listType || c.listType;
				alerts.push({ list_type: listType, work_item_id: hit.workItemId, hash: hit.hash });
				matchRows.push({
					list_type: listType,
					work_item_id: hit.workItemId,
					matched_normalized_value: c.normalizedValue,
				});
			}
		}

		let linked = 0;
		let inserted = 0;
		let statusSet = 0;
		let skippedEmpty = 0;
		let matchLog = "";
		let matchLogKey = "";

		if (matchRows.length > 0) {
			// ④ Supabase first, and it has to land completely. "Recorded" is both
			// halves — a row in ca_drop_work_item_match AND status = 'deleted' on
			// the work item — because either one missing leaves the match
			// unreportable to DROP.
			let outcome: MatchOutcome = { ok: true };

			if (opts.dryRun) {
				outcome = { ok: false, reason: "dryRun — nothing was written" };
			} else {
				const written = await recordMatches(this.env, matchRows);
				linked = written.linked;
				inserted = written.inserted;
				statusSet = written.statusSet;
				skippedEmpty = written.skippedEmpty;

				const reason = incompleteReason(written);
				if (reason) outcome = { ok: false, reason };
			}

			// The alert goes out either way, and its level says which happened:
			// warn for a match that was recorded, error for one that was not.
			matchLog = await logMatches(this.env, ctx, batch, alerts, outcome);

			if (!outcome.ok && !opts.dryRun) {
				// No R2 log. A file in the audit trail asserting a match was
				// handled, when the database has no record of it, is worse than
				// no file — so the run fails here instead and the batch is
				// retried from the cursor it has not yet moved.
				// NonRetryableError, so this fails on the first attempt rather
				// than the fifth. Every reason incompleteReason() gives is a
				// consistency problem between KV and Supabase -- a work item
				// that is not there, a status that did not take -- and none of
				// them resolve by asking again. Retrying would re-read the
				// batch from ClickHouse and redo its KV lookups four more
				// times, roughly fifteen minutes, to arrive at the same
				// answer. Transient faults still get their five attempts:
				// those throw before ever reaching here.
				throw new NonRetryableError(
					`batch ${batch}: found ${matchRows.length} DROP match(es) but did not record them — ${outcome.reason}`,
				);
			}

			// ⑤ Only now, with the matches durable, the evidence file.
			if (!opts.dryRun) {
				matchLogKey = await writeMatchLog(this.env, {
					run_id: ctx.run_id,
					batch,
					text: renderMatchLog(ctx, batch, alerts, new Date(), outcome),
				});
			}
		}

		return {
			keysRead: candidates.length,
			keysCompared: distinctHashes.length,
			rowsReturned: rows.length,
			matchesFound: matchRows.length,
			linked,
			inserted,
			statusSet,
			skippedEmpty,
			matchLog,
			matchLogKey,
			done: false,
			cursorType: last.type,
			cursorValue: last.normalized_value,
			cursorOffset: Number(last.base) + emittedFromLast,
		};
	}
}

/**
 * Run `fn` over `items` with at most `limit` of them in flight.
 *
 * A Worker invocation may only have six connections waiting on response
 * headers at once, so firing tens of thousands of kv.get calls with
 * Promise.all does not make them faster — it makes them queue, and it holds
 * every pending promise in memory while they do.
 */
async function mapPool<T, R>(
	items: T[],
	limit: number,
	fn: (item: T) => Promise<R>,
): Promise<R[]> {
	const out = new Array<R>(items.length);
	let next = 0;

	const worker = async () => {
		for (;;) {
			const i = next++;
			if (i >= items.length) return;
			out[i] = await fn(items[i]);
		}
	};

	await Promise.all(
		Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, worker),
	);
	return out;
}

/**
 * One batch of candidate keys, starting at the cursor.
 *
 * The view is one row per identifier holding four arrays of keys, and those
 * arrays are wildly uneven — most rows carry a single key, a few carry over a
 * million. So the unit of work is the key, not the row: the arrays are
 * concatenated into one stream per row, the cursor addresses a position
 * inside that stream, and a running total cuts the batch off at `budget`
 * wherever that falls, mid-row if need be.
 *
 *   src   the next `row_scan` rows at or after the cursor row. ORDER BY
 *         matches the view's own ORDER BY (type, normalized_value), so this
 *         is an index seek and not a scan from the beginning.
 *   adj   drop the part of the cursor row a previous batch already consumed,
 *         and cap each row at `budget` — without that cap a single row's
 *         1.2M keys would be materialised in full just to take 25k of them.
 *   cum   a running key count in cursor order.
 *
 * The final WHERE keeps rows until the budget is used up — `running - n` is
 * the key count before this row, so a row is in if the budget had anything
 * left when the scan reached it. That one condition covers key-less rows
 * correctly too, and it has to be the only condition: an `OR n = 0` also
 * looks right, but it readmits key-less rows from PAST the cutoff, and since
 * the cursor follows the last row returned, the key-bearing rows in between
 * get jumped over and never compared against KV. A skipped key is a DROP
 * match that silently does not happen.
 *
 * Every value is a server-side parameter. normalized_value is arbitrary text
 * and the hashes are Base64 containing + / = — neither belongs in SQL built
 * by hand.
 */
const BATCH_SQL = `
WITH
src AS (
    SELECT
        type,
        normalized_value,
        toInt64(length(email_keys) + length(phone_keys)
              + length(ndz_keys)   + length(namevin_keys)) AS total_keys,
        arrayConcat(
            arrayMap(h -> ('email', h),   email_keys),
            arrayMap(h -> ('phone', h),   phone_keys),
            arrayMap(h -> ('ndz', h),     ndz_keys),
            arrayMap(h -> ('namevin', h), namevin_keys)
        ) AS all_keys
    FROM default.ca_drop_combined_search_result
    WHERE {cur_type:String} = ''
       OR (type, normalized_value) >= ({cur_type:String}, {cur_value:String})
    ORDER BY type, normalized_value
    LIMIT {row_scan:UInt64}
),
adj AS (
    SELECT
        type,
        normalized_value,
        total_keys,
        toInt64(if(type = {cur_type:String} AND normalized_value = {cur_value:String},
                   {cur_offset:UInt64}, 0)) AS base,
        arraySlice(
            all_keys,
            if(type = {cur_type:String} AND normalized_value = {cur_value:String},
               toInt64({cur_offset:UInt64}) + 1, 1),
            {budget:Int64}
        ) AS keys
    FROM src
),
cum AS (
    SELECT
        type, normalized_value, total_keys, base, keys,
        toInt64(length(keys)) AS n,
        toInt64(sum(length(keys)) OVER (ORDER BY type, normalized_value
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS running
    FROM adj
)
SELECT
    type,
    normalized_value,
    total_keys,
    base,
    arraySlice(keys, 1, if(running <= {budget:Int64}, n, {budget:Int64} - (running - n))) AS keys
FROM cum
WHERE (running - n) < {budget:Int64}
ORDER BY type, normalized_value
`;
