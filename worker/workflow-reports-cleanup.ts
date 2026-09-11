import { WorkflowEntrypoint } from "cloudflare:workers";
import type { WorkflowEvent, WorkflowStep } from "cloudflare:workers";
import { chQuery, chInsert } from "./ch";
import { logRun, tracer } from "./logs";

/**
 * Cron C — ClickHouse check  (workflow: drop-reports-cleanup)
 *
 *   ⓿ optionally refresh ca_drop_combined_search_result
 *   ① copy the DROP hash set from KV into default.ca_drop_work_items
 *   ② match, once per list: INSERT ... SELECT entirely inside ClickHouse
 *   ③ prune ca_drop_work_items down to the hashes that matched
 *   ④ summary
 *
 * The candidate keys never leave the database. The earlier version streamed
 * them out and did one kv.get() per key, which is ~2.5M subrequests for the
 * current key volume — far past the Worker per-invocation limit. ClickHouse
 * cannot JOIN against KV, so the DROP set is mirrored into a table instead;
 * KV remains the source of truth and still serves the real-time Lookup gate.
 *
 * Not implemented yet: the R2 log, the expire in entity_search_results,
 * the Supabase status write and the BetterStack alert.
 *
 * run_id is the workflow instanceId, so a retry reuses it. ca_drop_match_run
 * is PARTITION BY run_id, so a bad run can be discarded as one partition:
 *   ALTER TABLE default.ca_drop_match_run DROP PARTITION '<run_id>'
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
	/** KV keys read per sync step. Keep the whole step under the ~1000
	 *  subrequest cap: worst case it is one list + one get per key. */
	kvPageSize?: number;
	/** run SYSTEM REFRESH VIEW first — needs the SYSTEM VIEWS privilege */
	refreshView?: boolean;
	/** skip the KV -> ClickHouse copy and match against whatever is already there */
	skipKvSync?: boolean;
	/** safety rail on the sync loop */
	maxKvPages?: number;
	/** keep the full KV mirror in ca_drop_work_items instead of pruning it
	 *  down to the matched hashes at the end of the run */
	keepFullDropSet?: boolean;
};

type KvMeta = { work_item_id?: string; list_type?: string; request_date?: string };

export class DropReportsCleanupWorkflow extends WorkflowEntrypoint<Env, Params> {
	async run(event: WorkflowEvent<Params>, step: WorkflowStep) {
		const runId = event.instanceId;
		const kvPageSize = event.payload?.kvPageSize ?? 400;
		const refreshView = event.payload?.refreshView ?? false;
		const skipKvSync = event.payload?.skipKvSync ?? false;
		const maxKvPages = event.payload?.maxKvPages ?? 500;

		// Every step below reports started / completed / failed to Better Stack.
		const ctx = { workflow: "drop-reports-cleanup", run_id: runId };
		const tracedStep = tracer(this.env, step, ctx);
		const runStartedAt = Date.now();
		await logRun(this.env, ctx, "started");
		const keepFullDropSet = event.payload?.keepFullDropSet ?? false;

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

		// ---------------------------------------------------------------
		// ⓿ refresh the combined view, so the match sees current data
		// ---------------------------------------------------------------
		if (refreshView) {
			await notifyStep("refresh view", "running");
			await tracedStep("refresh combined view", async () => {
				await chQuery(
					this.env,
					"SYSTEM REFRESH VIEW default.ca_drop_combined_search_result",
				);
			});
			// SYSTEM REFRESH VIEW returns immediately; wait for it to settle.
			await tracedStep(
				"await refresh",
				{ retries: { limit: 60, delay: "10 seconds", backoff: "constant" } },
				async () => {
					const [r] = await chQuery<{ status: string; exception: string }>(
						this.env,
						`SELECT status, exception FROM system.view_refreshes
						 WHERE view = 'ca_drop_combined_search_result'`,
					);
					if (!r) throw new Error("view_refreshes has no row for the view");
					if (r.exception) throw new Error(`refresh failed: ${r.exception}`);
					if (r.status !== "Scheduled") throw new Error(`refresh still ${r.status}`);
					return r.status;
				},
			);
			await notifyStep("refresh view", "completed");
		}

		// ---------------------------------------------------------------
		// ① KV -> ClickHouse. One page per step, so each step stays well
		//    inside the subrequest cap. Cron A is untouched by this.
		// ---------------------------------------------------------------
		let synced = 0;
		let skipped = 0;

		if (!skipKvSync) {
			await notifyStep("sync DROP set", "running");

			// One pass over the whole namespace: the key IS the hash, Base64
			// exactly as DROP published it, with no prefix to filter on. What
			// list a hash belongs to lives in the metadata the downloader
			// wrote, and kv.list returns metadata, so this needs no kv.get.
			let cursor: string | undefined;
			let page = 0;

			for (;;) {
				page += 1;
				if (page > maxKvPages) {
					throw new Error(`sync: stopped after ${maxKvPages} pages — raise maxKvPages deliberately`);
				}

				// The cursor is returned by the step, so a retry resumes here.
				const result: { cursor?: string; inserted: number; ignored: number } = await tracedStep(
					`sync DROP set · page ${page}`,
					async () => {
						const listed = await this.env.kv.list<KvMeta>({ limit: kvPageSize, cursor });

						const rows: object[] = [];
						for (const k of listed.keys) {
							const meta = k.metadata;
							// Without list_type there is nothing to match against, so
							// the key is counted and left alone rather than guessed at.
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
					},
				);

				synced += result.inserted;
				skipped += result.ignored;
				cursor = result.cursor;
				if (!cursor) break;
			}

			await notifyStep("sync DROP set", "completed");
		}

		// ---------------------------------------------------------------
		// ② the match — one INSERT ... SELECT per list, inside ClickHouse.
		//    Nothing is read into the Worker.
		// ---------------------------------------------------------------
		await notifyStep("match", "running");

		const matched: Record<string, number> = {};

		for (const listType of LISTS) {
			matched[listType] = await tracedStep(
				`match ${listType}`,
				{ timeout: "15 minutes", retries: { limit: 2, delay: "30 seconds", backoff: "linear" } },
				async () => {
					await chQuery(this.env, matchSql(KEY_COLUMN[listType]), {
						run_id: runId,
						list_type: listType,
					});
					// uniqExact, not count(): the table is a ReplacingMergeTree and a
					// retried step re-inserts the same rows before they merge.
					const [row] = await chQuery<{ n: number }>(
						this.env,
						`SELECT uniqExact((work_item_id, hash, type, normalized_value)) AS n
						 FROM default.ca_drop_match_run
						 WHERE run_id = {run_id:String} AND list_type = {list_type:String}`,
						{ run_id: runId, list_type: listType },
					);
					return Number(row?.n ?? 0);
				},
			);
		}

		await notifyStep("match", "completed");

		// ---------------------------------------------------------------
		// ③ prune the mirror down to the hashes that actually matched.
		//    ca_drop_work_items is a working copy, not a store: KV (and R2)
		//    remain the durable DROP set, so dropping the non-matching rows
		//    loses nothing and keeps unmatched consumer hashes out of
		//    ClickHouse. The next run re-syncs from KV before matching.
		// ---------------------------------------------------------------
		let prunedTo = -1;
		if (!keepFullDropSet) {
			await notifyStep("prune DROP set", "running");
			prunedTo = await tracedStep(
				"prune DROP set · keep matched only",
				{ timeout: "15 minutes", retries: { limit: 2, delay: "30 seconds", backoff: "linear" } },
				async () => {
					// ALTER TABLE ... DELETE, not the lightweight DELETE FROM: the
					// lightweight form is implemented as a mutation of the virtual
					// _row_exists column and therefore demands ALTER UPDATE on top
					// of ALTER DELETE. This form needs only ALTER DELETE.
					// mutations_sync = 2 makes it synchronous, so the count below is
					// taken after the rows are actually gone.
					await chQuery(
						this.env,
						`ALTER TABLE default.ca_drop_work_items
						 DELETE WHERE concat(list_type, ':', hash) NOT IN (
						     SELECT concat(list_type, ':', hash)
						     FROM default.ca_drop_match_run
						     WHERE run_id = {run_id:String}
						 )
						 SETTINGS mutations_sync = 2`,
						{ run_id: runId },
					);
					const [row] = await chQuery<{ n: number }>(
						this.env,
						"SELECT count() AS n FROM default.ca_drop_work_items",
					);
					return Number(row?.n ?? 0);
				},
			);
			await notifyStep("prune DROP set", "completed");
		}

		// ---------------------------------------------------------------
		// ④ summary
		// ---------------------------------------------------------------
		await notifyStep("summary", "running");
		const summary = await tracedStep("summary · count run rows", async () => {
			const [row] = await chQuery<{ rows: number; work_items: number; identifiers: number }>(
				this.env,
				`SELECT uniqExact((list_type, work_item_id, hash, type, normalized_value)) AS rows,
				        uniqExact(work_item_id) AS work_items,
				        uniqExact((type, normalized_value)) AS identifiers
				 FROM default.ca_drop_match_run WHERE run_id = {run_id:String}`,
				{ run_id: runId },
			);
			return {
				runId,
				syncedFromKv: synced,
				keysWithoutMetadata: skipped,
				matchesByList: matched,
				dropSetRowsKept: prunedTo,
				rowsInRunTable: Number(row?.rows ?? 0),
				distinctWorkItems: Number(row?.work_items ?? 0),
				distinctIdentifiers: Number(row?.identifiers ?? 0),
			};
		});
		await notifyStep("summary", "completed");

		console.log("Cron C partial run finished:", summary);
		await logRun(this.env, ctx, "completed", {
			result: summary,
			duration_ms: Date.now() - runStartedAt,
		});
		return summary;
	}
}

/**
 * The match. Candidate keys are expanded with arrayJoin and streamed past the
 * DROP set, which ClickHouse hashes into memory as the build side — the cheap
 * direction, because the DROP set is small and the candidate side is millions
 * of rows. Both {run_id} and {list_type} are server-side parameters, so no
 * Base64 ever reaches the SQL text.
 */
function matchSql(keyColumn: string): string {
	return `
INSERT INTO default.ca_drop_match_run
    (run_id, list_type, work_item_id, hash, type, normalized_value)
SELECT
    {run_id:String}    AS run_id,
    {list_type:String} AS list_type,
    d.work_item_id,
    c.hash,
    c.type,
    c.normalized_value
FROM
(
    SELECT type, normalized_value, arrayJoin(${keyColumn}) AS hash
    FROM default.ca_drop_combined_search_result
    WHERE notEmpty(${keyColumn})
) AS c
INNER JOIN
(
    SELECT hash, argMax(work_item_id, loaded_at) AS work_item_id
    FROM default.ca_drop_work_items
    WHERE list_type = {list_type:String} AND revoked = 0
    GROUP BY hash
) AS d
USING (hash)
`;
}

