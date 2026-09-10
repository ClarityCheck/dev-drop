import { WorkflowEntrypoint, WorkflowStep } from "cloudflare:workers";
import type { WorkflowEvent } from "cloudflare:workers";
import { chQuery, chInsert, toB64Url } from "./ch";

/**
 * Cron C — ClickHouse check (partial: steps ① and ② only)
 *
 *   ① read candidate keys from ca_drop_combined_search_result
 *   ② check each against the DROP hash set in KV
 *   ③ write every match into ca_drop_match_run
 *
 * Not implemented yet: the R2 log, the expire in entity_search_results,
 * the D1 status write and the BetterStack alert.
 *
 * run_id is the workflow instanceId, so a retry reuses it. ca_drop_match_run
 * is PARTITION BY run_id, so a bad run can be discarded as one partition:
 *   ALTER TABLE default.ca_drop_match_run DROP PARTITION '<run_id>'
 */

type Params = {
	/** identifiers per ClickHouse page */
	batchSize?: number;
	/** run SYSTEM REFRESH VIEW first — needs SYSTEM VIEWS privilege */
	refreshView?: boolean;
	/** stop after N pages; safety rail while this is partial */
	maxPages?: number;
};

type KeyRow = {
	type: string;
	normalized_value: string;
	list_type: string;
	key: string; // Base64, as stored in ClickHouse
};

/** One page of candidate keys. Cursor pagination on the view's sort key. */
const KEYS_SQL = `
SELECT type, normalized_value, list_type, key
FROM (
    SELECT type, normalized_value, 'email'   AS list_type, arrayJoin(email_keys)   AS key
    FROM default.ca_drop_combined_search_result WHERE notEmpty(email_keys)
    UNION ALL
    SELECT type, normalized_value, 'phone'   AS list_type, arrayJoin(phone_keys)   AS key
    FROM default.ca_drop_combined_search_result WHERE notEmpty(phone_keys)
    UNION ALL
    SELECT type, normalized_value, 'ndz'     AS list_type, arrayJoin(ndz_keys)     AS key
    FROM default.ca_drop_combined_search_result WHERE notEmpty(ndz_keys)
    UNION ALL
    SELECT type, normalized_value, 'namevin' AS list_type, arrayJoin(namevin_keys) AS key
    FROM default.ca_drop_combined_search_result WHERE notEmpty(namevin_keys)
)
WHERE (type, normalized_value) > ({cur_type:String}, {cur_nv:String})
ORDER BY type, normalized_value
LIMIT {batch:UInt32}
`;

export class MyWorkflow extends WorkflowEntrypoint<Env, Params> {
	async run(event: WorkflowEvent<Params>, step: WorkflowStep) {
		const runId = event.instanceId;
		const batchSize = event.payload?.batchSize ?? 20000;
		const maxPages = event.payload?.maxPages ?? 200;
		const refreshView = event.payload?.refreshView ?? false;

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
			await step.do("refresh-view", async () => {
				await chQuery(this.env, "SYSTEM REFRESH VIEW default.ca_drop_combined_search_result");
			});
			// SYSTEM REFRESH VIEW returns immediately; wait for it to settle.
			await step.do(
				"await-refresh",
				{ retries: { limit: 30, delay: "10 seconds", backoff: "constant" } },
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
		// ① + ② page through the keys, check KV, write matches
		// ---------------------------------------------------------------
		await notifyStep("match keys", "running");

		// The cursor lives in a local variable, not in a step return. Step returns
		// are persisted and size-capped, so they carry counters only — never keys.
		let curType = "";
		let curNv = "";
		let page = 0;
		let keysScanned = 0;
		let matchesFound = 0;

		for (;;) {
			page += 1;
			if (page > maxPages) throw new Error(`stopped after ${maxPages} pages — raise maxPages deliberately`);

			const rows = await step.do(`fetch-keys-${page}`, async () =>
				chQuery<KeyRow>(this.env, KEYS_SQL, {
					cur_type: curType,
					cur_nv: curNv,
					batch: batchSize,
				}),
			);

			if (rows.length === 0) break;

			const matches = await step.do(`match-kv-${page}`, async () => {
				const found: object[] = [];
				const CONCURRENCY = 50;

				for (let i = 0; i < rows.length; i += CONCURRENCY) {
					const slice = rows.slice(i, i + CONCURRENCY);
					const hits = await Promise.all(
						slice.map(async (r) => {
							const workItemId = await this.env.kv.get(`drop:${r.list_type}:${toB64Url(r.key)}`);
							return workItemId ? { r, workItemId } : null;
						}),
					);
					for (const hit of hits) {
						if (!hit) continue;
						found.push({
							run_id: runId,
							list_type: hit.r.list_type,
							work_item_id: hit.workItemId,
							hash: hit.r.key,
							type: hit.r.type,
							normalized_value: hit.r.normalized_value,
						});
					}
				}
				return found;
			});

			if (matches.length > 0) {
				await step.do(`insert-matches-${page}`, async () =>
					chInsert(this.env, "default.ca_drop_match_run", matches),
				);
			}

			keysScanned += rows.length;
			matchesFound += matches.length;

			const last = rows[rows.length - 1];
			curType = last.type;
			curNv = last.normalized_value;
		}

		await notifyStep("match keys", "completed");

		// ---------------------------------------------------------------
		// summary
		// ---------------------------------------------------------------
		await notifyStep("summary", "running");
		const summary = await step.do("summary", async () => {
			const [row] = await chQuery<{ rows: number; work_items: number }>(
				this.env,
				`SELECT count() AS rows, uniqExact(work_item_id) AS work_items
				 FROM default.ca_drop_match_run WHERE run_id = {run_id:String}`,
				{ run_id: runId },
			);
			return {
				runId,
				pages: page - 1,
				keysScanned,
				matchesFound,
				rowsInRunTable: row?.rows ?? 0,
				distinctWorkItems: row?.work_items ?? 0,
			};
		});
		await notifyStep("summary", "completed");

		console.log("Cron C partial run finished:", summary);
		return summary;
	}
}
