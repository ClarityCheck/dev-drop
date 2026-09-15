import { WorkflowEntrypoint } from "cloudflare:workers";
import type { WorkflowEvent, WorkflowStep } from "cloudflare:workers";
import { countWorkItems, pageSuppressedSearches, pageWorkItems } from "./db";
import { logRun, tracer } from "./logs";
import { SUPPRESSION_TTL_SECONDS, suppressedKey } from "./drop-suppression";

/**
 * KV repair  (workflow: drop-kv-repair)
 *
 * Rewrites the DROP hash set in KV from public.ca_drop_work_item.
 *
 * It exists because KV is the one store in this pipeline that can be wrong
 * without anything noticing. Supabase is queryable and R2 is immutable, but
 * KV is a flat namespace that the real-time gate reads one key at a time —
 * and a missing key there is indistinguishable from a consumer who was never
 * on the list. Cron C is no help either: with an empty KV it matches nothing
 * and reports a clean run.
 *
 * Supabase can rebuild it because it already holds exactly what KV needs:
 * (list_type, work_item_id, hash, request_date) is the key, the value and the
 * metadata. No ZIP parsing, no R2.
 *
 * Deliberately NOT a flag on Cron A. Cron A ingests a download; a bulk rewrite
 * of the thing the real-time gate depends on should be something you run on
 * purpose, not something that rides along inside another job.
 *
 * Safe to run at any time, and safe to interrupt. Every write is kv.put of the
 * same key to the same value, so re-running repeats work rather than changing
 * the outcome, and a half-finished run leaves KV strictly closer to correct
 * than it was.
 *
 * It never deletes. An entry in KV that is not in Supabase stays — that is
 * over-suppression, which returns an empty result to someone who should have
 * got one. Unhelpful, but not a breach. Removing those needs a full
 * reconciliation and is a different job.
 */

type Params = {
	/** rows read from Supabase, and keys written to KV, per step */
	pageSize?: number;
	/** safety rail on the page loop */
	maxPages?: number;
	/** resume from a known id instead of starting over */
	afterId?: string;
	/** read Supabase and count, but write nothing */
	dryRun?: boolean;
};

export class DropKvRepairWorkflow extends WorkflowEntrypoint<Env, Params> {
	async run(event: WorkflowEvent<Params>, step: WorkflowStep) {
		const runId = event.instanceId;
		const pageSize = Math.max(1, Math.min(1000, event.payload?.pageSize ?? 500));
		const maxPages = event.payload?.maxPages ?? 50_000;
		const dryRun = event.payload?.dryRun ?? false;

		const ctx = { workflow: "drop-kv-repair", run_id: runId };
		const tracedStep = tracer(this.env, step, ctx);
		const startedAt = Date.now();
		await logRun(this.env, ctx, "started");

		let cursor = event.payload?.afterId ?? "0";
		let page = 0;
		let read = 0;
		let written = 0;

		try {
			const expected = await tracedStep("count work items", async () =>
				countWorkItems(this.env),
			);

			for (;;) {
				page += 1;
				if (page > maxPages) {
					throw new Error(
						`stopped after ${maxPages} pages — raise maxPages or pageSize deliberately`,
					);
				}

				// The cursor is the step's return value, so a retry resumes from
				// the last page that actually finished rather than the start.
				const result: { cursor: string; read: number; written: number; done: boolean } =
					await tracedStep(`repair KV · page ${page}`, async () => {
						const rows = await pageWorkItems(this.env, cursor, pageSize);
						if (rows.length === 0) {
							return { cursor, read: 0, written: 0, done: true };
						}

						let put = 0;
						if (!dryRun) {
							for (const r of rows) {
								// Exactly what Cron A writes: the key is the hash as
								// DROP published it, with no prefix, so the gate stays
								// a single kv.get with nothing to parse.
								await this.env.kv.put(r.hash, r.work_item_id, {
									metadata: {
										work_item_id: r.work_item_id,
										list_type: r.list_type,
										request_date: r.request_date ?? undefined,
									},
								});
								put += 1;
							}
						}

						return {
							cursor: rows[rows.length - 1].id,
							read: rows.length,
							written: put,
							done: rows.length < pageSize,
						};
					});

				read += result.read;
				written += result.written;
				cursor = result.cursor;
				if (result.done) break;
			}

			const summary = {
				runId,
				workItemsInSupabase: expected,
				rowsRead: read,
				keysWritten: written,
				pages: page,
				lastId: Number(cursor),
				dryRun: dryRun ? 1 : 0,
			};

			// Reading fewer rows than the table holds means the walk ended early —
			// the count is taken before the pages, so a concurrent Cron A load can
			// legitimately make `read` the larger of the two, but never the smaller.
			if (read < expected) {
				throw new Error(
					`repair read ${read} of ${expected} work items — the walk did not finish`,
				);
			}

			// The suppressed-search keys live in the same namespace and are wiped
			// by the same clearKv, so the repair has to put them back too. Their
			// absence is not dangerous — the gate falls back to the DROP list
			// alone, which is what it did before they existed — but every
			// short-circuited search starts costing a credit again.
			let suppressionsRestored = 0;
			let suppressionCursor = "0";
			let suppressionPage = 0;

			for (;;) {
				suppressionPage += 1;
				if (suppressionPage > maxPages) {
					throw new Error(
						`suppressions: stopped after ${maxPages} pages — raise maxPages deliberately`,
					);
				}

				const restored: { cursor: string; written: number; done: boolean } =
					await tracedStep(`restore suppressions · page ${suppressionPage}`, async () => {
						const rows = await pageSuppressedSearches(
							this.env,
							suppressionCursor,
							pageSize,
						);
						if (rows.length === 0) {
							return { cursor: suppressionCursor, written: 0, done: true };
						}

						let put = 0;
						if (!dryRun) {
							for (const r of rows) {
								await this.env.kv.put(suppressedKey(r.hash), "suppressed", {
									// Restored with the same expiry it had, so a repair cannot
									// turn a finding that was due to be re-checked into one
									// that lives forever.
									expirationTtl: SUPPRESSION_TTL_SECONDS,
									metadata: {
										kind: "suppressed-report",
										search_type: r.search_type,
										restored_at: new Date().toISOString(),
									},
								});
								put += 1;
							}
						}

						return {
							cursor: rows[rows.length - 1].id,
							written: put,
							done: rows.length < pageSize,
						};
					});

				suppressionsRestored += restored.written;
				suppressionCursor = restored.cursor;
				if (restored.done) break;
			}

			const withSuppressions = { ...summary, suppressionsRestored };

			console.log("KV repair finished:", withSuppressions);
			await logRun(this.env, ctx, "completed", {
				result: withSuppressions,
				duration_ms: Date.now() - startedAt,
			});
			return withSuppressions;
		} catch (e) {
			await logRun(this.env, ctx, "failed", {
				error: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
				duration_ms: Date.now() - startedAt,
				result: { pages: page, rowsRead: read, keysWritten: written },
			});
			throw e;
		}
	}
}
