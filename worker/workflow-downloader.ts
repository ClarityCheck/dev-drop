import { WorkflowEntrypoint } from "cloudflare:workers";
import type { WorkflowEvent, WorkflowStep } from "cloudflare:workers";
import { writeAuditLog } from "./audit";
import { markWorkItemsRevoked, upsertWorkItems } from "./db";
import { logRun, tracer } from "./logs";
import { LISTS, listTypeOf, parseCsv, unzip } from "./zip";
import type { ListType } from "./zip";

/**
 * Cron A — drop-downloader
 *
 *   ① download the ZIP from the DROP API   — NOT ENABLED: no account yet.
 *      The code is below, commented, along with the audit log it writes.
 *      Until then the ZIP is placed in R2 by hand under ca-drop/raw/ and
 *      step ② takes the newest one.
 *   ② locate the archive in R2
 *   ③ count the rows per list (the ZIP is the only copy of the data)
 *   ④ clear KV, then load the hashes into KV
 *   ⑤ upsert the rows into public.ca_drop_work_item
 *   ⑥ apply the Removed identifiers file
 *   ⑦ write the audit log to ca-drop/logs/
 *
 * Nothing intermediate is stored. Every step that needs rows reads the
 * archive from R2 and parses the slice it needs: the archive has to be kept
 * anyway (it is the only source the identifier set could be rebuilt from),
 * so a second parsed copy would be the same data twice. The cost is
 * re-inflating the ZIP per step, which is cheap next to the R2 round trip.
 *
 * KV shape: the key is the hash exactly as DROP publishes it (Base64), with
 * no prefix, so the real-time gate is a single kv.get(hash). What the hash
 * belongs to travels in the metadata instead:
 *   key   = "rkAezKLuIw+Iea+CUbE06yHm0twk0e13KEdY4ptU+6M="
 *   value = "Tp1Wr5FwiJee"                       (the DROP work item id)
 *   meta  = { work_item_id, list_type, request_date }
 */

type Params = {
	/** the ZIP to ingest; defaults to the newest object under ca-drop/raw/ */
	r2Key?: string;
	/** wipe KV before loading. Default false — see the note where it is read. */
	clearKv?: boolean;
	/** rows per page — also the KV writes per step, keep well under 1000 */
	pageSize?: number;
	/** safety rail on the clear loop */
	maxClearPages?: number;
};

type Row = { id: string; hash: string; request_date?: string; source_file: string };
type RemovedRow = { id: string; hash: string; list_type: string };

const RAW_PREFIX = "ca-drop/raw/";

export class DropDownloaderWorkflow extends WorkflowEntrypoint<Env, Params> {
	async run(event: WorkflowEvent<Params>, step: WorkflowStep) {
		const runId = event.instanceId;
		const pageSize = event.payload?.pageSize ?? 400;
		// Cumulative by default. Every download after the first is a delta, so
		// wiping first leaves KV holding only the newest one — and the real-time
		// gate then stops suppressing anyone listed in an earlier cycle, silently,
		// because a missing key is indistinguishable from never having been listed.
		// The upsert into Supabase already accumulates; this makes KV match it.
		const clearKv = event.payload?.clearKv ?? false;
		const maxClearPages = event.payload?.maxClearPages ?? 200;

		const ctx = { workflow: "drop-downloader", run_id: runId };
		const tracedStep = tracer(this.env, step, ctx);
		const startedAt = Date.now();
		await logRun(this.env, ctx, "started");

		try {
			// -----------------------------------------------------------
			// ① download from the DROP API — enable once the account exists.
			//
			// const zipKey = await tracedStep("download list", async () => {
			//   const res = await fetch("https://api.drop.privacy.ca.gov/data/download", {
			//     headers: { "X-API-KEY": this.env.DROP_API_KEY },
			//   });
			//   if (res.status === 202) throw new Error("202 — not ready yet, retry");
			//   if (res.status === 429) throw new Error("429 — rate limited, retry");
			//   if (!res.ok) {
			//     await writeAuditLog(this.env, {
			//       event: "download", run_id: runId, outcome: "failed",
			//       fields: { source: "drop-api", http_status: res.status },
			//       error: `DROP download ${res.status}`,
			//     });
			//     throw new Error(`DROP download ${res.status}`);
			//   }
			//   // Archive the bytes unopened, before anything parses them: this
			//   // archive is the only source the set could ever be rebuilt from.
			//   const key = `${RAW_PREFIX}${new Date().toISOString().slice(0, 10)}_${runId}.zip`;
			//   const put = await this.env.r2.put(key, res.body);
			//   await writeAuditLog(this.env, {
			//     event: "download", run_id: runId, outcome: "ok",
			//     fields: {
			//       source: "drop-api", http_status: res.status,
			//       zip: key, bytes: put?.size ?? "",
			//     },
			//   });
			//   return key;
			// });
			// -----------------------------------------------------------

			// -----------------------------------------------------------
			// ② the archive — for now, whatever was put there by hand
			// -----------------------------------------------------------
			const zipKey = await tracedStep("locate ZIP in R2", async () => {
				if (event.payload?.r2Key) return event.payload.r2Key;
				const listed = await this.env.r2.list({ prefix: RAW_PREFIX });
				const zips = listed.objects.filter((o) => o.key.toLowerCase().endsWith(".zip"));
				if (zips.length === 0) {
					throw new Error(`no .zip under ${RAW_PREFIX} in R2 — upload the fixture first`);
				}
				zips.sort((a, b) => b.uploaded.getTime() - a.uploaded.getTime());
				return zips[0].key;
			});

			// -----------------------------------------------------------
			// ③ how much there is. Counts only — a step's return value is
			//    persisted and size-capped, so rows never travel between steps.
			// -----------------------------------------------------------
			const counts = await tracedStep(
				"count rows per list",
				{ timeout: "5 minutes" },
				async () => {
					const { lists, removed, files } = await this.readArchive(zipKey);
					const out: Record<string, number> = { removed: removed.length };
					for (const l of LISTS) out[l] = lists[l]?.length ?? 0;
					out.files = files.length;
					return out;
				},
			);

			// -----------------------------------------------------------
			// ④ KV — cleared first, then loaded.
			//
			// NOTE worth a decision rather than a default: DROP downloads are
			// deltas — "future downloads will include only new identifiers
			// since previous list download" — so a KV holding only the newest
			// diff stops suppressing everyone listed in earlier cycles. Fine
			// for a fixture. Before production either keep KV cumulative
			// (clearKv: false) or rebuild it from every ZIP in ca-drop/raw/.
			// -----------------------------------------------------------
			let cleared = 0;
			if (clearKv) {
				// One pass, driven by the cursor — NOT by re-listing until a
				// page comes back empty. kv.list is eventually consistent: it
				// keeps returning keys that were just deleted, so an "until
				// empty" loop deletes the same keys over and over.
				let cursor: string | undefined;
				let page = 0;
				for (;;) {
					page += 1;
					if (page > maxClearPages) {
						throw new Error(`clear KV: stopped after ${maxClearPages} pages`);
					}
					const result: { cursor?: string; deleted: number } = await tracedStep(
						`clear KV · page ${page}`,
						async () => {
							const listed = await this.env.kv.list({ limit: pageSize, cursor });
							for (const k of listed.keys) await this.env.kv.delete(k.name);
							return {
								cursor: listed.list_complete ? undefined : listed.cursor,
								deleted: listed.keys.length,
							};
						},
					);
					cleared += result.deleted;
					cursor = result.cursor;
					if (!cursor) break;
				}
			}

			const loadedKv: Record<string, number> = {};
			for (const listType of LISTS) {
				const pages = Math.ceil((counts[listType] ?? 0) / pageSize);
				let count = 0;
				for (let page = 1; page <= pages; page++) {
					count += await tracedStep(
						`load KV · ${listType} · page ${page}`,
						{ timeout: "5 minutes" },
						async () => {
							const rows = await this.page(zipKey, listType, page, pageSize);
							for (const r of rows) {
								await this.env.kv.put(r.hash, r.id, {
									metadata: {
										work_item_id: r.id,
										list_type: listType,
										request_date: r.request_date,
									},
								});
							}
							return rows.length;
						},
					);
				}
				loadedKv[listType] = count;
			}

			// -----------------------------------------------------------
			// ⑤ Supabase — its own steps, so a database problem never leaves
			//    KV half-loaded and vice versa.
			// -----------------------------------------------------------
			const savedDb: Record<string, number> = {};
			for (const listType of LISTS) {
				const pages = Math.ceil((counts[listType] ?? 0) / pageSize);
				let count = 0;
				for (let page = 1; page <= pages; page++) {
					count += await tracedStep(
						`save Supabase · ${listType} · page ${page}`,
						{ timeout: "5 minutes", retries: { limit: 3, delay: "10 seconds", backoff: "linear" } },
						async () => {
							const rows = await this.page(zipKey, listType, page, pageSize);
							return upsertWorkItems(
								this.env,
								rows.map((r) => ({
									list_type: listType,
									work_item_id: r.id,
									hash: r.hash,
									request_date: r.request_date ?? null,
									source_file: r.source_file,
								})),
							);
						},
					);
				}
				savedDb[listType] = count;
			}

			// -----------------------------------------------------------
			// ⑥ Removed identifiers. A consumer has withdrawn their request,
			//    or the state has revoked the entry, so we stop suppressing
			//    them and stop answering for them.
			//
			//    SUPABASE FIRST, THEN KV, and the order is the safe one.
			//
			//    Stamped and then the KV delete fails: we keep suppressing
			//    someone who withdrew. Over-suppression, and the next run
			//    retries it.
			//
			//    KV deleted and then the stamp fails: we stop suppressing
			//    them AND still report them to California as ours to answer
			//    for — and kv-repair, which pages live work items, would put
			//    the hash straight back and suppress them again.
			// -----------------------------------------------------------
			let removed = 0;
			let revoked = 0;
			if ((counts.removed ?? 0) > 0) {
				const applied = await tracedStep("apply removals", async () => {
					const { removed: rows } = await this.readArchive(zipKey);
					const removals = rows as RemovedRow[];

					const marked = await markWorkItemsRevoked(
						this.env,
						removals.map((r) => ({ list_type: r.list_type, work_item_id: r.id })),
					);

					for (const r of removals) await this.env.kv.delete(r.hash);

					return { seen: removals.length, revoked: marked };
				});
				removed = applied.seen;
				revoked = applied.revoked;
			}

			// -----------------------------------------------------------
			// ⑦ the audit log: timestamps, outcome, counts. No identifiers.
			// -----------------------------------------------------------
			const rowsTotal = Object.values(savedDb).reduce((a, b) => a + b, 0);
			const logKey = await tracedStep("write audit log", async () =>
				writeAuditLog(this.env, {
					event: "supabase-upsert",
					run_id: runId,
					outcome: "ok",
					fields: {
						table: "public.ca_drop_work_item",
						zip: zipKey,
						rows_total: rowsTotal,
						rows_by_list: LISTS.map((l) => `${l}=${savedDb[l] ?? 0}`).join(" "),
						kv_loaded: Object.values(loadedKv).reduce((a, b) => a + b, 0),
						kv_cleared: clearKv ? cleared : "not cleared",
						removals_applied: removed,
						work_items_revoked: revoked,
						duration_ms: Date.now() - startedAt,
					},
				}),
			);

			const summary = {
				runId,
				zipKey,
				rowsParsed: counts,
				kvCleared: clearKv ? cleared : -1,
				loadedKv,
				savedDb,
				removalsApplied: removed,
				workItemsRevoked: revoked,
				auditLog: logKey,
			};
			console.log("drop-downloader finished:", summary);
			await logRun(this.env, ctx, "completed", {
				result: { rows: rowsTotal, kvCleared: cleared, removalsApplied: removed },
				duration_ms: Date.now() - startedAt,
			});
			return summary;
		} catch (e) {
			const error = e instanceof Error ? `${e.name}: ${e.message}` : String(e);
			// A failed run is also an outcome worth keeping.
			try {
				await writeAuditLog(this.env, {
					event: "supabase-upsert",
					run_id: runId,
					outcome: "failed",
					fields: { duration_ms: Date.now() - startedAt },
					error,
				});
			} catch {
				// the audit log must not replace the original failure
			}
			await logRun(this.env, ctx, "failed", { error, duration_ms: Date.now() - startedAt });
			throw e;
		}
	}

	/** Reads and parses the archive. The only place that touches the ZIP. */
	private async readArchive(zipKey: string): Promise<{
		lists: Partial<Record<ListType, Row[]>>;
		removed: RemovedRow[];
		files: string[];
	}> {
		const obj = await this.env.r2.get(zipKey);
		if (!obj) throw new Error(`${zipKey} not found in R2`);
		const entries = await unzip(await obj.arrayBuffer());

		const lists: Partial<Record<ListType, Row[]>> = {};
		let removed: RemovedRow[] = [];
		const files: string[] = [];

		for (const entry of entries) {
			const kind = listTypeOf(entry.name);
			if (!kind) continue;
			files.push(entry.name);
			const csv = parseCsv(entry.bytes);

			if (kind === "removed") {
				removed = csv
					.map((r) => ({
						id: r.id ?? "",
						hash: r.hash ?? "",
						list_type: (r.listtype ?? r.list_type ?? r.type ?? "").toLowerCase(),
					}))
					.filter((r) => r.id && r.hash);
			} else {
				const sourceFile = entry.name.split("/").pop() ?? entry.name;
				lists[kind] = csv
					.map((r) => ({
						id: r.id ?? "",
						hash: r.hash ?? "",
						request_date: r.requestdate ?? r.request_date ?? undefined,
						source_file: sourceFile,
					}))
					.filter((r) => r.id && r.hash);
			}
		}

		return { lists, removed, files };
	}

	private async page(
		zipKey: string,
		listType: ListType,
		page: number,
		pageSize: number,
	): Promise<Row[]> {
		const { lists } = await this.readArchive(zipKey);
		const rows = lists[listType] ?? [];
		return rows.slice((page - 1) * pageSize, page * pageSize);
	}
}
