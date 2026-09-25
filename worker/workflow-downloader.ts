import { WorkflowEntrypoint } from "cloudflare:workers";
import type { WorkflowEvent, WorkflowStep } from "cloudflare:workers";
import { writeAuditLog } from "./audit";
import { liveHoldersOf, markWorkItemsRevoked, upsertWorkItems } from "./db";
import type { HashHolder } from "./db";
import { logRun, tracer } from "./logs";
import { DROP_API_URL } from "./drop-status";
import { LISTS, listTypeOf, parseCsv, unzip } from "./zip";
import type { ListType } from "./zip";

/**
 * Cron A — drop-downloader
 *
 *   ① download the ZIP from the DROP API, with { download: true }. Without
 *      it the ZIP is placed in R2 by hand under ca-drop/raw/ and step ②
 *      takes the newest one. "200 no data" ends the run before R2 is touched.
 *   ② locate the archive in R2
 *   ③ stage it: unzip and parse ONCE, one JSON page per pageSize rows under
 *      ca-drop/staging/<run>/
 *   ④ clear KV, then load the hashes into KV
 *   ⑤ upsert the rows into public.ca_drop_work_item
 *   ⑥ apply the Removed identifiers file
 *   ⑦ write the audit log to ca-drop/logs/, and delete the staging pages
 *
 * Every later step reads one staged page, so the archive is inflated once
 * per run rather than once per page.
 *
 * KV shape: the key is the hash exactly as DROP publishes it (Base64), with
 * no prefix, so the real-time gate is a single kv.get(hash). What the hash
 * belongs to travels in the metadata instead:
 *   key   = "rkAezKLuIw+Iea+CUbE06yHm0twk0e13KEdY4ptU+6M="
 *   value = "Tp1Wr5FwiJee"                       (the DROP work item id)
 *   meta  = { work_item_id, list_type, request_date }
 */

type Params = {
	/** fetch the ZIP from the DROP API first */
	download?: boolean;
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

export type KvRemoval =
	| { hash: string; action: "delete" }
	| { hash: string; action: "reassign"; holder: HashHolder };

/**
 * What a removals file does to KV, one decision per hash.
 *
 * A removal withdraws a work item, and a hash can belong to more than one:
 * two consumers sharing a phone or e-mail are two work items under one hash,
 * and KV holds one key for both. Deleting the key for one of them would stop
 * suppressing the other, who never withdrew. So a hash leaves KV only when no
 * live work item still holds it; otherwise the key is rewritten to point at
 * one that does, because the id it carried may be the one just withdrawn.
 *
 * `holders` is read AFTER the removals are stamped, so the withdrawn work
 * items are already excluded from it.
 */
export function planKvRemovals(
	removals: { hash: string }[],
	holders: HashHolder[],
): KvRemoval[] {
	const byHash = new Map(holders.map((h) => [h.hash, h]));
	const seen = new Set<string>();
	const plan: KvRemoval[] = [];

	for (const r of removals) {
		if (seen.has(r.hash)) continue;
		seen.add(r.hash);
		const holder = byHash.get(r.hash);
		plan.push(holder ? { hash: r.hash, action: "reassign", holder } : { hash: r.hash, action: "delete" });
	}
	return plan;
}

const RAW_PREFIX = "ca-drop/raw/";
const STAGING_PREFIX = "ca-drop/staging/";
const R2_CONCURRENCY = 20;

export type DownloadOutcome =
	| { kind: "zip"; bytes: ArrayBuffer }
	| { kind: "no-data"; message: string }
	| { kind: "refused"; message: string };

function isZip(bytes: ArrayBuffer): boolean {
	if (bytes.byteLength < 4) return false;
	const sig = new DataView(bytes).getUint32(0, true);
	return sig === 0x04034b50 || sig === 0x06054b50;
}

export async function readDownload(res: Response): Promise<DownloadOutcome> {
	if (res.status === 202) throw new Error("DROP download 202 — the ZIP is being prepared, retrying");
	if (res.status === 429 || res.status >= 500) throw new Error(`DROP download ${res.status} — retrying`);
	if (res.status !== 200) {
		const text = await res.text();
		return { kind: "refused", message: `DROP download ${res.status}: ${text.slice(0, 300)}` };
	}

	const bytes = await res.arrayBuffer();
	if (isZip(bytes)) return { kind: "zip", bytes };

	const text = new TextDecoder().decode(bytes);
	let body: unknown;
	try {
		body = JSON.parse(text);
	} catch {
		return { kind: "refused", message: `DROP download 200 is neither a ZIP nor JSON: ${text.slice(0, 100)}` };
	}
	const message =
		typeof body === "object" && body !== null && typeof (body as { message?: unknown }).message === "string"
			? (body as { message: string }).message
			: text.slice(0, 300);
	return { kind: "no-data", message };
}

function stagedPage(runId: string, listType: ListType, page: number): string {
	return `${STAGING_PREFIX}${runId}/${listType}/${page}.json`;
}

function stagedRemovals(runId: string): string {
	return `${STAGING_PREFIX}${runId}/removed.json`;
}

async function inBatches<T>(items: T[], size: number, f: (item: T) => Promise<unknown>): Promise<void> {
	for (let i = 0; i < items.length; i += size) {
		await Promise.all(items.slice(i, i + size).map(f));
	}
}

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
			// ① download from the DROP API
			// -----------------------------------------------------------
			let downloadedKey: string | null = null;
			if (event.payload?.download) {
				const downloaded = await tracedStep(
					"download list",
					{ timeout: "10 minutes", retries: { limit: 10, delay: "30 seconds", backoff: "constant" } },
					async () => {
						if (!this.env.DROP_API_KEY) {
							return { key: null, message: "", refused: "download requested but DROP_API_KEY is not set" };
						}
						const res = await fetch(`${DROP_API_URL}/data/download`, {
							headers: {
								"X-API-KEY": this.env.DROP_API_KEY,
								Accept: "application/zip, application/json",
							},
						});
						const outcome = await readDownload(res);
						if (outcome.kind === "refused") return { key: null, message: "", refused: outcome.message };
						if (outcome.kind === "no-data") {
							await writeAuditLog(this.env, {
								event: "download",
								run_id: runId,
								outcome: "ok",
								fields: { source: "drop-api", http_status: res.status, result: "no data", message: outcome.message },
							});
							return { key: null, message: outcome.message, refused: "" };
						}
						// Archive the bytes unopened, before anything parses them: this
						// archive is the only source the set could ever be rebuilt from.
						const key = `${RAW_PREFIX}${new Date().toISOString().slice(0, 10)}_${runId}.zip`;
						const put = await this.env.r2.put(key, outcome.bytes);
						await writeAuditLog(this.env, {
							event: "download",
							run_id: runId,
							outcome: "ok",
							fields: { source: "drop-api", http_status: res.status, zip: key, bytes: put?.size ?? "" },
						});
						return { key, message: "", refused: "" };
					},
				);
				if (downloaded.refused) throw new Error(downloaded.refused);
				if (downloaded.key === null) {
					const summary = { runId, noData: 1, message: downloaded.message };
					await logRun(this.env, ctx, "completed", {
						result: summary,
						duration_ms: Date.now() - startedAt,
					});
					return summary;
				}
				downloadedKey = downloaded.key;
			}

			// -----------------------------------------------------------
			// ② the archive
			// -----------------------------------------------------------
			const zipKey = await tracedStep("locate ZIP in R2", async () => {
				if (downloadedKey) return downloadedKey;
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
			// ③ stage: the only step that inflates the archive. Counts come
			//    back; rows go to R2, because a step's return value is
			//    persisted and size-capped.
			// -----------------------------------------------------------
			const counts = await tracedStep(
				"stage archive",
				{ timeout: "15 minutes" },
				async () => {
					const { lists, removed, files } = await this.readArchive(zipKey);
					const out: Record<string, number> = { removed: removed.length, files: files.length };
					const puts: { key: string; body: string }[] = [];
					for (const l of LISTS) {
						const rows = lists[l] ?? [];
						out[l] = rows.length;
						for (let page = 1; (page - 1) * pageSize < rows.length; page++) {
							puts.push({
								key: stagedPage(runId, l, page),
								body: JSON.stringify(rows.slice((page - 1) * pageSize, page * pageSize)),
							});
						}
					}
					puts.push({ key: stagedRemovals(runId), body: JSON.stringify(removed) });
					await inBatches(puts, R2_CONCURRENCY, (p) => this.env.r2.put(p.key, p.body));
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
							const rows = await this.staged<Row>(stagedPage(runId, listType, page));
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
							const rows = await this.staged<Row>(stagedPage(runId, listType, page));
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
			let keptForOthers = 0;
			if ((counts.removed ?? 0) > 0) {
				const applied = await tracedStep("apply removals", async () => {
					const removals = await this.staged<RemovedRow>(stagedRemovals(runId));

					const marked = await markWorkItemsRevoked(
						this.env,
						removals.map((r) => ({ list_type: r.list_type, work_item_id: r.id })),
					);

					// Only now, with the withdrawn items stamped, ask who still
					// holds each hash — see planKvRemovals.
					const holders = await liveHoldersOf(
						this.env,
						removals.map((r) => ({ list_type: r.list_type, hash: r.hash })),
					);

					let reassigned = 0;
					for (const step of planKvRemovals(removals, holders)) {
						if (step.action === "delete") {
							await this.env.kv.delete(step.hash);
							continue;
						}
						await this.env.kv.put(step.hash, step.holder.work_item_id, {
							metadata: {
								work_item_id: step.holder.work_item_id,
								list_type: step.holder.list_type,
								request_date: step.holder.request_date ?? undefined,
							},
						});
						reassigned += 1;
					}

					return { seen: removals.length, revoked: marked, reassigned };
				});
				removed = applied.seen;
				revoked = applied.revoked;
				keptForOthers = applied.reassigned;
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
						kv_keys_kept_for_others: keptForOthers,
						duration_ms: Date.now() - startedAt,
					},
				}),
			);

			await tracedStep("delete staging", async () => this.deleteStaging(runId));

			const summary = {
				runId,
				zipKey,
				rowsParsed: counts,
				kvCleared: clearKv ? cleared : -1,
				loadedKv,
				savedDb,
				removalsApplied: removed,
				workItemsRevoked: revoked,
				kvKeysKeptForOthers: keptForOthers,
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
			try {
				await this.deleteStaging(runId);
			} catch {
				// nor must cleaning up
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

	private async staged<T>(key: string): Promise<T[]> {
		const obj = await this.env.r2.get(key);
		if (!obj) throw new Error(`${key} is missing — the archive was not staged by this run`);
		return (await obj.json()) as T[];
	}

	private async deleteStaging(runId: string): Promise<number> {
		const prefix = `${STAGING_PREFIX}${runId}/`;
		let deleted = 0;
		let cursor: string | undefined;
		for (;;) {
			const listed = await this.env.r2.list({ prefix, cursor, limit: 1000 });
			const keys = listed.objects.map((o) => o.key);
			if (keys.length > 0) await this.env.r2.delete(keys);
			deleted += keys.length;
			if (!listed.truncated) return deleted;
			cursor = listed.cursor;
		}
	}
}
