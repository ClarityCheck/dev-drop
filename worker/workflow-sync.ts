import { WorkflowEntrypoint } from "cloudflare:workers";
import type { WorkflowEvent, WorkflowStep } from "cloudflare:workers";
import { toB64Url } from "./ch";
import { hasSupabase, sbUpsert } from "./db";
import { LISTS, listTypeOf, parseCsv, unzip } from "./zip";
import type { ListType } from "./zip";

/**
 * Cron A — DROP sync (drop-sync)
 *
 *   ⓿ fetch the ZIP from the DROP API      — NOT ENABLED: no account yet.
 *                                            The ZIP is placed in R2 by hand.
 *   ① locate the ZIP in R2
 *   ② unzip, parse the list CSVs, write them back to R2 as small pages
 *   ③ clear the DROP prefixes in KV
 *   ④ load each page: KV, and upsert into ca_drop_work_item
 *   ⑤ apply the Removed identifiers file
 *   ⑥ drop the parsed pages, summary
 *
 * Why step ② writes pages back to R2: a step's return value is persisted and
 * size-capped, so the parsed rows cannot be handed from step to step in
 * memory. Pages of a few hundred rows in R2 keep every later step bounded
 * (one R2 read, N KV writes, one Supabase call) and make the run resumable
 * from wherever it failed.
 */

type Params = {
	/** the ZIP to ingest; defaults to the newest object under drop-raw/ */
	r2Key?: string;
	/** wipe drop:* in KV before loading (asked for; see the note in step ③) */
	clearKv?: boolean;
	/** rows per page — also the KV writes per step, keep well under 1000 */
	pageSize?: number;
	/** write to Supabase; defaults to on when the secrets are present */
	supabase?: boolean;
	/** keep the parsed R2 pages after the run, for inspection */
	keepParsedPages?: boolean;
	/** safety rails */
	maxClearPages?: number;
};

type Row = { id: string; hash: string; request_date?: string };
type RemovedRow = { id: string; hash: string; list_type: string };

const RAW_PREFIX = "drop-raw/";

export class DropSyncWorkflow extends WorkflowEntrypoint<Env, Params> {
	async run(event: WorkflowEvent<Params>, step: WorkflowStep) {
		const runId = event.instanceId;
		const pageSize = event.payload?.pageSize ?? 400;
		const clearKv = event.payload?.clearKv ?? true;
		const keepParsedPages = event.payload?.keepParsedPages ?? false;
		const maxClearPages = event.payload?.maxClearPages ?? 200;
		const useSupabase = event.payload?.supabase ?? hasSupabase(this.env);

		const parsedPrefix = `parsed/${runId}/`;

		// -------------------------------------------------------------
		// ⓿ fetch from the DROP API — enable once the account exists.
		//
		// const zipKey = await step.do("fetch list download", async () => {
		//   const res = await fetch("https://api.drop.privacy.ca.gov/data/download", {
		//     headers: { "X-API-KEY": this.env.DROP_API_KEY },
		//   });
		//   if (res.status === 202) throw new Error("202 — not ready, retry");
		//   if (res.status === 429) throw new Error("429 — rate limited, retry");
		//   if (!res.ok) throw new Error(`DROP download ${res.status}`);
		//   // Archive the bytes unopened, before anything parses them.
		//   const key = `${RAW_PREFIX}${new Date().toISOString().slice(0, 10)}_${runId}.zip`;
		//   await this.env.r2.put(key, res.body);
		//   return key;
		// });
		// -------------------------------------------------------------

		// -------------------------------------------------------------
		// ① locate the ZIP already sitting in R2
		// -------------------------------------------------------------
		const zipKey = await step.do("locate ZIP in R2", async () => {
			if (event.payload?.r2Key) return event.payload.r2Key;
			const listed = await this.env.r2.list({ prefix: RAW_PREFIX });
			const zips = listed.objects.filter((o) => o.key.toLowerCase().endsWith(".zip"));
			if (zips.length === 0) {
				throw new Error(`no .zip under ${RAW_PREFIX} in R2 — upload the fixture first`);
			}
			zips.sort((a, b) => b.uploaded.getTime() - a.uploaded.getTime());
			return zips[0].key;
		});

		// -------------------------------------------------------------
		// ② unzip → parse → pages in R2
		// -------------------------------------------------------------
		const parsed = await step.do(
			"parse ZIP → pages",
			{ timeout: "5 minutes" },
			async () => {
				const obj = await this.env.r2.get(zipKey);
				if (!obj) throw new Error(`${zipKey} not found in R2`);
				const entries = await unzip(await obj.arrayBuffer());

				const rows: Record<string, number> = {};
				const pages: Record<string, number> = {};
				const files: string[] = [];

				for (const entry of entries) {
					const kind = listTypeOf(entry.name);
					if (!kind) continue;
					files.push(entry.name);

					const csv = parseCsv(entry.bytes);
					const parsedRows =
						kind === "removed"
							? csv
									.map((r) => ({
										id: r.id ?? "",
										hash: r.hash ?? "",
										list_type: (r.listtype ?? r.list_type ?? r.type ?? "").toLowerCase(),
									}))
									.filter((r) => r.id && r.hash)
							: csv
									.map((r) => ({
										id: r.id ?? "",
										hash: r.hash ?? "",
										request_date: r.requestdate ?? r.request_date ?? undefined,
									}))
									.filter((r) => r.id && r.hash);

					let page = 0;
					for (let i = 0; i < parsedRows.length; i += pageSize) {
						page += 1;
						await this.env.r2.put(
							`${parsedPrefix}${kind}/${page}.json`,
							JSON.stringify(parsedRows.slice(i, i + pageSize)),
						);
					}
					rows[kind] = parsedRows.length;
					pages[kind] = page;
				}

				return { zipKey, files, rows, pages };
			},
		);

		// -------------------------------------------------------------
		// ③ clear KV.
		//
		// NOTE, and it is worth a decision rather than a default: DROP
		// downloads are deltas — "future downloads will include only new
		// identifiers since previous list download" — so a KV that holds only
		// the newest diff stops suppressing everyone listed in earlier cycles.
		// That is fine while this is a test fixture. Before this goes near
		// production, either keep KV cumulative (clearKv: false) or rebuild it
		// from the R2 archive on every run.
		// -------------------------------------------------------------
		let cleared = 0;
		if (clearKv) {
			let page = 0;
			for (;;) {
				page += 1;
				if (page > maxClearPages) {
					throw new Error(`clear KV: stopped after ${maxClearPages} pages`);
				}
				const n: number = await step.do(`clear KV · page ${page}`, async () => {
					const listed = await this.env.kv.list({ prefix: "drop:", limit: pageSize });
					for (const k of listed.keys) await this.env.kv.delete(k.name);
					return listed.keys.length;
				});
				cleared += n;
				if (n === 0) break;
			}
		}

		// -------------------------------------------------------------
		// ④ load each page into KV and Supabase
		// -------------------------------------------------------------
		const loaded: Record<string, number> = {};

		for (const listType of LISTS) {
			const pageCount = parsed.pages[listType] ?? 0;
			let count = 0;

			for (let page = 1; page <= pageCount; page++) {
				count += await step.do(
					`load ${listType} · page ${page}`,
					{ timeout: "5 minutes" },
					async () => this.loadPage(parsedPrefix, listType, page, useSupabase),
				);
			}
			loaded[listType] = count;
		}

		// -------------------------------------------------------------
		// ⑤ Removed identifiers → delete from KV.
		//    ca_drop_work_item has no column for a revocation, so this is
		//    recorded in the run summary only.
		// -------------------------------------------------------------
		let removed = 0;
		const removedPages = parsed.pages.removed ?? 0;
		for (let page = 1; page <= removedPages; page++) {
			removed += await step.do(`apply removals · page ${page}`, async () => {
				const obj = await this.env.r2.get(`${parsedPrefix}removed/${page}.json`);
				if (!obj) return 0;
				const rows = (await obj.json()) as RemovedRow[];
				for (const r of rows) {
					const lists = LISTS.includes(r.list_type as ListType)
						? [r.list_type as ListType]
						: LISTS;
					for (const l of lists) await this.env.kv.delete(`drop:${l}:${toB64Url(r.hash)}`);
				}
				return rows.length;
			});
		}

		// -------------------------------------------------------------
		// ⑥ tidy up + summary
		// -------------------------------------------------------------
		if (!keepParsedPages) {
			await step.do("drop parsed pages", async () => {
				const listed = await this.env.r2.list({ prefix: parsedPrefix });
				const keys = listed.objects.map((o) => o.key);
				if (keys.length > 0) await this.env.r2.delete(keys);
				return keys.length;
			});
		}

		const summary = {
			runId,
			zipKey: parsed.zipKey,
			files: parsed.files,
			rowsParsed: parsed.rows,
			kvCleared: clearKv ? cleared : -1,
			loaded,
			removalsApplied: removed,
			supabase: useSupabase,
		};
		console.log("Cron A sync finished:", summary);
		return summary;
	}

	/**
	 * One page: KV first, then Supabase. KV is what the real-time gate and the
	 * drop-reports match read, so it is the copy that must not be missed; the
	 * work_item upsert is keyed on (list_type, work_item_id) and so is safe to
	 * repeat if the step is retried.
	 */
	private async loadPage(
		parsedPrefix: string,
		listType: ListType,
		page: number,
		useSupabase: boolean,
	): Promise<number> {
		const obj = await this.env.r2.get(`${parsedPrefix}${listType}/${page}.json`);
		if (!obj) throw new Error(`missing parsed page ${listType}/${page}`);
		const rows = (await obj.json()) as Row[];

		for (const r of rows) {
			await this.env.kv.put(`drop:${listType}:${toB64Url(r.hash)}`, r.id, {
				metadata: { work_item_id: r.id, request_date: r.request_date, list_type: listType },
			});
		}

		if (useSupabase) {
			await sbUpsert(
				this.env,
				"ca_drop_work_item",
				rows.map((r) => ({
					list_type: listType,
					work_item_id: r.id,
					hash: r.hash,
					request_date: r.request_date ?? null,
				})),
				"list_type,work_item_id",
			);
		}

		return rows.length;
	}
}
