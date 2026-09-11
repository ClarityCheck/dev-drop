import { WorkflowEntrypoint } from "cloudflare:workers";
import type { WorkflowEvent, WorkflowStep } from "cloudflare:workers";
import { hasDb, upsertWorkItems } from "./db";
import { LISTS, listTypeOf, parseCsv, unzip } from "./zip";
import type { ListType } from "./zip";

/**
 * Cron A — drop-downloader
 *
 *   ① download the ZIP from the DROP API   — NOT ENABLED: no account yet.
 *      The code is below, commented. Until then the ZIP is placed in R2 by
 *      hand under ca-drop/raw/ and step ② picks the newest one.
 *   ② archive / locate the ZIP in R2
 *   ③ unzip, parse the list CSVs, write them back to R2 as small pages
 *   ④ clear KV, then load the hashes into KV
 *   ⑤ save the same rows into public.ca_drop_work_item
 *   ⑥ apply the Removed identifiers file, tidy up, summary
 *
 * Why step ③ writes pages back to R2: a step's return value is persisted and
 * size-capped, so parsed rows cannot be handed from step to step in memory.
 * Pages of a few hundred rows keep every later step bounded — one R2 read,
 * N KV writes, one database round trip — and make the run resumable from
 * wherever it failed.
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
	/** wipe KV before loading (see the note on step ④) */
	clearKv?: boolean;
	/** rows per page — also the KV writes per step, keep well under 1000 */
	pageSize?: number;
	/** write to Supabase; defaults to on when a database is configured */
	supabase?: boolean;
	/** keep the parsed R2 pages after the run, for inspection */
	keepParsedPages?: boolean;
	/** safety rail on the clear loop */
	maxClearPages?: number;
};

type Row = { id: string; hash: string; request_date?: string };
type RemovedRow = { id: string; hash: string; list_type: string };

const RAW_PREFIX = "ca-drop/raw/";
const PARSED_PREFIX = "ca-drop/parsed/";

export class DropDownloaderWorkflow extends WorkflowEntrypoint<Env, Params> {
	async run(event: WorkflowEvent<Params>, step: WorkflowStep) {
		const runId = event.instanceId;
		const pageSize = event.payload?.pageSize ?? 400;
		const clearKv = event.payload?.clearKv ?? true;
		const keepParsedPages = event.payload?.keepParsedPages ?? false;
		const maxClearPages = event.payload?.maxClearPages ?? 200;
		const useSupabase = event.payload?.supabase ?? hasDb(this.env);

		const parsedPrefix = `${PARSED_PREFIX}${runId}/`;

		// -------------------------------------------------------------
		// ① download from the DROP API — enable once the account exists.
		//
		// const zipKey = await step.do("download list", async () => {
		//   const res = await fetch("https://api.drop.privacy.ca.gov/data/download", {
		//     headers: { "X-API-KEY": this.env.DROP_API_KEY },
		//   });
		//   if (res.status === 202) throw new Error("202 — not ready yet, retry");
		//   if (res.status === 429) throw new Error("429 — rate limited, retry");
		//   if (!res.ok) throw new Error(`DROP download ${res.status}`);
		//   // Archive the bytes unopened, before anything parses them: the
		//   // archive is the only source the set could ever be rebuilt from.
		//   const key = `${RAW_PREFIX}${new Date().toISOString().slice(0, 10)}_${runId}.zip`;
		//   await this.env.r2.put(key, res.body);
		//   return key;
		// });
		// -------------------------------------------------------------

		// -------------------------------------------------------------
		// ② the archive — for now, whatever was put there by hand
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
		// ③ unzip → parse → pages in R2
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
		// ④ KV — cleared first, then loaded.
		//
		// NOTE worth a decision rather than a default: DROP downloads are
		// deltas — "future downloads will include only new identifiers since
		// previous list download" — so a KV holding only the newest diff stops
		// suppressing everyone listed in earlier cycles. Fine for a fixture.
		// Before production either keep KV cumulative (clearKv: false) or
		// rebuild it from every ZIP in the R2 archive on each run.
		// -------------------------------------------------------------
		let cleared = 0;
		if (clearKv) {
			let page = 0;
			for (;;) {
				page += 1;
				if (page > maxClearPages) throw new Error(`clear KV: stopped after ${maxClearPages} pages`);
				const n: number = await step.do(`clear KV · page ${page}`, async () => {
					const listed = await this.env.kv.list({ limit: pageSize });
					for (const k of listed.keys) await this.env.kv.delete(k.name);
					return listed.keys.length;
				});
				cleared += n;
				if (n === 0) break;
			}
		}

		const loadedKv: Record<string, number> = {};
		for (const listType of LISTS) {
			const pageCount = parsed.pages[listType] ?? 0;
			let count = 0;
			for (let page = 1; page <= pageCount; page++) {
				count += await step.do(
					`load KV · ${listType} · page ${page}`,
					{ timeout: "5 minutes" },
					async () => {
						const rows = await this.readPage(parsedPrefix, listType, page);
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

		// -------------------------------------------------------------
		// ⑤ Supabase — the same pages, separately, so a database problem
		//    never leaves KV half-loaded and vice versa.
		// -------------------------------------------------------------
		const savedDb: Record<string, number> = {};
		if (useSupabase) {
			for (const listType of LISTS) {
				const pageCount = parsed.pages[listType] ?? 0;
				let count = 0;
				for (let page = 1; page <= pageCount; page++) {
					count += await step.do(
						`save Supabase · ${listType} · page ${page}`,
						{ timeout: "5 minutes", retries: { limit: 3, delay: "10 seconds", backoff: "linear" } },
						async () => {
							const rows = await this.readPage(parsedPrefix, listType, page);
							return upsertWorkItems(
								this.env,
								rows.map((r) => ({
									list_type: listType,
									work_item_id: r.id,
									hash: r.hash,
									request_date: r.request_date ?? null,
								})),
							);
						},
					);
				}
				savedDb[listType] = count;
			}
		}

		// -------------------------------------------------------------
		// ⑥ Removed identifiers → delete from KV.
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
				for (const r of rows) await this.env.kv.delete(r.hash);
				return rows.length;
			});
		}

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
			loadedKv,
			savedDb: useSupabase ? savedDb : null,
			removalsApplied: removed,
		};
		console.log("drop-downloader finished:", summary);
		return summary;
	}

	private async readPage(
		parsedPrefix: string,
		listType: ListType,
		page: number,
	): Promise<Row[]> {
		const obj = await this.env.r2.get(`${parsedPrefix}${listType}/${page}.json`);
		if (!obj) throw new Error(`missing parsed page ${listType}/${page}`);
		return (await obj.json()) as Row[];
	}
}
