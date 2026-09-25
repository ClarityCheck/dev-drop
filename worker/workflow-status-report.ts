import { WorkflowEntrypoint } from "cloudflare:workers";
import { NonRetryableError } from "cloudflare:workflows";
import type { WorkflowEvent, WorkflowStep } from "cloudflare:workers";
import { writeAuditLog } from "./audit";
import { latestIngestAt, markReported, pageReportItems, planStatusReport } from "./db";
import type { ReportKind } from "./db";
import {
	DROP_API_URL,
	parseStatusCsv,
	parseUploadResponse,
	renderStatusCsv,
	reportSuffix,
	responseFileName,
} from "./drop-status";
import type { UploadOutcome } from "./drop-status";
import { logRun, tracer } from "./logs";

/**
 * Cron B — status report  (workflow: drop-status-report)
 *
 *   ① gate: a completed Cron C run that started after the newest work item
 *   ② plan: what to upload, what to amend, what to hold back
 *   ③ build one Id,Status CSV per downloaded file and kind, into R2
 *   ④ POST /data/upload and /data/amend  — only with `upload: true`
 *   ⑤ record what DROP accepted on the work items
 *   ⑥ fail the run if anything was rejected or could not be reported
 *
 * THE GATE IS ①, NOT A CLOCK. Absence of a status reads as 5 Not found, so a
 * Cron B that runs before Cron C has looked reports a clean sheet for matches
 * nobody searched for. The caller names the Cron C run; Cron B checks it
 * finished, did a real sweep, and began after the newest work item arrived.
 *
 * WHAT GETS WHICH CODE
 *   status 'deleted'     3   Cron C erased the records and verified it
 *   status 'exempted'    2   set by policy
 *   status 'opted_out'   4   set by policy
 *   anything else        5
 *   no status, matched   held back: Cron C found the consumer and has not
 *                        finished erasing, so neither 5 nor 3 is true yet
 *   revoked              never reported; DROP asks for no response
 *
 * An item reported once and whose code has changed since is sent again to
 * /data/amend: the spec requires an update within 45 days of the change.
 *
 * Every CSV is written to R2 before anything is sent, so the archive holds
 * exactly what was reported. Without `upload: true` and DROP_API_KEY the run
 * stops there — build and archive only.
 */

type Params = {
	/** the Cron C instance whose sweep this report rests on */
	cleanupInstanceId?: string;
	/** report without checking Cron C — for testing against a fixture only */
	skipCleanupGate?: boolean;
	/** send the files to DROP. Off by default. */
	upload?: boolean;
	/** work items read per query while building a file */
	pageSize?: number;
};

const REPORTS_PREFIX = "ca-drop/reports/";
const MARK_BATCH = 1000;

type BuiltFile = {
	fileName: string;
	r2Key: string;
	list_type: string;
	kind: ReportKind;
	rows: number;
	code2: number;
	code3: number;
	code4: number;
	code5: number;
};

/**
 * Why a Cron C run is not grounds for a report, or null if it is.
 *
 * `output` is the summary Cron C returns. Runs from before these fields
 * existed carry no startedAt and are refused: there is no way to tell when
 * they swept.
 */
export function cleanupGateFailure(
	status: { status: string; output?: unknown },
	latestIngestMs: number | null,
): string | null {
	if (status.status !== "complete") {
		return `Cron C run is "${status.status}", not complete`;
	}
	const out = (status.output ?? {}) as {
		dryRun?: number;
		kvSynced?: number;
		viewRefreshed?: number;
		startedAt?: string;
	};
	if (out.dryRun === 1) return "Cron C run was a dry run — it erased nothing";
	if (out.kvSynced !== 1) return "Cron C run skipped the KV sync — it matched a stale DROP set";
	if (out.viewRefreshed !== 1) return "Cron C run did not refresh the view — it matched stale data";
	const started = out.startedAt ? Date.parse(out.startedAt) : NaN;
	if (Number.isNaN(started)) return "Cron C run carries no startedAt — run Cron C again";
	if (latestIngestMs !== null && started < latestIngestMs) {
		return (
			`Cron C started ${new Date(started).toISOString()}, before the newest work item arrived ` +
			`${new Date(latestIngestMs).toISOString()} — run Cron C again`
		);
	}
	return null;
}

async function postStatusFiles(
	env: Env,
	kind: ReportKind,
	files: BuiltFile[],
): Promise<UploadOutcome & { httpStatus: number; fatal?: string }> {
	const form = new FormData();
	for (const f of files) {
		const obj = await env.r2.get(f.r2Key);
		if (!obj) throw new NonRetryableError(`${f.r2Key} is missing from R2`);
		form.append("files", new File([await obj.text()], f.fileName, { type: "text/csv" }));
	}

	const res = await fetch(`${DROP_API_URL}/data/${kind}`, {
		method: "POST",
		headers: { "X-API-KEY": env.DROP_API_KEY as string },
		body: form,
	});
	const text = await res.text();

	// The spec: retry 429 and 5xx, do not retry any other 4xx unchanged.
	if (res.status === 429 || res.status >= 500) {
		throw new Error(`DROP /data/${kind} answered ${res.status} — retrying`);
	}

	let body: unknown = null;
	try {
		body = JSON.parse(text);
	} catch {
		// not JSON; handled below
	}
	const outcome = parseUploadResponse(body);
	// 202 queued some files; 400 can mean none were accepted, and then the
	// body still says which were rejected and why.
	if ((res.status === 202 || res.status === 400) && outcome) {
		return { ...outcome, httpStatus: res.status };
	}
	// Any other answer will not change on retry. Returned rather than thrown so
	// the message survives the step boundary.
	return {
		accepted: [],
		rejected: [],
		httpStatus: res.status,
		fatal: `DROP /data/${kind} answered ${res.status}: ${text.slice(0, 300)}`,
	};
}

export class DropStatusReportWorkflow extends WorkflowEntrypoint<Env, Params> {
	async run(event: WorkflowEvent<Params>, step: WorkflowStep) {
		const runId = event.instanceId;
		const upload = event.payload?.upload ?? false;
		const pageSize = Math.max(1, Math.min(5000, event.payload?.pageSize ?? 2000));

		const ctx = { workflow: "drop-status-report", run_id: runId };
		const tracedStep = tracer(this.env, step, ctx);
		const startedAt = Date.now();
		let failureLogged = false;
		await logRun(this.env, ctx, "started");

		try {
			// ① the gate
			// A refusal is returned, not thrown, and thrown below outside the
			// step: Workflows replaces the message of a NonRetryableError that
			// escapes a step, and the reason is what an operator needs to see.
			const gate: { gated: number; refused?: string } = await tracedStep(
				"gate on Cron C",
				async () => {
					if (event.payload?.skipCleanupGate) return { gated: 0 };

					const id = event.payload?.cleanupInstanceId;
					if (!id) {
						return {
							gated: 0,
							refused: "cleanupInstanceId is required: name the Cron C run this report rests on",
						};
					}

					let status: { status: string; output?: unknown };
					try {
						status = await (await this.env.DROP_REPORTS_CLEANUP.get(id)).status();
					} catch (e) {
						return {
							gated: 0,
							refused: `Cron C run ${id} not found — ${e instanceof Error ? e.message : String(e)}`,
						};
					}

					const reason = cleanupGateFailure(status, await latestIngestAt(this.env));
					return reason ? { gated: 0, refused: reason } : { gated: 1 };
				},
			);
			if (gate.refused) throw new Error(gate.refused);

			// ② the plan
			const plan = await tracedStep("plan", { timeout: "5 minutes" }, () =>
				planStatusReport(this.env),
			);

			// ③ build and archive — one step per file, so a failure resumes here
			const built: BuiltFile[] = [];
			for (const g of plan.groups) {
				built.push(
					await tracedStep(
						`build · ${g.kind} · ${g.source_file}`,
						{ timeout: "10 minutes" },
						async () => {
							const rows: { work_item_id: string; code: number }[] = [];
							let cursor = "0";
							for (;;) {
								const page = await pageReportItems(this.env, g, cursor, pageSize);
								for (const r of page) rows.push({ work_item_id: r.work_item_id, code: r.code });
								if (page.length < pageSize) break;
								cursor = page[page.length - 1].id;
							}

							const fileName = responseFileName(g.source_file, reportSuffix(runId, g.kind));
							const r2Key = `${REPORTS_PREFIX}${runId}/${g.kind}/${fileName}`;
							await this.env.r2.put(r2Key, renderStatusCsv(rows), {
								httpMetadata: { contentType: "text/csv; charset=utf-8" },
							});

							const count = (code: number) => rows.filter((r) => r.code === code).length;
							return {
								fileName,
								r2Key,
								list_type: g.list_type,
								kind: g.kind,
								rows: rows.length,
								code2: count(2),
								code3: count(3),
								code4: count(4),
								code5: count(5),
							};
						},
					),
				);
			}

			const toSend = built.filter((f) => f.rows > 0);
			const outcomes: Record<ReportKind, UploadOutcome & { httpStatus: number }> = {
				upload: { accepted: [], rejected: [], httpStatus: 0 },
				amend: { accepted: [], rejected: [], httpStatus: 0 },
			};

			// ④ upload
			if (upload) {
				if (!this.env.DROP_API_KEY) {
					throw new Error(
						"upload requested but DROP_API_KEY is not set — the files are in R2, nothing was sent",
					);
				}

				for (const kind of ["upload", "amend"] as const) {
					const files = toSend.filter((f) => f.kind === kind);
					if (files.length === 0) continue;

					const posted = await tracedStep(
						`POST /data/${kind}`,
						{
							timeout: "5 minutes",
							retries: { limit: 3, delay: "30 seconds", backoff: "exponential" },
						},
						() => postStatusFiles(this.env, kind, files),
					);
					if (posted.fatal) throw new Error(posted.fatal);
					outcomes[kind] = posted;

					await tracedStep(`audit · ${kind}`, () =>
						writeAuditLog(this.env, {
							event: "status-report",
							run_id: runId,
							outcome: outcomes[kind].rejected.length === 0 ? "ok" : "failed",
							fields: {
								kind,
								http_status: outcomes[kind].httpStatus,
								files: files.map((f) => f.fileName).join(" "),
								rows: files.reduce((n, f) => n + f.rows, 0),
								accepted: outcomes[kind].accepted.join(" "),
								rejected: outcomes[kind].rejected
									.map((r) => `${r.fileName} (${r.message})`)
									.join("; "),
							},
						}),
					);
				}
			}

			// ⑤ record what DROP accepted
			let itemsMarked = 0;
			for (const kind of ["upload", "amend"] as const) {
				for (const fileName of outcomes[kind].accepted) {
					const file = toSend.find((f) => f.kind === kind && f.fileName === fileName);
					if (!file) continue;

					itemsMarked += await tracedStep(`mark reported · ${fileName}`, async () => {
						const obj = await this.env.r2.get(file.r2Key);
						if (!obj) throw new NonRetryableError(`${file.r2Key} is missing from R2`);
						const items = parseStatusCsv(await obj.text());
						let marked = 0;
						for (let i = 0; i < items.length; i += MARK_BATCH) {
							marked += await markReported(
								this.env,
								file.list_type,
								items.slice(i, i + MARK_BATCH),
							);
						}
						return marked;
					});
				}
			}

			const sum = (key: keyof BuiltFile) =>
				toSend.reduce((n, f) => n + (f[key] as number), 0);
			const rejected = [...outcomes.upload.rejected, ...outcomes.amend.rejected];

			const summary = {
				runId,
				uploaded: upload ? 1 : 0,
				filesBuilt: toSend.length,
				rowsUpload: toSend.filter((f) => f.kind === "upload").reduce((n, f) => n + f.rows, 0),
				rowsAmend: toSend.filter((f) => f.kind === "amend").reduce((n, f) => n + f.rows, 0),
				code2: sum("code2"),
				code3: sum("code3"),
				code4: sum("code4"),
				code5: sum("code5"),
				held: plan.held,
				missingSourceFile: plan.missingSourceFile,
				filesAccepted: outcomes.upload.accepted.length + outcomes.amend.accepted.length,
				filesRejected: rejected.length,
				itemsMarked,
				oldestUnreportedDays: plan.oldestUnreportedDays,
			};

			// ⑥ anything not reported is a failed run, loudly, after the summary
			// has been assembled so the log says how far it got.
			const problems: string[] = [];
			if (rejected.length > 0) {
				problems.push(
					`DROP rejected ${rejected.length} file(s): ` +
						rejected.map((r) => `${r.fileName} (${r.message})`).join("; "),
				);
			}
			if (plan.missingSourceFile > 0) {
				problems.push(
					`${plan.missingSourceFile} live work item(s) have no source_file and were not ` +
						"reported — re-run Cron A on their archived ZIP to backfill it",
				);
			}

			if (problems.length > 0) {
				await logRun(this.env, ctx, "failed", {
					error: problems.join(" | "),
					result: summary,
					duration_ms: Date.now() - startedAt,
				});
				failureLogged = true;
				throw new Error(problems.join(" | "));
			}

			console.log("Cron B finished:", summary);
			await logRun(this.env, ctx, "completed", {
				result: summary,
				duration_ms: Date.now() - startedAt,
			});
			return summary;
		} catch (e) {
			if (!failureLogged) {
				await logRun(this.env, ctx, "failed", {
					error: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
					duration_ms: Date.now() - startedAt,
				});
			}
			throw e;
		}
	}
}
