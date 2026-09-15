// Export the Workflow and Durable Object classes
export { DropReportsCleanupWorkflow } from "./workflow-reports-cleanup";
export { DropDownloaderWorkflow } from "./workflow-downloader";
export { WorkflowStatusDO } from "./durable-object";
export { DropKvRepairWorkflow } from "./workflow-kv-repair";

import { countWorkItems, dbPing, sampleWorkItems } from "./db";
import { dropKey, isDropListType } from "./drop-normalize";
import { IncidentFailed, recordEraseIncident } from "./drop-incident";
import type { IncidentStage } from "./drop-incident";
import { lookupGate, recordSuppression } from "./drop-suppression";
import {
	DROP_KEY_FAMILIES,
	MAX_REPORT_KEYS,
	buildReportKeys,
	countReportKeys,
	extractReportRecords,
	isReportType,
	lookupDropKeys,
	subjectFields,
} from "./drop-report";

const INCIDENT_STATUS: Partial<Record<IncidentStage | "unknown", number>> = {
	unverifiable: 503,
	verify: 409,
};

const INCIDENT_HINT: Partial<Record<IncidentStage | "unknown", string>> & { default: string } = {
	unverifiable: "nothing was recorded — retry once ClickHouse answers",
	verify: "delete the rows first, then call again",
	default: "the rows are gone but the trail is incomplete — retry, or let Cron C record it",
};

/**
 * Main Worker fetch handler
 *
 * Handles API routes and WebSocket upgrade requests for workflow management:
 * - POST /api/workflow/start - Create new workflow instance
 * - GET /api/workflow/status/:id - Get workflow status
 * - POST /api/workflow/event/:id - Send events to workflow
 * - GET /ws - WebSocket connection for real-time updates
 * - POST /api/downloader/start - Start the drop-downloader workflow
 * - GET /api/downloader/status/:id - Its status
 * - GET /api/db-test - Postgres reachability, grants and RLS, with real errors
 * - POST /api/drop/check - is this e-mail or phone on the DROP list?
 * - POST /api/drop/report-check - does a whole report touch any DROP key,
 *     and which of its records do? Answers only; it writes nothing and
 *     erases nothing. The caller filters before it persists, and Cron C
 *     sweeps what was stored before this check existed.
 * - POST /api/drop/erase-incident - the caller has already erased a cached
 *     report that has since joined the DROP list: every row for an e-mail
 *     or phone, the matched array elements for a people report. Confirms
 *     the match AND the erasure, then leaves the trail Cron C would have:
 *     match rows, alert, work item status, R2 evidence.
 * - GET /api/kv-health - is the DROP set in KV still complete?
 * - POST /api/kv-repair/start - rebuild KV from Supabase
 */
export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);

		// API: Start a new workflow instance
		if (url.pathname === "/api/workflow/start" && request.method === "POST") {
			try {
				// Optional body: { refreshView?: boolean, batchSize?: number, maxPages?: number }
				let params: Record<string, unknown> = {};
				try {
					params = (await request.json()) as Record<string, unknown>;
				} catch {
					// no body — run with defaults
				}

				const instance = await env.DROP_REPORTS_CLEANUP.create({ params });

				return Response.json({
					instanceId: instance.id,
					message: "Workflow started successfully",
				});
			} catch {
				return Response.json(
					{ error: "Failed to start workflow" },
					{ status: 500 },
				);
			}
		}

		// API: Get workflow status
		if (url.pathname.startsWith("/api/workflow/status/")) {
			const instanceId = url.pathname.split("/").pop();
			if (!instanceId) {
				return Response.json(
					{ error: "Instance ID required" },
					{ status: 400 },
				);
			}

			try {
				const instance = await env.DROP_REPORTS_CLEANUP.get(instanceId);
				const status = await instance.status();
				return Response.json(status);
			} catch {
				return Response.json(
					{ error: "Failed to get workflow status" },
					{ status: 500 },
				);
			}
		}

		// API: Send event to workflow instance
		if (
			url.pathname.startsWith("/api/workflow/event/") &&
			request.method === "POST"
		) {
			const instanceId = url.pathname.split("/").pop();
			if (!instanceId) {
				return Response.json(
					{ error: "Instance ID required" },
					{ status: 400 },
				);
			}

			try {
				const body = (await request.json()) as {
					approved: boolean;
					comment?: string;
				};
				const instance = await env.DROP_REPORTS_CLEANUP.get(instanceId);

				await instance.sendEvent({
					type: "user-approval",
					payload: body,
				});

				return Response.json({
					success: true,
					message: "Event sent successfully",
				});
			} catch {
				return Response.json(
					{ error: "Failed to send event" },
					{ status: 500 },
				);
			}
		}

		// WebSocket: Connect to workflow status updates
		if (url.pathname === "/ws") {
			const instanceId = url.searchParams.get("instanceId");
			if (!instanceId) {
				return new Response("instanceId query parameter required", {
					status: 400,
				});
			}

			const upgradeHeader = request.headers.get("Upgrade");
			if (upgradeHeader !== "websocket") {
				return new Response("Expected Upgrade: websocket", { status: 426 });
			}

			try {
				const doId = env.WORKFLOW_STATUS.idFromName(instanceId);
				const stub = env.WORKFLOW_STATUS.get(doId);
				return stub.fetch(request);
			} catch {
				return new Response("Failed to establish WebSocket connection", {
					status: 500,
				});
			}
		}

		// Cron A: start the downloader.
		// POST /api/downloader/start   body (all optional):
		//   { r2Key?, clearKv?, pageSize?, supabase?, keepParsedPages? }
		if (url.pathname === "/api/downloader/start" && request.method === "POST") {
			let params: Record<string, unknown> = {};
			try {
				params = (await request.json()) as Record<string, unknown>;
			} catch {
				// no body — run with defaults
			}
			const instance = await env.DROP_DOWNLOADER.create({ params });
			return Response.json({ instanceId: instance.id, workflow: "drop-downloader" });
		}

		if (url.pathname.startsWith("/api/downloader/status/")) {
			const instanceId = url.pathname.split("/").pop();
			if (!instanceId) return Response.json({ error: "Instance ID required" }, { status: 400 });
			const instance = await env.DROP_DOWNLOADER.get(instanceId);
			return Response.json(await instance.status());
		}

		// The real-time gate.
		//
		//   POST /api/drop/check   { "type": "phone", "value": "+12012000776" }
		//   -> { "type": "phone", "listed": false }
		//
		// POST rather than GET: an e-mail address or phone number in a query
		// string ends up in access logs, browser history and referrers.
		//
		// The answer is the whole response. Neither the value nor its hash
		// comes back — the caller already has the value, and the hash is a
		// stable identifier for a person that nothing downstream needs in
		// order to act on a yes or a no.
		//
		// The important property is what happens when the check CANNOT run.
		// KV unreachable must never answer "not listed": that is
		// indistinguishable from a clean result and would quietly serve data
		// for someone who asked to be deleted. Failure is a 503 and the caller
		// is expected to treat it as "unknown", not as "no".
		if (url.pathname === "/api/drop/check" && request.method === "POST") {
			let body: { type?: unknown; value?: unknown };
			try {
				body = (await request.json()) as { type?: unknown; value?: unknown };
			} catch {
				return Response.json({ error: "body must be JSON" }, { status: 400 });
			}

			const { type, value } = body;
			if (!isDropListType(type)) {
				return Response.json(
					{ error: 'type must be "email" or "phone"' },
					{ status: 400 },
				);
			}
			if (typeof value !== "string" || value.trim() === "") {
				return Response.json({ error: "value must be a non-empty string" }, { status: 400 });
			}

			const { normalized, hash } = await dropKey(type, value);
			if (normalized === "") {
				// Nothing left after normalization — a phone with no digits, or
				// an e-mail that was only whitespace. Not listed, and not an
				// error, but it never reaches KV.
				return Response.json({ type, listed: false, reason: "empty after normalization" });
			}

			try {
				const { listed, source } = await lookupGate(env.kv, hash);
				// `source` says which of the two answered. "drop" is California's
				// list; "suppressed-report" is our own finding that a report for
				// this value had to be suppressed even though the value itself is
				// not listed. A caller acting on the statutory fact must read it.
				return Response.json({ type, listed, ...(listed ? { source } : {}) });
			} catch (e) {
				return Response.json(
					{
						error: "check did not run",
						detail: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
						hint: "treat as unknown, not as not-listed",
					},
					{ status: 503 },
				);
			}
		}

		if (url.pathname === "/api/drop/report-check" && request.method === "POST") {
			let body: { type?: unknown; value?: unknown; report?: unknown };
			try {
				body = (await request.json()) as typeof body;
			} catch {
				return Response.json({ error: "body must be JSON" }, { status: 400 });
			}

			const { type, value, report } = body;
			if (!isReportType(type)) {
				return Response.json(
					{ error: 'type must be "email", "phone" or "people"' },
					{ status: 400 },
				);
			}
			if (type !== "people" && (typeof value !== "string" || value.trim() === "")) {
				return Response.json(
					{ error: `value must be a non-empty string for type "${type}"` },
					{ status: 400 },
				);
			}
			if (report === null || report === undefined) {
				return Response.json({ error: "report is required" }, { status: 400 });
			}

			const subject = subjectFields(type, typeof value === "string" ? value : undefined);
			const reportRecords = extractReportRecords(report);
			const groups = [subject, ...reportRecords.map((record) => record.fields)];

			const candidates = groups.reduce((total, group) => total + countReportKeys(group), 0);
			if (candidates === 0) {
				return Response.json({
					type,
					listed: false,
					subjectListed: false,
					matched: [],
					keysChecked: 0,
					records: reportRecords.map((record) => ({
						index: record.index,
						...(record.id === undefined ? {} : { id: record.id }),
						listed: false,
						matched: [],
					})),
					reason: "no DROP key could be derived from the report",
				});
			}
			if (candidates > MAX_REPORT_KEYS) {
				return Response.json(
					{
						error: "check did not run",
						detail: `report yields ${candidates} candidate keys, over the ${MAX_REPORT_KEYS} limit`,
						hint: "treat as unknown, not as not-listed",
					},
					{ status: 503 },
				);
			}

			let checked: Awaited<ReturnType<typeof lookupDropKeys>>;
			try {
				const keys = await Promise.all(groups.map(buildReportKeys));
				checked = await lookupDropKeys(env.kv, keys);
			} catch (e) {
				return Response.json(
					{
						error: "check did not run",
						detail: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
						hint: "treat as unknown, not as not-listed",
					},
					{ status: 503 },
				);
			}

			const { keysChecked, matched } = checked;
			const [subjectMatched, ...recordMatched] = matched;

			// The subject is clean but its report is not: the aggregated data
			// carries a listed person, or a listed secondary contact detail. The
			// value will be searched again, and without a record of this the funnel
			// spends a credit and rebuilds a report that gets suppressed again.
			//
			// Only for e-mail and phone, because only those are DROP keys the gate
			// can be asked about. A people search is a name, which /api/drop/check
			// does not accept.
			//
			// waitUntil: the answer is what the caller is waiting for, and a cost
			// optimisation must not delay it or be able to fail it.
			if (
				type !== "people" &&
				!subjectMatched.length &&
				matched.some((families) => families.length > 0)
			) {
				const { hash } = await dropKey(type, value as string);
				const families = DROP_KEY_FAMILIES.filter((family) =>
					recordMatched.some((f) => f.includes(family)),
				);
				ctx.waitUntil(
					recordSuppression(env, {
						searchType: type,
						hash,
						matched: families,
						recordsSuppressed: recordMatched.filter((f) => f.length > 0).length,
						recordsTotal: reportRecords.length,
					}),
				);
			}

			return Response.json({
				type,
				listed: matched.some((families) => families.length > 0),
				subjectListed: subjectMatched.length > 0,
				matched: DROP_KEY_FAMILIES.filter((family) =>
					matched.some((families) => families.includes(family)),
				),
				keysChecked,
				records: reportRecords.map((record, i) => ({
					index: record.index,
					...(record.id === undefined ? {} : { id: record.id }),
					listed: recordMatched[i].length > 0,
					matched: recordMatched[i],
				})),
			});
		}

		// A cached report whose subject has since joined the DROP list.
		//
		//   POST /api/drop/erase-incident
		//   { type, value, normalizedValue, report, rowsDeleted? }
		//
		// The lookup API owns the write path for entity_search_results, so it
		// owns the delete too — it has already erased the data by the time this
		// is called. What it cannot do is leave the trail: the match rows in
		// Supabase, the work item status Cron B reports to California, the Better
		// Stack alert and the R2 evidence file. That is what this does.
		//
		// `report` is the report AS IT MATCHED, before the erasure. It is what
		// identifies which DROP work items were hit, so a caller that sends the
		// already-cleaned version gets a 422 and nothing is recorded.
		//
		// normalizedValue is the caller's own normalized_value, the one it just
		// deleted by. It is not re-derived here: the lookup API's normalization
		// and DROP's are different (normalize-value.ts keeps every digit of a
		// phone, DROP keeps the last ten), and the value that addresses the rows
		// has to be the one that addressed them for the delete.
		if (url.pathname === "/api/drop/erase-incident" && request.method === "POST") {
			let body: {
				type?: unknown;
				value?: unknown;
				normalizedValue?: unknown;
				report?: unknown;
				rowsDeleted?: unknown;
			};
			try {
				body = (await request.json()) as typeof body;
			} catch {
				return Response.json({ error: "body must be JSON" }, { status: 400 });
			}

			const { type, value, normalizedValue, report } = body;
			if (!isReportType(type)) {
				return Response.json(
					{ error: 'type must be "email", "phone" or "people"' },
					{ status: 400 },
				);
			}
			if (typeof normalizedValue !== "string" || normalizedValue.trim() === "") {
				return Response.json(
					{ error: "normalizedValue must be a non-empty string" },
					{ status: 400 },
				);
			}
			if (report === null || report === undefined) {
				return Response.json({ error: "report is required" }, { status: 400 });
			}

			const subject = subjectFields(type, typeof value === "string" ? value : undefined);
			const reportRecords = extractReportRecords(report);
			const groups = [subject, ...reportRecords.map((record) => record.fields)];

			const candidates = groups.reduce((total, group) => total + countReportKeys(group), 0);
			if (candidates > MAX_REPORT_KEYS) {
				return Response.json(
					{
						error: "match could not be confirmed",
						detail: `report yields ${candidates} candidate keys, over the ${MAX_REPORT_KEYS} limit`,
					},
					{ status: 503 },
				);
			}

			let hits: Awaited<ReturnType<typeof lookupDropKeys>>["hits"];
			try {
				const keys = await Promise.all(groups.map(buildReportKeys));
				hits = (await lookupDropKeys(env.kv, keys)).hits;
			} catch (e) {
				return Response.json(
					{
						error: "match could not be confirmed",
						detail: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
						hint: "KV did not answer — nothing was recorded, retry",
					},
					{ status: 503 },
				);
			}

			// The caller says this report matched. If KV disagrees, the erasure
			// was not a DROP erasure, and recording it would put a match into
			// ca_drop_work_item_match that DROP never asked for and mark a work
			// item deleted on the strength of it.
			if (hits.length === 0) {
				return Response.json(
					{
						error: "no DROP match could be confirmed for this report",
						hint: "nothing was recorded. The rows may have been deleted for another reason",
					},
					{ status: 422 },
				);
			}

			const runId = crypto.randomUUID();
			const entity = { type, normalized_value: normalizedValue };

			try {
				const recorded = await recordEraseIncident(env, runId, entity, hits);
				return Response.json({ type, runId, recorded });
			} catch (e) {
				const stage = e instanceof IncidentFailed ? e.stage : "unknown";
				return Response.json(
					{
						type,
						runId,
						error: "the erasure was not fully recorded",
						stage,
						...(e instanceof IncidentFailed ? e.detail : {}),
						detail: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
						hint: INCIDENT_HINT[stage] ?? INCIDENT_HINT.default,
					},
					{ status: INCIDENT_STATUS[stage] ?? 500 },
				);
			}
		}

		// Is the DROP set in KV still complete?
		//
		// KV cannot be counted without listing it, which is one call per 1,000
		// keys — fine inside a job that is listing anyway, far too slow here. So
		// this samples instead: a handful of hashes taken at random from
		// Supabase, looked up in KV one by one. That catches the failure that
		// matters, because if KV has been emptied every sample misses at once,
		// and it catches partial loss with a probability that rises with the
		// sample size.
		//
		// Missing keys are the dangerous direction: the gate stops suppressing
		// someone who asked to be deleted, and a miss looks exactly like never
		// having been listed.
		if (url.pathname === "/api/kv-health") {
			const sampleSize = Math.max(
				1,
				Math.min(500, Number(url.searchParams.get("sample") ?? 100)),
			);
			try {
				const [expected, sample] = await Promise.all([
					countWorkItems(env),
					sampleWorkItems(env, sampleSize),
				]);

				const missing: { list_type: string; work_item_id: string }[] = [];
				for (const row of sample) {
					const hit = await env.kv.get(row.hash);
					if (hit === null) {
						missing.push({ list_type: row.list_type, work_item_id: row.work_item_id });
					}
				}

				const ok = missing.length === 0;
				return Response.json(
					{
						ok,
						workItemsInSupabase: expected,
						sampled: sample.length,
						missingFromKv: missing.length,
						// Capped: a wiped namespace would otherwise list the lot.
						missing: missing.slice(0, 20),
						verdict: ok
							? "every sampled hash is present in KV"
							: `${missing.length} of ${sample.length} sampled hashes are MISSING from KV — ` +
								"the real-time gate is under-suppressing. Run POST /api/kv-repair/start",
					},
					{ status: ok ? 200 : 500 },
				);
			} catch (e) {
				return Response.json(
					{ ok: false, error: e instanceof Error ? `${e.name}: ${e.message}` : String(e) },
					{ status: 500 },
				);
			}
		}

		// Rebuild KV from Supabase. Body (all optional):
		//   { pageSize?, maxPages?, afterId?, dryRun? }
		if (url.pathname === "/api/kv-repair/start" && request.method === "POST") {
			let params: Record<string, unknown> = {};
			try {
				params = (await request.json()) as Record<string, unknown>;
			} catch {
				// no body — run with defaults
			}
			const instance = await env.DROP_KV_REPAIR.create({ params });
			return Response.json({ instanceId: instance.id, workflow: "drop-kv-repair" });
		}

		if (url.pathname.startsWith("/api/kv-repair/status/")) {
			const instanceId = url.pathname.split("/").pop();
			if (!instanceId) return Response.json({ error: "Instance ID required" }, { status: 400 });
			const instance = await env.DROP_KV_REPAIR.get(instanceId);
			return Response.json(await instance.status());
		}

		// Diagnostic: one round trip to Postgres, reporting whatever went wrong
		// rather than the "CONNECTION_CLOSED" the workflow sees. The real cause
		// happens on the Hyperdrive -> Supabase hop and never reaches the
		// Worker as itself.
		if (url.pathname === "/api/db-test") {
			const result = await dbPing(env);
			return Response.json(result, { status: result.ok ? 200 : 500 });
		}

		return Response.json({ error: "Not Found" }, { status: 404 });
	},
} satisfies ExportedHandler<Env>;
