// Export the Workflow and Durable Object classes
export { DropReportsCleanupWorkflow } from "./workflow-reports-cleanup";
export { DropDownloaderWorkflow } from "./workflow-downloader";
export { WorkflowStatusDO } from "./durable-object";
export { DropKvRepairWorkflow } from "./workflow-kv-repair";

import { countWorkItems, dbPing, sampleWorkItems } from "./db";
import { dropKey, isDropListType } from "./drop-normalize";
import { IncidentFailed, recordEraseIncident, recordMatchFound } from "./drop-incident";
import type { IncidentStage } from "./drop-incident";
import { lookupGate, recordSuppression } from "./drop-suppression";
import {
	MAX_REPORT_KEYS,
	boundReportFields,
	buildReportKeys,
	countReportKeys,
	exactFieldsOnly,
	extractReportRecords,
	isReportType,
	lookupDropKeys,
	reportGroups,
	subjectFields,
} from "./drop-report";
import type { ReportType } from "./drop-report";

/**
 * The step both incident endpoints share: read the body, re-derive the DROP
 * keys from the report, and confirm against KV that a match really happened.
 *
 * The caller's word is not enough for either call. One writes a row into
 * ca_drop_work_item_match and the other sets the status Cron B reports to
 * California, so a match KV cannot confirm is refused and nothing is written.
 */
async function confirmIncidentMatch(
	env: Env,
	request: Request,
): Promise<
	| { response: Response }
	| {
			type: ReportType;
			entity: { type: string; normalized_value: string };
			hits: Awaited<ReturnType<typeof lookupDropKeys>>["hits"];
			runId?: unknown;
	  }
> {
	const bad = (body: Record<string, unknown>, status: number) => ({
		response: Response.json(body, { status }),
	});

	let body: {
		type?: unknown;
		value?: unknown;
		normalizedValue?: unknown;
		report?: unknown;
		runId?: unknown;
	};
	try {
		body = (await request.json()) as typeof body;
	} catch {
		return bad({ error: "body must be JSON" }, 400);
	}

	const { type, value, normalizedValue, report } = body;
	if (!isReportType(type)) {
		return bad({ error: 'type must be "email", "phone" or "people"' }, 400);
	}
	if (typeof normalizedValue !== "string" || normalizedValue.trim() === "") {
		return bad({ error: "normalizedValue must be a non-empty string" }, 400);
	}
	if (report === null || report === undefined) {
		return bad({ error: "report is required" }, 400);
	}

	const subject = subjectFields(type, typeof value === "string" ? value : undefined);
	const bounded = reportGroups(
		type,
		subject,
		extractReportRecords(report).map((record) => record.fields),
	).map(boundReportFields);

	// Bounded the same way report-check bounds it, and for a sharper reason
	// here: refusing a pathological report would mean an erasure that ALREADY
	// HAPPENED could never be recorded. A subset of the hits is worth having;
	// nothing is not. The exact e-mail and phone keys are never capped.
	let groups = bounded.map((b) => b.fields);
	if (groups.reduce((total, g) => total + countReportKeys(g), 0) > MAX_REPORT_KEYS) {
		groups = groups.map(exactFieldsOnly);
	}

	let hits: Awaited<ReturnType<typeof lookupDropKeys>>["hits"];
	try {
		const keys = await Promise.all(groups.map(buildReportKeys));
		hits = (await lookupDropKeys(env.kv, keys)).hits;
	} catch (e) {
		return bad(
			{
				error: "match could not be confirmed",
				detail: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
				hint: "KV did not answer — nothing was recorded, retry",
			},
			503,
		);
	}

	if (hits.length === 0) {
		return bad(
			{
				error: "no DROP match could be confirmed for this report",
				hint: "nothing was recorded. The data may have been erased for another reason",
			},
			422,
		);
	}

	return {
		type,
		entity: { type, normalized_value: normalizedValue },
		hits,
		runId: body.runId,
	};
}

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
 * - POST /api/drop/report-check - does a whole report touch any DROP key?
 *     Answers `{ type, listed }`, plus per-element `records` for a people
 *     report, whose elements are different people. It writes nothing and
 *     erases nothing, and a check it cannot run in full is a 503, never
 *     `listed: false`. The caller filters before it persists, and Cron C
 *     sweeps what was stored before this check existed.
 * - POST /api/drop/match-found - a cached report has joined the DROP list
 *     and the caller is ABOUT to erase it. Records the match row and the
 *     alert. Call this FIRST: the erase destroys the only other evidence
 *     the consumer was ever in the data.
 * - POST /api/drop/erase-incident - the caller has now erased it: every
 *     row for an e-mail or phone, the matched array elements for a people
 *     report. Verifies the match AND the erasure, then sets the work item
 *     status and writes the R2 evidence.
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
				const { listed, onDropList, source } = await lookupGate(env.kv, hash);
				// listed      do not search, do not serve -- true for both sources
				// onDropList  California's statutory fact, and ONLY that
				// source      which key answered
				//
				// Both are always present. Reading only `listed` gives the
				// cautious behaviour; the statutory fact has to be asked for by
				// name, so it cannot be inherited from an inference of ours.
				return Response.json({
					type,
					listed,
					onDropList,
					...(listed ? { source } : {}),
				});
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
			const groups = reportGroups(
				type,
				subject,
				reportRecords.map((record) => record.fields),
			);

			// The answer is `listed` and nothing else, so anything less than the
			// whole check is not an answer. A report whose cross product does not
			// fit, and one that yields no key at all, both fail here rather than
			// coming back as a reduced or empty "not listed" the caller cannot tell
			// apart from a clean one.
			const candidates = groups.reduce((total, group) => total + countReportKeys(group), 0);
			if (candidates === 0) {
				return Response.json(
					{
						error: "check did not run",
						detail: "no DROP key could be derived from the report",
						hint: "treat as unknown, not as not-listed",
					},
					{ status: 503 },
				);
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

			const { matched } = checked;
			const [subjectMatched, ...rest] = matched;
			const reportListed = matched.some((families) => families.length > 0);

			// The subject is clean but its report is not: the aggregated data
			// carries a listed person, or a listed contact detail of one. Remember
			// the value, so /api/drop/check short-circuits the next search for it
			// instead of paying for the whole funnel again.
			//
			// Nothing here un-remembers it. That cost a Hyperdrive connection and
			// an UPDATE on every clean lookup, and every one of those had nothing
			// to clear; the expiry on the KV key does the same job for nothing.
			//
			// waitUntil, with nothing awaited before it: the answer is what the
			// caller is waiting for, and a cache hint must not delay it or be able
			// to fail it.
			if (type !== "people" && !subjectMatched.length && reportListed) {
				ctx.waitUntil(
					recordSuppression(env, { searchType: type, value: value as string }),
				);
			}

			// A people report's elements are different people, each checked from
			// its own group, so the caller is told which ones matched — it is what
			// lets it remove those and keep the strangers. A phone or email report
			// is one person seen by several providers: the verdict is the report's
			// and there is nothing to name.
			return Response.json({
				type,
				listed: reportListed,
				...(type === "people"
					? {
							records: reportRecords.map((record, i) => ({
								index: record.index,
								listed: (rest[i] ?? []).length > 0,
							})),
						}
					: {}),
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
		if (
			(url.pathname === "/api/drop/match-found" ||
				url.pathname === "/api/drop/erase-incident") &&
			request.method === "POST"
		) {
			const confirmed = await confirmIncidentMatch(env, request);
			if ("response" in confirmed) return confirmed.response;

			const { type, entity, hits } = confirmed;
			const runId =
				typeof confirmed.runId === "string" ? confirmed.runId : crypto.randomUUID();

			// Before the erase: the match row and the alert only. Nothing here
			// claims the data is gone, because it is not yet.
			if (url.pathname === "/api/drop/match-found") {
				try {
					const recorded = await recordMatchFound(env, runId, entity, hits);
					return Response.json({
						type,
						runId,
						recorded,
						next: "erase the data, then POST /api/drop/erase-incident with this runId",
					});
				} catch (e) {
					return Response.json(
						{
							type,
							runId,
							error: "the match was not recorded",
							stage: e instanceof IncidentFailed ? e.stage : "unknown",
							detail: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
							hint: "DO NOT ERASE. Nothing records that this consumer was in the data yet",
						},
						{ status: 500 },
					);
				}
			}

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
