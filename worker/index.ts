// Export the Workflow and Durable Object classes
export { DropReportsCleanupWorkflow } from "./workflow-reports-cleanup";
export { DropDownloaderWorkflow } from "./workflow-downloader";
import { chSmokeTest } from "./ch";
import { dbSmokeTest } from "./db";
import { logsSmokeTest } from "./logs";
export { WorkflowStatusDO } from "./durable-object";

/**
 * Main Worker fetch handler
 *
 * Handles API routes and WebSocket upgrade requests for workflow management:
 * - POST /api/workflow/start - Create new workflow instance
 * - GET /api/workflow/status/:id - Get workflow status
 * - POST /api/workflow/event/:id - Send events to workflow
 * - GET /ws - WebSocket connection for real-time updates
 */
export default {
	async fetch(request: Request, env: Env): Promise<Response> {
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

		// Diagnostics: verify the wiring before running anything.
		// GET /api/ch-test   GET /api/db-test   GET /api/logs-test
		const diagnostics: Record<string, (e: Env) => Promise<Response>> = {
			"/api/ch-test": chSmokeTest,
			"/api/db-test": dbSmokeTest,
			"/api/logs-test": logsSmokeTest,
		};
		const diagnostic = diagnostics[url.pathname];
		if (diagnostic) {
			try {
				return await diagnostic(env);
			} catch (e) {
				// A diagnostic that 500s tells you nothing. Report the throw instead.
				return Response.json({
					ok: false,
					error: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
					where: url.pathname,
				});
			}
		}

		return Response.json({ error: "Not Found" }, { status: 404 });
	},
} satisfies ExportedHandler<Env>;
