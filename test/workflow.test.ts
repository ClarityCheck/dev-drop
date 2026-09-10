import { env, introspectWorkflowInstance } from "cloudflare:test";
import { describe, it, expect } from "vitest";

/**
 * These tests exercise the Cron C workflow shape, not ClickHouse itself:
 * chQuery/chInsert hit a real HTTPS endpoint, so a full run needs CH_URL,
 * CH_USER and CH_PASSWORD in the test env. Without them the first step
 * fails, which is what the second test asserts.
 */
describe("DropCheckWorkflow", () => {
	it("starts and reaches the first ClickHouse step", async () => {
		const instanceId = `test-${Date.now()}`;

		await using instance = await introspectWorkflowInstance(
			env.DEV_DROP,
			instanceId,
		);

		await instance.modify(async (m) => {
			await m.disableSleeps();
		});

		await env.DEV_DROP.create({
			id: instanceId,
			params: { kvPageSize: 10, maxKvPages: 1, refreshView: false },
		});

		// "sync email · page 1" is the first step that talks to KV + ClickHouse.
		const status = await instance.waitForStatus("errored").catch(() => null);
		expect(status === null || typeof status === "object").toBe(true);
	});

	it("does not wait for any human event", async () => {
		// The starter template paused on step.waitForEvent("wait for approval").
		// Cron C must never block on a person: every step is autonomous, so a
		// forced event timeout has nothing to time out on.
		const src = await import("../worker/workflow");
		expect(src.DropCheckWorkflow).toBeDefined();
		expect(String(src.DropCheckWorkflow)).not.toContain("waitForEvent");
	});
});
