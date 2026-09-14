import { env, introspectWorkflowInstance } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { incompleteReason } from "../worker/db";

/**
 * Cron C's control flow, tested without ClickHouse, KV or Supabase.
 *
 * The steps themselves are thin wrappers around three services that all need
 * credentials, so they are mocked. What is worth testing is what surrounds
 * them: that the chunk loop covers the match count, that it sums the results,
 * and that the safety rail stops a run rather than letting it spin.
 */

/** A mocked return from handleChunk — the same shape the real one produces. */
function chunkResult(over: Record<string, unknown> = {}) {
	return { linked: 0, inserted: 0, statusSet: 0, rowsExpired: 0, matchLog: "", ...over };
}

describe("DropReportsCleanupWorkflow", () => {
	it("chunks the match count and sums what each chunk did", async () => {
		const instanceId = `test-chunks-${Date.now()}`;

		await using instance = await introspectWorkflowInstance(
			env.DROP_REPORTS_CLEANUP,
			instanceId,
		);

		await instance.modify(async (m) => {
			await m.disableSleeps();
			// 25 matches at 10 per chunk is three chunks, the last one short.
			await m.mockStepResult({ name: "match" }, 25);
			await m.mockStepResult(
				{ name: "record and erase · chunk 1" },
				chunkResult({ linked: 10, inserted: 10, statusSet: 4, rowsExpired: 31 }),
			);
			await m.mockStepResult(
				{ name: "record and erase · chunk 2" },
				chunkResult({ linked: 10, inserted: 10, statusSet: 3, rowsExpired: 12 }),
			);
			await m.mockStepResult(
				{ name: "record and erase · chunk 3" },
				// inserted 0: a previous attempt had already written these rows
				chunkResult({ linked: 5, inserted: 0, statusSet: 2, rowsExpired: 7 }),
			);
		});

		await env.DROP_REPORTS_CLEANUP.create({
			id: instanceId,
			// Skips the three steps that cannot be reached without credentials.
			params: { refreshView: false, skipKvSync: true, keepDropSet: true, matchChunk: 10 },
		});

		await instance.waitForStatus("complete");
		const output = (await instance.getOutput()) as Record<string, number>;

		expect(output.chunks).toBe(3);
		expect(output.matchesFound).toBe(25);
		expect(output.matchesLinked).toBe(25);
		expect(output.matchRowsInserted).toBe(20);
		expect(output.workItemsMarkedDeleted).toBe(9);
		expect(output.entityRowsExpired).toBe(50);
		expect(output.matchesUnlinked).toBe(0);
	});

	// There is no test for "nothing matched", and not for want of trying:
	// mockStepResult({ name: "match" }, 0) is silently ignored, because the
	// harness treats a falsy result as no mock at all. The real step then runs
	// and fails on ClickHouse credentials. The zero path is one `if` and the
	// summary arithmetic above covers the rest.

	it("stops at maxChunks rather than looping", async () => {
		const instanceId = `test-rail-${Date.now()}`;

		await using instance = await introspectWorkflowInstance(
			env.DROP_REPORTS_CLEANUP,
			instanceId,
		);

		await instance.modify(async (m) => {
			await m.disableSleeps();
			await m.mockStepResult({ name: "match" }, 1_000_000);
			await m.mockStepResult({ name: "record and erase · chunk 1" }, chunkResult());
			await m.mockStepResult({ name: "record and erase · chunk 2" }, chunkResult());
		});

		await env.DROP_REPORTS_CLEANUP.create({
			id: instanceId,
			params: {
				refreshView: false,
				skipKvSync: true,
				keepDropSet: true,
				matchChunk: 1,
				maxChunks: 2,
			},
		});

		await instance.waitForStatus("errored");
		const error = await instance.getError();
		expect(error.message).toContain("stopped after 2 chunks");
	});

	it("does not wait for any human event", async () => {
		// The starter template paused on step.waitForEvent("wait for approval").
		// Cron C must never block on a person: every step is autonomous, so a
		// forced event timeout has nothing to time out on.
		const src = await import("../worker/workflow-reports-cleanup");
		expect(src.DropReportsCleanupWorkflow).toBeDefined();
		expect(String(src.DropReportsCleanupWorkflow)).not.toContain("waitForEvent");
	});
});

/**
 * What counts as the match rows having landed. Cron C fails the run when this
 * returns a reason, so a false positive stops a healthy pipeline and a false
 * negative loses a deletion request quietly.
 *
 * The status is NOT judged here. It is written later, and only once the
 * records are gone from ClickHouse.
 */
describe("incompleteReason", () => {
	const ok = {
		submitted: 3,
		skippedEmpty: 0,
		linked: 3,
		workItems: 2,
		inserted: 3,
		workItemIds: ["1", "2"],
	};

	it("passes when every match linked to a work item", () => {
		expect(incompleteReason(ok)).toBeNull();
	});

	it("passes when a retry re-runs the write and inserts nothing new", () => {
		// The rows were already written by the previous attempt. inserted drops
		// to 0 while the end state is still correct -- this must not read as a
		// failure, or no retried chunk could ever succeed.
		expect(incompleteReason({ ...ok, inserted: 0 })).toBeNull();
	});

	it("fails when a match has no work item to link to", () => {
		expect(incompleteReason({ ...ok, linked: 1 })).toContain("no row in ca_drop_work_item");
	});

	it("fails when a match was dropped for an empty identifier", () => {
		expect(incompleteReason({ ...ok, skippedEmpty: 2 })).toContain("empty normalized value");
	});
});
