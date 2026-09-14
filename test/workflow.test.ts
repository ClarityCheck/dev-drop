import { env, introspectWorkflowInstance } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { incompleteReason } from "../worker/db";

/**
 * Cron C's batch loop, tested without ClickHouse, KV or Supabase.
 *
 * The batch step itself is a thin wrapper around three services that all need
 * credentials, so it is mocked. What is worth testing is what surrounds it:
 * that the loop keeps going until a batch reports `done`, that it carries the
 * cursor from one batch to the next, that it sums the counts, and that the
 * safety rail stops a run rather than letting it spin.
 */

/** A mocked return from `runBatch` — same shape the real one produces. */
function batchResult(over: Partial<Record<string, unknown>> = {}) {
	return {
		keysRead: 0,
		keysCompared: 0,
		rowsReturned: 0,
		matchesFound: 0,
		linked: 0,
		inserted: 0,
		matchLog: "",
		done: false,
		cursorType: "",
		cursorValue: "",
		cursorOffset: 0,
		...over,
	};
}

describe("DropReportsCleanupWorkflow", () => {
	it("keeps batching until one reports done, and sums what they found", async () => {
		const instanceId = `test-sums-${Date.now()}`;

		await using instance = await introspectWorkflowInstance(
			env.DROP_REPORTS_CLEANUP,
			instanceId,
		);

		await instance.modify(async (m) => {
			await m.disableSleeps();
			await m.mockStepResult(
				{ name: "scan · batch 1" },
				batchResult({
					keysRead: 100,
					keysCompared: 90,
					rowsReturned: 4,
					matchesFound: 3,
					linked: 2,
					inserted: 2,
					cursorType: "email",
					cursorValue: "someone@example.test",
					cursorOffset: 25,
				}),
			);
			await m.mockStepResult(
				{ name: "scan · batch 2" },
				batchResult({
					keysRead: 10,
					keysCompared: 10,
					rowsReturned: 2,
					matchesFound: 1,
					linked: 1,
					inserted: 0, // already recorded by an earlier run
					done: true,
				}),
			);
		});

		await env.DROP_REPORTS_CLEANUP.create({
			id: instanceId,
			// refreshView off: SYSTEM REFRESH VIEW is the one step that cannot be
			// reached without ClickHouse credentials.
			params: { refreshView: false },
		});

		await instance.waitForStatus("complete");
		const output = (await instance.getOutput()) as Record<string, number>;

		expect(output.batches).toBe(2);
		expect(output.keysRead).toBe(110);
		expect(output.keysCompared).toBe(100);
		expect(output.viewRowSlicesRead).toBe(6);
		expect(output.matchesFound).toBe(4);
		expect(output.matchesLinked).toBe(3);
		expect(output.matchRowsInserted).toBe(2);
		// Found but not resolvable to a work item: KV and Supabase have drifted.
		expect(output.matchesUnlinked).toBe(1);
	});

	it("stops at maxBatches rather than looping on a cursor that never ends", async () => {
		const instanceId = `test-rail-${Date.now()}`;

		await using instance = await introspectWorkflowInstance(
			env.DROP_REPORTS_CLEANUP,
			instanceId,
		);

		await instance.modify(async (m) => {
			await m.disableSleeps();
			// Never `done`, so only the safety rail can end this run.
			await m.mockStepResult({ name: "scan · batch 1" }, batchResult({ keysRead: 1 }));
			await m.mockStepResult({ name: "scan · batch 2" }, batchResult({ keysRead: 1 }));
		});

		await env.DROP_REPORTS_CLEANUP.create({
			id: instanceId,
			params: { refreshView: false, maxBatches: 2 },
		});

		await instance.waitForStatus("errored");
		const error = await instance.getError();
		expect(error.message).toContain("stopped after 2 batches");
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
 * What counts as "the match was recorded". Cron C fails the run when this
 * returns a reason, so a false positive here stops a healthy pipeline and a
 * false negative loses a deletion request quietly.
 */
describe("incompleteReason", () => {
	const ok = { submitted: 3, skippedEmpty: 0, linked: 3, workItems: 2, inserted: 3, statusSet: 2 };

	it("passes when every match is linked and every work item has the status", () => {
		expect(incompleteReason(ok)).toBeNull();
	});

	it("passes when a retry re-runs the write and inserts nothing new", () => {
		// The rows were already written by the previous attempt. inserted drops
		// to 0 while the end state is still correct -- this must not be read as
		// a failure, or no retried batch could ever succeed.
		expect(incompleteReason({ ...ok, inserted: 0 })).toBeNull();
	});

	it("passes when the status was already set by an earlier batch", () => {
		// A work item matched again in a later batch is normal: NDZ and NameVIN
		// hashes stand for a person, so each of their identifiers turns up
		// separately. Nothing is rewritten -- statusSet counts the rows that
		// already carry it -- and that must still read as recorded.
		expect(incompleteReason({ ...ok, inserted: 0, statusSet: 2 })).toBeNull();
	});

	it("fails when a match has no work item to link to", () => {
		expect(incompleteReason({ ...ok, linked: 1 })).toContain("no row in ca_drop_work_item");
	});

	it("fails when a work item did not get status = deleted", () => {
		expect(incompleteReason({ ...ok, statusSet: 1 })).toContain("status = 'deleted'");
	});

	it("fails when a match was dropped for an empty identifier", () => {
		expect(incompleteReason({ ...ok, skippedEmpty: 2 })).toContain("empty normalized value");
	});
});
