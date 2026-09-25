import { env, introspectWorkflowInstance } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { incompleteReason, missingWebsiteDbs } from "../worker/db";
import { planErasures } from "../worker/workflow-reports-cleanup";
import type { MatchResult } from "../worker/workflow-reports-cleanup";
import { planKvRemovals } from "../worker/workflow-downloader";

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
	return {
		linked: 0,
		unlinked: 0,
		inserted: 0,
		statusSet: 0,
		rowsExpired: 0,
		recordsErased: 0,
		enrichmentErased: 0,
		searchHistoryDeleted: 0,
		matchLog: "",
		...over,
	};
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
				chunkResult({
					linked: 10,
					inserted: 10,
					statusSet: 4,
					rowsExpired: 31,
					recordsErased: 2,
					enrichmentErased: 6,
					searchHistoryDeleted: 9,
				}),
			);
			await m.mockStepResult(
				{ name: "record and erase · chunk 2" },
				chunkResult({
					linked: 10,
					inserted: 10,
					statusSet: 3,
					rowsExpired: 12,
					recordsErased: 5,
					enrichmentErased: 1,
					searchHistoryDeleted: 4,
				}),
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
		// The two erasures are summed apart: rows deleted whole for a phone or
		// e-mail identifier, records cut out of a people payload the row keeps.
		expect(output.peopleRecordsErased).toBe(7);
		// The website's copies, summed alongside the reports they came from.
		expect(output.enrichmentRowsErased).toBe(7);
		expect(output.searchHistoryRowsDeleted).toBe(13);
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
 * Which erase a match earns — Rule 2 in one function.
 *
 * The sweep is the primary defence and it is the destructive one, so getting
 * this wrong is not "a report is served for one more cycle": it is thirty-nine
 * strangers irreversibly deleted by ALTER TABLE ... DELETE because a fortieth
 * John Smith registered with DROP.
 */
describe("planErasures", () => {
	const match = (over: Partial<MatchResult> = {}): MatchResult => ({
		list_type: "ndz",
		work_item_id: "wi-1",
		type: "people",
		normalized_value: "john smith",
		hash: "aGFzaA==",
		provider: "combined_people",
		service: "people",
		created_at: "2026-09-16 10:00:00.000",
		element_digest: "ZGlnZXN0LTE=",
		...over,
	});

	it("takes only the matched element of a people report", () => {
		const plan = planErasures([match()]);

		expect(plan.rows).toEqual([]);
		expect(plan.elements).toEqual([
			{ normalized_value: "john smith", element_digest: "ZGlnZXN0LTE=" },
		]);
	});

	it("leaves the other people of the same report alone", () => {
		// Two elements of one report, one listed. Only one digest may travel:
		// the erase is keyed on the digest set, so a second entry here is a
		// second person deleted.
		const plan = planErasures([
			match({ element_digest: "bGlzdGVk" }),
			match({ list_type: "email", work_item_id: "wi-2", element_digest: "bGlzdGVk" }),
		]);

		expect(plan.elements).toHaveLength(1);
		expect(plan.elements[0].element_digest).toBe("bGlzdGVk");
	});

	it("takes every row of a matched phone or e-mail report", () => {
		// The opposite rule, and the one that must not leak into people:
		// every provider row describes the listed consumer.
		const plan = planErasures([
			match({ type: "phone", normalized_value: "4155559317", element_digest: "" }),
			match({
				type: "phone",
				normalized_value: "4155559317",
				work_item_id: "wi-2",
				element_digest: "",
			}),
		]);

		expect(plan.elements).toEqual([]);
		expect(plan.rows).toEqual([{ type: "phone", normalized_value: "4155559317" }]);
	});

	it("keeps the two shapes apart in one chunk", () => {
		const plan = planErasures([
			match(),
			match({ type: "email", normalized_value: "a@example.com", element_digest: "" }),
		]);

		expect(plan.rows).toEqual([{ type: "email", normalized_value: "a@example.com" }]);
		expect(plan.elements).toHaveLength(1);
	});

	it("refuses a people match it cannot place, instead of erasing the row", () => {
		// A v3 view has no element_digest. Failing the chunk leaves the records
		// in place for one more cycle, which is recoverable; widening the erase
		// to the whole row is not.
		expect(() => planErasures([match({ element_digest: "" })])).toThrow(/element_digest/);
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
		unlinked: 0,
		linked: 3,
		workItems: 2,
		inserted: 3,
		workItemIds: ["1", "2"],
		linkedWorkItems: [],
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
		expect(incompleteReason({ ...ok, unlinked: 1 })).toContain("no live row in ca_drop_work_item");
	});

	it("passes when one match links more work items than were submitted", () => {
		// Two consumers sharing a phone: one hash, one match, two work items.
		expect(incompleteReason({ ...ok, submitted: 1, linked: 2, workItems: 2 })).toBeNull();
	});

	it("fails when a match was dropped for an empty identifier", () => {
		expect(incompleteReason({ ...ok, skippedEmpty: 2 })).toContain("empty normalized value");
	});
});

describe("planKvRemovals", () => {
	const holder = (hash: string, work_item_id: string) => ({
		list_type: "phone",
		hash,
		work_item_id,
		request_date: null,
	});

	it("deletes a hash nobody else holds", () => {
		expect(planKvRemovals([{ hash: "h1" }], [])).toEqual([{ hash: "h1", action: "delete" }]);
	});

	it("keeps a shared hash and points it at the consumer who did not withdraw", () => {
		// Ben withdrew; Anna shares the phone and did not. Deleting the key
		// would stop suppressing Anna.
		expect(planKvRemovals([{ hash: "shared" }], [holder("shared", "anna")])).toEqual([
			{ hash: "shared", action: "reassign", holder: holder("shared", "anna") },
		]);
	});

	it("decides each hash once when a removals file names it twice", () => {
		const plan = planKvRemovals([{ hash: "h1" }, { hash: "h1" }, { hash: "h2" }], []);
		expect(plan.map((p) => p.hash)).toEqual(["h1", "h2"]);
	});
});

describe("missingWebsiteDbs", () => {
	it("names every website whose database the Worker cannot reach", () => {
		expect(missingWebsiteDbs({} as Env).map((s) => s.binding)).toEqual(["WEBSITE_CC_DB", "WEBSITE_RL_DB"]);
		const oneBound = { WEBSITE_CC_DB: { connectionString: "postgres://x" } } as unknown as Env;
		expect(missingWebsiteDbs(oneBound).map((s) => s.binding)).toEqual(["WEBSITE_RL_DB"]);
	});

	it("is satisfied in the test Worker, which has both bindings", () => {
		expect(missingWebsiteDbs(env as unknown as Env)).toEqual([]);
	});
});
