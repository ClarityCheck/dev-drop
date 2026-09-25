import { env, introspectWorkflowInstance } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import {
	parseStatusCsv,
	parseUploadResponse,
	renderStatusCsv,
	reportSuffix,
	responseFileName,
} from "../worker/drop-status";
import { cleanupGateFailure } from "../worker/workflow-status-report";

describe("response file naming", () => {
	it("names the response after the downloaded file, with the run's suffix", () => {
		expect(responseFileName("20260910_0000_NDZ.csv", "Uabc123def")).toBe(
			"20260910_0000_NDZ_Uabc123def.csv",
		);
	});

	it("replaces a suffix the downloaded file already had", () => {
		expect(responseFileName("ca-drop/x/20260910_0000_Email_part2.csv", "A1")).toBe(
			"20260910_0000_Email_A1.csv",
		);
	});

	it("keeps an upload and an amend of the same file apart", () => {
		const run = "3f2a9c1e-77b0-4d2e-9a1b-000000000000";
		expect(reportSuffix(run, "upload")).toBe("U3f2a9c1e7");
		expect(reportSuffix(run, "amend")).toBe("A3f2a9c1e7");
		expect(reportSuffix(run, "upload")).toHaveLength(10);
	});

	it("refuses a suffix the spec does not allow", () => {
		expect(() => responseFileName("20260910_0000_NDZ.csv", "too-long-suffix")).toThrow();
		expect(() => responseFileName("20260910_0000_NDZ.csv", "")).toThrow();
	});

	it("refuses a source file that does not follow DROP's pattern", () => {
		expect(() => responseFileName("ndz.csv", "U1")).toThrow(/YYYYMMDD/);
	});
});

describe("the status file", () => {
	it("writes the header DROP expects and one row per item", () => {
		expect(
			renderStatusCsv([
				{ work_item_id: "A7kP2xQ9Lm4R", code: 3 },
				{ work_item_id: "bK8rT3nV6pZa", code: 5 },
			]),
		).toBe("Id,Status\nA7kP2xQ9Lm4R,3\nbK8rT3nV6pZa,5\n");
	});

	it("writes a header alone for an empty list", () => {
		expect(renderStatusCsv([])).toBe("Id,Status\n");
	});

	it("refuses a code DROP does not define", () => {
		expect(() => renderStatusCsv([{ work_item_id: "x", code: 1 }])).toThrow(/status code/);
	});

	it("refuses an id that would break an unquoted CSV", () => {
		expect(() => renderStatusCsv([{ work_item_id: "a,b", code: 5 }])).toThrow(/unquoted/);
	});

	it("reads back what it wrote", () => {
		const rows = [
			{ work_item_id: "A7kP2xQ9Lm4R", code: 2 },
			{ work_item_id: "x92LmQ8Zp3Rt", code: 4 },
		];
		expect(parseStatusCsv(renderStatusCsv(rows))).toEqual(rows);
	});
});

describe("DROP's upload answer", () => {
	it("reads the accepted and rejected file names", () => {
		expect(
			parseUploadResponse({
				message: "queued",
				acceptedCount: 1,
				rejectedCount: 1,
				accepted: [{ fileName: "a.csv", fileSizeBytes: 10 }],
				rejected: [{ fileName: "b.csv", message: "bad header" }],
			}),
		).toEqual({ accepted: ["a.csv"], rejected: [{ fileName: "b.csv", message: "bad header" }] });
	});

	it("does not mistake an unrelated body for an answer", () => {
		expect(parseUploadResponse({ error: "nope" })).toBeNull();
		expect(parseUploadResponse(null)).toBeNull();
	});
});

describe("the gate on Cron C", () => {
	const at = "2026-09-20T10:00:00.000Z";
	const good = { dryRun: 0, kvSynced: 1, viewRefreshed: 1, startedAt: at, uncheckedReports: 0 };
	const before = Date.parse(at) - 60_000;
	const after = Date.parse(at) + 60_000;

	it("accepts a complete sweep that began after the newest work item", () => {
		expect(cleanupGateFailure({ status: "complete", output: good }, before)).toBeNull();
	});

	it("refuses a run that has not finished", () => {
		expect(cleanupGateFailure({ status: "running", output: good }, before)).toMatch(/not complete/);
	});

	it("refuses a sweep that began before the newest work item arrived", () => {
		// The case the gate exists for: Cron A loaded new items after Cron C
		// looked, so their absence of a status means nothing yet.
		expect(cleanupGateFailure({ status: "complete", output: good }, after)).toMatch(/before the newest/);
	});

	it("refuses a dry run, a skipped sync and a skipped refresh", () => {
		expect(cleanupGateFailure({ status: "complete", output: { ...good, dryRun: 1 } }, before)).toMatch(/dry run/);
		expect(cleanupGateFailure({ status: "complete", output: { ...good, kvSynced: 0 } }, before)).toMatch(/KV sync/);
		expect(
			cleanupGateFailure({ status: "complete", output: { ...good, viewRefreshed: 0 } }, before),
		).toMatch(/refresh/);
	});

	it("refuses a sweep that could not check every report, unless told to accept it", () => {
		const partial = { status: "complete", output: { ...good, uncheckedReports: 4 } };
		expect(cleanupGateFailure(partial, before)).toMatch(/4 report\(s\) over the key limit/);
		expect(cleanupGateFailure(partial, before, true)).toBeNull();
	});

	it("refuses a run from before Cron C counted what it could not check", () => {
		const old: Record<string, unknown> = { ...good };
		delete old.uncheckedReports;
		expect(cleanupGateFailure({ status: "complete", output: old }, before)).toMatch(/could not check/);
	});

	it("refuses a run from before Cron C recorded when it started", () => {
		const old = { dryRun: 0, kvSynced: 1, viewRefreshed: 1 };
		expect(cleanupGateFailure({ status: "complete", output: old }, before)).toMatch(/startedAt/);
	});
});

const PLAN = {
	groups: [
		{ source_file: "20260910_0000_NDZ.csv", list_type: "ndz", kind: "upload", rows: 3 },
		{ source_file: "20260910_0000_Email.csv", list_type: "email", kind: "amend", rows: 1 },
	],
	missingSourceFile: 0,
	held: 2,
	oldestUnreportedDays: 12,
};

function builtFile(over: Record<string, unknown>) {
	return {
		fileName: "",
		r2Key: "",
		list_type: "ndz",
		kind: "upload",
		rows: 1,
		code2: 0,
		code3: 0,
		code4: 0,
		code5: 1,
		...over,
	};
}

describe("DropStatusReportWorkflow", () => {
	it("refuses to run without naming the Cron C run it rests on", async () => {
		const instanceId = `test-b-nogate-${Date.now()}`;
		await using instance = await introspectWorkflowInstance(env.DROP_STATUS_REPORT, instanceId);
		await env.DROP_STATUS_REPORT.create({ id: instanceId, params: {} });

		await instance.waitForStatus("errored");
		expect((await instance.getError()).message).toContain("cleanupInstanceId is required");
	});

	it("builds and archives without sending when upload is off", async () => {
		const instanceId = `test-b-build-${Date.now()}`;
		await using instance = await introspectWorkflowInstance(env.DROP_STATUS_REPORT, instanceId);

		await instance.modify(async (m) => {
			await m.mockStepResult({ name: "plan" }, PLAN);
			await m.mockStepResult(
				{ name: "build · upload · 20260910_0000_NDZ.csv" },
				builtFile({ rows: 3, code3: 1, code5: 2 }),
			);
			await m.mockStepResult(
				{ name: "build · amend · 20260910_0000_Email.csv" },
				builtFile({ kind: "amend", list_type: "email", rows: 1, code3: 1, code5: 0 }),
			);
		});

		await env.DROP_STATUS_REPORT.create({
			id: instanceId,
			params: { skipCleanupGate: true },
		});

		await instance.waitForStatus("complete");
		const out = (await instance.getOutput()) as Record<string, number>;
		expect(out.uploaded).toBe(0);
		expect(out.filesBuilt).toBe(2);
		expect(out.rowsUpload).toBe(3);
		expect(out.rowsAmend).toBe(1);
		expect(out.code3).toBe(2);
		expect(out.code5).toBe(2);
		expect(out.held).toBe(2);
		expect(out.itemsMarked).toBe(0);
	});

	it("marks only what DROP accepted, and fails the run on a rejection", async () => {
		const instanceId = `test-b-reject-${Date.now()}`;
		await using instance = await introspectWorkflowInstance(env.DROP_STATUS_REPORT, instanceId);

		const ndz = "20260910_0000_NDZ_Utestbrej.csv";
		const email = "20260910_0000_Email_Atestbrej.csv";

		await instance.modify(async (m) => {
			await m.mockStepResult({ name: "plan" }, PLAN);
			await m.mockStepResult(
				{ name: "build · upload · 20260910_0000_NDZ.csv" },
				builtFile({ fileName: ndz, rows: 3 }),
			);
			await m.mockStepResult(
				{ name: "build · amend · 20260910_0000_Email.csv" },
				builtFile({ fileName: email, kind: "amend", list_type: "email" }),
			);
			await m.mockStepResult(
				{ name: "POST /data/upload" },
				{ accepted: [ndz], rejected: [], httpStatus: 202 },
			);
			await m.mockStepResult(
				{ name: "POST /data/amend" },
				{ accepted: [], rejected: [{ fileName: email, message: "unknown Id" }], httpStatus: 400 },
			);
			await m.mockStepResult({ name: `mark reported · ${ndz}` }, 3);
		});

		await env.DROP_STATUS_REPORT.create({
			id: instanceId,
			params: { skipCleanupGate: true, upload: true },
		});

		await instance.waitForStatus("errored");
		expect((await instance.getError()).message).toContain(`DROP rejected 1 file(s): ${email}`);
		expect(await instance.waitForStepResult({ name: `mark reported · ${ndz}` })).toBe(3);
	});
});
