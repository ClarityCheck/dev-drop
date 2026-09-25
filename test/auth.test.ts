import { SELF } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { requireOperator } from "../worker/auth";

const START_ROUTES = [
	"/api/downloader/start",
	"/api/workflow/start",
	"/api/status-report/start",
] as const;

function start(path: string, authorization?: string) {
	return SELF.fetch(`https://example.com${path}`, {
		method: "POST",
		headers: authorization ? { Authorization: authorization } : {},
		body: JSON.stringify({}),
	});
}

describe("starting Cron A, B and C", () => {
	for (const path of START_ROUTES) {
		it(`refuses ${path} without a token`, async () => {
			const res = await start(path);
			expect(res.status).toBe(401);
			expect(res.headers.get("WWW-Authenticate")).toContain("Bearer");
		});

		it(`refuses ${path} with the wrong token`, async () => {
			const res = await start(path, "Bearer not-the-token");
			expect(res.status).toBe(401);
		});
	}

	it("refuses a token sent without the Bearer scheme", async () => {
		const res = await start("/api/status-report/start", "test-operator");
		expect(res.status).toBe(401);
	});

	it("starts a run for the operator", async () => {
		// Cron B with no cleanupInstanceId: the instance is created, and then
		// refuses at its own gate, so nothing is read or written.
		const res = await start("/api/status-report/start", "Bearer test-operator");
		expect(res.status).toBe(200);
		expect(await res.json()).toMatchObject({ workflow: "drop-status-report" });
	});
});

describe("requireOperator", () => {
	const request = (authorization?: string) =>
		new Request("https://example.com/", {
			headers: authorization ? { Authorization: authorization } : {},
		});

	it("fails closed when the secret is not set", async () => {
		const denied = await requireOperator(request("Bearer anything"), {} as Env);
		expect(denied?.status).toBe(503);
	});

	it("lets the operator through", async () => {
		const env = { DROP_OPERATOR_TOKEN: "s3cret" } as Env;
		expect(await requireOperator(request("Bearer s3cret"), env)).toBeNull();
	});

	it("does not accept a prefix of the secret", async () => {
		const env = { DROP_OPERATOR_TOKEN: "s3cret" } as Env;
		expect((await requireOperator(request("Bearer s3cre"), env))?.status).toBe(401);
	});
});
