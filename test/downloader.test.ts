import { env, introspectWorkflowInstance } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { readDownload } from "../worker/workflow-downloader";
import { parseCsv, unzip } from "../worker/zip";

type File = { name: string; data: Uint8Array; deflate?: boolean };

async function deflateRaw(data: Uint8Array): Promise<Uint8Array> {
	const stream = new Response(data).body!.pipeThrough(new CompressionStream("deflate-raw"));
	return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function buildZip(files: File[], zip64: boolean): Promise<ArrayBuffer> {
	const encoder = new TextEncoder();
	const chunks: Uint8Array[] = [];
	const central: Uint8Array[] = [];
	let offset = 0;

	const push = (list: Uint8Array[], bytes: Uint8Array) => list.push(bytes);
	const u16 = (v: DataView, at: number, n: number) => v.setUint16(at, n, true);
	const u32 = (v: DataView, at: number, n: number) => v.setUint32(at, n, true);
	const u64 = (v: DataView, at: number, n: number) => v.setBigUint64(at, BigInt(n), true);

	for (const file of files) {
		const name = encoder.encode(file.name);
		const body = file.deflate ? await deflateRaw(file.data) : file.data;
		const method = file.deflate ? 8 : 0;

		const local = new Uint8Array(30 + name.length);
		const lv = new DataView(local.buffer);
		u32(lv, 0, 0x04034b50);
		u16(lv, 4, zip64 ? 45 : 20);
		u16(lv, 8, method);
		u32(lv, 18, body.length);
		u32(lv, 22, file.data.length);
		u16(lv, 26, name.length);
		local.set(name, 30);

		const extraLen = zip64 ? 28 : 0;
		const entry = new Uint8Array(46 + name.length + extraLen);
		const cv = new DataView(entry.buffer);
		u32(cv, 0, 0x02014b50);
		u16(cv, 4, 45);
		u16(cv, 6, zip64 ? 45 : 20);
		u16(cv, 10, method);
		u32(cv, 20, zip64 ? 0xffffffff : body.length);
		u32(cv, 24, zip64 ? 0xffffffff : file.data.length);
		u16(cv, 28, name.length);
		u16(cv, 30, extraLen);
		u32(cv, 42, zip64 ? 0xffffffff : offset);
		entry.set(name, 46);
		if (zip64) {
			const x = 46 + name.length;
			u16(cv, x, 0x0001);
			u16(cv, x + 2, 24);
			u64(cv, x + 4, file.data.length);
			u64(cv, x + 12, body.length);
			u64(cv, x + 20, offset);
		}

		push(chunks, local);
		push(chunks, body);
		push(central, entry);
		offset += local.length + body.length;
	}

	const cdirOffset = offset;
	const cdirSize = central.reduce((n, c) => n + c.length, 0);
	const tail: Uint8Array[] = [];

	if (zip64) {
		const record = new Uint8Array(56);
		const rv = new DataView(record.buffer);
		u32(rv, 0, 0x06064b50);
		u64(rv, 4, 44);
		u16(rv, 12, 45);
		u16(rv, 14, 45);
		u64(rv, 24, files.length);
		u64(rv, 32, files.length);
		u64(rv, 40, cdirSize);
		u64(rv, 48, cdirOffset);

		const locator = new Uint8Array(20);
		const kv = new DataView(locator.buffer);
		u32(kv, 0, 0x07064b50);
		u64(kv, 8, cdirOffset + cdirSize);
		u32(kv, 16, 1);
		tail.push(record, locator);
	}

	const eocd = new Uint8Array(22);
	const ev = new DataView(eocd.buffer);
	u32(ev, 0, 0x06054b50);
	u16(ev, 8, zip64 ? 0xffff : files.length);
	u16(ev, 10, zip64 ? 0xffff : files.length);
	u32(ev, 12, zip64 ? 0xffffffff : cdirSize);
	u32(ev, 16, zip64 ? 0xffffffff : cdirOffset);
	tail.push(eocd);

	const all = [...chunks, ...central, ...tail];
	const out = new Uint8Array(all.reduce((n, c) => n + c.length, 0));
	let p = 0;
	for (const c of all) {
		out.set(c, p);
		p += c.length;
	}
	return out.buffer;
}

const csv = (text: string) => new TextEncoder().encode(text);

const EMAIL_CSV = "Id,Hash\nA7kP2xQ9Lm4R,KA18MT/ph6IHYjzT9zwETySDQyvSh87YuoSBpOQtkhE=\n";
const NDZ_CSV = "Id,Hash\nbK8rT3nV6pZa,PQOfn1RffEKmqMmNAzDKKaoZCwxWbQZkQzPWmQo9REA=\n";

describe("unzip", () => {
	it("reads stored and deflated entries", async () => {
		const zip = await buildZip(
			[
				{ name: "20260910_0000_Email.csv", data: csv(EMAIL_CSV) },
				{ name: "20260910_0000_NDZ.csv", data: csv(NDZ_CSV), deflate: true },
			],
			false,
		);
		const entries = await unzip(zip);
		expect(entries.map((e) => e.name)).toEqual(["20260910_0000_Email.csv", "20260910_0000_NDZ.csv"]);
		expect(parseCsv(entries[1].bytes)).toEqual([
			{ id: "bK8rT3nV6pZa", hash: "PQOfn1RffEKmqMmNAzDKKaoZCwxWbQZkQzPWmQo9REA=" },
		]);
	});

	it("reads a Zip64 archive", async () => {
		const zip = await buildZip(
			[
				{ name: "20260910_0000_Email.csv", data: csv(EMAIL_CSV) },
				{ name: "20260910_0000_NDZ.csv", data: csv(NDZ_CSV), deflate: true },
			],
			true,
		);
		const entries = await unzip(zip);
		expect(entries).toHaveLength(2);
		expect(parseCsv(entries[0].bytes)[0].id).toBe("A7kP2xQ9Lm4R");
		expect(parseCsv(entries[1].bytes)[0].id).toBe("bK8rT3nV6pZa");
	});

	it("refuses a Zip64 marker with no Zip64 record behind it", async () => {
		const zip = new Uint8Array(await buildZip([{ name: "a.csv", data: csv(EMAIL_CSV) }], false));
		const view = new DataView(zip.buffer);
		view.setUint32(zip.length - 6, 0xffffffff, true);
		await expect(unzip(zip.buffer)).rejects.toThrow(/Zip64/);
	});
});

describe("readDownload", () => {
	it("takes a ZIP", async () => {
		const zip = await buildZip([{ name: "20260910_0000_Email.csv", data: csv(EMAIL_CSV) }], false);
		const outcome = await readDownload(new Response(zip, { headers: { "Content-Type": "application/zip" } }));
		expect(outcome.kind).toBe("zip");
	});

	it("recognises 200 no data before anything is stored", async () => {
		const outcome = await readDownload(
			Response.json({ message: "No new consumer request data is available" }),
		);
		expect(outcome).toEqual({ kind: "no-data", message: "No new consumer request data is available" });
	});

	it("retries while DROP prepares the ZIP, and on 429 and 5xx", async () => {
		await expect(readDownload(new Response(null, { status: 202 }))).rejects.toThrow(/202/);
		await expect(readDownload(new Response(null, { status: 429 }))).rejects.toThrow(/429/);
		await expect(readDownload(new Response(null, { status: 500 }))).rejects.toThrow(/500/);
	});

	it("refuses what retrying cannot fix", async () => {
		expect(await readDownload(new Response("bad key", { status: 401 }))).toMatchObject({ kind: "refused" });
		expect(await readDownload(new Response("<html>", { status: 200 }))).toMatchObject({ kind: "refused" });
	});
});

describe("DropDownloaderWorkflow", () => {
	it("stages the archive once, loads KV from the pages and removes the staging", async () => {
		const zip = await buildZip(
			[
				{ name: "20260910_0000_Email.csv", data: csv(EMAIL_CSV) },
				{ name: "20260910_0000_NDZ.csv", data: csv(NDZ_CSV), deflate: true },
			],
			true,
		);
		await env.r2.put("ca-drop/raw/fixture.zip", zip);

		const id = "downloader-stage-test";
		await using instance = await introspectWorkflowInstance(env.DROP_DOWNLOADER, id);
		await instance.modify(async (m) => {
			await m.disableSleeps();
			await m.mockStepResult({ name: "save Supabase · email · page 1" }, 1);
			await m.mockStepResult({ name: "save Supabase · ndz · page 1" }, 1);
		});

		await env.DROP_DOWNLOADER.create({ id, params: { r2Key: "ca-drop/raw/fixture.zip" } });
		await expect(instance.waitForStatus("complete")).resolves.not.toThrow();

		const output = (await instance.getOutput()) as Record<string, unknown>;
		expect(output.loadedKv).toEqual({ email: 1, phone: 0, ndz: 1, namevin: 0 });

		const email = await env.kv.getWithMetadata("KA18MT/ph6IHYjzT9zwETySDQyvSh87YuoSBpOQtkhE=");
		expect(email.value).toBe("A7kP2xQ9Lm4R");
		expect(email.metadata).toMatchObject({ list_type: "email", work_item_id: "A7kP2xQ9Lm4R" });

		const staging = await env.r2.list({ prefix: `ca-drop/staging/${id}/` });
		expect(staging.objects).toEqual([]);
	});
});
