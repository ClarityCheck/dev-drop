import { SELF, env } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { sha256Base64 } from "../worker/drop-normalize";
import {
	buildReportKeys,
	countReportKeys,
	extractReportFields,
	lookupDropKeys,
	normalizeDob,
	normalizeName,
	normalizeVin,
	normalizeZip,
	withSearchedValue,
} from "../worker/drop-report";

const CLICKHOUSE_NAME_VECTORS: { raw: string; normalized: string; hash: string }[] = [
	{ raw: "Anna", normalized: "anna", hash: "VVebVXiW0M4XZMR/7WRPmzX1i61iBnSvI/NW2A7QxQM=" },
	{ raw: "O'Brien", normalized: "obrien", hash: "tMtssz/kuGWGjeglAjoeJ5DcEqwB7MjXxa/oJUBxyLo=" },
	{ raw: "José", normalized: "jose", hash: "HsTtA3dmqhgdiECtBLn8bhlf033twEyYpXZ6Z9N1js4=" },
	{ raw: "Müller", normalized: "muller", hash: "qLH8g8uSrZC3MZN892EW72waukNvjQ0OxnPo00AirKg=" },
	{ raw: "Иванов", normalized: "ivanov", hash: "XADYpQziZ5wwj1rxgLAUMCgs1snfav0OfMyQorOVVIg=" },
	{ raw: "Ёлкина", normalized: "yolkina", hash: "76+cWLeQDX12pDMJWw+j6jNgjkh09RHp/1ovYf8WuMU=" },
	{
		raw: "Щербакова",
		normalized: "shcherbakova",
		hash: "lsADHYr3cQfIKD6Eq/o+SfsxvBHaIT7MuwP/n9sDVHQ=",
	},
	{
		raw: "Θεοδώρου",
		normalized: "theodoroy",
		hash: "oTqNXryIuGLEAMz7fjHhP/mOztw60RTlUBKXMZ4/vSA=",
	},
	{ raw: "Straße", normalized: "strasse", hash: "FtlpUgh3dP7gabdYXTmRsk2QwYHAmyEptJCMNbqn8MA=" },
	{ raw: "Æther", normalized: "aether", hash: "EGOFS79RVbz5/oz5tXjQ4z8ggj9nA67lRjvIikH4+aQ=" },
	{
		raw: "ĲsselMeer",
		normalized: "ijsselmeer",
		hash: "th8PITVjuws7ka23un+CahVuMTn0UIZP0c0hASn3P6I=",
	},
	{ raw: "田中", normalized: "田中", hash: "GAZJa8t1PCgPk0u94hpBrVeTAYcmaBwDjanPyKjIKiA=" },
	{ raw: "キムラ", normalized: "キムラ", hash: "8Qqz5cJl/Y2fQSPJcnsaFOYdS92Q456G5TFQ2h0Sh40=" },
	{
		raw: "김",
		normalized: "김",
		hash: "LiYobxObm0pDeis43MPt2cPTgEshVWpyJuVFVveod8A=",
	},
	{ raw: "محمد", normalized: "محمد", hash: "DLTJshBi3k0sDt1I78n6hrl/3IpqTn+nCSVE+E4SH2E=" },
	{ raw: "שלום", normalized: "שלום", hash: "t6wDmO90GTq3OLId8JEjKTA98Q2GjCusN6hKWdEsDi8=" },
	{
		raw: "Mary-Jane",
		normalized: "maryjane",
		hash: "8I9Eil56ncNhm7fBKfan1fxq8ALOoXrXHf3Bxo9NTg4=",
	},
	{
		raw: "  van  der  Berg ",
		normalized: "vanderberg",
		hash: "qtYHyjEh1dhDws1k6Mc8KiT6Oky4kO2NQKHvqK96FNU=",
	},
	{
		raw: "D'Angelo-Smith",
		normalized: "dangelosmith",
		hash: "8VQzh4kvp9WTjCm6FiucHZgGyeIaiBSlfgkl8kGthnw=",
	},
	{ raw: "ŁUKASZ", normalized: "lukasz", hash: "1uaqm/N1tDX5/ef2QSoktDyihGVEi6Mz+l7Fdydg4mA=" },
	{ raw: "Ħaġar", normalized: "hagar", hash: "t4eRwU4k1k+cWDZEcnd4ee6dGVz2d/0plpYnohl5QFs=" },
	{ raw: "ŊAMBI", normalized: "nambi", hash: "a9FFmHEi6zW/WgPFCZIY3HbggcZD786bbQEytPdkeLU=" },
	{ raw: "ıI", normalized: "ii", hash: "XX9JRJqyLerCLXZ7iVScVUE0yOR95NOOdIBJh1yDUDs=" },
	{ raw: "ЪЬЫ", normalized: "y", hash: "ofzkNjhU/4iM/0uOeHXWAMJoI5BBKoz3mzfQsRFIsPo=" },
	{
		raw: "ÅNGSTRÖM",
		normalized: "angstrom",
		hash: "s1cnzum/pCI/05/VMzxZkx6oQPPqX8MgYaQLNmzjUNA=",
	},
];

const CLICKHOUSE_DOB_VECTORS: [string, string][] = [
	["1980-01-01", "19800101"],
	["19800101", "19800101"],
	["2001-12-31T00:00:00Z", "20011231"],
	["1980-01-01 00:00:00", "19800101"],
	["2020-02-29", "20200229"],
	["1980/1/1", ""],
	["1980-01", ""],
	["1899-01-01", ""],
	["01/01/1980", ""],
	["", ""],
];

const CLICKHOUSE_ZIP_VECTORS: [string, string][] = [
	["90210", "90210"],
	["90210-1234", "90210"],
	[" 90210 ", "90210"],
	["02134", "2134"],
	["00501", "501"],
	["K1A 0B1", "k1a0b"],
	["k1a-0b1", "k1a"],
	["SW1A 1AA", "sw1a1"],
	["000000123456", "12345"],
	["0000", ""],
	["", ""],
];

const CLICKHOUSE_VIN_VECTORS: [string, string][] = [
	["1HGCM82633A004352", "1hgcm82633a004352"],
	["1hgcm82633a004352", "1hgcm82633a004352"],
	[" 1HGCM82633A-004352 ", "1hgcm82633a004352"],
	["---", ""],
	["", ""],
];

const CLICKHOUSE_COMBINED_VECTORS = [
	{
		first: "Anna",
		last: "Smith",
		dob: "1980-01-01",
		zip: "90210",
		vin: "1HGCM82633A004352",
		ndz: "5RRCy3l8zb+dLIRMmnMqesH9HFSAo2+uxsvktR51KQs=",
		namevin: "3X7p8p8JRP0Boz4JqChzZprEUuPk1Hg6NGjWPTn1gdg=",
	},
	{
		first: "José",
		last: "Müller",
		dob: "2001-12-31T00:00:00Z",
		zip: "02134",
		vin: "1hgcm82633a-004352",
		ndz: "f6/CwewrKt9FDefHidFdo6IklOAJJio8UAj/YHqmc14=",
		namevin: "rutk/UglYejFRxucH/yD8Y0/u/w05vEIx7FWbnCAw38=",
	},
	{
		first: "Иванов",
		last: "Щербакова",
		dob: "19991231",
		zip: "K1A 0B1",
		vin: "JH4KA7561PC008269",
		ndz: "99wpFeoE8ZmLYaGzFZpJ5D8VEK8E6HiaY/heuSvw1FE=",
		namevin: "AaIOsyWbDu6Kj7/x9t7mk+Pqq3B57HzTJzxN30WtJ1U=",
	},
];

describe("name normalization matches ca_drop_combined_search_result", () => {
	for (const vector of CLICKHOUSE_NAME_VECTORS) {
		it(`${JSON.stringify(vector.raw)} -> ${JSON.stringify(vector.normalized)}`, async () => {
			expect(normalizeName(vector.raw)).toBe(vector.normalized);
			expect(await sha256Base64(normalizeName(vector.raw))).toBe(vector.hash);
		});
	}

	it("keeps the last character of a name whose lowercasing grows it", () => {
		expect(normalizeName("İzmir")).toBe("izmir");
	});
});

describe("dob, zip and vin normalization match ca_drop_combined_search_result", () => {
	for (const [raw, expected] of CLICKHOUSE_DOB_VECTORS) {
		it(`dob ${JSON.stringify(raw)} -> ${JSON.stringify(expected)}`, () => {
			expect(normalizeDob(raw)).toBe(expected);
		});
	}
	for (const [raw, expected] of CLICKHOUSE_ZIP_VECTORS) {
		it(`zip ${JSON.stringify(raw)} -> ${JSON.stringify(expected)}`, () => {
			expect(normalizeZip(raw)).toBe(expected);
		});
	}
	for (const [raw, expected] of CLICKHOUSE_VIN_VECTORS) {
		it(`vin ${JSON.stringify(raw)} -> ${JSON.stringify(expected)}`, () => {
			expect(normalizeVin(raw)).toBe(expected);
		});
	}
});

describe("combined keys match ca_drop_combined_search_result", () => {
	for (const vector of CLICKHOUSE_COMBINED_VECTORS) {
		it(`${vector.first} ${vector.last}`, async () => {
			const keys = await buildReportKeys({
				emails: [],
				phones: [],
				firstNames: [normalizeName(vector.first)],
				lastNames: [normalizeName(vector.last)],
				dobs: [normalizeDob(vector.dob)],
				zips: [normalizeZip(vector.zip)],
				vins: [normalizeVin(vector.vin)],
			});
			expect(keys.ndz).toEqual([vector.ndz]);
			expect(keys.namevin).toEqual([vector.namevin]);
		});
	}
});

describe("extractReportFields", () => {
	it("reads the aggregated-data shape", () => {
		const fields = extractReportFields({
			personalInfo: {
				firstName: "Anna",
				firstNames: ["Anna", "Ann"],
				lastName: "Smith",
				birthDate: "1980-01-01",
			},
			contactInfo: {
				email: "Anna.Smith@Domain.com",
				phones: ["+1 (415) 555-9317"],
				zip: "90210",
				fullAddresses: [{ zip: "90210-1234" }, { zip: "02134" }],
			},
			vehicles: [{ vin: "1HGCM82633A004352" }],
		});

		expect(fields.firstNames).toEqual(["anna", "ann"]);
		expect(fields.lastNames).toEqual(["smith"]);
		expect(fields.dobs).toEqual(["19800101"]);
		expect(fields.zips).toEqual(["90210", "2134"]);
		expect(fields.vins).toEqual(["1hgcm82633a004352"]);
		expect(fields.emails).toEqual(["anna.smith@domain.com"]);
		expect(fields.phones).toEqual(["4155559317"]);
	});

	it("reads every person of the people-record shape, not just the first", () => {
		const fields = extractReportFields([
			{
				names: [{ first: "Anna", last: "Smith" }],
				dateOfBirth: { start: "1980-01-01" },
				addresses: [{ zipCode: "90210" }],
				emails: [{ address: "anna@example.com" }],
				phones: [{ number: "+14155559317" }],
				vehicles: [{ vin: "1HGCM82633A004352" }],
			},
			{
				names: [{ first: "José", last: "Müller" }],
				addresses: [{ zipCode: "02134" }],
			},
		]);

		expect(fields.firstNames).toEqual(["anna", "jose"]);
		expect(fields.lastNames).toEqual(["smith", "muller"]);
		expect(fields.zips).toEqual(["90210", "2134"]);
		expect(fields.emails).toEqual(["anna@example.com"]);
	});

	it("keeps nothing from a report with no identifiers", () => {
		const fields = extractReportFields({ socialProfiles: { twitter: "@nobody" } });
		expect(countReportKeys(fields)).toBe(0);
	});

	it("survives a payload that is not an object", () => {
		expect(countReportKeys(extractReportFields("not a report"))).toBe(0);
		expect(countReportKeys(extractReportFields(null))).toBe(0);
		expect(countReportKeys(extractReportFields([1, 2, 3]))).toBe(0);
	});
});

describe("countReportKeys", () => {
	it("counts the full cross product plus the single-value keys", () => {
		const fields = {
			emails: ["a@b.com"],
			phones: ["4155559317"],
			firstNames: ["anna", "ann"],
			lastNames: ["smith"],
			dobs: ["19800101"],
			zips: ["90210", "2134", "10001"],
			vins: ["1hgcm82633a004352"],
		};
		expect(countReportKeys(fields)).toBe(1 + 1 + 2 * 1 * 1 * 3 + 2 * 1 * 1);
	});

	it("is zero when a cross-product factor is missing", () => {
		expect(
			countReportKeys({
				emails: [],
				phones: [],
				firstNames: ["anna"],
				lastNames: ["smith"],
				dobs: ["19800101"],
				zips: [],
				vins: [],
			}),
		).toBe(0);
	});
});

describe("withSearchedValue", () => {
	const empty = {
		emails: [],
		phones: [],
		firstNames: [],
		lastNames: [],
		dobs: [],
		zips: [],
		vins: [],
	};

	it("adds the searched e-mail and phone", () => {
		expect(withSearchedValue(empty, "email", "Anna.Smith@Domain.com").emails).toEqual([
			"anna.smith@domain.com",
		]);
		expect(withSearchedValue(empty, "phone", "+1 (415) 555-9317").phones).toEqual([
			"4155559317",
		]);
	});

	it("does not turn a searched name into a single-value key", () => {
		expect(withSearchedValue(empty, "people", "Anna Smith")).toEqual(empty);
	});
});

describe("POST /api/drop/report-check", () => {
	async function check(body: unknown): Promise<{ status: number; json: Record<string, unknown> }> {
		const response = await SELF.fetch("https://example.com/api/drop/report-check", {
			method: "POST",
			body: JSON.stringify(body),
		});
		return { status: response.status, json: (await response.json()) as Record<string, unknown> };
	}

	it("rejects an unknown type", async () => {
		const { status } = await check({ type: "vin", value: "x", report: {} });
		expect(status).toBe(400);
	});

	it("requires a value for email and phone", async () => {
		expect((await check({ type: "email", report: {} })).status).toBe(400);
		expect((await check({ type: "phone", value: "  ", report: {} })).status).toBe(400);
	});

	it("requires a report", async () => {
		expect((await check({ type: "people" })).status).toBe(400);
	});

	it("says so when no key can be derived", async () => {
		const { status, json } = await check({ type: "people", report: { socialProfiles: {} } });
		expect(status).toBe(200);
		expect(json.listed).toBe(false);
		expect(json.keysChecked).toBe(0);
		expect(json.reason).toBe("no DROP key could be derived from the report");
	});

	it("refuses to answer when the report yields too many combinations", async () => {
		const many = (prefix: string, count: number) =>
			Array.from({ length: count }, (_, i) => `${prefix}${i}`);
		const { status, json } = await check({
			type: "people",
			report: [
				{
					names: many("first", 30).map((first, i) => ({
						first,
						last: many("last", 30)[i],
					})),
					dateOfBirth: { start: "1980-01-01" },
					addresses: many("", 40).map((_, i) => ({ zipCode: `1${String(i).padStart(4, "0")}` })),
				},
			],
		});
		expect(status).toBe(503);
		expect(json.error).toBe("check did not run");
		expect(json.hint).toBe("treat as unknown, not as not-listed");
	});

	it("answers false for a report that touches no DROP key", async () => {
		const { status, json } = await check({
			type: "email",
			value: "nobody@example.com",
			report: { personalInfo: { firstName: "Nobody", lastName: "Here" } },
		});
		expect(status).toBe(200);
		expect(json).toMatchObject({ type: "email", listed: false, matched: [] });
		expect(json.keysChecked).toBe(1);
	});

	it("answers true and names the family when the searched e-mail is listed", async () => {
		const listed = await sha256Base64("listed.person@example.com");
		await env.kv.put(listed, "work-item-email");

		const { status, json } = await check({
			type: "email",
			value: "Listed.Person@Example.com",
			report: { personalInfo: { firstName: "Listed" } },
		});
		expect(status).toBe(200);
		expect(json).toMatchObject({ type: "email", listed: true, matched: ["email"] });
	});

	it("matches a people report on a name, dob and zip combination", async () => {
		await env.kv.put(CLICKHOUSE_COMBINED_VECTORS[0].ndz, "work-item-ndz");

		const { json } = await check({
			type: "people",
			value: "Anna Smith",
			report: [
				{
					names: [{ first: "Anna", last: "Smith" }],
					dateOfBirth: { start: "1980-01-01" },
					addresses: [{ zipCode: "90210" }],
				},
			],
		});
		expect(json).toMatchObject({ type: "people", listed: true, matched: ["ndz"] });
	});

	it("matches a people report on a name and vin combination", async () => {
		await env.kv.put(CLICKHOUSE_COMBINED_VECTORS[0].namevin, "work-item-namevin");

		const { json } = await check({
			type: "people",
			report: [
				{
					names: [{ first: "Anna", last: "Smith" }],
					vehicles: [{ vin: "1HGCM82633A004352" }],
				},
			],
		});
		expect(json).toMatchObject({ type: "people", listed: true, matched: ["namevin"] });
	});
});

describe("lookupDropKeys", () => {
	it("checks more keys than one bulk read holds", async () => {
		const keys = await Promise.all(
			Array.from({ length: 250 }, (_, i) => sha256Base64(`bulk-${i}`)),
		);
		await env.kv.put(keys[249], "work-item-last");

		const result = await lookupDropKeys(env.kv, {
			email: keys,
			phone: [],
			ndz: [],
			namevin: [],
		});
		expect(result.keysChecked).toBe(250);
		expect(result.matched).toEqual(["email"]);
	});

	it("counts a key shared by two families once and reports both", async () => {
		const shared = await sha256Base64("shared-key");
		await env.kv.put(shared, "work-item-shared");

		const result = await lookupDropKeys(env.kv, {
			email: [shared],
			phone: [shared],
			ndz: [],
			namevin: [],
		});
		expect(result.keysChecked).toBe(1);
		expect(result.matched).toEqual(["email", "phone"]);
	});
});
