import { SELF, env } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { waitForEntityRows } from "../worker/drop-erase";
import { normalizeEmail, normalizePhone, sha256Base64 } from "../worker/drop-normalize";
import {
	NO_FIELDS,
	buildReportKeys,
	countReportKeys,
	extractReportRecords,
	lookupDropKeys,
	normalizeDob,
	normalizeName,
	normalizeVin,
	normalizeZip,
	subjectFields,
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

const PUBLISHED_SPEC_VECTORS: {
	normalize: (raw: string) => string;
	raw: string;
	normalized: string;
	hash: string;
}[] = [
	{
		normalize: normalizeName,
		raw: "Juan Pablo",
		normalized: "juanpablo",
		hash: "91hIbrbzNeqHs3o81O5yNrXUj7wDd2shvZ6THKi9qz8=",
	},
	{
		normalize: normalizeName,
		raw: "Martinez",
		normalized: "martinez",
		hash: "2wRPGbwBNxhShjRczx8GfS2c4cjvs4NJskeWloUNtp8=",
	},
	{
		normalize: normalizeEmail,
		raw: "Anna.Smith@Domain.com",
		normalized: "anna.smith@domain.com",
		hash: "KA18MT/ph6IHYjzT9zwETySDQyvSh87YuoSBpOQtkhE=",
	},
	{
		normalize: normalizeEmail,
		raw: "danielle.johnson12@example.com",
		normalized: "danielle.johnson12@example.com",
		hash: "mKDnDvwF2inxrKcK1hJN2TRkxPfL6kzNNTtU12eH8Bw=",
	},
	{
		normalize: normalizePhone,
		raw: "+1(415)555-9317",
		normalized: "4155559317",
		hash: "vGM7y5n+hBXRSEAklhHDPCbysyNgYTmXdMcagGUOY8E=",
	},
	{
		normalize: normalizePhone,
		raw: "+84(90)123 4567",
		normalized: "4901234567",
		hash: "ptzVkgbv9DonwvPCHmXmJ2SEOaolSh37z3ZzY/Gmm+U=",
	},
	{
		normalize: normalizePhone,
		raw: "+354(123)4567",
		normalized: "3541234567",
		hash: "Btrzydf5K6ALAKKXJGFHSx7u5bDzHC9WlVYtpq1n2rY=",
	},
	{
		normalize: normalizePhone,
		raw: "5551273811",
		normalized: "5551273811",
		hash: "jr/RAWYVN+ODBf2vRxwBASPwiO4x27OGI1y3IDhcwLo=",
	},
	{
		normalize: normalizeDob,
		raw: "1985-07-04",
		normalized: "19850704",
		hash: "IWi7qxOAbBJe0fNciDj76Eg84gmj40rB7aNMK/VnFOI=",
	},
	{
		normalize: normalizeZip,
		raw: "91790-3771",
		normalized: "91790",
		hash: "2FPZucR4x7U8KlM+SFAX4LPGhwNz/PIZUCSUdDh0o/s=",
	},
	{
		normalize: normalizeZip,
		raw: "M1B 1A1",
		normalized: "m1b1a",
		hash: "n8L9q8mVeT6Xt9/EeUNiTukGDrkbPJ3DvOEx14uElxk=",
	},
	{
		normalize: normalizeZip,
		raw: "00712345",
		normalized: "71234",
		hash: "aeNUYKh7Xw5sqpxSbSP9eOHsj6iXewbUyavv89DIuhQ=",
	},
	{
		normalize: normalizeZip,
		raw: "00300-9999",
		normalized: "300",
		hash: "mDvWFLta/s5as7YCP3EUfNe2vCMU+dJ690IlQcZVg4k=",
	},
	{
		normalize: normalizeVin,
		raw: "1HGCM82633A004352",
		normalized: "1hgcm82633a004352",
		hash: "iNswy1m+0VSt8jAfFrvaiQ1R/0HAbgSwNGkwqo6QBss=",
	},
];

describe("published DROP specification examples", () => {
	for (const vector of PUBLISHED_SPEC_VECTORS) {
		it(`${JSON.stringify(vector.raw)} -> ${JSON.stringify(vector.normalized)}`, async () => {
			expect(vector.normalize(vector.raw)).toBe(vector.normalized);
			expect(await sha256Base64(vector.normalized)).toBe(vector.hash);
		});
	}

	it("builds the published NDZ key for Danielle Johnson", async () => {
		const keys = await buildReportKeys({
			emails: [],
			phones: [],
			firstNames: [normalizeName("Danielle")],
			lastNames: [normalizeName("Johnson")],
			dobs: [normalizeDob("1985-07-04")],
			zips: [normalizeZip("91790")],
			vins: [],
		});
		expect(keys.ndz).toEqual(["PQOfn1RffEKmqMmNAzDKKaoZCwxWbQZkQzPWmQo9REA="]);
	});

	it("builds the published NameVIN key for Eve Genesis", async () => {
		const keys = await buildReportKeys({
			emails: [],
			phones: [],
			firstNames: [normalizeName("Eve")],
			lastNames: [normalizeName("Genesis")],
			dobs: [],
			zips: [],
			vins: [normalizeVin("1HGCM82633A004352")],
		});
		expect(keys.namevin).toEqual(["rtnDuXIe63jXYQQXW5r07GJ7lSsrib8+46QuKFwkOmk="]);
	});

	it("drops a date of birth that is not year-first, rather than mis-keying it", () => {
		expect(normalizeDob("07/04/1985")).toBe("");
		expect(normalizeDob("45")).toBe("");
		expect(normalizeDob("1776-07-04")).toBe("");
	});
});

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

describe("extractReportRecords", () => {
	it("reads the aggregated-data shape as one record", () => {
		const extracted = extractReportRecords({
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

		expect(extracted).toHaveLength(1);
		expect(extracted[0].index).toBe(0);
		expect(extracted[0].fields).toEqual({
			firstNames: ["anna", "ann"],
			lastNames: ["smith"],
			dobs: ["19800101"],
			zips: ["90210", "2134"],
			vins: ["1hgcm82633a004352"],
			emails: ["anna.smith@domain.com"],
			phones: ["4155559317"],
		});
	});

	it("keeps each person of a people report in its own record", () => {
		const extracted = extractReportRecords([
			{
				names: [{ first: "Anna", last: "Smith" }],
				dateOfBirth: { start: "1980-01-01" },
				addresses: [{ zipCode: "90210" }],
				emails: [{ address: "anna@example.com" }],
				phones: [{ number: "+14155559317" }],
				vehicles: [{ vin: "1HGCM82633A004352" }],
				pipl_id: "pipl-1",
			},
			{
				names: [{ first: "José", last: "Müller" }],
				dateOfBirth: { start: "1999-12-31" },
				addresses: [{ zipCode: "02134" }],
			},
		]);

		expect(extracted).toHaveLength(2);
		expect(extracted[0]).toMatchObject({ index: 0, id: "pipl-1" });
		expect(extracted[0].fields.firstNames).toEqual(["anna"]);
		expect(extracted[0].fields.zips).toEqual(["90210"]);
		expect(extracted[1]).toMatchObject({ index: 1 });
		expect(extracted[1].id).toBeUndefined();
		expect(extracted[1].fields.firstNames).toEqual(["jose"]);
		expect(extracted[1].fields.zips).toEqual(["2134"]);
	});

	it("never combines one person's name with another person's dob and zip", async () => {
		const extracted = extractReportRecords([
			{ names: [{ first: "Anna", last: "Smith" }], dateOfBirth: { start: "1980-01-01" } },
			{ names: [{ first: "José", last: "Müller" }], addresses: [{ zipCode: "90210" }] },
		]);

		for (const record of extracted) {
			expect(countReportKeys(record.fields)).toBe(0);
			expect((await buildReportKeys(record.fields)).ndz).toEqual([]);
		}
	});

	it("keeps nothing from a report with no identifiers", () => {
		const extracted = extractReportRecords({ socialProfiles: { twitter: "@nobody" } });
		expect(countReportKeys(extracted[0].fields)).toBe(0);
	});

	it("survives a payload that is not an object", () => {
		expect(extractReportRecords("not a report")).toEqual([]);
		expect(extractReportRecords(null)).toEqual([]);
		expect(extractReportRecords([1, 2, 3])).toEqual([]);
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

describe("subjectFields", () => {
	it("keys the searched e-mail and phone", () => {
		expect(subjectFields("email", "Anna.Smith@Domain.com").emails).toEqual([
			"anna.smith@domain.com",
		]);
		expect(subjectFields("phone", "+1 (415) 555-9317").phones).toEqual(["4155559317"]);
	});

	it("does not turn a searched name into a single-value key", () => {
		expect(subjectFields("people", "Anna Smith")).toEqual(NO_FIELDS);
		expect(subjectFields("email", undefined)).toEqual(NO_FIELDS);
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
		const zips = Array.from({ length: 60 }, (_, i) => ({
			zipCode: `1${String(i).padStart(4, "0")}`,
		}));
		const person = {
			names: Array.from({ length: 12 }, (_, i) => ({ first: `first${i}`, last: `last${i}` })),
			dateOfBirth: { start: "1980-01-01" },
			addresses: zips,
		};
		const { status, json } = await check({
			type: "people",
			report: Array.from({ length: 4 }, () => person),
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
		expect(json).toMatchObject({
			type: "email",
			listed: false,
			subjectListed: false,
			matched: [],
			records: [{ index: 0, listed: false, matched: [] }],
		});
		expect(json.keysChecked).toBe(1);
	});

	it("never reports success when the erase could not be completed", async () => {
		await env.kv.put(await sha256Base64("listed.person@example.com"), "work-item-email");

		// Supabase and ClickHouse are unreachable from the test runner, so the
		// erase cannot get past its first stage. The answer must still say the
		// match is real, and must not come back 200.
		const { status, json } = await check({
			type: "email",
			value: "Listed.Person@Example.com",
			report: { personalInfo: { firstName: "Listed" } },
			waitMs: 0,
		});

		expect(status).toBe(500);
		expect(json).toMatchObject({
			type: "email",
			listed: true,
			subjectListed: true,
			matched: ["email"],
			error: "match found but not fully honoured",
			stage: "record",
			rowsErased: 0,
		});
		expect(json.hint).toContain("do not serve this report");
		expect(json.runId).toEqual(expect.any(String));
	});

	it("does not attempt an erase when nothing matched", async () => {
		const { status, json } = await check({
			type: "phone",
			value: "+1 (415) 555-0000",
			report: { personalInfo: { firstName: "Nobody" } },
		});
		expect(status).toBe(200);
		expect(json).toMatchObject({ type: "phone", listed: false, subjectListed: false });
		expect(json.erased).toBeUndefined();
		expect(json.error).toBeUndefined();
	});

	it("detects a people match without erasing anything", async () => {
		await env.kv.put(CLICKHOUSE_COMBINED_VECTORS[0].namevin, "work-item-namevin");

		const { status, json } = await check({
			type: "people",
			report: [
				{
					names: [{ first: "Anna", last: "Smith" }],
					vehicles: [{ vin: "1HGCM82633A004352" }],
				},
			],
		});

		expect(status).toBe(200);
		expect(json).toMatchObject({
			type: "people",
			listed: true,
			matched: ["namevin"],
			erased: null,
			records: [{ index: 0, listed: true, matched: ["namevin"] }],
		});
		expect(json.reason).toContain("not erased here");
	});

	it("names only the matching record of a people report", async () => {
		await env.kv.put(CLICKHOUSE_COMBINED_VECTORS[0].ndz, "work-item-ndz");

		const { json } = await check({
			type: "people",
			value: "Anna Smith",
			report: [
				{
					names: [{ first: "Unrelated", last: "Person" }],
					dateOfBirth: { start: "1970-02-02" },
					addresses: [{ zipCode: "10001" }],
					pipl_id: "pipl-clean",
				},
				{
					names: [{ first: "Anna", last: "Smith" }],
					dateOfBirth: { start: "1980-01-01" },
					addresses: [{ zipCode: "90210" }],
					pipl_id: "pipl-listed",
				},
			],
		});

		expect(json).toMatchObject({
			type: "people",
			listed: true,
			subjectListed: false,
			matched: ["ndz"],
			records: [
				{ index: 0, id: "pipl-clean", listed: false, matched: [] },
				{ index: 1, id: "pipl-listed", listed: true, matched: ["ndz"] },
			],
		});
	});

	it("does not flag a record built from another record's dob and zip", async () => {
		await env.kv.put(CLICKHOUSE_COMBINED_VECTORS[0].ndz, "work-item-ndz");

		const { json } = await check({
			type: "people",
			report: [
				{ names: [{ first: "Anna", last: "Smith" }], addresses: [{ zipCode: "10001" }] },
				{
					names: [{ first: "Unrelated", last: "Person" }],
					dateOfBirth: { start: "1980-01-01" },
					addresses: [{ zipCode: "90210" }],
				},
			],
		});

		expect(json).toMatchObject({
			type: "people",
			listed: false,
			records: [
				{ index: 0, listed: false },
				{ index: 1, listed: false },
			],
		});
	});

	it("flags the record whose own e-mail is listed", async () => {
		await env.kv.put(await sha256Base64("second@example.com"), "work-item-record-email");

		const { json } = await check({
			type: "people",
			report: [
				{ names: [{ first: "Anna", last: "Smith" }], emails: [{ address: "first@example.com" }] },
				{ names: [{ first: "José", last: "Müller" }], emails: [{ address: "Second@Example.com" }] },
			],
		});

		expect(json).toMatchObject({
			type: "people",
			listed: true,
			subjectListed: false,
			matched: ["email"],
			records: [
				{ index: 0, listed: false, matched: [] },
				{ index: 1, listed: true, matched: ["email"] },
			],
		});
	});

});

describe("waitForEntityRows", () => {
	it("returns as soon as the report lands", async () => {
		let calls = 0;
		const appeared = await waitForEntityRows(
			async () => (++calls >= 3 ? 7 : 0),
			5_000,
			10,
		);
		expect(appeared).toMatchObject({ rows: 7, polls: 3 });
	});

	it("does not poll again once the row is there", async () => {
		let calls = 0;
		const appeared = await waitForEntityRows(
			async () => {
				calls += 1;
				return 4;
			},
			5_000,
			10,
		);
		expect(calls).toBe(1);
		expect(appeared.rows).toBe(4);
	});

	it("gives up with zero rows once the deadline passes", async () => {
		const appeared = await waitForEntityRows(async () => 0, 60, 20);
		expect(appeared.rows).toBe(0);
		expect(appeared.polls).toBeGreaterThan(1);
		expect(appeared.waitedMs).toBeLessThan(1_000);
	});

	it("polls at least once even with no time to wait", async () => {
		let calls = 0;
		const appeared = await waitForEntityRows(
			async () => {
				calls += 1;
				return 0;
			},
			0,
			10,
		);
		expect(calls).toBe(1);
		expect(appeared.rows).toBe(0);
	});

	it("tolerates a blip and still sees the row", async () => {
		let calls = 0;
		const appeared = await waitForEntityRows(
			async () => {
				calls += 1;
				if (calls === 1) throw new Error("ClickHouse 502");
				return 2;
			},
			5_000,
			10,
		);
		expect(appeared.rows).toBe(2);
		expect(appeared.polls).toBe(2);
	});

	it("throws when it never got a clean answer, rather than reporting no rows", async () => {
		// An unreachable ClickHouse must not look like an absent row: one would
		// leave the erase unconfirmed, the other would be read as nothing to do.
		await expect(
			waitForEntityRows(async () => {
				throw new Error("ClickHouse unreachable");
			}, 60, 20),
		).rejects.toThrow("ClickHouse unreachable");
	});
});

describe("lookupDropKeys", () => {
	it("checks more keys than one bulk read holds", async () => {
		const keys = await Promise.all(
			Array.from({ length: 250 }, (_, i) => sha256Base64(`bulk-${i}`)),
		);
		await env.kv.put(keys[249], "work-item-last");

		const result = await lookupDropKeys(env.kv, [
			{ email: keys, phone: [], ndz: [], namevin: [] },
		]);
		expect(result.keysChecked).toBe(250);
		expect(result.matched).toEqual([["email"]]);
	});

	it("counts a key shared by two families once and reports both", async () => {
		const shared = await sha256Base64("shared-key");
		await env.kv.put(shared, "work-item-shared");

		const result = await lookupDropKeys(env.kv, [
			{ email: [shared], phone: [shared], ndz: [], namevin: [] },
		]);
		expect(result.keysChecked).toBe(1);
		expect(result.matched).toEqual([["email", "phone"]]);
	});

	it("reads a key shared by two groups once and reports it against both", async () => {
		const shared = await sha256Base64("shared-between-records");
		await env.kv.put(shared, "work-item-shared-group");
		const missing = await sha256Base64("absent-from-kv");

		const result = await lookupDropKeys(env.kv, [
			{ email: [], phone: [], ndz: [shared], namevin: [] },
			{ email: [], phone: [], ndz: [missing], namevin: [] },
			{ email: [], phone: [], ndz: [shared], namevin: [] },
		]);
		expect(result.keysChecked).toBe(2);
		expect(result.matched).toEqual([["ndz"], [], ["ndz"]]);
	});
});
