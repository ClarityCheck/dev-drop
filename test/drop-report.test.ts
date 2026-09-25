import { SELF, env } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { listedRecordsIn } from "../worker/drop-incident";
import { normalizeEmail, normalizePhone, sha256Base64 } from "../worker/drop-normalize";
import {
	MAX_REPORT_KEYS,
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
	["1899-01-01", "18990101"],
	["01/01/1980", "19800101"],
	["", ""],
	// v5: formats found in DEV provider data, outputs taken from the view
	["11/03/1985", "19851103"],
	["3/7/1985", "19850307"],
	["25/03/1985", "19850325"],
	["08/18/1987 (MM/DD/YYYY)", "19870818"],
	["#<Date: 1978-03-19 ((2443587j,0s,0n),+0s,2299161j)>", "19780319"],
	["09141990", "19900914"],
	["13131990", ""],
	["July 4, 1776", "17760704"],
	["Jul 4 1776", "17760704"],
	["Sept. 9, 1990", "19900909"],
	["junk 4, 1990", ""],
	["499132800000", "19851026"],
	["-315619200000", "19600101"],
	["99999999999999", ""],
	["1985-13-01", ""],
	["2100-01-01", ""],
	["1000-01-01", ""],
	["0000-00-00 00:00:00", ""],
	["1977", ""],
	["11/21/78", ""],
];

// fullName -> first and last candidates, normalized; outputs taken from the view
const CLICKHOUSE_FULL_NAME_VECTORS: { fullName: string; firsts: string[]; lasts: string[] }[] = [
	{ fullName: "Anna Maria Smith", firsts: ["anna", "annamaria"], lasts: ["smith"] },
	{ fullName: "Smith, José Luis", firsts: ["jose", "joseluis"], lasts: ["smith"] },
	{ fullName: "John O'Neil Jr.", firsts: ["john"], lasts: ["oneil"] },
	{ fullName: "Mary-Jane Watson", firsts: ["maryjane"], lasts: ["watson"] },
	{ fullName: "Ωmega Test", firsts: ["omega"], lasts: ["test"] },
	{ fullName: " Björn Ålund ", firsts: ["bjorn"], lasts: ["alund"] },
	{ fullName: "Cher", firsts: [], lasts: [] },
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
		normalize: normalizeDob,
		raw: "July 4, 1776",
		normalized: "17760704",
		hash: "skXYXxBER6HQTZ3rXSZH1wVGLQ054mS5rbR/bwvzy4I=",
	},
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

	it("reads a US date month first, and drops what it cannot read", () => {
		expect(normalizeDob("07/04/1985")).toBe("19850704");
		expect(normalizeDob("1776-07-04")).toBe("17760704");
		expect(normalizeDob("45")).toBe("");
		expect(normalizeDob("*0suDIs1yoF8kUCG4nOkq29w==")).toBe("");
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

describe("fullName splitting matches ca_drop_combined_search_result", () => {
	for (const vector of CLICKHOUSE_FULL_NAME_VECTORS) {
		it(`${JSON.stringify(vector.fullName)}`, () => {
			const [record] = extractReportRecords([{ personalInfo: { fullName: vector.fullName } }]);
			expect([...record.fields.firstNames].sort()).toEqual(vector.firsts);
			expect([...record.fields.lastNames].sort()).toEqual(vector.lasts);
		});
	}

	it("adds to firstName and lastName rather than replacing them", () => {
		const [record] = extractReportRecords([
			{ personalInfo: { firstName: "Ann", lastName: "Smyth", fullName: "Anna Smith" } },
		]);
		expect([...record.fields.firstNames].sort()).toEqual(["ann", "anna"]);
		expect([...record.fields.lastNames].sort()).toEqual(["smith", "smyth"]);
	});

	it("gives a provider that sends only fullName an NDZ key", async () => {
		const [record] = extractReportRecords([
			{ personalInfo: { fullName: "Anna Smith", birthDate: "11/03/1985" }, contactInfo: { zip: "94107" } },
		]);
		const keys = await buildReportKeys(record.fields);
		expect(keys.ndz).toHaveLength(1);
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

	it("fails when no key can be derived", async () => {
		const { status, json } = await check({ type: "people", report: { socialProfiles: {} } });
		expect(status).toBe(503);
		expect(json).toMatchObject({
			error: "check did not run",
			detail: "no DROP key could be derived from the report",
		});
	});

	it("fails a pathological report rather than reducing it", async () => {
		// Measured on DEV, one aggregated email row reaches 33,202,400 candidate
		// keys: 82 first names, 71 last names, 230 birthDates, 140 ZIPs, all
		// multiplied together. The answer is `listed` alone, so a reduced check
		// has no way to say it was reduced — it fails instead, and the lookup API
		// marks the search failed.
		const many = (n: number, prefix: string) =>
			Array.from({ length: n }, (_, i) => `${prefix}${i}`);

		const { status, json } = await check({
			type: "email",
			value: "noisy.aggregate@example.com",
			report: {
				personalInfo: {
					firstNames: many(82, "first"),
					lastNames: many(71, "last"),
					birthDates: many(230, "1980010").map((_, i) => `19800${String(i % 900).padStart(3, "0")}`),
				},
				contactInfo: {
					fullAddresses: many(140, "").map((_, i) => ({
						zip: `1${String(i).padStart(4, "0")}`,
					})),
				},
			},
		});

		expect(status).toBe(503);
		expect(json).toMatchObject({ error: "check did not run" });
		expect(json.detail).toContain(`over the ${MAX_REPORT_KEYS} limit`);
	});

	it("fails when many records are individually fine but together over the limit", async () => {
		const person = (i: number) => ({
			names: Array.from({ length: 10 }, (_, n) => ({ first: `f${n}`, last: `l${n}` })),
			dateOfBirth: { start: "1980-01-01" },
			addresses: Array.from({ length: 24 }, (_, z) => ({
				zipCode: `1${String(z).padStart(4, "0")}`,
			})),
			emails: [{ address: `p${i}@example.com` }],
		});

		const { status, json } = await check({
			type: "people",
			report: Array.from({ length: 60 }, (_, i) => person(i)),
		});

		expect(status).toBe(503);
		expect(json).toMatchObject({ error: "check did not run" });
	});

	it("answers false for a report that touches no DROP key", async () => {
		const { status, json } = await check({
			type: "email",
			value: "nobody@example.com",
			report: { personalInfo: { firstName: "Nobody", lastName: "Here" } },
		});

		// Two fields, and nothing else: no per-record verdicts, no key families,
		// no count of what was checked.
		expect(status).toBe(200);
		expect(json).toEqual({ type: "email", listed: false });
	});

	it("answers listed when the searched e-mail is on the list", async () => {
		await env.kv.put(await sha256Base64("listed.person@example.com"), "work-item-email");

		const { status, json } = await check({
			type: "email",
			value: "Listed.Person@Example.com",
			report: { personalInfo: { firstName: "Listed" } },
		});

		expect(status).toBe(200);
		expect(json).toEqual({ type: "email", listed: true });
	});

	it("combines name, date of birth and ZIP across providers into one NDZ key", async () => {
		// The point of the merge. No single provider carries all four factors,
		// so a per-provider cross product derives no NDZ key at all and the
		// consumer is never matched.
		const ndz = await sha256Base64(
			(await sha256Base64("ada")) +
				(await sha256Base64("lovelace")) +
				(await sha256Base64("19851103")) +
				(await sha256Base64("94107")),
		);
		await env.kv.put(ndz, "work-item-ndz");

		const { status, json } = await check({
			type: "phone",
			value: "+1 415 555 9317",
			report: [
				{ personalInfo: { firstName: "Ada", lastName: "Lovelace" } },
				{ personalInfo: { birthDate: "1985-11-03" }, contactInfo: { zip: "94107" } },
			],
		});

		expect(status).toBe(200);
		expect(json).toEqual({ type: "phone", listed: true });
	});

	it("keeps a people report's elements independent of one another", async () => {
		// The mirror of the test above: the listed NDZ key is Ada's name and dob
		// with the 94107 ZIP, and those factors sit in two different ARRAY
		// ELEMENTS — two different people. Each element derives its own keys, so
		// the check runs in full; combining them across elements would invent a
		// key for someone who does not exist, and nothing matches.
		const ndz = await sha256Base64(
			(await sha256Base64("ada")) +
				(await sha256Base64("lovelace")) +
				(await sha256Base64("19851103")) +
				(await sha256Base64("94107")),
		);
		await env.kv.put(ndz, "work-item-ndz-people");

		const { status, json } = await check({
			type: "people",
			report: [
				{
					personalInfo: { firstName: "Ada", lastName: "Lovelace", birthDate: "1985-11-03" },
					contactInfo: { zip: "10001" },
				},
				{
					personalInfo: { firstName: "Unrelated", lastName: "Person", birthDate: "1970-01-01" },
					contactInfo: { zip: "94107" },
				},
			],
		});

		expect(status).toBe(200);
		expect(json).toEqual({
			type: "people",
			listed: false,
			records: [
				{ index: 0, listed: false },
				{ index: 1, listed: false },
			],
		});
	});

	it("answers a clean subject with a listed report, and the recording cannot fail it", async () => {
		await env.kv.put(CLICKHOUSE_COMBINED_VECTORS[0].ndz, "work-item-suppress");

		// The subject is not on the DROP list; its report carries someone who
		// is. That finding is recorded in ca_drop_suppressed_value — in
		// waitUntil, after the response, and Supabase is unreachable from the
		// test runner, so a 200 here is the assertion that the write is off the
		// answer's path entirely.
		const { status, json } = await check({
			type: "email",
			value: "clean.subject@example.com",
			report: {
				personalInfo: { firstName: "Anna", lastName: "Smith", birthDate: "1980-01-01" },
				contactInfo: { fullAddresses: [{ zip: "90210" }] },
			},
		});

		expect(status).toBe(200);
		expect(json).toEqual({ type: "email", listed: true });
	});

	it("answers without writing to or erasing anything", async () => {
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

		// Supabase, ClickHouse and R2 are all unreachable from the test runner,
		// so a 200 here is itself the assertion: the endpoint touched none of
		// them. The caller filters before it persists.
		expect(status).toBe(200);
		expect(json).toEqual({
			type: "people",
			listed: true,
			records: [{ index: 0, listed: true }],
		});
	});

	it("names only the matching element of a people report", async () => {
		await env.kv.put(CLICKHOUSE_COMBINED_VECTORS[0].ndz, "work-item-ndz");

		// The elements are different people, so the verdict is per element and
		// the caller removes only the one that matched. The other thirty-nine
		// John Smiths are strangers and their records stay.
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

		expect(json).toEqual({
			type: "people",
			listed: true,
			records: [
				{ index: 0, listed: false },
				{ index: 1, listed: true },
			],
		});
	});

	it("does not invent a key from another record's dob and zip", async () => {
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

		expect(json).toEqual({
			type: "people",
			listed: false,
			records: [
				{ index: 0, listed: false },
				{ index: 1, listed: false },
			],
		});
	});

	it("names the element whose own e-mail is listed", async () => {
		await env.kv.put(await sha256Base64("second@example.com"), "work-item-record-email");

		const { json } = await check({
			type: "people",
			report: [
				{ names: [{ first: "Anna", last: "Smith" }], emails: [{ address: "first@example.com" }] },
				{ names: [{ first: "José", last: "Müller" }], emails: [{ address: "Second@Example.com" }] },
			],
		});

		expect(json).toEqual({
			type: "people",
			listed: true,
			records: [
				{ index: 0, listed: false },
				{ index: 1, listed: true },
			],
		});
	});
});

describe("POST /api/drop/erase-incident", () => {
	async function incident(
		body: unknown,
	): Promise<{ status: number; json: Record<string, unknown> }> {
		const response = await SELF.fetch("https://example.com/api/drop/erase-incident", {
			method: "POST",
			body: JSON.stringify(body),
		});
		return { status: response.status, json: (await response.json()) as Record<string, unknown> };
	}

	const listedReport = {
		type: "email",
		value: "Incident.Person@Example.com",
		normalizedValue: "incident.person@example.com",
		report: { personalInfo: { firstName: "Incident" } },
	};

	async function matchFound(
		body: unknown,
	): Promise<{ status: number; json: Record<string, unknown> }> {
		const response = await SELF.fetch("https://example.com/api/drop/match-found", {
			method: "POST",
			body: JSON.stringify(body),
		});
		return { status: response.status, json: (await response.json()) as Record<string, unknown> };
	}

	describe("match-found, which has to come before the erase", () => {
		it("does not ask whether the data is gone", async () => {
			await env.kv.put(
				await sha256Base64("incident.person@example.com"),
				"work-item-incident",
			);

			// The whole point of this call is that it runs BEFORE the erase, so
			// requiring the rows to be gone would make it impossible to use. It
			// gets as far as Supabase, which the test runner cannot reach — the
			// stage proves it never went looking for a row count.
			const { status, json } = await matchFound(listedReport);

			expect(status).toBe(500);
			expect(json.stage).toBe("record");
			expect(json.stage).not.toBe("verify");
			expect(json.stage).not.toBe("unverifiable");
		});

		it("tells the caller not to erase when the match could not be recorded", async () => {
			await env.kv.put(
				await sha256Base64("incident.person@example.com"),
				"work-item-incident",
			);

			// This is the sentence that prevents the unrecoverable case. If the
			// caller erases anyway, the rows are gone, the view has nothing to
			// match, and no record that the consumer was ever in the data exists.
			const { json } = await matchFound(listedReport);

			expect(json.error).toBe("the match was not recorded");
			expect(json.hint).toContain("DO NOT ERASE");
		});

		it("confirms a match report-check found beyond the field caps", async () => {
			// Eleven first names across two providers, and the listed person's is
			// "zoe" — eleventh in sort order, so a path that caps first names at
			// ten never builds this NDZ key. report-check builds every
			// combination and says listed; match-found has to confirm the same
			// match, or the API withholds a report it can never erase.
			const ndz = await sha256Base64(
				(await sha256Base64("zoe")) +
					(await sha256Base64("smith")) +
					(await sha256Base64("19851103")) +
					(await sha256Base64("94107")),
			);
			await env.kv.put(ndz, "work-item-beyond-cap", {
				metadata: { work_item_id: "work-item-beyond-cap", list_type: "ndz" },
			});

			const report = [
				{
					personalInfo: {
						firstNames: ["Anna", "Bella", "Carla", "Dora", "Emma", "Fiona", "Gina", "Hana", "Iris", "Jana"],
						lastName: "Smith",
					},
				},
				{
					personalInfo: { firstName: "Zoe", lastName: "Smith", birthDate: "1985-11-03" },
					contactInfo: { zip: "94107" },
				},
			];
			const body = { type: "phone", value: "+1 415 555 0101", normalizedValue: "14155550101", report };

			const check = await SELF.fetch("https://example.com/api/drop/report-check", {
				method: "POST",
				body: JSON.stringify(body),
			});
			expect(((await check.json()) as { listed: boolean }).listed).toBe(true);

			const { status, json } = await matchFound(body);
			expect(status).not.toBe(422);
			expect(json.stage).toBe("record");
		});

		it("refuses a match it cannot confirm, before writing anything", async () => {
			const { status, json } = await matchFound({
				type: "email",
				value: "not.listed@example.com",
				normalizedValue: "not.listed@example.com",
				report: { personalInfo: { firstName: "Nobody" } },
			});

			expect(status).toBe(422);
			expect(json.runId).toBeUndefined();
		});

		it("carries the caller's runId so both halves correlate", async () => {
			await env.kv.put(
				await sha256Base64("incident.person@example.com"),
				"work-item-incident",
			);

			const { json } = await matchFound({ ...listedReport, runId: "run-abc-123" });

			expect(json.runId).toBe("run-abc-123");
		});
	});

	it("rejects a body with no normalizedValue", async () => {
		const { status, json } = await incident({
			type: "email",
			value: "a@b.com",
			report: {},
		});
		expect(status).toBe(400);
		expect(json.error).toContain("normalizedValue");
	});

	it("rejects an unknown type and a missing report", async () => {
		expect((await incident({ type: "vin", normalizedValue: "x", report: {} })).status).toBe(400);
		expect(
			(await incident({ type: "email", normalizedValue: "x" })).status,
		).toBe(400);
	});

	it("refuses to record an erasure it cannot confirm was a DROP match", async () => {
		// Nothing for this report is in KV. Recording it would put a match into
		// ca_drop_work_item_match that DROP never asked for, and mark a work item
		// deleted on the strength of it.
		const { status, json } = await incident({
			type: "email",
			value: "not.listed@example.com",
			normalizedValue: "not.listed@example.com",
			report: { personalInfo: { firstName: "Nobody" } },
		});

		expect(status).toBe(422);
		expect(json.error).toContain("no DROP match could be confirmed");
		expect(json.hint).toContain("nothing was recorded");
	});

	it("refuses when it cannot confirm the rows are gone, and says so distinctly", async () => {
		await env.kv.put(
			await sha256Base64("incident.person@example.com"),
			"work-item-incident",
		);

		// ClickHouse is unreachable from the test runner, so the count cannot be
		// read at all. That is not the same as reading a non-zero count: telling
		// a caller that did delete the rows to "delete the rows first" sends it
		// chasing work it already did. 503, not 409.
		const { status, json } = await incident(listedReport);

		expect(status).toBe(503);
		expect(json).toMatchObject({
			type: "email",
			error: "the erasure was not fully recorded",
			stage: "unverifiable",
		});
		expect(json.hint).toContain("nothing was recorded");
		expect(json.rowsRemaining).toBeUndefined();
		expect(json.runId).toEqual(expect.any(String));
	});

	it("accepts a people report, where only some records were removed", async () => {
		await env.kv.put(CLICKHOUSE_COMBINED_VECTORS[0].ndz, "work-item-people-incident");

		// The row survives a people erasure — only the matched elements go — so
		// a zero row count would be the wrong thing to require. This gets as far
		// as reading the stored payload back, which the test runner cannot do,
		// and so reports unverifiable rather than "you did not delete it".
		const { status, json } = await incident({
			type: "people",
			value: "Anna Smith",
			normalizedValue: "anna smith",
			report: [
				{
					names: [{ first: "Anna", last: "Smith" }],
					dateOfBirth: { start: "1980-01-01" },
					addresses: [{ zipCode: "90210" }],
				},
			],
		});

		expect(status).toBe(503);
		expect(json).toMatchObject({ type: "people", stage: "unverifiable" });
		expect(json.detail).toContain("could not confirm the erasure");
	});

	it("confirms the match before it looks at ClickHouse at all", async () => {
		// The unconfirmed case returns 422 without a runId, which is how you can
		// tell nothing was attempted: no run was started, so nothing was logged
		// and no evidence file exists.
		const { status, json } = await incident({
			type: "phone",
			value: "+1 (415) 555-0000",
			normalizedValue: "14155550000",
			report: {},
		});
		expect(status).toBe(422);
		expect(json.runId).toBeUndefined();
		expect(json.stage).toBeUndefined();
	});
});

describe("the real-time gate", () => {
	async function gate(
		body: unknown,
	): Promise<{ status: number; json: Record<string, unknown> }> {
		const response = await SELF.fetch("https://example.com/api/drop/check", {
			method: "POST",
			body: JSON.stringify(body),
		});
		return { status: response.status, json: (await response.json()) as Record<string, unknown> };
	}

	it("answers from the DROP list", async () => {
		await env.kv.put(await sha256Base64("on.the.list@example.com"), "work-item-gate");

		const { status, json } = await gate({
			type: "email",
			value: "On.The.List@Example.com",
		});
		expect(status).toBe(200);
		expect(json).toEqual({ type: "email", listed: true });
	});

	it("answers from a suppressed value the same way", async () => {
		// Not on the DROP list: its report was suppressed because the aggregated
		// data carried someone who is. It lives in suppressed_kv, not kv.
		await env.suppressed_kv.put(await sha256Base64("clean.subject@example.com"), "email");

		const { status, json } = await gate({
			type: "email",
			value: "Clean.Subject@Example.com",
		});
		expect(status).toBe(200);
		expect(json).toEqual({ type: "email", listed: true });
	});

	it("answers listed when a value is in both namespaces", async () => {
		const hash = await sha256Base64("4155559317");
		await env.kv.put(hash, "work-item-both");
		await env.suppressed_kv.put(hash, "phone");

		const { json } = await gate({ type: "phone", value: "+1 (415) 555-9317" });
		expect(json).toEqual({ type: "phone", listed: true });
	});

	it("answers not listed when neither namespace has the value", async () => {
		const { json } = await gate({ type: "email", value: "unrelated@example.com" });
		expect(json).toEqual({ type: "email", listed: false });
	});

	it("keeps suppressed values out of the namespace Cron C matches against", async () => {
		// Cron C lists kv and copies it into ClickHouse as the DROP set. A value
		// in suppressed_kv must not appear there.
		const hash = await sha256Base64("kept.apart@example.com");
		await env.suppressed_kv.put(hash, "email");

		expect(await env.kv.get(hash)).toBeNull();
		const listed = await env.kv.list();
		expect(listed.keys.map((k) => k.name)).not.toContain(hash);
	});

	it("answers not listed for a value that normalizes to nothing", async () => {
		const { json } = await gate({ type: "phone", value: "+()- " });
		expect(json).toEqual({ type: "phone", listed: false });
	});
});

describe("listedRecordsIn", () => {
	const anna = {
		names: [{ first: "Anna", last: "Smith" }],
		dateOfBirth: { start: "1980-01-01" },
		addresses: [{ zipCode: "90210" }],
	};
	const unrelated = {
		names: [{ first: "Unrelated", last: "Person" }],
		dateOfBirth: { start: "1970-02-02" },
		addresses: [{ zipCode: "10001" }],
	};

	const payload = (persons: unknown[]) => [
		{ label: "ELI_Snusbase_Pipl", payload_json: JSON.stringify(persons) },
	];

	it("counts nothing when the matched record is gone", async () => {
		await env.kv.put(CLICKHOUSE_COMBINED_VECTORS[0].ndz, "work-item-verify");

		expect(await listedRecordsIn(env.kv, payload([unrelated]))).toBe(0);
	});

	it("catches the caller removing the wrong element", async () => {
		await env.kv.put(CLICKHOUSE_COMBINED_VECTORS[0].ndz, "work-item-verify");

		// The row count after removing one of two elements is identical whichever
		// one went, so only reading the contents can tell that the listed person
		// is the one still there.
		expect(await listedRecordsIn(env.kv, payload([anna]))).toBe(1);
	});

	it("counts each remaining listed record", async () => {
		await env.kv.put(CLICKHOUSE_COMBINED_VECTORS[0].ndz, "work-item-verify");

		expect(await listedRecordsIn(env.kv, payload([anna, unrelated, anna]))).toBe(2);
	});

	it("accepts an emptied array", async () => {
		expect(await listedRecordsIn(env.kv, payload([]))).toBe(0);
	});

	it("refuses to pass a payload it cannot parse", async () => {
		await expect(
			listedRecordsIn(env.kv, [{ label: "broken", payload_json: "{not json" }]),
		).rejects.toThrow("not valid JSON");
	});

	it("refuses to pass a payload too large to verify", async () => {
		const persons = Array.from({ length: 40 }, () => ({
			names: Array.from({ length: 12 }, (_, i) => ({ first: `f${i}`, last: `l${i}` })),
			dateOfBirth: { start: "1980-01-01" },
			addresses: Array.from({ length: 60 }, (_, i) => ({
				zipCode: `1${String(i).padStart(4, "0")}`,
			})),
		}));

		await expect(listedRecordsIn(env.kv, payload(persons))).rejects.toThrow(
			"the erasure cannot be verified",
		);
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

	it("takes list_type from DROP's own metadata, not from the family it derived", async () => {
		// The hash spaces are disjoint, so these agree in every sane case. But
		// list_type is half the key recordMatches joins ca_drop_work_item on, so
		// inferring it would turn a misfiled DROP CSV into "KV and Supabase have
		// drifted" and point the operator at the wrong thing.
		const key = await sha256Base64("derived-as-ndz");
		await env.kv.put(key, "value-is-ignored", {
			metadata: { list_type: "email", work_item_id: "wi-from-metadata" },
		});

		const result = await lookupDropKeys(env.kv, [
			{ email: [], phone: [], ndz: [key], namevin: [] },
		]);

		// The family we matched on is still ndz — that is ours, and it is what
		// `matched` reports.
		expect(result.matched).toEqual([["ndz"]]);
		// The list_type and work item are DROP's, read from the metadata.
		expect(result.hits).toEqual([
			{ list_type: "email", work_item_id: "wi-from-metadata", hash: key },
		]);
	});

	it("falls back to the derived family when a key has no metadata", async () => {
		const key = await sha256Base64("no-metadata-at-all");
		await env.kv.put(key, "wi-from-value");

		const result = await lookupDropKeys(env.kv, [
			{ email: [], phone: [], ndz: [key], namevin: [] },
		]);

		expect(result.hits).toEqual([
			{ list_type: "ndz", work_item_id: "wi-from-value", hash: key },
		]);
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
