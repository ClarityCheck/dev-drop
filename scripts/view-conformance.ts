import { readFileSync } from "node:fs";
import { MAX_REPORT_KEYS, buildReportKeys, reportKeyGroups } from "../worker/drop-report";
import type { ReportType } from "../worker/drop-report";

type Row = { type: ReportType; value: string; provider: string; service: string; payload: unknown };

const many = (n: number, f: (i: number) => string) => Array.from({ length: n }, (_, i) => f(i));

const FIXTURES: Row[] = [
	{
		type: "email",
		value: "aligned@example.com",
		provider: "Pipl",
		service: "Unknown",
		payload: [
			{
				personalInfo: {
					firstNames: ["Anna", "Bella", "Carla", "Dora", "Emma", "Fiona", "Gina", "Hana", "Iris", "Jana", "Zoe"],
					lastNames: ["Smith", "Jones", "Brown", "Lee", "King", "Hill", "Ward", "Cole", "Reed", "Hart", "Quinn"],
					birthDate: "1985-11-03",
				},
				contactInfo: { zip: "94107" },
			},
		],
	},
	{
		type: "email",
		value: "crossed@example.com",
		provider: "LeakCheck",
		service: "Unknown",
		payload: [
			{
				personalInfo: {
					firstName: "Anna",
					firstNames: ["Anna", "Ann"],
					lastName: "Smith",
					fullNames: ["Anna Maria Smith", "Smith, José Luis", "John O'Neil Jr.", "Cher"],
					birthDates: ["11/03/1985", "July 4, 1776"],
				},
				contactInfo: { fullAddresses: [{ zip: "02134-1234" }, { zipCode: "K1A 0B1" }] },
				vehicles: [{ vin: "1HGCM82633A004352" }],
			},
		],
	},
	{
		type: "email",
		value: "merged@example.com",
		provider: "Veriphone",
		service: "Unknown",
		payload: [{ personalInfo: { firstName: "Zoe", lastName: "Quinn" } }],
	},
	{
		type: "email",
		value: "merged@example.com",
		provider: "Pipl",
		service: "Unknown",
		payload: [
			{
				personalInfo: { birthDate: "499132800000", fullName: "Björn Ålund" },
				contactInfo: { email: "Other@Example.com", phones: ["+1 (415) 555-9317"], zip: "90210" },
			},
		],
	},
	{
		type: "phone",
		value: "14155550101",
		provider: "Pipl",
		service: "Unknown",
		payload: [
			{
				personalInfo: { firstNames: ["김민준", "やまだ", "José"], lastNames: ["Иванов", "Θεοδώρου", "Straße"], birthDate: "#<Date: 1978-03-19 ((2443587j,0s,0n),+0s,2299161j)>" },
				contactInfo: { zip: "00501" },
			},
		],
	},
	{
		type: "people",
		value: "anna smith",
		provider: "PDL",
		service: "Unknown",
		payload: [
			{ names: [{ first: "Anna", last: "Smith" }], dateOfBirth: { start: "1980-01-01" } },
			{ names: [{ first: "José", last: "Müller" }], addresses: [{ zipCode: "90210" }] },
			{
				names: [{ first: "Mia", last: "Lopez" }, { first: "Mia" }, { first: "Zoe", last: "null" }],
				dateOfBirth: { start: "09141990" },
				addresses: [{ zipCode: "10001" }, { zip: "10002" }],
				vehicles: [{ vin: "1hgcm82633a004352" }],
			},
		],
	},
	{
		type: "email",
		value: "oversized@example.com",
		provider: "LeakCheck",
		service: "Unknown",
		payload: [
			{
				personalInfo: {
					firstNames: many(30, (i) => `first${i}`),
					lastNames: many(29, (i) => `last${i}`),
					birthDates: many(5, (i) => `1980-01-0${i + 1}`),
				},
				contactInfo: { fullAddresses: many(5, (i) => ({ zip: `1000${i}` })) },
			},
		],
	},
];

function literal(value: string): string {
	return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
}

function array(values: string[]): string {
	return `[${values.map(literal).join(", ")}]`;
}

async function expected(): Promise<string[]> {
	const reports = new Map<string, { type: ReportType; value: string; records: unknown[] }>();
	for (const row of FIXTURES) {
		const id = `${row.type}\u0000${row.value}`;
		const report = reports.get(id) ?? { type: row.type, value: row.value, records: [] };
		report.records.push(...(row.payload as unknown[]));
		reports.set(id, report);
	}

	const out: string[] = [];
	for (const { type, value, records } of reports.values()) {
		const { groups, candidates } = reportKeyGroups(type, type === "people" ? undefined : value, records);
		const oversized = candidates > MAX_REPORT_KEYS;
		const families = { email: new Set<string>(), phone: new Set<string>(), ndz: new Set<string>(), namevin: new Set<string>() };
		for (const group of groups) {
			const keys = await buildReportKeys(oversized ? { ...group, names: [] } : group);
			for (const family of Object.keys(families) as (keyof typeof families)[]) {
				for (const key of keys[family]) families[family].add(key);
			}
		}
		const sorted = (set: Set<string>) => array([...set].sort());
		out.push(
			`(${literal(type)}, ${literal(value)}, ${sorted(families.email)}, ${sorted(families.phone)}, ` +
				`${sorted(families.ndz)}, ${sorted(families.namevin)}, ${oversized ? 1 : 0})`,
		);
	}
	return out;
}

function viewBody(): string {
	const sql = readFileSync("sql/clickhouse.sql", "utf8");
	const start = sql.indexOf("SQL SECURITY DEFINER\nAS\n");
	if (start < 0) throw new Error("view definition not found in sql/clickhouse.sql");
	const body = sql.slice(start + "SQL SECURITY DEFINER\nAS\n".length);
	const end = body.indexOf("\n);\n");
	if (end < 0) throw new Error("end of the view definition not found");
	return body.slice(0, end + 2);
}

function sourceRows(): string {
	const rows = FIXTURES.map(
		(row, i) =>
			`(${literal(row.type)}, ${literal(row.value)}, ${literal(row.provider)}, ${literal(row.service)}, ` +
			`toDateTime64(${1700000000 + i}, 3, 'UTC'), ${literal(JSON.stringify(row.payload))})`,
	);
	return (
		"(SELECT * FROM values('type String, normalized_value String, provider String, service String, " +
		"created_at DateTime64(3, \\'UTC\\'), payload_json String',\n" +
		rows.join(",\n") +
		"))"
	);
}

async function main() {
	const body = viewBody().replace("FROM default.entity_search_results", `FROM ${sourceRows()}`);
	if (body.includes("default.entity_search_results")) throw new Error("source table substitution failed");

	const expectedRows = await expected();
	process.stdout.write(`SELECT
    if(v.type = '', e.type, v.type) AS type,
    if(v.normalized_value = '', e.normalized_value, v.normalized_value) AS normalized_value,
    arrayFilter(x -> x != '', [
        if(v.email = e.email, '', 'email'),
        if(v.phone = e.phone, '', 'phone'),
        if(v.ndz = e.ndz, '', 'ndz'),
        if(v.namevin = e.namevin, '', 'namevin'),
        if(v.oversized = e.oversized, '', 'oversized')
    ]) AS differs,
    length(v.ndz) AS view_ndz, length(e.ndz) AS worker_ndz,
    length(v.namevin) AS view_namevin, length(e.namevin) AS worker_namevin
FROM
(
    SELECT
        toString(type) AS type,
        normalized_value,
        arraySort(arrayDistinct(arrayFlatten(groupArray(email_keys))))   AS email,
        arraySort(arrayDistinct(arrayFlatten(groupArray(phone_keys))))   AS phone,
        arraySort(arrayDistinct(arrayFlatten(groupArray(ndz_keys))))     AS ndz,
        arraySort(arrayDistinct(arrayFlatten(groupArray(namevin_keys)))) AS namevin,
        toUInt8(max(oversized_records))                                  AS oversized
    FROM
    (
${body}
    )
    GROUP BY type, normalized_value
) AS v
FULL OUTER JOIN
(
    SELECT * FROM values('type String, normalized_value String, email Array(String), phone Array(String), ndz Array(String), namevin Array(String), oversized UInt8',
${expectedRows.join(",\n")})
) AS e
ON v.type = e.type AND v.normalized_value = e.normalized_value
ORDER BY type, normalized_value
`);
}

await main();
