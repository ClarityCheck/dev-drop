import { normalizeEmail, normalizePhone, sha256Base64 } from "./drop-normalize";

export const REPORT_TYPES = ["email", "phone", "people"] as const;
export type ReportType = (typeof REPORT_TYPES)[number];

export function isReportType(x: unknown): x is ReportType {
	return typeof x === "string" && (REPORT_TYPES as readonly string[]).includes(x);
}

export const DROP_KEY_FAMILIES = ["email", "phone", "ndz", "namevin"] as const;
export type DropKeyFamily = (typeof DROP_KEY_FAMILIES)[number];

export const MAX_REPORT_KEYS = 5000;
const KV_BULK_LIMIT = 100;
const KV_BULK_CONCURRENCY = 10;

function transliterationTable(
	sources: string,
	targets: readonly string[],
): Record<string, string> {
	const characters = [...sources];
	if (characters.length !== targets.length) {
		throw new Error(
			`transliteration table: ${characters.length} sources for ${targets.length} targets`,
		);
	}
	const table: Record<string, string> = {};
	for (let i = 0; i < characters.length; i++) table[characters[i]] = targets[i];
	return table;
}

const GREEK = transliterationTable(
	"αβγδεζηθικλμνξοπρσςτυφχψωάέίήύόώϊΐϋΰ",
	[
		"a", "v", "g", "d", "e", "z", "i", "th", "i", "k", "l", "m", "n", "x", "o",
		"p", "r", "s", "s", "t", "y", "f", "ch", "ps", "o",
		"a", "e", "i", "i", "y", "o", "o", "i", "i", "y", "y",
	],
);

const CYRILLIC = transliterationTable(
	"абвгдеёжзийклмнопрстуфхцчшщъыьэюяєіїґў",
	[
		"a", "b", "v", "g", "d", "e", "yo", "zh", "z", "i", "y", "k", "l", "m",
		"n", "o", "p", "r", "s", "t", "u", "f", "kh", "ts", "ch", "sh", "shch",
		"", "y", "", "e", "yu", "ya", "e", "i", "i", "g", "u",
	],
);

const SPECIAL = transliterationTable(
	"ßæœøðþłđħŋıĳŀ",
	["ss", "ae", "oe", "o", "d", "th", "l", "d", "h", "n", "i", "ij", "l"],
);

const TRANSLITERATION: Record<string, string> = { ...GREEK, ...CYRILLIC, ...SPECIAL };

const NON_NAME_CHARACTERS =
	/[^a-z0-9\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}\p{Script=Hangul}\p{Script=Arabic}\p{Script=Hebrew}]/gu;

export function normalizeName(raw: string): string {
	let transliterated = "";
	for (const character of raw.toLowerCase()) {
		transliterated += TRANSLITERATION[character] ?? character;
	}
	return transliterated.normalize("NFD").replace(NON_NAME_CHARACTERS, "");
}

export function normalizeDob(raw: string): string {
	if (!/^(19|20)\d{2}/.test(raw)) return "";
	const digits = raw.replace(/[^0-9]/g, "").slice(0, 8);
	return digits.length === 8 ? digits : "";
}

export function normalizeZip(raw: string): string {
	const head = raw.split("-")[0].toLowerCase().replace(/[^a-z0-9]/g, "");
	return head.replace(/^0+/, "").slice(0, 5);
}

export function normalizeVin(raw: string): string {
	return raw.toLowerCase().replace(/[^a-z0-9]/g, "");
}

export type ReportFields = {
	emails: string[];
	phones: string[];
	firstNames: string[];
	lastNames: string[];
	dobs: string[];
	zips: string[];
	vins: string[];
};

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function scalars(value: unknown): string[] {
	const items = Array.isArray(value) ? value : [value];
	const out: string[] = [];
	for (const item of items) {
		if (typeof item === "string") out.push(item);
		else if (typeof item === "number" && Number.isFinite(item)) out.push(String(item));
	}
	return out;
}

function records(value: unknown): JsonRecord[] {
	if (Array.isArray(value)) return value.filter(isRecord);
	return isRecord(value) ? [value] : [];
}

function nested(sources: JsonRecord[], key: string): JsonRecord[] {
	return sources.flatMap((source) => records(source[key]));
}

function fields(sources: JsonRecord[], ...keys: string[]): string[] {
	const out: string[] = [];
	for (const source of sources) {
		for (const key of keys) out.push(...scalars(source[key]));
	}
	return out;
}

function distinct(values: string[], normalize: (value: string) => string): string[] {
	const out = new Set<string>();
	for (const value of values) {
		const normalized = normalize(value);
		if (normalized !== "") out.add(normalized);
	}
	return [...out];
}

export function extractReportFields(report: unknown): ReportFields {
	const emails: string[] = [];
	const phones: string[] = [];
	const firstNames: string[] = [];
	const lastNames: string[] = [];
	const dobs: string[] = [];
	const zips: string[] = [];
	const vins: string[] = [];

	for (const person of records(report)) {
		const personalInfo = records(person.personalInfo);
		const contactInfo = records(person.contactInfo);
		const names = records(person.names);
		const addresses = [...nested(contactInfo, "fullAddresses"), ...records(person.addresses)];

		firstNames.push(
			...fields(personalInfo, "firstName", "firstNames"),
			...fields(names, "first"),
		);
		lastNames.push(
			...fields(personalInfo, "lastName", "lastNames"),
			...fields(names, "last"),
		);
		dobs.push(
			...fields(personalInfo, "birthDate", "birthDates"),
			...fields(records(person.dateOfBirth), "start"),
		);
		zips.push(...fields(addresses, "zip", "zipCode"), ...fields(contactInfo, "zip"));
		vins.push(...fields(records(person.vehicles), "vin"));
		emails.push(
			...fields(contactInfo, "email", "emails"),
			...fields(records(person.emails), "address"),
		);
		phones.push(
			...fields(contactInfo, "phone", "phones"),
			...fields(records(person.phones), "number"),
		);
	}

	return {
		emails: distinct(emails, normalizeEmail),
		phones: distinct(phones, normalizePhone),
		firstNames: distinct(firstNames, normalizeName),
		lastNames: distinct(lastNames, normalizeName),
		dobs: distinct(dobs, normalizeDob),
		zips: distinct(zips, normalizeZip),
		vins: distinct(vins, normalizeVin),
	};
}

export function withSearchedValue(
	report: ReportFields,
	type: ReportType,
	value: string,
): ReportFields {
	if (type === "email") {
		return { ...report, emails: distinct([value, ...report.emails], normalizeEmail) };
	}
	if (type === "phone") {
		return { ...report, phones: distinct([value, ...report.phones], normalizePhone) };
	}
	return report;
}

export function countReportKeys(report: ReportFields): number {
	const nameCombinations = report.firstNames.length * report.lastNames.length;
	return (
		report.emails.length +
		report.phones.length +
		nameCombinations * report.dobs.length * report.zips.length +
		nameCombinations * report.vins.length
	);
}

export async function buildReportKeys(
	report: ReportFields,
): Promise<Record<DropKeyFamily, string[]>> {
	const digests = new Map<string, string>();
	const digest = async (value: string): Promise<string> => {
		const cached = digests.get(value);
		if (cached !== undefined) return cached;
		const computed = await sha256Base64(value);
		digests.set(value, computed);
		return computed;
	};

	const [email, phone, firsts, lasts, births, zips, vins] = await Promise.all([
		Promise.all(report.emails.map(digest)),
		Promise.all(report.phones.map(digest)),
		Promise.all(report.firstNames.map(digest)),
		Promise.all(report.lastNames.map(digest)),
		Promise.all(report.dobs.map(digest)),
		Promise.all(report.zips.map(digest)),
		Promise.all(report.vins.map(digest)),
	]);

	const ndzInputs = new Set<string>();
	const namevinInputs = new Set<string>();
	for (const first of firsts) {
		for (const last of lasts) {
			for (const birth of births) {
				for (const zip of zips) ndzInputs.add(first + last + birth + zip);
			}
			for (const vin of vins) namevinInputs.add(first + last + vin);
		}
	}

	const [ndz, namevin] = await Promise.all([
		Promise.all([...ndzInputs].map(sha256Base64)),
		Promise.all([...namevinInputs].map(sha256Base64)),
	]);

	return { email, phone, ndz, namevin };
}

export async function lookupDropKeys(
	kv: KVNamespace,
	keys: Record<DropKeyFamily, string[]>,
): Promise<{ keysChecked: number; matched: DropKeyFamily[] }> {
	const families = new Map<string, DropKeyFamily[]>();
	for (const family of DROP_KEY_FAMILIES) {
		for (const key of keys[family]) {
			const existing = families.get(key);
			if (existing) existing.push(family);
			else families.set(key, [family]);
		}
	}

	const chunks: string[][] = [];
	const all = [...families.keys()];
	for (let i = 0; i < all.length; i += KV_BULK_LIMIT) {
		chunks.push(all.slice(i, i + KV_BULK_LIMIT));
	}

	const matched = new Set<DropKeyFamily>();
	for (let i = 0; i < chunks.length; i += KV_BULK_CONCURRENCY) {
		const batch = chunks.slice(i, i + KV_BULK_CONCURRENCY);
		const results = await Promise.all(batch.map((chunk) => kv.get(chunk)));
		for (const result of results) {
			for (const [key, value] of result) {
				if (value === null) continue;
				for (const family of families.get(key) ?? []) matched.add(family);
			}
		}
	}

	return {
		keysChecked: families.size,
		matched: DROP_KEY_FAMILIES.filter((family) => matched.has(family)),
	};
}
