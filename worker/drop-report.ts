import { normalizeEmail, normalizePhone, sha256Base64 } from "./drop-normalize";

export const REPORT_TYPES = ["email", "phone", "people"] as const;
export type ReportType = (typeof REPORT_TYPES)[number];

export function isReportType(x: unknown): x is ReportType {
	return typeof x === "string" && (REPORT_TYPES as readonly string[]).includes(x);
}

export const DROP_KEY_FAMILIES = ["email", "phone", "ndz", "namevin"] as const;
export type DropKeyFamily = (typeof DROP_KEY_FAMILIES)[number];

export const MAX_REPORT_KEYS = 20000;
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

export const NO_FIELDS: ReportFields = {
	emails: [],
	phones: [],
	firstNames: [],
	lastNames: [],
	dobs: [],
	zips: [],
	vins: [],
};

export type ReportRecord = { index: number; id?: string; fields: ReportFields };

function personFields(person: JsonRecord): ReportFields {
	const personalInfo = records(person.personalInfo);
	const contactInfo = records(person.contactInfo);
	const names = records(person.names);
	const addresses = [...nested(contactInfo, "fullAddresses"), ...records(person.addresses)];

	return {
		emails: distinct(
			[...fields(contactInfo, "email", "emails"), ...fields(records(person.emails), "address")],
			normalizeEmail,
		),
		phones: distinct(
			[...fields(contactInfo, "phone", "phones"), ...fields(records(person.phones), "number")],
			normalizePhone,
		),
		firstNames: distinct(
			[...fields(personalInfo, "firstName", "firstNames"), ...fields(names, "first")],
			normalizeName,
		),
		lastNames: distinct(
			[...fields(personalInfo, "lastName", "lastNames"), ...fields(names, "last")],
			normalizeName,
		),
		dobs: distinct(
			[
				...fields(personalInfo, "birthDate", "birthDates"),
				...fields(records(person.dateOfBirth), "start"),
			],
			normalizeDob,
		),
		zips: distinct(
			[...fields(addresses, "zip", "zipCode"), ...fields(contactInfo, "zip")],
			normalizeZip,
		),
		vins: distinct(fields(records(person.vehicles), "vin"), normalizeVin),
	};
}

export function extractReportRecords(report: unknown): ReportRecord[] {
	return records(report).map((person, index) => {
		const id = scalars(person.pipl_id)[0] ?? scalars(person.pdl_id)[0];
		const fields = personFields(person);
		return id === undefined ? { index, fields } : { index, id, fields };
	});
}

export function subjectFields(type: ReportType, value: string | undefined): ReportFields {
	if (typeof value !== "string") return NO_FIELDS;
	if (type === "email") return { ...NO_FIELDS, emails: distinct([value], normalizeEmail) };
	if (type === "phone") return { ...NO_FIELDS, phones: distinct([value], normalizePhone) };
	return NO_FIELDS;
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
	groups: Record<DropKeyFamily, string[]>[],
): Promise<{ keysChecked: number; matched: DropKeyFamily[][] }> {
	const owners = new Map<string, { group: number; family: DropKeyFamily }[]>();
	for (let group = 0; group < groups.length; group++) {
		for (const family of DROP_KEY_FAMILIES) {
			for (const key of groups[group][family]) {
				const existing = owners.get(key);
				if (existing) existing.push({ group, family });
				else owners.set(key, [{ group, family }]);
			}
		}
	}

	const chunks: string[][] = [];
	const all = [...owners.keys()];
	for (let i = 0; i < all.length; i += KV_BULK_LIMIT) {
		chunks.push(all.slice(i, i + KV_BULK_LIMIT));
	}

	const found = groups.map(() => new Set<DropKeyFamily>());
	for (let i = 0; i < chunks.length; i += KV_BULK_CONCURRENCY) {
		const batch = chunks.slice(i, i + KV_BULK_CONCURRENCY);
		const results = await Promise.all(batch.map((chunk) => kv.get(chunk)));
		for (const result of results) {
			for (const [key, value] of result) {
				if (value === null) continue;
				for (const owner of owners.get(key) ?? []) found[owner.group].add(owner.family);
			}
		}
	}

	return {
		keysChecked: owners.size,
		matched: found.map((families) => DROP_KEY_FAMILIES.filter((f) => families.has(f))),
	};
}
