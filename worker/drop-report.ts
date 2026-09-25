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

const LATIN = /\p{Script=Latin}/u;

export function normalizeName(raw: string): string {
	let out = "";
	for (const character of raw.normalize("NFC").toLowerCase()) {
		const mapped = TRANSLITERATION[character] ?? character;
		out += LATIN.test(mapped) ? mapped.normalize("NFD") : mapped;
	}
	return out.replace(NON_NAME_CHARACTERS, "");
}

const MONTH_NAMES = [
	"january", "february", "march", "april", "may", "june",
	"july", "august", "september", "october", "november", "december",
];

function monthNumber(word: string): number {
	const w = word.toLowerCase();
	const full = MONTH_NAMES.indexOf(w);
	if (full >= 0) return full + 1;
	const short = MONTH_NAMES.findIndex((m) => m.slice(0, 3) === w);
	if (short >= 0) return short + 1;
	return w === "sept" ? 9 : 0;
}

function yyyymmdd(year: number, month: number, day: number): string {
	if (year < 1700 || year > 2099 || month < 1 || month > 12 || day < 1 || day > 31) return "";
	return `${year}${String(month).padStart(2, "0")}${String(day).padStart(2, "0")}`;
}

/** Epoch milliseconds, in the range ClickHouse's DateTime64 can also read. */
const EPOCH_MS_MIN = -2208988800000;
const EPOCH_MS_MAX = 4102444799999;

/**
 * DROP wants YYYYMMDD with a four-digit year. Providers send, in order of how
 * this tries them:
 *
 *   -315619200000       epoch milliseconds, 11-13 digits, read in UTC
 *   1985-11-03[T…]      year first — the digits, in order
 *   11/03/1985, 3/7/1985
 *                       month first, as US data is, unless the first number
 *                       is over 12 and so can only be the day
 *   09141990            eight digits that are not year first: MMDDYYYY, by the
 *                       same month-first rule
 *   July 4, 1776        a month name, full or three letters — the spec's own
 *                       example
 *
 * Before that, two wrappers seen in provider data are taken off: a trailing
 * format note ("08/18/1987 (MM/DD/YYYY)") and a Ruby Date printout
 * ("#<Date: 1978-03-19 ((2443587j,0s,0n),+0s,2299161j)>").
 *
 * Anything else yields no date. The same rules are in the ClickHouse view.
 */
export function normalizeDob(value: string): string {
	let raw = value.replace(/ \([A-Za-z/]+\)$/, "");
	const ruby = /^#<Date: (\d{4}-\d{2}-\d{2})/.exec(raw);
	if (ruby) raw = ruby[1];

	if (/^-?\d{11,13}$/.test(raw)) {
		const ms = Number(raw);
		if (ms < EPOCH_MS_MIN || ms > EPOCH_MS_MAX) return "";
		const t = new Date(ms);
		return yyyymmdd(t.getUTCFullYear(), t.getUTCMonth() + 1, t.getUTCDate());
	}

	if (/^(1[7-9]|20)\d{2}/.test(raw)) {
		const digits = raw.replace(/[^0-9]/g, "").slice(0, 8);
		if (digits.length !== 8) return "";
		return yyyymmdd(Number(digits.slice(0, 4)), Number(digits.slice(4, 6)), Number(digits.slice(6, 8)));
	}

	const numeric =
		/^(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})$/.exec(raw) ?? /^(\d{2})(\d{2})(\d{4})$/.exec(raw);
	if (numeric) {
		const [a, b, year] = [Number(numeric[1]), Number(numeric[2]), Number(numeric[3])];
		return a > 12 ? yyyymmdd(year, b, a) : yyyymmdd(year, a, b);
	}

	const named = /^([A-Za-z]+)\.? +(\d{1,2}),? +(\d{4})$/.exec(raw);
	if (named) {
		const month = monthNumber(named[1]);
		return month ? yyyymmdd(Number(named[3]), month, Number(named[2])) : "";
	}

	return "";
}

const NAME_SUFFIXES = new Set(["jr", "sr", "ii", "iii", "iv"]);

/**
 * First and last name candidates from a full name, before normalization.
 *
 *   "Anna Maria Smith"   first: "Anna", "Anna Maria"   last: "Smith"
 *   "Smith, Anna Maria"  the same, from the comma form
 *   "John Smith Jr"      a trailing Jr / Sr / II / III / IV is dropped first
 *
 * Splitting a full name is a guess, so both first-name readings are offered —
 * the first word alone, and every given name run together the way DROP's
 * compound-name rule would write "Juan Pablo". A wrong guess produces a key
 * nobody holds, so extra candidates can only add matches, never cause one.
 * A single word yields nothing: there is no last name to pair it with.
 */
export function fullNameParts(raw: string): { firsts: string[]; lasts: string[] } {
	const comma = raw.indexOf(",");
	let given: string[];
	let last: string;

	if (comma >= 0) {
		last = raw.slice(0, comma);
		given = raw.slice(comma + 1).split(" ").filter((t) => t !== "");
	} else {
		const tokens = raw.split(" ").filter((t) => t !== "");
		const tail = (tokens[tokens.length - 1] ?? "").toLowerCase().replace(/[^a-z]/g, "");
		if (tokens.length > 2 && NAME_SUFFIXES.has(tail)) tokens.pop();
		if (tokens.length < 2) return { firsts: [], lasts: [] };
		last = tokens[tokens.length - 1];
		given = tokens.slice(0, -1);
	}

	if (given.length === 0) return { firsts: [], lasts: [] };
	return { firsts: [given[0], given.join(" ")], lasts: [last] };
}

export function normalizeZip(raw: string): string {
	const head = raw.split("-")[0].toLowerCase().replace(/[^a-z0-9]/g, "");
	return head.replace(/^0+/, "").slice(0, 5);
}

export function normalizeVin(raw: string): string {
	return raw.toLowerCase().replace(/[^a-z0-9]/g, "");
}

export type NamePair = readonly [first: string, last: string];

export type ReportFields = {
	emails: string[];
	phones: string[];
	names: NamePair[];
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
	names: [],
	dobs: [],
	zips: [],
	vins: [],
};

function distinctPairs(pairs: NamePair[]): NamePair[] {
	const out = new Map<string, NamePair>();
	for (const [first, last] of pairs) {
		if (first !== "" && last !== "") out.set(`${first}\u0000${last}`, [first, last]);
	}
	return [...out.values()];
}

function crossPairs(firsts: string[], lasts: string[]): NamePair[] {
	return firsts.flatMap((first) => lasts.map((last): NamePair => [first, last]));
}

function nonEmpty(values: string[]): string[] {
	return values.filter((value) => value !== "" && value !== "null");
}

function namePairs(personalInfo: JsonRecord[], names: JsonRecord[]): NamePair[] {
	const raw: NamePair[] = [];
	for (const info of personalInfo) {
		const firsts = nonEmpty(fields([info], "firstName", "firstNames"));
		const lasts = nonEmpty(fields([info], "lastName", "lastNames"));
		if (firsts.length === lasts.length) {
			firsts.forEach((first, i) => raw.push([first, lasts[i]]));
		} else {
			raw.push(...crossPairs(firsts, lasts));
		}
		for (const full of nonEmpty(fields([info], "fullName", "fullNames"))) {
			const parts = fullNameParts(full);
			raw.push(...crossPairs(parts.firsts, parts.lasts));
		}
	}
	for (const entry of names) {
		raw.push(...crossPairs(nonEmpty(fields([entry], "first")), nonEmpty(fields([entry], "last"))));
	}
	return distinctPairs(raw.map(([first, last]) => [normalizeName(first), normalizeName(last)]));
}

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
		names: namePairs(personalInfo, names),
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

const FIELD_NAMES = (Object.keys(NO_FIELDS) as (keyof ReportFields)[]).filter(
	(field): field is Exclude<keyof ReportFields, "names"> => field !== "names",
);

export function mergeReportFields(groups: ReportFields[]): ReportFields {
	const merged = { ...NO_FIELDS };
	for (const field of FIELD_NAMES) {
		merged[field] = [...new Set(groups.flatMap((group) => group[field]))];
	}
	merged.names = distinctPairs(groups.flatMap((group) => group.names));
	return merged;
}

/**
 * How a report is divided into key groups, which decides what a cross product
 * may span.
 *
 * people   one group per array element. The elements are different people, so
 *          combining one person's name with another's ZIP would invent a key
 *          for someone who does not exist.
 *
 * email    one group for the whole report. The elements are providers, not
 * phone    people -- every one of them describes the searched subject -- so the
 *          union of their fields is that one person, and a name from one
 *          provider belongs with a DOB and a ZIP from another. Splitting them
 *          per provider derives no NDZ key at all whenever the four factors
 *          arrive from different providers, which is the ordinary case.
 *
 * The subject stays its own group either way, so a caller can still tell the
 * statutory fact -- the searched identifier is itself on a DROP list -- from
 * the wider finding that the report is about someone who is.
 */
export function reportGroups(
	type: ReportType,
	subject: ReportFields,
	perRecord: ReportFields[],
): ReportFields[] {
	if (type === "people") return [subject, ...perRecord];
	return [subject, mergeReportFields([subject, ...perRecord])];
}

export function reportKeyGroups(
	type: ReportType,
	value: string | undefined,
	report: unknown,
): { records: ReportRecord[]; groups: ReportFields[]; candidates: number } {
	const records = extractReportRecords(report);
	const groups = reportGroups(
		type,
		subjectFields(type, value),
		records.map((record) => record.fields),
	);
	const candidates = groups.reduce((total, group) => total + countReportKeys(group), 0);
	return { records, groups, candidates };
}

export function countReportKeys(report: ReportFields): number {
	return (
		report.emails.length +
		report.phones.length +
		report.names.length * report.dobs.length * report.zips.length +
		report.names.length * report.vins.length
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

	const [email, phone, names, births, zips, vins] = await Promise.all([
		Promise.all(report.emails.map(digest)),
		Promise.all(report.phones.map(digest)),
		Promise.all(
			report.names.map(async ([first, last]) => (await digest(first)) + (await digest(last))),
		),
		Promise.all(report.dobs.map(digest)),
		Promise.all(report.zips.map(digest)),
		Promise.all(report.vins.map(digest)),
	]);

	const ndzInputs = new Set<string>();
	const namevinInputs = new Set<string>();
	for (const name of names) {
		for (const birth of births) {
			for (const zip of zips) ndzInputs.add(name + birth + zip);
		}
		for (const vin of vins) namevinInputs.add(name + vin);
	}

	const [ndz, namevin] = await Promise.all([
		Promise.all([...ndzInputs].map(sha256Base64)),
		Promise.all([...namevinInputs].map(sha256Base64)),
	]);

	return { email, phone, ndz, namevin };
}

/**
 * A confirmed match, as DROP itself names it. work_item_id and hash are DROP's
 * own published identifiers and carry no plaintext, which is why they are the
 * only things allowed into a log or an evidence file.
 */
/** The metadata Cron A writes beside each DROP hash. */
type KvMeta = { work_item_id?: string; list_type?: string; request_date?: string };

export type DropHit = { list_type: string; work_item_id: string; hash: string };

export async function lookupDropKeys(
	kv: KVNamespace,
	groups: Record<DropKeyFamily, string[]>[],
): Promise<{ keysChecked: number; matched: DropKeyFamily[][]; hits: DropHit[] }> {
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
	const hits = new Map<string, DropHit>();
	for (let i = 0; i < chunks.length; i += KV_BULK_CONCURRENCY) {
		const batch = chunks.slice(i, i + KV_BULK_CONCURRENCY);
		// getWithMetadata, not get, and the bulk form of it — same one
		// subrequest per 100 keys. The metadata is what Cron A wrote next to
		// the hash, and it carries DROP's own list_type.
		const results = await Promise.all(
			batch.map((chunk) => kv.getWithMetadata<KvMeta>(chunk)),
		);
		for (const result of results) {
			for (const [key, entry] of result) {
				// A key that is not there maps to null, not to an entry whose value
				// is null — the type says otherwise, so the truthiness check is
				// load-bearing rather than defensive.
				if (!entry || entry.value === null) continue;
				for (const owner of owners.get(key) ?? []) {
					// Two different things, deliberately kept apart.
					//
					// The FAMILY is ours: which shape of key we derived and matched
					// on, which is what `matched` reports and what tells a caller
					// whether a person or a contact detail was hit.
					found[owner.group].add(owner.family);

					// The LIST TYPE is DROP's, read from the metadata rather than
					// assumed from the family. They agree in every sane case — the
					// hash spaces are disjoint by construction — but this value is
					// half the key recordMatches joins ca_drop_work_item on, so
					// inferring it would turn a misfiled DROP CSV into
					// "KV and Supabase have drifted" and point at the wrong thing.
					hits.set(`${owner.family}::${key}`, {
						list_type: entry.metadata?.list_type ?? owner.family,
						work_item_id: entry.metadata?.work_item_id ?? entry.value,
						hash: key,
					});
				}
			}
		}
	}

	return {
		keysChecked: owners.size,
		matched: found.map((families) => DROP_KEY_FAMILIES.filter((f) => families.has(f))),
		hits: [...hits.values()],
	};
}
