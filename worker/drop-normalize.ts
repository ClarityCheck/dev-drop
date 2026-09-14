/**
 * The DROP normalization and hashing rules, for the real-time gate.
 *
 * Source: https://privacy.ca.gov/drop-for-data-brokers/technical-specifications/working-with-data/
 *
 *   e-mail   "Remove whitespace and convert to lowercase. Do not remove dots,
 *             plus signs, or other characters."
 *   phone    "Strip all non-numeric characters. Use the last 10 digits, or all
 *             digits if fewer than 10 are present."
 *   hash     "Hash with SHA-256 using UTF-8 input encoding. Output as Base64."
 *
 * ---------------------------------------------------------------------------
 * THE SAME RULES ALSO EXIST IN SQL, AND THE TWO MUST NOT DRIFT.
 *
 * ca_drop_combined_search_result derives email_keys and phone_keys with
 * exactly these rules, in ClickHouse:
 *
 *   email  base64Encode(SHA256(lowerUTF8(replaceRegexpAll(v, '\s', ''))))
 *   phone  base64Encode(SHA256(right(replaceRegexpAll(v, '[^0-9]', ''), 10)))
 *
 * Cron C matches against those keys; this file answers the live lookup. If the
 * two implementations disagree by one rule, nothing errors — the gate simply
 * says "not listed" for someone Cron C would have matched, or the reverse. A
 * consumer stops being suppressed and no alarm sounds anywhere.
 *
 * So the tests are conformance vectors generated from the view's own SQL
 * rather than from a reading of the spec. If ClickHouse and this file ever
 * disagree, a test goes red instead of a deletion request going unhonoured.
 * Regenerate them by running the view's expressions over the same inputs.
 * ---------------------------------------------------------------------------
 */

export const DROP_LIST_TYPES = ["email", "phone"] as const;
export type DropListType = (typeof DROP_LIST_TYPES)[number];

export function isDropListType(x: unknown): x is DropListType {
	return typeof x === "string" && (DROP_LIST_TYPES as readonly string[]).includes(x);
}

/**
 * Remove whitespace, lowercase. Nothing else.
 *
 * Dots and plus signs are deliberately kept: to a mail server
 * `user+tag@example.com` and `user@example.com` may be the same inbox, but to
 * DROP they are different identifiers, and "fixing" that here would silently
 * suppress people who are not on the list.
 */
export function normalizeEmail(raw: string): string {
	return raw.replace(/\s/g, "").toLowerCase();
}

/**
 * Digits only, last ten.
 *
 * "Or all digits if fewer than 10" is what slice(-10) already does on a
 * shorter string, matching ClickHouse's right() on the SQL side.
 *
 * Taking from the RIGHT is what drops a country code: +1 415 555 9317 and
 * 415 555 9317 both become 4155559317. It also means trailing digits that are
 * not part of the number — an extension, say — shift the window and produce a
 * different identifier. That is the spec's behaviour, not a bug to route
 * around: the SQL side does the same, and the two agreeing matters more than
 * either being cleverer.
 */
export function normalizePhone(raw: string): string {
	const digits = raw.replace(/[^0-9]/g, "");
	return digits.slice(-10);
}

export function normalize(type: DropListType, raw: string): string {
	return type === "email" ? normalizeEmail(raw) : normalizePhone(raw);
}

/** SHA-256 of the UTF-8 bytes, Base64. 44 characters, usually ending in '='. */
export async function sha256Base64(value: string): Promise<string> {
	const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
	const bytes = new Uint8Array(digest);
	let binary = "";
	for (const b of bytes) binary += String.fromCharCode(b);
	return btoa(binary);
}

/**
 * The KV key for a raw identifier: normalize, then hash.
 *
 * The result is the key itself — Cron A stores the DROP hashes with no prefix
 * precisely so the gate is one kv.get with nothing to parse.
 */
export async function dropKey(
	type: DropListType,
	raw: string,
): Promise<{ normalized: string; hash: string }> {
	const normalized = normalize(type, raw);
	return { normalized, hash: await sha256Base64(normalized) };
}
