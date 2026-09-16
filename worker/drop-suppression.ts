import { recordSuppressedValue } from "./db";
import { dropKey } from "./drop-normalize";
import type { DropListType } from "./drop-normalize";
import { logRun } from "./logs";

/**
 * A search that must be short-circuited even though its subject is not on the
 * DROP list.
 *
 * A phone number or e-mail address can be absent from the list and still
 * produce a report that cannot be served: the aggregated data carries a person
 * whose name, date of birth and ZIP are listed, or a secondary contact detail
 * that is. The subject is not suppressed; the report is. Every later search for
 * that value would call the providers, spend a credit, build the report and
 * have it suppressed again.
 *
 * WHY THE KEY IS PREFIXED
 *
 * DROP hashes are stored in KV under the bare hash, with no prefix, so the
 * real-time gate is one kv.get with nothing to parse. A suppression is NOT a
 * DROP hash and must not be mistaken for one — Cron C lists the namespace and
 * copies what it finds into ca_drop_work_items to match against. So these keys
 * carry a prefix, which keeps the two sets apart in the one place it matters,
 * and the gate reads both in a single bulk get: two keys, one subrequest.
 *
 * WHY IT IS NOT THE RECORD OF ANYTHING
 *
 * "Searching this yields data we must suppress" is derived, and ours. "This
 * identifier belongs to a consumer who asked to be deleted" is California's,
 * and lives in ca_drop_work_item. Cron B reports status from there and must
 * never report from here, which is why the gate says which of the two it
 * answered from.
 */
export const SUPPRESSED_PREFIX = "suppressed:";

export function suppressedKey(hash: string): string {
	return `${SUPPRESSED_PREFIX}${hash}`;
}

export function isSuppressedKey(key: string): boolean {
	return key.startsWith(SUPPRESSED_PREFIX);
}

/**
 * The real-time gate's read: is this hash on the DROP list, or has a report for
 * it been suppressed before?
 *
 * One bulk get for both keys, so the second source costs no latency and no
 * extra subrequest.
 *
 * One answer, and it is the operational one: do not search this, do not serve
 * it. Both sources mean that to a funnel, and which key answered is not the
 * caller's business — a compliance record is written from
 * ca_drop_work_item_match, never from a gate read.
 */
export async function lookupGate(kv: KVNamespace, hash: string): Promise<boolean> {
	const found = await kv.get([hash, suppressedKey(hash)]);

	return [hash, suppressedKey(hash)].some((key) => {
		const value = found.get(key);

		return value !== null && value !== undefined;
	});
}

export type Suppression = {
	searchType: DropListType;
	/** the raw searched value; normalization and hashing happen here */
	value: string;
};

/**
 * Remember that a search produced a report we had to suppress.
 *
 * Two writes and nothing else. Supabase takes the normalized value, so the
 * finding is readable and auditable and survives a KV wipe; KV takes the hash,
 * because that is what the gate has in hand and it keeps plaintext out of a
 * store the gate reads on ordinary traffic.
 *
 * It is written once and stays. A phone number or e-mail whose report carried
 * DROP-listed data is not processed again -- there is no clearing path and no
 * expiry, so a value recorded here is answered from here from then on.
 *
 * Best effort by design, and the one place in this Worker where that is the
 * right answer. Losing it costs one provider fan-out, which is what happened
 * before any of this existed. It is not a compliance record -- that is
 * ca_drop_work_item_match -- so it must never fail a request or change an
 * answer, and everything here is meant to run after the response has gone out.
 */
export async function recordSuppression(env: Env, s: Suppression): Promise<boolean> {
	const ctx = { workflow: "drop-suppressed-value", run_id: crypto.randomUUID() };

	try {
		const { normalized, hash } = await dropKey(s.searchType, s.value);
		if (normalized === "") return false;

		await recordSuppressedValue(env, s.searchType, normalized);
		await putSuppressedKey(env, hash, s.searchType);
		return true;
	} catch (e) {
		await logRun(env, ctx, "failed", {
			error: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
		});
		return false;
	}
}

/**
 * The KV half, also used by the repair.
 *
 * No list_type in the metadata and a prefixed name, so Cron C's pass over the
 * namespace cannot read this as a DROP hash. The value is the search type, not
 * the identifier: KV is read on ordinary traffic and has no business holding
 * the plaintext when the hash is the thing being looked up.
 */
export async function putSuppressedKey(
	env: Env,
	hash: string,
	searchType: string,
): Promise<void> {
	await env.kv.put(suppressedKey(hash), searchType, {
		metadata: { kind: "suppressed-report", search_type: searchType },
	});
}
