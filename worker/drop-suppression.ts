import { recordSuppressedSearch } from "./db";
import { dropKey } from "./drop-normalize";
import type { DropListType } from "./drop-normalize";
import { logRun } from "./logs";
import type { DropKeyFamily } from "./drop-report";

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

export type GateSource = "drop" | "suppressed-report";

/**
 * The real-time gate's read: is this hash on the DROP list, or has a report for
 * it been suppressed before?
 *
 * One bulk get for both keys, so adding the second source costs no latency and
 * no extra subrequest. DROP membership wins the `source` when both are present,
 * because it is the statutory fact and the other is an inference from it.
 */
export async function lookupGate(
	kv: KVNamespace,
	hash: string,
): Promise<{ listed: boolean; source?: GateSource }> {
	const found = await kv.get([hash, suppressedKey(hash)]);

	if (found.get(hash) !== null && found.get(hash) !== undefined) {
		return { listed: true, source: "drop" };
	}

	const suppressed = found.get(suppressedKey(hash));
	if (suppressed !== null && suppressed !== undefined) {
		return { listed: true, source: "suppressed-report" };
	}

	return { listed: false };
}

export type Suppression = {
	searchType: DropListType;
	/** the raw searched value; the hash is derived here, not by the caller */
	value: string;
	matched: DropKeyFamily[];
	recordsSuppressed: number;
	recordsTotal: number;
};

/**
 * Remember a suppressed search, in Supabase and in KV.
 *
 * Supabase is the source of truth — Cron A with clearKv wipes the KV namespace,
 * and the fast path is rebuilt from the table afterwards. KV is written second
 * and is what the gate actually reads.
 *
 * Best effort by design, and the one place in this Worker where that is the
 * right answer. The finding is a cost optimisation: losing it means the next
 * search spends a credit and gets suppressed again, which is what happened
 * before this existed. It is not a compliance record — that is
 * ca_drop_work_item_match — so it must never fail a request or change an
 * answer. Failures are logged and swallowed.
 *
 * Everything it does, the hashing included, is meant to run after the response
 * has been sent. Nothing here is on the answer's path.
 */
export async function recordSuppression(env: Env, s: Suppression): Promise<boolean> {
	let hash: string;
	const ctx = { workflow: "drop-suppressed-search", run_id: s.searchType };

	try {
		hash = (await dropKey(s.searchType, s.value)).hash;
	} catch (e) {
		await logRun(env, ctx, "failed", {
			error: `hash: ${e instanceof Error ? `${e.name}: ${e.message}` : String(e)}`,
		});
		return false;
	}

	try {
		await recordSuppressedSearch(env, {
			search_type: s.searchType,
			hash,
			matched: s.matched,
			records_suppressed: s.recordsSuppressed,
			records_total: s.recordsTotal,
		});
	} catch (e) {
		await logRun(env, ctx, "failed", {
			error: e instanceof Error ? `${e.name}: ${e.message}` : String(e),
			result: { recordsSuppressed: s.recordsSuppressed },
		});
		return false;
	}

	try {
		// No list_type in the metadata, and a prefixed name, so Cron C's pass over
		// the namespace cannot read this as a DROP hash.
		await env.kv.put(suppressedKey(hash), s.matched.join(",") || "suppressed", {
			metadata: {
				kind: "suppressed-report",
				search_type: s.searchType,
				matched: s.matched,
				recorded_at: new Date().toISOString(),
			},
		});
	} catch (e) {
		await logRun(env, ctx, "failed", {
			error: `KV: ${e instanceof Error ? `${e.name}: ${e.message}` : String(e)}`,
		});
		return false;
	}

	return true;
}
