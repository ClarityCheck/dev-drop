import { clearSuppressedSearch, recordSuppressedSearch } from "./db";
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

/**
 * How long a suppression stays on the fast path before it has to be re-earned.
 *
 * A finding about a report is not permanent the way DROP membership is. The
 * report can change, and DROP can revoke the work item that caused it — its
 * removal list does exactly that. Without an expiry the gate would answer
 * listed: true for that value forever, on evidence nobody ever re-checks,
 * because a short-circuited search never reaches the check that would notice.
 *
 * So the key expires. On the next search after that, the funnel runs once, the
 * report is checked properly, and the finding is either re-recorded or cleared.
 * Thirty days is the cost of one wasted lookup per value per month against a
 * suppression that can never be wrong for longer than that.
 */
export const SUPPRESSION_TTL_SECONDS = 30 * 24 * 60 * 60;

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
			expirationTtl: SUPPRESSION_TTL_SECONDS,
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

/**
 * A later report for this value came back clean, so the suppression no longer
 * applies: stamp the row and take the key off the fast path.
 *
 * This is the other half of the expiry. The TTL guarantees a suppression is
 * re-checked eventually; this acts on the re-check when it happens, so a
 * consumer DROP has released stops being suppressed at the first search rather
 * than at the end of the window.
 *
 * Best effort, for the same reason as recording: it can only ever cost one
 * wasted lookup.
 */
export async function clearSuppression(
	env: Env,
	searchType: DropListType,
	value: string,
): Promise<boolean> {
	const ctx = { workflow: "drop-suppressed-search", run_id: searchType };

	try {
		const { hash } = await dropKey(searchType, value);

		// KV first. It is what the gate reads, so clearing it is what actually
		// stops the short-circuit; the row is bookkeeping.
		await env.kv.delete(suppressedKey(hash));
		return await clearSuppressedSearch(env, searchType, hash);
	} catch (e) {
		await logRun(env, ctx, "failed", {
			error: `clear: ${e instanceof Error ? `${e.name}: ${e.message}` : String(e)}`,
		});
		return false;
	}
}
