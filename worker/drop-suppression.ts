import { recordSuppressedValue } from "./db";
import { dropKey } from "./drop-normalize";
import type { DropListType } from "./drop-normalize";
import { logRun } from "./logs";

/**
 * The real-time gate's read: is this hash on the DROP list, or is it a value
 * whose report we had to suppress?
 *
 * Two namespaces, read together:
 *
 *   kv             the DROP hash set. Cron C copies it into ClickHouse to match
 *                  and erase against, so it holds California's hashes and
 *                  nothing else.
 *   suppressed_kv  hashes of the values in ca_drop_suppressed_value. Never
 *                  listed by Cron C, so they cannot join the match set.
 *
 * Both mean the same thing to the website: do not search this, do not serve it.
 */
export async function lookupGate(env: Env, hash: string): Promise<boolean> {
	const [drop, suppressed] = await Promise.all([
		env.kv.get(hash),
		env.suppressed_kv.get(hash),
	]);

	return drop !== null || suppressed !== null;
}

export type Suppression = {
	searchType: DropListType;
	/** the raw searched value; normalization and hashing happen here */
	value: string;
};

/**
 * Record a phone or e-mail whose report matched the DROP list although the
 * value itself is not on it.
 *
 * Supabase first, then suppressed_kv. The row is the source of truth and what
 * the repair rebuilds suppressed_kv from; the key is what the gate reads.
 *
 * Best effort. It is not a compliance record -- that is ca_drop_work_item_match
 * -- so it must never fail a request or change an answer, and it runs after the
 * response has gone out.
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
 * The suppressed_kv write, shared with the repair. The key is the bare hash --
 * the namespace is separate, so there is nothing to tell it apart from. The
 * value is the search type; KV never holds the plaintext.
 */
export async function putSuppressedKey(
	env: Env,
	hash: string,
	searchType: string,
): Promise<void> {
	await env.suppressed_kv.put(hash, searchType, {
		metadata: { search_type: searchType },
	});
}
