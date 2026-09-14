/**
 * Audit log files in R2, under ca-drop/logs/.
 *
 * What the spec asks for is modest — the prerequisites list "ability to log
 * request timestamps and upload/download outcomes" — so that is exactly what
 * these files carry: when it happened, what was attempted, whether it
 * succeeded, and how many rows were involved. CalPrivacy has not published a
 * retention period for processing records (the audit rulemaking was still at
 * the comment stage), so these are kept alongside the raw archives.
 *
 * Cron A's events carry NO identifiers at all: counts and outcomes only.
 *
 * Cron C's match log is the one exception, and a narrow one. It records
 * work_item_id, list_type and hash — DROP's own published, pseudonymous
 * identifiers, which carry no plaintext. It never records
 * matched_normalized_value, which is the consumer's actual e-mail address or
 * phone number. That line matters: the match log is evidence that a deletion
 * request was honoured, and it has to name the request; it does not have to
 * name the person.
 *
 * Plain text, one `key: value` per line, so it reads without tooling years
 * from now. One file per event, named by timestamp so the bucket sorts
 * chronologically.
 */

const LOG_PREFIX = "ca-drop/logs/";

export type AuditEvent = {
	event: "download" | "supabase-upsert" | "drop-match";
	run_id: string;
	outcome: "ok" | "failed";
	/** everything else, rendered as key: value lines in the order given */
	fields?: Record<string, string | number | undefined>;
	error?: string;
};

/** Returns the R2 key it wrote, so the caller can put it in the run summary. */
export async function writeAuditLog(env: Env, e: AuditEvent): Promise<string> {
	const at = new Date();
	const stamp = at.toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
	const key = `${LOG_PREFIX}${stamp}_${e.event}_${e.run_id}.txt`;

	const lines = [
		`event: ${e.event}`,
		`at: ${at.toISOString()}`,
		`outcome: ${e.outcome}`,
		`run_id: ${e.run_id}`,
	];
	for (const [k, v] of Object.entries(e.fields ?? {})) {
		if (v !== undefined && v !== "") lines.push(`${k}: ${v}`);
	}
	if (e.error) lines.push(`error: ${e.error}`);

	await env.r2.put(key, lines.join("\n") + "\n", {
		httpMetadata: { contentType: "text/plain; charset=utf-8" },
	});
	return key;
}


/**
 * Cron C's match log: one file per batch that matched something.
 *
 * Written only AFTER Supabase has both recorded the matches and set the
 * status, so the presence of this file means the match is durable. A batch
 * whose write did not fully land fails the run instead, and leaves no file
 * behind claiming otherwise — an audit trail that records matches the
 * database does not have is worse than no audit trail.
 *
 * Returns the R2 key, so the caller can put it in the run summary.
 */
export async function writeMatchLog(
	env: Env,
	e: { run_id: string; batch: number; text: string },
): Promise<string> {
	const at = new Date();
	const stamp = at.toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
	const key = `${LOG_PREFIX}${stamp}_match_${e.run_id}_b${e.batch}.txt`;

	await env.r2.put(key, e.text.endsWith("\n") ? e.text : e.text + "\n", {
		httpMetadata: { contentType: "text/plain; charset=utf-8" },
	});
	return key;
}
