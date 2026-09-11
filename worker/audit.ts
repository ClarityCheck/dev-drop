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
 * Deliberately NO identifiers: no hashes, no e-mails, no phone numbers, no
 * work item ids. Counts and outcomes only. The identifiers themselves live in
 * the places that need them — KV, ca_drop_work_item, and the untouched ZIPs
 * in ca-drop/raw/ — and a log file is not a second copy of consumer data.
 *
 * Plain text, one `key: value` per line, so it reads without tooling years
 * from now. One file per event, named by timestamp so the bucket sorts
 * chronologically.
 */

const LOG_PREFIX = "ca-drop/logs/";

export type AuditEvent = {
	event: "download" | "supabase-upsert";
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
