/**
 * Cron B's file format, from the specification:
 * https://privacy.ca.gov/drop-for-data-brokers/technical-specifications/working-with-data/
 *
 *   one CSV per downloaded list file, UTF-8, header `Id,Status`
 *   named <YYYYMMDD>_<DataBrokerId>_<DataType>[_<Suffix>].csv, after the file
 *     it answers; the suffix is up to 10 alphanumeric characters and makes a
 *     second file for the same list unique within the cycle
 *   2 Exempted, 3 Deleted, 4 Opted out, 5 Not found
 */

export const DROP_API_URL = "https://api.drop.privacy.ca.gov";

export const STATUS_CODES = [2, 3, 4, 5] as const;
export type StatusCode = (typeof STATUS_CODES)[number];

/**
 * The suffix for one run's files: U for an upload, A for an amend, then the
 * run id. The letter keeps an upload and an amend answering the same
 * downloaded file in one run from sharing a name.
 */
export function reportSuffix(runId: string, kind: "upload" | "amend"): string {
	const id = runId.replace(/[^A-Za-z0-9]/g, "").slice(0, 9);
	if (id === "") throw new Error(`run id ${runId} yields no alphanumeric suffix`);
	return `${kind === "upload" ? "U" : "A"}${id}`;
}

/**
 * The response file's name: the downloaded file's first three segments, then
 * the suffix. A downloaded suffix is replaced rather than kept, because the
 * pattern allows only one.
 */
export function responseFileName(sourceFile: string, suffix: string): string {
	if (!/^[A-Za-z0-9]{1,10}$/.test(suffix)) {
		throw new Error(`suffix ${JSON.stringify(suffix)} is not 1-10 alphanumeric characters`);
	}
	const base = sourceFile.split("/").pop()!.replace(/\.csv$/i, "");
	const segments = base.split("_");
	if (segments.length < 3) {
		throw new Error(`${sourceFile} is not <YYYYMMDD>_<DataBrokerId>_<DataType>.csv`);
	}
	return `${segments.slice(0, 3).join("_")}_${suffix}.csv`;
}

export function renderStatusCsv(rows: { work_item_id: string; code: number }[]): string {
	const lines = ["Id,Status"];
	for (const r of rows) {
		if (!(STATUS_CODES as readonly number[]).includes(r.code)) {
			throw new Error(`work item ${r.work_item_id}: ${r.code} is not a DROP status code`);
		}
		if (/[",\r\n]/.test(r.work_item_id)) {
			throw new Error(`work item id ${JSON.stringify(r.work_item_id)} cannot be written unquoted`);
		}
		lines.push(`${r.work_item_id},${r.code}`);
	}
	return lines.join("\n") + "\n";
}

export function parseStatusCsv(text: string): { work_item_id: string; code: number }[] {
	const lines = text.split(/\r?\n/).filter((l) => l.trim() !== "");
	if (lines[0]?.trim() !== "Id,Status") throw new Error("status file has no Id,Status header");
	return lines.slice(1).map((line) => {
		const [id, code] = line.split(",");
		return { work_item_id: id, code: Number(code) };
	});
}

export type UploadOutcome = {
	accepted: string[];
	rejected: { fileName: string; message: string }[];
};

/** The 202 / 400 body of POST /data/upload and /data/amend. */
export function parseUploadResponse(body: unknown): UploadOutcome | null {
	if (typeof body !== "object" || body === null) return null;
	const b = body as {
		accepted?: { fileName?: unknown }[];
		rejected?: { fileName?: unknown; message?: unknown }[];
	};
	if (!Array.isArray(b.accepted) && !Array.isArray(b.rejected)) return null;
	return {
		accepted: (b.accepted ?? [])
			.map((a) => a.fileName)
			.filter((n): n is string => typeof n === "string"),
		rejected: (b.rejected ?? [])
			.filter((r) => typeof r.fileName === "string")
			.map((r) => ({
				fileName: r.fileName as string,
				message: typeof r.message === "string" ? r.message : "",
			})),
	};
}
