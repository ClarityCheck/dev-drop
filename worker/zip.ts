/**
 * Minimal ZIP reader and CSV parser for the DROP list downloads.
 *
 * No dependency: the deflate side is the runtime's own
 * DecompressionStream("deflate-raw"), and the container format is read
 * straight from the central directory. That is deliberate — the npm registry
 * is not reachable from the build environment, and a ZIP reader for files we
 * produce ourselves is 80 lines, not a library.
 *
 * Zip64 is not supported. A DROP list big enough to need it would be a
 * different problem anyway (the parse would have to stream), so it throws
 * rather than reading a truncated size silently.
 */

export type ZipEntry = { name: string; bytes: Uint8Array };

const EOCD_SIG = 0x06054b50;
const CDIR_SIG = 0x02014b50;
const LOCAL_SIG = 0x04034b50;

export async function unzip(buffer: ArrayBuffer): Promise<ZipEntry[]> {
	const view = new DataView(buffer);
	const bytes = new Uint8Array(buffer);

	// The end-of-central-directory record sits at the end, possibly followed by
	// a comment, so scan backwards for its signature.
	let eocd = -1;
	for (let i = bytes.length - 22; i >= 0 && i >= bytes.length - 22 - 65535; i--) {
		if (view.getUint32(i, true) === EOCD_SIG) {
			eocd = i;
			break;
		}
	}
	if (eocd < 0) throw new Error("not a ZIP file: no end-of-central-directory record");

	const entryCount = view.getUint16(eocd + 10, true);
	const cdirOffset = view.getUint32(eocd + 16, true);
	if (cdirOffset === 0xffffffff) throw new Error("Zip64 archives are not supported");

	const entries: ZipEntry[] = [];
	let p = cdirOffset;

	for (let n = 0; n < entryCount; n++) {
		if (view.getUint32(p, true) !== CDIR_SIG) {
			throw new Error(`corrupt central directory at entry ${n}`);
		}

		const method = view.getUint16(p + 10, true);
		const compressedSize = view.getUint32(p + 20, true);
		const uncompressedSize = view.getUint32(p + 24, true);
		const nameLen = view.getUint16(p + 28, true);
		const extraLen = view.getUint16(p + 30, true);
		const commentLen = view.getUint16(p + 32, true);
		const localOffset = view.getUint32(p + 42, true);

		if (compressedSize === 0xffffffff || uncompressedSize === 0xffffffff) {
			throw new Error("Zip64 entry sizes are not supported");
		}

		const name = new TextDecoder().decode(bytes.subarray(p + 46, p + 46 + nameLen));
		p += 46 + nameLen + extraLen + commentLen;

		// Directory entries have no payload.
		if (name.endsWith("/")) continue;

		if (view.getUint32(localOffset, true) !== LOCAL_SIG) {
			throw new Error(`corrupt local header for ${name}`);
		}
		const lNameLen = view.getUint16(localOffset + 26, true);
		const lExtraLen = view.getUint16(localOffset + 28, true);
		const dataStart = localOffset + 30 + lNameLen + lExtraLen;
		const raw = bytes.subarray(dataStart, dataStart + compressedSize);

		if (method === 0) {
			entries.push({ name, bytes: raw });
		} else if (method === 8) {
			entries.push({ name, bytes: await inflateRaw(raw) });
		} else {
			throw new Error(`${name}: unsupported compression method ${method}`);
		}
	}

	return entries;
}

async function inflateRaw(raw: Uint8Array): Promise<Uint8Array> {
	const stream = new Response(raw).body!.pipeThrough(
		new DecompressionStream("deflate-raw"),
	);
	return new Uint8Array(await new Response(stream).arrayBuffer());
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

/**
 * DROP list rows. Base64 hashes contain + / = but never a comma or a quote,
 * and the Ids are alphanumeric, so a split on commas is safe here. Surrounding
 * quotes are stripped anyway in case a future column is quoted.
 */
export type CsvRow = Record<string, string>;

export function parseCsv(bytes: Uint8Array): CsvRow[] {
	const text = new TextDecoder().decode(bytes).replace(/^﻿/, "");
	const lines = text.split(/\r?\n/).filter((l) => l.trim() !== "");
	if (lines.length === 0) return [];

	const header = splitLine(lines[0]).map((h) => h.trim().toLowerCase());
	const rows: CsvRow[] = [];

	for (let i = 1; i < lines.length; i++) {
		const cells = splitLine(lines[i]);
		const row: CsvRow = {};
		for (let c = 0; c < header.length; c++) row[header[c]] = (cells[c] ?? "").trim();
		rows.push(row);
	}
	return rows;
}

function splitLine(line: string): string[] {
	return line.split(",").map((c) => c.replace(/^"(.*)"$/, "$1"));
}

// ---------------------------------------------------------------------------
// File naming
// ---------------------------------------------------------------------------

export const LISTS = ["email", "phone", "ndz", "namevin"] as const;
export type ListType = (typeof LISTS)[number];

/**
 * DROP names its files <YYYYMMDD>_<DataBrokerId>_<DataType>[_<suffix>].csv,
 * e.g. 20260910_0000_NameVIN.csv. The data type is read from the third
 * segment, with a substring match as a fallback so a naming tweak on their
 * side does not silently drop a list.
 */
export function listTypeOf(fileName: string): ListType | "removed" | null {
	const base = fileName.split("/").pop()!.replace(/\.csv$/i, "").toLowerCase();
	const segments = base.split("_");
	const candidates = [segments[2] ?? "", base];

	for (const c of candidates) {
		if (c === "removed" || c.includes("removed")) return "removed";
		for (const l of LISTS) if (c === l || c.includes(l)) return l;
	}
	return null;
}
