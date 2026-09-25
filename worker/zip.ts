/**
 * Minimal ZIP reader and CSV parser for the DROP list downloads.
 *
 * No dependency: the deflate side is the runtime's own
 * DecompressionStream("deflate-raw"), and the container format is read
 * straight from the central directory. That is deliberate — the npm registry
 * is not reachable from the build environment, and a ZIP reader for files we
 * produce ourselves is 80 lines, not a library.
 *
 * Zip64 is read: the end-of-central-directory record and every size or
 * offset saturated at 0xFFFF / 0xFFFFFFFF are taken from their 64-bit forms.
 * Writers use it for large archives and some, streaming ones, always.
 */

export type ZipEntry = { name: string; bytes: Uint8Array };

const EOCD_SIG = 0x06054b50;
const CDIR_SIG = 0x02014b50;
const LOCAL_SIG = 0x04034b50;
const EOCD64_LOCATOR_SIG = 0x07064b50;
const EOCD64_SIG = 0x06064b50;
const ZIP64_EXTRA = 0x0001;
const U16_MAX = 0xffff;
const U32_MAX = 0xffffffff;

function uint64(view: DataView, offset: number): number {
	const value = view.getBigUint64(offset, true);
	if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("Zip64 value is out of range");
	return Number(value);
}

function zip64Fields(
	view: DataView,
	extraStart: number,
	extraLen: number,
	needed: { uncompressed: boolean; compressed: boolean; offset: boolean },
): { uncompressed?: number; compressed?: number; offset?: number } {
	let p = extraStart;
	const end = extraStart + extraLen;
	while (p + 4 <= end) {
		const id = view.getUint16(p, true);
		const size = view.getUint16(p + 2, true);
		if (id === ZIP64_EXTRA) {
			let q = p + 4;
			const out: { uncompressed?: number; compressed?: number; offset?: number } = {};
			if (needed.uncompressed) {
				out.uncompressed = uint64(view, q);
				q += 8;
			}
			if (needed.compressed) {
				out.compressed = uint64(view, q);
				q += 8;
			}
			if (needed.offset) out.offset = uint64(view, q);
			return out;
		}
		p += 4 + size;
	}
	throw new Error("Zip64 entry has no Zip64 extra field");
}

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

	let entryCount = view.getUint16(eocd + 10, true);
	let cdirOffset = view.getUint32(eocd + 16, true);
	if (entryCount === U16_MAX || cdirOffset === U32_MAX) {
		const locator = eocd - 20;
		if (locator < 0 || view.getUint32(locator, true) !== EOCD64_LOCATOR_SIG) {
			throw new Error("Zip64 archive without a Zip64 end-of-central-directory locator");
		}
		const eocd64 = uint64(view, locator + 8);
		if (view.getUint32(eocd64, true) !== EOCD64_SIG) {
			throw new Error("corrupt Zip64 end-of-central-directory record");
		}
		entryCount = uint64(view, eocd64 + 32);
		cdirOffset = uint64(view, eocd64 + 48);
	}

	const entries: ZipEntry[] = [];
	let p = cdirOffset;

	for (let n = 0; n < entryCount; n++) {
		if (view.getUint32(p, true) !== CDIR_SIG) {
			throw new Error(`corrupt central directory at entry ${n}`);
		}

		const method = view.getUint16(p + 10, true);
		let compressedSize = view.getUint32(p + 20, true);
		const uncompressedSize = view.getUint32(p + 24, true);
		const nameLen = view.getUint16(p + 28, true);
		const extraLen = view.getUint16(p + 30, true);
		const commentLen = view.getUint16(p + 32, true);
		let localOffset = view.getUint32(p + 42, true);

		const needed = {
			uncompressed: uncompressedSize === U32_MAX,
			compressed: compressedSize === U32_MAX,
			offset: localOffset === U32_MAX,
		};
		if (needed.uncompressed || needed.compressed || needed.offset) {
			const wide = zip64Fields(view, p + 46 + nameLen, extraLen, needed);
			compressedSize = wide.compressed ?? compressedSize;
			localOffset = wide.offset ?? localOffset;
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
	const text = new TextDecoder().decode(bytes).replace(/^\uFEFF/, "");
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
