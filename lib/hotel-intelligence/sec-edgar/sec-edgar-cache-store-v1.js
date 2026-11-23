/**
 * SEC EDGAR response-cache store — transparent gzip, atomic writes, integrity helpers.
 *
 * Permanent lossless compression for HIF / Hotel Explorer research caches.
 * - Reads: prefer valid `.json.gz`, fall back to legacy `.json` (never assume same basename ⇒ same content).
 * - Writes: atomic `.json.gz` only (temp file + rename).
 * - Migration delete of uncompressed originals requires explicit authorization (never default).
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import zlib from "node:zlib";

export const SEC_EDGAR_CACHE_STORE_VERSION = "sec-edgar-cache-store-v1";
export const AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE =
  "AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE";

const GZIP_MAGIC_0 = 0x1f;
const GZIP_MAGIC_1 = 0x8b;

export function sha256Buffer(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

export function sha256Utf8(text) {
  return sha256Buffer(Buffer.from(String(text), "utf8"));
}

export function isGzipBuffer(buf) {
  return Buffer.isBuffer(buf) && buf.length >= 2 && buf[0] === GZIP_MAGIC_0 && buf[1] === GZIP_MAGIC_1;
}

export function canonicalJsonBytes(record) {
  return Buffer.from(JSON.stringify(record), "utf8");
}

export function gzipJsonRecord(record, opts = {}) {
  const level = Number.isFinite(opts.level) ? opts.level : zlib.constants.Z_BEST_COMPRESSION;
  const uncompressed = canonicalJsonBytes(record);
  const compressed = zlib.gzipSync(uncompressed, { level });
  return {
    uncompressed,
    compressed,
    uncompressedBytes: uncompressed.length,
    compressedBytes: compressed.length,
    sha256: sha256Buffer(uncompressed),
  };
}

export function gunzipToUtf8(buf) {
  if (!isGzipBuffer(buf)) {
    throw new Error("NOT_GZIP");
  }
  return zlib.gunzipSync(buf).toString("utf8");
}

/**
 * Read a single cache file path (`.json` or `.json.gz`) into a parsed record.
 * Does not mutate files. Throws on corrupt gzip / invalid JSON when throwOnError.
 */
export function readCacheFile(filePath, opts = {}) {
  const throwOnError = Boolean(opts.throwOnError);
  const monitor = opts.monitor || null;
  try {
    const onDisk = fs.readFileSync(filePath);
    const lower = String(filePath).toLowerCase();
    let utf8;
    let encoding;
    if (lower.endsWith(".json.gz") || isGzipBuffer(onDisk)) {
      utf8 = gunzipToUtf8(onDisk);
      encoding = "json.gz";
    } else {
      utf8 = onDisk.toString("utf8");
      encoding = "json";
    }
    const record = JSON.parse(utf8);
    const sha256 = sha256Utf8(utf8);
    if (monitor) {
      monitor.reads += 1;
      monitor.readBytesOnDisk += onDisk.length;
      monitor.readUncompressedBytes += Buffer.byteLength(utf8, "utf8");
      if (encoding === "json.gz") monitor.gzipReads += 1;
      else monitor.jsonReads += 1;
    }
    return {
      ok: true,
      record,
      encoding,
      bytesOnDisk: onDisk.length,
      uncompressedBytes: Buffer.byteLength(utf8, "utf8"),
      sha256,
      path: filePath,
    };
  } catch (err) {
    if (monitor) {
      monitor.readFailures += 1;
      monitor.lastReadError = String(err?.message || err);
    }
    if (throwOnError) throw err;
    return {
      ok: false,
      error: String(err?.message || err),
      path: filePath,
    };
  }
}

export function jsonPathForKey(cacheDir, key) {
  return path.join(cacheDir, `${key}.json`);
}

export function gzipPathForKey(cacheDir, key) {
  return path.join(cacheDir, `${key}.json.gz`);
}

/**
 * Resolve which file to read for a cache key.
 * Prefer a valid `.json.gz`; otherwise fall back to `.json`.
 * Never treat basename equality as content equality — each path is hashed independently.
 */
export function resolveCacheEntry(cacheDir, key, opts = {}) {
  const gzipPath = gzipPathForKey(cacheDir, key);
  const jsonPath = jsonPathForKey(cacheDir, key);
  const monitor = opts.monitor || null;

  if (fs.existsSync(gzipPath)) {
    const gz = readCacheFile(gzipPath, { monitor, throwOnError: false });
    if (gz.ok) return { ...gz, key, preferred: "json.gz" };
    // Corrupt gzip — fall through to legacy JSON if present.
    if (monitor) monitor.corruptGzip += 1;
  }

  if (fs.existsSync(jsonPath)) {
    const js = readCacheFile(jsonPath, { monitor, throwOnError: false });
    if (js.ok) return { ...js, key, preferred: "json" };
  }

  return { ok: false, key, error: "CACHE_MISS" };
}

function ensureParentDir(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

/**
 * Atomic write of a gzip-compressed JSON cache entry.
 * Writes `<dest>.tmp.<pid>.<rand>` then renames into place.
 */
export function writeGzipCacheAtomic(destGzipPath, record, opts = {}) {
  const monitor = opts.monitor || null;
  ensureParentDir(destGzipPath);
  const packed = gzipJsonRecord(record, opts);
  const tmp = `${destGzipPath}.tmp.${process.pid}.${crypto.randomBytes(4).toString("hex")}`;
  try {
    fs.writeFileSync(tmp, packed.compressed);
    try {
      fs.renameSync(tmp, destGzipPath);
    } catch (renameErr) {
      // Windows: replace existing destination
      if (fs.existsSync(destGzipPath)) {
        fs.unlinkSync(destGzipPath);
      }
      fs.renameSync(tmp, destGzipPath);
      void renameErr;
    }
    if (monitor) {
      monitor.writes += 1;
      monitor.writeUncompressedBytes += packed.uncompressedBytes;
      monitor.writeCompressedBytes += packed.compressedBytes;
    }
    return {
      ok: true,
      path: destGzipPath,
      uncompressedBytes: packed.uncompressedBytes,
      compressedBytes: packed.compressedBytes,
      sha256: packed.sha256,
      ratio:
        packed.uncompressedBytes > 0
          ? 1 - packed.compressedBytes / packed.uncompressedBytes
          : 0,
    };
  } catch (err) {
    try {
      if (fs.existsSync(tmp)) fs.unlinkSync(tmp);
    } catch {
      /* ignore cleanup */
    }
    if (monitor) {
      monitor.writeFailures += 1;
      monitor.lastWriteError = String(err?.message || err);
    }
    return { ok: false, error: String(err?.message || err), path: destGzipPath };
  }
}

export function createCacheMonitor(seed = {}) {
  return {
    reads: 0,
    gzipReads: 0,
    jsonReads: 0,
    readFailures: 0,
    corruptGzip: 0,
    readBytesOnDisk: 0,
    readUncompressedBytes: 0,
    writes: 0,
    writeFailures: 0,
    writeUncompressedBytes: 0,
    writeCompressedBytes: 0,
    lastReadError: null,
    lastWriteError: null,
    ...seed,
  };
}

export function monitorSnapshot(monitor) {
  const m = monitor || createCacheMonitor();
  const savings =
    m.writeUncompressedBytes > 0
      ? 1 - m.writeCompressedBytes / m.writeUncompressedBytes
      : null;
  return {
    version: SEC_EDGAR_CACHE_STORE_VERSION,
    ...m,
    writeCompressionSavings: savings,
  };
}

/**
 * True when another process likely has the file open/locked (best-effort).
 * On Windows, opening with exclusive access fails if locked.
 */
export function isFileBusy(filePath) {
  try {
    const fd = fs.openSync(filePath, "r+");
    fs.closeSync(fd);
    return false;
  } catch (err) {
    const code = err && err.code;
    if (code === "EBUSY" || code === "EPERM" || code === "EACCES") return true;
    if (code === "ENOENT") return false;
    return false;
  }
}

/**
 * Migrate one uncompressed `.json` cache file to `.json.gz`.
 * Dry-run by default. Deletes original only with explicit authorization after SHA-256 verify.
 */
export function migrateJsonCacheFile(jsonPath, opts = {}) {
  const dryRun = opts.dryRun !== false; // default true
  const deleteOriginal = Boolean(opts.deleteOriginal);
  const authorize = String(opts.authorizeDelete || "");
  const monitor = opts.monitor || null;

  const base = String(jsonPath);
  if (!base.toLowerCase().endsWith(".json") || base.toLowerCase().endsWith(".json.gz")) {
    return { ok: false, skipped: true, reason: "NOT_JSON_CACHE", path: jsonPath };
  }
  if (!fs.existsSync(jsonPath)) {
    return { ok: false, skipped: true, reason: "MISSING", path: jsonPath };
  }
  if (isFileBusy(jsonPath)) {
    return { ok: false, skipped: true, reason: "FILE_BUSY", path: jsonPath };
  }

  const before = readCacheFile(jsonPath, { throwOnError: true, monitor });
  const gzPath = `${jsonPath}.gz`;
  const plan = {
    ok: true,
    dryRun,
    path: jsonPath,
    gzipPath: gzPath,
    sha256Before: before.sha256,
    uncompressedBytes: before.uncompressedBytes,
  };

  if (dryRun) {
    const packed = gzipJsonRecord(before.record);
    return {
      ...plan,
      compressedBytes: packed.compressedBytes,
      ratio: packed.uncompressedBytes > 0 ? 1 - packed.compressedBytes / packed.uncompressedBytes : 0,
      wouldDeleteOriginal: deleteOriginal && authorize === AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE,
      action: "DRY_RUN",
    };
  }

  // If gzip already exists, verify content before any delete — never assume equality by name.
  if (fs.existsSync(gzPath)) {
    const existing = readCacheFile(gzPath, { throwOnError: false, monitor });
    if (existing.ok && existing.sha256 === before.sha256) {
      if (deleteOriginal && authorize === AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE) {
        fs.unlinkSync(jsonPath);
        return { ...plan, action: "ALREADY_COMPRESSED_DELETED_ORIGINAL", compressedBytes: existing.bytesOnDisk };
      }
      return {
        ...plan,
        action: "ALREADY_COMPRESSED_KEEP_ORIGINAL",
        compressedBytes: existing.bytesOnDisk,
        note: "Original retained — delete requires --apply --delete-original + authorize token",
      };
    }
    if (existing.ok && existing.sha256 !== before.sha256) {
      return {
        ok: false,
        path: jsonPath,
        gzipPath: gzPath,
        reason: "CONTENT_MISMATCH_SAME_BASENAME",
        sha256Json: before.sha256,
        sha256Gzip: existing.sha256,
        action: "ABORT_PRESERVE_BOTH",
      };
    }
  }

  const written = writeGzipCacheAtomic(gzPath, before.record, { monitor });
  if (!written.ok) {
    return { ok: false, path: jsonPath, error: written.error, action: "WRITE_FAILED" };
  }

  const after = readCacheFile(gzPath, { throwOnError: true, monitor });
  if (after.sha256 !== before.sha256) {
    // Leave both files; do not delete. Remove bad gzip if hashes diverge.
    try {
      fs.unlinkSync(gzPath);
    } catch {
      /* preserve for forensics if unlink fails */
    }
    return {
      ok: false,
      path: jsonPath,
      reason: "SHA256_MISMATCH_AFTER_WRITE",
      sha256Before: before.sha256,
      sha256After: after.sha256,
      action: "ROLLED_BACK_GZIP",
    };
  }

  let deleted = false;
  if (deleteOriginal) {
    if (authorize !== AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE) {
      return {
        ok: true,
        ...plan,
        compressedBytes: written.compressedBytes,
        ratio: written.ratio,
        action: "COMPRESSED_ORIGINAL_RETAINED",
        note: "Pass authorizeDelete=AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE to remove .json after verify",
      };
    }
    fs.unlinkSync(jsonPath);
    deleted = true;
  }

  return {
    ok: true,
    ...plan,
    compressedBytes: written.compressedBytes,
    ratio: written.ratio,
    deletedOriginal: deleted,
    action: deleted ? "COMPRESSED_AND_DELETED_ORIGINAL" : "COMPRESSED_ORIGINAL_RETAINED",
  };
}

/**
 * Scan a cache directory for size / compression monitoring (read-only).
 */
export function scanCacheDirectory(cacheDir, opts = {}) {
  const limit = Math.max(0, Number(opts.limit || 0) || 0);
  const sampleCompress = opts.sampleCompress !== false;
  const sampleLimit = Math.max(0, Number(opts.sampleLimit || 5) || 5);
  if (!fs.existsSync(cacheDir)) {
    return {
      ok: false,
      cacheDir,
      error: "MISSING_DIR",
      jsonFiles: 0,
      gzipFiles: 0,
      otherFiles: 0,
      jsonBytes: 0,
      gzipBytes: 0,
    };
  }

  let jsonFiles = 0;
  let gzipFiles = 0;
  let otherFiles = 0;
  let jsonBytes = 0;
  let gzipBytes = 0;
  const samples = [];
  const names = fs.readdirSync(cacheDir);
  for (const name of names) {
    if (limit && jsonFiles + gzipFiles + otherFiles >= limit) break;
    const full = path.join(cacheDir, name);
    let st;
    try {
      st = fs.statSync(full);
    } catch {
      continue;
    }
    if (!st.isFile()) continue;
    const lower = name.toLowerCase();
    if (lower.endsWith(".json.gz")) {
      gzipFiles += 1;
      gzipBytes += st.size;
    } else if (lower.endsWith(".json")) {
      jsonFiles += 1;
      jsonBytes += st.size;
      if (sampleCompress && samples.length < sampleLimit) {
        try {
          const raw = fs.readFileSync(full);
          const packed = zlib.gzipSync(raw, { level: zlib.constants.Z_BEST_COMPRESSION });
          samples.push({
            file: name,
            uncompressedBytes: raw.length,
            compressedBytes: packed.length,
            ratio: raw.length > 0 ? 1 - packed.length / raw.length : 0,
            sha256: sha256Buffer(raw),
          });
        } catch (err) {
          samples.push({ file: name, error: String(err?.message || err) });
        }
      }
    } else if (lower.includes(".tmp.")) {
      otherFiles += 1;
    } else {
      otherFiles += 1;
    }
  }

  const sampleAvg =
    samples.filter((s) => s.ratio != null).reduce((a, s) => a + s.ratio, 0) /
    Math.max(1, samples.filter((s) => s.ratio != null).length);

  return {
    ok: true,
    version: SEC_EDGAR_CACHE_STORE_VERSION,
    cacheDir,
    jsonFiles,
    gzipFiles,
    otherFiles,
    jsonBytes,
    gzipBytes,
    totalBytes: jsonBytes + gzipBytes,
    sampleCompression: samples,
    sampleAverageSavings: samples.some((s) => s.ratio != null) ? sampleAvg : null,
    projectedGzipBytes:
      samples.some((s) => s.ratio != null) && jsonBytes > 0
        ? Math.round(jsonBytes * (1 - sampleAvg))
        : null,
  };
}

export default {
  SEC_EDGAR_CACHE_STORE_VERSION,
  AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE,
  sha256Buffer,
  sha256Utf8,
  isGzipBuffer,
  canonicalJsonBytes,
  gzipJsonRecord,
  gunzipToUtf8,
  readCacheFile,
  jsonPathForKey,
  gzipPathForKey,
  resolveCacheEntry,
  writeGzipCacheAtomic,
  createCacheMonitor,
  monitorSnapshot,
  isFileBusy,
  migrateJsonCacheFile,
  scanCacheDirectory,
};
