/**
 * File-backed local store for hotel_id maps, evidence, review queue, batches.
 * Default root: data/hotel-intelligence/ (override via HOTEL_INTELLIGENCE_DATA_DIR).
 */

import fs from "node:fs";
import path from "node:path";

export const LOCAL_STORE_VERSION = "hotel-intelligence-local-store-v1";

export function resolveDataRoot(env = process.env) {
  const override = String(env.HOTEL_INTELLIGENCE_DATA_DIR || "").trim();
  if (override) return path.resolve(override);
  return path.resolve(process.cwd(), "data", "hotel-intelligence");
}

export function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
  return dirPath;
}

export function readJsonFile(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return structuredClone(fallback);
    const buf = fs.readFileSync(filePath);
    if (!buf.length || buf.every((b) => b === 0)) {
      return structuredClone(fallback);
    }
    const raw = buf.toString("utf8").replace(/^\uFEFF/, "");
    if (!raw.trim() || raw.trimStart().startsWith("\u0000")) {
      return structuredClone(fallback);
    }
    return JSON.parse(raw);
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn(
        JSON.stringify({
          module: "hotel-intelligence/local-store",
          event: "read_json_failed",
          file: path.basename(filePath),
          message: String(err?.message || err).slice(0, 200),
        })
      );
    }
    return structuredClone(fallback);
  }
}

export function writeJsonFile(filePath, data) {
  ensureDir(path.dirname(filePath));
  const payload = `${JSON.stringify(data, null, 2)}\n`;
  const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, payload, "utf8");
  // Windows cannot rename over an existing file — copy then unlink tmp.
  fs.copyFileSync(tmp, filePath);
  try {
    fs.unlinkSync(tmp);
  } catch {
    /* ignore */
  }
}

export function createLocalStore(opts = {}) {
  const root = opts.root || resolveDataRoot(opts.env || process.env);
  ensureDir(root);
  const paths = {
    root,
    hotelIdMap: path.join(root, "hotel-id-map.json"),
    evidence: path.join(root, "field-evidence.json"),
    reviewQueue: path.join(root, "review-queue.json"),
    batches: path.join(root, "batch-jobs.json"),
    stagedHotels: path.join(root, "staged-hotels.json"),
    /** Operational Apify cost ledger (not authoritative hotel data). */
    apifyUsageDir: path.join(root, "apify-usage"),
  };

  return {
    version: LOCAL_STORE_VERSION,
    root,
    paths,
    readHotelIdMap() {
      return readJsonFile(paths.hotelIdMap, { version: 1, by_hotel_id: {}, by_airtable_id: {}, by_external: {} });
    },
    writeHotelIdMap(data) {
      writeJsonFile(paths.hotelIdMap, data);
    },
    readEvidence() {
      return readJsonFile(paths.evidence, { version: 1, items: [] });
    },
    writeEvidence(data) {
      writeJsonFile(paths.evidence, data);
    },
    readReviewQueue() {
      return readJsonFile(paths.reviewQueue, { version: 1, items: [] });
    },
    writeReviewQueue(data) {
      writeJsonFile(paths.reviewQueue, data);
    },
    readBatches() {
      return readJsonFile(paths.batches, { version: 1, jobs: [] });
    },
    writeBatches(data) {
      writeJsonFile(paths.batches, data);
    },
    readStagedHotels() {
      return readJsonFile(paths.stagedHotels, { version: 1, hotels: {} });
    },
    writeStagedHotels(data) {
      writeJsonFile(paths.stagedHotels, data);
    },
  };
}
