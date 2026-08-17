import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_LOG_FILE = path.join(
  __dirname,
  "..",
  "data",
  "opportunity-reviews.jsonl"
);

/**
 * Resolve append-only opportunity-review log path.
 * Railway: mount a volume at /data and set
 * OPPORTUNITY_REVIEW_LOG_FILE=/data/opportunity-reviews.jsonl
 */
export function getOpportunityReviewLogFile() {
  const fromFile = (process.env.OPPORTUNITY_REVIEW_LOG_FILE || "").trim();
  if (fromFile) return path.resolve(fromFile);

  const fromDir = (process.env.OPPORTUNITY_REVIEW_LOG_DIR || "").trim();
  if (fromDir) {
    return path.join(path.resolve(fromDir), "opportunity-reviews.jsonl");
  }

  return DEFAULT_LOG_FILE;
}

export function ensureOpportunityReviewLogDir(logFile) {
  const dir = path.dirname(logFile);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

export function getOpportunityReviewStorageMeta() {
  const logFile = getOpportunityReviewLogFile();
  const persistent = Boolean(
    process.env.OPPORTUNITY_REVIEW_LOG_FILE ||
      process.env.OPPORTUNITY_REVIEW_LOG_DIR ||
      logFile.replace(/\\/g, "/").startsWith("/data/")
  );

  let lineCount = 0;
  let bytes = 0;
  let exists = false;

  if (fs.existsSync(logFile)) {
    exists = true;
    const raw = fs.readFileSync(logFile, "utf8");
    bytes = Buffer.byteLength(raw, "utf8");
    lineCount = raw.split(/\r?\n/).filter(Boolean).length;
  }

  return {
    logFile: logFile.replace(/\\/g, "/"),
    exists,
    lineCount,
    bytes,
    persistent,
  };
}
