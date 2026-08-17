import fs from "fs";
import {
  ensureOpportunityReviewLogDir,
  getOpportunityReviewLogFile,
} from "./marketing-opportunity-review-path.js";

const RATE_WINDOW_MS = 60 * 60 * 1000;
const RATE_MAX = 5;
const rateBuckets = new Map();

export function sanitizeString(value, maxLen) {
  if (typeof value !== "string") return "";
  return value.trim().replace(/\s+/g, " ").slice(0, maxLen);
}

export function checkRateLimit(ip) {
  const key = String(ip || "unknown");
  const now = Date.now();
  let bucket = rateBuckets.get(key);
  if (!bucket || now - bucket.startedAt > RATE_WINDOW_MS) {
    bucket = { startedAt: now, count: 0 };
    rateBuckets.set(key, bucket);
  }
  if (bucket.count >= RATE_MAX) {
    return { allowed: false, remaining: 0 };
  }
  bucket.count += 1;
  return { allowed: true, remaining: RATE_MAX - bucket.count };
}

/**
 * Persist one opportunity-review submission (append-only JSONL).
 * @param {object} record
 */
export function appendOpportunityReview(record) {
  const logFile = getOpportunityReviewLogFile();
  ensureOpportunityReviewLogDir(logFile);
  fs.appendFileSync(logFile, JSON.stringify(record) + "\n", "utf8");
  return logFile;
}
