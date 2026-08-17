/**
 * File-store persistence for competitive gap records (P0C).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const DEFAULT_GAPS_DIR = path.join(
  __dirname,
  "..",
  "..",
  "..",
  "data",
  "ai-visibility",
  "gaps"
);

/**
 * @param {object} payload
 * @param {string} [filePath]
 */
export function saveGapDetectionReport(payload, filePath) {
  const out = filePath || path.join(DEFAULT_GAPS_DIR, `gap-detection-${payload.runId || Date.now()}.json`);
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(payload, null, 2));
  return out;
}

/**
 * @param {object[]} gaps
 * @param {string} [filePath]
 */
export function saveGapRecords(gaps, filePath) {
  const out = filePath || path.join(DEFAULT_GAPS_DIR, "latest-gaps-v1.json");
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(
    out,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        gapCount: gaps.length,
        gaps,
      },
      null,
      2
    )
  );
  return out;
}
