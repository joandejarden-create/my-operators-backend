/**
 * Client publication gate for AI Visibility monitoring batches.
 * Scorecard sees all batches; client reads only PASS + publishable.
 */

import fs from "fs";
import {
  resolveValidationStorageRoot,
  validationManifestPath,
  validationReportPath,
} from "./validation-storage-root.js";
import { OVERALL_VALIDATION_STATUS } from "./validation-status.js";

export const PUBLICATION_GATE_VERSION = "ai_intelligence_publication_gate_v1";

/** Only PASS is client-publishable in this phase. */
export const CLIENT_PUBLISHABLE_STATUSES = Object.freeze([
  OVERALL_VALIDATION_STATUS.PASS,
]);

/**
 * @param {string} batchId
 * @param {{ rootDir?: string }} [options]
 */
export function loadBatchValidationManifest(batchId, options = {}) {
  if (!batchId) return null;
  const { rootDir } = resolveValidationStorageRoot(options);
  const p = validationManifestPath(rootDir, batchId);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * @param {string} batchId
 * @param {{ rootDir?: string, requireManifest?: boolean }} [options]
 */
export function isBatchClientPublishable(batchId, options = {}) {
  const manifest = loadBatchValidationManifest(batchId, options);
  if (!manifest) {
    // No validation manifest → NOT_VALIDATED → not client-visible
    return false;
  }
  if (manifest.publishable !== true) return false;
  if (!CLIENT_PUBLISHABLE_STATUSES.includes(manifest.overallValidationStatus)) {
    return false;
  }
  return true;
}

/**
 * Filter summaries to client-publishable only (preserve sort order).
 * @template T
 * @param {T[]} summaries
 * @param {{ rootDir?: string }} [options]
 * @returns {{
 *   publishable: T[],
 *   excluded: T[],
 *   latestExcludedBatchId: string|null,
 *   fallbackBatchId: string|null,
 *   latestBatchFailedValidation: boolean,
 * }}
 */
export function filterSummariesForClientPublication(summaries, options = {}) {
  const list = Array.isArray(summaries) ? summaries : [];
  const { rootDir } = resolveValidationStorageRoot(options);
  const reportPath = validationReportPath(rootDir);
  const gateActive = fs.existsSync(reportPath);

  // Until a validation report exists, do not hide monitoring (tests / pre-gate envs).
  if (!gateActive) {
    return {
      publishable: list,
      excluded: [],
      latestExcludedBatchId: null,
      fallbackBatchId: list[0]?.batchId || list[0]?.wave1Id || null,
      latestBatchFailedValidation: false,
      gateActive: false,
    };
  }

  const publishable = [];
  const excluded = [];
  for (const s of list) {
    const id = s?.batchId || s?.wave1Id;
    if (id && isBatchClientPublishable(id, options)) publishable.push(s);
    else excluded.push(s);
  }
  const latestId = list[0]?.batchId || list[0]?.wave1Id || null;
  const latestExcluded =
    latestId && !isBatchClientPublishable(latestId, options) ? latestId : null;
  return {
    publishable,
    excluded,
    latestExcludedBatchId: latestExcluded,
    fallbackBatchId: publishable[0]?.batchId || publishable[0]?.wave1Id || null,
    latestBatchFailedValidation: Boolean(latestExcluded),
    gateActive: true,
  };
}

export function loadPublicationIndex(options = {}) {
  const { rootDir } = resolveValidationStorageRoot(options);
  const reportPath = validationReportPath(rootDir);
  if (!fs.existsSync(reportPath)) {
    return { rootDir, batches: {}, generatedAt: null };
  }
  try {
    const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    const batches = {};
    for (const b of report.batches || []) {
      if (b.BATCH_ID) batches[b.BATCH_ID] = b;
    }
    return { rootDir, batches, generatedAt: report.generatedAt || null };
  } catch {
    return { rootDir, batches: {}, generatedAt: null };
  }
}
