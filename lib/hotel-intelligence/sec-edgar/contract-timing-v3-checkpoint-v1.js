/**
 * Checkpointed adaptive queue for Contract Timing V3 scale batches.
 */

import fs from "node:fs";
import path from "node:path";

export const CONTRACT_TIMING_V3_CHECKPOINT_V1 = "contract-timing-v3-checkpoint-v1";

export function defaultCheckpointPath(cwd = process.cwd()) {
  return path.join(cwd, "data", "sec-edgar", "contract-timing-v3-checkpoint.json");
}

export function loadV3Checkpoint(filePath = defaultCheckpointPath()) {
  if (!fs.existsSync(filePath)) {
    return {
      version: CONTRACT_TIMING_V3_CHECKPOINT_V1,
      processedAccessions: {},
      processedFindingKeys: {},
      batches: [],
      weights: {},
      updatedAt: null,
    };
  }
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return {
      version: CONTRACT_TIMING_V3_CHECKPOINT_V1,
      processedAccessions: {},
      processedFindingKeys: {},
      batches: [],
      weights: {},
      updatedAt: null,
    };
  }
}

export function saveV3Checkpoint(checkpoint, filePath = defaultCheckpointPath()) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  checkpoint.updatedAt = new Date().toISOString();
  fs.writeFileSync(filePath, JSON.stringify(checkpoint, null, 2), "utf8");
  return checkpoint;
}

export function markAccessionProcessed(checkpoint, accession, meta = {}) {
  if (!accession) return checkpoint;
  checkpoint.processedAccessions[accession] = {
    at: new Date().toISOString(),
    ...meta,
  };
  return checkpoint;
}

export function markFindingKey(checkpoint, key) {
  if (!key) return checkpoint;
  checkpoint.processedFindingKeys[key] = true;
  return checkpoint;
}

export function isAccessionProcessed(checkpoint, accession) {
  return Boolean(checkpoint.processedAccessions?.[accession]);
}

export function isFindingSeen(checkpoint, key) {
  return Boolean(checkpoint.processedFindingKeys?.[key]);
}

export function appendBatchSummary(checkpoint, summary) {
  checkpoint.batches = checkpoint.batches || [];
  checkpoint.batches.push({ ...summary, at: new Date().toISOString() });
  return checkpoint;
}

export default {
  loadV3Checkpoint,
  saveV3Checkpoint,
  markAccessionProcessed,
  markFindingKey,
  isAccessionProcessed,
  isFindingSeen,
  appendBatchSummary,
};
