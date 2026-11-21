/**
 * Resumable factory checkpoints — durable root (worktree-independent).
 */

import fs from "node:fs";
import path from "node:path";
import { resolveDurableRoot, ensureDurableLayout, runDirDurable } from "./durable-paths-v1.js";

export const HIF_CHECKPOINT_V1 = "hotel-intelligence-factory-checkpoint-v1";

export function resolveFactoryPaths(opts = {}) {
  const durable = resolveDurableRoot(opts);
  const layout = ensureDurableLayout(durable.root);
  return { ...durable, layout };
}

/** @deprecated Prefer runDirDurable via resolveFactoryPaths — kept for callers passing durableRoot */
export function runDir(cwdOrRoot, runId, opts = {}) {
  if (opts.durableRoot || process.env.HIF_DURABLE_ROOT) {
    const { root } = resolveDurableRoot({ durableRoot: opts.durableRoot, cwd: cwdOrRoot });
    return runDirDurable(root, runId);
  }
  // If first arg looks like durable root (contains hif-durable) use directly
  if (String(cwdOrRoot).includes("hif-durable")) {
    return runDirDurable(cwdOrRoot, runId);
  }
  const { root } = resolveDurableRoot({ cwd: cwdOrRoot });
  return runDirDurable(root, runId);
}

export function checkpointPath(durableRootOrCwd, runId, opts = {}) {
  return path.join(runDir(durableRootOrCwd, runId, opts), "checkpoint.json");
}

export function loadCheckpoint(durableRootOrCwd, runId, opts = {}) {
  const p = checkpointPath(durableRootOrCwd, runId, opts);
  if (!fs.existsSync(p)) {
    return {
      version: HIF_CHECKPOINT_V1,
      runId,
      durableRoot: resolveDurableRoot({ cwd: durableRootOrCwd, durableRoot: opts.durableRoot }).root,
      completedStages: [],
      units: {
        accessions: {},
        findingKeys: {},
        manifestRows: {},
        persistKeys: {},
        providerFingerprints: {},
      },
      updatedAt: null,
    };
  }
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function saveCheckpoint(durableRootOrCwd, runId, checkpoint, opts = {}) {
  const dir = runDir(durableRootOrCwd, runId, opts);
  fs.mkdirSync(dir, { recursive: true });
  checkpoint.runId = runId;
  checkpoint.updatedAt = new Date().toISOString();
  fs.writeFileSync(checkpointPath(durableRootOrCwd, runId, opts), JSON.stringify(checkpoint, null, 2), "utf8");
  return checkpoint;
}

export function markUnit(checkpoint, bucket, key, meta = {}) {
  if (!key) return checkpoint;
  if (!checkpoint.units[bucket]) checkpoint.units[bucket] = {};
  checkpoint.units[bucket][key] = { at: new Date().toISOString(), ...meta };
  return checkpoint;
}

export function isUnitDone(checkpoint, bucket, key) {
  return Boolean(checkpoint?.units?.[bucket]?.[key]);
}

export function markStageComplete(checkpoint, stage) {
  if (!checkpoint.completedStages.includes(stage)) {
    checkpoint.completedStages.push(stage);
  }
  return checkpoint;
}

export default {
  resolveFactoryPaths,
  loadCheckpoint,
  saveCheckpoint,
  markUnit,
  isUnitDone,
  markStageComplete,
  runDir,
  checkpointPath,
};
