/**
 * Immutable raw Parallel response store — separate from normalized output.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { resolveDataRoot, ensureDir, writeJsonFile } from "../../../local-store.js";

export const PARALLEL_RAW_STORE_VERSION = "parallel-raw-artifact-v1";

export function resolveParallelRawStoreRoot(env = process.env) {
  return path.join(resolveDataRoot(env), "research", "providers", "parallel", "raw");
}

export function buildRawArtifactRecord(input = {}) {
  const ts = input.timestamp || new Date().toISOString();
  const runId = input.run_id || input.provider_run_id || `prun_${crypto.randomBytes(6).toString("hex")}`;
  return {
    version: PARALLEL_RAW_STORE_VERSION,
    provider: "PARALLEL",
    run_id: runId,
    timestamp: ts,
    input_hash: input.input_hash || null,
    spec_hash: input.spec_hash || null,
    investigation_id: input.investigation_id || null,
    hotel_id: input.hotel_id || null,
    template_id: input.template_id || null,
    compiled: input.compiled || null,
    raw_response: input.raw_response || null,
    sources: input.sources || [],
    usage: input.usage || null,
    latency_ms: input.latency_ms ?? null,
    cost_usd: input.cost_usd ?? null,
    status: input.status || "STORED",
    claim_handoff: { auto_promote: false },
  };
}

/**
 * Write immutable raw artifact. Never overwrites existing run files.
 */
export function storeParallelRawArtifact(record, opts = {}) {
  const root = opts.root || resolveParallelRawStoreRoot(opts.env || process.env);
  ensureDir(root);
  const runId = String(record.run_id || "").replace(/[^a-zA-Z0-9_-]/g, "_");
  const file = path.join(root, `${runId}.json`);
  if (fs.existsSync(file) && !opts.allow_overwrite) {
    const err = new Error("parallel_raw_artifact_exists");
    err.code = "parallel_raw_artifact_exists";
    err.path = file;
    throw err;
  }
  writeJsonFile(file, record);
  return { path: file, run_id: runId };
}
