/**
 * Missing baseline fingerprint inventory (Phase 3B.5).
 */

import fs from "fs";
import path from "path";
import { buildProviderBaselineExecutionPlan } from "./provider-baseline-plan.js";
import { resolveProviderBaselineStoreRoot } from "./storage/resolve-store-root.js";
import { PEER_SET_ID_V2 } from "./peer-sets.js";
import { METRIC_VERSION } from "./config.js";

export const BASELINE_MISSING_INVENTORY_VERSION = "ai_visibility_baseline_missing_inventory_v1";

export const PHASE_3B5_WAVE_IDS = Object.freeze({
  gemini: "aiv_baseline_gemini_20260814_1105_9b7e19",
  claude: "aiv_baseline_claude_20260814_1204_2a263a",
  perplexity: "aiv_baseline_perplexity_20260814_1007_223198",
});

/** Accepted full baselines — do not load checkpoints for execution. */
export const ACCEPTED_FULL_BASELINE_COUNTS = Object.freeze({
  openai: 84,
  perplexity: 84,
});

function readJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function loadCheckpoint(provider, waveId) {
  const storeRoot = resolveProviderBaselineStoreRoot(provider);
  return readJson(path.join(storeRoot, "checkpoints", `${waveId}.json`));
}

function reasonMissingForFingerprint(cp, fingerprint, exec) {
  if (cp?.completedFingerprints?.[fingerprint]) return null;
  const failed = cp?.failedFingerprints?.[fingerprint];
  if (failed) {
    return {
      priorAttemptStatus: "failed_exhausted",
      reasonMissing: failed.error?.message || failed.error?.category || "provider_failure",
      category: failed.error?.category || null,
      retryable: failed.error?.retryable === true,
    };
  }
  if (cp?.stopReason === "hard_cost_cap") {
    return {
      priorAttemptStatus: "not_executed_cost_cap",
      reasonMissing: "hard_cost_cap",
      category: "COST_CAP",
      retryable: true,
    };
  }
  return {
    priorAttemptStatus: "not_executed",
    reasonMissing: "never_executed",
    category: null,
    retryable: true,
  };
}

/**
 * Inventory missing baseline fingerprints for a provider with an active checkpoint.
 */
export function inventoryProviderMissingFingerprints(provider, waveId) {
  const id = String(provider || "").toLowerCase();
  const plan = buildProviderBaselineExecutionPlan(id);
  if (!plan.ok) {
    return { ok: false, provider: id, waveId, errors: plan.errors, missing: [] };
  }

  const cp = loadCheckpoint(id, waveId);
  if (!cp) {
    return { ok: false, provider: id, waveId, errors: ["checkpoint_not_found"], missing: [] };
  }

  const successful = Object.keys(cp.completedFingerprints || {}).length;
  const missing = [];

  for (const exec of plan.EXECUTIONS) {
    if (cp.completedFingerprints?.[exec.fingerprint]) continue;
    const prior = reasonMissingForFingerprint(cp, exec.fingerprint, exec);
    missing.push({
      provider: id,
      fingerprint: exec.fingerprint,
      promptId: exec.promptId,
      promptFamily: exec.promptFamily,
      intent: exec.intent,
      geography: exec.geographyKey,
      language: exec.language,
      semanticPairId: exec.semanticPairId || null,
      baselineWaveId: waveId,
      baselineSeriesId: cp.baselineSeriesId || plan.baselineSeriesId,
      slot: exec.slot,
      priorAttemptStatus: prior.priorAttemptStatus,
      reasonMissing: prior.reasonMissing,
      retryable: prior.retryable,
      errorCategory: prior.category,
    });
  }

  return {
    ok: true,
    provider: id,
    waveId,
    baselineSeriesId: cp.baselineSeriesId || plan.baselineSeriesId,
    planned: 84,
    successful,
    missingCount: missing.length,
    missing,
    checkpoint: cp,
  };
}

/**
 * Full four-provider missing inventory (Phase 3B.5 preflight).
 */
export function auditMissingBaselineFingerprints(options = {}) {
  const waveIds = { ...PHASE_3B5_WAVE_IDS, ...options.waveIds };

  const openai = {
    provider: "openai",
    planned: 84,
    successful: ACCEPTED_FULL_BASELINE_COUNTS.openai,
    missingCount: 0,
    missing: [],
    accepted: true,
  };

  const perplexityInv = inventoryProviderMissingFingerprints("perplexity", waveIds.perplexity);
  const perplexity = perplexityInv.ok
    ? {
        provider: "perplexity",
        planned: 84,
        successful: perplexityInv.successful,
        missingCount: perplexityInv.missingCount,
        missing: perplexityInv.missing,
        waveId: waveIds.perplexity,
        accepted: perplexityInv.missingCount === 0,
      }
    : {
        provider: "perplexity",
        planned: 84,
        successful: ACCEPTED_FULL_BASELINE_COUNTS.perplexity,
        missingCount: 0,
        missing: [],
        accepted: true,
        note: "checkpoint_optional_accepted_full",
      };

  const gemini = inventoryProviderMissingFingerprints("gemini", waveIds.gemini);
  const claude = inventoryProviderMissingFingerprints("claude", waveIds.claude);

  const totalSuccessful =
    openai.successful +
    (perplexity.successful ?? 0) +
    (gemini.successful ?? 0) +
    (claude.successful ?? 0);
  const totalMissing =
    openai.missingCount +
    (perplexity.missingCount ?? 0) +
    (gemini.missingCount ?? 0) +
    (claude.missingCount ?? 0);

  return {
    version: BASELINE_MISSING_INVENTORY_VERSION,
    peerSetId: PEER_SET_ID_V2,
    metricVersion: METRIC_VERSION,
    promptLibrary: "showcase_prompts_v1",
    OPENAI: openai,
    PERPLEXITY: perplexity,
    GEMINI: gemini.ok
      ? {
          provider: "gemini",
          planned: 84,
          successful: gemini.successful,
          missingCount: gemini.missingCount,
          missing: gemini.missing,
          waveId: gemini.waveId,
          checkpoint: gemini.checkpoint,
        }
      : { ok: false, errors: gemini.errors },
    CLAUDE: claude.ok
      ? {
          provider: "claude",
          planned: 84,
          successful: claude.successful,
          missingCount: claude.missingCount,
          missing: claude.missing,
          waveId: claude.waveId,
          checkpoint: claude.checkpoint,
        }
      : { ok: false, errors: claude.errors },
    TOTAL_SUCCESSFUL: totalSuccessful,
    TOTAL_MISSING: totalMissing,
    RECONCILES_TO_12: totalMissing === 12,
    inventoryValid:
      gemini.ok &&
      claude.ok &&
      gemini.missingCount === 1 &&
      claude.missingCount === 11 &&
      openai.missingCount === 0 &&
      (perplexity.missingCount ?? 0) === 0,
  };
}
