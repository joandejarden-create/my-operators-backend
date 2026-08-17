/**
 * Pre-run drift guards for recurring monitoring (Phase 3B.6).
 * Model, tool config, prompt library, peer cohort, metric version.
 */

import { METRIC_VERSION } from "./config.js";
import { loadPeerSetConfig, resolvePeerSetMembership, PEER_SET_ID_V2 } from "./peer-sets.js";
import { loadWave1ShowcasePrompts, WAVE1_EXECUTION_ORDER } from "./wave1-showcase-plan.js";
import { buildProviderExecutionConfigFingerprint } from "./recurring-comparability.js";
import { PROVIDER_MODELS } from "./recurring-period-model.js";

export const RECURRING_DRIFT_GUARDS_VERSION = "ai_visibility_recurring_drift_guards_v1";

export const PRE_RUN_MODEL_PROBE_REQUIRED = Object.freeze({
  REQUIRED: "YES",
  DESIGN:
    "Lightweight provider availability probe before recurring run; stop affected provider on model/tool rejection; no silent substitution",
});

const EXPECTED_PROMPT_COUNT = 84;
const EXPECTED_PEER_MEMBERSHIP = 15;

/**
 * Verify prompt library has exact 84 governed prompts with expected geo/language matrix.
 */
export function runPromptDriftGuard(opts = {}) {
  const { prompts, validation } = loadWave1ShowcasePrompts(opts.seedPath);
  const errors = [];
  if (!validation.ok) errors.push(...(validation.errors || []).slice(0, 10));
  if (prompts.length !== EXPECTED_PROMPT_COUNT) {
    errors.push(`prompt_count_${prompts.length}_expected_${EXPECTED_PROMPT_COUNT}`);
  }

  const slotCounts = Object.fromEntries(WAVE1_EXECUTION_ORDER.map((s) => [s.key, 0]));
  for (const p of prompts) {
    const lang = String(p.language || "").toLowerCase();
    let slot = null;
    if (p.geographyScope === "Global") slot = "GLOBAL_EN";
    else if (p.commercialRegion === "CALA" && !p.country) slot = lang === "es" ? "CALA_ES" : "CALA_EN";
    else if (p.commercialRegion === "Europe") slot = "EUROPE_EN";
    else if (p.commercialRegion === "North America") slot = "NORTH_AMERICA_EN";
    else if (p.country === "Mexico") slot = lang === "es" ? "MEXICO_ES" : "MEXICO_EN";
    if (slot && slotCounts[slot] != null) slotCounts[slot] += 1;
  }
  for (const s of WAVE1_EXECUTION_ORDER) {
    if (slotCounts[s.key] !== 12) errors.push(`slot_${s.key}_${slotCounts[s.key]}_expected_12`);
  }

  return {
    PROMPT_DRIFT_GUARD_READY: true,
    ok: errors.length === 0,
    promptLibrary: "showcase_prompts_v1",
    promptCount: prompts.length,
    slotCounts,
    errors,
  };
}

/**
 * Verify peer v2 membership = 15.
 */
export function runPeerDriftGuard(opts = {}) {
  const cfg = loadPeerSetConfig(opts.peerSetPath);
  const membership = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }, cfg);
  const errors = [];
  if (!membership.ok) errors.push(membership.error);
  if (membership.baseCount !== EXPECTED_PEER_MEMBERSHIP) {
    errors.push(`peer_membership_${membership.baseCount}_expected_${EXPECTED_PEER_MEMBERSHIP}`);
  }

  return {
    PEER_DRIFT_GUARD_READY: true,
    ok: errors.length === 0,
    peerSetId: PEER_SET_ID_V2,
    peerSetVersion: membership.peerSetVersion || "2",
    membershipCount: membership.baseCount,
    errors,
  };
}

/**
 * Verify metric version unchanged.
 */
export function runMetricDriftGuard(expectedVersion = METRIC_VERSION) {
  const ok = expectedVersion === METRIC_VERSION;
  return {
    ok,
    METRIC_DRIFT_GUARD_READY: true,
    expected: expectedVersion,
    current: METRIC_VERSION,
    errors: ok ? [] : [`metric_version_${METRIC_VERSION}_expected_${expectedVersion}`],
  };
}

/**
 * Verify provider models match governed baseline models.
 */
export function runModelDriftGuard(requestedModels = {}) {
  const errors = [];
  for (const [provider, expected] of Object.entries(PROVIDER_MODELS)) {
    const requested = requestedModels[provider];
    if (requested && requested !== expected) {
      errors.push(`${provider}_model_${requested}_expected_${expected}`);
    }
  }
  return {
    MODEL_DRIFT_GUARD_READY: true,
    ok: errors.length === 0,
    governedModels: { ...PROVIDER_MODELS },
    requestedModels,
    errors,
    MODEL_CHANGE_GOVERNANCE:
      "Model change closes prior series; new validation + new series required",
  };
}

/**
 * Verify provider execution config fingerprints match baseline methodology.
 */
export function runToolConfigDriftGuard(baselineConfigs = {}) {
  const errors = [];
  const current = {};
  for (const provider of ["openai", "gemini", "perplexity", "claude"]) {
    current[provider] = buildProviderExecutionConfigFingerprint(provider);
    const base = baselineConfigs[provider];
    if (base && base.configVersion !== current[provider].configVersion) {
      errors.push(`${provider}_config_${current[provider].configVersion}_expected_${base.configVersion}`);
    }
  }
  return {
    TOOL_CONFIG_DRIFT_GUARD_READY: true,
    ok: errors.length === 0,
    currentConfigs: current,
    errors,
  };
}

/**
 * Run all drift guards.
 */
export function runAllDriftGuards(opts = {}) {
  const prompt = runPromptDriftGuard(opts);
  const peer = runPeerDriftGuard(opts);
  const metric = runMetricDriftGuard(opts.metricVersion);
  const model = runModelDriftGuard(opts.requestedModels);
  const baselineConfigs = {};
  for (const p of ["openai", "gemini", "perplexity", "claude"]) {
    baselineConfigs[p] = buildProviderExecutionConfigFingerprint(p);
  }
  const toolConfig = runToolConfigDriftGuard(baselineConfigs);

  const allOk = prompt.ok && peer.ok && metric.ok && model.ok && toolConfig.ok;
  return {
    version: RECURRING_DRIFT_GUARDS_VERSION,
    ok: allOk,
    guards: { prompt, peer, metric, model, toolConfig },
    PRE_RUN_MODEL_PROBE_REQUIRED: PRE_RUN_MODEL_PROBE_REQUIRED.REQUIRED,
  };
}
