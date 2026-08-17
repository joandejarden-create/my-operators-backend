/**
 * Operator Fit Engine v2 — feature flag (server-controlled).
 * Semantic name: operatorFitEngineV2
 * Default: OFF. Set OPERATOR_FIT_ENGINE_V2=1 to enable.
 *
 * Differentiation v2.1 (targeted refinement) is a separate methodology switch:
 * OPERATOR_FIT_DIFFERENTIATION_V21=1 enables v2.1 scoring for internal/shadow only.
 * Default OFF — v2 path remains the reproducible default.
 *
 * @see docs/architecture/decisions/operator-fit-phase-1-2-founder-decisions.md
 * @see docs/architecture/decisions/operator-fit-v21-differentiation-founder-decisions.md
 */

export const OPERATOR_FIT_ENGINE_V2_FLAG = "operatorFitEngineV2";
/** Historic / default engine identity for v2 path (shortlist snapshots use this). */
export const OPERATOR_FIT_ENGINE_VERSION = "operator-fit-v2.1.0";
/** Explicit v2 methodology label for before/after reports. */
export const OPERATOR_FIT_METHODOLOGY_V2 = "v2";
/** Differentiation refinement methodology. */
export const OPERATOR_FIT_METHODOLOGY_V21 = "v21";
export const OPERATOR_FIT_ENGINE_VERSION_V21 = "operator-fit-v2.1.0-diff";
export const OPERATOR_FIT_DIFFERENTIATION_V21_FLAG = "operatorFitDifferentiationV21";

function truthy(raw) {
  const v = String(raw || "0").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

/**
 * @param {NodeJS.ProcessEnv} [env]
 * @returns {boolean}
 */
export function isOperatorFitEngineV2Enabled(env = process.env) {
  return truthy(env.OPERATOR_FIT_ENGINE_V2);
}

/**
 * Internal/shadow only. Does not enable owner pilot or My Deals.
 * @param {NodeJS.ProcessEnv} [env]
 */
export function isOperatorFitDifferentiationV21Enabled(env = process.env) {
  return truthy(env.OPERATOR_FIT_DIFFERENTIATION_V21);
}

/**
 * Shadow diagnostics may run without exposing owner UI when
 * OPERATOR_FIT_ENGINE_V2_SHADOW=1 (still no Airtable writes).
 * @param {NodeJS.ProcessEnv} [env]
 */
export function isOperatorFitEngineV2ShadowEnabled(env = process.env) {
  if (isOperatorFitEngineV2Enabled(env)) return true;
  return truthy(env.OPERATOR_FIT_ENGINE_V2_SHADOW);
}

/**
 * Resolve methodology for evaluation. Explicit opts.methodology wins;
 * else env OPERATOR_FIT_DIFFERENTIATION_V21; else v2.
 */
export function resolveOperatorFitMethodology(opts = {}, env = process.env) {
  const m = String(opts.methodology || opts.engineMethodology || "").toLowerCase();
  if (m === "v21" || m === "v2.1" || m === OPERATOR_FIT_METHODOLOGY_V21) {
    return OPERATOR_FIT_METHODOLOGY_V21;
  }
  if (m === "v2" || m === OPERATOR_FIT_METHODOLOGY_V2) {
    return OPERATOR_FIT_METHODOLOGY_V2;
  }
  if (opts.useV21 === true || isOperatorFitDifferentiationV21Enabled(env)) {
    return OPERATOR_FIT_METHODOLOGY_V21;
  }
  return OPERATOR_FIT_METHODOLOGY_V2;
}

export function getOperatorFitEngineVersionForMethodology(methodology) {
  return methodology === OPERATOR_FIT_METHODOLOGY_V21
    ? OPERATOR_FIT_ENGINE_VERSION_V21
    : OPERATOR_FIT_ENGINE_VERSION;
}

export function getOperatorFitEngineFlagState(env = process.env) {
  const differentiationV21 = isOperatorFitDifferentiationV21Enabled(env);
  return {
    flag: OPERATOR_FIT_ENGINE_V2_FLAG,
    version: OPERATOR_FIT_ENGINE_VERSION,
    versionV21: OPERATOR_FIT_ENGINE_VERSION_V21,
    methodologyDefault: resolveOperatorFitMethodology({}, env),
    enabled: isOperatorFitEngineV2Enabled(env),
    shadowEnabled: isOperatorFitEngineV2ShadowEnabled(env),
    differentiationV21,
    differentiationEnvKey: "OPERATOR_FIT_DIFFERENTIATION_V21",
    envKey: "OPERATOR_FIT_ENGINE_V2",
    defaultOff: true,
    ownerPilotDisabled: !isOperatorFitEngineV2Enabled(env),
  };
}
