/**
 * Ownership discovery fallback policy.
 * Disabled by default in production — Context.dev remains primary unless explicitly enabled.
 */
import { OWNERSHIP_DISCOVERY_PROVIDERS } from "./ownership-source-discovery.js";

export const OWNERSHIP_DISCOVERY_FALLBACK_VERSION = "ownership-discovery-fallback-v1";

export const FALLBACK_TRIGGER = Object.freeze({
  NO_SEARCH_RESULT: "NO_SEARCH_RESULT",
  RESULT_FILTERED: "RESULT_FILTERED",
  FETCH_FAILED: "FETCH_FAILED",
  NO_OWNERSHIP_PASSAGE: "NO_OWNERSHIP_PASSAGE",
  RELATIONSHIP_AMBIGUOUS: "RELATIONSHIP_AMBIGUOUS",
  GENERIC_ONLY: "GENERIC_ONLY",
});

/**
 * Production default: fallback OFF.
 * Enable only with both:
 *   OWNERSHIP_DISCOVERY_FALLBACK_ENABLED=1
 *   OWNERSHIP_DISCOVERY_FALLBACK_PROVIDER=parallel
 */
export function isOwnershipDiscoveryFallbackEnabled(env = process.env) {
  const on = String(env.OWNERSHIP_DISCOVERY_FALLBACK_ENABLED || "") === "1";
  const provider = String(env.OWNERSHIP_DISCOVERY_FALLBACK_PROVIDER || "").toLowerCase();
  return on && provider === OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL;
}

export function getOwnershipDiscoveryFallbackProvider(env = process.env) {
  if (!isOwnershipDiscoveryFallbackEnabled(env)) return null;
  return OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL;
}

/**
 * Decide whether Context.dev outcome warrants optional Parallel fallback.
 * Never auto-promotes Parallel results to canonical ownership.
 */
export function shouldInvokeOwnershipDiscoveryFallback({
  contextDiscovery = null,
  contextAdjudication = null,
  remainingParallelUsd = 0,
  env = process.env,
  policyOverride = null,
} = {}) {
  const enabled =
    policyOverride?.enabled != null
      ? Boolean(policyOverride.enabled)
      : isOwnershipDiscoveryFallbackEnabled(env);

  if (!enabled) {
    return {
      invoke: false,
      reason: "FALLBACK_DISABLED_BY_DEFAULT",
      provider: null,
      production_default: "context_dev_primary_only",
    };
  }

  if (remainingParallelUsd < 0.01) {
    return { invoke: false, reason: "FALLBACK_BUDGET_EXHAUSTED", provider: null };
  }

  const stop = contextDiscovery?.stop_reason || null;
  const earliest = contextAdjudication?.earliest_failure_stage || null;
  const score = contextAdjudication?.score || {};

  if (score.supported_current_owner_evidence) {
    return { invoke: false, reason: "CONTEXT_ALREADY_HAS_CURRENT_OWNER", provider: null };
  }

  const triggers = [];
  if (stop === "NO_SEARCH_RESULT" || earliest === "NO_SEARCH_RESULT") {
    triggers.push(FALLBACK_TRIGGER.NO_SEARCH_RESULT);
  }
  if (stop === "RESULT_FILTERED" || earliest === "RESULT_FILTERED") {
    triggers.push(FALLBACK_TRIGGER.RESULT_FILTERED);
  }
  if (stop === "FETCH_FAILED" || earliest === "FETCH_FAILED") {
    triggers.push(FALLBACK_TRIGGER.FETCH_FAILED);
  }
  if (earliest === "NO_OWNERSHIP_PASSAGE" || stop === "NO_OWNERSHIP_PASSAGE") {
    triggers.push(FALLBACK_TRIGGER.NO_OWNERSHIP_PASSAGE);
  }
  if (earliest === "RELATIONSHIP_AMBIGUOUS" || score.ambiguous_relationship) {
    triggers.push(FALLBACK_TRIGGER.RELATIONSHIP_AMBIGUOUS);
  }
  if (
    (contextDiscovery?.raw_result_count || 0) > 0 &&
    !score.hotel_specific_ownership_passage_retrieved
  ) {
    triggers.push(FALLBACK_TRIGGER.GENERIC_ONLY);
  }

  if (!triggers.length) {
    return { invoke: false, reason: "NO_FALLBACK_TRIGGER", provider: null };
  }

  return {
    invoke: true,
    reason: triggers.join("|"),
    triggers,
    provider: OWNERSHIP_DISCOVERY_PROVIDERS.PARALLEL,
    promotion_to_canonical: false,
    enrichment_authorized: false,
    note: "Fallback results remain staging-only; native adjudication still required.",
  };
}

/**
 * Merge Context + optional Parallel adjudications with provenance preserved.
 */
export function mergeDiscoveryAdjudications({ context = null, parallel = null, fallbackDecision = null } = {}) {
  return {
    version: OWNERSHIP_DISCOVERY_FALLBACK_VERSION,
    fallback_decision: fallbackDecision,
    context,
    parallel,
    primary_provider: "context_dev",
    fallback_provider: parallel ? "parallel" : null,
    supported_current_owner_by_provider: {
      context_dev: Boolean(context?.score?.supported_current_owner_evidence),
      parallel: Boolean(parallel?.score?.supported_current_owner_evidence),
    },
    parallel_found_hotel_specific_missed_by_context: Boolean(
      parallel?.score?.hotel_specific_ownership_passage_retrieved &&
        !context?.score?.hotel_specific_ownership_passage_retrieved
    ),
    parallel_more_current_owner: Boolean(
      parallel?.score?.supported_current_owner_evidence &&
        !context?.score?.supported_current_owner_evidence
    ),
    enrichment_authorized: false,
    publication_authorized: false,
    canonical_writes: false,
  };
}
