/**
 * Provider validation cost ledger (Phase 3B.2).
 */

import { estimateProviderCost, COST_UNKNOWN } from "./providers/provider-cost.js";

/** Prudent validation hard cap per provider (12 calls). */
export const VALIDATION_HARD_CAP_USD = Object.freeze({
  gemini: 25,
  perplexity: 20,
  claude: 25,
});

/** Recommended full 84-call hard caps (post-validation calibration updates these). */
export const RECOMMENDED_FULL_WAVE_HARD_CAP_USD = Object.freeze({
  gemini: 125,
  perplexity: 100,
  claude: 125,
});

export function createValidationCostLedger(provider, hardCapUsd) {
  const cap =
    hardCapUsd ??
    VALIDATION_HARD_CAP_USD[String(provider || "").toLowerCase()] ??
    25;
  return {
    provider,
    hardCapUsd: Number(cap),
    actualUsd: 0,
    providerReportedUsd: 0,
    inputTokens: 0,
    outputTokens: 0,
    totalTokens: 0,
    providerAttempts: 0,
    logicalCallsCharged: 0,
    capBreached: false,
    stoppedReason: null,
  };
}

export function applyValidationCallCost(ledger, usage, providerId) {
  const est = estimateProviderCost(providerId, usage);
  let cost = est.amountUsd;
  if (cost == null || est.status === COST_UNKNOWN) {
    // Conservative fallback for cap tracking when unknown
    const inTok = Number(usage?.inputTokens || 0);
    const outTok = Number(usage?.outputTokens || 0);
    cost = Number(((inTok + outTok) / 1_000_000) * 15 + 0.05).toFixed(6);
  }
  if (usage?.providerCostUsd != null) {
    ledger.providerReportedUsd = Number(
      (ledger.providerReportedUsd + Number(usage.providerCostUsd)).toFixed(6)
    );
    cost = Number(usage.providerCostUsd);
  }
  ledger.actualUsd = Number((ledger.actualUsd + Number(cost)).toFixed(6));
  ledger.inputTokens += Number(usage?.inputTokens || 0);
  ledger.outputTokens += Number(usage?.outputTokens || 0);
  ledger.totalTokens += Number(usage?.totalTokens || 0);
  ledger.logicalCallsCharged += 1;
  if (ledger.actualUsd >= ledger.hardCapUsd) {
    ledger.capBreached = true;
    ledger.stoppedReason = "hard_cost_cap";
  }
  return { cost: Number(cost), ledger, estimate: est };
}

export function project84CallCost(sampleUsd, sampleCalls) {
  if (!(sampleCalls > 0) || !(sampleUsd > 0)) {
    return { LOW: null, EXPECTED: null, HIGH: null, STATUS: "COST_ESTIMATE_PARTIAL" };
  }
  const avg = sampleUsd / sampleCalls;
  return {
    LOW: Number((avg * 84 * 0.75).toFixed(2)),
    EXPECTED: Number((avg * 84).toFixed(2)),
    HIGH: Number((avg * 84 * 1.35).toFixed(2)),
    AVG_PER_CALL: Number(avg.toFixed(6)),
    STATUS: "CALIBRATED_FROM_VALIDATION",
  };
}
