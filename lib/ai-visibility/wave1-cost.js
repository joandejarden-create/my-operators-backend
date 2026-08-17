/**
 * Wave-1 showcase cost ledger helpers (Phase 3A.11).
 * Conservative estimates for hard-cap enforcement — no false precision.
 */

import { WAVE1_COST_EVIDENCE, WAVE1_RETRY_POLICY } from "./wave1-showcase-plan.js";

export const WAVE1_HARD_CAP_USD = WAVE1_COST_EVIDENCE.RECOMMENDED_HARD_CAP_USD;

/**
 * Token-based estimate with web-search premium floor.
 * Prefer provider usage tokens when present; else historical expected per call.
 */
export function estimateWave1CallCostUsd(usage = null, opts = {}) {
  const floor = Number(opts.floorPerCall ?? WAVE1_COST_EVIDENCE.EXPECTED_PER_CALL * 0.35);
  const inTok = Number(usage?.inputTokens || 0);
  const outTok = Number(usage?.outputTokens || 0);
  const tot = Number(usage?.totalTokens || inTok + outTok);
  if (!(tot > 0)) {
    return Number(Math.max(floor, WAVE1_COST_EVIDENCE.EXPECTED_PER_CALL).toFixed(6));
  }
  // Conservative blended rate (USD / 1M tokens) + web_search overhead attribution
  const inputRate = Number(opts.inputRatePerM ?? 5);
  const outputRate = Number(opts.outputRatePerM ?? 40);
  const webSearchOverhead = Number(opts.webSearchOverheadUsd ?? 0.08);
  const tokenCost =
    (inTok / 1_000_000) * inputRate + (outTok / 1_000_000) * outputRate + webSearchOverhead;
  return Number(Math.max(floor, tokenCost).toFixed(6));
}

export function createWave1CostLedger(hardCapUsd = WAVE1_HARD_CAP_USD) {
  return {
    hardCapUsd: Number(hardCapUsd),
    actualUsd: 0,
    inputTokens: 0,
    outputTokens: 0,
    totalTokens: 0,
    providerAttempts: 0,
    logicalCallsCharged: 0,
    capBreached: false,
    stoppedReason: null,
  };
}

export function applyWave1CallCost(ledger, usage, attemptCount = 1) {
  const cost = estimateWave1CallCostUsd(usage);
  const inTok = Number(usage?.inputTokens || 0);
  const outTok = Number(usage?.outputTokens || 0);
  const tot = Number(usage?.totalTokens || inTok + outTok);
  ledger.actualUsd = Number((ledger.actualUsd + cost).toFixed(6));
  ledger.inputTokens += inTok;
  ledger.outputTokens += outTok;
  ledger.totalTokens += tot;
  ledger.providerAttempts += Math.max(1, attemptCount);
  ledger.logicalCallsCharged += 1;
  if (ledger.actualUsd >= ledger.hardCapUsd) {
    ledger.capBreached = true;
    ledger.stoppedReason = "hard_cost_cap";
  }
  return { cost, ledger };
}

/**
 * Before starting a new provider attempt, refuse if remaining headroom cannot cover
 * a conservative next-call high estimate.
 */
export function wouldBreachHardCap(ledger, nextCallHighUsd = WAVE1_COST_EVIDENCE.HIGH_PER_CALL) {
  if (ledger.capBreached) return true;
  return ledger.actualUsd + Number(nextCallHighUsd) > ledger.hardCapUsd;
}

export function projectWaveCostFromSample(sampleActualUsd, sampleLogicalCalls, planned = 84) {
  if (!(sampleLogicalCalls > 0)) {
    return {
      averagePerCall: null,
      projected84: null,
      likelyHardCapBreach: false,
      method: "insufficient_sample",
    };
  }
  const averagePerCall = sampleActualUsd / sampleLogicalCalls;
  const projected84 = averagePerCall * planned;
  return {
    averagePerCall: Number(averagePerCall.toFixed(6)),
    projected84: Number(projected84.toFixed(4)),
    likelyHardCapBreach: projected84 > WAVE1_HARD_CAP_USD,
    method: "first_slot_average_times_84",
    hardCapUsd: WAVE1_HARD_CAP_USD,
    plannedLogicalCalls: planned,
    maxAttempts: WAVE1_RETRY_POLICY.maxTotalAttempts,
  };
}
