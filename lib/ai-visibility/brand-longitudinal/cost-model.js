/**
 * Brand longitudinal cost model — historic per-provider rates from baseline ledger.
 */

import { HISTORIC_PROVIDER_COST } from "../stability-policy.js";
import { buildMonthlyExecutionMatrix } from "./cohort-v1.js";

export const BRAND_LONGITUDINAL_COST_MODEL_VERSION = "brand_longitudinal_cost_model_v1";

export const MAX_INITIAL_LONGITUDINAL_AI_SPEND_USD = 75;

export const MONTHLY_COST_TARGET_USD = 75;

export const MONTHLY_COST_STOP_USD = 100;

/**
 * Per-provider effective costs from verified baseline ledger.
 */
export function getProviderEffectiveCosts() {
  return {
    openai: {
      historicUsdPerCall: HISTORIC_PROVIDER_COST.openai.historicUsdPerCall,
      conservativeUsdPerCall: HISTORIC_PROVIDER_COST.openai.conservativeUsdPerCall,
      sampleSize: HISTORIC_PROVIDER_COST.openai.sampleSize,
      source: HISTORIC_PROVIDER_COST.openai.source,
    },
    gemini: {
      historicUsdPerCall: HISTORIC_PROVIDER_COST.gemini.historicUsdPerCall,
      conservativeUsdPerCall: HISTORIC_PROVIDER_COST.gemini.conservativeUsdPerCall,
      sampleSize: HISTORIC_PROVIDER_COST.gemini.sampleSize,
      source: HISTORIC_PROVIDER_COST.gemini.source,
    },
    perplexity: {
      historicUsdPerCall: HISTORIC_PROVIDER_COST.perplexity.historicUsdPerCall,
      conservativeUsdPerCall: HISTORIC_PROVIDER_COST.perplexity.conservativeUsdPerCall,
      sampleSize: HISTORIC_PROVIDER_COST.perplexity.sampleSize,
      source: HISTORIC_PROVIDER_COST.perplexity.source,
    },
    claude: {
      historicUsdPerCall: HISTORIC_PROVIDER_COST.claude.historicUsdPerCall,
      conservativeUsdPerCall: HISTORIC_PROVIDER_COST.claude.conservativeUsdPerCall,
      sampleSize: HISTORIC_PROVIDER_COST.claude.sampleSize,
      source: HISTORIC_PROVIDER_COST.claude.source,
    },
  };
}

function sumCallCosts(rows, rateKey) {
  const rates = getProviderEffectiveCosts();
  let total = 0;
  const byProvider = {};
  for (const row of rows) {
    const p = String(row.provider || "").toLowerCase();
    const rate = rates[p]?.[rateKey] ?? 0;
    total += rate;
    byProvider[p] = (byProvider[p] || 0) + 1;
  }
  return { total: Number(total.toFixed(4)), byProvider, callCount: rows.length };
}

/**
 * Full cost model for monthly longitudinal cycle.
 */
export function buildLongitudinalCostModel(cohortConfig = null) {
  const matrix = buildMonthlyExecutionMatrix(cohortConfig);
  const core4 = matrix.rows.filter((r) =>
    ["openai", "gemini", "perplexity", "claude"].every((p) =>
      matrix.rows.some((x) => x.promptId === r.promptId && x.provider === p)
    )
  );
  const selective = matrix.rows.filter((r) => {
    const provs = matrix.rows.filter((x) => x.promptId === r.promptId).map((x) => x.provider);
    return provs.length === 2 && provs.includes("openai") && provs.includes("perplexity");
  });

  const historic = sumCallCosts(matrix.rows, "historicUsdPerCall");
  const conservative = sumCallCosts(matrix.rows, "conservativeUsdPerCall");

  const withinTarget = historic.total <= MONTHLY_COST_TARGET_USD;
  const withinStop = historic.total <= MONTHLY_COST_STOP_USD;

  return {
    version: BRAND_LONGITUDINAL_COST_MODEL_VERSION,
    cohortPrompts: matrix.promptCount,
    callsPerCycle: matrix.callCount,
    coreProviderCalls: core4.length,
    extendedProviderCalls: selective.length,
    providerCosts: getProviderEffectiveCosts(),
    historicExpectedCostUsd: historic.total,
    conservativeExpectedCostUsd: conservative.total,
    monthlyExpectedUsd: historic.total,
    quarterlyExpectedUsd: Number((historic.total * 3).toFixed(2)),
    annualizedExpectedUsd: Number((historic.total * 12).toFixed(2)),
    withinMonthlyTarget: withinTarget,
    withinMonthlyStop: withinStop,
    costGatePass: withinStop,
    maxInitialSpendUsd: MAX_INITIAL_LONGITUDINAL_AI_SPEND_USD,
    note: withinTarget
      ? "Monthly cohort within $75 target"
      : withinStop
        ? "Within $100 stop — review before activation"
        : "STOP — redesign sampling; exceeds $100 historic-effective expectation",
  };
}
