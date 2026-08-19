/**
 * Operator foundation cost model. Shared prompt × provider. Does not scale × operator.
 * Reuses Brand HISTORIC_PROVIDER_COST. No provider calls.
 */

import { HISTORIC_PROVIDER_COST } from "../stability-policy.js";
import { listOperatorPrompts, promptLibraryStats } from "./prompts.js";

export const OPERATOR_FOUNDATION_COST_VERSION = "operator_ai_foundation_cost_v1";
export const MAX_OPERATOR_FOUNDATION_PROVIDER_SPEND = 60;
export const CORE_PROVIDERS = Object.freeze(["openai", "gemini", "perplexity", "claude"]);
export const EXTENDED_PROVIDERS = Object.freeze(["openai", "perplexity"]);

function rate(provider, key) {
  return HISTORIC_PROVIDER_COST[provider]?.[key] ?? 0;
}

function costRows(rows, key) {
  let total = 0;
  const byProvider = { openai: 0, gemini: 0, perplexity: 0, claude: 0 };
  for (const row of rows) {
    total += rate(row.provider, key);
    byProvider[row.provider] += 1;
  }
  return { total: Number(total.toFixed(4)), byProvider, callCount: rows.length };
}

export function buildOperatorFoundationExecutionMatrix() {
  const rows = [];
  for (const prompt of listOperatorPrompts({ tier: "CORE" })) {
    for (const provider of CORE_PROVIDERS) {
      rows.push({ promptId: prompt.promptId, provider, tier: "CORE" });
    }
  }
  for (const prompt of listOperatorPrompts({ tier: "EXTENDED" })) {
    for (const provider of EXTENDED_PROVIDERS) {
      rows.push({ promptId: prompt.promptId, provider, tier: "EXTENDED" });
    }
  }
  return rows;
}

export function costOperatorFoundationWave() {
  const stats = promptLibraryStats();
  const rows = buildOperatorFoundationExecutionMatrix();
  const historic = costRows(rows, "historicUsdPerCall");
  const conservative = costRows(rows, "conservativeUsdPerCall");
  const gate =
    conservative.total <= MAX_OPERATOR_FOUNDATION_PROVIDER_SPEND ? "PASS" : "FAIL";

  return {
    version: OPERATOR_FOUNDATION_COST_VERSION,
    executionGrain: "PROMPT_PROVIDER",
    costScalesByOperator: false,
    marginalCostAddOperator: 0,
    perOperatorProviderExecution: 0,
    promptStats: stats,
    corePrompts: stats.core,
    extendedPrompts: stats.extended,
    coreProviderMatrix: [...CORE_PROVIDERS],
    extendedProviderMatrix: [...EXTENDED_PROVIDERS],
    openaiCalls: historic.byProvider.openai,
    geminiCalls: historic.byProvider.gemini,
    perplexityCalls: historic.byProvider.perplexity,
    claudeCalls: historic.byProvider.claude,
    totalCalls: historic.callCount,
    projectedHistoricCost: historic.total,
    projectedConservativeCost: conservative.total,
    hardCapUsd: MAX_OPERATOR_FOUNDATION_PROVIDER_SPEND,
    costGate: gate,
    actualCost: 0,
    providerCalls: 0,
    dataforseoCalls: 0,
    waveStatus: gate === "PASS" ? "NOT_RUN" : "BLOCKED_OVER_CAP",
    note:
      "Foundation costs the wave but does not execute it. Next phase: OPERATOR_PRESENCE_VALIDATION.",
  };
}
