/**
 * Recurring monitoring configuration — cadence, caps, execution order (Phase 3B.6).
 */

import { WAVE1_EXECUTION_ORDER } from "./wave1-showcase-plan.js";

export const RECURRING_MONITORING_CONFIG_VERSION = "ai_visibility_recurring_monitoring_config_v1";

export const RECURRING_CADENCE = Object.freeze({
  FREQUENCY: "WEEKLY",
  INTERVAL: 2,
  LABEL: "biweekly",
  TIMEZONE: "UTC",
  SCHEDULER_ENABLED: false,
});

export const RECURRING_PROVIDER_EXECUTION_ORDER = Object.freeze([
  {
    provider: "perplexity",
    order: 1,
    WHY: "Lowest cost/latency; completes quickly; failure isolation before heavier providers",
  },
  {
    provider: "gemini",
    order: 2,
    WHY: "Moderate cost; grounding reliable; precedes OpenAI/Claude in compact window",
  },
  {
    provider: "openai",
    order: 3,
    WHY: "Established Wave-1 adapter; mid-high cost; before Claude bottleneck",
  },
  {
    provider: "claude",
    order: 4,
    WHY: "Highest cost/latency; web-search heavy; run last so cap issues do not block other providers",
  },
]);

export const COLLECTION_WINDOW = Object.freeze({
  TARGET: "< 12 hours",
  HARD_MAX: "24 hours",
  WHY: "Tighter than first baseline multi-day window; not statistical simultaneity",
});

/**
 * Derive recurring hard caps from baseline actuals + buffer.
 * @param {Record<string, number>} baselineCosts
 */
export function deriveRecurringHardCaps(baselineCosts = {}) {
  const openai = Number(baselineCosts.openai || 38.41);
  const gemini = Number(baselineCosts.gemini || 6.56);
  const perplexity = Number(baselineCosts.perplexity || 0.47);
  const claude = Number(baselineCosts.claude || 58.19);

  const caps = {
    openai: Number(Math.max(openai * 1.35 * 1.15, 15).toFixed(2)),
    gemini: Number(Math.max(gemini * 1.35 * 1.25, 8).toFixed(2)),
    perplexity: Number(Math.max(perplexity * 1.35 * 1.25, 1.5).toFixed(2)),
    claude: Number(Math.max(claude * 1.15, 45).toFixed(2)),
  };

  caps.TOTAL_PERIOD_CAP = Number(
    (caps.openai + caps.gemini + caps.perplexity + caps.claude).toFixed(2)
  );
  caps.WARNING_THRESHOLD = Number((caps.TOTAL_PERIOD_CAP * 0.85).toFixed(2));
  caps.BASIS =
    "baseline_actual_usd × operational buffer (provider-specific); not experimental validation caps";

  return caps;
}

/**
 * Recurring cost estimate from baseline actuals.
 */
export function estimateRecurringPeriodCost(baselineCosts = {}) {
  const openai = Number(baselineCosts.openai || 38.41);
  const gemini = Number(baselineCosts.gemini || 6.56);
  const perplexity = Number(baselineCosts.perplexity || 0.47);
  const claude = Number(baselineCosts.claude || 58.19);
  const total = openai + gemini + perplexity + claude;
  return {
    OPENAI: openai,
    GEMINI: gemini,
    PERPLEXITY: perplexity,
    CLAUDE: claude,
    TOTAL_BASELINE_COST: Number(total.toFixed(2)),
    LOW: Number((total * 0.9).toFixed(2)),
    EXPECTED: Number(total.toFixed(2)),
    HIGH: Number((total * 1.15).toFixed(2)),
  };
}

export const RECURRING_MATRIX = Object.freeze({
  providers: ["openai", "gemini", "perplexity", "claude"],
  callsPerProvider: 84,
  totalLogicalCalls: 336,
  slots: WAVE1_EXECUTION_ORDER.map((s) => ({ key: s.key, planned: 12 })),
  promptLibrary: "showcase_prompts_v1",
  peerSetId: "peers_uu_collection_lifestyle_owner_decision_v2",
  metricVersion: "ai_visibility_metrics_v1",
});

export function getRecurringMonitoringConfig(baselineCosts = {}) {
  return {
    version: RECURRING_MONITORING_CONFIG_VERSION,
    cadence: { ...RECURRING_CADENCE },
    executionOrder: [...RECURRING_PROVIDER_EXECUTION_ORDER],
    collectionWindow: { ...COLLECTION_WINDOW },
    matrix: { ...RECURRING_MATRIX },
    hardCaps: deriveRecurringHardCaps(baselineCosts),
    costEstimate: estimateRecurringPeriodCost(baselineCosts),
  };
}
