/**
 * ADP Lite research mode for free AI Demand Leak Audits.
 * Internal: adp_lite_leak_audit · External: Limited AI Demand Leak Audit
 *
 * Math: maxScenarios × maxProviders = maxObservations (default 15 × 4 = 60).
 * Never runs full paid ADP prompt library, history, or publish writers.
 */

export const INTERNAL_RESEARCH_MODE = "adp_lite_leak_audit";
export const EXTERNAL_PRODUCT_LABEL = "Limited AI Demand Leak Audit";

export const RESEARCH_MODES = Object.freeze({
  LEAK_AUDIT_LITE: "leak_audit_lite",
  ADP_PILOT_MONTHLY: "adp_pilot_monthly",
  ADP_FULL_PLATFORM: "adp_full_platform",
});

export const PROMPT_SET_VERSION_LITE = "leak_audit_lite_v1";
export const PROVIDER_SET_VERSION_LITE = "leak_audit_provider_set_v1";

export const LITE_PROVIDERS = Object.freeze([
  "openai",
  "gemini",
  "perplexity",
  "claude",
]);

/** Cost-control defaults for free single-property audits. */
export const LEAK_AUDIT_LITE_COST_CONTROLS = Object.freeze({
  researchMode: RESEARCH_MODES.LEAK_AUDIT_LITE,
  internalMode: INTERNAL_RESEARCH_MODE,
  maxHotels: 1,
  maxScenarios: 15,
  maxProviders: 4,
  maxObservations: 60,
  minProviders: 2,
  minDemandTerritories: 5,
  maxDemandTerritories: 6,
  maxCompetitorsShown: 2,
  maxEvidenceCards: 4,
  maxActionItems: 3,
  maxSourceLinksPerEvidence: 3,
  noFullSourceCrawl: true,
  noHistoricalTrend: true,
  noPaidAdpWrite: true,
  promptSetVersion: PROMPT_SET_VERSION_LITE,
  providerSetVersion: PROVIDER_SET_VERSION_LITE,
});

/** Backward-compatible alias used by runners. */
export const FREE_AUDIT_SCOPE_LITE = LEAK_AUDIT_LITE_COST_CONTROLS;

export function buildScenariosMonitoredLabel({
  scenariosRun = LEAK_AUDIT_LITE_COST_CONTROLS.maxScenarios,
  providersRun = LEAK_AUDIT_LITE_COST_CONTROLS.maxProviders,
} = {}) {
  const scenarios = Number(scenariosRun) || 0;
  const providers = Number(providersRun) || 0;
  const observations = scenarios * providers;
  return {
    title: "Scenarios Monitored",
    value: `${scenarios} × ${providers}`,
    valueSubLabel: `${observations} observations`,
    description: `${scenarios} traveler scenarios across ChatGPT, Gemini, Perplexity and Claude.`,
  };
}

export function buildCoverMetaLine({
  providers = LEAK_AUDIT_LITE_COST_CONTROLS.maxProviders,
  scenarios = LEAK_AUDIT_LITE_COST_CONTROLS.maxScenarios,
  observations = null,
  actionItems = LEAK_AUDIT_LITE_COST_CONTROLS.maxActionItems,
} = {}) {
  const obs =
    observations != null
      ? Number(observations)
      : Number(scenarios) * Number(providers);
  return `PROVIDERS ${providers} · SCENARIOS ${scenarios} · OBSERVATIONS ${obs} · ACTION ITEMS ${actionItems}`;
}

export function buildDiagnosticScopeLabel({
  scenarios = LEAK_AUDIT_LITE_COST_CONTROLS.maxScenarios,
  providers = LEAK_AUDIT_LITE_COST_CONTROLS.maxProviders,
} = {}) {
  return `Limited diagnostic · ${scenarios} traveler scenarios across ${providers} AI providers`;
}

export function assertLiteCostControls(controls = {}) {
  const c = { ...LEAK_AUDIT_LITE_COST_CONTROLS, ...controls };
  const failures = [];
  if (c.researchMode !== RESEARCH_MODES.LEAK_AUDIT_LITE) {
    failures.push("researchMode_must_be_leak_audit_lite");
  }
  if (c.maxScenarios > LEAK_AUDIT_LITE_COST_CONTROLS.maxScenarios) {
    failures.push("maxScenarios_exceeds_lite_cap");
  }
  if (c.maxProviders > LEAK_AUDIT_LITE_COST_CONTROLS.maxProviders) {
    failures.push("maxProviders_exceeds_lite_cap");
  }
  if (c.maxObservations > LEAK_AUDIT_LITE_COST_CONTROLS.maxObservations) {
    failures.push("maxObservations_exceeds_lite_cap");
  }
  if (c.maxScenarios * c.maxProviders > c.maxObservations) {
    failures.push("scenario_provider_product_exceeds_maxObservations");
  }
  if (c.noPaidAdpWrite !== true) failures.push("noPaidAdpWrite_required");
  if (c.noFullSourceCrawl !== true) failures.push("noFullSourceCrawl_required");
  if (c.noHistoricalTrend !== true) failures.push("noHistoricalTrend_required");
  return { ok: failures.length === 0, failures, controls: c };
}

/** Paid ADP writers that lite mode must never invoke. */
export const FORBIDDEN_PAID_ADP_WRITERS = Object.freeze([
  "savePeriod",
  "savePublishedSnapshotBundle",
  "persistAdpHistoricalPeriod",
  "ADP_AIRTABLE_PUBLISH_APPLY",
  "ADP_HISTORY_AIRTABLE_WRITE_APPLY",
  "publish-ai-demand-positioning-snapshot",
]);
