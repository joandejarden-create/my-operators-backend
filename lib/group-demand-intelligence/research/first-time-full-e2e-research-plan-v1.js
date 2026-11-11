/**
 * First-time Full E2E research planning helpers (hotel-generic, zero-paid).
 * Builds multi-pass discovery strategies and corrective spend envelopes
 * without authorizing paid execution.
 */
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";

export const FIRST_TIME_FULL_E2E_RESEARCH_PLAN_V1 =
  "gdi-first-time-full-e2e-research-plan-v1";

export const EXA_MEDIUM_USD = 0.1;
export const EXA_HIGH_USD = 0.5;

/**
 * Multi-pass discovery strategies for first-time / corrective Full E2E.
 * Not hotel-specific — geographies/languages come from hotel context.
 */
export function buildFirstTimeDiscoveryPassPlan(ctx = {}) {
  const geos = Array.isArray(ctx.geographies) ? ctx.geographies : [];
  const languages = Array.isArray(ctx.languages) ? ctx.languages : ["en"];
  const hasFr = languages.some((l) => /^fr/i.test(l));
  const horizon = ctx.horizon || "18_months";

  const passes = [
    {
      passId: "pass_1_en_primary_corridor",
      language: "en",
      focus: "Primary commercial corridor + named demand mechanisms",
      geographies: geos.slice(0, 6),
      horizon,
      effort: "medium",
      quotedUsd: EXA_MEDIUM_USD,
    },
  ];
  if (hasFr) {
    passes.push({
      passId: "pass_2_fr_local_market",
      language: "fr",
      focus: "FR-first local organisers, chambers, associations, IO/NGO lodging language",
      geographies: geos,
      horizon,
      effort: "medium",
      quotedUsd: EXA_MEDIUM_USD,
    });
  }
  passes.push({
    passId: "pass_3_adjacent_access_corridor",
    language: hasFr ? "fr" : "en",
    focus: "Adjacent access markets / cross-border demand that still books the hotel corridor",
    geographies: geos,
    horizon,
    effort: "medium",
    quotedUsd: EXA_MEDIUM_USD,
    optional: true,
    stopIf: "NO_NET_NEW_AFTER_PASS_2_AND_BUDGET_TIGHT",
  });

  return {
    version: FIRST_TIME_FULL_E2E_RESEARCH_PLAN_V1,
    passes,
    requiredPasses: passes.filter((p) => !p.optional).length,
    note: "Planning only — does not execute provider calls.",
  };
}

export function quoteExaMedium() {
  return quoteBenchmarkCall({
    provider: "exa",
    operation: "agent_run",
    params: { effort: "medium" },
  });
}

export function quoteExaHigh() {
  return quoteBenchmarkCall({
    provider: "exa",
    operation: "agent_run",
    params: { effort: "high" },
  });
}

/**
 * Adaptive corrective envelope for incomplete first-time Full E2E.
 */
export function buildCorrectiveFinancialEnvelope(opts = {}) {
  const hardCeiling = Number(opts.hardCeilingUsd ?? 18);
  const estimated = Number(opts.estimatedSpendUsd ?? 9.4);
  const adaptiveReserve = Number(opts.adaptiveReserveUsd ?? 4);
  const perOpportunity = Number(opts.perOpportunityCeilingUsd ?? 0.6);
  const exaCeiling = Number(opts.exaCeilingUsd ?? hardCeiling);
  const apifyCeiling = Number(opts.apifyCeilingUsd ?? 0);
  const surfeCeiling = Number(opts.surfeCeilingUsd ?? 0);
  const firecrawlCeiling = Number(opts.firecrawlCeilingUsd ?? 0);

  return {
    version: FIRST_TIME_FULL_E2E_RESEARCH_PLAN_V1,
    contractId: opts.contractId || "GDI_V23_CORRECTIVE_DISCOVERY_COMPLETENESS_V1",
    pricingBasis: {
      exaMediumUsd: EXA_MEDIUM_USD,
      exaHighUsd: EXA_HIGH_USD,
      source: "benchmark-provider-pricing-v1 / Exa Agent fixed efforts",
    },
    estimatedSpendUsd: estimated,
    hardAggregateCeilingUsd: hardCeiling,
    perProviderCeilings: {
      exaUsd: exaCeiling,
      apifyUsd: apifyCeiling,
      surfeUsd: surfeCeiling,
      firecrawlUsd: firecrawlCeiling,
    },
    perOpportunityCeilingUsd: perOpportunity,
    adaptiveReserveUsd: adaptiveReserve,
    sharedLedgerId: opts.ledgerId || null,
    dispatchTimeEnforcement: true,
    priorRemainderNotAutoReusable: true,
    priorRemainderNote:
      opts.priorRemainderNote ||
      "Prior campaign unspent remainder is NOT automatically available. New founder GO required.",
    authorizationBoundary: {
      authorizedNow: [
        "zero_paid_repairs",
        "testing",
        "research_planning",
        "budget_preparation",
      ],
      requiresFounderGo: [
        "paid_exa",
        "paid_apify",
        "surfe",
        "firecrawl",
        "airtable_production_writes",
        "customer_facing_promotion",
        "external_publication",
      ],
    },
  };
}
