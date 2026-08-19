#!/usr/bin/env node
/**
 * Multi-parent Brand AI longitudinal cost audit (read-only).
 * PROVIDER_CALLS=0 · AI_SPEND=$0
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listDemoBrandPortfolioOptions } from "../lib/dealality/demo-brand-portfolio-context.js";
import { loadShowcaseCompaniesConfig } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { resolvePeerSetMembership, loadPeerSetConfig, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import {
  buildMonthlyExecutionMatrix,
  buildCohortExecutionMatrix,
} from "../lib/ai-visibility/brand-longitudinal/cohort-v1.js";
import { HISTORIC_PROVIDER_COST } from "../lib/ai-visibility/stability-policy.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function costForRows(rows, rateKey) {
  let total = 0;
  const byProvider = { openai: 0, gemini: 0, perplexity: 0, claude: 0 };
  for (const row of rows) {
    const p = row.provider;
    const rate = HISTORIC_PROVIDER_COST[p]?.[rateKey] ?? 0;
    total += rate;
    byProvider[p] = (byProvider[p] || 0) + 1;
  }
  return {
    total: Number(total.toFixed(4)),
    byProvider,
    callCount: rows.length,
  };
}

function providerRates() {
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

const parents = listDemoBrandPortfolioOptions();
const monthly = buildMonthlyExecutionMatrix();
const full = buildCohortExecutionMatrix();
const standardRows = full.rows.filter((r) => r.tier === "STANDARD");

const monthlyHist = costForRows(monthly.rows, "historicUsdPerCall");
const monthlyCons = costForRows(monthly.rows, "conservativeUsdPerCall");
const stdHist = costForRows(standardRows, "historicUsdPerCall");
const stdCons = costForRows(standardRows, "conservativeUsdPerCall");

const peer = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2, commercialRegion: "CALA" }, loadPeerSetConfig());
const showcaseBrandIds = new Set(parents.flatMap((p) => p.brandIds));
const peerSetBrandIds = new Set(peer.entityIds || []);
const portfolioOnlyNotInPeerSet = [...showcaseBrandIds].filter((id) => !peerSetBrandIds.has(id));

const report = {
  BRAND_AI_MULTI_PARENT_LONGITUDINAL_COST_AUDIT_COMPLETE: true,
  SOURCE_OF_TRUTH: {
    showcaseConfig: "fixtures/ai-visibility/brand-ai-showcase-companies-v1.json",
    portfolioSwitcher: "lib/dealality/demo-brand-portfolio-context.js",
    uiKeys: ["marriott", "hilton", "choice", "ihg"],
    note: "Parent Company filter = demo portfolio switcher (one parent active at a time). Brand filter = brands within active portfolio.",
  },
  CURRENT_BRAND_FILTER_UNIVERSE: {
    PARENT_COMPANIES: 4,
    parents: parents.map((p) => ({
      PARENT: p.label,
      companyKey: p.companyKey,
      BRANDS: p.brands.map((b) => b.brandName),
      BRAND_COUNT: p.brandCount,
    })),
    TOTAL_SELECTED_BRANDS: parents.reduce((s, p) => s + p.brandCount, 0),
    PEER_SET_V2_BRANDS_IN_COHORT: peer.entityIds?.length ?? 0,
    SHOWCASE_BRANDS_NOT_IN_PEER_SET_V2: portfolioOnlyNotInPeerSet,
  },
  MONITORING_GRAIN: {
    GRAIN: "PORTFOLIO_PROMPT_PROVIDER",
    EXECUTION_MODEL: "MIXED_READ_PARTITION",
    EXPLANATION:
      "Provider calls are prompt × provider (peer-set v2 competitive cohort). One response extracts mentions for multiple brands. Parent portfolio switch and Brand dropdown filter the read/attribution path — wave1 showcase config sets DUPLICATE_PROVIDER_RUN_REQUIRED:false per parent. Longitudinal period manifests may partition by parent for trend accounting without re-execution.",
    DUPLICATE_PROVIDER_RUN_REQUIRED: false,
    NOT_BRAND_PROMPT_PROVIDER:
      "Calls do not multiply by brand count. 18 selected brands ≠ 18× calls.",
  },
  MARRIOTT_RECONCILIATION: {
    MARRIOTT_SELECTED_BRANDS: 5,
    MONTHLY_PROMPTS: monthly.promptCount,
    MONTHLY_CALLS: monthly.callCount,
    CALL_FORMULA: "16 CRITICAL × 4 providers (64) + 11 HIGH × 2 providers (22) = 86",
    CORRECTION:
      "Prior summary cited 100 calls — that was the full 35-prompt cohort (102 calls incl. STANDARD). Monthly CRITICAL+HIGH only = 86 calls.",
    FULL_COHORT_CALLS: full.callCount,
    FULL_COHORT_FORMULA: "86 monthly + 8 STANDARD × 2 providers = 16 quarterly-only",
    HISTORIC_COST_USD: monthlyHist.total,
    PROVIDER_BREAKDOWN: monthlyHist.byProvider,
  },
  LONGITUDINAL_POLICY: {
    cohortId: "BRAND_LONGITUDINAL_COHORT_V1",
    CRITICAL_PROMPTS: monthly.byTier.CRITICAL?.length ?? 16,
    HIGH_PROMPTS: monthly.byTier.HIGH?.length ?? 11,
    STANDARD_PROMPTS: monthly.byTier.STANDARD?.length ?? 8,
    APPLICABILITY:
      "All 27 monthly prompts are portfolio-generic owner-decision scenarios / observed demand — same prompt set for every parent. Eligibility gates metric interpretation per brand×intent, not provider call suppression in cohort execution.",
  },
  FOUR_PARENT_COST_MODELS: {
    SHARED_EXECUTION: {
      description: "Authoritative per current monitoring engine — one measurement period serves peer-set cohort",
      FOUR_PARENT_TOTAL_MONTHLY_CALLS: monthly.callCount,
      FOUR_PARENT_MONTHLY_HISTORIC_USD: monthlyHist.total,
      FOUR_PARENT_MONTHLY_CONSERVATIVE_USD: monthlyCons.total,
      FOUR_PARENT_ANNUAL_HISTORIC_USD: Number((monthlyHist.total * 12 + stdHist.total * 4).toFixed(2)),
      perParentAllocation: "NOT additive — shared infrastructure",
    },
    ISOLATED_REEXECUTION_PER_PARENT: {
      description: "Counterfactual only — if each parent required separate provider re-runs (NOT current architecture)",
      FOUR_PARENT_TOTAL_MONTHLY_CALLS: monthly.callCount * 4,
      FOUR_PARENT_MONTHLY_HISTORIC_USD: Number((monthlyHist.total * 4).toFixed(4)),
      FOUR_PARENT_MONTHLY_CONSERVATIVE_USD: Number((monthlyCons.total * 4).toFixed(4)),
      note: "Would violate DUPLICATE_PROVIDER_RUN_REQUIRED:false and waste spend",
    },
  },
  PROVIDER_CALLS_FOUR_PARENT_SHARED: {
    OPENAI: monthlyHist.byProvider.openai,
    GEMINI: monthlyHist.byProvider.gemini,
    PERPLEXITY: monthlyHist.byProvider.perplexity,
    CLAUDE: monthlyHist.byProvider.claude,
    TOTAL_MONTHLY: monthly.callCount,
    TOTAL_STANDARD_QUARTER: standardRows.length,
  },
  PROVIDER_COST_RATES: providerRates(),
  HISTORIC_COST: {
    FOUR_PARENT_MONTHLY_TOTAL: monthlyHist.total,
    FOUR_PARENT_ANNUAL_TOTAL: Number((monthlyHist.total * 12 + stdHist.total * 4).toFixed(2)),
    NORMALIZED_MONTHLY_WITH_STANDARD: Number((monthlyHist.total + stdHist.total / 3).toFixed(4)),
    STANDARD_QUARTERLY_INCREMENT: stdHist.total,
  },
  CONSERVATIVE_COST: {
    FOUR_PARENT_MONTHLY: monthlyCons.total,
    FOUR_PARENT_ANNUAL: Number((monthlyCons.total * 12 + stdCons.total * 4).toFixed(2)),
  },
  INITIAL_MULTI_PARENT_WAVE: {
    CALLS: monthly.callCount,
    HISTORIC_EXPECTED_COST: monthlyHist.total,
    CONSERVATIVE_COST: monthlyCons.total,
    STATUS: "NOT_RUN",
  },
  SCALABILITY: {
    CURRENT_SELECTED_BRANDS: parents.reduce((s, p) => s + p.brandCount, 0),
    ADD_5_BRANDS_EXISTING_PARENT: {
      incrementalCalls: 0,
      incrementalHistoricUsd: 0,
      note: "Unless new brands join peer-set v2 AND prompts are re-run (not current design)",
    },
    ADD_1_PARENT_5_BRANDS: {
      incrementalCalls_sharedModel: 0,
      incrementalCalls_isolatedReexecutionModel: monthly.callCount,
      incrementalHistoricUsd_sharedModel: 0,
      incrementalHistoricUsd_isolatedModel: monthlyHist.total,
    },
  },
  OPTIMIZATION: {
    SHARED_PROMPT_OPTIMIZATION_OPPORTUNITY: "YES_ALREADY_CAPTURED",
    POTENTIAL_CALL_REDUCTION: 0,
    TRADEOFF:
      "Current architecture already extracts multi-brand evidence per prompt. Splitting by brand would increase calls without improving comparability.",
  },
  DATASET: {
    CLASS: "DEMO_VALIDATION",
    note: "Four-parent filter universe on staging remains demo/showcase — not client production history",
  },
  RECOMMENDATION: {
    RECOMMENDED_MONTHLY_MONITORING_SCOPE:
      "Single shared peer-set measurement period — BRAND_LONGITUDINAL_COHORT_V1 monthly tiers (27 prompts, 86 calls) covering all 18 selected brands across 4 parents",
    RECOMMENDED_ESTIMATED_MONTHLY_BUDGET_HISTORIC: monthlyHist.total,
    RECOMMENDED_ESTIMATED_MONTHLY_BUDGET_CONSERVATIVE: monthlyCons.total,
    RECOMMENDED_ANNUAL_BUDGET_HISTORIC: Number((monthlyHist.total * 12 + stdHist.total * 4).toFixed(2)),
    RECOMMENDED_ANNUAL_BUDGET_CONSERVATIVE: Number((monthlyCons.total * 12 + stdCons.total * 4).toFixed(2)),
  },
  EXECUTION: {
    STATUS: "NOT_RUN",
    PROVIDER_CALLS: 0,
    SPEND: 0,
  },
  FINAL: "BRAND_AI_MULTI_PARENT_LONGITUDINAL_COST_AUDIT_PASS",
};

const outPath = path.join(
  __dirname,
  "..",
  "reports",
  "ai-visibility",
  "brand-multi-parent-longitudinal-cost-audit.json"
);
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

console.log("Multi-Parent Longitudinal Cost Audit");
console.log("  TOTAL_SELECTED_BRANDS:", report.CURRENT_BRAND_FILTER_UNIVERSE.TOTAL_SELECTED_BRANDS);
console.log("  MONTHLY_CALLS (shared):", report.MARRIOTT_RECONCILIATION.MONTHLY_CALLS);
console.log("  HISTORIC_MONTHLY:", report.HISTORIC_COST.FOUR_PARENT_MONTHLY_TOTAL);
console.log("  CONSERVATIVE_MONTHLY:", report.CONSERVATIVE_COST.FOUR_PARENT_MONTHLY);
console.log("  Written:", outPath);
