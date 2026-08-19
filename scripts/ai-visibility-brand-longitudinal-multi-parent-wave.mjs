#!/usr/bin/env node
/**
 * Initial multi-parent Brand AI longitudinal wave.
 * Default: --preflight (no provider calls).
 * Paid execution: --execute after preflight PASS.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildMultiParentWavePreflight,
  executeMultiParentLongitudinalWave,
  MULTI_PARENT_HARD_CAP_USD,
  PLANNED_PROVIDER_CALLS_EXPECTED,
} from "../lib/ai-visibility/brand-longitudinal/multi-parent-wave-orchestrator.js";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = path.join(root, "reports", "ai-visibility");
const preflightPath = path.join(outDir, "brand-longitudinal-multi-parent-wave-preflight.json");
const reportPath = path.join(outDir, "brand-longitudinal-multi-parent-wave.json");

const args = process.argv.slice(2);
const execute = args.includes("--execute");
const preflightOnly = args.includes("--preflight") || !execute;

fs.mkdirSync(outDir, { recursive: true });

process.env.AI_VISIBILITY_ENABLED = process.env.AI_VISIBILITY_ENABLED || "true";
if (execute) {
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS =
    process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || "300000";
  if (!process.env.OPENAI_API_KEY && process.env.FDD_INTELLIGENCE_MODEL_API_KEY) {
    process.env.OPENAI_API_KEY = process.env.FDD_INTELLIGENCE_MODEL_API_KEY;
  }
}

const preflight = await buildMultiParentWavePreflight({
  requireCredentials: execute,
});

const sanitizedPreflight = {
  ...preflight,
  configs: (preflight.configs || []).map((c) => ({
    promptId: c.promptId,
    language: c.language,
    geographyKey: c.geographyKey,
    tier: c.tier,
    promptOrigin: c.promptOrigin,
  })),
  monthly: {
    promptCount: preflight.monthly?.promptCount,
    callCount: preflight.monthly?.callCount,
  },
};
fs.writeFileSync(preflightPath, JSON.stringify(sanitizedPreflight, null, 2));

console.log(
  JSON.stringify(
    {
      mode: execute ? "execute" : "preflight",
      RADISSON_MEASUREMENT_ELIGIBLE: preflight.radisson?.RADISSON_MEASUREMENT_ELIGIBLE,
      RADISSON_DROPDOWN_PRESENT: preflight.radisson?.RADISSON_DROPDOWN_PRESENT,
      RADISSON_ALIAS_COLLISION: preflight.radisson?.RADISSON_ALIAS_COLLISION,
      TOTAL_SELECTED_BRANDS: preflight.universe?.TOTAL_SELECTED_BRANDS,
      PLANNED_CALLS: preflight.PLANNED_CALLS,
      PROJECTED_HISTORIC_COST: preflight.PROJECTED_HISTORIC_COST,
      PROJECTED_CONSERVATIVE_COST: preflight.PROJECTED_CONSERVATIVE_COST,
      COST_GATE: preflight.COST_GATE,
      DUPLICATE_LOCK: preflight.DUPLICATE_LOCK,
      failReasons: preflight.failReasons,
      HARD_CAP: MULTI_PARENT_HARD_CAP_USD,
      EXPECTED_CALLS: PLANNED_PROVIDER_CALLS_EXPECTED,
      PROVIDER_CALLS: 0,
      preflightPath,
    },
    null,
    2
  )
);

if (preflightOnly && !execute) {
  process.exit(preflight.blocked ? 1 : 0);
}

if (preflight.blocked) {
  console.error("PREFLIGHT_BLOCKED", preflight.failReasons);
  process.exit(1);
}

const result = await executeMultiParentLongitudinalWave({ preflight });
const report = {
  BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_COMPLETE: true,
  FINAL: result.FINAL,
  STATUS: result.STATUS,
  periodId: result.periodId,
  measurementDate: result.measurementDate,
  qualityState: result.qualityState,
  universe: {
    PARENT_COMPANIES: preflight.universe.TOTAL_PARENT_COMPANIES,
    SELECTED_BRANDS: preflight.universe.TOTAL_SELECTED_BRANDS,
    parents: preflight.universe.parents,
  },
  preflight: sanitizedPreflight,
  execution: {
    OPENAI_CALLS: result.stats?.byProvider?.openai ?? 0,
    GEMINI_CALLS: result.stats?.byProvider?.gemini ?? 0,
    PERPLEXITY_CALLS: result.stats?.byProvider?.perplexity ?? 0,
    CLAUDE_CALLS: result.stats?.byProvider?.claude ?? 0,
    TOTAL_CALLS: result.stats?.ATTEMPTED ?? 0,
    SUCCESSFUL_CALLS: result.stats?.SUCCEEDED ?? 0,
    FAILED_CALLS: result.stats?.FAILED ?? 0,
    RETRIES: result.stats?.RETRIES ?? 0,
  },
  actualCost: {
    OPENAI: result.ledger?.byProvider?.openai?.actualUsd ?? 0,
    GEMINI: result.ledger?.byProvider?.gemini?.actualUsd ?? 0,
    PERPLEXITY: result.ledger?.byProvider?.perplexity?.actualUsd ?? 0,
    CLAUDE: result.ledger?.byProvider?.claude?.actualUsd ?? 0,
    TOTAL: result.ledger?.actualUsd ?? 0,
    MAX_ALLOWED: MULTI_PARENT_HARD_CAP_USD,
  },
  coverage: result.coverage,
  brandCompare: result.brandCompare,
  radisson: result.radisson,
  longitudinal: result.longitudinal,
  storage: result.storage,
  NEXT_RECOMMENDED_MEASUREMENT_DATE: result.NEXT_RECOMMENDED_MEASUREMENT_DATE,
  SCHEDULER_ENABLE: 0,
  NEXT_PHASE: "OPERATOR_AI_INTELLIGENCE_FOUNDATION",
  EXECUTION_STATUS: result.status || result.STATUS,
  PROVIDER_CALLS: result.PROVIDER_CALLS,
  SPEND: result.SPEND,
  SECRET_EXPOSURE: "NONE",
};
fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
console.log("Wrote", reportPath);
console.log("FINAL", result.FINAL);
process.exit(result.FINAL === "BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_PASS" ? 0 : 1);
