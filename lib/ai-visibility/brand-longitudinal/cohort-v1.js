/**
 * BRAND_LONGITUDINAL_COHORT_V1 — governed membership + provider policy.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const COHORT_ID = "BRAND_LONGITUDINAL_COHORT_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_COHORT_PATH = path.join(
  __dirname,
  "..",
  "..",
  "..",
  "fixtures",
  "ai-visibility",
  "brand-longitudinal-cohort-v1.json"
);

export function loadBrandLongitudinalCohortV1(filePath = DEFAULT_COHORT_PATH) {
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return {
    ...raw,
    cohortId: raw.cohortId || COHORT_ID,
  };
}

/**
 * Resolve provider list for a cohort member.
 */
export function resolveMemberProviders(member, cohortConfig) {
  const policyKey = member.providers || "CORE_4_PROVIDER";
  const policy = cohortConfig.providerPolicy || {};
  return policy[policyKey] || policy.CORE_4_PROVIDER || ["openai", "gemini", "perplexity", "claude"];
}

/**
 * Build execution matrix: prompt × provider rows for one measurement period.
 */
export function buildCohortExecutionMatrix(cohortConfig = null) {
  const cfg = cohortConfig || loadBrandLongitudinalCohortV1();
  const rows = [];
  const byTier = { CRITICAL: [], HIGH: [], STANDARD: [], EXPLORATORY: [] };

  for (const member of cfg.members || []) {
    const providers = resolveMemberProviders(member, cfg);
    byTier[member.tier] = byTier[member.tier] || [];
    byTier[member.tier].push(member.promptId);
    for (const provider of providers) {
      rows.push({
        promptId: member.promptId,
        tier: member.tier,
        provider,
        cadence: member.cadence || null,
        providersPolicy: member.providers,
      });
    }
  }

  return {
    cohortId: cfg.cohortId,
    cohortVersion: cfg.cohortVersion,
    promptCount: (cfg.members || []).length,
    callCount: rows.length,
    rows,
    byTier,
    observedIncluded: (cfg.members || [])
      .filter((m) => String(m.promptId || "").startsWith("p_obs_"))
      .map((m) => m.promptId),
    derivedIncluded: [],
    periodicObserved: (cfg.periodicObserved || []).map((p) => p.promptId),
    researchOnly: (cfg.researchOnly || []).map((p) => p.promptId),
  };
}

/**
 * Members scheduled for a monthly cycle (CRITICAL + HIGH only).
 */
export function buildMonthlyExecutionMatrix(cohortConfig = null) {
  const matrix = buildCohortExecutionMatrix(cohortConfig);
  const monthlyTiers = new Set(["CRITICAL", "HIGH"]);
  const rows = matrix.rows.filter((r) => monthlyTiers.has(r.tier));
  return {
    ...matrix,
    rows,
    callCount: rows.length,
    promptCount: new Set(rows.map((r) => r.promptId)).size,
    cadence: "monthly",
  };
}
