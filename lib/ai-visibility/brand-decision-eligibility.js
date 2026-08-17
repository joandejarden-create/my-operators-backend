/**
 * Brand decision eligibility (Phase 3A.7).
 * Deterministic config only — no LLM inference.
 * UNKNOWN ≠ NOT_ELIGIBLE.
 * Language does not change structural eligibility.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const DECISION_ELIGIBILITY_CONFIG_ID = "brand_decision_eligibility_v1";
/** Default expected config version; live version is always read from fixture. */
export const DECISION_ELIGIBILITY_VERSION = "1.3";

export const ELIGIBILITY = Object.freeze({
  ELIGIBLE: "ELIGIBLE",
  NOT_ELIGIBLE: "NOT_ELIGIBLE",
  UNKNOWN: "UNKNOWN",
});

/** Full territory list retained for continuity (includes legacy + deferred). */
export const SHOWCASE_DECISION_TERRITORIES = Object.freeze([
  "Conversion",
  "Collection / Soft Brand",
  "Lifestyle Positioning",
  "Upper-Upscale Positioning",
  "New Build",
  "Branded Residences",
  "Branded Residences / Mixed Use",
  "Soft-Brand Affiliation Flexibility",
  "Owner Economics / Flexibility",
]);

/** Wave-1 bilingual showcase intents only. */
export const ACTIVE_SHOWCASE_DECISION_TERRITORIES = Object.freeze([
  "Conversion",
  "Collection / Soft Brand",
  "Lifestyle Positioning",
  "Upper-Upscale Positioning",
  "Branded Residences",
  "Soft-Brand Affiliation Flexibility",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_PATH = path.join(
  __dirname,
  "..",
  "..",
  "fixtures",
  "ai-visibility",
  "brand-decision-eligibility-v1.json"
);

export function loadDecisionEligibilityConfig(filePath = DEFAULT_PATH) {
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return {
    ...raw,
    id: raw.id || DECISION_ELIGIBILITY_CONFIG_ID,
    version: String(raw.version || DECISION_ELIGIBILITY_VERSION),
  };
}

/**
 * @returns {{ state: string, source: string, reason: string, version: string } | { state: 'UNKNOWN', source: string, reason: string }}
 */
export function getBrandDecisionEligibility(brandId, decisionTerritory, config) {
  const cfg = config || loadDecisionEligibilityConfig();
  const territory = String(decisionTerritory || "").trim();
  const entry = (cfg.entries || []).find(
    (e) => e.brandId === brandId && e.decisionTerritory === territory
  );
  if (!entry) {
    return {
      state: ELIGIBILITY.UNKNOWN,
      source: "brand_decision_eligibility_v1",
      reason: "No governed eligibility entry for this brand × intent.",
      version: cfg.version,
      LANGUAGE_NEUTRAL: true,
    };
  }
  return {
    state: entry.eligibility,
    source: entry.source,
    reason: entry.reason,
    version: entry.version || cfg.version,
    LANGUAGE_NEUTRAL: true,
  };
}

export function listEligibilityForBrand(brandId, config) {
  const cfg = config || loadDecisionEligibilityConfig();
  return (cfg.entries || []).filter((e) => e.brandId === brandId);
}

export function listEligibilityByTerritory(decisionTerritory, config) {
  const cfg = config || loadDecisionEligibilityConfig();
  const territory = String(decisionTerritory || "").trim();
  const rows = (cfg.entries || []).filter((e) => e.decisionTerritory === territory);
  return {
    decisionTerritory: territory,
    ELIGIBLE: rows.filter((r) => r.eligibility === ELIGIBILITY.ELIGIBLE),
    NOT_ELIGIBLE: rows.filter((r) => r.eligibility === ELIGIBILITY.NOT_ELIGIBLE),
    UNKNOWN: rows.filter((r) => r.eligibility === ELIGIBILITY.UNKNOWN),
  };
}

export function summarizeIntentCompetitiveDensity(config, territories = null) {
  const cfg = config || loadDecisionEligibilityConfig();
  const brandIds = [...new Set((cfg.entries || []).map((e) => e.brandId))];
  const list =
    territories ||
    cfg.activeShowcaseTerritories ||
    ACTIVE_SHOWCASE_DECISION_TERRITORIES;
  const out = [];
  for (const territory of list) {
    const rows = (cfg.entries || []).filter((e) => e.decisionTerritory === territory);
    const eligible = rows.filter((r) => r.eligibility === ELIGIBILITY.ELIGIBLE);
    const notEligible = rows.filter((r) => r.eligibility === ELIGIBILITY.NOT_ELIGIBLE);
    const unknown = rows.filter((r) => r.eligibility === ELIGIBILITY.UNKNOWN);
    out.push({
      decisionTerritory: territory,
      TOTAL_COHORT: brandIds.length,
      ELIGIBLE: eligible.length,
      NOT_ELIGIBLE: notEligible.length,
      UNKNOWN: unknown.length,
      ELIGIBLE_BRANDS: eligible.map((e) => e.brandName || e.brandId),
      FLAG:
        eligible.length < 3
          ? "LOW_ELIGIBLE_DENSITY"
          : unknown.length > eligible.length
            ? "UNKNOWN_DOMINATED"
            : eligible.length >= brandIds.length - 1
              ? "BROAD_NEAR_UNIVERSAL"
              : null,
    });
  }
  return out;
}

/**
 * Structural eligibility is language-neutral.
 */
export function eligibilityIsLanguageNeutral() {
  return true;
}
