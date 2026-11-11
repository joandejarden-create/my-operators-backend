/**
 * ADP_PORTFOLIO_ELIGIBILITY_V1
 *
 * Canonical ADP portfolio eligibility — separate from paid-measurement authorization
 * and from production publication authorization.
 *
 * Founder policy (binding): all 31 staged ADP hotels are eligible, including
 * Bethesda Marriott (adp_bethesda_marriott / recLuxvwwxID7U2B8).
 *
 * Stale "Bethesda ADP remains excluded" research exclusions are retired.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const ADP_PORTFOLIO_ELIGIBILITY_V1 = "ADP_PORTFOLIO_ELIGIBILITY_V1";

/** Binding portfolio counts */
export const ADP_PORTFOLIO_TOTAL = 31;
export const ADP_ELIGIBLE_TOTAL = 31;
export const BETHESDA_ADP_ELIGIBLE = true;

export const BETHESDA_ADP_PROPERTY_ID = "adp_bethesda_marriott";
export const BETHESDA_HPC_ID = "recLuxvwwxID7U2B8";
export const BETHESDA_PUBLISHED_PERIOD_ID =
  "adp_period_adp_bethesda_marriott_20261002142154_62428a";

/**
 * Explicit research exclusions — MUST remain empty under current founder policy.
 * Historical Bethesda exclusion retired 2026-10-20.
 */
export const ADP_RESEARCH_EXCLUDED_PROPERTY_IDS = Object.freeze(new Set([]));

export function isAdpResearchExcluded(propertyId) {
  return ADP_RESEARCH_EXCLUDED_PROPERTY_IDS.has(String(propertyId || "").trim());
}

export function isAdpPortfolioEligible(propertyId) {
  const id = String(propertyId || "").trim();
  if (!id.startsWith("adp_")) return false;
  if (isAdpResearchExcluded(id)) return false;
  return true;
}

export function assertBethesdaAdpEligible() {
  return (
    BETHESDA_ADP_ELIGIBLE === true &&
    isAdpPortfolioEligible(BETHESDA_ADP_PROPERTY_ID) &&
    !isAdpResearchExcluded(BETHESDA_ADP_PROPERTY_ID)
  );
}

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

/**
 * List canonical 31 ADP property ids from staged store (authoritative universe).
 */
export function listCanonicalAdpPortfolioPropertyIds(cwd = ROOT) {
  const staged = path.join(cwd, "data/ai-demand-positioning/staged");
  if (!fs.existsSync(staged)) return [];
  return fs
    .readdirSync(staged)
    .filter((d) => d.startsWith("adp_"))
    .sort();
}

export function certifyAdpPortfolioEligibilityCounts(cwd = ROOT) {
  const ids = listCanonicalAdpPortfolioPropertyIds(cwd);
  const eligible = ids.filter((id) => isAdpPortfolioEligible(id));
  const failures = [];
  if (ids.length !== ADP_PORTFOLIO_TOTAL) {
    failures.push(`PORTFOLIO_COUNT_${ids.length}_EXPECTED_${ADP_PORTFOLIO_TOTAL}`);
  }
  if (eligible.length !== ADP_ELIGIBLE_TOTAL) {
    failures.push(`ELIGIBLE_COUNT_${eligible.length}_EXPECTED_${ADP_ELIGIBLE_TOTAL}`);
  }
  if (!ids.includes(BETHESDA_ADP_PROPERTY_ID)) {
    failures.push("BETHESDA_MISSING_FROM_STAGED_UNIVERSE");
  }
  if (!assertBethesdaAdpEligible()) {
    failures.push("BETHESDA_NOT_ELIGIBLE");
  }
  if (isAdpResearchExcluded(BETHESDA_ADP_PROPERTY_ID)) {
    failures.push("BETHESDA_STILL_IN_RESEARCH_EXCLUSIONS");
  }
  return {
    contract: ADP_PORTFOLIO_ELIGIBILITY_V1,
    portfolioTotal: ids.length,
    eligibleTotal: eligible.length,
    bethesdaEligible: assertBethesdaAdpEligible(),
    propertyIds: ids,
    pass: failures.length === 0,
    failures,
  };
}
