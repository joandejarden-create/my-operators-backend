/**
 * Customer-facing GDI opportunity visibility.
 * Excludes synthetic test/fixture/sample rows AND inactive/invalid
 * customer-surface rows (past, chrome, promotions, failed promotion gate).
 *
 * Convergence V1: live list = surface-eligible AND
 *   (strict-ready OR temporary legacyVisibilityPreserved cohort).
 * New opportunities must pass isGdiCustomerOpportunityReady — no legacy stamp on create.
 */

import { isCustomerSurfaceActiveEligible } from "./customer-surface-revalidation-v1.js";
import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import { isLegacyVisibilityCompatible } from "./legacy-visibility-compatibility-v1.js";

const TEST_ID_RE =
  /_test_|fixture|sample|link_only|canary_promo|schema_repair|gdi_pe_test|v13_test|temp_validation/i;

const SYNTHETIC_TITLE_RE =
  /\bpe link test\b|\[test\]|\[schema repair\]|(?:^|:\s*)\S+\s+(?:GARDEN|WEDDING(?:\s+VENUE)?|COUNTRY(?:\s+CLUB)?|BANQUET(?:\s+HALL)?|EVENT(?:\s+VENUE)?|MUSEUM|RELIGIOUS|HISTORIC|PRIVATE|WINERY)\s+\d+/i;

/**
 * True when the opportunity must never appear on customer auth/share surfaces.
 */
export function isGdiTestOrFixtureOpportunity(opp = {}) {
  if (opp == null || typeof opp !== "object") return false;
  if (opp.isTestData === true) return true;
  if (opp.customerVisible === false) return true;
  const id = String(opp.id || opp.opportunityId || "");
  if (TEST_ID_RE.test(id)) return true;
  const title = String(opp.title || opp.opportunityName || "");
  const org = String(opp.organizationName || "");
  if (SYNTHETIC_TITLE_RE.test(title) || SYNTHETIC_TITLE_RE.test(org)) return true;
  if (opp.peVenueId === "pev_test" || /^pev_test/i.test(String(opp.peVenueId || ""))) {
    return true;
  }
  const sources = Array.isArray(opp.sources)
    ? opp.sources.map((s) => s?.url || "").join(" ")
    : "";
  if (/\.example\b/i.test(sources)) return true;
  return false;
}

/**
 * Active customer surface eligibility (excludes test + past + invalid entity + DQ).
 * Does NOT apply the readiness gate — use isCustomerFacingOpportunity for list visibility.
 * Pass { includeHistorical: true } only for explicit history endpoints.
 */
export function isActiveCustomerOpportunity(opp = {}, opts = {}) {
  if (isGdiTestOrFixtureOpportunity(opp)) return false;
  if (opts.includeHistorical === true) {
    // Historical view may include past/closed; still never test rows
    return true;
  }
  return isCustomerSurfaceActiveEligible(opp, opts);
}

/**
 * Canonical customer-list visibility (readiness convergence V1).
 *
 * visible ⇔ surface-active AND (strict-ready OR temporary legacy compatibility)
 *
 * opts.surfaceOnly=true — audit/freeze escape hatch (pre-convergence surface definition)
 */
export function isCustomerFacingOpportunity(opp = {}, opts = {}) {
  if (!isActiveCustomerOpportunity(opp, opts)) return false;
  if (opts.surfaceOnly === true) return true;
  if (opts.includeHistorical === true) return true;
  if (isGdiCustomerOpportunityReady(opp, opts).ok) return true;
  if (isLegacyVisibilityCompatible(opp)) return true;
  return false;
}

export function filterCustomerFacingOpportunities(opportunities = [], opts = {}) {
  return (opportunities || []).filter((o) => isCustomerFacingOpportunity(o, opts));
}

/**
 * Mark synthetic validation writes so they cannot leak to customers.
 */
export function markAsTestOpportunity(opp = {}) {
  return {
    ...opp,
    isTestData: true,
    customerVisible: false,
  };
}
