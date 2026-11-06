/**
 * GDI_FULL_E2E_RESEARCH_INVENTORY_POLICY_V1
 *
 * The 10–15 opportunity objective is a MINIMUM yield ambition — never a maximum.
 * Every discovered unique opportunity must receive investigation/disposition.
 * Prioritized shortlists are optional viewing layers only.
 *
 * No paid research. No publication. No Airtable writes.
 */

export const GDI_FULL_E2E_RESEARCH_INVENTORY_POLICY_V1 =
  "GDI_FULL_E2E_RESEARCH_INVENTORY_POLICY_V1";

export const FULL_E2E_INVENTORY_POLICY = Object.freeze({
  version: GDI_FULL_E2E_RESEARCH_INVENTORY_POLICY_V1,
  minimumYieldAmbition: 10,
  recommendedYieldAmbition: 15,
  maximumYield: null, // intentional — no maximum
  preserveAllQualifiedOpportunities: true,
  shortlistsAreViewLayersOnly: true,
  missingNeedDatesDoNotSuppressDiscovery: true,
  needDatePrioritiesInfluenceOrderingOnly: true,
  standards: Object.freeze([
    "Evidence-grounded hotel-specific demand thesis",
    "Named commercial buyer or credible commercial entry point",
    "Reliable inferred demand may qualify",
    "Immediate RFP not required",
    "Confirmed room nights not required",
    "No fabricated buyers",
    "No unrelated contacts",
    "No automatic promotion from desk labels or organization names alone",
    "No promotion based solely on a named contact",
  ]),
});

export function assertFullInventoryPolicy(inventory = {}, opts = {}) {
  const opportunities = Array.isArray(inventory)
    ? inventory
    : inventory.opportunities || inventory.rows || [];
  const qualified = opportunities.filter((o) => {
    const s = String(o.disposition || o.qualificationStatus || o.status || "").toUpperCase();
    return (
      s.includes("SALES_READY") ||
      s.includes("ACCOUNT_DEVELOPMENT") ||
      s.includes("QUALIFIED") ||
      o.prospectHotelFacing === true
    );
  });

  const violations = [];
  if (opts.discardedBecauseOverCap === true) {
    violations.push("DISCARD_BECAUSE_OVER_CAP_PROHIBITED");
  }
  if (opts.forcedShortlistAsInventory === true) {
    violations.push("SHORTLIST_MUST_NOT_REPLACE_INVENTORY");
  }
  if (
    opts.needDatesMissing === true &&
    opts.suppressedDiscoveryBecauseNoNeedDates === true
  ) {
    violations.push("MISSING_NEED_DATES_MUST_NOT_SUPPRESS_DISCOVERY");
  }

  return {
    version: GDI_FULL_E2E_RESEARCH_INVENTORY_POLICY_V1,
    ok: violations.length === 0,
    opportunityCount: opportunities.length,
    qualifiedCount: qualified.length,
    minimumYieldAmbition: FULL_E2E_INVENTORY_POLICY.minimumYieldAmbition,
    meetsMinimumAmbition:
      opportunities.length >= FULL_E2E_INVENTORY_POLICY.minimumYieldAmbition,
    maximumYield: null,
    violations,
    policy: FULL_E2E_INVENTORY_POLICY,
  };
}

export function needDateResearchOrderingHint(needDateConfig = {}) {
  const status = String(needDateConfig.status || "NOT_ESTABLISHED");
  return {
    preferConfirmedPeriods: status === "HOTEL_CONFIRMED",
    useEstimatesAsHypothesesOnly: status === "MARKET_ESTIMATED",
    unrestrictedDiscovery: true,
    missingNeedDatesDoNotBlock: status === "NOT_ESTABLISHED",
    note:
      "Need-date priorities influence research ordering only; they never restrict the full opportunity universe.",
  };
}
