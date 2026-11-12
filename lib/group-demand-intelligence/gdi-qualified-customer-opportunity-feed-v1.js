/**
 * Qualified customer opportunity feed V1
 *
 * Local auth commercial list and published/share customer list must share the
 * same eligibility: commercially qualified + appropriately approved only.
 *
 * Research states (WATCHLIST, Research Continuation, Geography Blocked,
 * EXCLUDE, Duplicate, Incomplete, unapproved founder-review) stay in research /
 * admin surfaces — never the hotel opportunity feed.
 *
 * Does not publish. Does not invent maturity. Preserves approved SALES_READY /
 * ACCOUNT_DEVELOPMENT / REVIEWED_PACKAGE / published-registry rows.
 */

export const GDI_QUALIFIED_CUSTOMER_OPPORTUNITY_FEED_V1 =
  "gdi-qualified-customer-opportunity-feed-v1";

function upper(v) {
  return String(v == null ? "" : v).trim().toUpperCase();
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/** Approved commercial inventory that may remain on the customer feed. */
export function hasApprovedCustomerCommercialClass(opp = {}) {
  if (
    opp.waterstonePrimarySalesInventory === true &&
    opp.waterstoneInventoryLane === "PRIMARY_SALES_READY"
  ) {
    return true;
  }
  const inventoryClass = upper(opp.commercialInventoryClass);
  if (
    inventoryClass === "SALES_READY" ||
    inventoryClass === "ACCOUNT_DEVELOPMENT"
  ) {
    return true;
  }
  if (
    opp.gdiPublishedInventoryRegistry === true &&
    opp.customerVisible !== false &&
    upper(opp.publicationState || "PUBLISHED") !== "REVOKED"
  ) {
    return true;
  }
  if (upper(opp.prospectPublicationBasis) === "REVIEWED_PACKAGE_V1") {
    return true;
  }
  const maturity = upper(opp.commercialMaturity || opp.gdiMaturityState);
  if (maturity === "SALES_READY" || maturity === "ACCOUNT_DEVELOPMENT") {
    return true;
  }
  return false;
}

/**
 * True when the opportunity is research/incomplete and must not appear on the
 * hotel's main opportunity feed (local or published).
 */
export function isBlockedFromQualifiedCustomerOpportunityFeed(opp = {}) {
  if (opp == null || typeof opp !== "object") return true;

  // Explicit founder/customer suppressions
  if (
    opp.archived === true ||
    opp.isArchived === true ||
    opp.customerVisible === false ||
    opp.prospectSuppressed === true ||
    opp.campaignCustomerFacingDisabled === true ||
    opp.founderCustomerVisibilityDisable === true
  ) {
    return true;
  }

  // Local research / shadow / certification previews are not the hotel feed
  if (
    opp.certificationFixtureOnly === true ||
    opp.liveCampaignV1Shadow === true ||
    opp.stageEShadowPreview === true ||
    opp.localShadowOnly === true ||
    opp.localShadowPortfolio === true ||
    opp.bethesdaInternalResearchInventory === true ||
    opp.gdiInternalUnionOnly === true ||
    opp.publicationReviewReady === true
  ) {
    // Approved published commercial rows may carry residual stamps — allow those
    if (!hasApprovedCustomerCommercialClass(opp)) return true;
  }

  // publicationReviewReady alone stages for founder — not customer feed
  if (
    opp.publicationReviewReady === true &&
    opp.customerPublicationReady !== true &&
    !hasApprovedCustomerCommercialClass(opp)
  ) {
    return true;
  }

  if (hasApprovedCustomerCommercialClass(opp)) {
    return false;
  }

  const priority = upper(opp.priority);
  if (priority === "WATCHLIST" || priority === "WATCH" || priority === "EXCLUDE") {
    return true;
  }

  const maturityBlob = [
    opp.commercialMaturity,
    opp.gdiMaturityState,
    opp.historicalRecoveryDisposition,
    opp.eligibilityStatus,
    opp.accountDevelopmentStatus,
    opp.hotelGeographyFit,
    opp.commercialSurfaceDisposition,
  ]
    .map(upper)
    .join("|");

  if (
    /RESEARCH_CONTINUATION|GEOGRAPHY_BLOCKED|WEAK_OR_OUT_OF_MARKET|CONTESTED_METRO|EXCLUDE|DUPLICATE|INCOMPLETE|QUARANTINE|LINKED_DUPLICATE|MARKET_INTELLIGENCE|RESEARCH_PIPELINE/.test(
      maturityBlob
    )
  ) {
    return true;
  }

  if (
    opp.geographyBlocked === true ||
    opp.duplicateOfOpportunityId ||
    opp.listVisible === false
  ) {
    return true;
  }

  // Thin early-watch titles without approved maturity
  if (
    /early watch|future.?year watch|hotel block TBA|hotel not yet named/i.test(
      text(opp.title)
    ) &&
    (priority === "WATCHLIST" || priority === "WATCH" || !priority)
  ) {
    return true;
  }

  return false;
}

export function isQualifiedCustomerOpportunityFeedEligible(opp = {}, opts = {}) {
  if (opts.includeResearchInventory === true) return true;
  return !isBlockedFromQualifiedCustomerOpportunityFeed(opp);
}

export function filterQualifiedCustomerOpportunityFeed(
  opportunities = [],
  opts = {}
) {
  return (opportunities || []).filter((o) =>
    isQualifiedCustomerOpportunityFeedEligible(o, opts)
  );
}
