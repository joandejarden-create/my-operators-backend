/**
 * Discoverability snapshot foundation (Phase 3C.1).
 * Longitudinal storage contract — no alerts yet.
 */

export const DISCOVERABILITY_SNAPSHOT_VERSION = "ai_visibility_discoverability_snapshot_v1";

export function buildDiscoverabilitySnapshot(input = {}) {
  return {
    version: DISCOVERABILITY_SNAPSHOT_VERSION,
    snapshotId: input.snapshotId || `disc_snap_${Date.now()}`,
    brandId: input.brandId || null,
    checkedAt: input.checkedAt || new Date().toISOString(),
    domain: input.domain || null,
    pageUrl: input.pageUrl || null,
    pageType: input.pageType || null,
    status: input.status || null,
    robots: input.robots || null,
    canonical: input.canonical || null,
    indexability: input.indexability || null,
    crawlerAccess: input.crawlerAccess || null,
    contentInInitialHtml: input.contentInInitialHtml || null,
    developmentContent: input.developmentContent || null,
    dataState: input.dataState || "MEASURABLE_PUBLICLY",
    evidence: input.evidence || [],
  };
}

export const SNAPSHOT_FOUNDATION_READY = true;

export const FUTURE_LONGITUDINAL_SIGNALS = Object.freeze([
  "newly_blocked",
  "newly_crawlable",
  "page_disappeared",
  "canonical_changed",
]);

export const REVIEW_ITEM_FOUNDATION = Object.freeze({
  READY: true,
  FUTURE_RULES: [
    "priority_development_page_blocked",
    "crawler_denied",
    "development_page_missing",
    "ai_referrals_detected_after_previously_none",
    "qualified_development_actions_observed",
  ],
  ARBITRARY_PRIORITY_SCORE: false,
});
