/**
 * Discoverability dimensions, metrics, and priority page model (Phase 3C.1).
 */

export const DISCOVERABILITY_DIMENSIONS_VERSION =
  "ai_visibility_discoverability_dimensions_v1";

export const DISCOVERABILITY_DIMENSIONS_V1 = Object.freeze([
  {
    id: "robots_ai_crawler_access",
    label: "Robots / AI crawler access",
    dataStateDefault: "MEASURABLE_PUBLICLY",
  },
  {
    id: "sitemap_availability",
    label: "Sitemap availability",
    dataStateDefault: "MEASURABLE_PUBLICLY",
  },
  {
    id: "priority_page_indexability",
    label: "Priority page indexability",
    dataStateDefault: "MEASURABLE_PUBLICLY",
  },
  {
    id: "canonical_integrity",
    label: "Canonical integrity",
    dataStateDefault: "MEASURABLE_PUBLICLY",
  },
  {
    id: "crawlable_html",
    label: "Crawlable HTML",
    dataStateDefault: "MEASURABLE_PUBLICLY",
  },
  {
    id: "development_page_accessibility",
    label: "Development/franchise page accessibility",
    dataStateDefault: "MEASURABLE_PUBLICLY",
  },
  {
    id: "ai_crawler_log_evidence",
    label: "AI crawler/bot evidence (logs)",
    dataStateDefault: "CONNECTION_REQUIRED",
  },
  {
    id: "last_observed_crawl",
    label: "Last observed crawl (logs)",
    dataStateDefault: "CONNECTION_REQUIRED",
  },
]);

export const PRIORITY_PAGE_STATUS = Object.freeze({
  PRESENT: "PRESENT",
  MISSING: "MISSING",
  UNKNOWN: "UNKNOWN",
});

export const PRIORITY_DEVELOPMENT_PAGE_TYPES = Object.freeze([
  { id: "brand_development_page", label: "Brand development page" },
  { id: "franchise_development_page", label: "Franchise/development page" },
  { id: "owner_page", label: "Owner page" },
  { id: "brand_overview", label: "Brand overview" },
  { id: "conversion_page", label: "Conversion page" },
  { id: "residences_page", label: "Residences page" },
  { id: "contact_development_inquiry", label: "Contact/development inquiry page" },
]);

export const DISCOVERABILITY_METRICS_V1 = Object.freeze([
  { id: "priority_pages_monitored", label: "Priority Pages Monitored" },
  { id: "priority_pages_crawlable", label: "Priority Pages Crawlable" },
  { id: "priority_pages_indexable", label: "Priority Pages Indexable" },
  { id: "ai_crawler_access", label: "AI Crawler Access" },
  { id: "pages_with_crawlable_development_content", label: "Pages With Crawlable Development Content" },
  { id: "last_observed_ai_crawl", label: "Last Observed AI Crawl" },
]);

export const CONTENT_IN_INITIAL_HTML = Object.freeze({
  YES: "YES",
  NO: "NO",
  PARTIAL: "PARTIAL",
  UNKNOWN: "UNKNOWN",
});

export const INDEXABILITY_STATUS = Object.freeze({
  TECHNICALLY_INDEXABLE: "technically_indexable",
  NOT_TECHNICALLY_INDEXABLE: "not_technically_indexable",
  UNKNOWN: "unknown",
});

export const DEVELOPMENT_CONTENT_FIELDS_V1 = Object.freeze([
  "brandNamePresent",
  "developmentFranchisePositioningPresent",
  "conversionLanguagePresent",
  "ownerDeveloperContactPresent",
  "residencesPresent",
  "geographicDevelopmentInfoPresent",
  "brandDifferentiationContentPresent",
]);
