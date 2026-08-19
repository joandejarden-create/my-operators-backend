/**
 * AI Demand Positioning — Airtable field map (central mapping object).
 * Apply schema via scripts/ensure-ai-demand-positioning-schema.mjs only.
 */

export const ADP_PUBLISHED_REPORTS_TABLE = "AI Demand Positioning - Published Reports";

export const ADP_CENSUS_TABLE =
  process.env.ADP_CENSUS_TABLE_NAME || "Hotel Property Census";

/** Field names — use map values in writers/readers, never scatter raw names. */
export const map_adp_published_report = Object.freeze({
  table: ADP_PUBLISHED_REPORTS_TABLE,
  reportName: "Report Name",
  adpPropertyId: "ADP Property ID",
  periodId: "Period ID",
  propertyName: "Property Name",
  city: "City",
  state: "State",
  market: "Market",
  executionDate: "Execution Date",
  publishedAt: "Published At",
  publishStatus: "Publish Status",
  productVersion: "Product Version",
  demandCaptureRate: "Demand Capture Rate",
  providerCount: "Provider Count",
  payloadJson: "Payload JSON",
  evidenceIndexJson: "Evidence Index JSON",
  payloadStoreRef: "Payload Store Ref",
  censusRecordId: "Census Record ID",
  /** Cross-base census join: store Hotel Property Census rec… until native link is available in same base. */
  linkedCensusProperty: "Linked Census Property",
});

/** Validates Airtable census record id format (rec + 14+ alphanumeric). */
export function isValidCensusRecordId(value) {
  const v = String(value || "").trim();
  return /^rec[a-zA-Z0-9]{10,}$/.test(v);
}

export const ADP_PUBLISH_STATUS_OPTIONS = Object.freeze([
  "Draft",
  "Validated",
  "Live",
  "Archived",
]);

export function isAllowedPublishStatus(value) {
  return ADP_PUBLISH_STATUS_OPTIONS.includes(value);
}
