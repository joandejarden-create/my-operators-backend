/**
 * Independent hotel census staging tables (Deal Capture Platform / AIRTABLE_BASE_ID_ALT).
 *
 * Phase 1: constants only. No ingest, no promotion, no writes to Hotel Census.
 * See docs/independent-hotel-census-inventory.md and docs/independent-hotel-census-pipeline.md.
 */

import { HOTEL_CENSUS_TABLE } from "../hotel-census/fields.js";

/** Current production census — link/compare only; never write from this module. */
export const CURRENT_CENSUS_TABLE_FOR_LINKS = HOTEL_CENSUS_TABLE;

export const CANDIDATES_TABLE =
  process.env.AIRTABLE_INDEPENDENT_CENSUS_CANDIDATES_TABLE ||
  "Independent Hotel Source Candidates";

export const EVIDENCE_TABLE =
  process.env.AIRTABLE_INDEPENDENT_CENSUS_EVIDENCE_TABLE ||
  "Independent Hotel Source Evidence";

export const VERIFIED_TABLE =
  process.env.AIRTABLE_VERIFIED_INDEPENDENT_CENSUS_TABLE ||
  "Verified Independent Hotel Census";

export const CANDIDATE_FIELDS = {
  sourceName: "Source Name",
  sourceType: "Source Type",
  sourceLicense: "Source License",
  sourceUrl: "Source URL",
  sourceRecordId: "Source Record ID",
  rawHotelName: "Raw Hotel Name",
  rawAddress: "Raw Address",
  rawCity: "Raw City",
  rawCountry: "Raw Country",
  rawLatitude: "Raw Latitude",
  rawLongitude: "Raw Longitude",
  rawWebsite: "Raw Website",
  rawPhone: "Raw Phone",
  rawBrand: "Raw Brand",
  rawPayloadJson: "Raw Payload JSON",
  importBatchId: "Import Batch ID",
  importedAt: "Imported At",
  reviewStatus: "Review Status",
  possibleMatchInCurrentCensus: "Possible Match in Current Census",
  possibleMatchConfidence: "Possible Match Confidence",
  recommendedAction: "Recommended Action",
  candidateDedupeKey: "Candidate Dedupe Key",
  linkedEvidence: "Linked Evidence",
};

export const EVIDENCE_FIELDS = {
  candidate: "Candidate",
  evidenceType: "Evidence Type",
  evidenceUrl: "Evidence URL",
  evidenceText: "Evidence Text",
  capturedAt: "Captured At",
  capturedBy: "Captured By",
  comparesToCensusRecord: "Compares To Census Record",
  matchScore: "Match Score",
  matchReason: "Match Reason",
};

export const VERIFIED_FIELDS = {
  verifiedHotelName: "Verified Hotel Name",
  verifiedAddress: "Verified Address",
  verifiedCity: "Verified City",
  verifiedState: "Verified State",
  verifiedCountry: "Verified Country",
  verifiedPostalCode: "Verified Postal Code",
  verifiedLatitude: "Verified Latitude",
  verifiedLongitude: "Verified Longitude",
  verifiedWebsite: "Verified Website",
  verifiedPhone: "Verified Phone",
  verifiedBrandLabel: "Verified Brand Label",
  primarySourceCandidate: "Primary Source Candidate",
  approvedAt: "Approved At",
  approvedBy: "Approved By",
  approvalNotes: "Approval Notes",
  censusReconciliationStatus: "Census Reconciliation Status",
  linkedCensusRecord: "Linked Census Record",
  verifiedDedupeKey: "Verified Dedupe Key",
  active: "Active",
};

/** Source Type single-select values (Airtable). */
export const SOURCE_TYPES = {
  OSM: "osm",
  WIKIDATA: "wikidata",
  BRAND_DIRECTORY: "brand_directory",
  GOVERNMENT_REGISTRY: "government_registry",
  MANUAL_UPLOAD: "manual_upload",
};

/** Review Status on candidates. */
export const REVIEW_STATUS = {
  PENDING: "pending",
  IN_REVIEW: "in_review",
  APPROVED: "approved",
  REJECTED: "rejected",
  DUPLICATE: "duplicate",
};

/** Possible Match Confidence on candidates. */
export const MATCH_CONFIDENCE = {
  NONE: "none",
  LOW: "low",
  MEDIUM: "medium",
  HIGH: "high",
};

/** Recommended Action on candidates. */
export const RECOMMENDED_ACTION = {
  PROMOTE: "promote",
  MERGE_WITH_CENSUS: "merge_with_census",
  SKIP: "skip",
  NEEDS_RESEARCH: "needs_research",
};

/** Evidence Type single-select values. */
export const EVIDENCE_TYPES = {
  SOURCE_SNAPSHOT: "source_snapshot",
  GEOCODE: "geocode",
  MANUAL_NOTE: "manual_note",
  CENSUS_COMPARISON: "census_comparison",
  LICENSE_CHECK: "license_check",
};

/** Census Reconciliation Status on verified rows. */
export const RECONCILIATION_STATUS = {
  NOT_IN_CENSUS: "not_in_census",
  LIKELY_DUPLICATE: "likely_duplicate",
  MATCHED_TO_CENSUS: "matched_to_census",
  /** Backwards-match: legacy census used read-only for confidence only. */
  LEGACY_CENSUS_MATCHED_READ_ONLY: "legacy_census_matched_read_only",
};

export const STAGING_TABLES = {
  candidates: CANDIDATES_TABLE,
  evidence: EVIDENCE_TABLE,
  verified: VERIFIED_TABLE,
};
