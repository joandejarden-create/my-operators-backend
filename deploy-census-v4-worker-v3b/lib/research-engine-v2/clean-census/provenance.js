/**
 * Clean Census Reconstruction — provenance classifications & material field set.
 * Uses actual Hotel Census / directory enrichment field names — no invented columns.
 */

import { CENSUS_FIELDS } from "../../hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../../hotel-census/brand-directory-enrichment-contract.js";

export const PROVENANCE_CLASSES = Object.freeze([
  "Independent",
  "First-Party Validated",
  "Legacy-Origin — Unreconstructed",
  "Mixed Provenance",
  "Unknown Provenance",
]);

export const CLEAN_CENSUS_RECORD_STATUSES = Object.freeze([
  "Independent — Complete",
  "Independent — Materially Complete",
  "Independent — Remediation Required",
  "Independent — Deep Research Required",
  "Legacy Match — Independent Reconstruction Complete",
  "Legacy Only — Independent Confirmation Pending",
  "First-Party Validated",
  "Hold — Evidence Conflict",
]);

export const RESEARCH_MODES_CLEAN = Object.freeze({
  CLEAN_CENSUS_RECONSTRUCTION: "clean_census_reconstruction",
  LEGACY_RECONCILIATION: "legacy_reconciliation",
  LEGACY_ONLY_CHALLENGE: "legacy_only_challenge",
  FIRST_PARTY_VALIDATION: "first_party_validation",
});

/** Material fields Research Engine may attempt — from real schema maps only. */
export const MATERIAL_CENSUS_FIELDS = Object.freeze([
  CENSUS_FIELDS.name,
  CENSUS_FIELDS.affiliation,
  CENSUS_FIELDS.parentCompany,
  CENSUS_FIELDS.status,
  CENSUS_FIELDS.rooms,
  CENSUS_FIELDS.country,
  CENSUS_FIELDS.city,
  CENSUS_FIELDS.market,
  CENSUS_FIELDS.submarket,
  CENSUS_FIELDS.chainScale,
  CENSUS_FIELDS.location,
  CENSUS_FIELDS.managementCompany,
  CENSUS_FIELDS.propertyType,
  CENSUS_FIELDS.hotelServiceModel,
  MAP_DIRECTORY_ENRICHMENT.openDate,
  MAP_DIRECTORY_ENRICHMENT.projectedOpenDate,
  MAP_DIRECTORY_ENRICHMENT.lat,
  MAP_DIRECTORY_ENRICHMENT.lng,
  MAP_DIRECTORY_ENRICHMENT.state,
  MAP_DIRECTORY_ENRICHMENT.address1,
  MAP_DIRECTORY_ENRICHMENT.postalCode,
  MAP_DIRECTORY_ENRICHMENT.telephone,
  MAP_DIRECTORY_ENRICHMENT.website,
  MAP_DIRECTORY_ENRICHMENT.brandPropertyCode,
  MAP_DIRECTORY_ENRICHMENT.amenities,
  "Property ID",
  "Website",
]);

/** Core identity/status fields required for "Materially Complete". */
export const CORE_MATERIAL_FIELDS = Object.freeze([
  CENSUS_FIELDS.name,
  CENSUS_FIELDS.affiliation,
  CENSUS_FIELDS.parentCompany,
  CENSUS_FIELDS.status,
  CENSUS_FIELDS.country,
  CENSUS_FIELDS.city,
  "Website",
  "Property ID",
]);

/**
 * @param {object} claim
 */
export function createFieldClaim(claim = {}) {
  return {
    field: claim.field,
    value: claim.value ?? null,
    source: claim.source || null,
    source_type: claim.source_type || null,
    evidence_url: claim.evidence_url || null,
    evidence_date: claim.evidence_date || null,
    retrieval_date: claim.retrieval_date || new Date().toISOString(),
    confidence: claim.confidence || null,
    research_mode: claim.research_mode || RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
    claim_status: claim.claim_status || (claim.value != null ? "Observed" : "Unknown"),
    temporal_status: claim.temporal_status || "current",
    origin: claim.origin || "Independent",
    first_party_validation: claim.first_party_validation || "None",
    legacy_used_as_source: false,
  };
}

/**
 * @param {object[]} claims
 */
export function scoreMaterialCompleteness(claims) {
  const byField = new Map(claims.filter((c) => c.value != null && c.value !== "").map((c) => [c.field, c]));
  const corePresent = CORE_MATERIAL_FIELDS.filter((f) => byField.has(f)).length;
  const materialPresent = MATERIAL_CENSUS_FIELDS.filter((f) => byField.has(f)).length;
  const independentSupported = claims.filter(
    (c) =>
      c.value != null &&
      c.value !== "" &&
      c.origin === "Independent" &&
      c.legacy_used_as_source === false
  ).length;
  return {
    corePresent,
    coreTotal: CORE_MATERIAL_FIELDS.length,
    corePct: Math.round((corePresent / CORE_MATERIAL_FIELDS.length) * 100),
    materialPresent,
    materialTotal: MATERIAL_CENSUS_FIELDS.length,
    materialPct: Math.round((materialPresent / MATERIAL_CENSUS_FIELDS.length) * 100),
    independentSupportedFieldCount: independentSupported,
    unresolvedCore: CORE_MATERIAL_FIELDS.filter((f) => !byField.has(f)),
  };
}

/**
 * @param {object} completeness
 * @param {{ conflicts?: boolean, firstParty?: boolean }} [opts]
 */
export function decideReconstructionStatus(completeness, opts = {}) {
  if (opts.conflicts) return "Hold — Evidence Conflict";
  if (opts.firstParty) return "First-Party Validated";
  if (completeness.corePct >= 100 && completeness.materialPct >= 70) return "Independent — Complete";
  if (completeness.corePct >= 100) return "Independent — Materially Complete";
  if (completeness.corePct >= 60) return "Independent — Remediation Required";
  return "Independent — Deep Research Required";
}
