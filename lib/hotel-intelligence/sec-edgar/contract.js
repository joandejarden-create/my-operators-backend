/**
 * SEC EDGAR Contract Intelligence — vocabulary & publication gates.
 * Contractual milestones persist via existing HI Graph + allowlisted HPC fields.
 * No new Airtable tables/fields without separate founder schema approval.
 */

export const SEC_EDGAR_CONTRACT_ID = "SEC_EDGAR_CONTRACT_INTELLIGENCE_V1";
export const SEC_EDGAR_CONTRACT_VERSION = "1.0.0";

/** Pilot paid-processing ceiling (USD). SEC HTTP itself is free. */
export const SEC_EDGAR_PILOT_PAID_CEILING_USD = 25;

export const VERIFICATION_CLASS = Object.freeze({
  VERIFIED_SIGNING_DATE: "VERIFIED_SIGNING_DATE",
  VERIFIED_EFFECTIVE_DATE: "VERIFIED_EFFECTIVE_DATE",
  VERIFIED_EXPIRATION: "VERIFIED_EXPIRATION",
  VERIFIED_RENEWAL_OPTION: "VERIFIED_RENEWAL_OPTION",
  VERIFIED_RENEWAL_NOTICE_DEADLINE: "VERIFIED_RENEWAL_NOTICE_DEADLINE",
  VERIFIED_TERMINATION: "VERIFIED_TERMINATION",
  VERIFIED_AMENDMENT: "VERIFIED_AMENDMENT",
  HISTORICAL_AGREEMENT: "HISTORICAL_AGREEMENT",
  ESTIMATED_RENEWAL_WINDOW: "ESTIMATED_RENEWAL_WINDOW",
  CONVERSION_SIGNAL: "CONVERSION_SIGNAL",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
});

/** Classes that may never be written as verified contractual facts. */
export const NON_VERIFIED_CLASSES = Object.freeze([
  VERIFICATION_CLASS.ESTIMATED_RENEWAL_WINDOW,
  VERIFICATION_CLASS.CONVERSION_SIGNAL,
  VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE,
]);

export const AGREEMENT_TYPE = Object.freeze({
  MANAGEMENT: "MANAGEMENT_AGREEMENT",
  FRANCHISE: "FRANCHISE_AGREEMENT",
  LEASE: "HOTEL_LEASE",
  AFFILIATION: "SOFT_BRAND_AFFILIATION",
  LICENSE: "BRAND_LICENSE",
  UNKNOWN: "UNKNOWN_AGREEMENT",
});

export const CHANGE_STATUS = Object.freeze({
  NEW: "NEW",
  UPDATED: "UPDATED",
  UNCHANGED: "UNCHANGED",
  SUPERSEDED: "SUPERSEDED",
  CONFLICTED: "CONFLICTED",
});

export const PERSISTENCE_CLASS = Object.freeze({
  STAGED_LOCAL: "STAGED_LOCAL",
  PLANNED_DRY_RUN: "PLANNED_DRY_RUN",
  PERSISTED_VERIFIED: "PERSISTED_VERIFIED",
  QUARANTINED: "QUARANTINED",
  REJECTED_ESTIMATE: "REJECTED_ESTIMATE",
  SKIPPED_NO_HPC_MATCH: "SKIPPED_NO_HPC_MATCH",
});

/** Canonical Field Name values for HI Graph Evidence (convention — not new schema). */
export const EVIDENCE_FIELD_NAMES = Object.freeze({
  MGMT_SIGNING: "Management Agreement Signing Date",
  MGMT_EFFECTIVE: "Management Agreement Effective Date",
  MGMT_EXPIRATION: "Management Agreement Expiration Date",
  MGMT_RENEWAL_OPTION: "Management Agreement Renewal Option",
  MGMT_RENEWAL_NOTICE: "Management Agreement Renewal Notice Deadline",
  MGMT_TERMINATION: "Management Agreement Termination Date",
  FRANCHISE_SIGNING: "Franchise Agreement Signing Date",
  FRANCHISE_EFFECTIVE: "Franchise Agreement Effective Date",
  FRANCHISE_EXPIRATION: "Franchise Agreement Expiration Date",
  FRANCHISE_RENEWAL: "Franchise Agreement Renewal Provision",
  LEASE_COMMENCEMENT: "Hotel Lease Commencement Date",
  LEASE_EXPIRATION: "Hotel Lease Expiration Date",
  AGREEMENT_AMENDMENT: "Agreement Amendment",
  OWNERSHIP_TRANSITION: "Ownership Transition Date",
  MANAGEMENT_TRANSITION: "Management Transition Date",
});

export const PUBLICATION_GATE = Object.freeze({
  customerFacing: false,
  autoPublish: false,
  layer: "AIRTABLE_PERSISTENCE_INTERNAL_ONLY",
  note:
    "Persisted contract intelligence stays INTERNAL_ONLY until separate publication approval.",
});

/** HPC fields that may receive verified null-fills (existing schema only). */
export const HPC_ALLOWLISTED_PROMOTION_FIELDS = Object.freeze([
  "Management Model",
  "Operator / Management Company",
  "Operator Type",
  "Operator Source URL",
  "Operator Confidence",
  "Affiliation As-Of Date",
  "Affiliation Start Date",
]);

export const SCHEMA_GAPS_REQUIRING_FOUNDER_APPROVAL = Object.freeze([
  {
    gap: "No HPC columns for agreement expiration / renewal / termination",
    mitigation:
      "Persist milestones on HI Graph Evidence Field Name + Value Observed; relationship Effective From/To when interval is filing-backed.",
  },
  {
    gap: "No SEC Source Type enum on Partner Source Library",
    mitigation: "Use HI Graph Evidence Source Type free text 'SEC EDGAR Filing'.",
  },
  {
    gap: "No dedicated CIK registry table",
    mitigation: "Pilot CIK map in lib + HI Organizations Registry / Identifier.",
  },
]);

export function isPersistableVerifiedClass(cls) {
  const c = String(cls || "");
  if (!c) return false;
  if (NON_VERIFIED_CLASSES.includes(c)) return false;
  return c.startsWith("VERIFIED_") || c === VERIFICATION_CLASS.HISTORICAL_AGREEMENT;
}

export function writeGuarantees() {
  return {
    contract: SEC_EDGAR_CONTRACT_ID,
    version: SEC_EDGAR_CONTRACT_VERSION,
    new_airtable_tables: false,
    new_airtable_fields: false,
    customer_publication: false,
    destructive_overwrite: false,
    estimates_as_verified: false,
    paid_ceiling_usd: SEC_EDGAR_PILOT_PAID_CEILING_USD,
    destinations: [
      "HI Graph Evidence",
      "HI Hotel–Org Relationships",
      "HI Organizations",
      "HI Ownership Research Runs",
      "HPC allowlisted null-fill only",
    ],
  };
}
