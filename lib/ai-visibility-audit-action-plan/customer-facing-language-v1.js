/**
 * AVA customer-facing language translations.
 * Internal research codes stay in Admin; PDF/customer surfaces use plain language.
 */

export const CUSTOMER_FACING_LANGUAGE_CONTRACT =
  "AVA_CUSTOMER_FACING_LANGUAGE_V1";

/** Exact token → customer phrase (word-boundary safe via replaceInternalLanguage). */
export const CUSTOMER_LANGUAGE_MAP = Object.freeze({
  PARTIAL_PUBLIC_SEARCH: "limited public research",
  PARTIAL_PUBLIC_REVIEW: "limited public review",
  PUBLIC_PARTIAL: "limited public research",
  VALIDATION_REQUIRED: "awaiting quality review",
  AUDIT_VALIDATED: "technically validated",
  VERIFIED_EXTERNAL_MENTION: "verified third-party reference",
  VERIFIED_BACKLINK: "verified website link",
  POTENTIAL_BACKLINK_OPPORTUNITY: "potential website-link opportunity",
  POTENTIAL_CITATION_OPPORTUNITY: "potential AI citation opportunity",
  OBSERVED_AI_CITATION: "observed AI citation",
  SUBJECT_COVERAGE_NOT_ESTABLISHED: "coverage not yet established",
  RECOMMENDED_INVESTIGATION: "Needs Investigation",
  REQUIRES_HOTEL_CONFIRMATION: "Hotel confirmation needed",
  VERIFIED_ISSUE: "Verified Problem",
  OPTIONAL_COMMERCIAL_OPPORTUNITY: "Optional Opportunity",
  VERIFIED_PUBLIC: "Verified public evidence",
  TECHNICAL_OBSERVATION: "Access note",
  UNABLE_TO_VERIFY: "Unable to verify",
  NOT_CURRENTLY_VERIFIED: "Not currently verified",
  HOTEL_CONFIRMATION_REQUIRED: "Hotel confirmation needed",
  FIRST_PARTY_OFFICIAL_SITE: "Hotel-operated website",
  BRAND_PROPERTY_PAGE: "Brand property page",
  AWAITING_HOTEL_DECISION: "Awaiting hotel decision",
  ZERO_COST: "standard technical checks",
  "zero-cost technical probes": "standard technical checks",
  "validation-stage payload": "this draft report",
  "Airtable write": "system record update",
  "ADP remeasurement": "new AI visibility measurement",
  "certified ADP baseline": "Dealality's latest AI visibility measurements",
  "no customer publication was performed":
    "this report has not been sent to the hotel",
  "no paid backlink inventory": "not a comprehensive paid link inventory",
  "Public V2 discovery": "Public research",
  "public V2 discovery": "public research",
  "first-party brand-family": "brand-owned websites",
  "low-quality referrers": "low-value directory pages",
  "A_DIRECT_HOTEL_BACKLINKS": "hotel website link search",
  "B_HOTEL_MENTIONS": "hotel mention search",
  "C_COMPETITOR_BACKLINKS": "competitor visibility search",
  "D_INDUSTRY_DESTINATION": "destination and industry search",
});

/** Patterns that should never appear in customer PDFs (case-insensitive). */
export const INTERNAL_LANGUAGE_PATTERNS = Object.freeze([
  /\bPARTIAL_PUBLIC_SEARCH\b/i,
  /\bPARTIAL_PUBLIC_REVIEW\b/i,
  /\bVALIDATION_REQUIRED\b/i,
  /\bVERIFIED_EXTERNAL_MENTION\b/i,
  /\bVERIFIED_BACKLINK\b/i,
  /\bPOTENTIAL_BACKLINK_OPPORTUNITY\b/i,
  /\bPOTENTIAL_CITATION_OPPORTUNITY\b/i,
  /\bSUBJECT_COVERAGE_NOT_ESTABLISHED\b/i,
  /\bRECOMMENDED_INVESTIGATION\b/i,
  /\bAVA-[A-Z0-9]+-\d{3}\b/,
  /\bAVA-[A-Z]+-DA-\d{3}\b/,
  /\bAVA-[A-Z]+-DCR-\d{3}\b/,
  /validation-stage payload/i,
  /\bthis payload\b/i,
  /\bTECHNICAL OBSERVATION\b/i,
  /\bTechnical observation\b/i,
  /\bAirtable write\b/i,
  /\bADP remeasurement\b/i,
  /zero-cost technical probes/i,
  /no customer publication was performed/i,
  /\bPUBLIC_PARTIAL\b/i,
  /\bfindingClass\b/i,
  /\bAUDIT_VALIDATED\b/i,
  /\bPublic V2 discovery\b/i,
  /\bfirst-party brand-family\b/i,
  /\bA_DIRECT_HOTEL_BACKLINKS\b/i,
  /\bB_HOTEL_MENTIONS\b/i,
  /\bC_COMPETITOR_BACKLINKS\b/i,
  /\bD_INDUSTRY_DESTINATION\b/i,
  /\bdiscovery strategies\b/i,
  /\bresearchInventory\b/i,
]);

export function customerLabelForCode(code) {
  if (code == null || code === "") return "";
  const key = String(code).trim();
  if (CUSTOMER_LANGUAGE_MAP[key]) return CUSTOMER_LANGUAGE_MAP[key];
  // Soft-brand underscore codes
  if (/^[A-Z][A-Z0-9_]+$/.test(key) && CUSTOMER_LANGUAGE_MAP[key]) {
    return CUSTOMER_LANGUAGE_MAP[key];
  }
  return CUSTOMER_LANGUAGE_MAP[key] || key.replace(/_/g, " ").toLowerCase();
}

export function claimTypeCustomerLabel(t) {
  const m = {
    VERIFIED_ISSUE: "Verified Problem",
    RECOMMENDED_INVESTIGATION: "Needs Investigation",
    OPTIONAL_COMMERCIAL_OPPORTUNITY: "Optional Opportunity",
    REQUIRES_HOTEL_CONFIRMATION: "Hotel confirmation needed",
  };
  return m[t] || customerLabelForCode(t) || t || "";
}

export function evidenceKindCustomerLabel(kind) {
  const m = {
    VERIFIED_PUBLIC: "Verified public evidence",
    TECHNICAL_OBSERVATION: "Access note",
    REQUIRES_HOTEL_CONFIRMATION: "Hotel confirmation needed",
    NOT_PUBLICLY_LINKABLE: "Not publicly linkable",
    UNABLE_TO_VERIFY: "Unable to verify",
    NOT_CURRENTLY_VERIFIED: "Not currently verified",
  };
  return m[kind] || customerLabelForCode(kind) || "Evidence";
}

export function findingClassCustomerLabel(cls) {
  const m = {
    VERIFIED_BACKLINK: "Verified website link",
    VERIFIED_EXTERNAL_MENTION: "Verified third-party reference",
    POTENTIAL_BACKLINK_OPPORTUNITY: "Potential website-link opportunity",
    POTENTIAL_CITATION_OPPORTUNITY: "Potential AI citation opportunity",
    OBSERVED_AI_CITATION: "Observed AI citation",
    SUBJECT_COVERAGE_NOT_ESTABLISHED: "Coverage not yet established",
    FIRST_PARTY_OFFICIAL_SITE: "Hotel-operated website",
  };
  return m[cls] || customerLabelForCode(cls) || cls || "";
}

/**
 * Replace known internal tokens inside free text for customer surfaces.
 * Does not invent content; only swaps known codes/phrases.
 */
export function replaceInternalLanguage(text) {
  if (text == null) return text;
  let out = String(text);
  // Longer phrases first
  const entries = Object.entries(CUSTOMER_LANGUAGE_MAP).sort(
    (a, b) => b[0].length - a[0].length
  );
  for (const [from, to] of entries) {
    if (!from) continue;
    if (/^[A-Z0-9_]+$/.test(from)) {
      const re = new RegExp(`\\b${from}\\b`, "g");
      out = out.replace(re, to);
    } else {
      const re = new RegExp(from.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi");
      out = out.replace(re, to);
    }
  }
  // Hide raw action IDs in customer prose (keep in Admin payloads)
  out = out.replace(/\bAVA-[A-Z0-9]+(?:-[A-Z]+)?-\d{3}\b/g, "the related action");
  return out;
}

export function scanInternalLanguageLeaks(text, section = "unknown") {
  const s = String(text || "");
  const hits = [];
  for (const re of INTERNAL_LANGUAGE_PATTERNS) {
    const m = s.match(re);
    if (m) {
      hits.push({
        section,
        pattern: String(re),
        match: m[0],
        severity: "INTERNAL_LANGUAGE_LEAK",
      });
    }
  }
  return hits;
}

/** Recursively scan string fields of a customer-facing payload slice. */
export function scanObjectForInternalLanguage(obj, section, out = []) {
  if (obj == null) return out;
  if (typeof obj === "string") {
    out.push(...scanInternalLanguageLeaks(obj, section));
    return out;
  }
  if (Array.isArray(obj)) {
    obj.forEach((v, i) => scanObjectForInternalLanguage(v, `${section}[${i}]`, out));
    return out;
  }
  if (typeof obj === "object") {
    for (const [k, v] of Object.entries(obj)) {
      // Skip pure Admin/meta keys
      if (
        /^(contract|moduleId|researchMode|paidProvider|scoreFactors|implementationStatus|pipeline|certification|sha256)/i.test(
          k
        )
      ) {
        continue;
      }
      // Internal IDs and classification codes stay in payload / Admin — not customer prose.
      if (
        [
          "findingClass",
          "claimType",
          "kind",
          "gapClass",
          "actionId",
          "relatedActionId",
          "opportunityId",
          "findingId",
          "questionId",
          "attributeId",
          "propertyId",
          "adpPeriodId",
          "moduleId",
          "contract",
          "classification",
          "authorityStatus",
          "sourceCategory",
          "status",
          "researchMode",
        ].includes(k)
      ) {
        continue;
      }
      scanObjectForInternalLanguage(v, `${section}.${k}`, out);
    }
  }
  return out;
}
