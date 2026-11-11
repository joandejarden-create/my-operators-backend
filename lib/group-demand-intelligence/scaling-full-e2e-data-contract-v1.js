/**
 * Scaling Companies Full E2E — canonical data contract V1.
 *
 * One field vocabulary across ORS → maturation → publication → Airtable →
 * share sanitize → customer DTO → drawer. Does not invent a parallel schema;
 * validates essential fields survive each boundary.
 */

import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import {
  applyScalingTwoContactStandard,
  extractPrimaryPersonName,
  SCALING_CONTACT_COVERAGE,
} from "./scaling-two-contact-standard-v1.js";
import { sanitizeOpportunityForCustomerPresentation } from "./customer-presentation-sanitize-v1.js";
import { sanitizeOpportunityForShare } from "./share/gdi-signed-share-capability-v1.js";
import { AIRTABLE_LONG_TEXT_SAFE_CHARS } from "./airtable-opportunity-store.js";

export const SCALING_FULL_E2E_DATA_CONTRACT_V1 =
  "gdi-scaling-full-e2e-data-contract-v1";

export const SCALING_CONTRACT_REQUIRED_FIELDS = Object.freeze([
  "hotelId",
  "id",
  "organizationName",
  "researchSegment",
  "whatIsHappening",
  "localConnection",
  "proposedHotelUse",
  "recommendedAction",
  "customerVisible",
]);

export const SCALING_RESEARCH_OUTCOME = Object.freeze({
  RESEARCH_INCOMPLETE: "RESEARCH_INCOMPLETE",
  COMMERCIALLY_INELIGIBLE: "COMMERCIALLY_INELIGIBLE",
  ELIGIBLE_INFERRED_DEMAND: "ELIGIBLE_INFERRED_DEMAND",
  VERIFIED_SOURCING: "VERIFIED_SOURCING",
  TECHNICAL_FAILURE: "TECHNICAL_FAILURE",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Classify research outcome vs technical failure (orthogonal to coverage).
 */
export function classifyScalingResearchOutcome(opportunity = {}, opts = {}) {
  if (opts.technicalFailure === true || opportunity.technicalFailure === true) {
    return SCALING_RESEARCH_OUTCOME.TECHNICAL_FAILURE;
  }
  const disp = text(
    opportunity.scalingCommercialDisposition || opportunity.disposition || "",
  );
  if (/HOLD_NOT_HOTEL_BUYER|EXCLUDE|BETTER_FIT|COMMERCIALLY_INELIGIBLE/i.test(disp)) {
    return SCALING_RESEARCH_OUTCOME.COMMERCIALLY_INELIGIBLE;
  }
  if (/HOLD_INSUFFICIENT|RESEARCH_CONTINUATION/i.test(disp)) {
    return SCALING_RESEARCH_OUTCOME.RESEARCH_INCOMPLETE;
  }
  if (
    opportunity.verifiedHotelSourcing === true ||
    /verified\s+(room\s+block|hotel\s+sourc|rfp)/i.test(
      text(opportunity.proposedHotelUse),
    )
  ) {
    return SCALING_RESEARCH_OUTCOME.VERIFIED_SOURCING;
  }
  if (opportunity.customerVisible !== false && opportunity.organizationName) {
    return SCALING_RESEARCH_OUTCOME.ELIGIBLE_INFERRED_DEMAND;
  }
  return SCALING_RESEARCH_OUTCOME.RESEARCH_INCOMPLETE;
}

/**
 * Assert essential Scaling fields are present on a matured opportunity.
 */
export function validateScalingCanonicalFields(opportunity = {}) {
  const missing = [];
  for (const key of SCALING_CONTRACT_REQUIRED_FIELDS) {
    if (key === "id") {
      if (!text(opportunity.id || opportunity.opportunityId)) missing.push("id");
      continue;
    }
    if (key === "customerVisible") {
      if (opportunity.customerVisible === undefined) missing.push("customerVisible");
      continue;
    }
    if (!text(opportunity[key])) missing.push(key);
  }
  return {
    ok: missing.length === 0,
    missing,
    contract: SCALING_FULL_E2E_DATA_CONTRACT_V1,
  };
}

/**
 * Boundary: matured → share sanitize → customer presentation.
 * Detects silent field loss for contacts and sales narrative.
 */
export function assertScalingFieldsSurviveSanitization(opportunity = {}) {
  const matured = applyScalingTwoContactStandard(
    {
      ...opportunity,
      researchSegment: opportunity.researchSegment || "scaling_companies",
    },
    { force: true },
  );
  const customer = sanitizeOpportunityForCustomerPresentation(matured);
  const share = sanitizeOpportunityForShare(customer);
  const defects = [];

  const primaryBefore = extractPrimaryPersonName(matured);
  const primaryAfter =
    share?.scalingTwoContactPresentation?.primary?.name ||
    share?.primaryContact?.name ||
    null;
  if (primaryBefore && looksLikePersonName(primaryBefore) && !primaryAfter) {
    defects.push({
      code: "PRIMARY_NAMED_DROPPED_IN_SHARE",
      before: primaryBefore,
    });
  }

  const secBefore = text(matured.secondaryContact?.name);
  const secAfter =
    share?.scalingTwoContactPresentation?.secondary?.name ||
    share?.secondaryContact?.name ||
    null;
  // Secondary may be cleared when promoted to primary — not a defect
  if (
    secBefore &&
    looksLikePersonName(secBefore) &&
    primaryBefore &&
    secBefore.toLowerCase() !== primaryBefore.toLowerCase() &&
    !secAfter &&
    matured.scalingTwoContactPresentation?.secondary?.customerFacingNamed
  ) {
    defects.push({
      code: "SECONDARY_NAMED_DROPPED_IN_SHARE",
      before: secBefore,
    });
  }

  if (
    text(matured.whatIsHappening) &&
    !text(share?.whatIsHappening || share?.summaryWhat)
  ) {
    defects.push({ code: "WHAT_IS_HAPPENING_DROPPED_IN_SHARE" });
  }

  if (!share?.scalingTwoContactPresentation) {
    defects.push({ code: "SCALING_TWO_CONTACT_PRESENTATION_MISSING_IN_SHARE" });
  }

  // Org phone must not appear as Direct phone without a named person phone
  const p = share?.scalingTwoContactPresentation?.primary;
  if (
    p &&
    !p.name &&
    p.phoneLabel === "Direct phone" &&
    p.phone
  ) {
    defects.push({ code: "ORG_PHONE_MISLABELED_AS_DIRECT" });
  }

  return {
    ok: defects.length === 0,
    defects,
    primaryBefore,
    primaryAfter,
    coverage: share?.scalingContactCoverage || null,
    contract: SCALING_FULL_E2E_DATA_CONTRACT_V1,
  };
}

/**
 * Detect historical defect patterns on a single opportunity (offline).
 */
export function detectHistoricalScalingDefects(opportunity = {}) {
  const findings = [];
  const o = opportunity || {};

  // deep.opportunity vs deep.after.opportunity — surface if both shapes present inconsistently
  if (o._deepResearchShape === "top_level_only") {
    findings.push({ code: "DEEP_OPPORTUNITY_SHAPE_TOP_LEVEL_ONLY", severity: "HIGH" });
  }

  const primary = extractPrimaryPersonName(o);
  const sec = text(o.secondaryContact?.name);
  if (!primary && sec && looksLikePersonName(sec)) {
    findings.push({
      code: "ORPHAN_SECONDARY_NOT_PROMOTED",
      severity: "HIGH",
      detail: sec,
    });
  }

  if (
    /evidence dated\s+20\d{2}-\d{2}-\d{2}/i.test(
      text(o.whatIsHappening || o.datedGrowthEvidence),
    )
  ) {
    findings.push({ code: "RESEARCH_DATE_AS_GROWTH_DATE", severity: "HIGH" });
  }

  if (
    o.primaryContact?.phone &&
    !primary &&
    /person|direct/i.test(String(o.primaryContact?.phoneLabel || o.contactPhoneLabel || ""))
  ) {
    findings.push({ code: "COMPANY_PHONE_AS_PERSON_DIRECT", severity: "HIGH" });
  }

  if (
    /\.ai\b/i.test(text(o.organizationName) + " " + text(o.website || o.domain)) &&
    /UNKNOWN_INVALID|rejected.*\.ai/i.test(String(o.entityTruthClass || o.entityClass || ""))
  ) {
    findings.push({ code: "AI_TLD_FALSE_NEGATIVE", severity: "HIGH" });
  }

  const payload = JSON.stringify(o);
  if (payload.length > AIRTABLE_LONG_TEXT_SAFE_CHARS) {
    findings.push({
      code: "PAYLOAD_EXCEEDS_AIRTABLE_LIMIT",
      severity: "HIGH",
      chars: payload.length,
      limit: AIRTABLE_LONG_TEXT_SAFE_CHARS,
    });
  }

  if (
    /ORS|pre-ORS|OPEN_UNRESOLVED|NAMED_DIRECT|Evidence:\s*Exploratory/i.test(
      text(o.whatIsHappening) + " " + text(o.recommendedAction),
    )
  ) {
    findings.push({ code: "INTERNAL_RESEARCH_LANGUAGE_IN_NARRATIVE", severity: "MEDIUM" });
  }

  return {
    opportunityId: o.id || o.opportunityId || null,
    findings,
    pass: !findings.some((f) => f.severity === "HIGH"),
  };
}

export { SCALING_CONTACT_COVERAGE, AIRTABLE_LONG_TEXT_SAFE_CHARS };
