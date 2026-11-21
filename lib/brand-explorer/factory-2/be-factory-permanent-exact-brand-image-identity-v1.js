/**
 * BE Factory — Permanent Exact-Brand Image Identity V1
 *
 * Founder Quality Lesson: BE-QL-IMAGE-EXACT-BRAND-001
 * Contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1
 *
 * An image is eligible only when its actual hotel property is verified as
 * belonging to the EXACT brand shown in the target Brand Explorer profile.
 * Same parent company alone is never sufficient.
 */
import {
  BE_PERMANENT_SAME_BRAND_IMAGE_AFFINITY_V1,
  SAME_BRAND_IMAGE_FAIL,
  assessSameBrandImageAffinity,
  BRAND_IMAGE_AFFINITY_BY_SLUG,
} from "../be-permanent-same-brand-image-affinity-v1.js";

export const BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1 =
  "BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1";

export const BE_QL_IMAGE_EXACT_BRAND_001 = "BE-QL-IMAGE-EXACT-BRAND-001";

export const EXACT_BRAND_DECISION = Object.freeze({
  ELIGIBLE: "ELIGIBLE",
  REJECT_SIBLING_BRAND: "REJECT_SIBLING_BRAND",
  REJECT_WRONG_BRAND: "REJECT_WRONG_BRAND",
  HOLD_UNVERIFIED_PROPERTY: "HOLD_UNVERIFIED_PROPERTY",
  HOLD_AFFILIATION_CONFLICT: "HOLD_AFFILIATION_CONFLICT",
  HOLD_SPECIAL_CASE: "HOLD_SPECIAL_CASE",
  REJECT_MISSING_CONTEXT: "REJECT_MISSING_CONTEXT",
});

export const EXACT_BRAND_FAILURE_CODES = Object.freeze({
  IMAGE_SIBLING_BRAND_REJECTED: "IMAGE_SIBLING_BRAND_REJECTED",
  IMAGE_EXACT_BRAND_UNVERIFIED: "IMAGE_EXACT_BRAND_UNVERIFIED",
  IMAGE_ASSET_IDENTITY_MISMATCH: "IMAGE_ASSET_IDENTITY_MISMATCH",
  IMAGE_PROPERTY_AFFILIATION_CONFLICT: "IMAGE_PROPERTY_AFFILIATION_CONFLICT",
  IMAGE_RIGHTS_BLOCKED: "IMAGE_RIGHTS_BLOCKED",
  IMAGE_PROPOSED_REGRESSION: "IMAGE_PROPOSED_REGRESSION",
  IMAGE_ATTACHMENT_READBACK_FAILED: "IMAGE_ATTACHMENT_READBACK_FAILED",
});

/** Founder table examples — unit-test fixtures (targetSlug → forbidden sibling). */
export const EXACT_BRAND_REJECT_FIXTURES = Object.freeze([
  {
    targetSlug: "waldorf-astoria",
    propertyBrandSlug: "hilton-hotels-resorts",
    imageMeta: { propertyName: "Hilton Panama", caption: "Hilton Hotels & Resorts exterior" },
    imageUrl: "https://www.hilton.com/im/en/PTYHFHH/hilton-panama.jpg",
    expect: EXACT_BRAND_DECISION.REJECT_SIBLING_BRAND,
  },
  {
    targetSlug: "waldorf-astoria",
    propertyBrandSlug: "conrad-hotels-resorts",
    imageMeta: { propertyName: "Conrad Singapore", caption: "Conrad Hotels & Resorts" },
    imageUrl: "https://www.hilton.com/im/en/SINHICI/conrad-singapore.jpg",
    expect: EXACT_BRAND_DECISION.REJECT_SIBLING_BRAND,
  },
  {
    targetSlug: "ac-hotels-by-marriott",
    propertyBrandSlug: "courtyard",
    imageMeta: { propertyName: "Courtyard Miami", caption: "Courtyard by Marriott" },
    imageUrl: "https://cache.marriott.com/content/dam/marriott-renditions/MIACY/courtyard.jpg",
    expect: EXACT_BRAND_DECISION.REJECT_SIBLING_BRAND,
  },
  {
    targetSlug: "park-hyatt",
    propertyBrandSlug: "grand-hyatt",
    imageMeta: { propertyName: "Grand Hyatt Tokyo", caption: "Grand Hyatt" },
    imageUrl: "https://www.hyatt.com/grand-hyatt/tokyo",
    expect: EXACT_BRAND_DECISION.REJECT_SIBLING_BRAND,
  },
  {
    targetSlug: "kimpton",
    propertyBrandSlug: "hotel-indigo",
    imageMeta: { propertyName: "Hotel Indigo Miami", caption: "Hotel Indigo" },
    imageUrl: "https://www.ihg.com/hotelindigo/hotels/us/en/miami",
    expect: EXACT_BRAND_DECISION.REJECT_SIBLING_BRAND,
  },
  {
    targetSlug: "sofitel",
    propertyBrandSlug: "novotel",
    imageMeta: { propertyName: "Novotel Paris", caption: "Novotel" },
    imageUrl: "https://all.accor.com/hotel/novotel-paris",
    expect: EXACT_BRAND_DECISION.REJECT_SIBLING_BRAND,
  },
]);

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function normId(id) {
  const s = nz(id);
  return /^rec[a-zA-Z0-9]+$/.test(s) ? s : "";
}

/**
 * Compare canonical brand IDs when both sides have them.
 * Falls back to affinity slug heuristics when IDs are absent.
 *
 * @param {{
 *   targetBrandId?: string|null,
 *   targetSlug?: string|null,
 *   targetBrandName?: string|null,
 *   propertyBrandId?: string|null,
 *   propertyBrandSlug?: string|null,
 *   propertyBrandName?: string|null,
 *   propertyCensusId?: string|null,
 *   propertyVerified?: boolean,
 *   specialCase?: 'soft_brand'|'collection'|'dual_brand'|'rebrand'|'historic'|'mixed_use'|null,
 *   specialCaseApproved?: boolean,
 *   imageUrl?: string|null,
 *   imageMeta?: object,
 *   rightsOk?: boolean|null,
 * }} input
 */
export function evaluateExactBrandImageIdentity(input = {}) {
  const targetBrandId = normId(input.targetBrandId);
  const propertyBrandId = normId(input.propertyBrandId);
  const targetSlug = nz(input.targetSlug).toLowerCase();
  const propertySlug = nz(input.propertyBrandSlug).toLowerCase();
  const propertyVerified = input.propertyVerified === true || !!normId(input.propertyCensusId);

  if (input.rightsOk === false) {
    return {
      ok: false,
      decision: EXACT_BRAND_DECISION.REJECT_WRONG_BRAND,
      failureCode: EXACT_BRAND_FAILURE_CODES.IMAGE_RIGHTS_BLOCKED,
      contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
      lesson: BE_QL_IMAGE_EXACT_BRAND_001,
      reason: "usage_rights_blocked",
    };
  }

  if (input.specialCase && input.specialCaseApproved !== true) {
    return {
      ok: false,
      decision: EXACT_BRAND_DECISION.HOLD_SPECIAL_CASE,
      failureCode: EXACT_BRAND_FAILURE_CODES.IMAGE_PROPERTY_AFFILIATION_CONFLICT,
      contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
      lesson: BE_QL_IMAGE_EXACT_BRAND_001,
      reason: `special_case_requires_explicit_approval:${input.specialCase}`,
      specialCase: input.specialCase,
    };
  }

  // Canonical ID equality is authoritative when both present.
  if (targetBrandId && propertyBrandId) {
    if (targetBrandId === propertyBrandId) {
      if (!propertyVerified && input.requirePropertyVerification === true) {
        return {
          ok: false,
          decision: EXACT_BRAND_DECISION.HOLD_UNVERIFIED_PROPERTY,
          failureCode: EXACT_BRAND_FAILURE_CODES.IMAGE_EXACT_BRAND_UNVERIFIED,
          contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
          lesson: BE_QL_IMAGE_EXACT_BRAND_001,
          reason: "property_affiliation_unverified",
        };
      }
      return {
        ok: true,
        decision: EXACT_BRAND_DECISION.ELIGIBLE,
        failureCode: null,
        contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
        lesson: BE_QL_IMAGE_EXACT_BRAND_001,
        reason: "canonical_brand_id_match",
        matchMethod: "brand_basics_id",
      };
    }
    return {
      ok: false,
      decision: EXACT_BRAND_DECISION.REJECT_WRONG_BRAND,
      failureCode: EXACT_BRAND_FAILURE_CODES.IMAGE_ASSET_IDENTITY_MISMATCH,
      contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
      lesson: BE_QL_IMAGE_EXACT_BRAND_001,
      reason: "canonical_brand_id_mismatch",
      targetBrandId,
      propertyBrandId,
    };
  }

  if (targetSlug && propertySlug) {
    if (targetSlug === propertySlug) {
      return {
        ok: true,
        decision: EXACT_BRAND_DECISION.ELIGIBLE,
        failureCode: null,
        contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
        lesson: BE_QL_IMAGE_EXACT_BRAND_001,
        reason: "slug_match",
        matchMethod: "slug",
      };
    }
    // Different slugs under known sibling tables → sibling reject.
    return {
      ok: false,
      decision: EXACT_BRAND_DECISION.REJECT_SIBLING_BRAND,
      failureCode: EXACT_BRAND_FAILURE_CODES.IMAGE_SIBLING_BRAND_REJECTED,
      contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
      lesson: BE_QL_IMAGE_EXACT_BRAND_001,
      reason: "slug_mismatch_treated_as_sibling_or_foreign",
      targetSlug,
      propertySlug,
    };
  }

  // Heuristic affinity on URL/meta when property brand ID unknown.
  if (targetSlug && BRAND_IMAGE_AFFINITY_BY_SLUG[targetSlug]) {
    const affinity = assessSameBrandImageAffinity(
      targetSlug,
      input.imageUrl || "",
      input.imageMeta || {}
    );
    if (affinity.ok === true) {
      return {
        ok: true,
        decision: EXACT_BRAND_DECISION.ELIGIBLE,
        failureCode: null,
        contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
        lesson: BE_QL_IMAGE_EXACT_BRAND_001,
        reason: "affinity_heuristic_pass",
        matchMethod: "affinity",
        affinity,
        parentContract: BE_PERMANENT_SAME_BRAND_IMAGE_AFFINITY_V1,
      };
    }
    if (affinity.code === SAME_BRAND_IMAGE_FAIL.SIBLING_BRAND) {
      return {
        ok: false,
        decision: EXACT_BRAND_DECISION.REJECT_SIBLING_BRAND,
        failureCode: EXACT_BRAND_FAILURE_CODES.IMAGE_SIBLING_BRAND_REJECTED,
        contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
        lesson: BE_QL_IMAGE_EXACT_BRAND_001,
        reason: "affinity_sibling_rejected",
        affinity,
      };
    }
    return {
      ok: false,
      decision: EXACT_BRAND_DECISION.HOLD_UNVERIFIED_PROPERTY,
      failureCode: EXACT_BRAND_FAILURE_CODES.IMAGE_EXACT_BRAND_UNVERIFIED,
      contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
      lesson: BE_QL_IMAGE_EXACT_BRAND_001,
      reason: affinity.code || "affinity_unverified",
      affinity,
    };
  }

  if (!targetBrandId && !targetSlug) {
    return {
      ok: false,
      decision: EXACT_BRAND_DECISION.REJECT_MISSING_CONTEXT,
      failureCode: EXACT_BRAND_FAILURE_CODES.IMAGE_EXACT_BRAND_UNVERIFIED,
      contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
      lesson: BE_QL_IMAGE_EXACT_BRAND_001,
      reason: "missing_target_brand_context",
    };
  }

  return {
    ok: false,
    decision: EXACT_BRAND_DECISION.HOLD_UNVERIFIED_PROPERTY,
    failureCode: EXACT_BRAND_FAILURE_CODES.IMAGE_EXACT_BRAND_UNVERIFIED,
    contract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
    lesson: BE_QL_IMAGE_EXACT_BRAND_001,
    reason: "property_brand_identity_unverified",
  };
}

/**
 * Certification gate: exact-brand failure always blocks IMAGE_CERTIFIED.
 */
export function blocksImageCertification(decisionResult) {
  if (!decisionResult) return true;
  if (decisionResult.ok === true) return false;
  return true;
}

export default {
  BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
  BE_QL_IMAGE_EXACT_BRAND_001,
  EXACT_BRAND_DECISION,
  EXACT_BRAND_FAILURE_CODES,
  EXACT_BRAND_REJECT_FIXTURES,
  evaluateExactBrandImageIdentity,
  blocksImageCertification,
};
