/**
 * Brand Explorer Factory — Permanent Image Integrity Contract V3
 *
 * Extends V2 with:
 * - Founder V3 classification vocabulary (IMAGE_*)
 * - Exact-brand identity gate
 * - Shared Verified Hotel Image Library linkage
 * - Certification layers (SIX_IMAGES_* / scenario tiers)
 *
 * Contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3
 */
import {
  BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2,
  REQUIRED_IMAGE_SLOTS,
  GALLERY_SLOTS,
  SCENARIO_SLOTS,
  IMAGE_SLOT_CLASS,
  IMAGE_LIFECYCLE,
  IMAGE_COVERAGE_LAYER,
  classifyLiveAttachment,
  classifyProposedUrlStatus,
  isAirtableSignedCdnUrl,
  bumpCoverage,
  emptyCoverageTotals,
  decodeImageDimensions,
  isRequiredImageSlot,
  extractImageUrl,
  urlFingerprint,
} from "./be-factory-permanent-image-integrity-v2.js";
import {
  BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
  EXACT_BRAND_FAILURE_CODES,
  evaluateExactBrandImageIdentity,
  blocksImageCertification,
} from "./be-factory-permanent-exact-brand-image-identity-v1.js";

export const BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3 =
  "BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3";

export {
  BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2,
  BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
  REQUIRED_IMAGE_SLOTS,
  GALLERY_SLOTS,
  SCENARIO_SLOTS,
  IMAGE_SLOT_CLASS,
  IMAGE_LIFECYCLE,
  IMAGE_COVERAGE_LAYER,
  EXACT_BRAND_FAILURE_CODES,
  classifyLiveAttachment,
  classifyProposedUrlStatus,
  isAirtableSignedCdnUrl,
  bumpCoverage,
  emptyCoverageTotals,
  decodeImageDimensions,
  isRequiredImageSlot,
  extractImageUrl,
  urlFingerprint,
  evaluateExactBrandImageIdentity,
  blocksImageCertification,
};

/** Founder V3 classification vocabulary (do not collapse). */
export const IMAGE_V3_CLASS = Object.freeze({
  IMAGE_MISSING: "IMAGE_MISSING",
  IMAGE_ATTACHMENT_PRESENT: "IMAGE_ATTACHMENT_PRESENT",
  IMAGE_FETCH_FAILED: "IMAGE_FETCH_FAILED",
  IMAGE_DECODE_FAILED: "IMAGE_DECODE_FAILED",
  IMAGE_SIGNED_URL_EXPIRED: "IMAGE_SIGNED_URL_EXPIRED",
  IMAGE_WRONG_BRAND: "IMAGE_WRONG_BRAND",
  IMAGE_SIBLING_BRAND: "IMAGE_SIBLING_BRAND",
  IMAGE_PROPERTY_UNVERIFIED: "IMAGE_PROPERTY_UNVERIFIED",
  IMAGE_WRONG_SCENE: "IMAGE_WRONG_SCENE",
  IMAGE_DUPLICATE: "IMAGE_DUPLICATE",
  IMAGE_RIGHTS_UNVERIFIED: "IMAGE_RIGHTS_UNVERIFIED",
  IMAGE_TECHNICALLY_VALID: "IMAGE_TECHNICALLY_VALID",
  IMAGE_BRAND_VERIFIED: "IMAGE_BRAND_VERIFIED",
  IMAGE_BROWSER_VALIDATED: "IMAGE_BROWSER_VALIDATED",
  IMAGE_CERTIFIED: "IMAGE_CERTIFIED",
});

/** Gallery completeness layers (count ≠ certification). */
export const GALLERY_CERT_LAYER = Object.freeze({
  SIX_IMAGES_PRESENT: "SIX_IMAGES_PRESENT",
  SIX_IMAGES_TECHNICALLY_VALID: "SIX_IMAGES_TECHNICALLY_VALID",
  SIX_IMAGES_BRAND_VERIFIED: "SIX_IMAGES_BRAND_VERIFIED",
  SIX_IMAGES_USAGE_APPROVED: "SIX_IMAGES_USAGE_APPROVED",
  SIX_IMAGES_BROWSER_VALIDATED: "SIX_IMAGES_BROWSER_VALIDATED",
  SIX_IMAGES_CERTIFIED: "SIX_IMAGES_CERTIFIED",
});

export const SCENARIO_CERT_LAYER = Object.freeze({
  THREE_IMAGES_PRESENT: "THREE_IMAGES_PRESENT",
  THREE_IMAGES_TECHNICALLY_VALID: "THREE_IMAGES_TECHNICALLY_VALID",
  THREE_IMAGES_BRAND_VERIFIED: "THREE_IMAGES_BRAND_VERIFIED",
  THREE_IMAGES_USAGE_APPROVED: "THREE_IMAGES_USAGE_APPROVED",
  THREE_IMAGES_BROWSER_VALIDATED: "THREE_IMAGES_BROWSER_VALIDATED",
  THREE_IMAGES_CERTIFIED: "THREE_IMAGES_CERTIFIED",
});

/**
 * Map V2 slot class (+ optional exact-brand decision) → V3 founder vocabulary.
 */
export function mapV2ClassToV3(v2Class, { exactBrand = null, rightsOk = null, browserOk = null, duplicate = false } = {}) {
  if (duplicate) return IMAGE_V3_CLASS.IMAGE_DUPLICATE;
  if (exactBrand && exactBrand.ok === false) {
    if (exactBrand.failureCode === EXACT_BRAND_FAILURE_CODES.IMAGE_SIBLING_BRAND_REJECTED) {
      return IMAGE_V3_CLASS.IMAGE_SIBLING_BRAND;
    }
    if (exactBrand.failureCode === EXACT_BRAND_FAILURE_CODES.IMAGE_RIGHTS_BLOCKED) {
      return IMAGE_V3_CLASS.IMAGE_RIGHTS_UNVERIFIED;
    }
    if (
      exactBrand.decision === "HOLD_UNVERIFIED_PROPERTY" ||
      exactBrand.failureCode === EXACT_BRAND_FAILURE_CODES.IMAGE_EXACT_BRAND_UNVERIFIED
    ) {
      return IMAGE_V3_CLASS.IMAGE_PROPERTY_UNVERIFIED;
    }
    return IMAGE_V3_CLASS.IMAGE_WRONG_BRAND;
  }
  switch (v2Class) {
    case IMAGE_SLOT_CLASS.MISSING:
    case IMAGE_SLOT_CLASS.ATTACHMENT_MISSING:
      return IMAGE_V3_CLASS.IMAGE_MISSING;
    case IMAGE_SLOT_CLASS.SIGNED_URL_EXPIRED:
      return IMAGE_V3_CLASS.IMAGE_SIGNED_URL_EXPIRED;
    case IMAGE_SLOT_CLASS.ATTACHMENT_RENDER_FAILED:
    case IMAGE_SLOT_CLASS.SOURCE_FETCH_FAILED:
      return IMAGE_V3_CLASS.IMAGE_FETCH_FAILED;
    case IMAGE_SLOT_CLASS.IMAGE_CORRUPT:
    case IMAGE_SLOT_CLASS.FORMAT_UNSUPPORTED:
      return IMAGE_V3_CLASS.IMAGE_DECODE_FAILED;
    case IMAGE_SLOT_CLASS.WRONG_BRAND:
      return IMAGE_V3_CLASS.IMAGE_WRONG_BRAND;
    case IMAGE_SLOT_CLASS.WRONG_SCENE:
      return IMAGE_V3_CLASS.IMAGE_WRONG_SCENE;
    case IMAGE_SLOT_CLASS.DUPLICATE_IMAGE:
      return IMAGE_V3_CLASS.IMAGE_DUPLICATE;
    case IMAGE_SLOT_CLASS.RIGHTS_UNVERIFIED:
      return IMAGE_V3_CLASS.IMAGE_RIGHTS_UNVERIFIED;
    case IMAGE_SLOT_CLASS.ATTACHMENT_PRESENT_UNVERIFIED:
      return IMAGE_V3_CLASS.IMAGE_ATTACHMENT_PRESENT;
    case IMAGE_SLOT_CLASS.VALIDATED:
      if (browserOk === true && exactBrand?.ok === true && rightsOk === true) {
        return IMAGE_V3_CLASS.IMAGE_CERTIFIED;
      }
      if (browserOk === true) return IMAGE_V3_CLASS.IMAGE_BROWSER_VALIDATED;
      if (exactBrand?.ok === true) return IMAGE_V3_CLASS.IMAGE_BRAND_VERIFIED;
      return IMAGE_V3_CLASS.IMAGE_TECHNICALLY_VALID;
    case IMAGE_SLOT_CLASS.CERTIFIED:
      return IMAGE_V3_CLASS.IMAGE_CERTIFIED;
    default:
      return IMAGE_V3_CLASS.IMAGE_ATTACHMENT_PRESENT;
  }
}

/**
 * Evaluate gallery certification layer for a brand's six gallery slot results.
 * @param {Array<{ v3Class: string }>} gallerySlotResults length 6 expected
 */
export function evaluateGalleryCertLayer(gallerySlotResults = []) {
  const slots = gallerySlotResults.filter(Boolean);
  if (slots.length < 6) return null;
  const present = slots.every((s) => s.v3Class !== IMAGE_V3_CLASS.IMAGE_MISSING);
  if (!present) return null;
  const techOk = slots.every((s) =>
    [
      IMAGE_V3_CLASS.IMAGE_TECHNICALLY_VALID,
      IMAGE_V3_CLASS.IMAGE_BRAND_VERIFIED,
      IMAGE_V3_CLASS.IMAGE_BROWSER_VALIDATED,
      IMAGE_V3_CLASS.IMAGE_CERTIFIED,
    ].includes(s.v3Class)
  );
  const brandOk = slots.every((s) =>
    [
      IMAGE_V3_CLASS.IMAGE_BRAND_VERIFIED,
      IMAGE_V3_CLASS.IMAGE_BROWSER_VALIDATED,
      IMAGE_V3_CLASS.IMAGE_CERTIFIED,
    ].includes(s.v3Class)
  );
  const rightsOk = slots.every((s) => s.rightsOk === true);
  const browserOk = slots.every((s) => s.browserOk === true);
  const certified = slots.every((s) => s.v3Class === IMAGE_V3_CLASS.IMAGE_CERTIFIED);

  if (certified) return GALLERY_CERT_LAYER.SIX_IMAGES_CERTIFIED;
  if (browserOk && brandOk && rightsOk) return GALLERY_CERT_LAYER.SIX_IMAGES_BROWSER_VALIDATED;
  if (brandOk && rightsOk) return GALLERY_CERT_LAYER.SIX_IMAGES_USAGE_APPROVED;
  if (brandOk) return GALLERY_CERT_LAYER.SIX_IMAGES_BRAND_VERIFIED;
  if (techOk) return GALLERY_CERT_LAYER.SIX_IMAGES_TECHNICALLY_VALID;
  return GALLERY_CERT_LAYER.SIX_IMAGES_PRESENT;
}

export function evaluateScenarioCertLayer(scenarioSlotResults = []) {
  const slots = scenarioSlotResults.filter(Boolean);
  if (slots.length < 3) return null;
  const present = slots.every((s) => s.v3Class !== IMAGE_V3_CLASS.IMAGE_MISSING);
  if (!present) return null;
  const techOk = slots.every((s) =>
    [
      IMAGE_V3_CLASS.IMAGE_TECHNICALLY_VALID,
      IMAGE_V3_CLASS.IMAGE_BRAND_VERIFIED,
      IMAGE_V3_CLASS.IMAGE_BROWSER_VALIDATED,
      IMAGE_V3_CLASS.IMAGE_CERTIFIED,
    ].includes(s.v3Class)
  );
  const brandOk = slots.every((s) =>
    [
      IMAGE_V3_CLASS.IMAGE_BRAND_VERIFIED,
      IMAGE_V3_CLASS.IMAGE_BROWSER_VALIDATED,
      IMAGE_V3_CLASS.IMAGE_CERTIFIED,
    ].includes(s.v3Class)
  );
  const rightsOk = slots.every((s) => s.rightsOk === true);
  const browserOk = slots.every((s) => s.browserOk === true);
  const certified = slots.every((s) => s.v3Class === IMAGE_V3_CLASS.IMAGE_CERTIFIED);

  if (certified) return SCENARIO_CERT_LAYER.THREE_IMAGES_CERTIFIED;
  if (browserOk && brandOk && rightsOk) return SCENARIO_CERT_LAYER.THREE_IMAGES_BROWSER_VALIDATED;
  if (brandOk && rightsOk) return SCENARIO_CERT_LAYER.THREE_IMAGES_USAGE_APPROVED;
  if (brandOk) return SCENARIO_CERT_LAYER.THREE_IMAGES_BRAND_VERIFIED;
  if (techOk) return SCENARIO_CERT_LAYER.THREE_IMAGES_TECHNICALLY_VALID;
  return SCENARIO_CERT_LAYER.THREE_IMAGES_PRESENT;
}

export default {
  BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3,
  IMAGE_V3_CLASS,
  GALLERY_CERT_LAYER,
  SCENARIO_CERT_LAYER,
  mapV2ClassToV3,
  evaluateGalleryCertLayer,
  evaluateScenarioCertLayer,
};
