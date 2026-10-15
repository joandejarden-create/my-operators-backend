/**
 * Exact-property image evidence. Unverified is not wrong.
 * A same-brand donor image does not satisfy property identity.
 */

export const IMAGE_EVIDENCE_STATES = Object.freeze([
  "VERIFIED_EXACT_PROPERTY",
  "VERIFIED_WRONG_PROPERTY",
  "UNVERIFIED_PROPERTY_IDENTITY",
  "CROSS_PROPERTY_DUPLICATE",
  "MISSING_PROPERTY_IMAGE",
  "APPROVED_NO_IMAGE",
  "PROPERTY_IMAGE_RESEARCH_REQUIRED",
  "NON_PROPERTY_VISUAL",
]);

const ACCEPTABLE = new Set(["VERIFIED_EXACT_PROPERTY", "APPROVED_NO_IMAGE"]);

export function classifyPropertyImage(property = {}) {
  if (property.sameBrandDonor === true) {
    return { state: "UNVERIFIED_PROPERTY_IDENTITY", acceptable: false, reason: "same_brand_donor" };
  }
  const state = property.imageEvidence || "PROPERTY_IMAGE_RESEARCH_REQUIRED";
  if (!IMAGE_EVIDENCE_STATES.includes(state)) {
    return { state: "PROPERTY_IMAGE_RESEARCH_REQUIRED", acceptable: false, reason: "unknown_state" };
  }
  if (state === "VERIFIED_WRONG_PROPERTY" || state === "CROSS_PROPERTY_DUPLICATE" || state === "MISSING_PROPERTY_IMAGE") {
    return { state, acceptable: false, reason: state };
  }
  if (state === "UNVERIFIED_PROPERTY_IDENTITY" || state === "PROPERTY_IMAGE_RESEARCH_REQUIRED" || state === "NON_PROPERTY_VISUAL") {
    return { state, acceptable: false, reason: state };
  }
  return { state, acceptable: ACCEPTABLE.has(state), reason: null };
}
