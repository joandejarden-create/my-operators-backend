/**
 * GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1
 *
 * One versioned contract for the complete approved customer-facing object
 * used by tile, list, drawer, contacts, recommendations, qualification,
 * evidence, and export surfaces.
 *
 * Publication freezes this object as approvedCustomerFacingDetail.
 * EXTERNAL_SHARE / auth presentation must preserve it — never rebuild from a
 * lean list DTO after publish.
 */
export const GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1 =
  "gdi-canonical-customer-presentation-contract-v1";

/** Admin / portfolio status vocabulary (exact strings). */
export const GDI_FULL_CONTENT_PARITY_STATUS = Object.freeze({
  VERIFIED: "VERIFIED",
  MISMATCH: "MISMATCH",
  NOT_PUBLISHED: "NOT_PUBLISHED",
  NOT_VERIFIABLE: "NOT_VERIFIABLE",
  VERIFICATION_PENDING: "VERIFICATION_PENDING",
});

/**
 * Required top-level keys that must survive freeze → hydrate → share detail.
 * Nested commercial content is discovered recursively by the full-content
 * schema (not a closed shortlist of leaves).
 */
export const REQUIRED_APPROVED_TOP_LEVEL_KEYS = Object.freeze([
  "id",
  "title",
  "salesActionKey",
  "salesActionPillLabel",
  "priorityFilterKey",
  "commercialInventoryClass",
  "whatIsHappening",
  "potentialHotelBusiness",
  "whyThisHotel",
  "whyActNow",
  "establishedFacts",
  "uncertainties",
  "qualificationQuestions",
  "recommendedNextStep",
  "primaryContact",
  "sources",
]);

/**
 * Fail closed: published snapshot must carry the frozen approved object.
 */
export function assertApprovedCustomerFacingDetailPresent(snapshot = {}) {
  const fails = [];
  const approved = snapshot?.approvedCustomerFacingDetail;
  if (!approved || typeof approved !== "object") {
    fails.push("approvedCustomerFacingDetail:missing");
    return {
      ok: false,
      contractVersion: GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
      fails,
      schemaReady: false,
    };
  }
  for (const key of REQUIRED_APPROVED_TOP_LEVEL_KEYS) {
    const v = approved[key];
    if (v == null || v === "") {
      fails.push(`approvedCustomerFacingDetail.${key}:missing`);
    } else if (Array.isArray(v) && v.length === 0 && key !== "uncertainties") {
      // Empty qualificationQuestions / facts / sources are substantive gaps.
      if (
        key === "establishedFacts" ||
        key === "qualificationQuestions" ||
        key === "sources"
      ) {
        fails.push(`approvedCustomerFacingDetail.${key}:empty`);
      }
    }
  }
  // Nested objects/arrays must survive freeze — reject lean list projections.
  if (Array.isArray(approved) || typeof approved !== "object") {
    fails.push("approvedCustomerFacingDetail:not_object");
  }
  const primary = approved.primaryContact;
  if (!primary || typeof primary !== "object" || !String(primary.name || "").trim()) {
    fails.push("approvedCustomerFacingDetail.primaryContact.name:missing");
  }
  if (snapshot.publishedPresentationLocked !== true) {
    fails.push("publishedPresentationLocked:required");
  }
  return {
    ok: fails.length === 0,
    contractVersion: GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
    fails,
    schemaReady: fails.length === 0,
  };
}

/**
 * Validate a complete approved object before publication freeze commits.
 * Fail closed if required customer-facing information is lost.
 */
export function validateCanonicalPresentationObject(approved = {}, opts = {}) {
  const fakeSnap = {
    approvedCustomerFacingDetail: approved,
    publishedPresentationLocked:
      opts.publishedPresentationLocked === false ? false : true,
  };
  return assertApprovedCustomerFacingDetailPresent(fakeSnap);
}
