/**
 * ADP_ATOMIC_REPORT_VERSION_LOADING_V1 — Core↔BPP pairing helpers.
 * Customer reports must serve the BPP embedded in the same Core artifact.
 */

import { applyBppCustomerPresentationV2 } from "./bpp-customer-presentation-v2.js";

export const ADP_ATOMIC_REPORT_VERSION_LOADING_V1 =
  "ADP_ATOMIC_REPORT_VERSION_LOADING_V1";

export function isEmbeddedCertifiedLoyaltyBpp(bpp) {
  if (!bpp || typeof bpp !== "object") return false;
  return (
    bpp._bppPackSource === "loyalty_bpp_staged" ||
    bpp._bppPackSource === "loyalty_bpp_comparable_cohort_rank" ||
    bpp.measurement?.measurementFamily === "LOYALTY_BPP" ||
    bpp.measurementFamily === "LOYALTY_BPP" ||
    bpp.bppCustomerState === "BPP_COMPARISON_CERTIFIED" ||
    bpp.bppMeasurementState === "BPP_COMPARISON_CERTIFIED" ||
    Boolean(bpp._bppLoyaltyStagedVersionId)
  );
}

/**
 * Prefer the BPP baked into the Core report artifact (atomic pairing).
 */
export function preferEmbeddedLoyaltyBppForAtomicReport(
  embeddedBpp,
  { reportVersion = "published" } = {}
) {
  if (!isEmbeddedCertifiedLoyaltyBpp(embeddedBpp)) return null;
  return applyBppCustomerPresentationV2({
    ...embeddedBpp,
    customerPublished:
      reportVersion === "published" ? true : embeddedBpp.customerPublished === true,
    measurement: {
      ...(embeddedBpp.measurement || {}),
      customerPublished:
        reportVersion === "published"
          ? true
          : embeddedBpp.measurement?.customerPublished === true,
      stagedReviewOnly:
        reportVersion === "published"
          ? false
          : embeddedBpp.measurement?.stagedReviewOnly === true,
    },
    _bppSelectionPolicy:
      reportVersion === "published"
        ? "PUBLISHED_ATOMIC_EMBEDDED_LOYALTY_BPP_PAIRED_WITH_CORE"
        : "STAGED_ATOMIC_EMBEDDED_LOYALTY_BPP_PAIRED_WITH_CORE",
    _bppAtomicPairing: true,
  });
}
