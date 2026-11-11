/**
 * Reconcile commercial-eligibility-contract-v1 vs V41 dispositions.
 *
 * Eligibility = demand + named entry-point composition (research gate).
 * V41 = customer-readiness / contact ownership / geo disposition spine.
 * Final disposition is the more conservative customer-facing posture;
 * both stage results are preserved with an explanation. Never auto-promotes.
 */

export const GDI_COMMERCIAL_DISPOSITION_RECONCILE_V1 =
  "gdi-commercial-disposition-reconcile-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function rankDisposition(d) {
  const s = text(d).toUpperCase();
  if (!s) return 0;
  if (/EXCLUDE|INELIGIBLE/.test(s)) return 10;
  if (/WATCH/.test(s)) return 40;
  if (/ACCOUNT_DEVELOPMENT|RESEARCH_CONTINUATION/.test(s)) return 60;
  if (/ACTIVE|SALES_READY|QUALIFIED/.test(s)) return 90;
  return 30;
}

/**
 * @returns {{
 *   finalDisposition: string|null,
 *   explanation: string,
 *   conflict: boolean,
 *   eligibilityStatus: string|null,
 *   v41Disposition: string|null,
 *   promotionBlocked: boolean,
 * }}
 */
export function reconcileCommercialDispositionV1({
  eligibilityResult = null,
  v41Qualification = null,
  priorDisposition = null,
} = {}) {
  const eligibilityStatus =
    eligibilityResult?.status ||
    eligibilityResult?.commercialEligibilityStatus ||
    null;
  const v41Disposition =
    v41Qualification?.internalDisposition ||
    priorDisposition ||
    null;
  const salesReady = eligibilityResult?.salesReady === true;
  const researchContinuation =
    eligibilityResult?.researchContinuation === true ||
    eligibilityStatus === "RESEARCH_CONTINUATION";
  const commerciallyIneligible =
    eligibilityStatus === "COMMERCIALLY_INELIGIBLE";

  let finalDisposition = v41Disposition || priorDisposition || null;
  let explanation =
    "V41 customer-readiness disposition retained as primary customer-facing posture.";
  let conflict = false;
  let promotionBlocked = true;

  if (commerciallyIneligible) {
    finalDisposition = v41Disposition?.includes("EXCLUDE")
      ? v41Disposition
      : "EXCLUDE";
    explanation =
      "Commercial eligibility marked COMMERCIALLY_INELIGIBLE — final disposition excluded; V41 result preserved for audit.";
    conflict = Boolean(
      v41Disposition && !/EXCLUDE|WATCH/i.test(String(v41Disposition))
    );
  } else if (salesReady && /WATCH/i.test(String(v41Disposition || ""))) {
    conflict = true;
    finalDisposition = v41Disposition;
    explanation =
      "Eligibility SALES_READY reflects composite demand+entry-point composition, but V41 remains WATCHLIST due to contact ownership, geo-fit, or customer-readiness gates. Final disposition stays WATCHLIST — no automatic promotion.";
    promotionBlocked = true;
  } else if (salesReady && /ACCOUNT_DEVELOPMENT/i.test(String(v41Disposition || ""))) {
    conflict = false;
    finalDisposition = v41Disposition;
    explanation =
      "Eligibility SALES_READY with V41 ACCOUNT_DEVELOPMENT — AD retained as founder-facing development posture; not customer-published.";
    promotionBlocked = true;
  } else if (researchContinuation) {
    if (
      !finalDisposition ||
      /UNKNOWN/i.test(String(finalDisposition)) ||
      rankDisposition(finalDisposition) > 60
    ) {
      // Do not upgrade to sales-ready while research continues.
      if (/ACTIVE|SALES_READY/i.test(String(finalDisposition || ""))) {
        conflict = true;
        finalDisposition = "WATCHLIST_RESEARCH";
        explanation =
          "Eligibility RESEARCH_CONTINUATION overrides premature active/sales-ready V41 posture.";
      } else {
        explanation =
          "Eligibility RESEARCH_CONTINUATION — incomplete demand or contact evidence; continuation required.";
      }
    } else {
      explanation =
        "Eligibility RESEARCH_CONTINUATION aligned with non-active V41 disposition.";
    }
    promotionBlocked = true;
  } else if (
    salesReady &&
    /ACTIVE|ACCOUNT_DEVELOPMENT|SALES_READY/i.test(String(v41Disposition || ""))
  ) {
    conflict = false;
    finalDisposition = v41Disposition;
    explanation =
      "Eligibility SALES_READY aligned with V41 development/active disposition — still not auto-published.";
    promotionBlocked = true;
  }

  return {
    version: GDI_COMMERCIAL_DISPOSITION_RECONCILE_V1,
    finalDisposition: finalDisposition || "WATCHLIST_RESEARCH",
    explanation,
    conflict,
    eligibilityStatus,
    v41Disposition,
    promotionBlocked,
    at: new Date().toISOString(),
  };
}

export default {
  GDI_COMMERCIAL_DISPOSITION_RECONCILE_V1,
  reconcileCommercialDispositionV1,
};
