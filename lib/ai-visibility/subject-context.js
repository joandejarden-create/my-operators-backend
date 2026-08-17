/**
 * Analytical subject normalization for AI Visibility / AI Recommendation Intelligence.
 *
 * Brand / Operator → AI Visibility subjects
 * Owner → deal | hotel_asset (AI Recommendation Intelligence — not company visibility)
 */

export const SUBJECT_TYPES = Object.freeze({
  BRAND: "brand",
  BRAND_PORTFOLIO: "brand_portfolio",
  OPERATOR: "operator",
  DEAL: "deal",
  HOTEL_ASSET: "hotel_asset",
});

export const SUBJECT_CONTEXT_VERSION = "ai_visibility_subject_context_v1";

const REC_ID = /^rec[a-zA-Z0-9]{5,}$/;

function isRecId(v) {
  return typeof v === "string" && REC_ID.test(v.trim());
}

/**
 * @param {object} input
 * @returns {{ ok: true, subject: object } | { ok: false, reasonCode: string, error: string }}
 */
export function normalizeAiVisibilitySubject(input) {
  if (!input || typeof input !== "object") {
    return { ok: false, reasonCode: "INVALID_SUBJECT", error: "subject_required" };
  }

  const subjectType = String(input.subjectType || "").trim().toLowerCase();
  if (!Object.values(SUBJECT_TYPES).includes(subjectType)) {
    return {
      ok: false,
      reasonCode: "SUBJECT_TYPE_UNSUPPORTED",
      error: `unsupported_subject_type:${subjectType || "empty"}`,
    };
  }

  const subject = {
    subjectContextVersion: SUBJECT_CONTEXT_VERSION,
    subjectType,
    subjectEntityId: null,
    subjectCompanyId: null,
    subjectDealId: null,
    subjectHotelId: null,
    canonicalName: input.canonicalName ? String(input.canonicalName).trim() : null,
  };

  if (subjectType === SUBJECT_TYPES.BRAND) {
    const id = String(input.subjectEntityId || input.brandId || "").trim();
    if (!isRecId(id)) {
      return { ok: false, reasonCode: "INVALID_SUBJECT", error: "brand_subject_entity_id_required" };
    }
    subject.subjectEntityId = id;
  } else if (subjectType === SUBJECT_TYPES.BRAND_PORTFOLIO) {
    const companyId = String(input.subjectCompanyId || input.companyId || "").trim();
    if (!isRecId(companyId)) {
      return {
        ok: false,
        reasonCode: "INVALID_SUBJECT",
        error: "brand_portfolio_subject_company_id_required",
      };
    }
    subject.subjectCompanyId = companyId;
  } else if (subjectType === SUBJECT_TYPES.OPERATOR) {
    const id = String(input.subjectEntityId || input.operatorId || "").trim();
    if (!isRecId(id)) {
      return { ok: false, reasonCode: "INVALID_SUBJECT", error: "operator_subject_entity_id_required" };
    }
    subject.subjectEntityId = id;
  } else if (subjectType === SUBJECT_TYPES.DEAL) {
    const dealId = String(input.subjectDealId || input.dealId || "").trim();
    if (!isRecId(dealId)) {
      return { ok: false, reasonCode: "INVALID_SUBJECT", error: "deal_subject_id_required" };
    }
    subject.subjectDealId = dealId;
  } else if (subjectType === SUBJECT_TYPES.HOTEL_ASSET) {
    const hotelId = String(input.subjectHotelId || input.hotelId || "").trim();
    const dealId = String(input.subjectDealId || input.dealId || "").trim();
    if (!isRecId(hotelId) && !isRecId(dealId)) {
      return {
        ok: false,
        reasonCode: "INVALID_SUBJECT",
        error: "hotel_asset_requires_hotel_or_deal_id",
      };
    }
    if (isRecId(hotelId)) subject.subjectHotelId = hotelId;
    if (isRecId(dealId)) subject.subjectDealId = dealId;
  }

  return { ok: true, subject };
}
