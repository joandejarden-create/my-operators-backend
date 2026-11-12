/**
 * Presentation sanitizer — remove unsafe open-RFP / open-lodging copy when
 * target-hotel sourcing is not confirmed. Applied after ledger remediation.
 */

import {
  TARGET_HOTEL_SOURCING_STATUS,
  normalizeTargetHotelSourcingFromLegacy,
  recommendCommercialAction,
} from "./commercial-status-dimensions-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

const OPEN_RFP_ACTION =
  /request rfp|rfp\s*\/\s*site-?visit|site-visit inclusion|request RFP \/ site-visit/i;
const OPEN_PATH_NARRATIVE =
  /lodging demand path open|lodging still open|housing open(?!\s*announcement)|overflow\s*\/\s*housing opportunity|overflow housing opportunity(?!\s*(unconfirmed|not|hypothesis))|this overflow opportunity|potential overflow \/ housing/i;

/**
 * Rewrite recommended actions that ask for RFPs without confirmed open/overflow sourcing.
 */
export function sanitizeRecommendedActionForSourcing(opp = {}) {
  const sourcing = normalizeTargetHotelSourcingFromLegacy(opp);
  let action = text(opp.recommendedAction || opp.suggestedAction);
  if (!action) return opp;

  const sourcingConfirmed =
    sourcing === TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED ||
    sourcing === TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED;
  const overflowConfirmed =
    sourcing === TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED;

  const reasons = [];
  if (!overflowConfirmed && /this overflow opportunity|overflow opportunity/i.test(action)) {
    action = action.replace(
      /this overflow opportunity/gi,
      "this hypothesized overflow path (unconfirmed)"
    );
    action = action.replace(
      /overflow opportunity/gi,
      "overflow hypothesis (unconfirmed)"
    );
    reasons.push("OVERFLOW_CLAIM_WITHOUT_OVERFLOW_EVIDENCE");
  }

  if (!sourcingConfirmed && OPEN_RFP_ACTION.test(action)) {
    const contact =
      opp.primaryContact?.email ||
      opp.primaryContact?.name ||
      opp.organizationContactRoute ||
      "";
    const safe = recommendCommercialAction({
      competitorVenueStatus: opp.competitorVenueStatus,
      targetHotelSourcingStatus: sourcing,
      commercialActionability:
        opp.commercialActionability || opp.cmiCommercialActionability,
      competitorHotel: opp.competitorHotel || opp.likelyCompetitor || "",
      contactHint: contact,
    });
    action = /verify|confirm|qualify|monitor/i.test(safe)
      ? safe
      : contact
        ? `Contact ${contact} to confirm whether venue/housing sourcing is still open for this edition before any RFP request — do not claim open sourcing without evidence.`
        : "Confirm whether venue/housing sourcing is still open for this edition before any RFP request — do not claim open sourcing without evidence.";
    reasons.push("OPEN_RFP_WITHOUT_CONFIRMED_SOURCING");
  }

  if (!reasons.length && action === text(opp.recommendedAction || opp.suggestedAction)) {
    return opp;
  }

  return {
    ...opp,
    recommendedAction: action,
    suggestedAction: action,
    commercialPresentationSanitized: true,
    commercialPresentationSanitizeReasons: [
      ...(opp.commercialPresentationSanitizeReasons || []),
      ...reasons,
    ],
  };
}

/**
 * Soften affirmative open-lodging narrative when sourcing/overflow is unconfirmed.
 */
export function sanitizeOpenLodgingNarrative(opp = {}) {
  const sourcing = normalizeTargetHotelSourcingFromLegacy(opp);
  const sourcingConfirmed =
    sourcing === TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED ||
    sourcing === TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED;

  if (sourcingConfirmed) return opp;

  const fields = [
    "whyNow",
    "summaryWhat",
    "summaryWhyMatters",
    "summaryWhyHotel",
    "lodgingEvidence",
    "fitExplanation",
  ];
  let changed = false;
  const next = { ...opp };
  for (const key of fields) {
    const val = text(next[key]);
    if (!val || !OPEN_PATH_NARRATIVE.test(val)) continue;
    next[key] = val
      .replace(/lodging demand path open\.?/gi, "lodging demand path not established.")
      .replace(/lodging still open/gi, "lodging status still unconfirmed")
      .replace(/housing open(?!\s*announcement)/gi, "housing status unconfirmed")
      .replace(
        /overflow\s*\/\s*housing opportunity/gi,
        "overflow/housing hypothesis (unconfirmed)"
      )
      .replace(
        /overflow housing opportunity(?!\s*(unconfirmed|not))/gi,
        "overflow housing hypothesis (unconfirmed)"
      )
      .replace(
        /potential overflow \/ housing opportunity/gi,
        "potential overflow/housing hypothesis (unconfirmed)"
      )
      .replace(
        /this overflow opportunity/gi,
        "this hypothesized overflow path (unconfirmed)"
      );
    changed = true;
  }
  if (!changed) return opp;
  return {
    ...next,
    commercialPresentationSanitized: true,
    commercialPresentationSanitizeReasons: [
      ...(opp.commercialPresentationSanitizeReasons || []),
      "OPEN_LODGING_NARRATIVE_WITHOUT_SOURCING",
    ],
  };
}

/**
 * Soften overflow-as-opportunity language when overflow is not confirmed.
 */
export function sanitizeOverflowPresentation(opp = {}) {
  const sourcing = normalizeTargetHotelSourcingFromLegacy(opp);
  if (sourcing === TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED) return opp;

  const title = text(opp.title);
  if (!/overflow play|overflow opportunity(?!\s*\()/i.test(title)) return opp;

  return {
    ...opp,
    title: title
      .replace(/\s*[—-]\s*overflow play/i, " — competitor host (overflow unconfirmed)")
      .replace(/overflow opportunity/i, "overflow hypothesis (unconfirmed)"),
    opportunityName: undefined,
    commercialPresentationSanitized: true,
    commercialPresentationSanitizeReasons: [
      ...(opp.commercialPresentationSanitizeReasons || []),
      "OVERFLOW_CLAIM_WITHOUT_OVERFLOW_EVIDENCE",
    ],
  };
}

export function sanitizeCommercialPresentation(opp = {}) {
  let next = sanitizeRecommendedActionForSourcing(opp);
  next = sanitizeOpenLodgingNarrative(next);
  next = sanitizeOverflowPresentation(next);
  if (next.opportunityName === undefined && next.title) {
    next.opportunityName = next.title;
  }
  return next;
}
