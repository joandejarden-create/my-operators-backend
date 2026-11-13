/**
 * Presentation sanitizer — remove unsafe open-RFP / open-lodging copy when
 * target-hotel sourcing is not confirmed. Applied after ledger remediation.
 */

import {
  TARGET_HOTEL_SOURCING_STATUS,
  normalizeTargetHotelSourcingFromLegacy,
  recommendCommercialAction,
  composeCustomerFacingHotelSourcingLabel,
} from "./commercial-status-dimensions-v1.js";
import { applyCompetitiveFutureCyclePresentation } from "./competitive-future-cycle-presentation-v1.js";
import { applySemanticCommercialRemediation } from "./gdi-semantic-commercial-remediation-v1.js";
import { applyCustomerFacingOpportunityTitleContract } from "../customer-facing-opportunity-title-contract-v1.js";

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
  const summaryWhat = text(opp.summaryWhat);
  const whyNow = text(opp.whyNow);
  const titleNeeds =
    /overflow play|overflow opportunity(?!\s*\()|\/\s*overflow housing|\boverflow housing\b/i.test(
      title
    );
  const proseNeeds =
    /\boverflow housing\b(?!\s*(hypothesis|\(unconfirmed\)|unconfirmed))/i.test(
      `${summaryWhat} ${whyNow}`
    );
  if (!titleNeeds && !proseNeeds) return opp;

  const next = { ...opp };
  if (titleNeeds) {
    next.title = title
      .replace(/\s*[—-]\s*overflow play/i, " — competitor host (overflow unconfirmed)")
      .replace(/overflow opportunity/i, "overflow hypothesis (unconfirmed)")
      .replace(/\s*\/\s*Overflow Housing\b/i, " — overflow hypothesis (unconfirmed)")
      .replace(/\bOverflow Housing\b/i, "overflow hypothesis (unconfirmed)");
    next.opportunityName = undefined;
  }
  if (proseNeeds) {
    if (summaryWhat) {
      next.summaryWhat = summaryWhat
        .replace(/\s*\/\s*Overflow Housing\b/i, " — overflow hypothesis (unconfirmed)")
        .replace(/\bOverflow Housing\b/i, "overflow hypothesis (unconfirmed)");
    }
  }

  return {
    ...next,
    commercialPresentationSanitized: true,
    commercialPresentationSanitizeReasons: [
      ...(opp.commercialPresentationSanitizeReasons || []),
      "OVERFLOW_CLAIM_WITHOUT_OVERFLOW_EVIDENCE",
    ],
  };
}

/**
 * Strip repeated action-label prefixes ("Qualify Overflow — Qualify Overflow — …")
 * and malformed template glue ("dated in Host hotel contracted").
 */
export function sanitizeDuplicatedActionNarrative(opp = {}) {
  const fields = [
    "recommendedAction",
    "suggestedAction",
    "whyNow",
    "summaryWhat",
    "summaryWhyMatters",
    "summaryWhyHotel",
    "cardWhyNowLine",
    "whatIsHappening",
    // Thesis / fit fields also ingested the Host-hotel-contracted template glue
    "whyThisHotel",
    "hotelOpportunityThesis",
    "whyHotelDemand",
    "potentialBusinessForThisHotel",
    "fitExplanation",
    "lodgingEvidence",
    "tileCustomerSummary",
    "occasionSummary",
    "commercialSummary",
  ];
  const next = { ...opp };
  let changed = false;
  const ACTION_PREFIX =
    /^(?:Qualify Overflow|Monitor Future Cycle|Develop Organizer Relationship|Pursue Open Business|Pursue Now)\s*[—–-]\s*/i;

  function scrubMalformedHostContractedGlue(val) {
    return String(val || "")
      // Malformed template: "{event} is dated in Host hotel contracted — {venue} with hotel sourcing unconfirmed…"
      .replace(
        /\bis\s+dated\s+in\s+Host\s+hotel\s+contracted\s*[—–-]\s*([^.—–-]+?)\s+with\s+hotel\s+sourcing\s+unconfirmed[^.]*\.?/gi,
        (_m, venue) =>
          `has a confirmed competitor host at ${String(venue).trim()}; target-hotel overflow sourcing remains unconfirmed.`
      )
      .replace(
        /\bis\s+Host\s+hotel\s+contracted\s*[—–-]\s*([^.—–-]+?)\s+with\s+hotel\s+sourcing\s+unconfirmed[^.]*\.?/gi,
        (_m, venue) =>
          `has a confirmed competitor host at ${String(venue).trim()}; target-hotel overflow sourcing remains unconfirmed.`
      )
      .replace(
        /\bis\s+dated\s+in\s+Host\s+hotel\s+contracted\s*[—–-]\s*/gi,
        "has a confirmed competitor host at "
      )
      .replace(/\bdated in Host hotel contracted\b/gi, "hosted with competitor venue confirmed")
      .replace(/\bHost hotel contracted\s*[—–-]\s*Host hotel contracted\b/gi, "Host hotel contracted")
      .replace(/\b(Qualify Overflow\s*[—–-]\s*){2,}/gi, "")
      .replace(/\s{2,}/g, " ")
      .trim();
  }

  for (const key of fields) {
    let val = text(next[key]);
    if (!val) continue;
    const before = val;
    // Collapse repeated leading action labels
    let guard = 0;
    while (guard < 4 && ACTION_PREFIX.test(val)) {
      const stripped = val.replace(ACTION_PREFIX, "").trim();
      if (stripped === val) break;
      // Keep a single semantic sentence — do not re-prefix
      val = stripped;
      guard += 1;
    }
    val = scrubMalformedHostContractedGlue(val);
    if (val !== before) {
      next[key] = val;
      changed = true;
    }
  }

  // Nested commercial intelligence / sales brief / hotel-relevance thesis fields
  for (const nestKey of ["commercialIntelligenceBrief", "salesBrief"]) {
    const nest = next[nestKey];
    if (!nest || typeof nest !== "object") continue;
    const nestedNext = { ...nest };
    let nestChanged = false;
    for (const key of [
      "whyThisHotel",
      "hotelOpportunityThesis",
      "whyHotelDemand",
      "whatIsHappening",
      "summaryWhat",
      "recommendedAction",
      "whyNow",
    ]) {
      const before = text(nestedNext[key]);
      if (!before) continue;
      const after = scrubMalformedHostContractedGlue(before);
      if (after !== before) {
        nestedNext[key] = after;
        nestChanged = true;
      }
    }
    if (nestChanged) {
      next[nestKey] = nestedNext;
      changed = true;
    }
  }

  const hrr = next.hotelRelevanceResearch;
  if (hrr && typeof hrr === "object") {
    const hrrNext = { ...hrr };
    let hrrChanged = false;
    const mech = hrr.hotelBusinessMechanism;
    if (mech && typeof mech === "object") {
      const before = text(mech.text);
      const after = scrubMalformedHostContractedGlue(before);
      if (after && after !== before) {
        hrrNext.hotelBusinessMechanism = { ...mech, text: after };
        hrrChanged = true;
      }
    }
    for (const key of ["summary", "narrative", "whyHotelDemand"]) {
      const before = text(hrrNext[key]);
      if (!before) continue;
      const after = scrubMalformedHostContractedGlue(before);
      if (after !== before) {
        hrrNext[key] = after;
        hrrChanged = true;
      }
    }
    if (hrrChanged) {
      next.hotelRelevanceResearch = hrrNext;
      changed = true;
    }
  }
  if (!changed) return opp;
  return {
    ...next,
    commercialPresentationSanitized: true,
    commercialPresentationSanitizeReasons: [
      ...(opp.commercialPresentationSanitizeReasons || []),
      "DUPLICATED_ACTION_OR_MALFORMED_NARRATIVE",
    ],
  };
}

export function sanitizeCommercialPresentation(opp = {}) {
  let next = sanitizeRecommendedActionForSourcing(opp);
  next = sanitizeOpenLodgingNarrative(next);
  next = sanitizeOverflowPresentation(next);
  next = sanitizeDuplicatedActionNarrative(next);
  // Shared semantic-audit P1 remediations (dedupe URLs, monitor/urgency,
  // tournament-housing leak, pipeline slug titles, whyActNow≠potentialBusiness).
  next = applySemanticCommercialRemediation(next);
  // Canonical customer title — competitor venues stay on competitorHotel fields.
  next = applyCustomerFacingOpportunityTitleContract(next);
  // Explicit competitor-vs-target sourcing line (never collapses dimensions).
  const composedSourcing = composeCustomerFacingHotelSourcingLabel(next);
  next = {
    ...next,
    hotelSourcingStatusLabel: composedSourcing,
    targetHotelSourcingStatusLabel:
      next.targetHotelSourcingStatusLabel || composedSourcing,
  };
  if (next.eventsOpportunityOverview && typeof next.eventsOpportunityOverview === "object") {
    const ov = next.eventsOpportunityOverview;
    const org = ov.organizerName || "";
    const type = ov.eventTypeLabel || next.eventTypeLabel || "";
    next = {
      ...next,
      eventsOpportunityOverview: {
        ...ov,
        hotelSourcing: composedSourcing || ov.hotelSourcing,
        tileClassificationLine: composedSourcing
          ? [org, type, composedSourcing]
              .filter((p) => p && p !== "Organizer TBD")
              .join(" · ")
          : ov.tileClassificationLine,
        location:
          /host\s+hotel\s+contracted|competitor\s+(?:venue|hotel|booking)/i.test(
            String(ov.location || "")
          )
            ? text(
                next.eventLocationSummary ||
                  next.eventLocationCity ||
                  next.eventCity ||
                  next.city
              ) || "Location TBD"
            : ov.location,
      },
    };
  }
  if (next.opportunityName === undefined && next.title) {
    next.opportunityName = next.displayTitle || next.title;
  }
  // Split confirmed competitor booking vs future-cycle opportunity presentation.
  next = applyCompetitiveFutureCyclePresentation(next);
  next = sanitizeDuplicatedActionNarrative(next);
  return next;
}
