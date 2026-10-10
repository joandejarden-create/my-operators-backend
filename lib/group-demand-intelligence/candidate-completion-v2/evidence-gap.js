/**
 * Evidence gap map — PASS / PARTIAL / FAIL / UNKNOWN per gate family.
 */

import { classifyEntityTruth } from "../entity-truth-gate-v1.js";
import { classifyActiveDate, ACTIVE_DATE_CLASS } from "../active-eligibility-v1.js";
import { classifyWhoHowPath, WHO_PATH_CLASS } from "../opportunity-who-resolution-v1.js";
import { evaluateGdiSummaryQuality, SUMMARY_QUALITY } from "../opportunity-summary-v1.js";
import { classifyCustomerSurfaceOpportunity } from "../customer-surface-revalidation-v1.js";

export const GAP_STATE = Object.freeze({
  PASS: "PASS",
  PARTIAL: "PARTIAL",
  FAIL: "FAIL",
  UNKNOWN: "UNKNOWN",
});

export const LODGING_CLASS = Object.freeze({
  DIRECT_ROOM_BLOCK: "DIRECT_ROOM_BLOCK",
  OFFICIAL_HOUSING_PROGRAM: "OFFICIAL_HOUSING_PROGRAM",
  OFFICIAL_ACCOMMODATION_GUIDANCE: "OFFICIAL_ACCOMMODATION_GUIDANCE",
  TRAVELING_DELEGATION: "TRAVELING_DELEGATION",
  MULTI_DAY_GROUP_INFERENCE: "MULTI_DAY_GROUP_INFERENCE",
  NO_LODGING_SUPPORT: "NO_LODGING_SUPPORT",
});

export function classifyLodgingCompletion(opp = {}, pageText = "") {
  const blob = `${opp.title || ""} ${pageText} ${JSON.stringify(opp.lodgingEvidence || {})}`;
  const le = opp.lodgingEvidence || {};
  if (le.roomBlockMentioned || /\bofficial room block|host hotel block|bloc de chambres\b/i.test(blob)) {
    return LODGING_CLASS.DIRECT_ROOM_BLOCK;
  }
  if (le.housingPageFound || /\bhousing bureau|official housing|hébergement officiel|alojamiento oficial\b/i.test(blob)) {
    return LODGING_CLASS.OFFICIAL_HOUSING_PROGRAM;
  }
  if (/\/accommodation|accommodation guidance|hotel list|recommended hotels/i.test(blob)) {
    return LODGING_CLASS.OFFICIAL_ACCOMMODATION_GUIDANCE;
  }
  if (opp.teamSupported || /\bdelegation|travelling (team|party)|out[- ]of[- ]market\b/i.test(blob)) {
    return LODGING_CLASS.TRAVELING_DELEGATION;
  }
  if (
    opp.eventStartDate &&
    opp.eventEndDate &&
    String(opp.eventStartDate).slice(0, 10) !== String(opp.eventEndDate).slice(0, 10) &&
    /\b(conference|congress|summit|meeting)\b/i.test(blob)
  ) {
    return LODGING_CLASS.MULTI_DAY_GROUP_INFERENCE;
  }
  return LODGING_CLASS.NO_LODGING_SUPPORT;
}

/**
 * Build gap map for one candidate.
 */
export function buildEvidenceGapMap(opp = {}, opts = {}) {
  const entity = classifyEntityTruth(opp);
  const date = classifyActiveDate(opp, opts);
  const who = classifyWhoHowPath(opp);
  const summary = evaluateGdiSummaryQuality(opp);
  const surface = classifyCustomerSurfaceOpportunity(opp, opts);
  const lodgingClass = classifyLodgingCompletion(opp, opts.pageText || "");

  const ENTITY = entity.validEntity ? GAP_STATE.PASS : GAP_STATE.FAIL;
  let TIMING = GAP_STATE.UNKNOWN;
  if (
    date.activeDateClass === ACTIVE_DATE_CLASS.ACTIVE_FUTURE ||
    date.activeDateClass === ACTIVE_DATE_CLASS.ACTIVE_CURRENT
  ) {
    TIMING = GAP_STATE.PASS;
  } else if (date.activeDateClass === ACTIVE_DATE_CLASS.PAST_CLOSED) {
    TIMING = GAP_STATE.FAIL;
  } else if (opp.eventYear || opp.futureCycleEvidenceState) {
    TIMING = GAP_STATE.PARTIAL;
  }

  const geoTokens = opts.geoTokens || [];
  const geoBlob = `${opp.eventLocationSummary || ""} ${opp.destinationStatus || ""} ${opp.title || ""} ${opp.officialSource || ""}`.toLowerCase();
  let GEOGRAPHY = GAP_STATE.UNKNOWN;
  if (geoTokens.length && geoTokens.some((t) => geoBlob.includes(String(t).toLowerCase()))) {
    GEOGRAPHY = GAP_STATE.PASS;
  } else if (opp.discoveryMeta?.marketPlace || opp.eventLocationSummary) {
    GEOGRAPHY = GAP_STATE.PARTIAL;
  }

  let LODGING = GAP_STATE.FAIL;
  if (
    lodgingClass === LODGING_CLASS.DIRECT_ROOM_BLOCK ||
    lodgingClass === LODGING_CLASS.OFFICIAL_HOUSING_PROGRAM
  ) {
    LODGING = GAP_STATE.PASS;
  } else if (
    lodgingClass === LODGING_CLASS.OFFICIAL_ACCOMMODATION_GUIDANCE ||
    lodgingClass === LODGING_CLASS.TRAVELING_DELEGATION
  ) {
    LODGING = GAP_STATE.PARTIAL;
  } else if (lodgingClass === LODGING_CLASS.MULTI_DAY_GROUP_INFERENCE) {
    LODGING = GAP_STATE.PARTIAL;
  } else {
    LODGING = GAP_STATE.FAIL;
  }

  const fitScore = opp.hotelFitScore ?? opp.hotelFit;
  let FIT = GAP_STATE.UNKNOWN;
  if (fitScore != null && Number(fitScore) >= 45) FIT = GAP_STATE.PASS;
  else if (opp.hotelOpportunityThesis || opp.summaryWhyHotel) FIT = GAP_STATE.PARTIAL;
  else FIT = GAP_STATE.FAIL;

  let PLACEMENT = GAP_STATE.UNKNOWN;
  const vs = String(opp.venueStatus || "");
  if (/FULLY_PLACED|PRIMARY_NO_OVERFLOW/i.test(vs)) PLACEMENT = GAP_STATE.FAIL;
  else if (vs && !/^UNKNOWN$/i.test(vs)) PLACEMENT = GAP_STATE.PARTIAL;
  else PLACEMENT = GAP_STATE.UNKNOWN;

  let WHO = GAP_STATE.FAIL;
  if (who.pathClass === WHO_PATH_CLASS.NAMED_DIRECT || who.pathClass === WHO_PATH_CLASS.NAMED_PARTIAL) {
    WHO = GAP_STATE.PASS;
  } else if (who.pathClass === WHO_PATH_CLASS.FUNCTIONAL || who.pathClass === WHO_PATH_CLASS.ORG_PATH) {
    WHO = GAP_STATE.PARTIAL;
  } else if (who.pathClass === WHO_PATH_CLASS.NO_CONTACT_AFTER_RESEARCH) {
    WHO = GAP_STATE.PARTIAL;
  } else WHO = GAP_STATE.FAIL;

  let CONTACT = WHO;
  if (opp.organizationContactUrl || opp.officialContactPath || opp.functionalContactEmail) {
    CONTACT = GAP_STATE.PASS;
  }

  let SUMMARY = GAP_STATE.FAIL;
  if (summary.quality === SUMMARY_QUALITY.STRONG || summary.quality === SUMMARY_QUALITY.ADEQUATE) {
    SUMMARY = GAP_STATE.PASS;
  } else if (summary.quality === SUMMARY_QUALITY.THIN) SUMMARY = GAP_STATE.PARTIAL;

  const SURFACE = surface.keepActive ? GAP_STATE.PASS : GAP_STATE.FAIL;

  const gaps = { ENTITY, TIMING, GEOGRAPHY, LODGING, FIT, PLACEMENT, WHO, CONTACT, SUMMARY, SURFACE };
  const failKeys = Object.entries(gaps)
    .filter(([, v]) => v === GAP_STATE.FAIL || v === GAP_STATE.UNKNOWN)
    .map(([k]) => k);

  // Smallest missing fact heuristic
  let smallestMissingFact = null;
  if (gaps.TIMING !== GAP_STATE.PASS) smallestMissingFact = "next_confirmed_or_expected_cycle_date";
  else if (gaps.LODGING === GAP_STATE.FAIL) smallestMissingFact = "official_housing_or_room_block_evidence";
  else if (gaps.WHO === GAP_STATE.FAIL && gaps.CONTACT !== GAP_STATE.PASS) {
    smallestMissingFact = "organizer_or_housing_contact_path";
  } else if (gaps.SURFACE !== GAP_STATE.PASS) smallestMissingFact = "hotel_motion_thesis_or_lodging_stamp";
  else if (gaps.SUMMARY !== GAP_STATE.PASS) smallestMissingFact = "adequate_customer_summary";
  else if (failKeys[0]) smallestMissingFact = failKeys[0].toLowerCase();

  return {
    gaps,
    lodgingClass,
    smallestMissingFact,
    failOrUnknownCount: failKeys.length,
    entityPass: ENTITY === GAP_STATE.PASS,
    geographyPass: GEOGRAPHY === GAP_STATE.PASS || GEOGRAPHY === GAP_STATE.PARTIAL,
    lodgingHint: LODGING === GAP_STATE.PARTIAL || LODGING === GAP_STATE.PASS,
  };
}
