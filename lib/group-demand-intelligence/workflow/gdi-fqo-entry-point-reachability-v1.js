/**
 * Audited rule: when a verified non-buyer entry point may satisfy WHO/HOW
 * reachability for strict FQO — without relaxing unrelated six-rule gates.
 *
 * Aligns with Scaling Companies relevant-entry-point policy:
 * commercially relevant named person + person-specific outreach route +
 * credible pursuit action; buyer status must remain accurate (not DIRECT_BUYER).
 */

import { WHO_HOW_STATUS } from "./gdi-who-how-agent-contract-v1.js";
import { evaluatePersonSpecificOutreachChannel } from "./gdi-finalized-qualified-opportunity-contract-v1.js";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";

export const GDI_FQO_ENTRY_POINT_REACHABILITY_V1 =
  "gdi-fqo-entry-point-reachability-v1";

export const ENTRY_POINT_FQO_RULE_ID =
  "FQO_NAMED_REACHABILITY_ALLOWS_VERIFIED_ENTRY_POINT_V1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Evaluate whether WHO/HOW named-person + channel gates can pass via entry point.
 *
 * @returns {{
 *   ok: boolean,
 *   ruleId: string,
 *   whoHowStatus: string|null,
 *   buyerLabelMustRemain: string,
 *   reasons: string[],
 *   satisfiesNamedPersonGate: boolean,
 *   satisfiesChannelGate: boolean,
 * }}
 */
export function evaluateEntryPointFqoReachability(opportunity = {}, opts = {}) {
  const reasons = [];
  const whoHow = opportunity.whoHowResolution || opts.whoHow || null;
  const status = text(
    opportunity.whoHowStatus || whoHow?.whoHowStatus || "",
  ).toUpperCase();

  const primary = opportunity.primaryContact || {};
  const name = text(primary.name || opportunity.contactName);
  const buyerVsEntry = text(
    opportunity.buyerVsEntry || primary.buyerVsEntry || "",
  ).toUpperCase();
  const authority =
    primary.hotelBuyingAuthorityVerified === true ||
    opportunity.hotelBuyingAuthorityVerified === true;

  const allowedStatuses = new Set([
    WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER,
    WHO_HOW_STATUS.VERIFIED_BUYER_INFLUENCER,
    WHO_HOW_STATUS.VERIFIED_ENTRY_POINTS_2,
    WHO_HOW_STATUS.VERIFIED_ENTRY_POINT_1,
  ]);

  if (!allowedStatuses.has(status) && !opts.allowLegacyEntryPointClass) {
    // Legacy path: RELEVANT_COMMERCIAL_ENTRY_POINT without V3 status
    if (
      buyerVsEntry === "RELEVANT_COMMERCIAL_ENTRY_POINT" ||
      primary.contactClass === "RELEVANT_COMMERCIAL_ENTRY_POINT"
    ) {
      // continue
    } else {
      reasons.push("WHO_HOW_STATUS_NOT_ENTRY_OR_BUYER");
      return fail(status, reasons);
    }
  }

  if (!looksLikePersonName(name)) {
    reasons.push("NAMED_PERSON_REQUIRED");
  }

  if (authority && status !== WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER) {
    reasons.push("AUTHORITY_FLAG_INCONSISTENT_WITH_STATUS");
  }

  if (
    status === WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER &&
    authority !== true
  ) {
    reasons.push("DIRECT_BUYER_REQUIRES_AUTHORITY_VERIFIED");
  }

  // Entry points must not be mislabeled as DIRECT_BUYER
  if (
    (status === WHO_HOW_STATUS.VERIFIED_ENTRY_POINT_1 ||
      status === WHO_HOW_STATUS.VERIFIED_ENTRY_POINTS_2 ||
      status === WHO_HOW_STATUS.VERIFIED_BUYER_INFLUENCER) &&
    buyerVsEntry === "DIRECT_BUYER"
  ) {
    reasons.push("ENTRY_POINT_MISLABELED_AS_DIRECT_BUYER");
  }

  const commerciallyRelevant =
    primary.commerciallyRelevant !== false &&
    primary.contactClass !== "UNRELATED" &&
    primary.unrelatedToHotelSourcing !== true;

  if (!commerciallyRelevant) {
    reasons.push("ENTRY_POINT_NOT_COMMERCIALLY_RELEVANT");
  }

  const why =
    text(primary.whyThisContact) ||
    text(primary.relationshipToOpportunity) ||
    text(opportunity.whoPrimaryReason);
  if (
    status !== WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER &&
    why.length < 12
  ) {
    reasons.push("MISSING_WHY_ENTRY_POINT_RELEVANT");
  }

  const action =
    text(primary.recommendedContactAction) ||
    text(opportunity.recommendedContactAction) ||
    text(opportunity.recommendedSalesAction) ||
    text(opportunity.hotelSpecificAction);
  if (action.length < 8) {
    reasons.push("MISSING_CREDIBLE_PURSUIT_ACTION");
  }

  const channelEval = evaluatePersonSpecificOutreachChannel(opportunity, opts);
  if (!channelEval.ok) {
    reasons.push("PERSON_SPECIFIC_CHANNEL_REQUIRED");
  }

  // Unrelated people fail even with LinkedIn
  if (opts.forceUnrelated === true) {
    reasons.push("ENTRY_POINT_UNRELATED_TO_HOTEL_SOURCING");
  }

  const ok = reasons.length === 0;
  return {
    version: GDI_FQO_ENTRY_POINT_REACHABILITY_V1,
    ruleId: ENTRY_POINT_FQO_RULE_ID,
    ok,
    whoHowStatus: status || null,
    buyerLabelMustRemain:
      status === WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER
        ? "DIRECT_BUYER"
        : "RELEVANT_COMMERCIAL_ENTRY_POINT",
    satisfiesNamedPersonGate: looksLikePersonName(name) && commerciallyRelevant,
    satisfiesChannelGate: channelEval.ok === true,
    channelEval,
    reasons,
    note:
      "Entry-point reachability may satisfy NAMED_COMMERCIAL_PERSON + PERSON_SPECIFIC_CHANNEL only. Demand, hotel fit, future pathway, and hotel-specific action remain independent.",
  };
}

function fail(status, reasons) {
  return {
    version: GDI_FQO_ENTRY_POINT_REACHABILITY_V1,
    ruleId: ENTRY_POINT_FQO_RULE_ID,
    ok: false,
    whoHowStatus: status || null,
    buyerLabelMustRemain: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    satisfiesNamedPersonGate: false,
    satisfiesChannelGate: false,
    channelEval: null,
    reasons,
    note:
      "Entry-point reachability may satisfy NAMED_COMMERCIAL_PERSON + PERSON_SPECIFIC_CHANNEL only. Demand, hotel fit, future pathway, and hotel-specific action remain independent.",
  };
}

/**
 * Patch FQO gate evaluation context: if entry-point rule passes, named+channel
 * gates can be considered satisfied for commercial FQO (other gates untouched).
 */
export function mergeEntryPointReachabilityIntoFqoGates(fqoResult, reachability) {
  if (!fqoResult || !reachability) return fqoResult;
  const gates = { ...(fqoResult.gates || {}) };
  const reasons = [...(fqoResult.reasons || [])];

  if (reachability.ok) {
    if (gates.NAMED_COMMERCIAL_PERSON) {
      gates.NAMED_COMMERCIAL_PERSON = {
        ...gates.NAMED_COMMERCIAL_PERSON,
        ok: true,
        entryPointRule: ENTRY_POINT_FQO_RULE_ID,
      };
    }
    if (gates.PERSON_SPECIFIC_CHANNEL) {
      gates.PERSON_SPECIFIC_CHANNEL = {
        ...gates.PERSON_SPECIFIC_CHANNEL,
        ok: true,
        entryPointRule: ENTRY_POINT_FQO_RULE_ID,
      };
    }
    const filtered = reasons.filter(
      (r) =>
        r !== "NAMED_COMMERCIAL_PERSON_REQUIRED" &&
        r !== "PERSON_SPECIFIC_CHANNEL_REQUIRED",
    );
    const allOk = Object.values(gates).every((g) => !g || g.ok === true);
    return {
      ...fqoResult,
      gates,
      reasons: filtered,
      finalizedQualified: allOk,
      status: allOk ? "FINALIZED_QUALIFIED" : fqoResult.status,
      entryPointReachability: reachability,
    };
  }

  return {
    ...fqoResult,
    entryPointReachability: reachability,
  };
}
