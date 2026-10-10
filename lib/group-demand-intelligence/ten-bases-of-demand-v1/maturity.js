/**
 * Opportunity maturity states for 10 Bases model.
 * PREDICTED_OPPORTUNITY must be labeled inferred — never customer-ready confirmed.
 */

import { OPPORTUNITY_MATURITY } from "./taxonomy.js";
import { PACKET_QUALITY } from "../complete-demand-packet-v8/packet-schema.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../future-watch/is-valid-future-watch-v1.js";

/**
 * Assign maturity from packet + evidence. Does not invent dates/locations.
 */
export function assignOpportunityMaturity(record = {}, packetEval = {}, opts = {}) {
  const quality = packetEval.quality || record.packetQuality;
  const timing = String(record.timingState || record.futureCycleEvidenceState || "");
  const isPredictedTiming =
    /ROTATION_PREDICTED|RECURRING_EXPECTED|PREDICTED/i.test(timing) ||
    record.maturityHint === "PREDICTED" ||
    record.isPredicted === true;

  const hasBuyer =
    Boolean(record.buyerEntity || record.organizer || record.publicContactPath) ||
    packetEval.pillars?.C_BUYER_ORGANIZER_PATH?.strength === "STRONG" ||
    packetEval.pillars?.C_BUYER_ORGANIZER_PATH?.strength === "PRESENT";

  const hasFuture =
    packetEval.pillars?.D_FUTURE_DECISION_POINT?.strength === "STRONG" ||
    packetEval.pillars?.D_FUTURE_DECISION_POINT?.strength === "PRESENT";

  const hasFit =
    packetEval.pillars?.F_TARGET_HOTEL_FIT?.strength === "STRONG" ||
    packetEval.pillars?.F_TARGET_HOTEL_FIT?.strength === "PRESENT";

  const hasLodging =
    packetEval.pillars?.E_HOTEL_LODGING_EVIDENCE?.strength === "STRONG" ||
    packetEval.pillars?.E_HOTEL_LODGING_EVIDENCE?.strength === "PRESENT";

  // Canonical gates first — never bypass
  let customerReady = false;
  let validWatch = false;
  try {
    const r = isGdiCustomerOpportunityReady(record, opts.gateOpts || {});
    customerReady = Boolean(r?.ok === true || r?.ready === true);
  } catch {
    customerReady = false;
  }
  try {
    const w = isValidFutureWatch(record, opts.gateOpts || {});
    validWatch = Boolean(w?.ok === true || w?.valid === true);
  } catch {
    validWatch = false;
  }

  if (quality === PACKET_QUALITY.REJECTED || quality === PACKET_QUALITY.DUPLICATE) {
    return {
      maturity: OPPORTUNITY_MATURITY.REJECTED,
      customerReady: false,
      validFutureWatch: false,
      predictedLabel: null,
    };
  }

  if (customerReady) {
    return {
      maturity: OPPORTUNITY_MATURITY.CONFIRMED_OPPORTUNITY,
      customerReady: true,
      validFutureWatch: false,
      predictedLabel: null,
    };
  }

  if (validWatch) {
    return {
      maturity: OPPORTUNITY_MATURITY.VALID_FUTURE_WATCH,
      customerReady: false,
      validFutureWatch: true,
      predictedLabel: null,
    };
  }

  // Predicted: rotation/recurrence + buyer + future cycle + fit + decision window
  // Explicitly NOT confirmed. Must not fabricate date/location.
  if (
    isPredictedTiming &&
    hasBuyer &&
    hasFuture &&
    hasFit &&
    (hasLodging || record.decisionWindow) &&
    (quality === PACKET_QUALITY.COMPLETE_STRONG ||
      quality === PACKET_QUALITY.COMPLETE_PLAUSIBLE ||
      quality === PACKET_QUALITY.PARTIAL_PACKET)
  ) {
    return {
      maturity: OPPORTUNITY_MATURITY.PREDICTED_OPPORTUNITY,
      customerReady: false,
      validFutureWatch: false,
      predictedLabel: "PREDICTED_INFERRED_NOT_CONFIRMED",
      predictionBasis: timing || "ROTATION_OR_RECURRENCE",
    };
  }

  if (
    quality === PACKET_QUALITY.COMPLETE_STRONG ||
    quality === PACKET_QUALITY.COMPLETE_PLAUSIBLE ||
    quality === PACKET_QUALITY.PARTIAL_PACKET
  ) {
    return {
      maturity: OPPORTUNITY_MATURITY.RESEARCH_LEAD,
      customerReady: false,
      validFutureWatch: false,
      predictedLabel: null,
    };
  }

  if (record.generatorOnly) {
    return {
      maturity: OPPORTUNITY_MATURITY.GENERATOR_INTELLIGENCE,
      customerReady: false,
      validFutureWatch: false,
      predictedLabel: null,
    };
  }

  return {
    maturity: OPPORTUNITY_MATURITY.RESEARCH_LEAD,
    customerReady: false,
    validFutureWatch: false,
    predictedLabel: null,
  };
}
