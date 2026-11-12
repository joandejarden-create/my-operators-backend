/**
 * GDI commercial demand thesis standard (binding philosophy).
 *
 * GDI is a proactive demand discovery platform. It identifies credible future
 * hotel demand before that demand necessarily becomes a confirmed booking
 * requirement or formal RFP.
 *
 * Demand confidence levels (canonical — do not invent parallel systems):
 *   LEVEL 1 CONFIRMED_DEMAND — independent evidence of a concrete hotel-relevant
 *     requirement (accommodation program, meeting, catering need, sourcing).
 *   LEVEL 2 RELIABLE_INFERRED_DEMAND — verified commercial activity/trigger
 *     supports a reasonable hotel-specific inference of potential demand
 *     (office opening, facility investment, project workforce, venue referral,
 *     planner activity, training program, etc.). Confirmed hotel booking NOT required.
 *   LEVEL 3 SPECULATIVE_DEMAND — interesting organization without a sufficiently
 *     specific trigger, mechanism, or hotel fit → research continuation only.
 *
 * Five-part commercial thesis (see gdi-proactive-commercial-intelligence-standard-v1):
 *   verified trigger → demand mechanism → hotel-specific fit → named entry point
 *   → actionable outreach ("why reach out now").
 *
 * Sales-ready eligibility requires:
 *   1. ≥1 verified named DIRECT_BUYER or RELEVANT_COMMERCIAL_ENTRY_POINT
 *   2. A reliable, evidence-grounded thesis explaining why the organization is
 *      realistically likely to need guestrooms and/or local catering
 *   3. A credible connection between that potential demand and the specific hotel
 *   4. A credible way for the hotel sales team to approach or reach that person
 *
 * Account Development requires CONFIRMED or RELIABLE_INFERRED plus the five-part
 * thesis; it does not auto-elevate to Sales Ready.
 *
 * Explicitly NOT required: confirmed room nights, confirmed group blocks,
 * published RFP, hotel booking announcement, active sourcing confirmation,
 * announced meeting, verified room-night volume, direct hotel-booking authority,
 * or a second named contact.
 *
 * Does not weaken contact verification. Does not fabricate commercial facts.
 * Preserves CONFIRMED / RELIABLE_INFERRED vs SPECULATIVE demand distinction.
 * Demand confidence remains separate from maturity, contact quality, revenue type,
 * publication-review readiness, and customer publication authorization.
 */

import {
  DEMAND_EVIDENCE_CLASS,
  classifyDemandMechanismEvidence,
} from "./demand-mechanism-research-v1.js";

export const COMMERCIAL_DEMAND_THESIS_STANDARD_V1 =
  "gdi-commercial-demand-thesis-standard-v1";

/** Canonical commercial demand thesis classes for all segments. */
export const DEMAND_THESIS_STANDARD = Object.freeze({
  CONFIRMED_DEMAND: "CONFIRMED_DEMAND",
  RELIABLE_INFERRED_DEMAND: "RELIABLE_INFERRED_DEMAND",
  SPECULATIVE_DEMAND: "SPECULATIVE_DEMAND",
  CONTRADICTORY_OR_NEGATIVE: "CONTRADICTORY_OR_NEGATIVE",
});

/**
 * Explicit non-requirements — never treat absence of these as a sales-ready gap.
 */
export const DEMAND_THESIS_NOT_REQUIRED = Object.freeze([
  "confirmed_room_night_demand",
  "confirmed_group_blocks",
  "published_rfp",
  "hotel_booking_announcement",
  "active_hotel_sourcing_confirmation",
  "second_named_contact",
  "confirmed_purchase_intent",
  "direct_hotel_booking_authority",
  "person_direct_email_when_org_route_reaches_named_buyer",
  "announced_meeting",
  "verified_room_night_volume",
  "confirmed_hotel_selection",
  "active_rfp",
  "universal_direct_hotel_booking_responsibility",
]);

export const SALES_READY_REQUIRES = Object.freeze([
  "verified_named_direct_buyer_or_relevant_commercial_entry_point",
  "reliable_evidence_grounded_guestroom_or_catering_thesis",
  "credible_connection_to_specific_hotel",
  "credible_approach_or_reach_path_for_named_contact",
]);

/**
 * Map Phase 2G demand evidence classes → commercial thesis standard.
 * A → CONFIRMED, B → RELIABLE_INFERRED, C → SPECULATIVE, D → CONTRADICTORY.
 */
export function mapDemandEvidenceClassToThesisStandard(evidenceClass) {
  switch (evidenceClass) {
    case DEMAND_EVIDENCE_CLASS.A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT:
      return DEMAND_THESIS_STANDARD.CONFIRMED_DEMAND;
    case DEMAND_EVIDENCE_CLASS.B_EVIDENCE_GROUNDED_REALISTIC_HOTEL_DEMAND:
      return DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND;
    case DEMAND_EVIDENCE_CLASS.D_CONTRADICTORY_OR_NEGATIVE_EVIDENCE:
      return DEMAND_THESIS_STANDARD.CONTRADICTORY_OR_NEGATIVE;
    case DEMAND_EVIDENCE_CLASS.C_PLAUSIBLE_UNSUPPORTED_HYPOTHESIS:
    default:
      return DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND;
  }
}

/**
 * Whether this thesis standard may support sales-ready (still needs buyer + hotel fit).
 */
export function thesisStandardMayQualifySalesReady(standard) {
  return (
    standard === DEMAND_THESIS_STANDARD.CONFIRMED_DEMAND ||
    standard === DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND
  );
}

/**
 * Classify opportunity (+ optional extraction) into the commercial thesis standard.
 */
export function classifyCommercialDemandThesis(opportunity = {}, extraction = {}, opts = {}) {
  const mechanism = classifyDemandMechanismEvidence(opportunity, extraction, opts);
  const thesisStandard = mapDemandEvidenceClassToThesisStandard(mechanism.evidenceClass);
  const mayQualify = thesisStandardMayQualifySalesReady(thesisStandard);

  return {
    version: COMMERCIAL_DEMAND_THESIS_STANDARD_V1,
    thesisStandard,
    mayQualifySalesReadyDemandGate: mayQualify,
    mustRemainResearchContinuation:
      thesisStandard === DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND ||
      thesisStandard === DEMAND_THESIS_STANDARD.CONTRADICTORY_OR_NEGATIVE,
    evidenceClass: mechanism.evidenceClass,
    reason: mechanism.reason,
    mechanismsDetected: mechanism.mechanismsDetected,
    salesReadyRequires: [...SALES_READY_REQUIRES],
    notRequired: [...DEMAND_THESIS_NOT_REQUIRED],
    fabricatedRoomBlocks: false,
    fabricatedRfpOrIntent: false,
    philosophy:
      "Proactive discovery of credible future hotel demand — CONFIRMED and RELIABLE_INFERRED may qualify; SPECULATIVE stays in research continuation.",
    note:
      thesisStandard === DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND
        ? "Reliable inferred demand does not require confirmed room nights, group blocks, RFP, or booking announcement."
        : thesisStandard === DEMAND_THESIS_STANDARD.CONFIRMED_DEMAND
          ? "Confirmed demand: explicit accommodation, meeting, or catering requirement evidence."
          : thesisStandard === DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND
            ? "Speculative demand — insufficient supporting facts; research continuation only."
            : "Contradictory or negative evidence — not sales-ready.",
    mechanism,
  };
}
