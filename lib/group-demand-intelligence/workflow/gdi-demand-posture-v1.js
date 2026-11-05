/**
 * Shared demand-posture classifier (startup-safe).
 *
 * Kept free of full-E2E OS / three-hotel workflow imports so customer-serving
 * modules can classify posture without pulling ADP/e2e-os dependency trees.
 */

const CLOSED_VENUE = new Set([
  "FULLY_PLACED",
  "PRIMARY_SELECTED_NO_OVERFLOW",
  "PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE",
  "CURRENT_CYCLE_CLOSED",
]);

const CONFIRMED_ROOM = new Set([
  "VERIFIED_ROOM_BLOCK",
  "VERIFIED_HOUSING_PROGRAM",
]);

export function classifyDemandPosture(opp = {}) {
  const venue = String(opp.venueSourcingStatus || "").toUpperCase();
  const room = String(opp.roomDemandStatus || "").toUpperCase();
  const evidenceTypes = [];
  for (const item of opp.evidence || opp.claimEvidence || []) {
    if (item?.evidenceType) evidenceTypes.push(String(item.evidenceType).toUpperCase());
  }
  const confirmedLodging = evidenceTypes.some(
    (type) =>
      type === "CONFIRMED_HOTEL_RFP" || type === "CONFIRMED_LODGING_CONTROL",
  );
  const overflowMechanism =
    String(opp.opportunityType || "").toUpperCase().includes("OVERFLOW") ||
    room === "OVERFLOW_ONLY" ||
    venue.includes("OVERFLOW");
  if (CLOSED_VENUE.has(venue) && !overflowMechanism) {
    return {
      posture: "NOT_OPEN_BOOKING",
      reason: "venue_contracted_or_closed_without_separate_opportunity_mechanism",
    };
  }
  if (
    confirmedLodging ||
    CONFIRMED_ROOM.has(room) ||
    venue === "RFP_ACTIVE_SOURCING"
  ) {
    return {
      posture: "CONFIRMED",
      reason: "structured_demand_or_sourcing_evidence",
    };
  }
  const facts = Array.isArray(opp.establishedFacts) && opp.establishedFacts.length > 0;
  const inferred =
    (Array.isArray(opp.inferredHotelDemand) &&
      opp.inferredHotelDemand.length > 0) ||
    room === "ESTIMATED_ROOM_DEMAND" ||
    room === "STRONG_ROOM_DEMAND_EVIDENCE" ||
    overflowMechanism;
  if (facts && inferred) {
    return { posture: "INFERRED", reason: "facts_plus_inferred_hotel_demand" };
  }
  if (inferred) {
    return { posture: "INFERRED", reason: "inferred_or_overflow_mechanism" };
  }
  return {
    posture: "EXPLORATORY",
    reason: "credible_target_without_confirmed_buying",
  };
}
