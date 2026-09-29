/**
 * Jev supporting-data router for hotel↔market opportunity pairs (shadow).
 * Dealality decides truth; Jev only nominates next permitted research action.
 * This pass records the decision — does not execute network fetches by default.
 */

export const JEV_SUPPORTING_ACTIONS = Object.freeze({
  VERIFY_EVENT_LOCATION: "VERIFY_EVENT_LOCATION",
  VERIFY_VENUE: "VERIFY_VENUE",
  VERIFY_ROOM_DEMAND: "VERIFY_ROOM_DEMAND",
  VERIFY_GROUP_SIZE: "VERIFY_GROUP_SIZE",
  VERIFY_LODGING_STATUS: "VERIFY_LODGING_STATUS",
  VERIFY_OVERFLOW: "VERIFY_OVERFLOW",
  VERIFY_HOTEL_SELECTION_STATUS: "VERIFY_HOTEL_SELECTION_STATUS",
  VERIFY_MEETING_REQUIREMENT: "VERIFY_MEETING_REQUIREMENT",
  VERIFY_TRANSPORT_ACCESS: "VERIFY_TRANSPORT_ACCESS",
  VERIFY_WHO: "VERIFY_WHO",
  VERIFY_ACTION_PATH: "VERIFY_ACTION_PATH",
  STOP_NO_FURTHER_EVIDENCE: "STOP_NO_FURTHER_EVIDENCE",
});

/**
 * Choose single highest-value next action for a hotel-opportunity pair.
 */
export function decideSupportingDataNextAction({
  marketPacket,
  geographicApplicability,
  hotelFit,
} = {}) {
  const missing = hotelFit?.missingData || [];
  const app = geographicApplicability?.applicability;

  if (app === "NONE" || hotelFit?.finalState === "NOT_APPLICABLE") {
    return {
      action: JEV_SUPPORTING_ACTIONS.STOP_NO_FURTHER_EVIDENCE,
      rationale: "Not geographically applicable — no supporting research",
      maxQueries: 0,
      maxFetches: 0,
    };
  }
  if (hotelFit?.finalState === "NOT_FIT" || hotelFit?.finalState === "CLOSED") {
    return {
      action: JEV_SUPPORTING_ACTIONS.STOP_NO_FURTHER_EVIDENCE,
      rationale: "Not fit / closed — stop",
      maxQueries: 0,
      maxFetches: 0,
    };
  }

  if (
    geographicApplicability?.oppGeo?.confidence === "METRO_ONLY" ||
    geographicApplicability?.oppGeo?.confidence === "UNKNOWN"
  ) {
    return {
      action: JEV_SUPPORTING_ACTIONS.VERIFY_EVENT_LOCATION,
      rationale: "Location precision insufficient for hotel applicability",
      maxQueries: 3,
      maxFetches: 5,
    };
  }

  if (missing.includes("room_demand_peak") || missing.includes("group_size")) {
    return {
      action: JEV_SUPPORTING_ACTIONS.VERIFY_ROOM_DEMAND,
      rationale: "Peak rooms / group size unknown — highest fit leverage",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  if (missing.includes("lodging_evidence")) {
    return {
      action: JEV_SUPPORTING_ACTIONS.VERIFY_LODGING_STATUS,
      rationale: "Lodging evidence missing",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  if (hotelFit?.productConstraint === "LIMITED_MEETING_SPACE") {
    return {
      action: JEV_SUPPORTING_ACTIONS.VERIFY_MEETING_REQUIREMENT,
      rationale: "Confirm whether demand is lodging-led vs meeting-led",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  if (missing.includes("who_contact")) {
    return {
      action: JEV_SUPPORTING_ACTIONS.VERIFY_WHO,
      rationale: "WHO path incomplete",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  if (hotelFit?.finalState === "HOTEL_MATCHED_NEEDS_MORE_DATA") {
    return {
      action: JEV_SUPPORTING_ACTIONS.VERIFY_HOTEL_SELECTION_STATUS,
      rationale: "Confirm hotel selection / overflow still open",
      maxQueries: 3,
      maxFetches: 5,
    };
  }
  if (hotelFit?.finalState === "CUSTOMER_READY") {
    return {
      action: JEV_SUPPORTING_ACTIONS.STOP_NO_FURTHER_EVIDENCE,
      rationale: "Shadow ready — no further evidence needed for fit bar",
      maxQueries: 0,
      maxFetches: 0,
    };
  }

  return {
    action: JEV_SUPPORTING_ACTIONS.STOP_NO_FURTHER_EVIDENCE,
    rationale: "No high-value permitted action",
    maxQueries: 0,
    maxFetches: 0,
  };
}
