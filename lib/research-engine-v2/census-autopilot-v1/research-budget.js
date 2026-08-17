/**
 * Research budget / stopping rules (Webhound lesson: unequal effort).
 */

/**
 * @param {object} fieldResult single field resolution
 */
export function shouldStopFieldResearch(fieldResult) {
  if (fieldResult.stop_research) return { stop: true, reason: "authoritative_high_confidence" };
  if (fieldResult.resolution_status === "Verified" && fieldResult.confidence === "High") {
    return { stop: true, reason: "confidence_gate" };
  }
  if (fieldResult.resolution_status === "Conflicting Evidence") {
    return { stop: true, reason: "escalate_conflict", escalate: true };
  }
  return { stop: false };
}

/**
 * @param {object} hotelEffort tracker
 * @param {object} [limits]
 */
export function shouldStopHotelResearch(hotelEffort, limits = {}) {
  const maxFields = limits.max_field_attempts ?? 80;
  const maxLaneB = limits.max_lane_b_attempts ?? 25;
  const maxEscalations = limits.max_escalations_before_hold ?? 8;

  if ((hotelEffort.field_attempts || 0) >= maxFields) {
    return { stop: true, reason: "max_field_attempts" };
  }
  if ((hotelEffort.lane_b_attempts || 0) >= maxLaneB) {
    return { stop: true, reason: "max_lane_b", escalate: true };
  }
  if ((hotelEffort.escalations || 0) >= maxEscalations) {
    return { stop: true, reason: "max_escalations", escalate: true };
  }
  if (hotelEffort.sufficient_authoritative_core) {
    return { stop: true, reason: "core_sufficient" };
  }
  return { stop: false };
}

export function createHotelEffortTracker(recordId) {
  return {
    independent_record_id: recordId,
    field_attempts: 0,
    lane_a_hits: 0,
    lane_b_attempts: 0,
    escalations: 0,
    sufficient_authoritative_core: false,
    external_cost_usd: 0,
  };
}
