/**
 * Observation → candidate relationship promotion.
 * Observations never become graph edges automatically.
 */

import { createOwnershipRelationship } from "./schemas.js";
import { PROMOTION_STATUSES } from "../intelligence-observation.js";

export const OBSERVATION_PROMOTION_VERSION = "ownership-observation-promotion-v1";

const PROMOTABLE_CLASSES = new Set(["canonical_relationship_candidate"]);

/**
 * @param {object} observation
 * @returns {{ eligible: boolean, reason: string }}
 */
export function evaluateObservationPromotionEligibility(observation) {
  if (!observation) return { eligible: false, reason: "missing_observation" };
  if (observation.product_truth === true && observation.promotion_status === "promoted_relationship") {
    return { eligible: false, reason: "already_promoted" };
  }
  if (observation.promotion_status === "rejected_for_graph") {
    return { eligible: false, reason: "rejected_for_graph" };
  }
  if (observation.source_provider === "webhound" && observation.product_truth !== true) {
    return { eligible: false, reason: "webhound_not_product_truth" };
  }
  if (!PROMOTABLE_CLASSES.has(observation.intelligence_class)) {
    return { eligible: false, reason: "class_not_promotable" };
  }
  if (!observation.intended_relationship_type) {
    return { eligible: false, reason: "no_intended_relationship_type" };
  }
  const vs = String(observation.verification_status || "");
  if (!["verified", "high"].includes(vs)) {
    return { eligible: false, reason: "verification_below_graph_bar" };
  }
  return { eligible: true, reason: "eligible_as_candidate" };
}

/**
 * Build a *candidate* relationship payload. Caller must persist after review.
 * Does not write the graph.
 * @param {object} observation
 * @param {{ subject_hotel_id?: string, subject_entity_id?: string, object_entity_id: string, research_run_id?: string }} ids
 */
export function observationToCandidateRelationship(observation, ids) {
  const gate = evaluateObservationPromotionEligibility(observation);
  if (!gate.eligible) {
    return { ok: false, reason: gate.reason, relationship: null };
  }
  const rel = createOwnershipRelationship({
    subject_hotel_id: ids.subject_hotel_id ?? null,
    subject_entity_id: ids.subject_entity_id ?? null,
    relationship_type: observation.intended_relationship_type,
    object_entity_id: ids.object_entity_id,
    confidence: Math.min(Number(observation.confidence || 0.7), 0.84),
    verification_status: "needs_review",
    is_current: !observation.historical,
    research_run_id: ids.research_run_id || observation.research_run_id,
  });
  return {
    ok: true,
    reason: gate.reason,
    relationship: rel,
    next_promotion_status: "candidate_relationship",
  };
}

export { PROMOTION_STATUSES };
