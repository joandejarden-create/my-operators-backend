/**
 * Packet 2.5 — High-bar promotion rules for OWNED_BY / CONTROLLED_BY / PropCo / OPERATED_BY.
 * Never promote operator → owner, developer → owner, brand → owner, portfolio listing alone.
 */

import { createOwnershipRelationship } from "../schemas.js";
import { createIntelligenceEvent } from "./events.js";

export const CLAIM_PROMOTE_VERSION = "claim-promote-v1";

const OWNER_TYPES = new Set(["OWNED_BY", "CONTROLLED_BY"]);

/**
 * @param {object} candidate
 * @param {object[]} supportingClaims
 * @returns {{ promote: boolean, status: string, reason: string, relationship: object|null, review: boolean }}
 */
export function evaluatePromotion(candidate, supportingClaims = []) {
  if (!candidate) {
    return { promote: false, status: "rejected", reason: "missing_candidate", relationship: null, review: false };
  }

  const rel = candidate.relationship_type;
  const claims = supportingClaims.filter((c) =>
    (candidate.supporting_claim_ids || []).includes(c.claim_id)
  );

  // Hard safety: never promote contradicted / unresolved conflict
  if (
    candidate.contradiction_status === "CONTRADICTED" ||
    candidate.contradiction_status === "UNRESOLVED_CONFLICT"
  ) {
    return {
      promote: false,
      status: "needs_review",
      reason: `conflict:${candidate.contradiction_status}`,
      relationship: null,
      review: true,
    };
  }

  // Never promote FORMER / ANNOUNCED as CURRENT
  if (
    (candidate.temporal_status === "FORMER" ||
      candidate.temporal_status === "ANNOUNCED" ||
      candidate.temporal_status === "PLANNED") &&
    OWNER_TYPES.has(rel)
  ) {
    return {
      promote: false,
      status: "observation",
      reason: "historical_or_announced_not_promoted_as_current_owner",
      relationship: null,
      review: false,
    };
  }

  if (rel === "OPERATED_BY") {
    return promoteOperator(candidate, claims);
  }

  if (OWNER_TYPES.has(rel)) {
    return promoteOwnership(candidate, claims);
  }

  if (rel === "BRANDED_BY") {
    return promoteBrand(candidate, claims);
  }

  if (rel === "JV_WITH") {
    return promoteJv(candidate, claims);
  }

  // Developer / sponsor / financed — candidate or observation only by default
  if (["DEVELOPED_BY", "SPONSORED_BY", "FINANCED_BY", "INVESTED_IN_BY"].includes(rel)) {
    return {
      promote: false,
      status: "observation",
      reason: "non_owner_role_observation_only",
      relationship: null,
      review: false,
    };
  }

  return {
    promote: false,
    status: "candidate",
    reason: "default_hold_candidate",
    relationship: null,
    review: false,
  };
}

function promoteOperator(candidate, claims) {
  const ok = claims.some(
    (c) =>
      ["ENTITY_OPERATES_HOTEL", "ENTITY_MANAGES_HOTEL"].includes(c.claim_type) &&
      (c.source_authority || 0) >= 0.5
  );
  if (!ok && (candidate.source_authority_max || 0) < 0.55) {
    return {
      promote: false,
      status: "candidate",
      reason: "operator_evidence_weak",
      relationship: null,
      review: false,
    };
  }
  return {
    promote: true,
    status: "promoted",
    reason: "operator_evidence_ok",
    relationship: toRelationship(candidate, Math.min(candidate.confidence || 0.75, 0.88)),
    review: false,
  };
}

function promoteOwnership(candidate, claims) {
  // Reject if only operator / brand / developer language
  const badOnly = claims.length > 0 && claims.every((c) =>
    [
      "ENTITY_OPERATES_HOTEL",
      "ENTITY_MANAGES_HOTEL",
      "ENTITY_BRANDS_HOTEL",
      "HOTEL_CURRENT_BRAND",
      "HOTEL_ANNOUNCED_BRAND",
      "ENTITY_DEVELOPED_PROPERTY",
      "ENTITY_IS_BORROWER",
      "ENTITY_FINANCED_PROPERTY",
    ].includes(c.claim_type)
  );
  if (badOnly) {
    return {
      promote: false,
      status: "rejected",
      reason: "operator_or_brand_or_developer_cannot_become_owner",
      relationship: null,
      review: false,
    };
  }

  // Partial ownership must not promote as sole OWNED_BY without JV semantics
  if (
    candidate.ownership_percentage != null &&
    candidate.ownership_percentage < 100 &&
    candidate.relationship_type === "OWNED_BY"
  ) {
    return {
      promote: false,
      status: "candidate",
      reason: "partial_ownership_requires_jv_or_percent_edge",
      relationship: null,
      review: true,
    };
  }

  const explicitOwner = claims.some((c) =>
    [
      "ENTITY_IS_OWNER_OF_PROPERTY",
      "ENTITY_HOLDS_TITLE",
      "ENTITY_ACQUIRED_ENTITY_OR_ASSET",
      "ENTITY_CONTROLS_ENTITY",
      "ENTITY_IS_PARENT_OF_ENTITY",
    ].includes(c.claim_type)
  );
  const highAuth = (candidate.source_authority_max || 0) >= 0.85;
  const multiSource =
    new Set(claims.map((c) => c.source_id).filter(Boolean)).size >= 2;

  if (explicitOwner && (highAuth || multiSource)) {
    return {
      promote: true,
      status: "promoted",
      reason: highAuth
        ? "explicit_ownership_authoritative_source"
        : "explicit_ownership_multi_source",
      relationship: toRelationship(
        candidate,
        Math.min(candidate.confidence || 0.8, highAuth ? 0.92 : 0.84)
      ),
      review: false,
    };
  }

  if (explicitOwner && (candidate.source_authority_max || 0) >= 0.7) {
    return {
      promote: false,
      status: "needs_review",
      reason: "ownership_signal_below_promotion_bar",
      relationship: null,
      review: true,
    };
  }

  return {
    promote: false,
    status: "observation",
    reason: "insufficient_ownership_evidence",
    relationship: null,
    review: false,
  };
}

function promoteBrand(candidate, claims) {
  const announced = claims.some(
    (c) =>
      c.claim_type === "HOTEL_ANNOUNCED_BRAND" ||
      c.temporal_status === "ANNOUNCED"
  );
  if (announced || candidate.temporal_status === "ANNOUNCED") {
    return {
      promote: false,
      status: "observation",
      reason: "announced_brand_not_current",
      relationship: null,
      review: false,
    };
  }
  const former =
    candidate.temporal_status === "FORMER" ||
    claims.some((c) => c.claim_type === "HOTEL_FORMER_BRAND");
  if (former) {
    return {
      promote: true,
      status: "promoted",
      reason: "former_brand_historical",
      relationship: toRelationship(
        { ...candidate, temporal_status: "FORMER" },
        0.8,
        false
      ),
      review: false,
    };
  }
  return {
    promote: true,
    status: "promoted",
    reason: "current_brand",
    relationship: toRelationship(candidate, 0.85, true),
    review: false,
  };
}

function promoteJv(candidate, claims) {
  const pctOk =
    candidate.ownership_percentage != null &&
    candidate.ownership_percentage > 0 &&
    candidate.ownership_percentage < 100;
  if (!pctOk && !claims.some((c) => c.claim_type === "ENTITY_PARTICIPATES_IN_JV")) {
    return {
      promote: false,
      status: "candidate",
      reason: "jv_without_percent_or_jv_language",
      relationship: null,
      review: true,
    };
  }
  return {
    promote: true,
    status: "promoted",
    reason: "jv_or_percent_stake",
    relationship: toRelationship(candidate, 0.82, true),
    review: false,
  };
}

function toRelationship(candidate, confidence, isCurrent = null) {
  const current =
    isCurrent != null
      ? isCurrent
      : candidate.temporal_status === "CURRENT" ||
        candidate.temporal_status === "CURRENT_UNVERIFIED";
  return createOwnershipRelationship({
    subject_hotel_id: candidate.subject_hotel_id || candidate.hotel_context_id,
    subject_entity_id: candidate.subject_canonical_id,
    relationship_type: candidate.relationship_type,
    object_entity_id: String(candidate.object_canonical_id || "").replace(
      /^raw:/,
      "unresolved:"
    ),
    confidence,
    verification_status: confidence >= 0.85 ? "high" : "probable",
    is_current: current,
    effective_from: candidate.effective_from,
    effective_to: candidate.effective_to,
  });
}

/**
 * Apply promotion across reconciled candidates.
 * @param {object[]} candidates
 * @param {object[]} claims
 */
export function promoteCandidates(candidates, claims) {
  const promoted = [];
  const held = [];
  const reviewQueue = [];
  const events = [];

  for (const cand of candidates || []) {
    const result = evaluatePromotion(cand, claims);
    const next = {
      ...cand,
      promotion_status: result.status,
      promotion_reason: result.reason,
    };
    if (result.promote && result.relationship) {
      promoted.push({ candidate: next, relationship: result.relationship });
      const ev = createIntelligenceEvent(next, claims);
      if (ev) events.push(ev);
    } else if (result.review) {
      reviewQueue.push(next);
      held.push(next);
    } else {
      held.push(next);
    }
  }

  return { promoted, held, reviewQueue, events };
}
