/**
 * Packet 2.5 — Claim → relationship candidates + temporal / conflict reconciliation.
 */

import { createRelationshipCandidate } from "./schemas.js";
import { claimTypeToRelationshipCandidate } from "./source-language.js";
import { CONTRADICTION_STATUSES } from "./claim-types.js";

export const CLAIM_RECONCILE_VERSION = "claim-reconcile-v1";

/**
 * Transform resolved claims into relationship candidates (pre-dedupe).
 * @param {object[]} claims
 */
export function claimsToRelationshipCandidates(claims) {
  const out = [];
  for (const c of claims || []) {
    const relType =
      c.relationship_candidate || claimTypeToRelationshipCandidate(c.claim_type);
    if (!relType) continue;
    // People title / room count / alias alone are not graph edges of ownership
    if (
      [
        "PERSON_HAS_TITLE",
        "PERSON_REPRESENTS_ORGANIZATION",
        "PROPERTY_HAS_ROOM_COUNT",
        "PROPERTY_ALIAS",
        "PROPERTY_IS_ADJACENT_TO",
        "OTHER_STRUCTURED_CLAIM",
      ].includes(c.claim_type)
    ) {
      continue;
    }
    // Need resolvable object entity for edge (org / brand / counterparty)
    if (!c.canonical_object_id && !c.object_raw) continue;

    // Skip edges where object resolved to a hotel id for OWNED_BY (wrong direction)
    if (
      relType === "OWNED_BY" &&
      c.canonical_object_id &&
      String(c.canonical_object_id).startsWith("dhl_")
    ) {
      continue;
    }

    const subjectHotel =
      c.hotel_context_id ||
      (c.hotel_resolution?.resolved ? c.hotel_resolution.entity_id : null);

    out.push(
      createRelationshipCandidate({
        subject_hotel_id: subjectHotel,
        subject_canonical_id: c.canonical_subject_id,
        relationship_type: relType,
        object_canonical_id: c.canonical_object_id || `raw:${normalize(c.object_raw)}`,
        hotel_context_id: subjectHotel,
        ownership_percentage: c.ownership_percentage,
        effective_from: c.effective_from,
        effective_to: c.effective_to,
        temporal_status: c.temporal_status || "UNKNOWN",
        supporting_claim_ids: [c.claim_id],
        source_authority_max: c.source_authority,
        confidence: c.overall_claim_confidence,
        promotion_status: "candidate",
        entity_scope:
          c.object_resolution?.entity?.scope ||
          c.subject_resolution?.entity?.scope ||
          "UNKNOWN_SCOPE",
        notes: c.claim_type,
      })
    );
  }
  return out;
}

/**
 * Reconcile overlapping candidates into a single edge per
 * (hotel, relationship_type, object, temporal bucket).
 * @param {object[]} candidates
 */
export function reconcileRelationshipCandidates(candidates) {
  const groups = new Map();
  for (const cand of candidates || []) {
    const key = [
      cand.hotel_context_id || cand.subject_hotel_id || "",
      cand.relationship_type,
      cand.object_canonical_id,
      temporalBucket(cand.temporal_status),
      cand.ownership_percentage ?? "",
    ].join("|");
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(cand);
  }

  const reconciled = [];
  for (const [, group] of groups) {
    const merged = mergeGroup(group);
    reconciled.push(merged);
  }

  // Detect conflicts: same hotel + OWNED_BY + CURRENT + different objects
  markOwnershipConflicts(reconciled);
  return reconciled;
}

function mergeGroup(group) {
  const base = { ...group[0] };
  const claimIds = new Set();
  let maxAuth = 0;
  let confSum = 0;
  let confN = 0;
  let pct = base.ownership_percentage;
  let temporal = base.temporal_status;

  for (const g of group) {
    for (const id of g.supporting_claim_ids || []) claimIds.add(id);
    if (g.source_authority_max != null) {
      maxAuth = Math.max(maxAuth, Number(g.source_authority_max));
    }
    if (g.confidence != null) {
      confSum += Number(g.confidence);
      confN += 1;
    }
    if (g.ownership_percentage != null) pct = g.ownership_percentage;
    temporal = preferTemporal(temporal, g.temporal_status);
  }

  return createRelationshipCandidate({
    ...base,
    candidate_id: base.candidate_id,
    supporting_claim_ids: [...claimIds],
    source_authority_max: maxAuth || base.source_authority_max,
    confidence: confN ? Math.round((confSum / confN) * 1000) / 1000 : base.confidence,
    ownership_percentage: pct,
    temporal_status: temporal,
    promotion_status: "candidate",
    contradiction_status: "NONE",
    notes: `reconciled_from_${group.length}_candidates`,
  });
}

function markOwnershipConflicts(candidates) {
  const currentOwners = candidates.filter(
    (c) =>
      c.relationship_type === "OWNED_BY" &&
      (c.temporal_status === "CURRENT" || c.temporal_status === "CURRENT_UNVERIFIED")
  );
  const byHotel = new Map();
  for (const c of currentOwners) {
    const h = c.hotel_context_id || c.subject_hotel_id || "_";
    if (!byHotel.has(h)) byHotel.set(h, []);
    byHotel.get(h).push(c);
  }
  for (const [, list] of byHotel) {
    const objects = new Set(list.map((c) => c.object_canonical_id));
    if (objects.size <= 1) {
      for (const c of list) c.contradiction_status = "CONFIRMED";
      continue;
    }
    // Rank by authority then confidence — do not blindly take latest string
    const ranked = [...list].sort(
      (a, b) =>
        (b.source_authority_max || 0) - (a.source_authority_max || 0) ||
        (b.confidence || 0) - (a.confidence || 0)
    );
    // If PropCo + parent both present — not a conflict (different scope)
    const scopes = new Set(list.map((c) => c.entity_scope));
    if (
      scopes.has("PROPERTY_SPECIFIC") &&
      scopes.has("ORGANIZATION_LEVEL") &&
      objects.size === 2
    ) {
      for (const c of list) c.contradiction_status = "CONFIRMED";
      continue;
    }
    ranked[0].contradiction_status = "UNRESOLVED_CONFLICT";
    ranked[0].promotion_status = "needs_review";
    for (let i = 1; i < ranked.length; i += 1) {
      ranked[i].contradiction_status = "CONTRADICTED";
      ranked[i].promotion_status = "needs_review";
      ranked[i].contradicting_claim_ids = [
        ...(ranked[i].contradicting_claim_ids || []),
        ...(ranked[0].supporting_claim_ids || []),
      ];
    }
  }
}

function temporalBucket(status) {
  if (status === "FORMER") return "FORMER";
  if (status === "ANNOUNCED" || status === "PLANNED") return "ANNOUNCED";
  if (status === "CURRENT" || status === "CURRENT_UNVERIFIED") return "CURRENT";
  return status || "UNKNOWN";
}

function preferTemporal(a, b) {
  const rank = {
    CURRENT: 5,
    CURRENT_UNVERIFIED: 4,
    ANNOUNCED: 3,
    FORMER: 3,
    PLANNED: 2,
    CONTESTED: 2,
    UNKNOWN: 1,
    CANCELLED: 1,
  };
  return (rank[b] || 0) >= (rank[a] || 0) ? b : a;
}

function normalize(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
}

export { CONTRADICTION_STATUSES };
