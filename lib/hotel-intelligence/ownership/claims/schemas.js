/**
 * Packet 2.5 — Intelligence Claim + alias + relationship-candidate factories.
 * Store reproducible evidence pointers — never hidden chain-of-thought.
 */

import {
  generateClaimId,
  generateRelationshipCandidateId,
  generateAliasId,
  normalizeEntityName,
} from "../ids.js";
import {
  CLAIM_ENGINE_VERSION,
  CLAIM_TYPES,
  TEMPORAL_STATUSES,
  CLAIM_VERIFICATION_STATUSES,
  CONTRADICTION_STATUSES,
  ALIAS_TYPES,
  PROMOTION_STATUSES,
  ENTITY_SCOPES,
} from "./claim-types.js";

function nowIso() {
  return new Date().toISOString();
}

/**
 * @param {Partial<object>} [partial]
 */
export function createIntelligenceClaim(partial = {}) {
  const now = nowIso();
  const claimType = String(partial.claim_type || "OTHER_STRUCTURED_CLAIM").trim();
  if (!CLAIM_TYPES.includes(claimType) && claimType !== "OTHER_STRUCTURED_CLAIM") {
    // Allow unknown only via OTHER_STRUCTURED_CLAIM
  }
  return {
    schema_version: "intelligence-claim-v1",
    claim_engine_version: CLAIM_ENGINE_VERSION,
    claim_id: partial.claim_id || generateClaimId(),
    subject_raw: partial.subject_raw ?? null,
    predicate_raw: partial.predicate_raw ?? null,
    object_raw: partial.object_raw ?? null,
    subject_entity_candidate: partial.subject_entity_candidate ?? null,
    object_entity_candidate: partial.object_entity_candidate ?? null,
    canonical_subject_id: partial.canonical_subject_id ?? null,
    canonical_object_id: partial.canonical_object_id ?? null,
    hotel_context_id: partial.hotel_context_id ?? null,
    claim_type: CLAIM_TYPES.includes(claimType)
      ? claimType
      : "OTHER_STRUCTURED_CLAIM",
    relationship_candidate: partial.relationship_candidate ?? null,
    claim_value: partial.claim_value ?? null,
    ownership_percentage:
      partial.ownership_percentage != null
        ? Number(partial.ownership_percentage)
        : null,
    source_id: partial.source_id ?? null,
    source_url: partial.source_url ?? null,
    source_type: partial.source_type ?? null,
    source_authority:
      partial.source_authority != null ? Number(partial.source_authority) : null,
    document_title: partial.document_title ?? null,
    publication_date: partial.publication_date ?? null,
    observed_date: partial.observed_date || now.slice(0, 10),
    effective_from: partial.effective_from ?? null,
    effective_to: partial.effective_to ?? null,
    temporal_status: TEMPORAL_STATUSES.includes(partial.temporal_status)
      ? partial.temporal_status
      : "UNKNOWN",
    evidence_excerpt: partial.evidence_excerpt ?? null,
    evidence_pointer: partial.evidence_pointer ?? null,
    page: partial.page ?? null,
    section: partial.section ?? null,
    extraction_confidence:
      partial.extraction_confidence != null
        ? Number(partial.extraction_confidence)
        : null,
    entity_resolution_confidence:
      partial.entity_resolution_confidence != null
        ? Number(partial.entity_resolution_confidence)
        : null,
    semantic_confidence:
      partial.semantic_confidence != null
        ? Number(partial.semantic_confidence)
        : null,
    overall_claim_confidence:
      partial.overall_claim_confidence != null
        ? Number(partial.overall_claim_confidence)
        : null,
    verification_status: CLAIM_VERIFICATION_STATUSES.includes(
      partial.verification_status
    )
      ? partial.verification_status
      : "EXTRACTED",
    contradiction_status: CONTRADICTION_STATUSES.includes(
      partial.contradiction_status
    )
      ? partial.contradiction_status
      : "NONE",
    research_run_id: partial.research_run_id ?? null,
    importance: partial.importance ?? null,
    derivation_notes: Array.isArray(partial.derivation_notes)
      ? [...partial.derivation_notes]
      : [],
    created_at: partial.created_at || now,
  };
}

/**
 * Evidence-backed alias record (not a merge key alone).
 * @param {Partial<object>} [partial]
 */
export function createEvidenceAlias(partial = {}) {
  const alias = String(partial.alias || "").trim();
  return {
    schema_version: "intelligence-alias-v1",
    alias_id: partial.alias_id || generateAliasId(),
    entity_id: String(partial.entity_id || "").trim(),
    alias,
    normalized_alias: partial.normalized_alias || normalizeEntityName(alias),
    alias_type: ALIAS_TYPES.includes(partial.alias_type)
      ? partial.alias_type
      : "OTHER",
    effective_from: partial.effective_from ?? null,
    effective_to: partial.effective_to ?? null,
    source_id: partial.source_id ?? null,
    source_url: partial.source_url ?? null,
    confidence:
      partial.confidence != null ? Number(partial.confidence) : null,
    entity_scope: ENTITY_SCOPES.includes(partial.entity_scope)
      ? partial.entity_scope
      : "UNKNOWN_SCOPE",
    created_at: partial.created_at || nowIso(),
  };
}

/**
 * Relationship candidate assembled from one or more claims.
 * @param {Partial<object>} [partial]
 */
export function createRelationshipCandidate(partial = {}) {
  const now = nowIso();
  return {
    schema_version: "relationship-candidate-v1",
    candidate_id: partial.candidate_id || generateRelationshipCandidateId(),
    subject_canonical_id: partial.subject_canonical_id ?? null,
    subject_hotel_id: partial.subject_hotel_id ?? null,
    relationship_type: String(partial.relationship_type || "").trim(),
    object_canonical_id: String(partial.object_canonical_id || "").trim(),
    hotel_context_id: partial.hotel_context_id ?? null,
    ownership_percentage:
      partial.ownership_percentage != null
        ? Number(partial.ownership_percentage)
        : null,
    effective_from: partial.effective_from ?? null,
    effective_to: partial.effective_to ?? null,
    temporal_status: TEMPORAL_STATUSES.includes(partial.temporal_status)
      ? partial.temporal_status
      : "UNKNOWN",
    supporting_claim_ids: Array.isArray(partial.supporting_claim_ids)
      ? [...partial.supporting_claim_ids]
      : [],
    contradicting_claim_ids: Array.isArray(partial.contradicting_claim_ids)
      ? [...partial.contradicting_claim_ids]
      : [],
    source_authority_max:
      partial.source_authority_max != null
        ? Number(partial.source_authority_max)
        : null,
    confidence:
      partial.confidence != null ? Number(partial.confidence) : null,
    promotion_status: PROMOTION_STATUSES.includes(partial.promotion_status)
      ? partial.promotion_status
      : "candidate",
    contradiction_status: CONTRADICTION_STATUSES.includes(
      partial.contradiction_status
    )
      ? partial.contradiction_status
      : "NONE",
    entity_scope: ENTITY_SCOPES.includes(partial.entity_scope)
      ? partial.entity_scope
      : "UNKNOWN_SCOPE",
    notes: partial.notes ?? null,
    created_at: partial.created_at || now,
    updated_at: partial.updated_at || now,
  };
}

/**
 * Derive overall claim confidence from calibrated components.
 * Does not inflate to hit thresholds.
 */
export function deriveOverallClaimConfidence(parts = {}) {
  const extraction = clamp01(parts.extraction_confidence);
  const entity = clamp01(parts.entity_resolution_confidence);
  const semantic = clamp01(parts.semantic_confidence);
  const temporal = clamp01(parts.temporal_confidence ?? semantic);
  const authority = clamp01(parts.source_authority);
  const corroboration = clamp01(parts.corroboration ?? 0.5);
  // Weighted geometric-ish mean: weak component pulls score down
  const weights = [
    [extraction, 0.2],
    [entity, 0.25],
    [semantic, 0.25],
    [temporal, 0.1],
    [authority, 0.1],
    [corroboration, 0.1],
  ];
  let sum = 0;
  let wsum = 0;
  for (const [v, w] of weights) {
    if (v == null) continue;
    sum += v * w;
    wsum += w;
  }
  if (wsum === 0) return null;
  return Math.round((sum / wsum) * 1000) / 1000;
}

function clamp01(v) {
  if (v == null || Number.isNaN(Number(v))) return null;
  return Math.max(0, Math.min(1, Number(v)));
}
