/**
 * Ownership Intelligence record factories (schema_version stamped).
 */

import { roleForRelationshipType } from "./ontology.js";
import {
  generateEntityId,
  generateRelationshipId,
  generateEvidenceId,
  generateAliasId,
  generateResearchRunId,
  normalizeEntityName,
} from "./ids.js";
import { verificationFromConfidence } from "./confidence-map.js";

export const OWNERSHIP_SCHEMAS_VERSION = "ownership-schemas-v1";

function nowIso() {
  return new Date().toISOString();
}

/**
 * @param {Partial<object>} [partial]
 */
export function createOwnershipEntity(partial = {}) {
  const now = nowIso();
  const legal = String(partial.legal_name || partial.display_name || "").trim();
  return {
    schema_version: "ownership-entity-v1",
    entity_id: partial.entity_id || generateEntityId(),
    legal_name: legal,
    display_name: String(partial.display_name || legal).trim() || legal,
    entity_type: partial.entity_type || "unknown",
    jurisdiction: partial.jurisdiction ?? null,
    country: partial.country ?? null,
    identifiers: Array.isArray(partial.identifiers) ? [...partial.identifiers] : [],
    lei: partial.lei ?? null,
    website: partial.website ?? null,
    domains: Array.isArray(partial.domains) ? [...partial.domains] : [],
    status: partial.status || "unknown",
    notes: partial.notes ?? null,
    created_at: partial.created_at || now,
    updated_at: partial.updated_at || now,
  };
}

/**
 * @param {Partial<object>} [partial]
 */
export function createEntityAlias(partial = {}) {
  const alias = String(partial.alias || "").trim();
  return {
    schema_version: "ownership-alias-v1",
    alias_id: partial.alias_id || generateAliasId(),
    entity_id: String(partial.entity_id || "").trim(),
    alias,
    normalized_alias: partial.normalized_alias || normalizeEntityName(alias),
    source: partial.source ?? null,
    created_at: partial.created_at || nowIso(),
  };
}

/**
 * @param {Partial<object>} [partial]
 */
export function createOwnershipRelationship(partial = {}) {
  const now = nowIso();
  const relationshipType = String(partial.relationship_type || "").trim();
  const role =
    partial.role ||
    roleForRelationshipType(relationshipType) ||
    "PROPERTY_OWNER";
  const confidence =
    partial.confidence != null ? Number(partial.confidence) : null;
  return {
    schema_version: "ownership-relationship-v1",
    relationship_id: partial.relationship_id || generateRelationshipId(),
    subject_hotel_id: partial.subject_hotel_id ?? null,
    subject_entity_id: partial.subject_entity_id ?? null,
    relationship_type: relationshipType,
    object_entity_id: String(partial.object_entity_id || "").trim(),
    role,
    confidence,
    verification_status:
      partial.verification_status ||
      (confidence != null
        ? verificationFromConfidence(confidence)
        : "unknown"),
    is_current: partial.is_current !== false,
    effective_from: partial.effective_from ?? null,
    effective_to: partial.effective_to ?? null,
    research_run_id: partial.research_run_id ?? null,
    created_at: partial.created_at || now,
    updated_at: partial.updated_at || now,
  };
}

/**
 * @param {Partial<object>} [partial]
 */
export function createRelationshipEvidence(partial = {}) {
  return {
    schema_version: "ownership-evidence-v1",
    evidence_id: partial.evidence_id || generateEvidenceId(),
    relationship_id: String(partial.relationship_id || "").trim(),
    source: String(partial.source || "").trim().toLowerCase(),
    source_url: partial.source_url ?? null,
    source_type: partial.source_type || "other",
    source_authority:
      partial.source_authority != null ? Number(partial.source_authority) : null,
    observed_at: partial.observed_at || nowIso().slice(0, 10),
    published_at: partial.published_at ?? null,
    extracted_claim: partial.extracted_claim ?? null,
    confidence: partial.confidence != null ? Number(partial.confidence) : null,
    extraction_method: partial.extraction_method || "manual",
    researcher: partial.researcher ?? null,
    conflicts_with_evidence_ids: Array.isArray(
      partial.conflicts_with_evidence_ids
    )
      ? [...partial.conflicts_with_evidence_ids]
      : [],
    notes: partial.notes ?? null,
    created_at: partial.created_at || nowIso(),
  };
}

/**
 * @param {Partial<object>} [partial]
 */
export function createResearchRun(partial = {}) {
  const now = nowIso();
  return {
    schema_version: "ownership-research-run-v1",
    run_id: partial.run_id || generateResearchRunId(),
    hotel_id: String(partial.hotel_id || "").trim(),
    status: partial.status || "pending",
    stages_completed: Array.isArray(partial.stages_completed)
      ? [...partial.stages_completed]
      : [],
    stop_reason: partial.stop_reason ?? null,
    metrics: {
      searches: Number(partial.metrics?.searches || 0),
      pages_fetched: Number(partial.metrics?.pages_fetched || 0),
      evidence_count: Number(partial.metrics?.evidence_count || 0),
      cost_usd:
        partial.metrics?.cost_usd != null
          ? Number(partial.metrics.cost_usd)
          : null,
      latency_ms:
        partial.metrics?.latency_ms != null
          ? Number(partial.metrics.latency_ms)
          : null,
    },
    created_at: partial.created_at || now,
    completed_at: partial.completed_at ?? null,
  };
}
