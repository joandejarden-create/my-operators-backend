/**
 * Ownership Intelligence validators — reject role-as-entity-type and forbidden sources.
 */

import {
  ENTITY_TYPES,
  RELATIONSHIP_TYPES,
  RELATIONSHIP_ROLES,
  VERIFICATION_STATUSES,
  ENTITY_STATUSES,
  SOURCE_TYPES,
  RESEARCH_RUN_STATUSES,
  EXTRACTION_METHODS,
  FORBIDDEN_EVIDENCE_SOURCES,
  isAllowedEnum,
} from "./ontology.js";
import {
  isEntityId,
  isRelationshipId,
  isEvidenceId,
  isAliasId,
  isResearchRunId,
  isObservationId,
  isDossierId,
} from "./ids.js";
import {
  validateIntelligenceObservation as validateObservationShape,
} from "../intelligence-observation.js";
import { validateResearchDossier as validateDossierShape } from "./research-dossier.js";

export const OWNERSHIP_VALIDATE_VERSION = "ownership-validate-v1";

/**
 * Roles that must never appear as EntityType.
 */
export const FORBIDDEN_ENTITY_TYPE_ROLES = Object.freeze([
  "propco",
  "parent",
  "sponsor",
  "developer",
  "operator",
  "asset_manager",
  "brand",
  "jv_partner",
  "capital_provider",
  "decision_maker",
  "property_owner",
  "parent_owner",
  "ultimate_sponsor",
]);

/**
 * @param {object} entity
 * @returns {{ ok: boolean, errors: string[] }}
 */
export function validateOwnershipEntity(entity) {
  const errors = [];
  if (!entity || typeof entity !== "object") {
    return { ok: false, errors: ["entity_required"] };
  }
  if (!isEntityId(entity.entity_id)) errors.push("invalid_entity_id");
  if (!String(entity.legal_name || "").trim()) errors.push("legal_name_required");
  const et = String(entity.entity_type || "").trim();
  if (FORBIDDEN_ENTITY_TYPE_ROLES.includes(et)) {
    errors.push("entity_type_must_not_be_relationship_role");
  }
  if (!isAllowedEnum(et, ENTITY_TYPES)) errors.push("invalid_entity_type");
  if (!isAllowedEnum(entity.status || "unknown", ENTITY_STATUSES)) {
    errors.push("invalid_entity_status");
  }
  if (entity.identifiers != null && !Array.isArray(entity.identifiers)) {
    errors.push("identifiers_must_be_array");
  }
  return { ok: errors.length === 0, errors };
}

/**
 * @param {object} alias
 */
export function validateEntityAlias(alias) {
  const errors = [];
  if (!alias || typeof alias !== "object") {
    return { ok: false, errors: ["alias_required"] };
  }
  if (!isAliasId(alias.alias_id)) errors.push("invalid_alias_id");
  if (!isEntityId(alias.entity_id)) errors.push("invalid_entity_id");
  if (!String(alias.alias || "").trim()) errors.push("alias_required");
  return { ok: errors.length === 0, errors };
}

/**
 * @param {object} rel
 */
export function validateOwnershipRelationship(rel) {
  const errors = [];
  if (!rel || typeof rel !== "object") {
    return { ok: false, errors: ["relationship_required"] };
  }
  if (!isRelationshipId(rel.relationship_id)) {
    errors.push("invalid_relationship_id");
  }
  if (!isAllowedEnum(rel.relationship_type, RELATIONSHIP_TYPES)) {
    errors.push("invalid_relationship_type");
  }
  if (!isEntityId(rel.object_entity_id)) {
    errors.push("invalid_object_entity_id");
  }
  const hasHotel = Boolean(String(rel.subject_hotel_id || "").trim());
  const hasSubjectEntity = Boolean(String(rel.subject_entity_id || "").trim());
  if (!hasHotel && !hasSubjectEntity) {
    errors.push("subject_hotel_or_entity_required");
  }
  if (
    rel.subject_entity_id &&
    !isEntityId(rel.subject_entity_id)
  ) {
    errors.push("invalid_subject_entity_id");
  }
  if (!isAllowedEnum(rel.role, RELATIONSHIP_ROLES)) {
    errors.push("invalid_relationship_role");
  }
  if (!isAllowedEnum(rel.verification_status, VERIFICATION_STATUSES)) {
    errors.push("invalid_verification_status");
  }
  if (rel.confidence != null && !Number.isFinite(Number(rel.confidence))) {
    errors.push("invalid_confidence");
  }
  return { ok: errors.length === 0, errors };
}

/**
 * @param {object} ev
 */
export function validateRelationshipEvidence(ev) {
  const errors = [];
  if (!ev || typeof ev !== "object") {
    return { ok: false, errors: ["evidence_required"] };
  }
  if (!isEvidenceId(ev.evidence_id)) errors.push("invalid_evidence_id");
  if (!isRelationshipId(ev.relationship_id)) {
    errors.push("invalid_relationship_id");
  }
  const source = String(ev.source || "").trim().toLowerCase();
  if (!source) errors.push("source_required");
  if (FORBIDDEN_EVIDENCE_SOURCES.includes(source)) {
    errors.push("forbidden_evidence_source");
  }
  if (!isAllowedEnum(ev.source_type || "other", SOURCE_TYPES)) {
    errors.push("invalid_source_type");
  }
  if (
    ev.extraction_method &&
    !isAllowedEnum(ev.extraction_method, EXTRACTION_METHODS)
  ) {
    errors.push("invalid_extraction_method");
  }
  return { ok: errors.length === 0, errors };
}

/**
 * @param {object} run
 */
export function validateResearchRun(run) {
  const errors = [];
  if (!run || typeof run !== "object") {
    return { ok: false, errors: ["research_run_required"] };
  }
  if (!isResearchRunId(run.run_id)) errors.push("invalid_run_id");
  if (!String(run.hotel_id || "").trim()) errors.push("hotel_id_required");
  if (!isAllowedEnum(run.status, RESEARCH_RUN_STATUSES)) {
    errors.push("invalid_run_status");
  }
  return { ok: errors.length === 0, errors };
}

/**
 * @param {object} obs
 */
export function validateIntelligenceObservation(obs) {
  const shape = validateObservationShape(obs);
  if (!shape.ok) return shape;
  const errors = [...shape.errors];
  if (obs.observation_id && !isObservationId(obs.observation_id)) {
    errors.push("invalid_observation_id");
  }
  return { ok: errors.length === 0, errors };
}

/**
 * @param {object} dossier
 */
export function validateResearchDossier(dossier) {
  const shape = validateDossierShape(dossier);
  if (!shape.ok) return shape;
  const errors = [...shape.errors];
  if (dossier.dossier_id && !isDossierId(dossier.dossier_id)) {
    errors.push("invalid_dossier_id");
  }
  if (Array.isArray(dossier.observation_ids)) {
    for (const id of dossier.observation_ids) {
      if (id && !isObservationId(id)) errors.push("invalid_observation_id_in_dossier");
    }
  }
  return { ok: errors.length === 0, errors };
}

/**
 * Throw if validation fails.
 * @param {{ ok: boolean, errors: string[] }} result
 * @param {string} label
 */
export function assertValid(result, label = "ownership_validation") {
  if (!result?.ok) {
    const msg = `${label}: ${(result?.errors || ["invalid"]).join(",")}`;
    const err = new Error(msg);
    err.code = "ownership_validation_failed";
    err.errors = result?.errors || [];
    throw err;
  }
  return result;
}
