/**
 * Packet 2.6B — normalized Full Hotel Intelligence Investigation schema.
 * Provider-neutral presentation model. Does not auto-promote to canonical HI.
 */

import {
  DOSSIER_TYPE,
  DOSSIER_TYPE_RESEARCH_ADDENDUM,
  DOSSIER_TITLE,
  DOSSIER_SECTION_IDS,
  isValidDossierStatus,
  isValidFindingStatus,
  isValidDossierType,
} from "./statuses.js";

export const DOSSIER_SCHEMA_VERSION = "hotel-intelligence-dossier-v1.1";

function nowIso() {
  return new Date().toISOString();
}

function asArray(v) {
  return Array.isArray(v) ? v : [];
}

/**
 * @param {Partial<object>} [partial]
 */
export function createEmptyDossier(partial = {}) {
  const now = nowIso();
  return {
    schema_version: DOSSIER_SCHEMA_VERSION,
    dossier_id: String(partial.dossier_id || "").trim() || null,
    hotel_id: String(partial.hotel_id || "").trim() || null,
    hotel_airtable_record_id: partial.hotel_airtable_record_id || null,
    hotel_name: partial.hotel_name || null,
    dossier_type: DOSSIER_TYPE,
    title: partial.title || DOSSIER_TITLE,
    status: partial.status || "NOT_STARTED",
    research_provider: partial.research_provider || null,
    research_run_id: partial.research_run_id || null,
    version: partial.version != null ? Number(partial.version) || 1 : 1,
    created_at: partial.created_at || now,
    started_at: partial.started_at || null,
    completed_at: partial.completed_at || null,
    updated_at: partial.updated_at || now,
    research_cost_usd: partial.research_cost_usd != null ? Number(partial.research_cost_usd) : null,
    source_count: 0,
    finding_count: 0,
    open_question_count: 0,
    executive_summary: {
      paragraphs: asArray(partial.executive_summary?.paragraphs),
    },
    key_findings: asArray(partial.key_findings),
    sections: asArray(partial.sections),
    findings: asArray(partial.findings),
    entities: asArray(partial.entities),
    relationships: asArray(partial.relationships),
    people: asArray(partial.people),
    events: asArray(partial.events),
    open_questions: asArray(partial.open_questions),
    sources: asArray(partial.sources),
    research_methods: asArray(partial.research_methods),
    methodology_summary: partial.methodology_summary || null,
    raw_artifact_reference: partial.raw_artifact_reference || null,
    mapping_notes: asArray(partial.mapping_notes),
    claim_handoff: {
      auto_promote: false,
      candidates: asArray(partial.claim_handoff?.candidates),
    },
  };
}

/**
 * Recompute counts from nested arrays.
 * @param {object} dossier
 */
export function refreshDossierCounts(dossier) {
  if (!dossier || typeof dossier !== "object") return dossier;
  dossier.source_count = asArray(dossier.sources).length;
  dossier.finding_count =
    asArray(dossier.key_findings).length || asArray(dossier.findings).length;
  dossier.open_question_count = asArray(dossier.open_questions).length;
  dossier.updated_at = nowIso();
  return dossier;
}

/**
 * Lightweight structural validation (not JSON Schema).
 * @param {object} dossier
 * @returns {{ ok: boolean, errors: string[] }}
 */
export function validateDossier(dossier) {
  const errors = [];
  if (!dossier || typeof dossier !== "object") {
    return { ok: false, errors: ["dossier_missing"] };
  }
  if (dossier.schema_version !== DOSSIER_SCHEMA_VERSION) {
    // Allow prior v1 fixtures during transition, but prefer v1.1.
    if (dossier.schema_version !== "hotel-intelligence-dossier-v1") {
      errors.push("schema_version_mismatch");
    }
  }
  if (!isValidDossierType(dossier.dossier_type)) {
    errors.push("dossier_type_invalid");
  }
  if (!dossier.dossier_id) errors.push("dossier_id_required");
  if (!dossier.hotel_id && !dossier.hotel_airtable_record_id) {
    errors.push("hotel_identity_required");
  }
  if (!isValidDossierStatus(dossier.status)) {
    errors.push("status_invalid");
  }
  for (const f of asArray(dossier.key_findings)) {
    if (f.status && !isValidFindingStatus(f.status)) {
      errors.push(`key_finding_status_invalid:${f.id || f.headline || "?"}`);
    }
  }
  for (const f of asArray(dossier.findings)) {
    if (f.status && !isValidFindingStatus(f.status)) {
      errors.push(`finding_status_invalid:${f.id || "?"}`);
    }
  }
  for (const s of asArray(dossier.sections)) {
    if (s.id && !DOSSIER_SECTION_IDS.includes(s.id)) {
      errors.push(`section_id_unknown:${s.id}`);
    }
  }
  if (dossier.claim_handoff && dossier.claim_handoff.auto_promote === true) {
    errors.push("claim_handoff_auto_promote_forbidden");
  }
  return { ok: errors.length === 0, errors };
}

/**
 * Library card projection (list endpoint).
 * @param {object} dossier
 */
export function toLibraryCard(dossier) {
  return {
    dossier_id: dossier.dossier_id,
    report_id: dossier.dossier_id,
    hotel_id: dossier.hotel_id,
    hotel_airtable_record_id: dossier.hotel_airtable_record_id,
    hotel_name: dossier.hotel_name,
    dossier_type: dossier.dossier_type,
    report_type: dossier.dossier_type || dossier.report_type || null,
    template_id: dossier.template_id || null,
    title: dossier.title,
    status: dossier.status,
    research_provider: dossier.research_provider,
    research_run_id: dossier.research_run_id,
    version: dossier.version,
    completed_at: dossier.completed_at,
    updated_at: dossier.updated_at,
    source_count: dossier.source_count,
    finding_count: dossier.finding_count,
    open_question_count: dossier.open_question_count,
    display_location:
      dossier.hotel_location?.label ||
      dossier.report_hotel_identity?.display_location ||
      null,
  };
}

export {
  DOSSIER_TYPE,
  DOSSIER_TYPE_RESEARCH_ADDENDUM,
  DOSSIER_TITLE,
  DOSSIER_TITLE_RESEARCH_ADDENDUM,
  DOSSIER_SECTION_IDS,
  DOSSIER_SECTION_TITLES,
  DOSSIER_STATUSES,
  FINDING_STATUSES,
  FINDING_STATUS_LABELS,
  isValidDossierType,
} from "./statuses.js";
