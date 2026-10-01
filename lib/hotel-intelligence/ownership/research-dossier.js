/**
 * Ownership research dossier — internal intelligence artifact.
 * Preserves how an answer was discovered without storing chain-of-thought.
 *
 * Webhound / deep-research outputs enter here as candidate evidence,
 * never as product graph truth.
 */

import { generateDossierId } from "./ids.js";

export const RESEARCH_DOSSIER_VERSION = "ownership-research-dossier-v1";

function nowIso() {
  return new Date().toISOString();
}

/**
 * Structured provenance step (reproducible, not private reasoning).
 * @param {Partial<object>} [partial]
 */
export function createProvenanceStep(partial = {}) {
  return {
    step: Number(partial.step || 0),
    method: partial.method || "other",
    finding: String(partial.finding || "").trim(),
    source_url: partial.source_url ?? null,
  };
}

/**
 * @param {Partial<object>} [partial]
 */
export function createResearchDossier(partial = {}) {
  const now = nowIso();
  return {
    schema_version: RESEARCH_DOSSIER_VERSION,
    dossier_id: partial.dossier_id || generateDossierId(),
    hotel_id: String(partial.hotel_id || "").trim(),
    entity_id: partial.entity_id ?? null,
    research_run_id: partial.research_run_id ?? null,
    seed: partial.seed && typeof partial.seed === "object" ? { ...partial.seed } : null,
    sources_consulted: Array.isArray(partial.sources_consulted)
      ? [...partial.sources_consulted]
      : [],
    decisive_sources: Array.isArray(partial.decisive_sources)
      ? [...partial.decisive_sources]
      : [],
    failed_sources: Array.isArray(partial.failed_sources)
      ? [...partial.failed_sources]
      : [],
    observation_ids: Array.isArray(partial.observation_ids)
      ? [...partial.observation_ids]
      : [],
    candidate_relationship_ids: Array.isArray(partial.candidate_relationship_ids)
      ? [...partial.candidate_relationship_ids]
      : [],
    validated_relationship_ids: Array.isArray(partial.validated_relationship_ids)
      ? [...partial.validated_relationship_ids]
      : [],
    unresolved_questions: Array.isArray(partial.unresolved_questions)
      ? [...partial.unresolved_questions]
      : [],
    research_methods: Array.isArray(partial.research_methods)
      ? [...partial.research_methods]
      : [],
    provenance_steps: Array.isArray(partial.provenance_steps)
      ? [...partial.provenance_steps]
      : [],
    citations: Array.isArray(partial.citations) ? [...partial.citations] : [],
    cost_usd: partial.cost_usd != null ? Number(partial.cost_usd) : null,
    latency_ms: partial.latency_ms != null ? Number(partial.latency_ms) : null,
    source_provider: partial.source_provider || "dealality_native",
    product_truth: partial.product_truth === true,
    notes: partial.notes ?? null,
    created_at: partial.created_at || now,
    updated_at: partial.updated_at || now,
  };
}

/**
 * @param {object} dossier
 */
export function validateResearchDossier(dossier) {
  const errors = [];
  if (!dossier || typeof dossier !== "object") {
    return { ok: false, errors: ["dossier_required"] };
  }
  if (!String(dossier.dossier_id || "").startsWith("dod_")) {
    errors.push("invalid_dossier_id");
  }
  if (!String(dossier.hotel_id || "").trim()) errors.push("hotel_id_required");
  return { ok: errors.length === 0, errors };
}

/**
 * Build provenance steps from Brazil corporate notes (structured, not CoT).
 * @param {string[]} notes
 */
export function provenanceStepsFromBrazilNotes(notes = []) {
  const steps = [];
  let i = 1;
  for (const n of notes || []) {
    const s = String(n);
    if (s.startsWith("explicit_seed_rejected:") || s.startsWith("seed_rejected:") || s.startsWith("cadastur_seed_rejected:")) {
      steps.push(createProvenanceStep({
        step: i++,
        method: "cnpj_validation",
        finding: `Rejected registry seed ${s.split(":").slice(1).join(":")}`,
      }));
    } else if (s.startsWith("branch_to_matrix:")) {
      steps.push(createProvenanceStep({
        step: i++,
        method: "branch_to_matrix",
        finding: `Resolved branch to matrix ${s.slice("branch_to_matrix:".length)}`,
      }));
    } else if (s === "family_qsa_pattern_detected") {
      steps.push(createProvenanceStep({
        step: i++,
        method: "qsa_partner_climb",
        finding: "QSA shows multiple individual partners consistent with family control",
      }));
    } else if (s.startsWith("discovered_cnpj_rejected:")) {
      steps.push(createProvenanceStep({
        step: i++,
        method: "hotel_name_to_company",
        finding: `Discovered CNPJ rejected after seed validation: ${s.slice("discovered_cnpj_rejected:".length)}`,
      }));
    }
  }
  return steps;
}
