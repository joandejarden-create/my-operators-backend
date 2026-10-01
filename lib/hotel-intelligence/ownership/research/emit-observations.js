/**
 * Emit intelligence observations from native ownership research stages.
 * These stay observation_only unless independently verified for the graph.
 */

import { createIntelligenceObservation } from "../../intelligence-observation.js";

/**
 * @param {object} ctx
 * @param {{ brazil?: object, franchise?: object, structure?: object, review?: object[] }} stages
 * @returns {object[]}
 */
export function emitNativeResearchObservations(ctx, stages = {}) {
  const hotelId = ctx.hotelId || ctx.hotel?.hotel_id;
  const runId = ctx.runId || null;
  const out = [];
  const brazil = stages.brazil || {};
  const metrics = brazil.metrics || {};
  const notes = brazil.notes || [];

  for (const n of notes) {
    if (/seed_rejected|explicit_seed_rejected|cadastur_seed_rejected/.test(String(n))) {
      out.push(
        createIntelligenceObservation({
          subject_type: "hotel",
          subject_id: hotelId,
          observation_type: "seed_rejected",
          intelligence_class: "corporate_identity_fact",
          finding: String(n),
          source_provider: "brasilapi_cnpj",
          verification_status: "high",
          confidence: 0.88,
          research_method: "cnpj_validation",
          research_run_id: runId,
          promotion_status: "rejected_for_graph",
          product_truth: false,
        })
      );
    }
  }

  if (brazil.primary_seed?.validation?.status && ["VALIDATED", "PROBABLE"].includes(brazil.primary_seed.validation.status)) {
    const reg = brazil.primary_seed.registry || {};
    out.push(
      createIntelligenceObservation({
        subject_type: "hotel",
        subject_id: hotelId,
        observation_type: "seed_validated",
        intelligence_class: "corporate_identity_fact",
        finding: `Validated CNPJ seed ${reg.cnpj_formatted || reg.cnpj}: ${reg.razao_social || ""} (${brazil.primary_seed.validation.status})`,
        source_provider: "brasilapi_cnpj",
        verification_status: brazil.primary_seed.validation.status === "VALIDATED" ? "high" : "probable",
        confidence: brazil.primary_seed.validation.composite_score || 0.7,
        research_method: "cnpj_validation",
        research_run_id: runId,
        related_names: [reg.razao_social].filter(Boolean),
        product_truth: false,
      })
    );
  }

  if (metrics.matrix_resolved) {
    out.push(
      createIntelligenceObservation({
        subject_type: "hotel",
        subject_id: hotelId,
        observation_type: "branch_matrix",
        intelligence_class: "corporate_identity_fact",
        finding: notes.find((n) => String(n).startsWith("branch_to_matrix:")) || "Branch resolved to matrix CNPJ",
        source_provider: "brasilapi_cnpj",
        verification_status: "high",
        confidence: 0.92,
        research_method: "branch_to_matrix",
        research_run_id: runId,
        intended_relationship_type: "CONTROLLED_BY",
        promotion_status: "observation_only",
        product_truth: false,
      })
    );
  }

  if (metrics.qsa_partners > 0) {
    out.push(
      createIntelligenceObservation({
        subject_type: "hotel",
        subject_id: hotelId,
        observation_type: "shareholder_qsa",
        intelligence_class: "corporate_identity_fact",
        finding: `QSA extracted ${metrics.qsa_partners} partner(s); ${metrics.individual_controllers || 0} individual(s). Entity CONTROLLED_BY edges are corporate, not hotel OWNED_BY.`,
        source_provider: "brasilapi_qsa",
        verification_status: "probable",
        confidence: 0.8,
        research_method: "qsa_partner_climb",
        research_run_id: runId,
        intended_relationship_type: "CONTROLLED_BY",
        product_truth: false,
      })
    );
  }

  for (const c of brazil.candidates || []) {
    if (c.propco_hint) {
      out.push(
        createIntelligenceObservation({
          subject_type: "hotel",
          subject_id: hotelId,
          observation_type: "propco_candidate",
          intelligence_class: "unresolved_candidate",
          finding: `Related entity PropCo candidate (partner overlap, not hotel ownership proof): ${c.entity?.legal_name || "unknown"}`,
          source_provider: "brasilapi_cnpj",
          verification_status: "needs_review",
          confidence: 0.55,
          research_method: "related_entity_discovery",
          research_run_id: runId,
          intended_relationship_type: "OWNED_BY",
          related_entity_ids: c.entity?.entity_id ? [c.entity.entity_id] : [],
          product_truth: false,
        })
      );
    }
  }

  const structure = stages.structure;
  if (structure?.structure && structure.structure !== "unknown") {
    out.push(
      createIntelligenceObservation({
        subject_type: "hotel",
        subject_id: hotelId,
        observation_type: "ownership_structure",
        intelligence_class: "ownership_structure_signal",
        finding: `Ownership structure classified as ${structure.structure} (${structure.verification || "unknown"}; reasons: ${(structure.reasons || []).join(", ")})`,
        source_provider: "dealality_classifier",
        verification_status: structure.verification === "high" ? "high" : "probable",
        confidence: structure.confidence || 0.5,
        research_method: "condo_structure_detection",
        research_run_id: runId,
        product_truth: false,
      })
    );
  }

  const franchise = stages.franchise || {};
  if ((franchise.metrics?.lease_disclosures || 0) > 0) {
    out.push(
      createIntelligenceObservation({
        subject_type: "hotel",
        subject_id: hotelId,
        observation_type: "lessor",
        intelligence_class: "canonical_relationship_candidate",
        finding: `Franchise/financial disclosure lane extracted ${franchise.metrics.lease_disclosures} lease/lessor claim(s)`,
        source_provider: "franchisor_filing",
        verification_status: "needs_review",
        confidence: 0.7,
        research_method: "franchisor_financial_disclosure",
        research_run_id: runId,
        intended_relationship_type: "LEASED_FROM",
        product_truth: false,
      })
    );
  }

  return out;
}
