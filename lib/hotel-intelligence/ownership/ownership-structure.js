/**
 * Ownership structure taxonomy + evidence-backed classifier (P1.6 / A′-BR-04).
 * Supplements the relationship graph — does not replace it.
 */

export const OWNERSHIP_STRUCTURE_VERSION = "ownership-structure-v1";

export const OWNERSHIP_STRUCTURES = Object.freeze([
  "single_corporate_owner",
  "family_controlled",
  "individual_owner",
  "propco_holdco",
  "institutional_fund",
  "joint_venture",
  "condo_hotel",
  "distributed_unit_ownership",
  "lease_lessor_structure",
  "trust_fideicomiso",
  "government_public",
  "mixed_structure",
  "unknown",
]);

/**
 * @param {object} signals
 * @returns {{ structure: string, confidence: number, verification: string, reasons: string[] }}
 */
export function classifyOwnershipStructure(signals = {}) {
  const reasons = [];
  let structure = "unknown";
  let confidence = 0.35;

  if (signals.condo_hotel || signals.distributed_units) {
    structure = signals.condo_hotel ? "condo_hotel" : "distributed_unit_ownership";
    confidence = signals.condo_hotel_evidence_strong ? 0.82 : 0.68;
    reasons.push("condo_or_fractional_unit_evidence");
    if (signals.no_single_corporate_propco) reasons.push("no_single_corporate_propco");
  } else if (signals.lease_lessor && signals.operating_entity) {
    structure = "lease_lessor_structure";
    confidence = signals.lease_disclosure ? 0.78 : 0.58;
    reasons.push("lease_structure_detected");
  } else if (signals.individual_ubo && !signals.corporate_parent) {
    structure = "individual_owner";
    confidence = signals.qsa_sole_partner ? 0.85 : 0.72;
    reasons.push("natural_person_ubo_from_qsa");
  } else if (signals.family_qsa_pattern && !signals.institutional_sponsor) {
    structure = "family_controlled";
    confidence = 0.7;
    reasons.push("multiple_related_individual_partners");
  } else if (signals.institutional_sponsor || signals.fund_entity) {
    structure = "institutional_fund";
    confidence = 0.75;
    reasons.push("institutional_sponsor_signal");
  } else if (signals.propco_spv && signals.corporate_parent) {
    structure = "propco_holdco";
    confidence = 0.72;
    reasons.push("propco_with_parent_chain");
  } else if (signals.jv_evidence) {
    structure = "joint_venture";
    confidence = 0.65;
    reasons.push("jv_language");
  } else if (signals.trust_fideicomiso) {
    structure = "trust_fideicomiso";
    confidence = 0.6;
    reasons.push("trust_structure_language");
  } else if (signals.single_corporate_owned) {
    structure = "single_corporate_owner";
    confidence = 0.7;
    reasons.push("single_corporate_owner_signal");
  }

  if (signals.mixed_signals) {
    structure = "mixed_structure";
    confidence = Math.min(confidence, 0.55);
    reasons.push("conflicting_structure_signals");
  }

  let verification = "unknown";
  if (confidence >= 0.85) verification = "high";
  else if (confidence >= 0.7) verification = "probable";
  else if (confidence >= 0.5) verification = "needs_review";

  return { structure, confidence, verification, reasons };
}

/**
 * Derive classifier signals from Brazil corporate stage output + graph hints.
 * @param {object} ctx
 */
export function buildStructureSignalsFromResearch(ctx = {}) {
  const metrics = ctx.brazil_corporate?.metrics || {};
  const notes = [
    ...(ctx.brazil_corporate?.notes || []),
    ...(ctx.franchise_disclosure?.notes || []),
    ...(ctx.review_notes || []),
  ].join(" ").toLowerCase();

  return {
    condo_hotel:
      /condo|condom[ií]nio|fractional|unit owner|apart[\s-]?hotel|pool hotel/i.test(
        notes
      ) || Boolean(metrics.condo_signals),
    distributed_units:
      /distributed|fragmented|multiple unit|sem propCo [úu]nico|no single/i.test(
        notes
      ),
    condo_hotel_evidence_strong: /condo-hotel|condominio edilicio|unit owners/i.test(
      notes
    ),
    no_single_corporate_propco: /no single|not evidenced|distributed/i.test(notes),
    lease_lessor:
      Boolean(metrics.lease_disclosures) ||
      /lease|loca[cç][aã]o|leased from|lessor|locador/i.test(notes),
    lease_disclosure: Boolean(metrics.lease_disclosures),
    operating_entity: Boolean(metrics.qsa_partners || metrics.matrix_resolved),
    individual_ubo: Boolean(metrics.individual_controllers),
    qsa_sole_partner: Boolean(metrics.sole_individual_partner),
    family_qsa_pattern: Number(metrics.individual_controllers || 0) >= 2,
    institutional_sponsor: Boolean(metrics.institutional_signals),
    fund_entity: /fund|fii|reit|private equity|investimento/i.test(notes),
    propco_spv: Boolean(metrics.propco_candidates),
    corporate_parent: Boolean(metrics.parent_candidates),
    jv_evidence: /joint venture|jv |em parceria com/i.test(notes),
    trust_fideicomiso: /fideicomiso|trust structure|fiduci/i.test(notes),
    single_corporate_owned: Boolean(metrics.single_corporate_owner),
    mixed_signals:
      /false seed|reject|ambiguous/i.test(notes) &&
      /condo|family|individual/i.test(notes),
  };
}
