/**
 * Normalized ownership evidence candidate contract (P1).
 */

export const OWNERSHIP_EVIDENCE_PROVIDER_VERSION = "ownership-evidence-provider-v1";

/**
 * @param {object} partial
 */
export function createOwnershipEvidenceCandidate(partial = {}) {
  const entity = partial.entity_candidate || {};
  return {
    schema_version: "ownership-evidence-candidate-v1",
    hotel_id: String(partial.hotel_id || "").trim() || null,
    source_provider: String(partial.source_provider || "").trim(),
    source_country: String(partial.source_country || "").trim() || null,
    source_url: partial.source_url || null,
    source_record_id: partial.source_record_id || null,
    entity_candidate: {
      legal_name: String(entity.legal_name || "").trim(),
      display_name: entity.display_name
        ? String(entity.display_name).trim()
        : String(entity.legal_name || "").trim(),
      jurisdiction: entity.jurisdiction || null,
      identifiers: Array.isArray(entity.identifiers) ? entity.identifiers : [],
    },
    supported_relationship: partial.supported_relationship ?? null,
    source_semantics: String(partial.source_semantics || "").trim(),
    extracted_claim: partial.extracted_claim || null,
    source_authority: Number(partial.source_authority) || 0,
    observed_at: partial.observed_at || new Date().toISOString(),
    effective_at: partial.effective_at ?? null,
    // Entity-resolution-only when relationship is null
    entity_resolution_only: partial.supported_relationship == null,
    max_verification: partial.max_verification || "needs_review",
    adapter_notes: Array.isArray(partial.adapter_notes) ? partial.adapter_notes : [],
  };
}

/**
 * Validate candidate shape (soft).
 * @param {object} c
 */
export function validateOwnershipEvidenceCandidate(c) {
  const errors = [];
  if (!c?.source_provider) errors.push("source_provider_required");
  if (!c?.entity_candidate?.legal_name) errors.push("legal_name_required");
  if (!c?.source_semantics) errors.push("source_semantics_required");
  const rel = c?.supported_relationship;
  if (
    rel != null &&
    ![
      "OWNED_BY",
      "CONTROLLED_BY",
      "SPONSORED_BY",
      "DEVELOPED_BY",
      "OPERATED_BY",
      "ASSET_MANAGED_BY",
    ].includes(rel)
  ) {
    errors.push("unsupported_relationship");
  }
  return { ok: errors.length === 0, errors };
}
