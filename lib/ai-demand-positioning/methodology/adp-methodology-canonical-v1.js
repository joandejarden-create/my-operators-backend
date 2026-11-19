/**
 * ADP_METHODOLOGY_CANONICAL_V1
 *
 * Authoritative methodology fingerprint for hotel ADP (not Brand AI).
 * Derived from approved code paths — does not invent new scoring formulas.
 */

export const ADP_METHODOLOGY_CANONICAL_V1 = "ADP_METHODOLOGY_CANONICAL_V1";

/** Latest founder-approved customer methodology fingerprint. */
export const ADP_METHODOLOGY_FINGERPRINT_CURRENT = Object.freeze({
  productVersion: "ai_demand_positioning_v1",
  measurementContractVersion: "ADP_MEASUREMENT_CONTRACT_V1",
  appearanceMethodologyId: "ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1",
  appearanceContractVersion: "1.1.1",
  presenceIndexMethodologyId: "adp_governed_ai_presence_index_v2",
  realityGapMethodologyId: "RG_WEIGHTED_PARTIAL_V2",
  realityGapHistoricalImmutableId: "RG_BINARY_GAP_V1",
  executiveCompositionVersion: "ADP_EXECUTIVE_READ_COMPOSITION_V3",
  bppMetricsVersion: "ADP_BRAND_PORTFOLIO_METRICS_V1_1",
  bppPublicationPack: "bpp-customer-v1.1-20260902-p2",
  metricSemanticsRegistry: "ADP_METRIC_SEMANTICS_REGISTRY_V1",
  performanceReviewBuilder: "ADP_MONTHLY_REVIEW_BUILDER_V1",
  performanceReviewTemplate: "AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1",
  performanceReviewPdf: "AdpMonthlyReviewReportPdfV1",
  rendererAssetVersion: "adp-v91-bpp-rank-only-parity-20260910",
});

export const ADP_METHODOLOGY_COMPATIBILITY = Object.freeze({
  CURRENT_COMPARABLE: "CURRENT_COMPARABLE",
  RECOMPUTABLE: "RECOMPUTABLE",
  PARTIALLY_COMPARABLE: "PARTIALLY_COMPARABLE",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
  EVIDENCE_INSUFFICIENT: "EVIDENCE_INSUFFICIENT",
  BLOCKED_SYNTHETIC: "BLOCKED_SYNTHETIC",
});

/** Fields that can be deterministically recomputed from durable evidence without paid research. */
export const ADP_METHODOLOGY_RECOMPUTABLE_FIELDS = Object.freeze([
  "realityGapMethodologyId",
  "executiveCompositionVersion",
  "appearanceMethodologyId",
  "presenceIndexMethodologyId",
]);

export function listCanonicalMethodologyKeys() {
  return Object.keys(ADP_METHODOLOGY_FINGERPRINT_CURRENT);
}

/**
 * Compact provenance block for archive/admin metadata (no customer UI clutter).
 * @param {object} provenance
 */
export function buildAdminMethodologyProvenanceBlock(provenance = {}) {
  return {
    schema: "ADP_METHODOLOGY_PROVENANCE_V1",
    capturedAt: new Date().toISOString(),
    researchPeriodId: provenance.researchPeriodId || null,
    researchPublishedAt: provenance.researchPublishedAt || null,
    productVersion: provenance.productVersion || null,
    measurementContractVersion: provenance.measurementContractVersion || null,
    appearanceMethodologyId: provenance.appearanceMethodologyId || null,
    realityGapMethodologyId: provenance.realityGapMethodologyId || null,
    presenceIndexMethodologyId: provenance.presenceIndexMethodologyId || null,
    executiveCompositionVersion: provenance.executiveCompositionVersion || null,
    reportTemplateVersion: provenance.reportTemplateVersion || null,
    reportContentVersion: provenance.reportContentVersion || null,
    presentationBundleVersion: provenance.presentationBundleVersion || null,
    evidenceSource: provenance.evidenceSource || null,
    sourceStagedVersionId: provenance.sourceStagedVersionId || null,
    compatibility: provenance.compatibility || null,
    canonicalFingerprintId: ADP_METHODOLOGY_CANONICAL_V1,
  };
}
