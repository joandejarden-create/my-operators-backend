/**
 * ADP methodology governance V1
 *
 * Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 *
 * Audits may strengthen implementation, validation, entity resolution,
 * reconciliation, regression tests, and release gates.
 * Audits may NOT autonomously alter approved analytical methodology,
 * metric definitions, comparability rules, or measurement semantics.
 */

export const ADP_METHODOLOGY_GOVERNANCE_VERSION = "adp_methodology_governance_v1";

export const ADP_AUDIT_LEARNS_ERROR_PREVENTION_NOT_METHODOLOGY =
  "ADP_AUDIT_LEARNS_ERROR_PREVENTION_NOT_METHODOLOGY";

export const ADP_METHODOLOGY_CHANGE_REQUIRES_EXPLICIT_GOVERNANCE =
  "ADP_METHODOLOGY_CHANGE_REQUIRES_EXPLICIT_GOVERNANCE";

export const ADP_AUDIT_DEFECT_CLASSIFICATION = "ADP_AUDIT_DEFECT_CLASSIFICATION";

/** Permanent Product Builder / ADP doctrine string. */
export const METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN =
  "METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.";

/**
 * What counts as methodology (frozen without founder governance).
 * Implementation may enforce these correctly; it may not redefine them.
 */
export const ADP_FROZEN_METHODOLOGY_SURFACES = Object.freeze([
  "AI_CONSIDERATION_DEFINITION",
  "SCENARIO_PRESENCE_DEFINITION",
  "PRESENCE_INDEX_DEFINITION",
  "SCENARIO_UNIVERSE_DESIGN",
  "PROVIDER_UNIVERSE",
  "DENOMINATOR_RULES",
  "PEER_METHODOLOGY",
  "MEASUREMENT_GRAIN",
  "COMPARABILITY_RULES",
  "MATERIALITY_RULES",
  "CERTIFICATION_POLICY",
  "PROVIDER_WEIGHTING",
  "METRIC_FORMULAS",
]);

/** Audit finding classification (§48). */
export const ADP_AUDIT_DEFECT_CLASS = Object.freeze({
  IMPLEMENTATION_DEFECT: "IMPLEMENTATION_DEFECT",
  DATA_QUALITY_DEFECT: "DATA_QUALITY_DEFECT",
  ENTITY_DEFECT: "ENTITY_DEFECT",
  PRESENTATION_DEFECT: "PRESENTATION_DEFECT",
  METHODOLOGY_REVIEW_REQUIRED: "METHODOLOGY_REVIEW_REQUIRED",
});

export const AUTO_REMEDIABLE_DEFECT_CLASSES = Object.freeze([
  ADP_AUDIT_DEFECT_CLASS.IMPLEMENTATION_DEFECT,
  ADP_AUDIT_DEFECT_CLASS.DATA_QUALITY_DEFECT,
  ADP_AUDIT_DEFECT_CLASS.ENTITY_DEFECT,
  ADP_AUDIT_DEFECT_CLASS.PRESENTATION_DEFECT,
]);

export const METHODOLOGY_CHANGE_REQUEST_STATUS = Object.freeze({
  DRAFT: "DRAFT",
  PENDING_FOUNDER_APPROVAL: "PENDING_FOUNDER_APPROVAL",
  APPROVED: "APPROVED",
  REJECTED: "REJECTED",
  SUPERSEDED: "SUPERSEDED",
});

/**
 * Pre-publication checks verify approved methodology was implemented correctly.
 * They must not ask the system to redesign methodology (§49).
 */
export const ADP_PRE_PUBLICATION_IMPLEMENTATION_CHECKS = Object.freeze([
  "right_hotel_identity",
  "approved_prompts_scenarios",
  "approved_provider_set",
  "subject_presence_path_correct",
  "aliases_consolidated",
  "approved_metric_calculated_correctly",
  "independent_recalculation_parity",
  "cross_surface_same_result",
  "evidence_supports_result",
  "narrative_faithful_to_result",
]);

/**
 * Target operating model (§52).
 */
export const ADP_QUALITY_OPERATING_MODEL = Object.freeze({
  sequence: [
    "APPROVED_METHODOLOGY",
    "IMPLEMENT_CONSISTENTLY",
    "AUTOMATIC_ERROR_CHECKS",
    "INDEPENDENT_AUDIT",
    "FIX_IMPLEMENTATION_DEFECTS",
    "ADD_REGRESSION_PROTECTION",
    "PUBLISH",
  ],
  forbidden: ["AUDIT_THEN_SILENT_METHODOLOGY_CHANGE"],
});

/**
 * Classify whether a proposed remediation may auto-apply.
 */
export function mayAutoRemediateDefect(classification) {
  return AUTO_REMEDIABLE_DEFECT_CLASSES.includes(classification);
}

/**
 * Hard-stop helper when an audit suggests methodology may be flawed.
 * Never apply a methodology change from this path.
 */
export function recommendMethodologyReview({
  issue,
  evidence = null,
  possibleImpact = null,
  proposedFounderQuestion = null,
} = {}) {
  return {
    status: "METHODOLOGY_REVIEW_RECOMMENDED",
    gate: ADP_METHODOLOGY_CHANGE_REQUIRES_EXPLICIT_GOVERNANCE,
    hardStop: true,
    methodologyChanged: false,
    autoRemediate: false,
    classification: ADP_AUDIT_DEFECT_CLASS.METHODOLOGY_REVIEW_REQUIRED,
    issue: issue || "Unspecified methodology concern",
    evidence,
    possibleImpact,
    proposedFounderQuestion:
      proposedFounderQuestion ||
      "Should Dealality change approved ADP measurement methodology for this issue?",
  };
}

/**
 * Build ADP_METHODOLOGY_CHANGE_REQUEST_V1 (§51). Inactive until founder approval.
 */
export function buildMethodologyChangeRequestV1({
  proposedChange,
  reason,
  affectedMetricOrProcess,
  historicalComparabilityImpact,
  affectedProperties = [],
  recommendedEffectivePeriod = null,
  founderApprovalStatus = METHODOLOGY_CHANGE_REQUEST_STATUS.DRAFT,
  requestId = null,
} = {}) {
  if (!proposedChange || !reason || !affectedMetricOrProcess) {
    throw new Error("ADP_METHODOLOGY_CHANGE_REQUEST_V1 requires proposedChange, reason, affectedMetricOrProcess");
  }
  const id =
    requestId ||
    `adp_mcr_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
  return {
    object: "ADP_METHODOLOGY_CHANGE_REQUEST_V1",
    requestId: id,
    proposedChange,
    reason,
    affectedMetricOrProcess,
    historicalComparabilityImpact: historicalComparabilityImpact || null,
    affectedProperties: [...affectedProperties],
    recommendedEffectivePeriod,
    founderApprovalStatus,
    active: founderApprovalStatus === METHODOLOGY_CHANGE_REQUEST_STATUS.APPROVED,
    createdAt: new Date().toISOString(),
    doctrine: METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
  };
}

/**
 * Assert a remediation ledger entry did not change methodology.
 */
export function assertRemediationDidNotChangeMethodology(entry) {
  const methodologyChanged = Boolean(entry?.methodologyChanged);
  if (methodologyChanged === true) {
    return {
      ok: false,
      code: "METHODOLOGY_CHANGED_WITHOUT_GOVERNANCE",
      message:
        "Remediation marked methodologyChanged=true without an approved ADP_METHODOLOGY_CHANGE_REQUEST_V1",
    };
  }
  if (entry?.classification === ADP_AUDIT_DEFECT_CLASS.METHODOLOGY_REVIEW_REQUIRED) {
    if (entry?.autoRemediated === true) {
      return {
        ok: false,
        code: "METHODOLOGY_REVIEW_AUTO_REMEDIATED",
        message: "METHODOLOGY_REVIEW_REQUIRED must never auto-remediate",
      };
    }
  }
  return { ok: true };
}

export function evaluateAuditLearningGovernance(ledgerEntries = [], methodologyReviews = []) {
  const results = (ledgerEntries || []).map((e) => ({
    defectId: e.defectId,
    ...assertRemediationDidNotChangeMethodology(e),
  }));
  const failed = results.filter((r) => !r.ok);
  const illicitReviews = (methodologyReviews || []).filter((r) => r.autoRemediated === true);
  return {
    gate: ADP_AUDIT_LEARNS_ERROR_PREVENTION_NOT_METHODOLOGY,
    doctrine: METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
    pass: failed.length === 0 && illicitReviews.length === 0,
    methodologyChangedAny: (ledgerEntries || []).some((e) => e.methodologyChanged === true),
    entryResults: results,
    founderMethodologyReviewsRequired: methodologyReviews || [],
    illicitAutoRemediations: illicitReviews,
  };
}
