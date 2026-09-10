/**
 * ADP Executive Read Composition V3 — proposed canonical composition contract.
 * PREVIEW / ACTIVATION-READYNESS ONLY until founder activates.
 *
 * Doctrine:
 *   METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 *   EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.
 *
 * Governs interpretation + narrative composition.
 * Does NOT redefine measurement methodology.
 */

export const ADP_EXECUTIVE_READ_COMPOSITION_V3 = "ADP_EXECUTIVE_READ_COMPOSITION_V3";
export const EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION_MEASUREMENT_REMAINS_GOVERNED =
  "EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.";

export const COMPOSITION_V3_STATUS = Object.freeze({
  proposed: false,
  activated: true,
  activatedAt: "2026-09-10T09:00:00.000Z",
  activationMode: "CUSTOMER_DEFAULT_RESTORED_PRODUCTION_BASELINE_V1",
  priorPartialActivation: Object.freeze({
    wasActivated: true,
    rolledBackAt: "2026-09-09T14:45:00.000Z",
    reason: "ADP_V3_PARTIAL_ACTIVATION_FAILSAFE_RESTORE",
    blocker: "REAL_BROWSER_FOUNDER_SEVEN_FAIL",
    note: "Failsafe rollback restored pre-activation writeup hashes and flipped activated=false, which silently reintroduced legacy Executive Read customer rendering despite stamped V3 compositionVersion on published editions.",
  }),
  restoredAt: "2026-09-10T09:00:00.000Z",
  restoreReason:
    "APPROVED_CUSTOMER_OUTPUT_IS_A_VERSIONED_PRODUCTION CONTRACT — stored ADP_EXECUTIVE_READ_COMPOSITION_V3 must render V3; failsafe flag must not silently downgrade customer surface.",
  note: "Customer default V3. Stored compositionVersion overrides generation flags. Invalid V3 sections fail closed (no silent legacy).",
});

export const COMPOSITION_V3_SECTIONS = Object.freeze([
  "HEADLINE",
  "KEY_INSIGHT",
  "WHY_IT_MATTERS",
  "FOCUS_NOW",
  "WATCH", // optional
  "WHAT_TO_REVIEW",
]);

export const COMPOSITION_V3_RULES = Object.freeze({
  targetWordCount: { min: 160, preferredMin: 180, preferredMax: 280, hardMax: 325 },
  headlineWords: { min: 12, max: 35 },
  keyInsightWords: { min: 45, max: 120 },
  whyItMattersWords: { min: 30, max: 90 },
  focusNowWords: { min: 35, max: 110 },
  whatToReviewWords: { min: 28, max: 90 },
  numericAnchorsPreferred: { min: 1, max: 3 },
  numericAnchorsHardMax: 4,
  numericAnchorDefaultSection: "KEY_INSIGHT",
  noKpiEnumeration: true,
  noImpliedCausation: true,
  noCustomerFacingMethodologyDefense: true,
  clearNotClever: true,
  customerLanguageLock: true,
  onePrimaryFocus: true,
  watchOptional: true,
  watchMustEarnFirst90Seconds: true,
  whatToReviewIncremental: true,
  sectionTraceability: true,
  vsMonthlyActionLayerSeparation: true,
  reactionScoreDiagnosticOnly: true,
  reactionScoreNotReleaseAuthority: true,
  futurePropertyZeroCodePath: true,
  noHardCodedHotelNarrativesInProductionComposer: true,
  doesNotRedefineMeasurement: true,
});

export const COMPOSITION_V3_GATES = Object.freeze([
  "ADP_EXECUTIVE_SUMMARY_MUST_ADD_ANALYTICAL_VALUE",
  "ADP_EXECUTIVE_SUMMARY_PROPERTY_SPECIFICITY",
  "ADP_EXECUTIVE_KEY_INSIGHT_SUBSTANTIVE",
  "ADP_EXECUTIVE_WHY_IT_MATTERS_SUBSTANTIVE",
  "ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY",
  "ADP_EXECUTIVE_FOCUS_NOW_FINAL_QUALITY",
  "ADP_EXECUTIVE_WHAT_TO_REVIEW_ACTIONABLE",
  "ADP_EXECUTIVE_WHAT_TO_REVIEW_INCREMENTAL_VALUE",
  "ADP_EXECUTIVE_SELECTIVE_NUMERIC_ANCHORS",
  "ADP_EXECUTIVE_NO_KPI_ENUMERATION",
  "ADP_EXECUTIVE_NUMERIC_REFERENCE_PARITY",
  "ADP_EXECUTIVE_NUMERIC_CONTRAST_VALUE",
  "ADP_EXECUTIVE_NO_IMPLIED_CAUSATION",
  "ADP_EXECUTIVE_CLEAR_NOT_CLEVER",
  "ADP_EXECUTIVE_CUSTOMER_LANGUAGE_LOCK",
  "ADP_EXECUTIVE_NO_CUSTOMER_FACING_METHODOLOGY_DEFENSE",
  "ADP_EXECUTIVE_WATCH_EXECUTIVE_MATERIALITY",
  "ADP_EXECUTIVE_READ_SECTION_TRACEABILITY",
  "ADP_EXECUTIVE_READ_VS_MONTHLY_ACTION_LAYER_SEPARATION",
  "ADP_EXECUTIVE_V3_FOUNDER_READ_TEST",
  "ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ZERO_CODE_PATH",
  "ADP_EXECUTIVE_READ_CROSS_SURFACE_CANONICAL_PARITY",
  "ADP_EXECUTIVE_NO_PROPERTY_HARDCODED_NARRATIVE",
  "ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY",
]);

export const COMPOSITION_V3_MAY_GOVERN = Object.freeze([
  "insight selection",
  "narrative composition",
  "selective numeric anchors",
  "one Primary Focus",
  "optional Watch",
  "What to Review",
  "section traceability",
  "property specificity",
  "claim discipline",
  "customer language",
]);

export const COMPOSITION_V3_MUST_NOT_GOVERN = Object.freeze([
  "metric methodology",
  "formulas",
  "scenarios",
  "provider universe",
  "peer methodology",
  "denominator policy",
  "comparability",
  "Measurement Assurance methodology",
  "certification",
]);

/**
 * Future-property composition path (required before activation).
 * Production must generate V3 from certified analytical state — not hard-coded hotel copy.
 */
export const COMPOSITION_V3_FUTURE_PROPERTY_PIPELINE = Object.freeze([
  "canonical measurement",
  "certified analytical state",
  "candidate insight generation",
  "strongest insight selection",
  "Primary Focus selection",
  "selective numeric anchors",
  "V3 composition",
  "traceability",
  "V3 quality gates",
  "client-readiness gates",
]);

export const COMPOSITION_V3_HISTORICAL_VERSIONING_POLICY = Object.freeze({
  rule: "COMPOSITION_VERSION_CHANGE_NOT_SILENT_HISTORY_MUTATION",
  currentUnpublishedOrNonDistributed: "May regenerate as new immutable composition version under governed republish when activation permits",
  previouslyIssuedReports: "Do not silently rewrite customer-issued snapshots; retain issued narrative under prior compositionVersion",
  futureRegeneratedVersions: "New period or CORRECTED edition may use V3; preserve lineage (periodId, reportEdition, compositionVersion)",
  immutableHistoricalSnapshots: "Issued share/snapshot narrative remains immutable; share tokens may point at current_published only by explicit policy — never backfill V3 into issued history without founder approval",
  requiredPayloadFields: Object.freeze([
    "compositionVersion",
    "compositionContract",
    "sections",
    "numericAnchors",
    "evidenceTrace",
    "primaryIssueId",
  ]),
});

export const COMPOSITION_V3_CROSS_SURFACE_CANONICAL = Object.freeze({
  canonicalRecord: "published report payload.executiveRead (compositionVersion=ADP_EXECUTIVE_READ_COMPOSITION_V3)",
  surfaces: Object.freeze([
    "owner dashboard (owner-ai-demand.html)",
    "signed customer share (owner-ai-demand-share.html)",
    "print/PDF (not yet implemented for ADP ER — required before full activation)",
    "monthly review references (must cite same composition record, not parallel copy)",
  ]),
  parityRule: "ADP_EXECUTIVE_READ_CROSS_SURFACE_CANONICAL_PARITY",
});

export function proposeExecutiveReadCompositionV3() {
  return {
    object: ADP_EXECUTIVE_READ_COMPOSITION_V3,
    status: COMPOSITION_V3_STATUS,
    doctrine: [
      "METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.",
      EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION_MEASUREMENT_REMAINS_GOVERNED,
    ],
    sections: COMPOSITION_V3_SECTIONS,
    rules: COMPOSITION_V3_RULES,
    gates: COMPOSITION_V3_GATES,
    mayGovern: COMPOSITION_V3_MAY_GOVERN,
    mustNotGovern: COMPOSITION_V3_MUST_NOT_GOVERN,
    futurePropertyPipeline: COMPOSITION_V3_FUTURE_PROPERTY_PIPELINE,
    historicalVersioningPolicy: COMPOSITION_V3_HISTORICAL_VERSIONING_POLICY,
    crossSurfaceCanonical: COMPOSITION_V3_CROSS_SURFACE_CANONICAL,
    activated: COMPOSITION_V3_STATUS.activated === true,
  };
}
