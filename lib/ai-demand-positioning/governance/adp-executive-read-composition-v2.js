/**
 * ADP Executive Read Composition V2 — PREVIEW ONLY (not activated).
 * Interpretation / composition contract. Does NOT redefine measurement.
 *
 * Doctrine:
 *   METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 *   EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.
 */

export const ADP_EXECUTIVE_READ_COMPOSITION_V2 = "ADP_EXECUTIVE_READ_COMPOSITION_V2";
export const EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION_MEASUREMENT_REMAINS_GOVERNED =
  "EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.";

export const COMPOSITION_V2_STATUS = Object.freeze({
  proposed: true,
  activated: false,
  note: "Inactive until founder approves controlled rewrite for production.",
});

export const COMPOSITION_V2_SECTIONS = Object.freeze([
  "HEADLINE",
  "NON_OBVIOUS_INSIGHT",
  "WHY_IT_MATTERS",
  "PRIMARY_FOCUS",
  "WATCH", // optional
  "MANAGEMENT_QUESTION",
]);

export const COMPOSITION_V2_RULES = Object.freeze({
  targetWordCount: { min: 90, max: 130, hardMax: 150 },
  headlineWords: { min: 15, max: 28 },
  noKpiEnumeration: true,
  noUniversalSpVsConsiderationOpening: true,
  noCausalOverclaim: true,
  strongHotelNoManufacturedProblems: true,
  weakHotelDiagnoseEntryFirst: true,
  primaryFocusActionable: true,
  watchOptional: true,
  doesNotRedefineMeasurement: true,
});

export const COMPOSITION_V2_GATES = Object.freeze([
  "ADP_EXECUTIVE_HEADLINE_SYNTHESIS",
  "ADP_EXECUTIVE_NON_OBVIOUS_INSIGHT",
  "ADP_EXECUTIVE_WHY_IT_MATTERS",
  "ADP_EXECUTIVE_PRIMARY_FOCUS_ACTIONABLE",
  "ADP_EXECUTIVE_MANAGEMENT_QUESTION_PROPERTY_SPECIFIC",
  "ADP_EXECUTIVE_READ_NO_KPI_ENUMERATION",
  "ADP_EXECUTIVE_SUMMARY_PROPERTY_SPECIFICITY",
  "ADP_EXECUTIVE_PRIMARY_FOCUS_TRACEABILITY",
  "ADP_EXECUTIVE_REWRITE_REACTION_SCORE",
  "ADP_EXECUTIVE_30_SECOND_TEST",
  "ADP_EXECUTIVE_PRIMARY_FOCUS_5_MINUTE_TEST",
  "ADP_EXECUTIVE_SUMMARY_MUST_ADD_ANALYTICAL_VALUE",
]);

/**
 * Build inactive composition contract proposal for founder review.
 */
export function proposeExecutiveReadCompositionV2() {
  return {
    object: ADP_EXECUTIVE_READ_COMPOSITION_V2,
    status: COMPOSITION_V2_STATUS,
    doctrine: [
      "METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.",
      EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION_MEASUREMENT_REMAINS_GOVERNED,
    ],
    sections: COMPOSITION_V2_SECTIONS,
    rules: COMPOSITION_V2_RULES,
    gates: COMPOSITION_V2_GATES,
    mayGovern: [
      "insight selection",
      "synthesis",
      "priority selection",
      "narrative structure",
    ],
    mustNotGovern: [
      "metric definition",
      "formula",
      "scenario universe",
      "provider universe",
      "denominator",
      "peer methodology",
      "materiality thresholds for measurement",
      "comparability rules",
      "certification policy",
    ],
    activated: false,
  };
}
