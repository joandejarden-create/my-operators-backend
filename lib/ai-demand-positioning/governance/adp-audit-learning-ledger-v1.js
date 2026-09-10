/**
 * Durable learning ledger for ADP client-readiness / full-universe remediation.
 * Records implementation/entity/presentation fixes that strengthen quality controls
 * without changing approved methodology.
 */

import {
  ADP_AUDIT_DEFECT_CLASS,
  METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
  evaluateAuditLearningGovernance,
} from "./adp-methodology-governance-v1.js";

export const ADP_AUDIT_LEARNING_LEDGER_VERSION =
  "adp_audit_learning_ledger_client_readiness_full_universe_v1";

/**
 * Defects fixed during CALA + full-universe client-readiness remediation.
 * Methodology Changed? = NO for every entry.
 */
export const ADP_CLIENT_READINESS_REMEDIATION_LEARNING_LEDGER_V1 = Object.freeze([
  Object.freeze({
    defectId: "dual_subject_presence_path_a_b",
    defect: "Dual Path-A / Path-B subject presence (enrichObservationsWithRank overwrote Path-A mention)",
    classification: ADP_AUDIT_DEFECT_CLASS.IMPLEMENTATION_DEFECT,
    rootCause:
      "enrichObservationsWithRank set mentioned from weak extractPropertyRank (Path B) instead of detectPropertyMention / governedInterpretation (Path A)",
    canonicalFix:
      "enrichObservationsWithRank always applyGovernedInterpretation + projectGovernedObservations; Path-A-only for presence",
    regressionTestAdded: "test:adp-single-canonical-subject-presence-path-v1 / structural dualPathActive=false",
    permanentGateAdded: "SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH / ADP_FULL_UNIVERSE_SINGLE_SUBJECT_PATH",
    methodologyChanged: false,
  }),
  Object.freeze({
    defectId: "stale_governed_interpretation_short_circuit",
    defect: "Stale governedInterpretation trusted without recomputing from rawResponse (RTS false positives)",
    classification: ADP_AUDIT_DEFECT_CLASS.IMPLEMENTATION_DEFECT,
    rootCause: "Enrich path short-circuited on existing governedInterpretation that disagreed with detectPropertyMention",
    canonicalFix: "Always recompute Path A from rawResponse during enrich; never trust stale governed blob alone",
    regressionTestAdded: "Force reprocess + independent metric parity on RTS published metrics",
    permanentGateAdded: "ADP_FULL_UNIVERSE_INDEPENDENT_METRIC_PARITY",
    methodologyChanged: false,
  }),
  Object.freeze({
    defectId: "jaragua_alias_split",
    defect: "Renaissance Santo Domingo Jaragua alias split across competitor rows",
    classification: ADP_AUDIT_DEFECT_CLASS.ENTITY_DEFECT,
    rootCause: "Multiple raw strings for one physical hotel not bound to one canonicalHotelId before aggregation",
    canonicalFix: "Expand cala-six / property entity registries with Jaragua aliases; aggregate by entityId first",
    regressionTestAdded: "Entity duplicate / Jaragua-class alias audit in master client-readiness",
    permanentGateAdded: "ADP_HOTEL_ALIAS_CONSOLIDATION_INTEGRITY / ADP_FULL_UNIVERSE_ENTITY_ALIAS_CONSOLIDATION",
    methodologyChanged: false,
  }),
  Object.freeze({
    defectId: "unbound_analytical_competitors",
    defect: "Analytical competitor rows with canonicalHotelId == null treated as hotels",
    classification: ADP_AUDIT_DEFECT_CLASS.ENTITY_DEFECT,
    rootCause: "Customer entity resolution fell through to unbound hotel strings / prose artifacts",
    canonicalFix: "Fail-closed unbound hotel strings; classify GENERIC_PLACE / PROSE_ARTIFACT / MALFORMED / AMBIGUOUS",
    regressionTestAdded: "Unbound competitor counts in master + full-universe audits",
    permanentGateAdded: "ADP_NO_UNBOUND_ANALYTICAL_COMPETITOR_ENTITIES / ADP_FULL_UNIVERSE_NO_UNBOUND_ANALYTICAL_ENTITIES",
    methodologyChanged: false,
  }),
  Object.freeze({
    defectId: "displacement_alias_double_count",
    defect: "Displacement double-counted alias variants (Cambridge Reefs / Tortuga class)",
    classification: ADP_AUDIT_DEFECT_CLASS.IMPLEMENTATION_DEFECT,
    rootCause: "aggregateDisplacement credited multiple alias strings instead of unique scenarioId × entityId",
    canonicalFix: "Deduplicate displacement credits by scenarioId × canonical entityId",
    regressionTestAdded: "Displacement / lost-demand aggregation checks in remediation + MA",
    permanentGateAdded: "ADP_COMPETITOR_ALIAS_PRE_AGGREGATION",
    methodologyChanged: false,
  }),
  Object.freeze({
    defectId: "reference_metrics_eligibility_mismatch",
    defect: "Independent reference Consideration used broader obs set than production CORE+CONDITIONAL+PROPERTY_SPECIFIC filter",
    classification: ADP_AUDIT_DEFECT_CLASS.IMPLEMENTATION_DEFECT,
    rootCause: "reference-metrics eligibility diverged from production consideration denominator policy implementation",
    canonicalFix: "Align refConsiderationAndScenario filter with production eligibility (same approved denominator policy)",
    regressionTestAdded: "Independent metric parity (Cap Cana class)",
    permanentGateAdded: "ADP_INDEPENDENT_METRIC_RECALCULATION_PARITY",
    methodologyChanged: false,
  }),
  Object.freeze({
    defectId: "top_alternative_raw_mention_leader",
    defect: "Top Observed AI Alternative fell back to competitiveSet.observed[0] (raw mention grain) vs Overall presence leader",
    classification: ADP_AUDIT_DEFECT_CLASS.IMPLEMENTATION_DEFECT,
    rootCause: "MA reconcile and/or payload used observed[0] instead of deriveTopObservedAiAlternative Overall unique-per-observation grain",
    canonicalFix: "Always deriveTopObservedAiAlternative; remove observed[0] fallback; serialize topObservedAlternative on owner payload",
    regressionTestAdded: "test:adp-top-observed-alternative-overall-presence-v1 / cross-surface top_alternative_vs_overview_presence",
    permanentGateAdded: "ADP_CROSS_SURFACE_METRIC_PARITY (top_alternative_vs_overview_presence)",
    methodologyChanged: false,
  }),
  Object.freeze({
    defectId: "metric_scope_label_ambiguity",
    defect: "Competitor % columns ambiguous vs subject Consideration (same-number different grain)",
    classification: ADP_AUDIT_DEFECT_CLASS.PRESENTATION_DEFECT,
    rootCause: "Client UI lacked explicit Scenario Appearance / scope labels for competitor metrics",
    canonicalFix: "metric-scope-labels-v1 + Scenario Appearance labeling on owner/share surfaces",
    regressionTestAdded: "ADP_METRIC_SCOPE_UNAMBIGUOUS structural check",
    permanentGateAdded: "ADP_METRIC_SCOPE_UNAMBIGUOUS / SAME_SCOPE_SAME_CANONICAL_METRIC",
    methodologyChanged: false,
  }),
  Object.freeze({
    defectId: "certified_period_silent_overwrite_risk",
    defect: "Risk of silently overwriting certified published metrics after Path-A / entity corrections",
    classification: ADP_AUDIT_DEFECT_CLASS.IMPLEMENTATION_DEFECT,
    rootCause: "Republish without correction lineage object",
    canonicalFix: "ADP_CERTIFIED_PERIOD_CORRECTION_V1 records (old/new/cause/exposure)",
    regressionTestAdded: "Correction governance gate in full-universe audit",
    permanentGateAdded: "ADP_FULL_UNIVERSE_CERTIFIED_CORRECTION_GOVERNANCE",
    methodologyChanged: false,
  }),
]);

/** No methodology reviews were auto-implemented during this remediation. */
export const FOUNDER_METHODOLOGY_REVIEW_REQUIRED_V1 = Object.freeze([]);

export function getClientReadinessRemediationLearningReport() {
  const governance = evaluateAuditLearningGovernance(
    ADP_CLIENT_READINESS_REMEDIATION_LEARNING_LEDGER_V1,
    FOUNDER_METHODOLOGY_REVIEW_REQUIRED_V1
  );
  return {
    version: ADP_AUDIT_LEARNING_LEDGER_VERSION,
    doctrine: METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
    gates: {
      ADP_AUDIT_LEARNS_ERROR_PREVENTION_NOT_METHODOLOGY: governance.pass,
      ADP_METHODOLOGY_CHANGE_REQUIRES_EXPLICIT_GOVERNANCE: true,
      ADP_AUDIT_DEFECT_CLASSIFICATION: true,
    },
    defectsFixed: ADP_CLIENT_READINESS_REMEDIATION_LEARNING_LEDGER_V1.map((d) => ({
      Defect: d.defect,
      Classification: d.classification,
      RootCause: d.rootCause,
      CanonicalFix: d.canonicalFix,
      RegressionTestAdded: d.regressionTestAdded,
      PermanentGateAdded: d.permanentGateAdded,
      "Methodology Changed?": d.methodologyChanged ? "YES" : "NO",
    })),
    methodologyChangedAny: false,
    FOUNDER_METHODOLOGY_REVIEW_REQUIRED: [...FOUNDER_METHODOLOGY_REVIEW_REQUIRED_V1],
    governance,
  };
}
