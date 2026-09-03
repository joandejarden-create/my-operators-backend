/**
 * Permanent ADP Cross-Surface Reconciliation Registry V1
 *
 * Governed summary↔detail relationships for every Existing Hotel ADP assurance run.
 * New customer-facing metrics MUST register here before release (Product Builder gate).
 *
 * Principles: CROSS_SURFACE_SEMANTIC_RECONCILIATION · ANALYTICAL_COHERENCE
 * Defect: CROSS_SURFACE_ANALYTICAL_CONTRADICTION
 */

export const CROSS_SURFACE_REGISTRY_VERSION = "adp_cross_surface_relationship_registry_v1";

/** Confirmed semantic relationship classes. */
export const RELATIONSHIP_CLASS = Object.freeze({
  MUST_MATCH: "MUST_MATCH",
  MUST_RECONCILE: "MUST_RECONCILE",
  MAY_DIFFER_BY_DESIGN: "MAY_DIFFER_BY_DESIGN",
  AMBIGUOUS: "AMBIGUOUS",
});

/**
 * Governed summary → detail mappings.
 * `invariant` is only set when relationshipClass === MUST_MATCH (or hard MUST_RECONCILE).
 * `payloadSummaryPath` / `payloadDetailPath` are discovery hints (dot paths).
 */
export const GOVERNED_SUMMARY_DETAIL_RELATIONSHIPS = Object.freeze([
  {
    id: "top_alternative_vs_overview_presence",
    summary: "Top Observed AI Alternative",
    detail: "Competitive Overview Overall AI Presence leader",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant: "topObservedAlternative.entityId === highestNonSubjectOverallPresence.entityId",
    payloadSummaryPath: "competitiveSet.topObservedAlternative",
    payloadDetailPath: "competitiveRankingByTerritory.byTerritory.overall",
    goldCases: [
      "adp_cambridge_beaches_bermuda",
      "adp_hotel_phillips_kansas_city",
      "adp_renaissance_times_square",
    ],
    customerReasonablenessSameConcept: true,
  },
  {
    id: "executive_consideration_vs_reference",
    summary: "Executive Consideration Rate",
    detail: "Independent reference / certified observation calculation",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant: "executiveConsideration === referenceConsideration",
    payloadSummaryPath: "executiveMetrics.considerationRate.rate",
    payloadDetailPath: null,
    customerReasonablenessSameConcept: true,
  },
  {
    id: "scenario_presence_vs_reference",
    summary: "Scenario Presence",
    detail: "Independent reference scenario-level presence",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant: "scenarioPresence === referenceScenarioPresence",
    payloadSummaryPath: "executiveMetrics.scenarioPresence.rate",
    payloadDetailPath: null,
    customerReasonablenessSameConcept: true,
  },
  {
    id: "demand_capture_vs_reference",
    summary: "Demand Capture",
    detail: "Governed scenario/territory demand-capture calculation",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant: "demandCapture === referenceDemandCapture",
    payloadSummaryPath: "demandCapture.overallRate",
    payloadDetailPath: null,
    customerReasonablenessSameConcept: true,
  },
  {
    id: "provider_summary_vs_provider_rows",
    summary: "Provider Presence summary",
    detail: "Provider table rows / comparable denominators",
    relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
    invariant: "providerDisplayedDenominator === providerComparableObservationCount",
    payloadSummaryPath: "evidence.providers",
    payloadDetailPath: "evidence.providers",
    customerReasonablenessSameConcept: true,
  },
  {
    id: "core_count_vs_governed_members",
    summary: "CORE comparable count",
    detail: "Governed CORE member list",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant: "coreCount === governedCoreMembers.length",
    payloadSummaryPath: "competitiveRankingByTerritory",
    payloadDetailPath: "competitiveRankingByTerritory",
    customerReasonablenessSameConcept: true,
  },
  {
    id: "core_benchmark_vs_core_rates",
    summary: "CORE benchmark",
    detail: "Underlying CORE peer rates",
    relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
    invariant: null,
    payloadSummaryPath: "intentPresenceIndex",
    payloadDetailPath: "competitiveRankingByTerritory",
    customerReasonablenessSameConcept: true,
  },
  {
    id: "presence_index_vs_subject_core",
    summary: "Presence Index",
    detail: "Subject presence / CORE benchmark composition",
    relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
    invariant: null,
    payloadSummaryPath: "intentPresenceIndex",
    payloadDetailPath: "demandCapture",
    customerReasonablenessSameConcept: true,
  },
  {
    id: "top_demand_territory_vs_territory_detail",
    summary: "Top Demand Territory",
    detail: "Territory ranking / per-territory competitive detail",
    relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
    invariant: null,
    payloadSummaryPath: "competitiveRankingByTerritory.byTerritory.overall",
    payloadDetailPath: "competitiveRankingByTerritory.byTerritory",
    customerReasonablenessSameConcept: true,
  },
  {
    id: "displacement_overview_vs_context_must_match",
    summary: "Competitive Overview Displacement vs You",
    detail: "Competitive Context Competitive Displacement",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant:
      "competitiveOverview.displacementScenarioCount(entity, scope) === competitiveContext.displacementScenarioCount(entity, scope) AND supporting scenarioId sets equal",
    payloadSummaryPath: "competitiveRankingByTerritory.byTerritory.*.displayRows[].displacement.count",
    payloadDetailPath: "lostDemand.displacement[].displacementCount",
    customerReasonablenessSameConcept: true,
    note: "SAME_CONCEPT_SAME_CANONICAL_SOURCE. Defect DUPLICATED_CONCEPT_CROSS_SURFACE_MISMATCH. Gold: Cambridge Reefs 1 vs 2 (alias double-count).",
    goldCases: ["adp_cambridge_beaches_bermuda"],
  },
  {
    id: "displacement_metric_vs_evidence_set",
    summary: "Displacement count",
    detail: "Displacement evidence drawer scenario set",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant: "metric.scenarioIds === evidence.scenarioIds (same competitor × scope)",
    payloadSummaryPath: "lostDemand.displacement",
    payloadDetailPath: "evidence?type=displacement",
    customerReasonablenessSameConcept: true,
    note: "Gate DISPLACEMENT_EVIDENCE_SET_INTEGRITY",
  },
  {
    id: "missing_evidence_vs_governed_absent_set",
    summary: "Demand Territory / Provider Missing Evidence",
    detail: "Governed subject-absent comparable observation set",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant:
      "missingEvidence.observationIds === governedComparableSubjectAbsent(context).observationIds",
    payloadSummaryPath: "demandCapture.byIntent / evidence.providers",
    payloadDetailPath: "evidence?type=missing&mode=missing",
    customerReasonablenessSameConcept: true,
    note: "Gate MISSING_EVIDENCE_CONTEXT_INTEGRITY. Distinct lane from Positive Evidence.",
  },
  {
    id: "positive_evidence_vs_governed_present_examples",
    summary: "Demand Territory / Provider Positive Evidence",
    detail: "Governed subject-present representative examples",
    relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
    invariant:
      "every positiveEvidence.example is governedComparableSubjectPresent(context); selection rule deterministic",
    payloadSummaryPath: "demandCapture.byIntent / evidence.providers",
    payloadDetailPath: "evidence?type=present&mode=positive",
    customerReasonablenessSameConcept: true,
    note: "Gate POSITIVE_EVIDENCE_CONTEXT_INTEGRITY. Representative sample — not full present set.",
  },
  {
    id: "rank_movement_vs_prior_certified_ranking",
    summary: "Future UI rank movement (#3 ↑2)",
    detail: "Prior + current competitive history ledger ranks",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant: "ui.rankDelta === priorRank - currentRank from COMPETITIVE_RANK_HISTORY ledger",
    payloadSummaryPath: "competitiveHistory.movement (future)",
    payloadDetailPath: "data/ai-demand-positioning/competitive-history/",
    customerReasonablenessSameConcept: true,
    note: "Gate COMPETITIVE_RANK_HISTORY_INTEGRITY. No production arrows until second comparable period.",
  },
  {
    id: "displacement_summary_vs_displacement_rows",
    summary: "Displacement summary",
    detail: "Displacement table / evidence",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant: "lostDemand.displacement counts === Overview displacement for shared entityIds",
    payloadSummaryPath: "lostDemand.displacement",
    payloadDetailPath: "lostDemand.scenarios",
    customerReasonablenessSameConcept: true,
  },
  {
    id: "displacement_vs_overview_presence",
    summary: "Displacement leader",
    detail: "Overall AI Presence leader",
    relationshipClass: RELATIONSHIP_CLASS.MAY_DIFFER_BY_DESIGN,
    invariant: null,
    note: "Displacement = subject-absent competitive pressure; Overall = unique-per-observation presence.",
    payloadSummaryPath: "lostDemand.displacement.0",
    payloadDetailPath: "competitiveRankingByTerritory.byTerritory.overall",
    customerReasonablenessSameConcept: false,
  },
  {
    id: "reality_gap_vs_attribute_evidence",
    summary: "Reality Gap summary",
    detail: "Attribute / evidence detail",
    relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
    invariant: null,
    payloadSummaryPath: "realityGap",
    payloadDetailPath: "realityGap",
    customerReasonablenessSameConcept: true,
  },
  {
    id: "source_citation_vs_evidence_records",
    summary: "Source / citation count",
    detail: "Source / evidence records",
    relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
    invariant: null,
    payloadSummaryPath: "evidence",
    payloadDetailPath: "evidence",
    customerReasonablenessSameConcept: true,
  },
  {
    id: "trend_baseline_vs_period_kpi",
    summary: "Trend baseline / latest point",
    detail: "Certified-period KPI values",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant: "trend.latest.consideration === certifiedPeriod.consideration",
    payloadSummaryPath: "trends",
    payloadDetailPath: "executiveMetrics.considerationRate.rate",
    customerReasonablenessSameConcept: true,
  },
  {
    id: "positive_evidence_vs_territory_presence",
    summary: "View AI evidence (where you appeared)",
    detail: "Demand Territory / Provider presence metric context",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant:
      "every positiveEvidence.example.intent === openedTerritoryIntent AND subjectAppeared === true AND periodId === certifiedPeriodId",
    payloadSummaryPath: "evidence.positiveEvidence",
    payloadDetailPath: "competitiveRankingByTerritory.byTerritory",
    customerReasonablenessSameConcept: true,
  },
  {
    id: "rank_movement_vs_history_ledger",
    summary: "Competitive rank movement display (future)",
    detail: "Immutable competitive history ledger prior/current ranks",
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    invariant: "displayedRankDelta === priorRank - currentRank from finalized history ledger",
    payloadSummaryPath: "competitiveRankHistory.preview",
    payloadDetailPath: "competitive-history/{periodId}.json",
    customerReasonablenessSameConcept: true,
    note: "UI arrows deferred until second comparable certified period exists.",
  },
  {
    id: "trend_change_vs_two_periods",
    summary: "Trend change",
    detail: "Two comparable certified periods",
    relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
    invariant: null,
    payloadSummaryPath: "trends",
    payloadDetailPath: "trends",
    customerReasonablenessSameConcept: true,
  },
  {
    id: "action_prompt_vs_observed_evidence",
    summary: "Action / review prompt",
    detail: "Supporting observed evidence",
    relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
    invariant: null,
    payloadSummaryPath: "actions",
    payloadDetailPath: "evidence",
    customerReasonablenessSameConcept: true,
  },
]);

/** Gold regression cases for Top Alternative ↔ Overview (permanent). */
export const TOP_ALTERNATIVE_GOLD_CASES = Object.freeze([
  {
    propertyId: "adp_cambridge_beaches_bermuda",
    rawMentionHistoricalLeader: "The Reefs Resort & Club",
    governedOverallLeaderPattern: /Grotto Bay/i,
    note: "Customer Top Alternative must equal Overall presence leader (Grotto), not raw-mention Reefs.",
  },
  {
    propertyId: "adp_hotel_phillips_kansas_city",
    rawMentionHistoricalLeader: "Hotel Kansas City, in The Unbound Collection by Hyatt",
    governedOverallLeaderPattern: /Loews/i,
    note: "Customer Top Alternative must equal Overall presence leader (Loews), not raw-mention Hotel KC.",
  },
  {
    propertyId: "adp_renaissance_times_square",
    rawMentionHistoricalLeader: "The Knickerbocker",
    governedOverallLeaderPattern: /Knickerbocker/i,
    note: "Already reconciles — permanent positive control.",
  },
]);

/**
 * Product Builder release checklist for any new customer-facing ADP metric/card/table/chart.
 * Incomplete if any field is missing.
 */
export const NEW_CUSTOMER_METRIC_RECONCILIATION_CHECKLIST = Object.freeze([
  "metricDefinition",
  "grain",
  "denominator",
  "scopeFilterBehavior",
  "canonicalEntityBehavior",
  "supportingDetailedSurface",
  "expectedReconciliationRelationship",
  "certificationInvariantOrCheck",
]);

export function validateNewMetricReconciliationSpec(spec = {}) {
  const missing = NEW_CUSTOMER_METRIC_RECONCILIATION_CHECKLIST.filter((k) => !spec[k]);
  return {
    complete: missing.length === 0,
    missing,
    incompleteMeans: "A new customer-facing metric without a defined reconciliation path is incomplete.",
  };
}

function pathExists(payload, dotPath) {
  if (!dotPath || !payload) return false;
  const parts = String(dotPath).split(".");
  let cur = payload;
  for (const p of parts) {
    if (cur == null) return false;
    cur = cur[p];
  }
  return cur != null;
}

/**
 * Discover customer-facing payload paths that look like summary KPIs without a registered relationship.
 * Proactive — does not wait for founder to name the pair.
 */
export function discoverUnregisteredSummarySurfaces(payload) {
  const registered = new Set(
    GOVERNED_SUMMARY_DETAIL_RELATIONSHIPS.flatMap((r) =>
      [r.payloadSummaryPath, r.payloadDetailPath].filter(Boolean)
    )
  );

  const candidates = [];
  const walk = (node, path, depth) => {
    if (!node || typeof node !== "object" || depth > 4) return;
    if (Array.isArray(node)) {
      if (node.length && typeof node[0] === "object") walk(node[0], `${path}.0`, depth + 1);
      return;
    }
    for (const [k, v] of Object.entries(node)) {
      const next = path ? `${path}.${k}` : k;
      const keyHint = /rate|count|pct|presence|index|leader|top|summary|total|benchmark/i.test(k);
      const isLeafNumber = typeof v === "number";
      const isNamedEntity = v && typeof v === "object" && (v.name || v.entityId) && !Array.isArray(v);
      if ((keyHint && (isLeafNumber || isNamedEntity)) || (isNamedEntity && /top|leader|alternative/i.test(k))) {
        const covered = [...registered].some((reg) => next === reg || next.startsWith(`${reg}.`) || reg.startsWith(`${next}.`));
        if (!covered) {
          candidates.push({
            path: next,
            kind: isLeafNumber ? "numeric_summary" : "entity_summary",
            question:
              "What detailed surface supports this? Should it MUST_MATCH / MUST_RECONCILE / MAY_DIFFER_BY_DESIGN? Register before next certification.",
            relationshipClass: RELATIONSHIP_CLASS.AMBIGUOUS,
          });
        }
      }
      if (v && typeof v === "object") walk(v, next, depth + 1);
    }
  };
  walk(payload, "", 0);

  // Dedupe by path
  const seen = new Set();
  return candidates.filter((c) => {
    if (seen.has(c.path)) return false;
    seen.add(c.path);
    return true;
  }).slice(0, 40);
}

/**
 * Inventory of governed relationships present on this payload + discovery gaps.
 */
export function buildRelationshipCoverageReport(payload) {
  const governed = GOVERNED_SUMMARY_DETAIL_RELATIONSHIPS.map((r) => ({
    id: r.id,
    summary: r.summary,
    detail: r.detail,
    relationshipClass: r.relationshipClass,
    invariant: r.invariant || null,
    summaryPresent: pathExists(payload, r.payloadSummaryPath),
    detailPresent: r.payloadDetailPath ? pathExists(payload, r.payloadDetailPath) : null,
    customerReasonablenessSameConcept: r.customerReasonablenessSameConcept,
  }));

  const discovered = discoverUnregisteredSummarySurfaces(payload);

  return {
    version: CROSS_SURFACE_REGISTRY_VERSION,
    governedCount: governed.length,
    governed,
    discoveredUnregistered: discovered,
    discoveryCount: discovered.length,
    newMetricChecklist: NEW_CUSTOMER_METRIC_RECONCILIATION_CHECKLIST,
  };
}

export function listHardInvariants() {
  return GOVERNED_SUMMARY_DETAIL_RELATIONSHIPS.filter((r) => r.invariant).map((r) => ({
    id: r.id,
    relationshipClass: r.relationshipClass,
    invariant: r.invariant,
  }));
}
