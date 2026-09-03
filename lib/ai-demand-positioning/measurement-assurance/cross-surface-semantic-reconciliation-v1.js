/**
 * CROSS_SURFACE_SEMANTIC_RECONCILIATION + ANALYTICAL_COHERENCE
 * Defect class: CROSS_SURFACE_ANALYTICAL_CONTRADICTION
 *
 * Detects contradictions between headline KPIs and detailed supporting surfaces.
 * Does not change measurement methodology — audits and classifies only.
 */

import { computeCompetitiveSet } from "../intelligence/competitive-set.js";
import { buildOverallCompetitiveRanking } from "../customer/competitive-ranking-overall-view-v1.js";
import {
  countCanonicalPresenceAppearances,
  resolveCompetitiveEntityId,
  SUBJECT_PRESENCE_KEY,
} from "../customer/canonical-presence-per-observation-v1.js";
import { filterComparableObservations } from "../metrics/grain-governance.js";
import { resolveCustomerFacingEntity } from "../customer/customer-entity-resolution-v1.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import { nearlyEqual } from "./reference-metrics.js";
import { assertTopAlternativeMatchesOverallLeader } from "../customer/top-observed-ai-alternative-v1.js";
import {
  buildRelationshipCoverageReport,
  listHardInvariants,
  RELATIONSHIP_CLASS,
  CROSS_SURFACE_REGISTRY_VERSION,
  TOP_ALTERNATIVE_GOLD_CASES,
} from "./cross-surface-relationship-registry-v1.js";

export const CROSS_SURFACE_SEMANTIC_RECONCILIATION = "CROSS_SURFACE_SEMANTIC_RECONCILIATION";
export const ANALYTICAL_COHERENCE = "ANALYTICAL_COHERENCE";
export const DEFECT_CROSS_SURFACE_ANALYTICAL_CONTRADICTION = "CROSS_SURFACE_ANALYTICAL_CONTRADICTION";

export const RELATIONSHIP_VERDICTS = Object.freeze({
  MUST_MATCH: "MUST_MATCH",
  MUST_RECONCILE: "MUST_RECONCILE",
  MAY_DIFFER_BY_DESIGN: "MAY_DIFFER_BY_DESIGN",
  CURRENTLY_AMBIGUOUS: "CURRENTLY_AMBIGUOUS",
  AMBIGUOUS: "AMBIGUOUS",
});

export const DISCREPANCY_CLASSES = Object.freeze({
  EXPLAINED_BY_METHODOLOGY: "EXPLAINED_BY_METHODOLOGY",
  EXPECTED_VARIATION: "EXPECTED_VARIATION",
  PRESENTATION_CLARIFICATION_REQUIRED: "PRESENTATION_CLARIFICATION_REQUIRED",
  DATA_QUALITY_REVIEW: "DATA_QUALITY_REVIEW",
  ENTITY_RESOLUTION_DEFECT: "ENTITY_RESOLUTION_DEFECT",
  IMPLEMENTATION_DEFECT: "IMPLEMENTATION_DEFECT",
  METHODOLOGY_AMBIGUITY: "METHODOLOGY_AMBIGUITY",
  MATERIAL_CERTIFICATION_BLOCKER: "MATERIAL_CERTIFICATION_BLOCKER",
});

export const MISMATCH_CODES = Object.freeze({
  DIFFERENT_DENOMINATOR: "DIFFERENT_DENOMINATOR",
  DIFFERENT_GRAIN: "DIFFERENT_GRAIN",
  MULTIPLE_MENTIONS_COUNTED_DIFFERENTLY: "MULTIPLE_MENTIONS_COUNTED_DIFFERENTLY",
  PROVIDER_AGGREGATION_DIFFERENCE: "PROVIDER_AGGREGATION_DIFFERENCE",
  TERRITORY_FILTER_DIFFERENCE: "TERRITORY_FILTER_DIFFERENCE",
  ENTITY_ALIAS_DIFFERENCE: "ENTITY_ALIAS_DIFFERENCE",
  SUBJECT_COMP_SET_FILTER_DIFFERENCE: "SUBJECT/COMP_SET_FILTER_DIFFERENCE",
  STALE_PAYLOAD: "STALE_PAYLOAD",
  CALCULATION_DEFECT: "CALCULATION_DEFECT",
  LABEL_SEMANTIC_DEFECT: "LABEL/SEMANTIC_DEFECT",
  OTHER: "OTHER",
});

/**
 * Metric contracts — implementation truth (not customer labels).
 */
export const TOP_OBSERVED_ALTERNATIVE_CONTRACT = Object.freeze({
  id: "top_observed_ai_alternative",
  sourcePath:
    "lib/ai-demand-positioning/customer/top-observed-ai-alternative-v1.js#deriveTopObservedAiAlternative",
  uiPath:
    "public/js/ai-demand-positioning/ai-demand-positioning.js#renderExecKpis → competitiveSet.topObservedAlternative",
  grain: "OBSERVATION_GRAIN unique-per-canonical-hotel (MAX_PRESENCE_CREDIT=1 per observation)",
  observationFilter: "filterComparableObservations only",
  entityResolution: "resolveCompetitiveEntityId (ONE CANONICAL ENTITY PATH)",
  numerator: "count of comparable observations where canonical competitor appears ≥1",
  denominator: "comparable observation count N (same as Overview AI Presence)",
  uniquePerObservation: true,
  subjectExclusion: "subject excluded; non-subject Overall presence leader",
  ranking: "sort by appearances desc, then display name A→Z (same as Overview)",
  rankingBasis: "OVERALL_UNIQUE_PER_OBSERVATION_PRESENCE",
  customerCopyImplies: "hotel appearing most often across monitored AI responses this period",
  diagnosticOnly: "RAW_COMPETITOR_MENTION_COUNT must not drive this KPI",
});

export const COMPETITIVE_OVERVIEW_AI_PRESENCE_CONTRACT = Object.freeze({
  id: "competitive_overview_overall_ai_presence",
  sourcePath:
    "lib/ai-demand-positioning/customer/competitive-ranking-overall-view-v1.js#buildOverallCompetitiveRanking",
  supportingPath:
    "lib/ai-demand-positioning/customer/canonical-presence-per-observation-v1.js#countCanonicalPresenceAppearances",
  grain: "OBSERVATION_GRAIN unique-per-canonical-hotel (MAX_PRESENCE_CREDIT=1 per observation)",
  observationFilter: "filterComparableObservations only",
  entityResolution: "resolveCompetitiveEntityId / canonicalizeForProperty",
  numerator: "count of comparable observations where canonical competitor appears ≥1",
  denominator: "comparable observation count N",
  uniquePerObservation: true,
  subjectExclusion: "subject keyed as __subject__; competitors ranked separately",
  ranking: "sort by appearances desc (= presence % desc for shared N)",
  customerCopyImplies: "share of comparable AI answers where the hotel appears",
});

/**
 * Founder-approved: Top Alternative MUST_MATCH Overview Overall presence leader.
 */
export function expectedTopAlternativeVsOverviewRelationship() {
  return {
    verdict: RELATIONSHIP_VERDICTS.MUST_MATCH,
    customerSemanticTest:
      "YES — same Overall unique-per-observation presence ranking as Competitive Overview.",
    implementationToday: "ALIGNED — deriveTopObservedAiAlternative from Overall presence grain",
    certificationImplication:
      "Hard invariant: topObservedAlternative.entityId === highestNonSubjectOverallPresence.entityId (ties: governed alphabetical leader).",
    note: "RAW_COMPETITOR_MENTION_COUNT is diagnostic-only and must not feed the KPI.",
  };
}

function normalizeName(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function namesLikelySame(a, b) {
  const na = normalizeName(a);
  const nb = normalizeName(b);
  if (!na || !nb) return false;
  if (na === nb) return true;
  if (na.includes(nb) || nb.includes(na)) return true;
  return false;
}

/**
 * Quantify raw-mention vs unique-presence for two entities on comparable scope.
 */
export function quantifyMentionVsPresence(observations, propertyProfile, entityIdOrName) {
  const scoped = filterComparableObservations(observations || []);
  const { counts } = countCanonicalPresenceAppearances(scoped, propertyProfile);
  let entityId = entityIdOrName;
  if (entityIdOrName && !counts[entityIdOrName]) {
    entityId = resolveCompetitiveEntityId(entityIdOrName, propertyProfile) || entityIdOrName;
  }
  const uniqueAppearances = counts[entityId] || 0;

  let rawMentionsComparable = 0;
  let rawMentionsAll = 0;
  let obsWithAnyMention = 0;
  for (const obs of observations || []) {
    let hit = false;
    for (const name of obs.competitorsMentioned || []) {
      const id = resolveCompetitiveEntityId(name, propertyProfile);
      const cust = resolveCustomerFacingEntity(name, propertyProfile);
      const match =
        id === entityId ||
        cust.mergeKey === entityId ||
        namesLikelySame(cust.displayName, entityIdOrName) ||
        namesLikelySame(name, entityIdOrName);
      if (match) {
        rawMentionsAll += 1;
        if (filterComparableObservations([obs]).length) rawMentionsComparable += 1;
        hit = true;
      }
    }
    if (hit && filterComparableObservations([obs]).length) obsWithAnyMention += 1;
  }

  return {
    entityId,
    uniqueAppearances,
    rawMentionsAll,
    rawMentionsComparable,
    obsWithAnyMention,
    comparableN: scoped.length,
    presencePct: scoped.length ? Math.round((uniqueAppearances / scoped.length) * 1000) / 10 : null,
    inflation: rawMentionsComparable - uniqueAppearances,
  };
}

/**
 * Reconcile Top Observed Alternative vs Overall Competitive Overview leader.
 */
export function reconcileTopAlternativeVsOverview(observations, scenarios, propertyProfile, payload = null) {
  const obs = enrichObservationsWithRank(observations || [], propertyProfile);
  const overall =
    payload?.competitiveRankingByTerritory?.byTerritory?.overall ||
    buildOverallCompetitiveRanking(obs, scenarios, propertyProfile);
  const competitiveSet =
    payload?.competitiveSet ||
    computeCompetitiveSet(obs, propertyProfile, { overallRanking: overall, scenarios });

  const topAlt =
    competitiveSet?.topObservedAlternative ||
    competitiveSet?.observed?.[0] ||
    null;

  const invariant = assertTopAlternativeMatchesOverallLeader(topAlt, overall);
  const highest = invariant.highestNonSubjectOverallPresence || null;
  const relationship = expectedTopAlternativeVsOverviewRelationship();

  if (!invariant.pass) {
    return {
      match: false,
      hardInvariant: invariant,
      topAlternative: topAlt
        ? {
            name: topAlt.name,
            entityId: topAlt.entityId || null,
            appearances: topAlt.appearances ?? topAlt.mentions,
            presencePct: topAlt.aiPresencePct ?? null,
          }
        : null,
      highestByPresence: highest,
      mismatchCodes: [MISMATCH_CODES.CALCULATION_DEFECT],
      classification: DISCREPANCY_CLASSES.MATERIAL_CERTIFICATION_BLOCKER,
      explanation:
        `Hard invariant FAIL: topObservedAlternative.entityId (${topAlt?.entityId}) !== ` +
        `highestNonSubjectOverallPresence.entityId (${highest?.entityId}).`,
      relationship,
      contracts: {
        topAlternative: TOP_OBSERVED_ALTERNATIVE_CONTRACT,
        overviewPresence: COMPETITIVE_OVERVIEW_AI_PRESENCE_CONTRACT,
      },
    };
  }

  const presenceAligned =
    topAlt && highest
      ? topAlt.aiPresencePct == null ||
        highest.aiPresencePct == null ||
        nearlyEqual(topAlt.aiPresencePct, highest.aiPresencePct)
      : true;

  return {
    match: presenceAligned,
    hardInvariant: invariant,
    topAlternative: topAlt
      ? {
          name: topAlt.name,
          entityId: topAlt.entityId || null,
          appearances: topAlt.appearances ?? topAlt.mentions,
          presencePct: topAlt.aiPresencePct ?? highest?.aiPresencePct ?? null,
        }
      : null,
    highestByPresence: highest
      ? {
          name: highest.name,
          entityId: highest.entityId || null,
          appearances: highest.appearances,
          presencePct: highest.aiPresencePct,
        }
      : null,
    mismatchCodes: presenceAligned ? [] : [MISMATCH_CODES.CALCULATION_DEFECT],
    classification: presenceAligned
      ? DISCREPANCY_CLASSES.EXPLAINED_BY_METHODOLOGY
      : DISCREPANCY_CLASSES.MATERIAL_CERTIFICATION_BLOCKER,
    explanation: presenceAligned
      ? "Leaders match under Overall unique-per-observation presence (hard invariant PASS)."
      : "Entity IDs match but AI Presence % differs — investigate payload staleness.",
    relationship,
    contracts: {
      topAlternative: TOP_OBSERVED_ALTERNATIVE_CONTRACT,
      overviewPresence: COMPETITIVE_OVERVIEW_AI_PRESENCE_CONTRACT,
    },
  };
}

/**
 * Broader analytical discrepancy register for one property payload + period.
 * Permanent gate content: ANALYTICAL DISCREPANCIES / QUESTIONS
 */
export function buildAnalyticalDiscrepancyRegister({
  propertyId,
  period,
  scenarios,
  propertyProfile,
  payload,
  referenceMetrics = null,
}) {
  const observations = enrichObservationsWithRank(period?.observations || [], propertyProfile);
  const discrepancies = [];
  const relationshipCoverage = buildRelationshipCoverageReport(payload || {});

  const topVsOverview = reconcileTopAlternativeVsOverview(
    observations,
    scenarios,
    propertyProfile,
    payload
  );
  discrepancies.push({
    id: "top_alternative_vs_overview_presence",
    surfaces: ["Top Observed AI Alternative", "Competitive Overview Overall AI Presence"],
    relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
    customerReasonablenessSameConcept: true,
    ...topVsOverview,
  });

  // Executive consideration vs reference
  if (referenceMetrics && payload?.executiveMetrics?.considerationRate?.rate != null) {
    const prod = payload.executiveMetrics.considerationRate.rate;
    const ref = referenceMetrics.considerationRate;
    const ok = ref == null || nearlyEqual(prod, ref);
    discrepancies.push({
      id: "executive_consideration_vs_reference",
      surfaces: ["Executive Consideration", "Independent reference"],
      relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
      match: ok,
      classification: ok
        ? DISCREPANCY_CLASSES.EXPLAINED_BY_METHODOLOGY
        : DISCREPANCY_CLASSES.MATERIAL_CERTIFICATION_BLOCKER,
      explanation: ok
        ? "Consideration matches independent reference."
        : `consideration prod=${prod} ref=${ref}`,
      mismatchCodes: ok ? [] : [MISMATCH_CODES.CALCULATION_DEFECT],
      customerReasonablenessSameConcept: true,
    });
  }

  // Scenario presence vs reference
  if (referenceMetrics && payload?.executiveMetrics?.scenarioPresence?.rate != null) {
    const prod = payload.executiveMetrics.scenarioPresence.rate;
    const ref = referenceMetrics.scenarioPresence;
    const ok = ref == null || nearlyEqual(prod, ref);
    discrepancies.push({
      id: "scenario_presence_vs_reference",
      surfaces: ["Scenario Presence", "Independent reference"],
      relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
      match: ok,
      classification: ok
        ? DISCREPANCY_CLASSES.EXPLAINED_BY_METHODOLOGY
        : DISCREPANCY_CLASSES.MATERIAL_CERTIFICATION_BLOCKER,
      explanation: ok
        ? "Scenario Presence matches independent reference."
        : `scenarioPresence prod=${prod} ref=${ref}`,
      mismatchCodes: ok ? [] : [MISMATCH_CODES.CALCULATION_DEFECT],
      customerReasonablenessSameConcept: true,
    });
  }

  // Demand capture vs reference
  if (referenceMetrics && payload?.demandCapture?.overallRate != null) {
    const prod = payload.demandCapture.overallRate;
    const ref = referenceMetrics.demandCapture;
    const ok = ref == null || nearlyEqual(prod, ref);
    discrepancies.push({
      id: "demand_capture_vs_reference",
      surfaces: ["Demand Capture", "Independent reference"],
      relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
      match: ok,
      classification: ok
        ? DISCREPANCY_CLASSES.EXPLAINED_BY_METHODOLOGY
        : DISCREPANCY_CLASSES.MATERIAL_CERTIFICATION_BLOCKER,
      explanation: ok
        ? "Demand Capture matches independent reference."
        : `demandCapture prod=${prod} ref=${ref}`,
      mismatchCodes: ok ? [] : [MISMATCH_CODES.CALCULATION_DEFECT],
      customerReasonablenessSameConcept: true,
    });
  }

  // Trend baseline vs period consideration
  const trends = payload?.trends || payload?.executiveMetrics?.trends || [];
  const trend0 = Array.isArray(trends) && trends.length ? trends[trends.length - 1] : null;
  const cons = payload?.executiveMetrics?.considerationRate?.rate;
  if (trend0?.considerationRate != null && cons != null && !nearlyEqual(trend0.considerationRate, cons)) {
    discrepancies.push({
      id: "trend_baseline_vs_period_consideration",
      surfaces: ["Trend latest consideration", "Period Consideration"],
      relationshipClass: RELATIONSHIP_CLASS.MUST_MATCH,
      match: false,
      classification: DISCREPANCY_CLASSES.MATERIAL_CERTIFICATION_BLOCKER,
      explanation: `trend=${trend0.considerationRate} period=${cons}`,
      mismatchCodes: [MISMATCH_CODES.STALE_PAYLOAD],
      customerReasonablenessSameConcept: true,
    });
  }

  // Provider rows — missing ≠ zero; denominator = comparable
  const providers = payload?.evidence?.providers || [];
  for (const p of providers) {
    if (p.presence === 0 && p.presenceUnavailable) {
      discrepancies.push({
        id: `provider_${p.provider}_missing_as_zero`,
        surfaces: ["Provider Presence"],
        relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
        match: false,
        classification: DISCREPANCY_CLASSES.IMPLEMENTATION_DEFECT,
        explanation: "Unavailable provider rendered as 0%",
        mismatchCodes: [MISMATCH_CODES.CALCULATION_DEFECT],
        customerReasonablenessSameConcept: true,
      });
    }
    if (
      p.comparable != null &&
      p.scheduled != null &&
      Number(p.comparable) > Number(p.scheduled)
    ) {
      discrepancies.push({
        id: `provider_${p.provider}_comparable_gt_scheduled`,
        surfaces: ["Provider Monitored denominator", "Scheduled observations"],
        relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
        match: false,
        classification: DISCREPANCY_CLASSES.DATA_QUALITY_REVIEW,
        explanation: `comparable=${p.comparable} > scheduled=${p.scheduled}`,
        mismatchCodes: [MISMATCH_CODES.DIFFERENT_DENOMINATOR],
        customerReasonablenessSameConcept: true,
      });
    }
  }

  // Reality gap coverage vs attribute counts when both present
  const rg = payload?.realityGap;
  if (rg && rg.coveredCount != null && rg.totalAttributes != null && rg.coverageRate != null) {
    const expected =
      rg.totalAttributes > 0 ? Math.round((rg.coveredCount / rg.totalAttributes) * 1000) / 10 : null;
    if (expected != null && !nearlyEqual(rg.coverageRate, expected) && Math.abs(rg.coverageRate - expected) > 1) {
      discrepancies.push({
        id: "reality_gap_rate_vs_counts",
        surfaces: ["Reality Gap coverage rate", "coveredCount / totalAttributes"],
        relationshipClass: RELATIONSHIP_CLASS.MUST_RECONCILE,
        match: false,
        classification: DISCREPANCY_CLASSES.IMPLEMENTATION_DEFECT,
        explanation: `rate=${rg.coverageRate} from counts≈${expected}`,
        mismatchCodes: [MISMATCH_CODES.CALCULATION_DEFECT],
        customerReasonablenessSameConcept: true,
      });
    }
  }

  // Displacement leader vs overview leader (MAY_DIFFER_BY_DESIGN)
  const dispTop = payload?.lostDemand?.displacement?.[0];
  if (dispTop && topVsOverview.highestByPresence && !namesLikelySame(dispTop.name, topVsOverview.highestByPresence.name)) {
    discrepancies.push({
      id: "displacement_leader_vs_overview_presence_leader",
      surfaces: ["Displacement top", "Overview highest presence"],
      relationshipClass: RELATIONSHIP_CLASS.MAY_DIFFER_BY_DESIGN,
      match: false,
      classification: DISCREPANCY_CLASSES.EXPECTED_VARIATION,
      explanation:
        `Displacement leader "${dispTop.name}" differs from presence leader "${topVsOverview.highestByPresence.name}" — ` +
        `displacement is subject-absent competitive pressure, not overall presence.`,
      mismatchCodes: [MISMATCH_CODES.DIFFERENT_GRAIN],
      customerReasonablenessSameConcept: false,
    });
  }

  // Discovery: unregistered summary surfaces → AMBIGUOUS questions (not auto-blockers)
  for (const d of relationshipCoverage.discoveredUnregistered.slice(0, 12)) {
    discrepancies.push({
      id: `discovery_${d.path.replace(/\./g, "_")}`,
      surfaces: [d.path, "(supporting detail TBD)"],
      relationshipClass: RELATIONSHIP_CLASS.AMBIGUOUS,
      match: null,
      classification: DISCREPANCY_CLASSES.METHODOLOGY_AMBIGUITY,
      explanation: d.question,
      mismatchCodes: [],
      discovery: true,
      customerReasonablenessSameConcept: null,
    });
  }

  const blockers = discrepancies.filter(
    (d) => d.classification === DISCREPANCY_CLASSES.MATERIAL_CERTIFICATION_BLOCKER && d.match === false
  );
  const clarifications = discrepancies.filter(
    (d) => d.classification === DISCREPANCY_CLASSES.PRESENTATION_CLARIFICATION_REQUIRED && d.match === false
  );
  const questions = discrepancies.filter(
    (d) =>
      d.match === false ||
      d.discovery ||
      d.classification === DISCREPANCY_CLASSES.METHODOLOGY_AMBIGUITY ||
      d.classification === DISCREPANCY_CLASSES.DATA_QUALITY_REVIEW
  );

  return {
    propertyId,
    gate: CROSS_SURFACE_SEMANTIC_RECONCILIATION,
    principle: ANALYTICAL_COHERENCE,
    defectClass: DEFECT_CROSS_SURFACE_ANALYTICAL_CONTRADICTION,
    registryVersion: CROSS_SURFACE_REGISTRY_VERSION,
    relationshipPolicy: expectedTopAlternativeVsOverviewRelationship(),
    relationshipCoverage,
    hardInvariants: listHardInvariants(),
    goldCases: TOP_ALTERNATIVE_GOLD_CASES,
    title: "ANALYTICAL DISCREPANCIES / QUESTIONS",
    discrepancies,
    questions,
    blockerCount: blockers.length,
    clarificationCount: clarifications.length,
    questionCount: questions.length,
    status: blockers.length ? "FAIL" : clarifications.length ? "PASS_WITH_DISCLOSURES" : "PASS",
  };
}

/**
 * Hard invariant (only when relationship is MUST_MATCH).
 */
export function assertTopAlternativeEqualsOverviewLeader(reconciliation) {
  if (reconciliation.relationship?.verdict !== RELATIONSHIP_VERDICTS.MUST_MATCH) {
    return { enforced: false, pass: true };
  }
  if (!reconciliation.topAlternative && !reconciliation.highestByPresence) {
    return { enforced: true, pass: true };
  }
  return { enforced: true, pass: reconciliation.match === true, reconciliation };
}
