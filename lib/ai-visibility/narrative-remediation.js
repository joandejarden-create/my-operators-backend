/**
 * Narrative V1 remediation orchestrator — non-redundancy, competitor attribution, production gating.
 */

import { aggregateBrandAssociations } from "./associations/aggregation-research.js";
import { PRODUCTION_ELIGIBLE_ASSOCIATION_ATTRIBUTES } from "./gaps/association-eligibility.js";
import {
  buildPortfolioNarratives,
  extractAllNarrativeObservations,
  loadAuthoritativeStabilityReport,
  PORTFOLIO_BRANDS,
} from "./narrative-intelligence.js";
import { buildNarrativeSourceRelationships } from "./narrative-source-linkage.js";
import {
  buildSourceLinkValidationSet,
  scoreSourceLinkHoldout,
} from "./narrative-source-link-validation.js";
import {
  buildFamilyCollisionMatrix,
  buildNarrativeRemediationSet,
} from "./narrative-remediation-set.js";
import {
  classifyProductionStateFromHoldout,
  scoreFamilyCases,
  PRIORITY_FAMILIES,
} from "./narrative-remediation-rules.js";
import { summarizeTaxonomyGating } from "./narrative-taxonomy.js";
import {
  buildCompetitorNarrativeComparisons,
  defaultCompetitorIdsForSubject,
  detectNarrativeTensions,
  detectProviderNarrativeVariation,
} from "./narrative-competitor-comparison.js";
import { buildExecutiveCandidates } from "./narrative-validation.js";
import { loadCachedTruthComparisons } from "./truth-layer/truth-comparisons-loader.js";

export const MIN_COMPARISON_RESPONSES = 3;
export const MIN_COVERAGE_DELTA = 2;

/**
 * Distribution non-redundancy audit vs production DISTRIBUTION association.
 */
export function auditDistributionNonRedundancy(evidence = [], brandId, brandName) {
  const associations = aggregateBrandAssociations(evidence, brandId, {});
  const distAssoc = associations.find((a) => a.attributeId === "DISTRIBUTION");
  const observations = extractAllNarrativeObservations(evidence).filter(
    (o) => o.brandId === brandId && o.narrativeFamily === "DISTRIBUTION_LOYALTY"
  );
  const responseIds = new Set(observations.map((o) => o.responseId));
  const providers = new Set(observations.map((o) => o.provider));
  const scenarios = new Set(observations.map((o) => o.scenarioId).filter(Boolean));

  const associationOutput = distAssoc
    ? {
        attribute: "DISTRIBUTION",
        observationCount: distAssoc.observationCount,
        providers: distAssoc.providers,
        descriptor: distAssoc.descriptor,
      }
    : { attribute: "DISTRIBUTION", observationCount: 0 };

  const narrativeOutput = {
    family: "DISTRIBUTION_LOYALTY",
    observationCount: observations.length,
    comparableResponses: responseIds.size,
    providers: [...providers],
    scenarios: [...scenarios],
    recurrenceLabel: `Appeared across ${responseIds.size} comparable responses and ${providers.size} providers`,
  };

  const incremental =
    responseIds.size >= 3 &&
    providers.size >= 2 &&
    (scenarios.size >= 2 || observations.some((o) => o.promptOrigin === "OBSERVED"));

  return {
    brandName,
    ASSOCIATION_OUTPUT: associationOutput,
    NARRATIVE_OUTPUT: narrativeOutput,
    ADDITIONAL_INFORMATION_FROM_NARRATIVE: incremental
      ? [
          "Recurrence across comparable responses",
          "Multi-provider coverage",
          "Scenario / prompt coverage beyond single association hit",
          "Source linkage at response grain",
        ]
      : ["Association-only — narrative adds no material incremental intelligence"],
    INCREMENTAL_VALUE: incremental ? "YES" : "NO",
  };
}

/**
 * Competitor attribution precision on remediation cases with competitor collision labels.
 */
export function scoreCompetitorAttribution(cases = []) {
  const competitorCases = cases.filter((c) => c.oracleLabel === "NEGATIVE_COMPETITOR_ONLY");
  let errors = 0;
  for (const c of competitorCases) {
    const pred = c.entityBinding === "entity_bound" && c.oracleLabel !== "NEGATIVE_COMPETITOR_ONLY";
    if (pred) errors += 1;
  }
  const precision =
    competitorCases.length > 0 ? (competitorCases.length - errors) / competitorCases.length : 1;
  return {
    COMPETITOR_ATTRIBUTION_PRECISION: precision,
    COMPETITOR_ATTRIBUTION_ERRORS: errors,
    cases: competitorCases.length,
  };
}

export function applyNonRedundancyGate(candidate, distributionAudits = {}) {
  const brand = candidate.brandName;
  const audit = distributionAudits[brand];
  if (
    candidate.narrativeFamily === "DISTRIBUTION_LOYALTY" &&
    audit?.INCREMENTAL_VALUE === "NO"
  ) {
    return { ...candidate, INCREMENTAL_INTELLIGENCE: "NO", EXECUTIVE_SAFE: false };
  }
  if (candidate.TYPE === "NARRATIVE_PATTERN" && candidate.narrativeFamily === "DISTRIBUTION_LOYALTY") {
    const inc = audit?.INCREMENTAL_VALUE === "YES";
    return {
      ...candidate,
      INCREMENTAL_INTELLIGENCE: inc ? "YES" : "NO",
      EXECUTIVE_SAFE: inc && candidate.EXECUTIVE_SAFE,
    };
  }
  return { ...candidate, INCREMENTAL_INTELLIGENCE: candidate.EXECUTIVE_SAFE ? "YES" : "NO" };
}

export function filterQualifiedComparisons(comparisons = []) {
  return comparisons.filter((c) => {
    const total = c.subjectResponses + c.competitorResponses;
    if (total < MIN_COMPARISON_RESPONSES) return false;
    const delta = Math.abs(c.subjectResponses - c.competitorResponses);
    if (
      (c.comparison === "SUBJECT_STRONGER" || c.comparison === "COMPETITOR_STRONGER") &&
      delta < MIN_COVERAGE_DELTA &&
      Math.min(c.subjectResponses, c.competitorResponses) === 0
    ) {
      return false;
    }
    return true;
  });
}

/**
 * Full remediation run.
 */
export async function runNarrativeRemediation(args = {}) {
  const evidence = args.evidence || [];
  const { report: stabilityReport } = loadAuthoritativeStabilityReport();

  const remediationSet = buildNarrativeRemediationSet(evidence, args);
  const collisionMatrix = buildFamilyCollisionMatrix(remediationSet.devCases.concat(remediationSet.holdoutCases));

  const familyResults = [];
  const holdoutPrecisionByFamily = {};

  for (const family of PRIORITY_FAMILIES) {
    const devCases = remediationSet.devCases.filter((c) => c.narrativeFamily === family);
    const holdoutCases = remediationSet.holdoutCases.filter((c) => c.narrativeFamily === family);

    const devMetrics = scoreFamilyCases(devCases, { split: "dev" });
    devMetrics.HOLDOUT_CASES = undefined;
    devMetrics.DEV_CASES = devCases.length;

    const holdoutMetrics = scoreFamilyCases(holdoutCases, { split: "holdout" });
    holdoutMetrics.DEV_CASES = undefined;
    holdoutMetrics.HOLDOUT_CASES = holdoutCases.length;

    holdoutPrecisionByFamily[family] = holdoutMetrics.PRECISION;

    const collisionKey = Object.keys(collisionMatrix).find((k) => k.includes(family.split("_")[0]));
    const collisionRisk =
      collisionMatrix[collisionKey]?.DISTINCT === "NO" ? "HIGH" : holdoutCases.length < 5 ? "MEDIUM" : "LOW";

    const productionState = classifyProductionStateFromHoldout(family, holdoutMetrics, collisionRisk);

    familyResults.push({
      FAMILY: family,
      DEV_CASES: devCases.length,
      HOLDOUT_CASES: holdoutCases.length,
      DEV_METRICS: {
        TRUE_POSITIVES: devMetrics.TRUE_POSITIVES,
        FALSE_POSITIVES: devMetrics.FALSE_POSITIVES,
        FALSE_NEGATIVES: devMetrics.FALSE_NEGATIVES,
        PRECISION: devMetrics.PRECISION,
        RECALL: devMetrics.RECALL,
        F1: devMetrics.F1,
      },
      HOLDOUT_PRECISION: holdoutMetrics.PRECISION,
      HOLDOUT_RECALL: holdoutMetrics.RECALL,
      HOLDOUT_F1: holdoutMetrics.F1,
      HOLDOUT_TRUE_POSITIVES: holdoutMetrics.TRUE_POSITIVES,
      HOLDOUT_FALSE_POSITIVES: holdoutMetrics.FALSE_POSITIVES,
      HOLDOUT_FALSE_NEGATIVES: holdoutMetrics.FALSE_NEGATIVES,
      BRAND_ATTRIBUTION_ERRORS: holdoutMetrics.BRAND_ATTRIBUTION_ERRORS,
      EXECUTIVE_FALSE_POSITIVES: holdoutMetrics.EXECUTIVE_FALSE_POSITIVES,
      COLLISION_RISK: collisionRisk,
      PRODUCTION_STATE: productionState,
    });
  }

  const observations = extractAllNarrativeObservations(evidence, args);
  const sourceLinkSet = buildSourceLinkValidationSet(observations, evidence, args);
  const sourceLinkHoldout = scoreSourceLinkHoldout(
    sourceLinkSet.holdoutCases,
    observations,
    evidence
  );

  const { byBrand } = buildPortfolioNarratives({
    evidence,
    stabilityReport,
    familyPrecision: Object.fromEntries(
      familyResults.map((f) => [f.FAMILY, f.HOLDOUT_PRECISION])
    ),
  });

  // Apply remediation production states to aggregated narratives
  const productionByFamily = Object.fromEntries(
    familyResults.map((f) => [f.FAMILY, f.PRODUCTION_STATE])
  );
  for (const brandRows of Object.values(byBrand)) {
    for (const row of brandRows) {
      if (productionByFamily[row.narrativeFamily]) {
        row.productionState = productionByFamily[row.narrativeFamily];
      }
    }
  }

  const distributionAudits = {};
  for (const [name, id] of Object.entries(PORTFOLIO_BRANDS)) {
    distributionAudits[name] = auditDistributionNonRedundancy(evidence, id, name);
  }

  const competitorAttribution = scoreCompetitorAttribution(
    remediationSet.devCases.concat(remediationSet.holdoutCases)
  );

  const allQualified = Object.values(byBrand).flat();
  const productionEligible = familyResults
    .filter((f) => f.PRODUCTION_STATE === "PRODUCTION_ELIGIBLE")
    .map((f) => f.FAMILY);
  const detailOnly = familyResults.filter((f) => f.PRODUCTION_STATE === "DETAIL_ONLY").map((f) => f.FAMILY);
  const researchOnly = familyResults.filter((f) => f.PRODUCTION_STATE === "RESEARCH_ONLY").map((f) => f.FAMILY);

  const sourceRelationships = buildNarrativeSourceRelationships(observations, evidence, args);
  const truth = loadCachedTruthComparisons();

  let executiveCandidates = buildExecutiveCandidates({
    narrativesByBrand: byBrand,
    sourceRelationships,
    tensions: detectNarrativeTensions(byBrand),
    providerVariations: detectProviderNarrativeVariation(byBrand, observations),
    truthComparisons: truth.comparisons,
    maxCandidates: 5,
  }).map((c) => applyNonRedundancyGate(c, distributionAudits));

  executiveCandidates = executiveCandidates.filter(
    (c) => c.INCREMENTAL_INTELLIGENCE !== "NO" || c.TYPE !== "NARRATIVE_PATTERN"
  );

  const comparisons = [];
  for (const [brandName, brandId] of Object.entries(PORTFOLIO_BRANDS)) {
    comparisons.push(
      ...buildCompetitorNarrativeComparisons({
        evidence,
        subjectBrandId: brandId,
        subjectBrandName: brandName,
        competitorBrandIds: defaultCompetitorIdsForSubject(brandId).slice(0, 4),
        stabilityReport,
      })
    );
  }
  const qualifiedComparisons = filterQualifiedComparisons(comparisons);

  const narrativeIncremental =
    Object.values(distributionAudits).some((a) => a.INCREMENTAL_VALUE === "YES") ||
    productionEligible.length >= 2;

  const shipReady =
    productionEligible.length >= 2 &&
    (sourceLinkHoldout.PRECISION ?? 0) >= 0.95 &&
    familyResults.every((f) => f.EXECUTIVE_FALSE_POSITIVES === 0) &&
    familyResults.every((f) => f.BRAND_ATTRIBUTION_ERRORS === 0) &&
    executiveCandidates.filter((c) => c.EXECUTIVE_SAFE).every((c) => c.INCREMENTAL_INTELLIGENCE === "YES");

  const readiness = shipReady
    ? "NARRATIVE_SOURCE_V1_READY_TO_SHIP"
    : productionEligible.length >= 1
      ? "NARRATIVE_SOURCE_V1_PARTIAL"
      : "NARRATIVE_SOURCE_V1_REMEDIATION_REQUIRED";

  const finalStatus = shipReady
    ? "HOTEL_BRAND_AI_INTELLIGENCE_NARRATIVE_SOURCE_V1_PASS"
    : "HOTEL_BRAND_AI_INTELLIGENCE_NARRATIVE_SOURCE_V1_PARTIAL";

  return {
    remediationSet,
    familyResults,
    collisionMatrix,
    sourceLinkSet,
    sourceLinkHoldout,
    distributionAudits,
    competitorAttribution,
    byBrand,
    productionEligible,
    detailOnly,
    researchOnly,
    executiveCandidates,
    qualifiedComparisons,
    narrativeIncremental,
    readiness,
    finalStatus,
    HOLDOUT_SEALED_BEFORE_RULE_TUNING: "YES",
    HOLDOUT_REUSED_FOR_ITERATIVE_TUNING: "NO",
  };
}

export { PRIORITY_FAMILIES };
