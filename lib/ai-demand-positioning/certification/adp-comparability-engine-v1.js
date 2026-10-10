/**
 * Formal ADP comparability engine (Parts 10–12).
 * Applies to PoP, peer, brand, portfolio, and market comparisons.
 */

import { PROVIDERS } from "../data-model.js";
import { scenarioIdsFromPeriod } from "./adp-scenario-universe-contract-v1.js";
import { mapLegacyCertificationToPipelineState, ADP_PERIOD_PIPELINE_STATES } from "./adp-period-pipeline-states-v1.js";

export const ADP_COMPARABILITY_ENGINE_VERSION = "adp_comparability_engine_v1";

export const ADP_COMPARABILITY_OUTCOMES = Object.freeze({
  EXACT_COMPARABLE: "EXACT_COMPARABLE",
  COMMON_SET_COMPARABLE: "COMMON_SET_COMPARABLE",
  DIRECTIONAL_ONLY: "DIRECTIONAL_ONLY",
  NOT_COMPARABLE: "NOT_COMPARABLE",
});

const CUSTOMER_COMPARATIVE_BLOCKLIST = Object.freeze([
  "improved",
  "declined",
  "outperformed",
  "underperformed",
  "vs peer",
  "change %",
  "change%",
]);

export const NOT_FORMALLY_COMPARABLE_CUSTOMER_COPY =
  "Not formally comparable due to measurement-universe differences.";

function providerSetFromPeriod(period) {
  const fromMeta = period?.providerSet || period?.providers;
  if (Array.isArray(fromMeta) && fromMeta.length) return [...fromMeta].sort();
  const fromObs = [
    ...new Set((period?.observations || []).map((o) => o.provider).filter(Boolean)),
  ].sort();
  return fromObs.length ? fromObs : [...PROVIDERS];
}

function parserVersions(period) {
  return {
    rankParserVersion: period?.rankParserVersion || period?.parserVersions?.rank || null,
    citationParserVersion: period?.citationParserVersion || period?.parserVersions?.citation || null,
    identityContractVersion:
      period?.identityContractVersion || period?.parserVersions?.identity || null,
    metricComputationVersion: period?.metricComputationVersion || null,
  };
}

function certificationQuality(period) {
  const status = mapLegacyCertificationToPipelineState(
    period?.certificationStatus || (period?.certified ? "CERTIFIED" : "LEGACY_UNCERTIFIED")
  );
  if (status === ADP_PERIOD_PIPELINE_STATES.CERTIFIED) return "CERTIFIED";
  if (status === ADP_PERIOD_PIPELINE_STATES.LEGACY_UNCERTIFIED) return "LEGACY_UNCERTIFIED";
  if (status === ADP_PERIOD_PIPELINE_STATES.QA_PASSED) return "QA_PASSED";
  return status;
}

function setEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

function intersect(a, b) {
  const bs = new Set(b);
  return a.filter((x) => bs.has(x));
}

/**
 * evaluateAdpComparability(periodA, periodB)
 */
export function evaluateAdpComparability(periodA, periodB, options = {}) {
  const reasons = [];
  if (!periodA || !periodB) {
    return {
      outcome: ADP_COMPARABILITY_OUTCOMES.NOT_COMPARABLE,
      reasons: ["missing_period"],
      commonComparableScenarioSet: [],
      engineVersion: ADP_COMPARABILITY_ENGINE_VERSION,
    };
  }

  const idsA = scenarioIdsFromPeriod(periodA);
  const idsB = scenarioIdsFromPeriod(periodB);
  const common = intersect(idsA, idsB).sort();
  const providersA = providerSetFromPeriod(periodA);
  const providersB = providerSetFromPeriod(periodB);
  const parsersA = parserVersions(periodA);
  const parsersB = parserVersions(periodB);
  const qualityA = certificationQuality(periodA);
  const qualityB = certificationQuality(periodB);

  if (!idsA.length || !idsB.length) {
    reasons.push("missing_scenario_ids");
  }
  if (idsA.length !== idsB.length) {
    reasons.push("scenario_count_mismatch");
  }
  if (!setEqual(idsA, idsB)) {
    reasons.push("scenario_id_set_mismatch");
  }
  if (!setEqual(providersA, providersB)) {
    reasons.push("provider_set_mismatch");
  }

  const methodA = periodA.measurementMethod || periodA.measurementPhase || null;
  const methodB = periodB.measurementMethod || periodB.measurementPhase || null;
  if (methodA && methodB && methodA !== methodB) {
    reasons.push("measurement_method_mismatch");
  }

  if (
    (parsersA.rankParserVersion || parsersB.rankParserVersion) &&
    parsersA.rankParserVersion !== parsersB.rankParserVersion
  ) {
    reasons.push("rank_parser_version_mismatch");
  }
  if (
    (parsersA.citationParserVersion || parsersB.citationParserVersion) &&
    parsersA.citationParserVersion !== parsersB.citationParserVersion
  ) {
    reasons.push("citation_parser_version_mismatch");
  }

  if (
    qualityA === "LEGACY_UNCERTIFIED" ||
    qualityB === "LEGACY_UNCERTIFIED"
  ) {
    reasons.push("legacy_uncertified_metadata");
  }

  // Territory distribution rough check
  const terrA = periodA.scenarioUniverse?.territoryAssignments || {};
  const terrB = periodB.scenarioUniverse?.territoryAssignments || {};
  if (Object.keys(terrA).length && Object.keys(terrB).length) {
    let terrMismatch = 0;
    for (const id of common) {
      if (terrA[id] && terrB[id] && terrA[id] !== terrB[id]) terrMismatch += 1;
    }
    if (terrMismatch > 0) reasons.push("territory_assignment_mismatch");
  }

  let outcome = ADP_COMPARABILITY_OUTCOMES.NOT_COMPARABLE;
  if (
    setEqual(idsA, idsB) &&
    setEqual(providersA, providersB) &&
    !reasons.includes("measurement_method_mismatch") &&
    !reasons.includes("rank_parser_version_mismatch") &&
    !reasons.includes("citation_parser_version_mismatch") &&
    !reasons.includes("territory_assignment_mismatch")
  ) {
    outcome =
      reasons.includes("legacy_uncertified_metadata") && options.strictLegacy !== false
        ? ADP_COMPARABILITY_OUTCOMES.DIRECTIONAL_ONLY
        : ADP_COMPARABILITY_OUTCOMES.EXACT_COMPARABLE;
  } else if (
    common.length >= Math.min(idsA.length, idsB.length) * 0.8 &&
    common.length >= 10 &&
    setEqual(providersA, providersB)
  ) {
    outcome = ADP_COMPARABILITY_OUTCOMES.COMMON_SET_COMPARABLE;
  } else if (common.length >= 5 && setEqual(providersA, providersB)) {
    outcome = ADP_COMPARABILITY_OUTCOMES.DIRECTIONAL_ONLY;
  } else {
    outcome = ADP_COMPARABILITY_OUTCOMES.NOT_COMPARABLE;
  }

  // Explicit override: scenario ID mismatch with different counts is never EXACT
  if (reasons.includes("scenario_id_set_mismatch")) {
    if (outcome === ADP_COMPARABILITY_OUTCOMES.EXACT_COMPARABLE) {
      outcome = common.length
        ? ADP_COMPARABILITY_OUTCOMES.COMMON_SET_COMPARABLE
        : ADP_COMPARABILITY_OUTCOMES.NOT_COMPARABLE;
    }
  }

  return {
    outcome,
    reasons,
    periodAId: periodA.periodId || null,
    periodBId: periodB.periodId || null,
    scenarioCountA: idsA.length,
    scenarioCountB: idsB.length,
    commonComparableScenarioSet: common,
    commonScenarioCount: common.length,
    providerSetA: providersA,
    providerSetB: providersB,
    identityQualityA: qualityA,
    identityQualityB: qualityB,
    parserVersionsA: parsersA,
    parserVersionsB: parsersB,
    customerGate: buildCustomerComparisonGate(outcome),
    engineVersion: ADP_COMPARABILITY_ENGINE_VERSION,
  };
}

/**
 * Customer comparison gate (Part 11).
 */
export function buildCustomerComparisonGate(comparabilityOutcome) {
  const outcome = String(comparabilityOutcome || ADP_COMPARABILITY_OUTCOMES.NOT_COMPARABLE);
  const formal =
    outcome === ADP_COMPARABILITY_OUTCOMES.EXACT_COMPARABLE ||
    outcome === ADP_COMPARABILITY_OUTCOMES.COMMON_SET_COMPARABLE;
  return {
    allowComparativeLanguage: formal,
    allowChangePercent: outcome === ADP_COMPARABILITY_OUTCOMES.EXACT_COMPARABLE,
    allowPeerVs: formal,
    blockedPhrases: formal ? [] : [...CUSTOMER_COMPARATIVE_BLOCKLIST],
    customerCopy: formal ? null : NOT_FORMALLY_COMPARABLE_CUSTOMER_COPY,
    outcome,
  };
}

/**
 * Matched peer comparison helper (Part 12).
 */
export function buildMatchedPeerComparison(periodA, periodB, options = {}) {
  const comparability = evaluateAdpComparability(periodA, periodB, options);
  const formal =
    comparability.outcome === ADP_COMPARABILITY_OUTCOMES.EXACT_COMPARABLE ||
    comparability.outcome === ADP_COMPARABILITY_OUTCOMES.COMMON_SET_COMPARABLE;
  return {
    formalPeerComparisonPermitted: formal,
    COMMON_COMPARABLE_SCENARIO_SET: comparability.commonComparableScenarioSet,
    comparability,
  };
}
