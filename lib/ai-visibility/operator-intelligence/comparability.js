/**
 * Operator commercial comparability V2 (final certification).
 * CORE only when both ELIGIBLE, geography overlaps, models are substitutable
 * for that scenario, production truth is sufficient, and Arbor is not CORE.
 * Brand peer sets and Census are not used.
 */

import { eligibilityFor, ELIGIBILITY } from "./eligibility.js";
import { getOperatorById, isPrimaryMonitoredOperator } from "./universe.js";
import {
  getComparabilityTruth,
  modelsCommerciallySubstitutable,
  OPERATOR_MODEL,
} from "./comparability-truth.js";

export const OPERATOR_COMMERCIAL_COMPARABILITY_VERSION = "operator_commercial_comparability_v2";

export const COMMERCIAL_RELATION = Object.freeze({
  CORE_COMPARABLE: "CORE_COMPARABLE",
  SECONDARY_CONTEXT: "SECONDARY_CONTEXT",
  CONDITIONAL: "CONDITIONAL",
  NON_COMPARABLE: "NON_COMPARABLE",
});

export const ARBOR_LODGING_ID = "recF5Z87OAqFgndoq";

const GEO_FAMILY = Object.freeze({
  GLOBAL: "GLOBAL",
  LATAM: "LATAM_CALA",
  CALA: "LATAM_CALA",
  US_SOUTHEAST: "US_SOUTHEAST",
});

function geoFamily(scope) {
  return GEO_FAMILY[scope] || null;
}

function geoOverlaps(scopeA, scopeB) {
  const a = geoFamily(scopeA);
  const b = geoFamily(scopeB);
  if (!a || !b) return false;
  if (a === "GLOBAL" || b === "GLOBAL") return true;
  return a === b;
}

export function overlappingMonitoredGeography(operatorA, operatorB) {
  return geoOverlaps(operatorA?.monitoredScope, operatorB?.monitoredScope);
}

export function productionTruthSufficientForComparability() {
  return {
    status: "PARTIAL",
    promotedRowsRequire: "YES",
    productionRequired: [
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    censusUsed: false,
    note:
      "Production-required comparability fields are resolved from Operator Master / universe for all 9 operators. Luxury, resort, conversion, and lifestyle remain DETAIL_ONLY, so global TRUTH_SUFFICIENT stays PARTIAL while promoted CORE rows can still be production-safe.",
  };
}

function relation(className, reason) {
  return { relation: className, reason };
}

function pairTruthSufficient(subjectId, otherId) {
  const a = getComparabilityTruth(subjectId);
  const b = getComparabilityTruth(otherId);
  if (!a || !b) return false;
  const required = ["OPERATOR_MODEL", "GEOGRAPHIC_OPERATING_SCOPE", "THIRD_PARTY_MANAGEMENT"];
  return required.every(
    (field) =>
      (a.fieldsResolved || []).includes(field) && (b.fieldsResolved || []).includes(field)
  );
}

/**
 * @returns {{ relation: string, reason: string }}
 */
export function classifyOperatorPair(subjectId, otherId, scenarioId) {
  if (!subjectId || !otherId || subjectId === otherId) {
    return relation(COMMERCIAL_RELATION.NON_COMPARABLE, "same_or_missing_operator");
  }
  if (!isPrimaryMonitoredOperator(subjectId) || !isPrimaryMonitoredOperator(otherId)) {
    return relation(COMMERCIAL_RELATION.NON_COMPARABLE, "unmonitored_operator_not_core_comparable");
  }

  const subject = getOperatorById(subjectId);
  const other = getOperatorById(otherId);
  const subjectTruth = getComparabilityTruth(subjectId);
  const otherTruth = getComparabilityTruth(otherId);
  const subjectElig = eligibilityFor(subjectId, scenarioId);
  const otherElig = eligibilityFor(otherId, scenarioId);
  const subjectModel = subjectTruth?.model || OPERATOR_MODEL.OTHER_UNCERTAIN;
  const otherModel = otherTruth?.model || OPERATOR_MODEL.OTHER_UNCERTAIN;

  const base = {
    subjectEligibility: subjectElig.status,
    otherEligibility: otherElig.status,
    subjectLens: subject.operatorLens,
    otherLens: other.operatorLens,
    subjectModel,
    otherModel,
    truthSufficient: pairTruthSufficient(subjectId, otherId),
  };

  if (subjectElig.status === ELIGIBILITY.OUT_OF_SCOPE || otherElig.status === ELIGIBILITY.OUT_OF_SCOPE) {
    return {
      ...relation(COMMERCIAL_RELATION.NON_COMPARABLE, "scenario_out_of_scope_for_at_least_one_operator"),
      ...base,
    };
  }

  if (!overlappingMonitoredGeography(subject, other)) {
    return {
      ...relation(COMMERCIAL_RELATION.NON_COMPARABLE, "regional_scope_mismatch"),
      ...base,
    };
  }

  if (subjectId === ARBOR_LODGING_ID || otherId === ARBOR_LODGING_ID) {
    return {
      ...relation(
        COMMERCIAL_RELATION.CONDITIONAL,
        "arbor_insufficient_operator_specific_evidence"
      ),
      ...base,
    };
  }

  if (
    subjectElig.status === ELIGIBILITY.INSUFFICIENT_TRUTH ||
    otherElig.status === ELIGIBILITY.INSUFFICIENT_TRUTH ||
    subjectElig.status === ELIGIBILITY.UNKNOWN ||
    otherElig.status === ELIGIBILITY.UNKNOWN
  ) {
    return { ...relation(COMMERCIAL_RELATION.CONDITIONAL, "insufficient_or_unknown_truth"), ...base };
  }

  if (
    subjectElig.status === ELIGIBILITY.CONDITIONALLY_ELIGIBLE ||
    otherElig.status === ELIGIBILITY.CONDITIONALLY_ELIGIBLE
  ) {
    return {
      ...relation(COMMERCIAL_RELATION.CONDITIONAL, "conditional_scenario_eligibility"),
      ...base,
    };
  }

  if (!pairTruthSufficient(subjectId, otherId)) {
    return { ...relation(COMMERCIAL_RELATION.CONDITIONAL, "comparability_truth_incomplete"), ...base };
  }

  if (modelsCommerciallySubstitutable(subjectModel, otherModel, scenarioId)) {
    return {
      ...relation(COMMERCIAL_RELATION.CORE_COMPARABLE, "eligible_overlapping_substitutable_models"),
      ...base,
    };
  }

  if (subjectElig.status === ELIGIBILITY.ELIGIBLE && otherElig.status === ELIGIBILITY.ELIGIBLE) {
    return {
      ...relation(
        COMMERCIAL_RELATION.SECONDARY_CONTEXT,
        "in_scope_but_not_direct_substitutes_for_scenario"
      ),
      ...base,
    };
  }

  return {
    ...relation(COMMERCIAL_RELATION.CONDITIONAL, "unresolved_substitutability"),
    ...base,
  };
}

export function classifyPresentOperators(subjectId, presentOperatorIds, scenarioId) {
  const ids = [...new Set((presentOperatorIds || []).filter((id) => id && id !== subjectId))];
  const byRelation = {
    [COMMERCIAL_RELATION.CORE_COMPARABLE]: [],
    [COMMERCIAL_RELATION.SECONDARY_CONTEXT]: [],
    [COMMERCIAL_RELATION.CONDITIONAL]: [],
    [COMMERCIAL_RELATION.NON_COMPARABLE]: [],
  };
  const pairs = [];
  for (const otherId of ids) {
    const classified = classifyOperatorPair(subjectId, otherId, scenarioId);
    pairs.push({ otherId, ...classified });
    byRelation[classified.relation].push(otherId);
  }
  return { pairs, byRelation };
}

export function summarizeComparabilityModel() {
  return {
    version: OPERATOR_COMMERCIAL_COMPARABILITY_VERSION,
    model: "OPERATOR_MODEL × GEOGRAPHIC_SCOPE × SCENARIO_ELIGIBILITY × SCENARIO_SUBSTITUTABILITY",
    brandPeerSetsReused: false,
    censusUsed: false,
    coreRule:
      "Both ELIGIBLE, overlapping geography, commercially substitutable models for that scenario, production truth sufficient, neither Arbor.",
    secondaryRule:
      "Both in-scope and geographically overlapping but not direct substitutes (including brand-managed vs third-party, and GHL mixed platform vs TPM).",
    conditionalRule:
      "Conditional/insufficient eligibility, incomplete capability truth, or Arbor insufficient presence evidence.",
    nonComparableRule: "Out of scope, regional mismatch, or unmonitored name.",
    truth: productionTruthSufficientForComparability(),
  };
}
