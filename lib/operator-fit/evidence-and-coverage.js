/**
 * Evidence Confidence + Data Coverage (separate layers).
 */

import {
  EVIDENCE_CLASSES,
  EVIDENCE_CLASS_RANK,
  EVIDENCE_CONFIDENCE,
  EVIDENCE_CONFIDENCE_RULES,
  PRIMARY_LAYER_WEIGHTS,
} from "./config.js";
import { listValue, isKnownPositive } from "./adapters/field-state.js";

/**
 * Data coverage: how much applicable scoring weight is known vs unknown.
 */
export function calculateDataCoverage({
  operatorProjectAgg,
  brandCompat,
  structureAlign,
}) {
  let applicable = operatorProjectAgg.applicableWeight || 0;
  let known = operatorProjectAgg.knownWeight || 0;
  let unknown = operatorProjectAgg.unknownWeight || 0;

  // Structure layer
  const sw = PRIMARY_LAYER_WEIGHTS.operatingStructureAlignment;
  applicable += sw;
  if (structureAlign.state === "unknown") unknown += sw;
  else if (structureAlign.state === "not_applicable") {
    applicable -= sw;
  } else known += sw;

  // Brand compat layer
  const bw = PRIMARY_LAYER_WEIGHTS.brandOperatorCompatibility;
  if (brandCompat.state === "not_applicable") {
    // N/A — not in applicable denominator
  } else {
    applicable += bw;
    if (brandCompat.state === "unknown") unknown += bw;
    else known += bw;
  }

  const coveragePct =
    applicable > 0 ? Math.round((known / applicable) * 1000) / 10 : 0;

  const materialMissing = [];
  for (const f of operatorProjectAgg.factors || []) {
    if (f.applicable && f.state === "unknown") {
      materialMissing.push(f.label);
    }
  }
  if (structureAlign.state === "unknown") materialMissing.push("Operating structure alignment");
  if (brandCompat.state === "unknown") materialMissing.push("Brand–operator compatibility");

  return {
    applicableWeight: applicable,
    knownWeight: known,
    unknownWeight: unknown,
    coveragePct,
    materialMissingFields: materialMissing,
  };
}

function bestEvidenceRank(operator) {
  const classes = listValue(operator.evidenceClasses);
  let best = EVIDENCE_CLASS_RANK[EVIDENCE_CLASSES.UNKNOWN];
  for (const c of classes) {
    const r = EVIDENCE_CLASS_RANK[c] ?? 0;
    if (r > best) best = r;
  }
  // Sources with verification flags
  if (isKnownPositive(operator.sources)) {
    for (const s of operator.sources.value || []) {
      if (s && s.verified) best = Math.max(best, EVIDENCE_CLASS_RANK[EVIDENCE_CLASSES.VERIFIED_PROJECT]);
      else if (s && s.independent) {
        best = Math.max(best, EVIDENCE_CLASS_RANK[EVIDENCE_CLASSES.INDEPENDENT_REFERENCED]);
      }
    }
  }
  if (isKnownPositive(operator.comparables)) {
    for (const c of operator.comparables.value || []) {
      if (c && c.verified) best = Math.max(best, EVIDENCE_CLASS_RANK[EVIDENCE_CLASSES.VERIFIED_PROJECT]);
      else if (c && c.referenced) {
        best = Math.max(best, EVIDENCE_CLASS_RANK[EVIDENCE_CLASSES.INDEPENDENT_REFERENCED]);
      } else {
        best = Math.max(best, EVIDENCE_CLASS_RANK[EVIDENCE_CLASSES.DETAILED_OPERATOR_PROVIDED]);
      }
    }
  }
  return best;
}

/**
 * Evidence confidence label — operator-reported alone cannot be Strong.
 */
export function calculateEvidenceConfidence(operator, coverage, operatorProjectAgg) {
  const bestRank = bestEvidenceRank(operator);
  const knownPct =
    (operatorProjectAgg.applicableWeight || 0) > 0
      ? (operatorProjectAgg.knownWeight / operatorProjectAgg.applicableWeight) * 100
      : coverage.coveragePct;

  const hasIndependent =
    bestRank >= EVIDENCE_CONFIDENCE_RULES.strongRequiresMinClassRank;

  let label = EVIDENCE_CONFIDENCE.LIMITED.label;
  let ceiling = EVIDENCE_CONFIDENCE.LIMITED.displayedScoreCeiling;
  let rationale = "Evidence is limited or primarily unknown.";

  if (
    hasIndependent &&
    knownPct >= EVIDENCE_CONFIDENCE_RULES.strongRequiresMaterialFactorCoveragePct
  ) {
    label = EVIDENCE_CONFIDENCE.STRONG.label;
    ceiling = EVIDENCE_CONFIDENCE.STRONG.displayedScoreCeiling;
    rationale =
      "At least one independently supported/referenced/verified source covers material factors.";
  } else if (
    bestRank >= EVIDENCE_CONFIDENCE_RULES.moderateRequiresMinClassRank &&
    knownPct >= EVIDENCE_CONFIDENCE_RULES.moderateRequiresKnownWeightPct
  ) {
    label = EVIDENCE_CONFIDENCE.MODERATE.label;
    ceiling = EVIDENCE_CONFIDENCE.MODERATE.displayedScoreCeiling;
    rationale =
      "Detailed operator-provided or portfolio evidence supports a moderate confidence band.";
  } else if (bestRank >= EVIDENCE_CLASS_RANK[EVIDENCE_CLASSES.GENERAL_CLAIM] && knownPct > 20) {
    label = EVIDENCE_CONFIDENCE.LIMITED.label;
    ceiling = EVIDENCE_CONFIDENCE.LIMITED.displayedScoreCeiling;
    rationale =
      "Available evidence is largely operator-reported or general claims — Strong confidence is not available.";
  }

  // Explicit rule: operator-reported only cannot be Strong
  if (
    label === EVIDENCE_CONFIDENCE.STRONG.label &&
    !hasIndependent
  ) {
    label = EVIDENCE_CONFIDENCE.MODERATE.label;
    ceiling = EVIDENCE_CONFIDENCE.MODERATE.displayedScoreCeiling;
    rationale =
      "Operator-reported evidence alone cannot produce Strong evidence confidence.";
  }

  return {
    label,
    displayedScoreCeiling: ceiling,
    bestEvidenceClassRank: bestRank,
    rationale,
  };
}

/**
 * Apply evidence ceiling to produce displayed score.
 */
export function applyEvidenceCeiling(rawAfterRisk, confidence) {
  const ceiling = confidence.displayedScoreCeiling;
  if (ceiling == null) {
    return {
      displayedScore: Math.max(0, Math.min(100, Math.round(rawAfterRisk * 10) / 10)),
      ceilingApplied: null,
    };
  }
  const displayed = Math.max(0, Math.min(ceiling, Math.round(rawAfterRisk * 10) / 10));
  return {
    displayedScore: displayed,
    ceilingApplied: ceiling,
  };
}
