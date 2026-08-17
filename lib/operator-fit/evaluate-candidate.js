/**
 * Compose layered Operator Alignment (raw + displayed) without hiding layers.
 */

import { PRIMARY_LAYER_WEIGHTS } from "./config.js";
import {
  scoreAllOperatorProjectFactors,
  aggregateOperatorProjectAlignment,
} from "./alignment-factors.js";
import { evaluateBrandOperatorCompatibility } from "./brand-operator-compatibility.js";
import { evaluateOperatingStructureAlignment } from "./operating-structure.js";
import {
  calculateDataCoverage,
  calculateEvidenceConfidence,
  applyEvidenceCeiling,
} from "./evidence-and-coverage.js";
import { evaluateExecutionRisk } from "./execution-risk.js";
import { evaluateEligibility } from "./eligibility.js";
import { buildExplanations } from "./explanations.js";
import {
  OPERATOR_FIT_ENGINE_VERSION,
  resolveOperatorFitMethodology,
  OPERATOR_FIT_METHODOLOGY_V21,
  getOperatorFitEngineVersionForMethodology,
} from "./feature-flag.js";
import { scalarValue } from "./adapters/field-state.js";
import { evaluateCandidateV21 } from "./v21/evaluate-candidate.js";

function composePrimaryRaw(opProjectRaw, structureAlign, brandCompat) {
  let weighted = 0;
  let denom = 0;

  const wOp = PRIMARY_LAYER_WEIGHTS.operatorProjectAlignment;
  weighted += opProjectRaw * wOp;
  denom += wOp;

  const wSt = PRIMARY_LAYER_WEIGHTS.operatingStructureAlignment;
  if (structureAlign.state === "not_applicable") {
    // skip
  } else if (structureAlign.state === "unknown" || structureAlign.score == null) {
    denom += wSt; // unknown contributes 0
  } else {
    weighted += structureAlign.score * wSt;
    denom += wSt;
  }

  const wBr = PRIMARY_LAYER_WEIGHTS.brandOperatorCompatibility;
  if (brandCompat.state === "not_applicable") {
    // skip — N/A not unknown
  } else if (brandCompat.state === "unknown" || brandCompat.numericForComposition == null) {
    denom += wBr;
  } else {
    weighted += brandCompat.numericForComposition * wBr;
    denom += wBr;
  }

  return denom > 0 ? Math.round((weighted / denom) * 10) / 10 : 0;
}

/**
 * Evaluate a single candidate against a project.
 * @param {object} [opts]
 * @param {'v2'|'v21'} [opts.methodology] — default v2 (reproducible). v21 is internal/shadow only.
 */
export function evaluateCandidate(project, operator, opts = {}) {
  const methodology = resolveOperatorFitMethodology(opts);
  if (methodology === OPERATOR_FIT_METHODOLOGY_V21) {
    return evaluateCandidateV21(project, operator, opts);
  }

  const eligibility = evaluateEligibility(project, operator);
  const factors = scoreAllOperatorProjectFactors(project, operator);
  const opProjectAgg = aggregateOperatorProjectAlignment(factors);
  const brandCompat = evaluateBrandOperatorCompatibility(project, operator);
  const structureAlign = evaluateOperatingStructureAlignment(project, operator);
  const coverage = calculateDataCoverage({
    operatorProjectAgg: opProjectAgg,
    brandCompat,
    structureAlign,
  });
  const confidence = calculateEvidenceConfidence(operator, coverage, opProjectAgg);
  const risk = evaluateExecutionRisk(project, operator, {
    eligibility,
    coverage,
    structureAlign,
    brandCompat,
  });

  const primaryRaw = composePrimaryRaw(
    opProjectAgg.rawScore,
    structureAlign,
    brandCompat
  );
  const afterRisk = Math.max(
    0,
    Math.round((primaryRaw - risk.cappedPenaltyPoints) * 10) / 10
  );
  const { displayedScore, ceilingApplied } = applyEvidenceCeiling(afterRisk, confidence);

  // Fee / economics — never invent a 75
  const feeField = operator.commercial?.feeEconomics;
  const economics = {
    state: feeField?.state || "unknown",
    summary: feeField?.state === "present" ? String(feeField.value) : null,
    numericScore: null,
    note:
      feeField?.state === "present"
        ? "Economics facts available; no comparative fee score applied."
        : "Operator economics are unknown — no default fee score applied.",
    validationItems:
      feeField?.state === "present"
        ? ["Validate fee competitiveness during outreach."]
        : ["Request comparable fee/commercial terms during outreach."],
  };

  const explanations = buildExplanations({
    operator,
    eligibility,
    factors,
    brandCompat,
    structureAlign,
    confidence,
    coverage,
    risk,
    economics,
  });

  const name =
    operator.identity?.value?.name ||
    scalarValue(operator.identity) ||
    "Operator";

  return {
    candidateId: operator.operatorId || name,
    candidateType: operator.candidateType,
    operatorName: name,
    eligibilityStatus: eligibility.status,
    eligibilityReasons: eligibility.reasons,
    eligibilityConditions: eligibility.conditions,
    eligibilityHardConflicts: eligibility.hardConflicts,
    eligibilityUnknowns: eligibility.unknowns,
    layers: {
      eligibility,
      operatorProjectAlignment: {
        rawScore: opProjectAgg.rawScore,
        factors: opProjectAgg.factors,
        applicableWeight: opProjectAgg.applicableWeight,
        knownWeight: opProjectAgg.knownWeight,
        unknownWeight: opProjectAgg.unknownWeight,
      },
      brandOperatorCompatibility: brandCompat,
      operatingStructureAlignment: structureAlign,
      evidenceConfidence: confidence,
      dataCoverage: coverage,
      executionRisk: risk,
      economics,
    },
    rawOperatorAlignment: afterRisk,
    rawBeforeRisk: primaryRaw,
    displayedOperatorAlignment: displayedScore,
    evidenceConfidence: confidence.label,
    dataCoveragePct: coverage.coveragePct,
    confidenceCeilingApplied: ceilingApplied,
    brandOperatorCompatibility: brandCompat.category,
    operatingStructureAlignmentScore: structureAlign.score,
    operatingStructureAlignmentState: structureAlign.state,
    executionRiskPenalty: risk.cappedPenaltyPoints,
    factorBreakdown: opProjectAgg.factors,
    whyItMatches: explanations.whyItMatches,
    potentialConcerns: explanations.potentialConcerns,
    unknowns: explanations.unknowns,
    validationQuestions: explanations.validationQuestions,
    sourceSummaries: explanations.sourceSummaries,
    relevantBrands: listPreferredOverlap(project, operator),
    featureVersion: getOperatorFitEngineVersionForMethodology(methodology) || OPERATOR_FIT_ENGINE_VERSION,
    methodology: "v2",
    rank: null,
  };
}

function listPreferredOverlap(project, operator) {
  const preferred = listValueSafe(project.selectedOrEvaluatedBrands);
  const opBrands = [
    ...listValueSafe(operator.brandsOperated),
    ...listValueSafe(operator.brandFamilies),
  ];
  if (operator.brandManagedMeta?.brandName) {
    return [operator.brandManagedMeta.brandName];
  }
  return preferred.filter((b) =>
    opBrands.some(
      (o) =>
        String(o).toLowerCase().includes(String(b).toLowerCase()) ||
        String(b).toLowerCase().includes(String(o).toLowerCase())
    )
  );
}

function listValueSafe(field) {
  if (!field) return [];
  if (field.state === "present" || field.state === "inferred") {
    const v = field.value;
    if (Array.isArray(v)) return v.map(String);
    if (v != null) return [String(v)];
  }
  return [];
}
