/**
 * Operator Fit v2.1 — evaluate single candidate (parallel to v2 evaluate-candidate).
 */

import { PRIMARY_LAYER_WEIGHTS } from "../config.js";
import {
  scoreAllOperatorProjectFactorsV21,
  aggregateOperatorProjectAlignment,
} from "./alignment-factors.js";
import { evaluateBrandOperatorCompatibility } from "../brand-operator-compatibility.js";
import { evaluateOperatingStructureAlignment } from "../operating-structure.js";
import {
  calculateDataCoverage,
  calculateEvidenceConfidence,
  applyEvidenceCeiling,
} from "../evidence-and-coverage.js";
import { evaluateExecutionRiskV21 } from "./execution-risk.js";
import { evaluateEligibility } from "../eligibility.js";
import { buildExplanations } from "../explanations.js";
import {
  OPERATOR_FIT_ENGINE_VERSION_V21,
  OPERATOR_FIT_METHODOLOGY_V21,
} from "../feature-flag.js";
import { scalarValue } from "../adapters/field-state.js";
import { V21_DEFAULT_CRI_FORMULATION } from "./config.js";

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
    denom += wSt;
  } else {
    weighted += structureAlign.score * wSt;
    denom += wSt;
  }

  const wBr = PRIMARY_LAYER_WEIGHTS.brandOperatorCompatibility;
  if (brandCompat.state === "not_applicable") {
    // skip
  } else if (brandCompat.state === "unknown" || brandCompat.numericForComposition == null) {
    denom += wBr;
  } else {
    weighted += brandCompat.numericForComposition * wBr;
    denom += wBr;
  }

  return denom > 0 ? Math.round((weighted / denom) * 10) / 10 : 0;
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

export function evaluateCandidateV21(project, operator, opts = {}) {
  const formulationId = opts.criFormulation || V21_DEFAULT_CRI_FORMULATION;
  const eligibility = evaluateEligibility(project, operator);
  const factors = scoreAllOperatorProjectFactorsV21(project, operator, {
    criFormulation: formulationId,
  });
  const opProjectAgg = aggregateOperatorProjectAlignment(factors);
  const brandCompat = evaluateBrandOperatorCompatibility(project, operator);
  const structureAlign = evaluateOperatingStructureAlignment(project, operator);
  const coverage = calculateDataCoverage({
    operatorProjectAgg: opProjectAgg,
    brandCompat,
    structureAlign,
  });
  const confidence = calculateEvidenceConfidence(operator, coverage, opProjectAgg);
  const risk = evaluateExecutionRiskV21(project, operator, {
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
    risk: {
      ...risk,
      items: (risk.items || []).map((i) => ({
        ...i,
        // explanations use kind for concerns; keep unknown as validation
        points: i.appliedPoints ?? 0,
      })),
    },
    economics,
  });

  // Soften comparative language for why — candidate-specific only
  const whyItMatches = (explanations.whyItMatches || []).map((w) =>
    String(w).replace(/^Aligned because of /i, "Relevant strengths include ")
  );

  const name =
    operator.identity?.value?.name ||
    scalarValue(operator.identity) ||
    "Operator";

  const preferred = listValueSafe(project.selectedOrEvaluatedBrands);
  const opBrands = [
    ...listValueSafe(operator.brandsOperated),
    ...listValueSafe(operator.brandFamilies),
  ];
  const relevantBrands = preferred.filter((b) =>
    opBrands.some(
      (o) =>
        String(o).toLowerCase().includes(String(b).toLowerCase()) ||
        String(b).toLowerCase().includes(String(o).toLowerCase())
    )
  );

  const assetFactor = factors.find((f) => f.key === "assetDevelopmentExperience");

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
      v21: {
        cri: assetFactor?.v21?.cri || null,
        geography: factors.find((f) => f.key === "geographyMarket")?.v21 || null,
        developmentMode: assetFactor?.v21?.developmentMode || null,
        criFormulation: formulationId,
      },
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
    whyItMatches: whyItMatches.slice(0, 3),
    potentialConcerns: explanations.potentialConcerns,
    unknowns: explanations.unknowns,
    validationQuestions: explanations.validationQuestions,
    sourceSummaries: explanations.sourceSummaries,
    relevantBrands,
    featureVersion: OPERATOR_FIT_ENGINE_VERSION_V21,
    methodology: OPERATOR_FIT_METHODOLOGY_V21,
    rank: null,
  };
}
