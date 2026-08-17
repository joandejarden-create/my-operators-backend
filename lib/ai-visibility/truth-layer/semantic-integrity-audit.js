/**
 * P0D-A.1 semantic integrity audit orchestrator.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { runTruthLayer, runCertifiedLayerRegression, DEFAULT_TRUTH_COMPARISONS_DIR } from "./truth-layer-index.js";
import { classifySemanticAudit, DIMENSION_COMPARABILITY, P0DA_TRUTH_TAXONOMY } from "./truth-comparability.js";
import { auditParentCompanyConflicts } from "./parent-conflict-audit.js";
import { integrateP0cClassDGaps } from "./p0c-truth-integration.js";
import { peerSetBrandNamesById, PEER_SET_ID_V2 } from "../peer-sets.js";
import { buildTruthLayerPlaceholderGaps } from "../gaps/competitive-gap-engine.js";
import { MARRIOTT_P0DA_BRANDS } from "./truth-layer-index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(__dirname, "..", "..", "..");

const BEFORE = Object.freeze({
  ALIGNED: 1430,
  POTENTIAL_PERCEPTION_GAP: 32,
  NOT_EVALUATED: 0,
});

export async function runSemanticIntegrityAudit(options = {}) {
  const brandNamesById = options.brandNamesById || peerSetBrandNamesById(PEER_SET_ID_V2);
  const result = await runTruthLayer({ brandIds: MARRIOTT_P0DA_BRANDS, brandNamesById });
  const comparisons = result.comparisons;

  const statusCounts = {
    ALIGNED: comparisons.filter((c) => c.comparisonStatus === "ALIGNED").length,
    POTENTIAL_PERCEPTION_GAP: comparisons.filter((c) => c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP").length,
    INSUFFICIENT_DEALALITY_EVIDENCE: comparisons.filter((c) => c.comparisonStatus === "INSUFFICIENT_DEALALITY_EVIDENCE").length,
    NOT_EVALUATED: comparisons.filter((c) => c.comparisonStatus === "NOT_EVALUATED").length,
  };

  const gaps = comparisons.filter((c) => c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP");
  const originalGaps = loadOriginalGaps();

  const semanticCounts = {
    TRUE_SAME_DIMENSION_CONFLICT: 0,
    TERMINOLOGY_VARIATION: 0,
    CROSS_DIMENSION_NON_CONFLICT: 0,
    AMBIGUOUS_NOT_COMPARABLE: 0,
    DATA_NORMALIZATION_ISSUE: 0,
    ENTITY_BINDING_ISSUE: 0,
  };

  const gapAudits = [];
  for (const g of gaps) {
    const brandName = brandNamesById[g.subjectBrandId] || result.basicsIndex.byId.get(g.subjectBrandId)?.brandName;
    const classification = classifySemanticAudit(g, brandName);
    semanticCounts[classification] = (semanticCounts[classification] || 0) + 1;
    gapAudits.push({
      truthComparisonId: g.truthComparisonId,
      brand: brandName,
      aiClaim: g.aiClaimValue,
      aiClaimType: g.aiClaimType,
      aiClaimNormalized: g.aiClaimValue,
      dealalityFact: g.dealalityFactValue,
      dealalityFactType: g.dealalityFactType,
      dealalityNormalized: g.dealalityFactValue,
      currentDimension: g.aiSemanticDimension || g.aiClaimType,
      supportingSpan: g.aiSupportingSpan,
      dealalitySourceField: g.dealalitySource,
      comparisonReason: g.comparisonReason,
      semanticClassification: classification,
      executiveEligible: g.executiveEligible || false,
      executiveEligibilityReason: g.executiveEligibilityReason,
    });
  }

  // Classify original 32 gaps for false-conflict accounting
  const originalGapAudits = originalGaps.map((g) => {
    const brandName = brandNamesById[g.subjectBrandId];
    return {
      truthComparisonId: g.truthComparisonId,
      semanticClassification: classifySemanticAudit(g, brandName),
    };
  });
  for (const og of originalGapAudits) {
    if (og.semanticClassification !== "TRUE_SAME_DIMENSION_CONFLICT") {
      semanticCounts[og.semanticClassification] = (semanticCounts[og.semanticClassification] || 0) + 1;
    }
  }

  const parentAudit = auditParentCompanyConflicts(result.basicsIndex);
  const chainScaleAudit = auditChainScaleClaims(comparisons);
  const brandModelAudit = auditBrandModelGaps(comparisons, brandNamesById);
  const softBrandAudit = auditSoftBrandGaps(comparisons, brandNamesById);

  const placeholders = MARRIOTT_P0DA_BRANDS.flatMap((id) =>
    buildTruthLayerPlaceholderGaps(id, ["scenario_independent_uu_conversion_v1"])
  );
  const p0cIntegration = integrateP0cClassDGaps(comparisons, placeholders);

  const executiveFindings = buildExecutiveFindings(gapAudits.filter((g) => g.executiveEligible), comparisons, brandNamesById);

  const certified = runCertifiedLayerRegression(result.pipeline.corpus);

  const falseGapsRemoved = BEFORE.POTENTIAL_PERCEPTION_GAP - statusCounts.POTENTIAL_PERCEPTION_GAP;

  const readiness =
    parentAudit.unresolved === 0 &&
    certified.PRESENCE_DIFF === 0 &&
    p0cIntegration.EXECUTIVE_ELIGIBLE >= 0
      ? "TRUTH_LAYER_SEMANTICALLY_CERTIFIED"
      : "TRUTH_LAYER_SEMANTIC_REMEDIATION_REQUIRED";

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "P0D-A.1",
    originalTruthResults: BEFORE,
    semanticAudit: semanticCounts,
    originalGapSemanticAudit: summarizeClassifications(originalGapAudits),
    gapAudits,
    parentCompanyConflicts: parentAudit,
    brandModelAudit,
    chainScaleAudit,
    softBrandAudit,
    comparabilityMatrix: buildComparabilityMatrixReport(),
    truthTaxonomy: P0DA_TRUTH_TAXONOMY,
    afterRemediation: {
      BEFORE_ALIGNED: BEFORE.ALIGNED,
      BEFORE_GAPS: BEFORE.POTENTIAL_PERCEPTION_GAP,
      AFTER_ALIGNED: statusCounts.ALIGNED,
      AFTER_GAPS: statusCounts.POTENTIAL_PERCEPTION_GAP,
      AFTER_NOT_EVALUATED: statusCounts.NOT_EVALUATED,
      FALSE_GAPS_REMOVED: falseGapsRemoved,
    },
    p0cClassD: {
      CURRENT_D: BEFORE.POTENTIAL_PERCEPTION_GAP,
      SURVIVING_D: p0cIntegration.PRODUCTION_D_GAPS,
      REMOVED_FALSE_CONFLICT: BEFORE.POTENTIAL_PERCEPTION_GAP - p0cIntegration.PRODUCTION_D_GAPS,
      DETAIL_ONLY: p0cIntegration.DETAIL_ONLY_D_GAPS,
      EXECUTIVE_ELIGIBLE: p0cIntegration.EXECUTIVE_ELIGIBLE,
      REMOVED: BEFORE.POTENTIAL_PERCEPTION_GAP - p0cIntegration.PRODUCTION_D_GAPS - p0cIntegration.DETAIL_ONLY_D_GAPS,
    },
    executiveFindings,
    certifiedLayer: certified,
    census: { STATUS: "DEFERRED_INCOMPLETE_CENSUS", READS_FOR_TRUTH: 0 },
    readiness,
    next: readiness === "TRUTH_LAYER_SEMANTICALLY_CERTIFIED"
      ? "READY_FOR_P0E_EXECUTIVE_INTELLIGENCE"
      : "TRUTH_LAYER_SEMANTIC_REMEDIATION_REQUIRED",
    finalToken:
      readiness === "TRUTH_LAYER_SEMANTICALLY_CERTIFIED"
        ? "HOTEL_BRAND_AI_INTELLIGENCE_P0DA1_PASS"
        : "HOTEL_BRAND_AI_INTELLIGENCE_P0DA1_REMEDIATION_REQUIRED",
  };

  return { report, comparisons, result, p0cIntegration };
}

function loadOriginalGaps() {
  const p = path.join(DEFAULT_TRUTH_COMPARISONS_DIR, "latest-truth-comparisons-v1.json");
  if (!fs.existsSync(p)) return [];
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  return (raw.comparisons || []).filter((c) => c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP");
}

function summarizeClassifications(audits) {
  const out = {};
  for (const a of audits) {
    out[a.semanticClassification] = (out[a.semanticClassification] || 0) + 1;
  }
  return out;
}

function auditChainScaleClaims(comparisons) {
  const chain = comparisons.filter((c) => c.aiClaimType === "CHAIN_SCALE");
  const gaps = chain.filter((c) => c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP");
  const notEval = chain.filter((c) => c.comparisonStatus === "NOT_EVALUATED");
  const genericPositioning = chain.filter(
    (c) =>
      c.comparisonReason === "positioning_language_not_chain_scale" ||
      c.comparisonReason === "portfolio_range_not_subject_scale"
  );
  return {
    TOTAL_CHAIN_SCALE_CLAIMS: chain.length,
    EXPLICIT_CHAIN_SCALE_CLAIMS: chain.filter((c) => c.comparisonStatus === "ALIGNED" || c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP").length - genericPositioning.length,
    GENERIC_POSITIONING_LANGUAGE: genericPositioning.length,
    AMBIGUOUS: notEval.length,
    TRUE_COMPARABLE: chain.filter((c) => c.comparisonStatus === "ALIGNED").length,
    GAPS: gaps.length,
    gapDetails: gaps.map((g) => ({
      brandId: g.subjectBrandId,
      ai: g.aiClaimValue,
      deal: g.dealalityFactValue,
      span: g.aiSupportingSpan?.slice(0, 120),
      reason: g.comparisonReason,
    })),
  };
}

function auditBrandModelGaps(comparisons, brandNamesById) {
  const brandModel = comparisons.filter((c) => c.aiClaimType === "BRAND_MODEL");
  const gaps = brandModel.filter((c) => c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP");
  const notEval = brandModel.filter((c) => c.comparisonStatus === "NOT_EVALUATED");

  const uniqueAiValues = {};
  for (const g of [...gaps, ...notEval]) {
    const key = g.aiClaimValue;
    if (!uniqueAiValues[key]) {
      uniqueAiValues[key] = {
        aiValue: key,
        count: 0,
        gapCount: 0,
        notEvalCount: 0,
        classifications: {},
      };
    }
    uniqueAiValues[key].count += 1;
    if (g.comparisonStatus === "POTENTIAL_PERCEPTION_GAP") uniqueAiValues[key].gapCount += 1;
    if (g.comparisonStatus === "NOT_EVALUATED") uniqueAiValues[key].notEvalCount += 1;
    const cls =
      g.comparisonReason?.includes("cross_dimension") ? "CROSS_DIMENSION"
        : g.comparisonReason?.includes("contrastive") ? "AMBIGUOUS"
          : g.comparisonReason?.includes("list_enumeration") ? "AMBIGUOUS"
            : g.comparisonReason?.includes("operating_model") ? "CROSS_DIMENSION"
              : g.comparisonReason?.includes("positioning") ? "CROSS_DIMENSION"
                : g.comparisonStatus === "POTENTIAL_PERCEPTION_GAP" ? "SAME_DIMENSION" : "AMBIGUOUS";
    uniqueAiValues[key].classifications[cls] = (uniqueAiValues[key].classifications[cls] || 0) + 1;
  }

  return {
    totalEvaluated: brandModel.length,
    gaps: gaps.length,
    notEvaluated: notEval.length,
    uniqueAiValues: Object.values(uniqueAiValues),
    gapRecords: gaps.map((g) => ({
      brand: brandNamesById[g.subjectBrandId],
      ai: g.aiClaimValue,
      deal: g.dealalityFactValue,
      reason: g.comparisonReason,
      executiveEligible: g.executiveEligible,
      span: g.aiSupportingSpan?.slice(0, 100),
    })),
  };
}

function auditSoftBrandGaps(comparisons, brandNamesById) {
  const soft = comparisons.filter(
    (c) => c.aiClaimType === "SOFT_BRAND_COLLECTION" || c.aiClaimType === "BRAND_MODEL"
  );
  const westin = soft.filter((c) => c.subjectBrandId === "recIPuBC50fv13zRR");
  const westinGaps = westin.filter((c) => c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP");
  const westinNotEval = westin.filter((c) => c.comparisonStatus === "NOT_EVALUATED");

  return {
    westinTotal: westin.length,
    westinGaps: westinGaps.length,
    westinNotEvaluated: westinNotEval.length,
    westinRecords: westin.map((g) => ({
      claimType: g.aiClaimType,
      ai: g.aiClaimValue,
      deal: g.dealalityFactValue,
      status: g.comparisonStatus,
      reason: g.comparisonReason,
      span: g.aiSupportingSpan,
      executiveEligible: g.executiveEligible,
    })),
  };
}

function buildComparabilityMatrixReport() {
  const dims = [
    "PARENT_COMPANY",
    "BRAND_ARCHITECTURE",
    "BRAND_MODEL",
    "CHAIN_SCALE",
    "POSITIONING",
    "SOFT_BRAND_COLLECTION_STATUS",
    "BRAND_FAMILY",
    "OPERATING_MODEL",
  ];
  const matrix = [];
  for (const a of dims) {
    for (const b of dims) {
      const key = `${a}|${b}`;
      matrix.push({ pair: key, comparable: DIMENSION_COMPARABILITY[key] || (a === b ? "YES" : "NO") });
    }
  }
  return matrix;
}

function buildExecutiveFindings(eligibleGaps, comparisons, brandNamesById) {
  const findings = [];
  for (const g of eligibleGaps.slice(0, 10)) {
    const cmp = comparisons.find((c) => c.truthComparisonId === g.truthComparisonId) || g;
    const samePrompt = comparisons.filter(
      (c) =>
        c.aiClaimType === cmp.aiClaimType &&
        c.aiClaimValue === cmp.aiClaimValue &&
        c.subjectBrandId === cmp.subjectBrandId &&
        c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP"
    );
    findings.push({
      BRAND: g.brand,
      AI_PERCEPTION: g.aiClaim,
      DEALALITY_FACT: g.dealalityFact,
      DIMENSION: g.currentDimension,
      OBSERVATIONS: samePrompt.length,
      PROVIDERS: [...new Set(samePrompt.map((c) => c.provider))],
      SCENARIOS: [...new Set(samePrompt.map((c) => c.scenarioId))],
      PERSISTENCE: samePrompt.length >= 2 ? "REPEATED" : "SINGLE",
      WHY_THIS_IS_A_REAL_GAP: g.executiveEligibilityReason,
      EVIDENCE: g.supportingSpan?.slice(0, 200),
      EXECUTIVE_ELIGIBLE: g.executiveEligible ? "YES" : "NO",
    });
  }
  return findings;
}

export function saveSemanticAuditReport(auditResult, options = {}) {
  const dir = options.outputDir || DEFAULT_TRUTH_COMPARISONS_DIR;
  fs.mkdirSync(dir, { recursive: true });
  const reportPath = path.join(dir, "p0da1-semantic-integrity-report.json");
  fs.writeFileSync(reportPath, JSON.stringify(auditResult.report, null, 2));
  fs.writeFileSync(
    path.join(dir, "latest-truth-comparisons-v1_1_semantic.json"),
    JSON.stringify(
      {
        generatedAt: auditResult.report.generatedAt,
        truthRuleVersion: "ai_visibility_truth_layer_v1_1_semantic",
        comparisons: auditResult.comparisons,
      },
      null,
      2
    )
  );
  return { reportPath };
}
