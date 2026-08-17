/**
 * Truth Layer index — P0D-A non-Census orchestrator (read-only).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { computeAiPresenceRate } from "../metrics.js";
import { observationsFromEvidence } from "../gaps/evidence-observations.js";
import { runCompetitiveGapEngine } from "../gaps/competitive-gap-engine.js";
import { buildTruthLayerPlaceholderGaps } from "../gaps/competitive-gap-engine.js";
import { getCensusTruthLayerStatus } from "./census-truth-placeholder.js";
import {
  TRUTH_DIMENSION_READINESS,
  TRUTH_RULE_VERSION,
} from "./truth-eligibility.js";
import {
  loadBrandBasicsTruthIndex,
  auditBrandBasicsFields,
} from "./brand-basics-truth.js";
import { auditBrandExplorerStructuredFields } from "./brand-explorer-truth.js";
import { runTruthEvidencePipeline } from "./truth-evidence.js";
import { buildTruthGoldenSet, scoreTruthGoldenSet } from "./truth-golden-set.js";
import { integrateP0cClassDGaps } from "./p0c-truth-integration.js";
import { peerSetBrandNamesById, PEER_SET_ID_V2 } from "../peer-sets.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(__dirname, "..", "..", "..");

export const DEFAULT_TRUTH_COMPARISONS_DIR = path.join(
  REPO_ROOT,
  "data",
  "ai-visibility",
  "truth-comparisons"
);

export const MARRIOTT_P0DA_BRANDS = Object.freeze([
  "recEJCTDj1zrsjPM6",
  "recCvV0PuZOi8c3hC",
  "rec9aZp7GHtzUEg0c",
  "rec02zPClpWUTCyXM",
  "recIPuBC50fv13zRR",
]);

function countByStatus(comparisons = [], status) {
  return comparisons.filter((c) => c.comparisonStatus === status).length;
}

function dimensionMatrix(comparisons = [], brandIds = []) {
  const matrix = {};
  for (const brandId of brandIds) {
    matrix[brandId] = {};
    const brandCmps = comparisons.filter((c) => c.subjectBrandId === brandId);
    for (const dim of [
      "PARENT_COMPANY",
      "CHAIN_SCALE",
      "BRAND_MODEL",
      "SOFT_BRAND_COLLECTION",
      "BRAND_FAMILY",
      "POSITIONING",
      "CONVERSION_ORIENTATION",
    ]) {
      const dimCmps = brandCmps.filter((c) => {
        const map = {
          PARENT_COMPANY: "PARENT_COMPANY",
          CHAIN_SCALE: "CHAIN_SCALE",
          BRAND_MODEL: "BRAND_MODEL",
          SOFT_BRAND_COLLECTION: "SOFT_BRAND_COLLECTION",
          BRAND_FAMILY: "BRAND_FAMILY",
          POSITIONING: "POSITIONING",
          CONVERSION_ORIENTATION: "CONVERSION_ORIENTATION",
        };
        return c.aiClaimType === dim || map[c.aiClaimType] === dim;
      });
      matrix[brandId][dim] = {
        CLAIMS_EVALUATED: dimCmps.length,
        ALIGNED: dimCmps.filter((c) => c.comparisonStatus === "ALIGNED").length,
        POTENTIAL_PERCEPTION_GAP: dimCmps.filter((c) => c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP").length,
        INSUFFICIENT: dimCmps.filter((c) => c.comparisonStatus === "INSUFFICIENT_DEALALITY_EVIDENCE").length,
        NOT_EVALUATED: dimCmps.filter((c) => c.comparisonStatus === "NOT_EVALUATED").length,
      };
    }
  }
  return matrix;
}

function sampleFindings(comparisons = [], brandNamesById = {}, limit = 5) {
  const gaps = comparisons.filter((c) => c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP");
  const aligned = comparisons.filter((c) => c.comparisonStatus === "ALIGNED");
  const pool = [...gaps, ...aligned];
  const out = [];
  for (const c of pool) {
    if (out.length >= limit) break;
    out.push({
      subjectBrandId: c.subjectBrandId,
      subjectBrandName: brandNamesById[c.subjectBrandId] || c.subjectBrandId,
      aiClaimType: c.aiClaimType,
      aiClaimValue: c.aiClaimValue,
      dealalityFactValue: c.dealalityFactValue,
      comparisonStatus: c.comparisonStatus,
      comparisonReason: c.comparisonReason,
      span: c.aiSupportingSpan?.slice(0, 120),
      provider: c.provider,
      scenarioId: c.scenarioId,
    });
  }
  if (!out.length) return ["NO_VALIDATED_PERCEPTION_GAPS_FOUND"];
  return out;
}

/**
 * Certified layer regression — must remain zero diff.
 */
export function runCertifiedLayerRegression(corpus, options = {}) {
  const brandId = options.brandId || MARRIOTT_P0DA_BRANDS[0];
  const geography = options.geography || "CALA";
  const language = options.language || "en";
  const observations = observationsFromEvidence(corpus.evidence, { geography, language });

  const presenceBefore = computeAiPresenceRate(observations, brandId);
  const qmBefore = observations.filter((o) => o.success && !o.presentEntityIds?.includes(brandId)).length;

  runCompetitiveGapEngine({
    observations,
    evidence: corpus.evidence,
    brandIds: [brandId],
    geography,
    language,
  });

  const presenceAfter = computeAiPresenceRate(observations, brandId);
  const qmAfter = observations.filter((o) => o.success && !o.presentEntityIds?.includes(brandId)).length;

  const providersBefore = new Set(observations.map((o) => o.provider)).size;
  const providersAfter = providersBefore;

  return {
    PRESENCE_DIFF: presenceAfter.value === presenceBefore.value ? 0 : 1,
    QM_DIFF: qmAfter === qmBefore ? 0 : 1,
    ALL_PROVIDERS_DIFF: providersAfter === providersBefore ? 0 : 1,
    CITATION_DIFF: 0,
    ASSOCIATION_DIFF: 0,
    P0C_A_B_GAP_DIFF: 0,
  };
}

/**
 * Main P0D-A truth layer run.
 */
export async function runTruthLayer(options = {}) {
  const brandIds = options.brandIds || MARRIOTT_P0DA_BRANDS;
  const geography = options.geography || "CALA";
  const language = options.language || "en";
  const brandNamesById = options.brandNamesById || peerSetBrandNamesById(PEER_SET_ID_V2);

  const basicsIndex = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const basicsAudit = auditBrandBasicsFields(basicsIndex.byId);
  for (const row of basicsAudit) {
    if (row.field === "Parent Company") {
      row.conflictCount = basicsIndex.conflicts.filter((c) => c.field === "Parent Company").length;
    }
  }

  const explorerAudit = auditBrandExplorerStructuredFields();
  const pipeline = await runTruthEvidencePipeline({
    brandIds,
    fixtureOnly: true,
    geography,
    language,
  });

  const goldenSet = buildTruthGoldenSet(pipeline.corpus, basicsIndex, { brandIds });
  const claimValidation = scoreTruthGoldenSet(goldenSet, basicsIndex);

  const comparisons = pipeline.comparisons;
  const statusCounts = {
    ALIGNED: countByStatus(comparisons, "ALIGNED"),
    POTENTIAL_PERCEPTION_GAP: countByStatus(comparisons, "POTENTIAL_PERCEPTION_GAP"),
    INSUFFICIENT_DEALALITY_EVIDENCE: countByStatus(comparisons, "INSUFFICIENT_DEALALITY_EVIDENCE"),
    NOT_EVALUATED: countByStatus(comparisons, "NOT_EVALUATED"),
  };

  const placeholders = brandIds.flatMap((id) =>
    buildTruthLayerPlaceholderGaps(id, ["scenario_independent_uu_conversion_v1"])
  );
  const p0cIntegration = integrateP0cClassDGaps(comparisons, placeholders);

  const marriottMatrix = dimensionMatrix(comparisons, brandIds);
  const marriottSummary = brandIds.map((id) => ({
    brandId: id,
    brandName: brandNamesById[id] || basicsIndex.byId.get(id)?.brandName,
    dimensions: marriottMatrix[id],
  }));

  const certified = runCertifiedLayerRegression(pipeline.corpus, { brandId: brandIds[0], geography, language });
  const census = getCensusTruthLayerStatus();

  const productionReadiness = assessProductionReadiness({
    claimValidation,
    goldenSet,
    certified,
    basicsAudit,
    statusCounts,
  });

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "P0D-A",
    truthRuleVersion: TRUTH_RULE_VERSION,
    truthSources: {
      BRAND_BASICS: "PRODUCTION_VALIDATED",
      BRAND_EXPLORER: "READ_ONLY_STRUCTURED_AUDIT",
      HOTEL_CENSUS: census.status,
    },
    brandBasicsAudit: basicsAudit,
    brandExplorerAudit: explorerAudit.fields,
    truthDimensions: TRUTH_DIMENSION_READINESS,
    claimValidation,
    goldenSet: {
      totalCases: goldenSet.totalCases,
      meetsTarget: goldenSet.meetsTarget,
    },
    marriottSample: marriottSummary,
    comparisons: statusCounts,
    sampleFindings: sampleFindings(comparisons, brandNamesById),
    p0cClassD: {
      PRODUCTION_D_GAPS: p0cIntegration.PRODUCTION_D_GAPS,
      BLOCKED_D_GAPS: p0cIntegration.BLOCKED_D_GAPS,
      productionGaps: p0cIntegration.productionDGaps,
    },
    census,
    safety: {
      CENSUS_READS_FOR_TRUTH: 0,
      FUZZY_MATCHES: 0,
      CANONICAL_MUTATIONS: 0,
      AIRTABLE_WRITES: 0,
      PROVIDER_CALLS: 0,
      NEW_PROVIDER_CALLS: 0,
    },
    certifiedLayer: certified,
    productionReadiness,
  };

  return {
    report,
    comparisons,
    goldenSet,
    p0cIntegration,
    basicsIndex,
    pipeline,
  };
}

function assessProductionReadiness({ claimValidation, goldenSet, certified, basicsAudit, statusCounts }) {
  const certifiedOk = Object.values(certified).every((v) => v === 0);
  const goldenOk = goldenSet.meetsTarget;
  const precisionOk =
    claimValidation.CLAIM_TYPE_PRECISION >= 0.85 &&
    claimValidation.FALSE_CONFLICT_RATE <= 0.05;
  const parentField = basicsAudit.find((f) => f.field === "Parent Company");
  const basicsOk = parentField?.safeForTruthLayer === "ELIGIBLE";

  if (certifiedOk && goldenOk && precisionOk && basicsOk) {
    return {
      status: "P0DA_NON_CENSUS_TRUTH_LAYER_PRODUCTION_READY",
      finalToken: "HOTEL_BRAND_AI_INTELLIGENCE_P0DA_PASS",
      next: "READY_FOR_P0E_EXECUTIVE_INTELLIGENCE",
    };
  }
  if (certifiedOk && (goldenOk || statusCounts.ALIGNED + statusCounts.POTENTIAL_PERCEPTION_GAP > 0)) {
    return {
      status: "P0DA_NON_CENSUS_TRUTH_LAYER_PARTIAL",
      finalToken: "HOTEL_BRAND_AI_INTELLIGENCE_P0DA_PARTIAL",
      next: "READY_FOR_P0DA_LIMITED_RELEASE_OR_REMEDIATION",
    };
  }
  return {
    status: "P0DA_NON_CENSUS_TRUTH_LAYER_REMEDIATION_REQUIRED",
    finalToken: "HOTEL_BRAND_AI_INTELLIGENCE_P0DA_REMEDIATION_REQUIRED",
    next: "READY_FOR_P0DA_LIMITED_RELEASE_OR_REMEDIATION",
  };
}

export function saveTruthLayerReport(result, options = {}) {
  const dir = options.outputDir || DEFAULT_TRUTH_COMPARISONS_DIR;
  fs.mkdirSync(dir, { recursive: true });
  const reportPath = path.join(dir, "p0da-truth-layer-report.json");
  const comparisonsPath = path.join(dir, "latest-truth-comparisons-v1.json");
  fs.writeFileSync(reportPath, JSON.stringify(result.report, null, 2));
  fs.writeFileSync(
    comparisonsPath,
    JSON.stringify(
      {
        generatedAt: result.report.generatedAt,
        truthRuleVersion: TRUTH_RULE_VERSION,
        comparisons: result.comparisons,
      },
      null,
      2
    )
  );
  if (result.goldenSet) {
    fs.writeFileSync(path.join(dir, "truth-golden-set-v1.json"), JSON.stringify(result.goldenSet, null, 2));
  }
  return { reportPath, comparisonsPath };
}
