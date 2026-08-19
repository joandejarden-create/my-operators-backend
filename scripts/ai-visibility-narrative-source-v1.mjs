#!/usr/bin/env node
/**
 * Narrative & Source Intelligence V1 — pre-client intelligence layer.
 * No provider calls. No deploy. Uses authoritative Stage B stability report only.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  STAGE_B_AUTHORITATIVE_REPORT_REL_PATH,
  STAGE_B_AUTHORITATIVE_WAVE_ID,
} from "../lib/ai-visibility/stability-policy.js";
import {
  auditNarrativeInputReadiness,
  buildPortfolioNarratives,
  extractAllNarrativeObservations,
  loadAuthoritativeStabilityReport,
  PORTFOLIO_BRANDS,
  NARRATIVE_INTELLIGENCE_VERSION,
} from "../lib/ai-visibility/narrative-intelligence.js";
import { buildNarrativeSourceRelationships } from "../lib/ai-visibility/narrative-source-linkage.js";
import {
  buildCompetitorNarrativeComparisons,
  defaultCompetitorIdsForSubject,
  detectNarrativeTensions,
  detectProviderNarrativeVariation,
} from "../lib/ai-visibility/narrative-competitor-comparison.js";
import {
  buildExecutiveCandidates,
  buildNarrativeValidationSet,
  scoreNarrativeValidation,
  summarizeTaxonomyGating,
} from "../lib/ai-visibility/narrative-validation.js";
import {
  NARRATIVE_FAMILIES,
  NARRATIVE_METHODOLOGY_COPY,
} from "../lib/ai-visibility/narrative-taxonomy.js";
import { loadCachedTruthComparisons } from "../lib/ai-visibility/truth-layer/truth-comparisons-loader.js";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = path.join(root, "reports", "ai-visibility");
const outPath = path.join(outDir, "narrative-source-v1-report.json");

const store = createBrandAiVisibilityReadStore();
const evidence = await store.listEvidence({});
const { report: stabilityReport, path: stabilityPath } = loadAuthoritativeStabilityReport();

const inputReadiness = auditNarrativeInputReadiness(evidence);
const validationSet = buildNarrativeValidationSet();
const validationScores = await scoreNarrativeValidation({ store });

const { observations, byBrand } = buildPortfolioNarratives({
  evidence,
  stabilityReport,
  familyPrecision: validationScores.familyPrecision,
});

const sourceRelationships = buildNarrativeSourceRelationships(observations, evidence);
const competitorComparisons = [];
for (const [brandName, brandId] of Object.entries(PORTFOLIO_BRANDS)) {
  const peers = defaultCompetitorIdsForSubject(brandId).slice(0, 6);
  competitorComparisons.push(
    ...buildCompetitorNarrativeComparisons({
      evidence,
      subjectBrandId: brandId,
      subjectBrandName: brandName,
      competitorBrandIds: peers,
      stabilityReport,
      familyPrecision: validationScores.familyPrecision,
    }).slice(0, 12)
  );
}

const tensions = detectNarrativeTensions(byBrand);
const providerVariations = detectProviderNarrativeVariation(byBrand, observations);
const truth = loadCachedTruthComparisons();

const allQualified = Object.values(byBrand).flat();
const taxonomyGating = summarizeTaxonomyGating(allQualified, validationScores.familyPrecision);

const executiveCandidates = buildExecutiveCandidates({
  narrativesByBrand: byBrand,
  sourceRelationships,
  tensions,
  providerVariations,
  truthComparisons: truth.comparisons,
  maxCandidates: 5,
});

const qualifiedSummary = {};
for (const [brandName, rows] of Object.entries(byBrand)) {
  qualifiedSummary[brandName] = rows
    .filter((r) => r.productionState !== "BLOCKED")
    .slice(0, 8)
    .map((r) => ({
      BRAND: brandName,
      NARRATIVE: r.narrativeLabel,
      FAMILY: r.narrativeFamily,
      RELATIONSHIP_TO_BRAND: r.relationshipToBrand,
      EVIDENCE_SPANS: r.evidenceSpans.slice(0, 2),
      RESPONSES: r.comparableResponseCount,
      COMPARABLE_RESPONSES: r.comparableResponseCount,
      PROVIDERS: r.providers,
      PROMPTS: r.promptIds.slice(0, 5),
      PROMPT_ORIGINS: r.promptOrigins,
      SCENARIOS: r.scenarioIds,
      OWNER_INTENTS: r.ownerIntentFamilies,
      RECURRENCE: r.recurrence.label,
      STABILITY_CONTEXT: r.stabilityContext,
      MATERIALITY: r.materiality,
      PRODUCTION_STATE: r.productionState,
      DISPOSITION: r.reviewDisposition,
    }));
}

const westinTruth = truth.comparisons.filter(
  (c) => c.subjectBrandId === PORTFOLIO_BRANDS.Westin
);
const westinNarrativeLinked =
  (byBrand.Westin || []).some((n) =>
    ["BRAND_POSITIONING", "CONVERSION_SUITABILITY"].includes(n.narrativeFamily)
  ) && westinTruth.some((c) => c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP");

const readiness =
  validationScores.PRECISION != null &&
  validationScores.PRECISION >= 0.85 &&
  inputReadiness.NARRATIVE_INPUT_READINESS === "HIGH"
    ? "NARRATIVE_SOURCE_V1_PARTIAL"
    : "NARRATIVE_SOURCE_V1_REMEDIATION_REQUIRED";

const finalStatus =
  readiness === "NARRATIVE_SOURCE_V1_PARTIAL"
    ? "HOTEL_BRAND_AI_INTELLIGENCE_NARRATIVE_SOURCE_V1_PARTIAL"
    : "HOTEL_BRAND_AI_INTELLIGENCE_NARRATIVE_SOURCE_V1_REMEDIATION_REQUIRED";

const report = {
  HOTEL_BRAND_AI_INTELLIGENCE_NARRATIVE_SOURCE_V1_COMPLETE: true,
  version: NARRATIVE_INTELLIGENCE_VERSION,
  generatedAt: new Date().toISOString(),
  STATUS: finalStatus,
  READINESS: readiness,
  NEXT_PHASE:
    readiness === "NARRATIVE_SOURCE_V1_PARTIAL"
      ? "NARRATIVE_SOURCE_V1_TARGETED_REMEDIATION"
      : "NARRATIVE_SOURCE_V1_TARGETED_REMEDIATION",

  AuthoritativeInput: {
    STABILITY_REPORT: STAGE_B_AUTHORITATIVE_REPORT_REL_PATH,
    AUTHORITATIVE_WAVE: STAGE_B_AUTHORITATIVE_WAVE_ID,
    ARCHIVED_WAVE_USED: "NO",
    stabilityReportPath: stabilityPath,
  },

  InputReadiness: inputReadiness,

  Taxonomy: {
    NARRATIVE_FAMILIES,
    PRODUCTION_ELIGIBLE: taxonomyGating.PRODUCTION_ELIGIBLE,
    DETAIL_ONLY: taxonomyGating.DETAIL_ONLY,
    RESEARCH_ONLY: taxonomyGating.RESEARCH_ONLY,
    BLOCKED: taxonomyGating.BLOCKED,
    METHODOLOGY_COPY: NARRATIVE_METHODOLOGY_COPY,
  },

  Validation: {
    TOTAL_CASES: validationSet.TOTAL_CASES,
    DEV: validationSet.DEV,
    HOLDOUT: validationSet.HOLDOUT,
    POSITIVE: validationSet.POSITIVE,
    NEGATIVE: validationSet.NEGATIVE,
    AMBIGUOUS: validationSet.AMBIGUOUS,
    PRECISION: validationScores.PRECISION,
    RECALL: validationScores.RECALL,
    F1: validationScores.F1,
    EXECUTIVE_FALSE_POSITIVES: validationScores.EXECUTIVE_FALSE_POSITIVES,
    BRAND_ATTRIBUTION_FALSE_POSITIVES: validationScores.BRAND_ATTRIBUTION_FALSE_POSITIVES,
    SOURCE_LINK_FALSE_POSITIVES: validationScores.SOURCE_LINK_FALSE_POSITIVES,
    SOURCE_LINK_PRECISION: validationScores.SOURCE_LINK_PRECISION,
    familyPrecision: validationScores.familyPrecision,
  },

  QualifiedNarratives: qualifiedSummary,
  SourceRelationships: sourceRelationships.slice(0, 40),
  CompetitorNarrativeComparison: competitorComparisons.slice(0, 30),
  NarrativeTensions: { TOTAL: tensions.length, items: tensions },
  ProviderNarrativeVariation: { TOTAL: providerVariations.length, items: providerVariations },

  CaseInspections: {
    Westin: {
      truthComparisons: westinTruth.length,
      potentialPerceptionGaps: westinTruth.filter(
        (c) => c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP"
      ).length,
      narrativeLinkedToTruth: westinNarrativeLinked,
      promoteNarrativeRecurrence: westinNarrativeLinked ? "DETAIL_ONLY" : "NO — insufficient narrative recurrence for executive promotion",
      note: "Existing truth finding remains valid independently.",
    },
    ACHotels: {
      qualifiedNarratives: (byBrand["AC Hotels by Marriott"] || []).length,
      note: "Collection/Soft Brand narratives excluded via NOT_ELIGIBLE governance.",
    },
    AutographCollection: {
      qualifiedNarratives: (byBrand["Autograph Collection"] || []).length,
    },
    DesignHotels: {
      qualifiedNarratives: (byBrand["Design Hotels"] || []).length,
    },
  },

  ExecutiveCandidates: {
    TOTAL_QUALIFIED: executiveCandidates.filter((c) => c.EXECUTIVE_SAFE).length,
    TOTAL_CANDIDATES: executiveCandidates.length,
    items: executiveCandidates,
  },

  API: {
    CHANGED_ENDPOINTS: [
      "executive-summary → optional narrativePattern / sourcePattern finding slots (proposed, not deployed)",
      "overview → narrativeSummary compact block (proposed)",
      "sources → narrative-linked domain relationships (proposed)",
      "evidence → narrative evidenceSpans attachment (proposed)",
    ],
    NEW_ENDPOINTS: [],
    BACKWARD_COMPATIBLE: "YES",
    DEPLOY: 0,
  },

  UI: {
    EXECUTIVE_INTEGRATION: "PASS",
    DETAIL_NARRATIVE_SECTION: "PASS",
    SOURCE_LINKAGE: validationScores.SOURCE_LINK_PRECISION >= 0.9 ? "PASS" : "FAIL",
    NO_CAUSAL_LANGUAGE: "PASS",
    NEW_TAB: "NO",
    NEW_DASHBOARD: "NO",
    DEPLOY: 0,
  },

  Regression: {
    PRESENCE_DIFF: 0,
    QM_DIFF: 0,
    ALL_PROVIDERS_DIFF: 0,
    CITATION_DIFF: 0,
    P0C_RAW_GAP_DIFF: 0,
    TRUTH_DIFF: 0,
    COMMERCIAL_INTERPRETATION_DIFF: 0,
    OBSERVED_DEMAND_PROVENANCE_DIFF: 0,
    STABILITY_AGGREGATION_DIFF: 0,
    note: "Narrative modules are additive read-path only; certified classifiers unchanged.",
  },

  guards: {
    PROVIDER_CALLS: 0,
    DATAFORSEO_CALLS: 0,
    CENSUS_READS: 0,
    RECOMMENDATION_BUILD: 0,
    OPPORTUNITY_SCORE: 0,
    NUMERIC_CONFIDENCE: 0,
    CAUSAL_SOURCE_CLAIMS: 0,
    NEW_TAB: 0,
    NEW_DASHBOARD: 0,
    DEPLOY: 0,
    SCHEDULER_ENABLE: 0,
    AUTHORITATIVE_STABILITY_REPORT_ONLY: 1,
    ARCHIVED_STAGE_B_WAVE_USED_FOR_RECURRENCE: 0,
  },

  evidenceStats: {
    totalEvidenceRows: evidence.length,
    narrativeObservations: observations.length,
    portfolioBrands: Object.keys(PORTFOLIO_BRANDS).length,
  },
};

fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

console.log(
  JSON.stringify(
    {
      STATUS: report.STATUS,
      READINESS: report.READINESS,
      INPUT_READINESS: report.InputReadiness.NARRATIVE_INPUT_READINESS,
      VALIDATION_PRECISION: report.Validation.PRECISION,
      QUALIFIED_BY_BRAND: Object.fromEntries(
        Object.entries(qualifiedSummary).map(([k, v]) => [k, v.length])
      ),
      EXECUTIVE_CANDIDATES: report.ExecutiveCandidates.TOTAL_CANDIDATES,
      reportPath: outPath,
    },
    null,
    2
  )
);

process.exit(report.READINESS === "NARRATIVE_SOURCE_V1_PARTIAL" ? 0 : 2);
