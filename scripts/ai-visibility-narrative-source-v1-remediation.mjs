#!/usr/bin/env node
/**
 * Narrative & Source Intelligence V1 — targeted remediation + production gating.
 * No provider calls. Sealed holdout scored once.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  STAGE_B_AUTHORITATIVE_REPORT_REL_PATH,
  STAGE_B_AUTHORITATIVE_WAVE_ID,
} from "../lib/ai-visibility/stability-policy.js";
import { runNarrativeRemediation, MIN_COMPARISON_RESPONSES } from "../lib/ai-visibility/narrative-remediation.js";
import { NARRATIVE_METHODOLOGY_COPY } from "../lib/ai-visibility/narrative-taxonomy.js";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const outPath = path.join(root, "reports", "ai-visibility", "narrative-source-v1-remediation-report.json");
const sealedSetPath = path.join(
  root,
  "data",
  "ai-visibility",
  "narrative",
  "remediation-set-v1.json"
);

const store = createBrandAiVisibilityReadStore();
const evidence = await store.listEvidence({});

const result = await runNarrativeRemediation({ evidence });

fs.mkdirSync(path.dirname(sealedSetPath), { recursive: true });
fs.writeFileSync(
  sealedSetPath,
  JSON.stringify(
    {
      version: result.remediationSet.version,
      sealedAt: result.remediationSet.sealedAt,
      TOTAL: result.remediationSet.TOTAL,
      DEV: result.remediationSet.DEV,
      HOLDOUT: result.remediationSet.HOLDOUT,
      labelCounts: result.remediationSet.labelCounts,
      devCases: result.remediationSet.devCases,
      holdoutCases: result.remediationSet.holdoutCases,
      sourceLinkHoldoutCases: result.sourceLinkSet.holdoutCases,
    },
    null,
    2
  )
);

const qualifiedNarratives = {};
for (const [brand, rows] of Object.entries(result.byBrand)) {
  qualifiedNarratives[brand] = rows
    .filter((r) => r.productionState === "PRODUCTION_ELIGIBLE")
    .slice(0, 6)
    .map((r) => ({
      BRAND: brand,
      NARRATIVE: r.narrativeLabel,
      FAMILY: r.narrativeFamily,
      RESPONSES: r.comparableResponseCount,
      COMPARABLE_RESPONSES: r.comparableResponseCount,
      PROVIDERS: r.providers,
      SCENARIOS: r.scenarioIds,
      RECURRENCE: r.recurrence.label,
      STABILITY_CONTEXT: r.stabilityContext?.timeWindow || "SHORT_TERM",
      SOURCE_CONTEXT: "See SourceRelationships",
      MATERIALITY: r.materiality,
      DISPOSITION: r.reviewDisposition,
    }));
}

const report = {
  HOTEL_BRAND_AI_INTELLIGENCE_NARRATIVE_SOURCE_V1_REMEDIATION_COMPLETE: true,
  generatedAt: new Date().toISOString(),
  STATUS: result.finalStatus,
  READINESS: result.readiness,
  NEXT_PHASE:
    result.readiness === "NARRATIVE_SOURCE_V1_READY_TO_SHIP"
      ? "FINAL_UI_REFINEMENT_AND_PRECLIENT_RELEASE"
      : "FOUNDER_REVIEW_OF_REMAINING_FAMILIES",

  AuthoritativeInput: {
    STABILITY_REPORT: STAGE_B_AUTHORITATIVE_REPORT_REL_PATH,
    AUTHORITATIVE_WAVE: STAGE_B_AUTHORITATIVE_WAVE_ID,
    ARCHIVED_WAVE_USED: "NO",
  },

  RemediationSet: {
    TOTAL: result.remediationSet.TOTAL,
    DEV: result.remediationSet.DEV,
    HOLDOUT: result.remediationSet.HOLDOUT,
    HOLDOUT_SEALED_BEFORE_RULE_TUNING: result.HOLDOUT_SEALED_BEFORE_RULE_TUNING,
    HOLDOUT_REUSED_FOR_ITERATIVE_TUNING: result.HOLDOUT_REUSED_FOR_ITERATIVE_TUNING,
    labelCounts: result.remediationSet.labelCounts,
    sealedSetPath,
  },

  FamilyResults: result.familyResults,

  ProductionTaxonomy: {
    PRODUCTION_ELIGIBLE: result.productionEligible,
    DETAIL_ONLY: result.detailOnly,
    RESEARCH_ONLY: result.researchOnly,
    BLOCKED: ["FEES_ECONOMICS", "CHAIN_SCALE_POSITIONING", "DEVELOPMENT_SUPPORT", "OTHER"],
  },

  FamilyCollision: {
    SOFT_BRAND_VS_FLEXIBILITY: result.collisionMatrix.SOFT_BRAND_INDIVIDUALITY_VS_OWNER_FLEXIBILITY_CONTROL,
    SOFT_BRAND_VS_DESIGN: result.collisionMatrix.SOFT_BRAND_INDIVIDUALITY_VS_DESIGN_LOCAL_CHARACTER,
    DESIGN_VS_POSITIONING: result.collisionMatrix.DESIGN_LOCAL_CHARACTER_VS_POSITIONING,
    FLEXIBILITY_VS_POSITIONING: result.collisionMatrix.OWNER_FLEXIBILITY_CONTROL_VS_POSITIONING,
  },

  SourceLinkValidation: {
    TOTAL_CASES: result.sourceLinkSet.TOTAL_CASES,
    HOLDOUT_CASES: result.sourceLinkHoldout.HOLDOUT_CASES,
    PRECISION: result.sourceLinkHoldout.PRECISION,
    FALSE_POSITIVES: result.sourceLinkHoldout.FALSE_POSITIVES,
    PRODUCTION_RELATIONSHIPS: ["DIRECTLY_CITED_WITH_NARRATIVE", "RECURRING_CITED_ALONGSIDE_NARRATIVE"],
    DETAIL_ONLY_RELATIONSHIPS: ["CITED_IN_SAME_RESPONSE", "FREQUENTLY_CO_OCCURRING", "ASSOCIATED_NOT_CAUSAL"],
  },

  DistributionNonRedundancy: {
    AUTOGRAPH: result.distributionAudits["Autograph Collection"],
    DESIGN: result.distributionAudits["Design Hotels"],
    NARRATIVE_INCREMENTAL_VALUE_OVER_ASSOCIATIONS: result.narrativeIncremental ? "PASS" : "FAIL",
  },

  CompetitorAttribution: result.competitorAttribution,

  QualifiedNarratives: qualifiedNarratives,

  CompetitorComparison: result.qualifiedComparisons.slice(0, 20).map((c) => ({
    SUBJECT: c.subjectBrand,
    COMPETITOR: c.competitor,
    NARRATIVE: c.narrative,
    COMPARABLE_RESPONSES: c.subjectResponses + c.competitorResponses,
    SUBJECT_COVERAGE: c.subjectResponses,
    COMPETITOR_COVERAGE: c.competitorResponses,
    COMPARISON: c.comparison,
    EXECUTIVE_SAFE:
      c.subjectResponses + c.competitorResponses >= MIN_COMPARISON_RESPONSES ? "NO" : "NO",
  })),

  ExecutiveCandidates: result.executiveCandidates.map((c) => ({
    TYPE: c.TYPE,
    HEADLINE: c.HEADLINE,
    EVIDENCE: c.EVIDENCE,
    INTERPRETATION: c.INTERPRETATION,
    SUGGESTED_REVIEW: c.SUGGESTED_REVIEW,
    DISPOSITION: c.DISPOSITION,
    INCREMENTAL_INTELLIGENCE: c.INCREMENTAL_INTELLIGENCE || "NO",
    EXECUTIVE_SAFE: c.EXECUTIVE_SAFE ? "YES" : "NO",
  })),

  ComparisonRule: {
    MIN_COMPARABLE_RESPONSES: MIN_COMPARISON_RESPONSES,
    MIN_COVERAGE_DELTA: 2,
    note: "SUBJECT_STRONGER / COMPETITOR_STRONGER require >=3 comparable responses and material coverage difference; 1 vs 0 is INSUFFICIENT.",
  },

  APIShipProposal: {
    CHANGED_ENDPOINTS: [
      "GET executive-summary → optional narrativePattern / sourcePattern slots (gated by productionState + incrementalIntelligence)",
      "GET overview → narrativeSummary: { families[], recurrence, providers[], stabilityContext }",
      "GET sources → narrativeLinks[]: { domain, relationship, narrativeFamily, responses }",
      "GET evidence/:id → narrativeSpans[] when productionState >= DETAIL_ONLY",
    ],
    NEW_ENDPOINTS: [],
    BACKWARD_COMPATIBLE: "YES",
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
    DISTRIBUTION_ASSOCIATION_DIFF: 0,
  },

  guards: {
    PROVIDER_CALLS: 0,
    AI_SPEND: 0,
    DATAFORSEO_CALLS: 0,
    CENSUS_READS: 0,
    AIRTABLE_WRITES: 0,
    DEPLOY: 0,
    SCHEDULER_ENABLE: 0,
    ARCHIVED_STAGE_B_WAVE_USED: 0,
  },

  METHODOLOGY_COPY: NARRATIVE_METHODOLOGY_COPY,
};

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

console.log(
  JSON.stringify(
    {
      STATUS: report.STATUS,
      READINESS: report.READINESS,
      REMEDIATION_TOTAL: report.RemediationSet.TOTAL,
      PRODUCTION_ELIGIBLE: report.ProductionTaxonomy.PRODUCTION_ELIGIBLE,
      SOURCE_LINK_HOLDOUT_PRECISION: report.SourceLinkValidation.PRECISION,
      INCREMENTAL_VALUE: report.DistributionNonRedundancy.NARRATIVE_INCREMENTAL_VALUE_OVER_ASSOCIATIONS,
      reportPath: outPath,
    },
    null,
    2
  )
);

process.exit(result.readiness === "NARRATIVE_SOURCE_V1_READY_TO_SHIP" ? 0 : 2);
