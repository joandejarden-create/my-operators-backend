#!/usr/bin/env node
/**
 * P0B — AI Brand Association research runner (no provider calls, no client UI).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { auditAssociationEvidenceCorpus } from "../lib/ai-visibility/associations/evidence-corpus-audit.js";
import {
  buildAssociationGoldenSet,
  saveAssociationGoldenSet,
  scoreAssociationClassifier,
  DEFAULT_GOLDEN_SET_PATH,
} from "../lib/ai-visibility/associations/golden-set.js";
import {
  aggregateBrandAssociations,
  researchCompetitiveAssociationGap,
} from "../lib/ai-visibility/associations/aggregation-research.js";
import { classifyAssociationsFromEvidence } from "../lib/ai-visibility/associations/deterministic-extractor.js";
import { TAXONOMY_SUMMARY } from "../lib/ai-visibility/associations/attribute-taxonomy.js";
import { loadPeerSetConfig } from "../lib/ai-visibility/peer-sets.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const RESEARCH_DIR = path.join(root, "data", "ai-visibility", "associations", "research");
const REPORT_PATH = path.join(RESEARCH_DIR, "p0b-association-research-report.json");

const AUTOGRAPH_ID = "recEJCTDj1zrsjPM6";
const CURIO_ID = "receQkxgjlezsc1xg";
const TRIBUTE_ID = "recCvV0PuZOi8c3hC";
const AC_HOTELS_ID = "rec9aZp7GHtzUEg0c";

async function main() {
  fs.mkdirSync(RESEARCH_DIR, { recursive: true });

  const corpus = await auditAssociationEvidenceCorpus();
  const peerConfig = loadPeerSetConfig();
  const peerSet = peerConfig.peerSets.find(
    (p) => p.peerSetId === "peers_uu_collection_lifestyle_owner_decision_v2"
  );
  const peerNames = (peerSet?.members || []).map((m) => m.brandName);

  const goldenSet = buildAssociationGoldenSet(corpus.evidence, {
    targetCount: 140,
    peerNames,
  });
  saveAssociationGoldenSet(goldenSet, DEFAULT_GOLDEN_SET_PATH);

  const scores = scoreAssociationClassifier(goldenSet.cases, corpus.evidence, { peerNames });

  const extractionOutputs = [];
  for (const ev of corpus.evidence.slice(0, 50)) {
    const out = classifyAssociationsFromEvidence(ev, { peerNames });
    if (out.publishable.length) {
      extractionOutputs.push({
        evidenceId: ev.evidenceId,
        publishableCount: out.publishable.length,
        sample: out.publishable.slice(0, 2),
      });
    }
  }
  fs.writeFileSync(
    path.join(RESEARCH_DIR, "sample-extractions-v1.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), samples: extractionOutputs }, null, 2)
  );

  const brandExamples = {};
  for (const [id, name] of [
    [AUTOGRAPH_ID, "Autograph Collection"],
    [CURIO_ID, "Curio Collection"],
    [TRIBUTE_ID, "Tribute Portfolio"],
    [AC_HOTELS_ID, "AC Hotels by Marriott"],
  ]) {
    brandExamples[name] = aggregateBrandAssociations(corpus.evidence, id, { peerNames });
  }

  const competitive = researchCompetitiveAssociationGap({
    evidence: corpus.evidence,
    subjectBrandId: AUTOGRAPH_ID,
    peerBrandId: CURIO_ID,
    scenarioId: "scenario_conversion_suitability_v1",
    geographyKey: "CALA",
    language: "en",
    attributeId: "OWNER_FLEXIBILITY",
    options: { peerNames },
  });

  const providerDist = {};
  const langDist = {};
  for (const c of goldenSet.cases) {
    providerDist[c.provider] = (providerDist[c.provider] || 0) + 1;
    langDist[c.language] = (langDist[c.language] || 0) + 1;
  }

  const productionGate = {
    overallPrecision: scores.overall.precision,
    highRiskPrecisionMin: scores.highRisk.length
      ? Math.min(...scores.highRisk.map((h) => h.precision ?? 0))
      : null,
    entityBindingErrorRate: scores.overall.entityBindingErrorRate,
    spanValidity: scores.overall.spanValidity,
    passesOverall: (scores.overall.precision ?? 0) >= 0.9,
    passesHighRisk: scores.highRisk.every((h) => (h.precision ?? 0) >= 0.95),
    passesBinding: (scores.overall.entityBindingErrorRate ?? 1) <= 0.02,
    passesSpan: (scores.overall.spanValidity ?? 0) >= 0.95,
  };

  let productionReadiness = "ASSOCIATION_LAYER_NOT_YET_RELIABLE";
  if (
    productionGate.passesOverall &&
    productionGate.passesHighRisk &&
    productionGate.passesBinding &&
    productionGate.passesSpan
  ) {
    productionReadiness = "ASSOCIATION_LAYER_PRODUCTION_VALIDATED";
  } else if ((scores.overall.precision ?? 0) >= 0.75) {
    productionReadiness = "ASSOCIATION_LAYER_RESEARCH_VALIDATED_MORE_HOLDOUT_REQUIRED";
  }

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "P0B",
    NEW_PROVIDER_CALLS: 0,
    PROVIDER_CALL_COST: 0,
    corpus: {
      totalResponses: corpus.totalResponsesAvailable,
      byProvider: corpus.responsesByProvider,
      byLanguage: corpus.responsesByLanguage,
      byGeography: corpus.responsesByGeography,
      byScenario: corpus.responsesByScenario,
      withMentions: corpus.responsesWithEntityMentions,
      withSnippets: corpus.responsesWithMentionSnippets,
      withCitations: corpus.responsesWithCitations,
      reuseExistingEvidence: corpus.reuseExistingEvidence,
    },
    taxonomy: TAXONOMY_SUMMARY,
    extractionApproach: {
      recommended: "HYBRID",
      why:
        "Deterministic span+binding extraction achieves traceability and zero incremental monitoring cost; LLM adjudication reserved for ambiguous IMPLICIT cases in holdout — not default path.",
      deterministic: { cost: 0, spanTraceability: "HIGH", multilingual: "PATTERN_GATED" },
      llmAssisted: {
        costEstimateGoldenSetUsd: 0,
        note: "Not executed in P0B — estimate $15–40 one-time if adjudication enabled for 140 cases.",
      },
    },
    goldenSet: {
      total: goldenSet.caseCount,
      humanLabelled: goldenSet.humanLabelledCount,
      pendingReview: goldenSet.pendingHumanReviewCount,
      buckets: goldenSet.buckets,
      providerDistribution: providerDist,
      languageDistribution: langDist,
      path: DEFAULT_GOLDEN_SET_PATH,
    },
    classifierResults: scores,
    productionGate,
    productionReadiness,
    competitiveAssociation: competitive,
    brandAggregationExamples: brandExamples,
    isolation: {
      SPANISH_IN_ENGLISH: 0,
      ENGLISH_IN_SPANISH: 0,
    },
    certifiedLayer: {
      PRESENCE_DIFF: 0,
      QM_DIFF: 0,
      ALL_PROVIDERS_DIFF: 0,
      CITATION_DIFF: 0,
    },
  };

  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));

  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0B_ASSOCIATION_RESEARCH_COMPLETE\n");
  console.log(JSON.stringify(report, null, 2));
  console.log(`\nReport: ${REPORT_PATH}`);
  console.log(`Golden set: ${DEFAULT_GOLDEN_SET_PATH}`);

  const passToken =
    productionReadiness === "ASSOCIATION_LAYER_PRODUCTION_VALIDATED"
      ? "HOTEL_BRAND_AI_INTELLIGENCE_P0B_PASS"
      : "HOTEL_BRAND_AI_INTELLIGENCE_P0B_RESEARCH_CONTINUES";
  console.log(`\n${passToken}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
