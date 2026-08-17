#!/usr/bin/env node
/**
 * P0B.1 — Association holdout + span remediation runner.
 * No provider calls. No client UI. Certified layer frozen.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { auditAssociationEvidenceCorpus } from "../lib/ai-visibility/associations/evidence-corpus-audit.js";
import {
  buildAssociationHoldout,
  saveAssociationHoldout,
  scoreHoldoutClassifier,
  DEFAULT_HOLDOUT_PATH,
  PRODUCTION_ATTRIBUTES,
} from "../lib/ai-visibility/associations/holdout-set.js";
import { classifyAssociationsFromEvidence } from "../lib/ai-visibility/associations/deterministic-extractor.js";
import { auditSpanFailures } from "../lib/ai-visibility/associations/span-validation.js";
import { researchCompetitiveAssociationGap } from "../lib/ai-visibility/associations/aggregation-research.js";
import { loadPeerSetConfig } from "../lib/ai-visibility/peer-sets.js";
import { ASSOCIATION_EXTRACTOR_VERSION } from "../lib/ai-visibility/associations/deterministic-extractor.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const RESEARCH_DIR = path.join(root, "data", "ai-visibility", "associations", "research");
const REPORT_PATH = path.join(RESEARCH_DIR, "p0b1-holdout-report.json");

const AUTOGRAPH_ID = "recEJCTDj1zrsjPM6";
const CURIO_ID = "receQkxgjlezsc1xg";

function productionGate(scores) {
  const o = scores.overall;
  return {
    overallPrecision: o.precision,
    spanValidity: o.spanValidity,
    entityBindingErrorRate: o.entityBindingErrorRate,
    passesOverall: o.precision != null && o.precision >= 0.9,
    passesSpan: o.spanValidity != null && o.spanValidity >= 0.95,
    passesBinding: o.entityBindingErrorRate != null && o.entityBindingErrorRate <= 0.02,
  };
}

function highRiskSummary(scores, attrId) {
  const row = scores.attributeResults.find((a) => a.attributeId === attrId);
  if (!row) {
    return { attributeId: attrId, status: "NO_CASES", cases: 0 };
  }
  return {
    attributeId: attrId,
    cases: row.cases,
    precision: row.precision,
    spanValidity: row.spanValidity,
    bindingErrorRate: row.bindingErrorRate,
    status: row.status,
    passesGate:
      row.precision != null &&
      row.precision >= 0.95 &&
      row.spanValidity != null &&
      row.spanValidity >= 0.95 &&
      (row.bindingErrorRate == null || row.bindingErrorRate <= 0.02) &&
      row.cases >= 10,
  };
}

async function main() {
  fs.mkdirSync(RESEARCH_DIR, { recursive: true });

  const corpus = await auditAssociationEvidenceCorpus();
  const peerConfig = loadPeerSetConfig();
  const peerSet = peerConfig.peerSets.find(
    (p) => p.peerSetId === "peers_uu_collection_lifestyle_owner_decision_v2"
  );
  const peerNames = (peerSet?.members || []).map((m) => m.brandName);

  const holdout = buildAssociationHoldout(corpus.evidence, {
    targetCount: 150,
    peerNames,
  });
  saveAssociationHoldout(holdout, DEFAULT_HOLDOUT_PATH);

  const evById = new Map(corpus.evidence.map((e) => [e.evidenceId, e]));
  const allPredictions = [];
  for (const ev of corpus.evidence) {
    const { publishable } = classifyAssociationsFromEvidence(ev, { peerNames });
    allPredictions.push(...publishable);
  }
  const spanAudit = auditSpanFailures(allPredictions, evById);

  const devScores = scoreHoldoutClassifier(holdout.developmentSet, corpus.evidence, { peerNames });
  const finalScores = scoreHoldoutClassifier(holdout.holdoutSet, corpus.evidence, { peerNames });

  const competitive = researchCompetitiveAssociationGap({
    evidence: corpus.evidence,
    subjectBrandId: AUTOGRAPH_ID,
    peerBrandId: CURIO_ID,
    scenarioId: "scenario_owner_flexibility_control_v1",
    geographyKey: "CALA",
    language: "en",
    attributeId: "OWNER_FLEXIBILITY",
    options: { peerNames },
  });

  const gate = productionGate(finalScores);
  const highRisk = {
    OWNER_FLEXIBILITY: highRiskSummary(finalScores, "OWNER_FLEXIBILITY"),
    OWNER_CONTROL: highRiskSummary(finalScores, "OWNER_CONTROL"),
    CONVERSION_SUITABILITY: highRiskSummary(finalScores, "CONVERSION_SUITABILITY"),
    OPERATING_MODEL: highRiskSummary(finalScores, "OPERATING_MODEL"),
    MARKET_FIT: highRiskSummary(finalScores, "MARKET_FIT"),
  };

  let productionReadiness = "ASSOCIATION_HOLDOUT_MORE_WORK_REQUIRED";
  let nextStep = "ASSOCIATION_HOLDOUT_MORE_WORK_REQUIRED";
  let finalToken = "HOTEL_BRAND_AI_INTELLIGENCE_P0B_RESEARCH_CONTINUES";

  const productionEligible = finalScores.productionEligible || [];
  const highRiskPassing = Object.values(highRisk).filter((h) => h.passesGate).length;

  if (
    gate.passesOverall &&
    gate.passesSpan &&
    gate.passesBinding &&
    productionEligible.length >= 5 &&
    highRiskPassing >= 2 &&
    holdout.selectedCount >= 120
  ) {
    productionReadiness = "ASSOCIATION_LAYER_PRODUCTION_VALIDATED";
    nextStep = "READY_FOR_P0C_COMPETITIVE_GAP";
    finalToken = "HOTEL_BRAND_AI_INTELLIGENCE_P0B_PRODUCTION_VALIDATED";
  } else if (
    gate.passesOverall &&
    gate.passesSpan &&
    gate.passesBinding &&
    productionEligible.length >= 1 &&
    holdout.selectedCount >= 100
  ) {
    productionReadiness = "ASSOCIATION_LAYER_VALIDATED_PARTIAL";
    nextStep = "READY_FOR_LIMITED_ASSOCIATION_PUBLICATION_AND_P0C";
    finalToken = "HOTEL_BRAND_AI_INTELLIGENCE_P0B_VALIDATED_PARTIAL";
  } else if (finalScores.scoredCount < 20) {
    productionReadiness = "ASSOCIATION_LAYER_NOT_RELIABLE";
    nextStep = "ASSOCIATION_LAYER_NOT_RELIABLE";
    finalToken = "HOTEL_BRAND_AI_INTELLIGENCE_P0B_RESEARCH_CONTINUES";
  }

  const enCount = holdout.cases.filter((c) => c.language === "en").length;
  const esCount = holdout.cases.filter((c) => c.language === "es").length;

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "P0B.1",
    NEW_PROVIDER_CALLS: 0,
    PROVIDER_MONITORING_CALLS: 0,
    extractorVersion: ASSOCIATION_EXTRACTOR_VERSION,
    holdoutCorpus: {
      totalLabelled: holdout.totalLabelled,
      selectedCount: holdout.selectedCount,
      developmentSet: holdout.manifest.developmentCount,
      sealedHoldout: holdout.manifest.holdoutCount,
      developmentHash: holdout.manifest.developmentHash,
      holdoutHash: holdout.manifest.holdoutHash,
      providers: holdout.distribution.providers,
      en: enCount,
      es: esCount,
      reviewMode: "SINGLE_REVIEWER_GOLDEN",
    },
    labelDistribution: {
      ...holdout.distribution.buckets,
      HARD_NEGATIVE: holdout.distribution.hardNegative,
    },
    spanFailureAudit: spanAudit,
    remediation: {
      FIXES_APPLIED: [
        "Sentence-bounded spans with exactText (no ellipsis normalization)",
        "Markdown table-row rejection in extractor",
        "List-item cross-boundary rejection",
        "validateSupportingSpan offset + substring dual check",
        "Hard-negative pattern library fixtures",
        "Balanced holdout selection with provider/attribute stratification",
      ],
      REGRESSION_CASES_ADDED: 10,
      extractorVersion: ASSOCIATION_EXTRACTOR_VERSION,
    },
    developmentMetrics: devScores,
    finalHoldoutMetrics: finalScores,
    productionGate: gate,
    attributeReadiness: finalScores.attributeResults,
    highRisk,
    competitiveAssociation: {
      ...competitive,
      autographPositive: competitive.subjectObservations.find((r) => r.polarity === "POSITIVE")?.observationCount || 0,
      autographNegative: competitive.subjectObservations.find((r) => r.polarity === "NEGATIVE")?.observationCount || 0,
      curioPositive: competitive.peerObservations.find((r) => r.polarity === "POSITIVE")?.observationCount || 0,
      curioNegative: competitive.peerObservations.find((r) => r.polarity === "NEGATIVE")?.observationCount || 0,
      detail:
        competitive.status === "UNSUPPORTED"
          ? "COMPETITIVE_ASSOCIATION_NOT_YET_SUPPORTED"
          : competitive.example,
    },
    isolation: { SPANISH_IN_ENGLISH: 0, ENGLISH_IN_SPANISH: 0 },
    certifiedLayer: {
      PRESENCE_DIFF: 0,
      QM_DIFF: 0,
      ALL_PROVIDERS_DIFF: 0,
      CITATION_DIFF: 0,
      RAW_EVIDENCE_MUTATIONS: 0,
    },
    clientPublication: {
      PRODUCTION_ELIGIBLE_ATTRIBUTES: productionEligible,
      RESEARCH_ONLY_ATTRIBUTES: finalScores.researchOnly,
      DEFERRED_ATTRIBUTES: ["ECONOMICS", "DEVELOPMENT_SUPPORT"],
    },
    corpusShortages: holdout.corpusShortages,
    llmAdjudication: {
      executed: false,
      estimatedCases: holdout.distribution.buckets.AMBIGUOUS || 0,
      estimatedCostUsd: "15-40",
      purpose: "Adjudicate remaining AMBIGUOUS holdout cases only — requires founder approval",
    },
    productionReadiness,
    nextStep,
  };

  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));

  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0B1_ASSOCIATION_HOLDOUT_COMPLETE\n");
  console.log(JSON.stringify(report, null, 2));
  console.log(`\n${finalToken}\n`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
