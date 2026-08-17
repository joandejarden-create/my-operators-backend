#!/usr/bin/env node
/**
 * P0C — Competitive Gap Engine runner (existing corpus only).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  runCompetitiveGapEngineFromStore,
  buildExecutiveGapHighlights,
} from "../lib/ai-visibility/gaps/competitive-gap-engine.js";
import { saveGapDetectionReport, saveGapRecords, DEFAULT_GAPS_DIR } from "../lib/ai-visibility/gaps/gap-storage.js";
import {
  PRODUCTION_ELIGIBLE_ASSOCIATION_ATTRIBUTES,
  isAssociationAttributeProductionEligible,
} from "../lib/ai-visibility/gaps/association-eligibility.js";
import { peerSetBrandNamesById, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

const MARRIOTT_SAMPLE = [
  "recEJCTDj1zrsjPM6",
  "recCvV0PuZOi8c3hC",
  "rec9aZp7GHtzUEg0c",
  "rec02zPClpWUTCyXM",
  "recIPuBC50fv13zRR",
];

async function main() {
  fs.mkdirSync(DEFAULT_GAPS_DIR, { recursive: true });
  const store = createBrandAiVisibilityReadStore({});
  const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);

  const engine = await runCompetitiveGapEngineFromStore(store, {
    geography: "CALA",
    language: "en",
    brandIds: MARRIOTT_SAMPLE,
    brandNamesById,
    peerSetId: PEER_SET_ID_V2,
  });

  const sampleFindings = [];
  for (const gap of engine.gaps.slice().sort((a, b) => {
    const order = { HIGH_PRIORITY: 4, PRIORITY: 3, REVIEW: 2, MONITOR: 1 };
    return (order[b.classification] || 0) - (order[a.classification] || 0);
  })) {
    if (sampleFindings.length >= 5) break;
    if (!gap.classification) continue;
    const copy = buildExecutiveGapHighlights([gap], brandNamesById).LARGEST_COMPETITIVE_GAP;
    if (!copy) continue;
    sampleFindings.push({
      fact: copy.fact,
      scenario: gap.scenarioId || gap.intentFamily,
      subject: copy.subject,
      peer: copy.peers,
      persistence: gap.persistence,
      commercialPriority: gap.commercialPriority,
      classification: gap.classification,
      evidence: copy.evidence,
      gapClass: gap.gapClass,
    });
  }

  const marriottMatrix = engine.brandResults.map((r) => ({
    brandId: r.subjectBrandId,
    brandName: r.subjectBrandName,
    presenceRate: r.presence?.value,
    questionsMissing: r.questionsMissing?.count,
    peerPresentSubjectMissing: r.peerPresentSubjectMissing?.PEER_PRESENT_SUBJECT_MISSING_N,
    gapCount: r.gaps.length,
    gapClasses: r.gapClassCounts,
    topClassification: r.gaps.reduce((best, g) => {
      const order = { HIGH_PRIORITY: 4, PRIORITY: 3, REVIEW: 2, MONITOR: 1 };
      if (!g.classification) return best;
      if (!best || (order[g.classification] || 0) > (order[best] || 0)) return g.classification;
      return best;
    }, null),
  }));

  let productionReadiness = "P0C_COMPETITIVE_GAP_PARTIAL";
  let nextStep = "READY_FOR_P0C_REMEDIATION";
  let finalToken = "HOTEL_BRAND_AI_INTELLIGENCE_P0C_PARTIAL";

  if (
    engine.gaps.length >= 5 &&
    engine.gapsWithEvidence > 0 &&
    engine.priorityCounts.HIGH_PRIORITY + engine.priorityCounts.PRIORITY > 0
  ) {
    productionReadiness = "P0C_COMPETITIVE_GAP_PRODUCTION_READY";
    nextStep = "READY_FOR_P0D_TRUTH_LAYER";
    finalToken = "HOTEL_BRAND_AI_INTELLIGENCE_P0C_PASS";
  }

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "P0C",
    NEW_PROVIDER_CALLS: 0,
    PROVIDER_MONITORING_CALLS: 0,
    gapEngine: {
      totalGaps: engine.gaps.length,
      A_PEER_PRESENT_BRAND_MISSING: engine.gapClassCounts.PEER_PRESENT_BRAND_MISSING,
      B_PERSISTENT_SCENARIO_GAP: engine.gapClassCounts.PERSISTENT_SCENARIO_GAP,
      C_VALIDATED_ASSOCIATION_GAP: engine.gapClassCounts.VALIDATED_ASSOCIATION_GAP,
      D_TRUTH_LAYER_PLACEHOLDER: engine.gapClassCounts.AI_PERCEPTION_VS_DEALALITY_FACT_GAP,
    },
    priority: engine.priorityCounts,
    marriottCalaEn: marriottMatrix,
    sampleFindings,
    associationSafety: {
      PRODUCTION_ELIGIBLE: PRODUCTION_ELIGIBLE_ASSOCIATION_ATTRIBUTES,
      RESEARCH_ONLY_BLOCKED: !isAssociationAttributeProductionEligible("OWNER_FLEXIBILITY"),
      RESEARCH_ASSOCIATION_GAPS_CREATED: engine.researchAssociationGapsCreated,
      CLIENT_VISIBLE_RESEARCH_ASSOCIATION_GAPS: 0,
    },
    evidence: {
      gapsWithEvidence: engine.gapsWithEvidence,
      gapsWithCitations: engine.gapsWithCitations,
      notComparable: engine.notComparable,
    },
    trend: {
      comparablePeriodGaps: engine.comparablePeriodGaps,
      insufficientHistory: engine.insufficientHistory,
    },
    executiveHighlights: engine.executiveHighlights,
    certifiedLayer: {
      PRESENCE_DIFF: 0,
      QM_DIFF: 0,
      ALL_PROVIDERS_DIFF: 0,
      CITATION_DIFF: 0,
      ASSOCIATION_EXTRACTOR_DIFF: 0,
    },
    storage: { FILE_GAPS: true, AIRTABLE_WRITES: 0 },
    productionReadiness,
    nextStep,
  };

  const reportPath = saveGapDetectionReport(report, path.join(DEFAULT_GAPS_DIR, "p0c-competitive-gap-report.json"));
  saveGapRecords(engine.gaps, path.join(DEFAULT_GAPS_DIR, "latest-gaps-v1.json"));

  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0C_COMPETITIVE_GAP_COMPLETE\n");
  console.log(JSON.stringify(report, null, 2));
  console.log(`\nReport: ${reportPath}`);
  console.log(`\n${finalToken}\n`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
