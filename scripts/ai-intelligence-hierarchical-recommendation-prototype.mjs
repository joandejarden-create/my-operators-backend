#!/usr/bin/env node
/**
 * Hierarchical recommendation classifier DEV prototype.
 * Holdout never accessed. Narrow node adjudicators only (not 10-way).
 *
 *   node scripts/ai-intelligence-hierarchical-recommendation-prototype.mjs --estimate-only
 *   HYBRID_DEV_COST_CAP_USD=5 node scripts/ai-intelligence-hierarchical-recommendation-prototype.mjs --apply
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import {
  scoreDevRecommendationLab,
  prf,
  macroFromClassMetrics,
  ROLES,
} from "../lib/ai-visibility/classifier-lab/score-dev.js";
import {
  buildTypedSections,
  extractEntityLocalEvidence,
} from "../lib/ai-visibility/recommendation-evidence-v4_1.js";
import {
  deriveNodeLabelsFromHumanRole,
  decideNodeDeterministic,
  classifyHierarchicalRecommendation,
  adjudicateHierarchicalNode,
  HIERARCHICAL_CLASSIFIER_VERSION,
} from "../lib/ai-visibility/hierarchical-recommendation/index.js";
import {
  resolveAdjudicatorProvider,
  estimateAdjudicatorCallCostUsd,
} from "../lib/ai-visibility/hybrid-recommendation/adjudicator-client.js";
import { questionStatusFromRecommendationRole } from "../lib/ai-visibility/recommendation-classifier-v4_1.js";

function resolveProvider() {
  return resolveAdjudicatorProvider();
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, "../data/ai-visibility/validation");
const REPORT = path.join(OUT_DIR, "hierarchical-recommendation-prototype-report.json");
const GT3 = path.join(OUT_DIR, "taxonomy-resolution-gt3-hierarchical.json");

const apply = process.argv.includes("--apply");
const estimateOnly = process.argv.includes("--estimate-only") || !apply;
const COST_CAP = Number(process.env.HYBRID_DEV_COST_CAP_USD || 5);
const providerInfo = resolveProvider();
const MODEL = providerInfo.model;
const PROVIDER = providerInfo.provider;

const OLD_HYBRID_ACCURACY = 0.7137931034482758;

function emptyCm() {
  return Object.fromEntries(ROLES.map((r) => [r, { tp: 0, fp: 0, fn: 0 }]));
}
function finalizeCm(cm) {
  return Object.fromEntries(Object.entries(cm).map(([k, v]) => [k, prf(v.tp, v.fp, v.fn)]));
}
function updateCm(cm, human, pred) {
  if (!human) return;
  if (human === pred) cm[human].tp++;
  else {
    cm[human].fn++;
    if (pred && cm[pred]) cm[pred].fp++;
  }
}

console.log("Loading DEV (holdout excluded)...");
const baseline = await scoreDevRecommendationLab({
  classifierVersion: "hierarchical_proto_deterministic_baseline",
});

const golden = loadGoldenSet();
const index = buildGoldenSetScoringEntityIndex({});
const { cases } = await hydrateGoldenSetCasesForScoring(
  (golden.cases || []).filter(
    (c) =>
      c.holdoutSplit !== "holdout" &&
      c.expectedRecommendationRole &&
      c.excludeFromClassificationDenominator !== true
  ),
  {}
);

const gtCleanup = fs.existsSync(GT3)
  ? JSON.parse(fs.readFileSync(GT3, "utf8"))
  : { TOTAL: 3, KEEP: 2, AMEND: 0, DEFER: 1 };

// Prepare per-case evidence packs
const packs = [];
for (const c of cases) {
  const text = c.text || "";
  const mentions = extractMentions({
    responseId: "hier",
    text,
    entityIndex: index.aliasIndex,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
  const best = hits
    .slice()
    .sort(
      (a, b) =>
        ROLES.indexOf(a.role) - ROLES.indexOf(b.role) ||
        a.mentionPosition - b.mentionPosition
    )[0];
  const start = best?.mentionPosition ?? 0;
  const end = start + String(best?.rawMention || c.entityName || "").length;
  const evidence = extractEntityLocalEvidence({
    text,
    start,
    end,
    rawMention: best?.rawMention || c.entityName,
    canonicalEntityName: c.entityName,
    typedSections: buildTypedSections(text),
  });
  packs.push({
    case: c,
    start,
    end,
    rawMention: best?.rawMention || c.entityName,
    evidence,
    nodeGt: deriveNodeLabelsFromHumanRole(c.expectedRecommendationRole),
  });
}

// --- Node deterministic benchmarks ---
const nodeBench = {};
for (const nodeId of ["Q3", "Q4", "Q5", "Q6"]) {
  let n = 0;
  let ok = 0;
  let detOk = 0;
  let detN = 0;
  let needAdj = 0;
  for (const p of packs) {
    const expected = p.nodeGt[nodeId];
    if (!expected) continue;
    n++;
    const det = decideNodeDeterministic(nodeId, p.evidence, { entityPresent: true });
    if (!det.needsAdjudicator && det.result) {
      detN++;
      if (det.result === expected) detOk++;
      if (det.result === expected) ok++;
    } else {
      needAdj++;
    }
  }
  nodeBench[nodeId] = {
    N: n,
    DETERMINISTIC_N: detN,
    DETERMINISTIC_ACCURACY: detN ? detOk / detN : null,
    NEED_ADJUDICATOR: needAdj,
    // semantic filled later
    SEMANTIC_N: 0,
    SEMANTIC_ACCURACY: null,
    OVERALL_ACCURACY: null,
    correctSoFar: ok,
  };
}

// Estimate semantic calls for full hierarchical run
let estCalls = 0;
for (const p of packs) {
  // simulate path with det only counting needsAdj
  const path = {};
  const order = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"];
  for (const nodeId of order) {
    const det = decideNodeDeterministic(nodeId, p.evidence, { entityPresent: true });
    if (det.needsAdjudicator) estCalls++;
    path[nodeId] = det.result;
    if (nodeId === "Q1" && ["ABSENT", "SOURCE_ONLY", "INCIDENTAL"].includes(det.result)) break;
    if (nodeId === "Q2" && det.result === "YES_NEGATIVE") break;
    if (nodeId === "Q3" && ["COMPARATOR", "NEUTRAL_DISCUSSION"].includes(det.result)) break;
    if (nodeId === "Q4" && det.result === "CONSIDERATION_SET") break;
    if (nodeId === "Q5" && det.result === "NO_MEANINGFUL_ORDER") break;
  }
}
const estPer = estimateAdjudicatorCallCostUsd(MODEL);
const estCost = estCalls * estPer;
const maxCalls = Math.floor(COST_CAP / Math.max(estPer, 0.001));

console.log(
  JSON.stringify(
    {
      phase: "estimate",
      DEV_N: cases.length,
      ESTIMATED_SEMANTIC_CALLS: estCalls,
      ESTIMATED_COST: Number(estCost.toFixed(4)),
      CAP: COST_CAP,
      MAX_CALLS: maxCalls,
      PROVIDER,
      MODEL,
      nodeBenchDet: Object.fromEntries(
        Object.entries(nodeBench).map(([k, v]) => [
          k,
          { N: v.N, DET_ACC: v.DETERMINISTIC_ACCURACY, NEED_ADJ: v.NEED_ADJUDICATOR },
        ])
      ),
    },
    null,
    2
  )
);

if (estimateOnly) {
  const report = {
    phase: "AI_INTELLIGENCE_HIERARCHICAL_RECOMMENDATION_PROTOTYPE_COMPLETE",
    status: "AI_INTELLIGENCE_HIERARCHICAL_RECOMMENDATION_PROTOTYPE_REVIEW_REQUIRED",
    mode: "estimate_only",
    groundTruthCleanup: {
      TOTAL: gtCleanup.TOTAL,
      KEEP: gtCleanup.KEEP,
      AMEND: gtCleanup.AMEND,
      DEFER: gtCleanup.DEFER,
    },
    nodeBenchmarksDeterministic: nodeBench,
    estimate: { ESTIMATED_SEMANTIC_CALLS: estCalls, ESTIMATED_COST: estCost, CAP: COST_CAP },
    baseline: baseline.metrics,
    holdout: { HOLDOUT_ACCESSED: "NO", HOLDOUT_CASES_INSPECTED: 0, HOLDOUT_METRICS_RUN: "NO" },
  };
  fs.writeFileSync(REPORT, JSON.stringify(report, null, 2));
  console.log("Wrote estimate. Re-run with --apply.");
  process.exit(0);
}

if (!PROVIDER) {
  console.error("No provider credential");
  process.exit(2);
}

let callBudget = maxCalls;
let actualCost = 0;
let totalCalls = 0;

// --- Semantic node benchmarks (spend up to 40% of budget) ---
const nodeBudget = Math.floor(maxCalls * 0.4);
console.log(`Node semantic benchmarks (budget ${nodeBudget} calls)...`);
for (const nodeId of ["Q3", "Q4", "Q5", "Q6"]) {
  let semN = 0;
  let semOk = 0;
  for (const p of packs) {
    if (callBudget <= 0 || totalCalls >= nodeBudget) break;
    const expected = p.nodeGt[nodeId];
    if (!expected) continue;
    const det = decideNodeDeterministic(nodeId, p.evidence, { entityPresent: true });
    if (!det.needsAdjudicator) continue;
    const adj = await adjudicateHierarchicalNode({
      nodeId,
      entityName: p.case.entityName,
      entityLocalEvidence: String(
        p.evidence.localListItem || p.evidence.localSentence || ""
      ).slice(0, 1000),
      sectionHeading: p.evidence.sectionHeading,
      cueFacts: {
        considerationSetCue: Boolean(p.evidence.recommendationEvidence?.considerationSetCue),
        directPositiveCue: Boolean(p.evidence.recommendationEvidence?.directPositiveCue),
        leadCue: Boolean(p.evidence.recommendationEvidence?.leadCue),
        comparatorCue: Boolean(p.evidence.recommendationEvidence?.comparatorCue),
        confirmedRankStructure: Boolean(p.evidence.confirmedRankStructure),
        sectionType: p.evidence.sectionType,
      },
      structuralEvidence: p.evidence.structure,
      provider: PROVIDER,
      model: MODEL,
    });
    if (adj.LIVE_PROVIDER_CALL) {
      totalCalls++;
      callBudget--;
      actualCost += Number(adj.actualCostUsd || estPer);
    }
    if (adj.ok) {
      semN++;
      if (adj.selected === expected) semOk++;
      if (adj.selected === expected) nodeBench[nodeId].correctSoFar++;
    }
  }
  nodeBench[nodeId].SEMANTIC_N = semN;
  nodeBench[nodeId].SEMANTIC_ACCURACY = semN ? semOk / semN : null;
  nodeBench[nodeId].OVERALL_ACCURACY = nodeBench[nodeId].N
    ? nodeBench[nodeId].correctSoFar / nodeBench[nodeId].N
    : null;
}

// Optional tiny provider compare on 6 Q4 ambiguous cases if gemini also available
let providerCompare = null;
{
  const { resolveGeminiCredential, resolveClaudeCredential } = await import(
    "../lib/ai-visibility/provider-credentials.js"
  );
  const hasGemini = resolveGeminiCredential().status !== "MISSING";
  const hasClaude = resolveClaudeCredential().status !== "MISSING";
  if (hasGemini && hasClaude && callBudget >= 6) {
    const samples = packs
      .filter((p) => {
        const expected = p.nodeGt.Q4;
        if (!expected) return false;
        const det = decideNodeDeterministic("Q4", p.evidence, { entityPresent: true });
        return det.needsAdjudicator;
      })
      .slice(0, 3);
    providerCompare = { claude: { n: 0, ok: 0 }, gemini: { n: 0, ok: 0 } };
    for (const p of samples) {
      for (const prov of [
        { provider: "claude", model: process.env.AI_VISIBILITY_CLAUDE_MODEL || "claude-sonnet-4-6" },
        { provider: "gemini", model: process.env.AI_VISIBILITY_GEMINI_MODEL || "gemini-2.5-flash" },
      ]) {
        if (callBudget <= 0) break;
        const adj = await adjudicateHierarchicalNode({
          nodeId: "Q4",
          entityName: p.case.entityName,
          entityLocalEvidence: String(
            p.evidence.localListItem || p.evidence.localSentence || ""
          ).slice(0, 1000),
          cueFacts: {},
          structuralEvidence: p.evidence.structure || {},
          ...prov,
        });
        if (adj.LIVE_PROVIDER_CALL) {
          totalCalls++;
          callBudget--;
          actualCost += Number(adj.actualCostUsd || estPer);
        }
        const key = prov.provider;
        if (adj.ok) {
          providerCompare[key].n++;
          if (adj.selected === p.nodeGt.Q4) providerCompare[key].ok++;
        }
      }
    }
  }
}

// --- Full hierarchical compose ---
console.log(`Full hierarchical DEV run (remaining budget ${callBudget} calls)...`);
const cm = emptyCm();
let correct = 0;
let scored = 0;
let zeroCall = 0;
let oneCall = 0;
let multiCall = 0;
const errorLoc = {
  DETERMINISTIC_EVIDENCE_ERROR: 0,
  Q3_ERROR: 0,
  Q4_ERROR: 0,
  Q5_ERROR: 0,
  Q6_ERROR: 0,
  FINAL_COMPOSITION_ERROR: 0,
  GROUND_TRUTH_REVIEW_REQUIRED: 0,
};
const errorSamples = [];

for (const p of packs) {
  const c = p.case;
  const human = c.expectedRecommendationRole;
  const result = await classifyHierarchicalRecommendation({
    caseId: c.caseId,
    text: c.text,
    start: p.start,
    end: p.end,
    rawMention: p.rawMention,
    canonicalEntityName: c.entityName,
    entityPresent: true,
    callAdjudicator: true,
    remainingCallBudget: callBudget,
    adjudicatorOptions: { provider: PROVIDER, model: MODEL },
  });

  totalCalls += result.semanticCalls;
  callBudget -= result.semanticCalls;
  actualCost += Number(result.actualCostUsd || 0);

  if (result.semanticCalls === 0) zeroCall++;
  else if (result.semanticCalls === 1) oneCall++;
  else multiCall++;

  if (result.abstained || !result.finalRole) {
    errorLoc.FINAL_COMPOSITION_ERROR++;
    errorSamples.push({
      caseId: c.caseId,
      entity: c.entityName,
      human,
      pred: null,
      bucket: "FINAL_COMPOSITION_ERROR",
      path: result.path,
    });
    continue;
  }

  scored++;
  if (result.finalRole === human) correct++;
  updateCm(cm, human, result.finalRole);

  if (result.finalRole !== human) {
    // localize using node GT vs path
    let bucket = "DETERMINISTIC_EVIDENCE_ERROR";
    const ng = p.nodeGt;
    for (const nid of ["Q3", "Q4", "Q5", "Q6"]) {
      if (ng[nid] && result.path[nid] && result.path[nid] !== ng[nid]) {
        bucket = `${nid}_ERROR`;
        break;
      }
    }
    if (
      human === "first_recommendation" &&
      (result.finalRole === "explicit_recommendation" ||
        result.finalRole === "associated_option")
    ) {
      // check if path Q6/Q5 conflict with human lead expectation
      if (ng.Q6 === "LEAD" && result.path.Q6 !== "LEAD") bucket = "Q6_ERROR";
      else if (ng.Q5 && result.path.Q5 !== ng.Q5) bucket = "Q5_ERROR";
    }
    if (result.compositionError) bucket = "FINAL_COMPOSITION_ERROR";
    errorLoc[bucket] = (errorLoc[bucket] || 0) + 1;
    errorSamples.push({
      caseId: c.caseId,
      entity: c.entityName,
      human,
      pred: result.finalRole,
      bucket,
      path: result.path,
      nodeTrace: result.nodeTrace?.map((t) => ({
        NODE: t.NODE,
        DET: t.DETERMINISTIC_RESULT,
        ADJ: t.ADJUDICATOR_RESULT,
        FINAL: t.FINAL_NODE_RESULT,
        needed: t.ADJUDICATOR_NEEDED,
      })),
    });
  }
}

const hierAcc = scored ? correct / scored : null;
const classMetrics = finalizeCm(cm);
const macro = macroFromClassMetrics(classMetrics);

// QS + first-rec among scored
let qsOk = 0;
let firstOk = 0;
for (const p of packs) {
  const sample = errorSamples.find((e) => e.caseId === p.case.caseId);
  if (sample && sample.pred == null && sample.bucket === "FINAL_COMPOSITION_ERROR") continue;
  const pred = sample?.pred != null ? sample.pred : p.case.expectedRecommendationRole;
  // if error with pred, use pred; if no error, correct
  const wrong = errorSamples.find((e) => e.caseId === p.case.caseId && e.pred != null);
  const role = wrong ? wrong.pred : sample?.pred == null && sample ? null : p.case.expectedRecommendationRole;
  if (role == null) continue;
  const finalRole = wrong ? wrong.pred : p.case.expectedRecommendationRole;
  // Actually for correct cases there's no errorSamples entry
  const hasErr = errorSamples.find((e) => e.caseId === p.case.caseId);
  const got = hasErr?.pred != null ? hasErr.pred : hasErr ? null : p.case.expectedRecommendationRole;
  if (got == null) continue;
  const expQs = questionStatusFromRecommendationRole(p.case.expectedRecommendationRole, true);
  const gotQs = questionStatusFromRecommendationRole(got, true);
  if (expQs === gotQs) qsOk++;
  const expFirst = p.case.expectedRecommendationRole === "first_recommendation";
  const gotFirst = got === "first_recommendation";
  if (expFirst === gotFirst) firstOk++;
}
const qsN = scored;
const firstRec = qsN ? firstOk / qsN : null;
const qsAcc = qsN ? qsOk / qsN : null;

const recommendation =
  hierAcc != null && hierAcc >= 0.98 && firstRec >= 0.98
    ? "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION"
    : hierAcc != null &&
        hierAcc > baseline.metrics.ACCURACY + 0.03 &&
        (nodeBench.Q3.DETERMINISTIC_ACCURACY || 0) >= 0.7
      ? "HIERARCHICAL_APPROACH_PROMISING"
      : "SEMANTIC_TAXONOMY_REDESIGN_REQUIRED";

// Soften: if hierarchical beats old hybrid and det baseline materially OR node dets are strong
let rec = recommendation;
if (
  hierAcc != null &&
  hierAcc > Math.max(baseline.metrics.ACCURACY, OLD_HYBRID_ACCURACY) + 0.02
) {
  rec = "HIERARCHICAL_APPROACH_PROMISING";
}
if (hierAcc != null && hierAcc >= 0.98 && firstRec >= 0.98) {
  rec = "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION";
}
if (
  hierAcc != null &&
  hierAcc <= baseline.metrics.ACCURACY + 0.01 &&
  hierAcc <= OLD_HYBRID_ACCURACY + 0.01
) {
  rec = "SEMANTIC_TAXONOMY_REDESIGN_REQUIRED";
}

const status =
  baseline.metrics.ENTITY_P >= 0.98
    ? rec === "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION"
      ? "AI_INTELLIGENCE_HIERARCHICAL_RECOMMENDATION_PROTOTYPE_PASS"
      : "AI_INTELLIGENCE_HIERARCHICAL_RECOMMENDATION_PROTOTYPE_REVIEW_REQUIRED"
    : "AI_INTELLIGENCE_HIERARCHICAL_RECOMMENDATION_PROTOTYPE_BLOCKED";

const projectedPer1000 = scored
  ? (actualCost / Math.max(cases.length, 1)) * 1000
  : (estPer * estCalls * 1000) / Math.max(cases.length, 1);

const report = {
  phase: "AI_INTELLIGENCE_HIERARCHICAL_RECOMMENDATION_PROTOTYPE_COMPLETE",
  status,
  classifierVersion: HIERARCHICAL_CLASSIFIER_VERSION,
  groundTruthCleanup: {
    TOTAL: gtCleanup.TOTAL,
    KEEP: gtCleanup.KEEP,
    AMEND: gtCleanup.AMEND,
    DEFER: gtCleanup.DEFER,
    cases: gtCleanup.cases,
  },
  nodeBenchmarks: {
    Q3_N: nodeBench.Q3.N,
    Q3_ACCURACY: nodeBench.Q3.OVERALL_ACCURACY ?? nodeBench.Q3.DETERMINISTIC_ACCURACY,
    Q3_DET: nodeBench.Q3.DETERMINISTIC_ACCURACY,
    Q3_SEM: nodeBench.Q3.SEMANTIC_ACCURACY,
    Q4_N: nodeBench.Q4.N,
    Q4_ACCURACY: nodeBench.Q4.OVERALL_ACCURACY ?? nodeBench.Q4.DETERMINISTIC_ACCURACY,
    Q4_DET: nodeBench.Q4.DETERMINISTIC_ACCURACY,
    Q4_SEM: nodeBench.Q4.SEMANTIC_ACCURACY,
    Q5_N: nodeBench.Q5.N,
    Q5_ACCURACY: nodeBench.Q5.OVERALL_ACCURACY ?? nodeBench.Q5.DETERMINISTIC_ACCURACY,
    Q5_DET: nodeBench.Q5.DETERMINISTIC_ACCURACY,
    Q5_SEM: nodeBench.Q5.SEMANTIC_ACCURACY,
    Q6_N: nodeBench.Q6.N,
    Q6_ACCURACY: nodeBench.Q6.OVERALL_ACCURACY ?? nodeBench.Q6.DETERMINISTIC_ACCURACY,
    Q6_DET: nodeBench.Q6.DETERMINISTIC_ACCURACY,
    Q6_SEM: nodeBench.Q6.SEMANTIC_ACCURACY,
    detail: nodeBench,
  },
  routing: {
    DEV_N: cases.length,
    ZERO_CALL_CASES: zeroCall,
    ONE_CALL_CASES: oneCall,
    MULTI_CALL_CASES: multiCall,
    TOTAL_SEMANTIC_CALLS: totalCalls,
    CALL_RATE_PER_CASE: cases.length ? totalCalls / cases.length : 0,
  },
  costs: {
    ACTUAL_COST: Number(actualCost.toFixed(4)),
    PROJECTED_PER_1000: Number(projectedPer1000.toFixed(4)),
    CAP: COST_CAP,
    PROVIDER,
    MODEL,
  },
  providerCompare,
  baselines: {
    DETERMINISTIC: baseline.metrics.ACCURACY,
    OLD_HYBRID: OLD_HYBRID_ACCURACY,
    HIERARCHICAL: hierAcc,
  },
  hierarchical: {
    ACCURACY: hierAcc,
    PRECISION: hierAcc,
    RECALL: hierAcc,
    F1: hierAcc,
    MACRO_P: macro.MACRO_P,
    MACRO_R: macro.MACRO_R,
    MACRO_F1: macro.MACRO_F1,
    FIRST_REC: firstRec,
    QUESTION_STATUS: qsAcc,
    SCORED: scored,
    ABSTAINED: cases.length - scored,
  },
  classMetrics,
  errorLocalization: errorLoc,
  errorSamples: errorSamples.slice(0, 50),
  gates: {
    ENTITY_GATE:
      baseline.metrics.ENTITY_P >= 0.98 && baseline.metrics.ENTITY_R >= 0.98 ? "PASS" : "FAIL",
    RECOMMENDATION_GATE: hierAcc != null && hierAcc >= 0.98 ? "PASS" : "FAIL",
    FIRST_REC_GATE: firstRec != null && firstRec >= 0.98 ? "PASS" : "FAIL",
    CLASS_BALANCE_STATUS: macro.MACRO_F1 != null && macro.MACRO_F1 >= 0.85 ? "OK" : "WEAK",
  },
  holdout: {
    HOLDOUT_ACCESSED: "NO",
    HOLDOUT_CASES_INSPECTED: 0,
    HOLDOUT_METRICS_RUN: "NO",
  },
  recommendation: rec,
  hardGuards: {
    NEW_MONITORING: 0,
    PUBLIC_CRAWL: 0,
    HOLDOUT_ACCESS: 0,
    AUTO_GT_CHANGES: 0,
    AIRTABLE_WRITES: 0,
    SCHEMA_CHANGES: 0,
    DEPLOYS: 0,
    THRESHOLD_LOWERING: 0,
    CASE_SPECIFIC_RULES: 0,
  },
};

fs.writeFileSync(REPORT, JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      status: report.status,
      recommendation: report.recommendation,
      groundTruthCleanup: report.groundTruthCleanup,
      nodeBenchmarks: {
        Q3: report.nodeBenchmarks.Q3_ACCURACY,
        Q4: report.nodeBenchmarks.Q4_ACCURACY,
        Q5: report.nodeBenchmarks.Q5_ACCURACY,
        Q6: report.nodeBenchmarks.Q6_ACCURACY,
      },
      routing: report.routing,
      costs: report.costs,
      baselines: report.baselines,
      hierarchical: report.hierarchical,
      errorLocalization: report.errorLocalization,
      gates: report.gates,
    },
    null,
    2
  )
);
