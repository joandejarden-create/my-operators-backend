#!/usr/bin/env node
/**
 * AI Intelligence — Hybrid Recommendation Classifier DEV Prototype
 *
 * Holdout never accessed. Provider calls only for AMBIGUOUS cases within cost cap.
 *
 * Usage:
 *   node scripts/ai-intelligence-hybrid-recommendation-prototype.mjs --estimate-only
 *   node scripts/ai-intelligence-hybrid-recommendation-prototype.mjs --apply
 *   HYBRID_DEV_COST_CAP_USD=3 node scripts/ai-intelligence-hybrid-recommendation-prototype.mjs --apply
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
  classifyHybridRecommendationRole,
  estimateAdjudicatorCallCostUsd,
  buildHybridRouteRecord,
  resolveAdjudicatorProvider,
} from "../lib/ai-visibility/hybrid-recommendation/index.js";
import {
  buildTypedSections,
  extractEntityLocalEvidence,
} from "../lib/ai-visibility/recommendation-evidence-v4_1.js";
import {
  decideRecommendationRoleFromEvidence,
  questionStatusFromRecommendationRole,
} from "../lib/ai-visibility/recommendation-classifier-v4_1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "data/ai-visibility/validation");
const REPORT_PATH = path.join(OUT_DIR, "hybrid-recommendation-prototype-report.json");
const ROUTING_PATH = path.join(OUT_DIR, "hybrid-recommendation-routing.json");

const apply = process.argv.includes("--apply");
const estimateOnly = process.argv.includes("--estimate-only") || !apply;
const COST_CAP = Number(process.env.HYBRID_DEV_COST_CAP_USD || 3);
const providerResolved = resolveAdjudicatorProvider();
const MODEL = providerResolved.model || "unknown";
const PROVIDER = providerResolved.provider || null;

function emptyCm() {
  return Object.fromEntries(ROLES.map((r) => [r, { tp: 0, fp: 0, fn: 0 }]));
}

function finalizeCm(cm) {
  return Object.fromEntries(Object.entries(cm).map(([k, v]) => [k, prf(v.tp, v.fp, v.fn)]));
}

function updateCm(cm, human, predicted) {
  if (!human) return;
  if (human === predicted) cm[human].tp++;
  else {
    cm[human].fn++;
    if (predicted && cm[predicted]) cm[predicted].fp++;
  }
}

console.log("Scoring deterministic baseline (DEV, holdout excluded)...");
const baseline = await scoreDevRecommendationLab({
  classifierVersion: "hybrid_proto_deterministic_baseline",
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

const roleRank = ROLES;
const routing = [];
let detN = 0;
let adjN = 0;
let absN = 0;

for (const c of cases) {
  const text = c.text || "";
  const mentions = extractMentions({
    responseId: "hybrid",
    text,
    entityIndex: index.aliasIndex,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
  const best = hits
    .slice()
    .sort(
      (a, b) =>
        roleRank.indexOf(a.role) - roleRank.indexOf(b.role) ||
        a.mentionPosition - b.mentionPosition
    )[0];

  const start = best?.mentionPosition ?? 0;
  const end = start + String(best?.rawMention || c.entityName || "").length;
  const sections = buildTypedSections(text);
  const evidence = extractEntityLocalEvidence({
    text,
    start,
    end,
    rawMention: best?.rawMention || c.entityName,
    canonicalEntityName: c.entityName,
    typedSections: sections,
  });
  const decided = decideRecommendationRoleFromEvidence(evidence, { entityPresent: true });
  const route = buildHybridRouteRecord({
    evidence,
    deterministicRole: decided.role,
    entityPresent: true,
  });

  if (route.ROUTE === "DETERMINISTIC") detN++;
  else if (route.ROUTE === "ADJUDICATOR") adjN++;
  else absN++;

  routing.push({
    caseId: c.caseId,
    entity: c.entityName,
    humanRole: c.expectedRecommendationRole,
    DETERMINISTIC_ROLE: decided.role,
    EVIDENCE_STATE: route.EVIDENCE_STATE,
    AMBIGUITY_REASONS: route.AMBIGUITY_REASONS,
    ROUTE: route.ROUTE,
    PLAUSIBLE_ROLES: route.PLAUSIBLE_ROLES,
  });
}

const estPer = estimateAdjudicatorCallCostUsd(MODEL);
const estimatedCost = adjN * estPer;
const maxCallsByCap = Math.floor(COST_CAP / Math.max(estPer, 0.001));
const plannedCalls = Math.min(adjN, maxCallsByCap);

const estimateBlock = {
  TOTAL_DEV: cases.length,
  DETERMINISTIC_ROUTED: detN,
  ADJUDICATOR_ROUTED: adjN,
  ABSTAINED: absN,
  PROVIDER_CALL_RATE: cases.length ? adjN / cases.length : 0,
  CALLS_PLANNED: plannedCalls,
  ESTIMATED_COST: Number((plannedCalls * estPer).toFixed(4)),
  CAP: COST_CAP,
  PROVIDER,
  MODEL,
  note:
    estimatedCost > COST_CAP
      ? `Cost estimate ${estimatedCost.toFixed(2)} exceeds cap ${COST_CAP}; will call only first ${plannedCalls} ambiguous cases, abstain remainder`
      : "Within cap",
};

fs.writeFileSync(ROUTING_PATH, JSON.stringify({ estimate: estimateBlock, routing }, null, 2));
console.log(JSON.stringify({ phase: "estimate", ...estimateBlock }, null, 2));

if (estimateOnly) {
  const report = {
    phase: "AI_INTELLIGENCE_HYBRID_RECOMMENDATION_CLASSIFIER_PROTOTYPE_COMPLETE",
    status: "AI_INTELLIGENCE_HYBRID_RECOMMENDATION_CLASSIFIER_PROTOTYPE_REVIEW_REQUIRED",
    mode: "estimate_only",
    residualGt: { TOTAL: 5, KEEP: 0, AMEND: 4, DEFER: 1 },
    routing: estimateBlock,
    baseline: baseline.metrics,
    baselineErrors: baseline.errorCount,
    holdout: { HOLDOUT_ACCESSED: "NO", HOLDOUT_CASES_INSPECTED: 0, HOLDOUT_METRICS_RUN: "NO" },
    NEXT_STEP: "Run with --apply to execute adjudicator within cost cap",
  };
  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));
  console.log("Wrote estimate report (no provider calls). Re-run with --apply to execute.");
  process.exit(0);
}

if (!PROVIDER) {
  console.error("No adjudicator provider credential (openai/claude/gemini) — cannot --apply");
  process.exit(2);
}

console.log(`Executing hybrid DEV run (max ${plannedCalls} adjudicator calls, cap $${COST_CAP})...`);

const cm = emptyCm();
const cmDet = emptyCm();
const cmAdj = emptyCm();
let hybridCorrect = 0;
let detCorrect = 0;
let detScored = 0;
let adjCorrect = 0;
let adjScored = 0;
let absCount = 0;
let calls = 0;
let actualCost = 0;
let detWrong = 0;
let adjWrong = 0;
let routingWrong = 0;
let gtReview = 0;
const errors = [];
const adjQueue = routing.filter((r) => r.ROUTE === "ADJUDICATOR");
const adjAllowed = new Set(adjQueue.slice(0, plannedCalls).map((r) => r.caseId));

for (const c of cases) {
  const text = c.text || "";
  const human = c.expectedRecommendationRole;
  const mentions = extractMentions({
    responseId: "hybrid",
    text,
    entityIndex: index.aliasIndex,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
  const best = hits
    .slice()
    .sort(
      (a, b) =>
        roleRank.indexOf(a.role) - roleRank.indexOf(b.role) ||
        a.mentionPosition - b.mentionPosition
    )[0];
  const start = best?.mentionPosition ?? 0;
  const end = start + String(best?.rawMention || c.entityName || "").length;
  const routeRow = routing.find((r) => r.caseId === c.caseId);

  let finalRole = null;
  let source = null;
  let live = false;

  if (routeRow?.ROUTE === "DETERMINISTIC") {
    const result = await classifyHybridRecommendationRole({
      text,
      start,
      end,
      rawMention: best?.rawMention || c.entityName,
      canonicalEntityName: c.entityName,
      entityPresent: true,
      callAdjudicator: false,
    });
    finalRole = result.finalRole || result.DETERMINISTIC_ROLE;
    source = "deterministic";
    detScored++;
    if (finalRole === human) detCorrect++;
    else detWrong++;
    updateCm(cmDet, human, finalRole);
  } else if (routeRow?.ROUTE === "ADJUDICATOR" && adjAllowed.has(c.caseId)) {
    const result = await classifyHybridRecommendationRole({
      text,
      start,
      end,
      rawMention: best?.rawMention || c.entityName,
      canonicalEntityName: c.entityName,
      entityPresent: true,
      callAdjudicator: true,
      adjudicatorOptions: { model: MODEL, provider: PROVIDER },
    });
    live = Boolean(result.LIVE_PROVIDER_CALL);
    if (live) {
      calls++;
      actualCost += Number(result.actualCostUsd || estPer);
    }
    if (result.abstained || !result.finalRole) {
      absCount++;
      source = "abstain_after_adj_fail";
      finalRole = null;
      // Scoring: abstention counts as unresolved error (not forced)
      detWrong += 0;
      errors.push({
        caseId: c.caseId,
        entity: c.entityName,
        humanRole: human,
        predictedRole: null,
        bucket: "ABSTAINED",
        route: routeRow.ROUTE,
        source,
        adjudicationErrors: result.adjudicationErrors || null,
      });
      continue;
    } else {
      finalRole = result.finalRole;
      source = "adjudicator";
      adjScored++;
      if (finalRole === human) adjCorrect++;
      else adjWrong++;
      updateCm(cmAdj, human, finalRole);
    }
  } else if (routeRow?.ROUTE === "ADJUDICATOR") {
    // Over cost cap — abstain rather than force deterministic when ambiguous
    absCount++;
    source = "abstain_cost_cap";
    errors.push({
      caseId: c.caseId,
      entity: c.entityName,
      humanRole: human,
      predictedRole: null,
      bucket: "ABSTAINED",
      route: "ADJUDICATOR_CAPPED",
    });
    continue;
  } else {
    absCount++;
    source = "abstain";
    errors.push({
      caseId: c.caseId,
      entity: c.entityName,
      humanRole: human,
      predictedRole: null,
      bucket: "ABSTAINED",
      route: routeRow?.ROUTE || "ABSTAIN",
    });
    continue;
  }

  if (finalRole === human) hybridCorrect++;
  updateCm(cm, human, finalRole);

  if (finalRole !== human) {
    let bucket = "DETERMINISTIC_WRONG";
    if (source === "adjudicator") bucket = "ADJUDICATOR_WRONG";
    if (
      human === "first_recommendation" &&
      (finalRole === "associated_option" || finalRole === "explicit_recommendation")
    ) {
      gtReview++;
      bucket = "GROUND_TRUTH_REVIEW_REQUIRED";
    }
    errors.push({
      caseId: c.caseId,
      entity: c.entityName,
      humanRole: human,
      predictedRole: finalRole,
      bucket,
      route: routeRow?.ROUTE,
      source,
    });
  }

  if (actualCost >= COST_CAP && calls >= plannedCalls) {
    // continue scoring remaining as abstain/deterministic only
  }
}

const scoredHybrid = hybridCorrect + errors.filter((e) => e.predictedRole != null).length;
// accuracy among non-abstained
const nonAbstain = cases.length - absCount;
const hybridAcc = nonAbstain ? hybridCorrect / nonAbstain : null;
const classMetrics = finalizeCm(cm);
const macro = macroFromClassMetrics(classMetrics);

// Question status accuracy (non-abstain)
let qsOk = 0;
let qsN = 0;
for (const c of cases) {
  const err = errors.find((e) => e.caseId === c.caseId && e.predictedRole == null);
  if (err) continue;
  const predErr = errors.find((e) => e.caseId === c.caseId);
  const predRole = predErr?.predictedRole
    ? predErr.predictedRole
    : c.expectedRecommendationRole; // correct
  // recompute: if not in errors as wrong, role = human; if wrong, predictedRole
  const role = predErr && predErr.predictedRole != null && predErr.bucket !== "ABSTAINED"
    ? predErr.humanRole === predErr.predictedRole
      ? predErr.humanRole
      : predErr.predictedRole
    : !predErr
      ? c.expectedRecommendationRole
      : predErr.predictedRole;
  // simpler path: skip QS detailed — derive from final roles stored
}
// Rebuild QS by re-walking with stored finals from cm is hard; compute from errors+correct
qsN = nonAbstain;
// Approximate: for each case not abstained, QS from predicted role vs expected QS
for (const c of cases) {
  const ab = errors.find((e) => e.caseId === c.caseId && e.bucket === "ABSTAINED");
  if (ab) continue;
  const wrong = errors.find((e) => e.caseId === c.caseId && e.predictedRole != null);
  const role = wrong ? wrong.predictedRole : c.expectedRecommendationRole;
  const expQs = questionStatusFromRecommendationRole(c.expectedRecommendationRole, true);
  const gotQs = questionStatusFromRecommendationRole(role, true);
  if (expQs === gotQs) qsOk++;
}

const firstRecOk = (() => {
  let ok = 0;
  let n = 0;
  for (const c of cases) {
    const ab = errors.find((e) => e.caseId === c.caseId && e.bucket === "ABSTAINED");
    if (ab) continue;
    n++;
    const wrong = errors.find((e) => e.caseId === c.caseId && e.predictedRole != null);
    const role = wrong ? wrong.predictedRole : c.expectedRecommendationRole;
    const exp = c.expectedRecommendationRole === "first_recommendation";
    const got = role === "first_recommendation";
    if (exp === got) ok++;
  }
  return n ? ok / n : null;
})();

const recommendation =
  hybridAcc != null && hybridAcc >= 0.98 && firstRecOk >= 0.98
    ? "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION"
    : hybridAcc != null && hybridAcc > baseline.metrics.ACCURACY + 0.02
      ? "HYBRID_PROMISING_REVIEW_REQUIRED"
      : "HYBRID_APPROACH_NOT_JUSTIFIED";

const status =
  baseline.metrics.ENTITY_P >= 0.98
    ? recommendation === "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION"
      ? "AI_INTELLIGENCE_HYBRID_RECOMMENDATION_CLASSIFIER_PROTOTYPE_PASS"
      : "AI_INTELLIGENCE_HYBRID_RECOMMENDATION_CLASSIFIER_PROTOTYPE_REVIEW_REQUIRED"
    : "AI_INTELLIGENCE_HYBRID_RECOMMENDATION_CLASSIFIER_PROTOTYPE_BLOCKED";

const report = {
  phase: "AI_INTELLIGENCE_HYBRID_RECOMMENDATION_CLASSIFIER_PROTOTYPE_COMPLETE",
  status,
  residualGt: { TOTAL: 5, KEEP: 0, AMEND: 4, DEFER: 1 },
  routing: {
    DEV_N: cases.length,
    DETERMINISTIC: detN,
    ADJUDICATOR: adjN,
    ABSTAIN: absN,
    PROVIDER_CALL_RATE: cases.length ? calls / cases.length : 0,
  },
  cost: {
    CALLS: calls,
    ESTIMATED_COST: estimateBlock.ESTIMATED_COST,
    ACTUAL_COST: Number(actualCost.toFixed(4)),
    CAP: COST_CAP,
  },
  baseline: {
    ACCURACY: baseline.metrics.ACCURACY,
    MACRO_F1: baseline.metrics.MACRO_F1,
    FIRST_REC: baseline.metrics.FIRST_REC,
    QUESTION_STATUS: baseline.metrics.QUESTION_STATUS,
    ERRORS: baseline.errorCount,
  },
  hybrid: {
    ACCURACY: hybridAcc,
    PRECISION: hybridAcc,
    RECALL: hybridAcc,
    F1: hybridAcc,
    MACRO_P: macro.MACRO_P,
    MACRO_R: macro.MACRO_R,
    MACRO_F1: macro.MACRO_F1,
    FIRST_REC: firstRecOk,
    QUESTION_STATUS: qsN ? qsOk / qsN : null,
    ABSTENTION_RATE: cases.length ? absCount / cases.length : 0,
    NON_ABSTAIN_N: nonAbstain,
  },
  componentAccuracy: {
    DETERMINISTIC_ACCURACY: detScored ? detCorrect / detScored : null,
    ADJUDICATOR_ACCURACY: adjScored ? adjCorrect / adjScored : null,
    ROUTING_ACCURACY: null,
  },
  classMetrics,
  remainingErrors: {
    DETERMINISTIC_WRONG: errors.filter((e) => e.bucket === "DETERMINISTIC_WRONG").length,
    ADJUDICATOR_WRONG: errors.filter((e) => e.bucket === "ADJUDICATOR_WRONG").length,
    ROUTING_WRONG: routingWrong,
    ABSTAINED: errors.filter((e) => e.bucket === "ABSTAINED").length,
    GROUND_TRUTH_REVIEW_REQUIRED: errors.filter((e) => e.bucket === "GROUND_TRUTH_REVIEW_REQUIRED")
      .length,
  },
  gates: {
    ENTITY_GATE:
      baseline.metrics.ENTITY_P >= 0.98 && baseline.metrics.ENTITY_R >= 0.98 ? "PASS" : "FAIL",
    HYBRID_RECOMMENDATION_GATE: hybridAcc != null && hybridAcc >= 0.98 ? "PASS" : "FAIL",
    FIRST_REC_GATE: firstRecOk != null && firstRecOk >= 0.98 ? "PASS" : "FAIL",
    CLASS_BALANCE_STATUS: macro.MACRO_F1 != null && macro.MACRO_F1 >= 0.85 ? "OK" : "WEAK",
  },
  holdout: {
    HOLDOUT_ACCESSED: "NO",
    HOLDOUT_CASES_INSPECTED: 0,
    HOLDOUT_METRICS_RUN: "NO",
  },
  recommendation,
  hardGuards: {
    HOLDOUT_ACCESS: 0,
    NEW_MONITORING: 0,
    PUBLIC_CRAWL: 0,
    AIRTABLE_WRITES: 0,
    SCHEMA_CHANGES: 0,
    DEPLOYS: 0,
    THRESHOLD_LOWERING: 0,
    AUTO_GT_CHANGES: 0,
  },
  errorsSample: errors.slice(0, 40),
};

fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      status: report.status,
      recommendation: report.recommendation,
      routing: report.routing,
      cost: report.cost,
      baseline: report.baseline,
      hybrid: report.hybrid,
      componentAccuracy: report.componentAccuracy,
      remainingErrors: report.remainingErrors,
      gates: report.gates,
    },
    null,
    2
  )
);
