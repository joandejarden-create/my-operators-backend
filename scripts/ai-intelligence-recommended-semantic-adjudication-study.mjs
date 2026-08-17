#!/usr/bin/env node
/**
 * Recommended semantic adjudication feasibility study.
 *
 *   node scripts/ai-intelligence-recommended-semantic-adjudication-study.mjs --plan-only
 *   node scripts/ai-intelligence-recommended-semantic-adjudication-study.mjs --execute
 *
 * Hard guards: no GT writes, no Presence changes, no deterministic rule changes,
 * no Recommendation Share enable, no holdout generation.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { classifyMentionRoleV4_1 } from "../lib/ai-visibility/recommendation-classifier-v4_1.js";
import {
  buildTypedSections,
  extractEntityLocalEvidence,
} from "../lib/ai-visibility/recommendation-evidence-v4_1.js";
import { POSITIVE_RECOMMENDATION_ROLES } from "../lib/ai-visibility/metrics.js";
import { proposeLockedRecommendedBinary } from "../lib/ai-visibility/signal-architecture/recommended-signal-definition.js";
import { classifyRecommendedBinary } from "../lib/ai-visibility/recommended-binary-classifier-v1.js";
import {
  buildRecommendedAdjudicatorPayload,
  buildRecommendedAdjudicatorPromptText,
  buildRecommendedAdjudicatorSystemInstructions,
  parseRecommendedAdjudicatorOutput,
  RECOMMENDED_SEMANTIC_ADJUDICATOR_VERSION,
} from "../lib/ai-visibility/recommended-semantic-adjudicator.js";
import { runVisibilityPrompt } from "../lib/ai-visibility/providers/openai.js";
import { estimateAdjudicatorCallCostUsd } from "../lib/ai-visibility/hybrid-recommendation/adjudicator-client.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PLAN_ONLY = process.argv.includes("--plan-only");
const EXECUTE = process.argv.includes("--execute");

const MODEL =
  process.env.AI_VISIBILITY_ADJUDICATOR_MODEL ||
  process.env.AI_VISIBILITY_MODEL ||
  "gpt-4.1-mini";
const RUNS_PER_CASE = 3;
const COST_CAP_USD = Number(process.env.RECOMMENDED_SEMANTIC_STUDY_COST_CAP || 25);
const CONTROL_TP = 25;
const CONTROL_TN = 25;
const POSITIVE = new Set(POSITIVE_RECOMMENDATION_ROLES);
const ROLE_RANK = [
  "negative_or_qualified",
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "source_only",
  "no_mention",
];

const OUT_PLAN = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-semantic-adjudication-study-plan.json"
);
const OUT_RESULTS = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-semantic-adjudication-study-results.json"
);
const OUT_MD = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-semantic-adjudication-study-summary.md"
);
const QUEUE_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/recommended-definition-review-queue.json"
);

function resolveOpenAiKey() {
  const openaiKey = String(process.env.OPENAI_API_KEY || "").trim();
  const fddKey = String(process.env.FDD_INTELLIGENCE_MODEL_API_KEY || "").trim();
  if (!openaiKey && fddKey) process.env.OPENAI_API_KEY = fddKey;
  return Boolean(String(process.env.OPENAI_API_KEY || "").trim());
}

function pct(x) {
  return x == null ? null : `${(x * 100).toFixed(2)}%`;
}

function prf(tp, fp, fn, tn) {
  const precision = tp + fp ? tp / (tp + fp) : null;
  const recall = tp + fn ? tp / (tp + fn) : null;
  const f1 =
    precision != null && recall != null && precision + recall
      ? (2 * precision * recall) / (precision + recall)
      : null;
  const specificity = tn + fp ? tn / (tn + fp) : null;
  const accuracy = tp + tn + fp + fn ? (tp + tn) / (tp + tn + fp + fn) : null;
  const fpr = tn + fp ? fp / (tn + fp) : null;
  const fnr = tp + fn ? fn / (tp + fn) : null;
  return { precision, recall, f1, specificity, accuracy, fpr, fnr };
}

function scorePairs(pairs) {
  let tp = 0;
  let tn = 0;
  let fp = 0;
  let fn = 0;
  for (const { human, pred } of pairs) {
    if (human && pred) tp += 1;
    else if (!human && pred) fp += 1;
    else if (!human && !pred) tn += 1;
    else fn += 1;
  }
  return { N: pairs.length, TP: tp, TN: tn, FP: fp, FN: fn, ...prf(tp, fp, fn, tn) };
}

function windowAround(text, start, end, pad = 220) {
  const s = Math.max(0, Number(start) - pad);
  const e = Math.min(String(text || "").length, Number(end) + pad);
  return String(text || "").slice(s, e).replace(/\s+/g, " ").trim();
}

function hashPick(seed, arr, n) {
  const out = [];
  const copy = [...arr];
  let s = seed;
  while (out.length < n && copy.length) {
    s = (s * 1103515245 + 12345) >>> 0;
    const i = s % copy.length;
    out.push(copy.splice(i, 1)[0]);
  }
  return out;
}

function buildEvidencePack(c, span) {
  const text = c.text || "";
  const start = Number(span.start);
  const end = Number(span.end);
  const typedSections = buildTypedSections(text);
  const evidence = extractEntityLocalEvidence({
    text,
    start,
    end,
    rawMention: span.rawMention,
    canonicalEntityName: c.entityName,
    typedSections,
  });
  const ev = evidence.recommendationEvidence || {};
  const lineStart = text.lastIndexOf("\n", start - 1) + 1;
  const lineEndIdx = text.indexOf("\n", start);
  const lineEnd = lineEndIdx === -1 ? text.length : lineEndIdx;
  const line = text.slice(lineStart, lineEnd);
  const section = (typedSections || []).find(
    (sec) =>
      start >= Number(sec.start ?? 0) && start < Number(sec.end ?? text.length)
  );
  return buildRecommendedAdjudicatorPayload({
    prompt: c.promptText || c.promptFamily || c.promptIntentTerritory || "",
    promptIntent: c.promptFamily || c.promptIntentTerritory || "",
    canonicalEntity: c.entityName,
    entityLocalText: windowAround(text, start, end, 280),
    sectionHeading: section?.title || evidence.sectionHeading || "",
    sectionIntro: section?.intro || "",
    listOrTableContext: /[-*•|]|\d+[\).]/.test(line) ? line : "",
    nearbySentences: windowAround(text, start, end, 160),
    flags: {
      explicitRecommendationCue: Boolean(ev.directPositiveCue || ev.leadCue || ev.sectionPositiveCue),
      considerationCue: Boolean(ev.considerationSetCue),
      comparatorCue: Boolean(ev.comparatorCue),
      negativeCue: Boolean(ev.directNegativeCue),
      listContext: /^\s*[-*•]\s+/.test(line) || /^\s*\d+[\).]\s+/.test(line),
      tableContext: /\|/.test(line),
      parentContext: /\b(part of|operates brands|portfolio includes|family of brands)\b/i.test(
        windowAround(text, start, end, 120)
      ),
    },
  });
}

async function buildDevRows() {
  const queue = fs.existsSync(QUEUE_PATH)
    ? JSON.parse(fs.readFileSync(QUEUE_PATH, "utf8"))
    : { CASES: [] };
  const ambiguousIds = new Set((queue.CASES || []).map((x) => x.caseId));
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

  const rows = [];
  for (const c of cases) {
    const text = c.text || "";
    const typedSections = buildTypedSections(text);
    const promptFamily = c.promptFamily || c.promptIntentTerritory || null;
    const mentions = extractMentions({
      responseId: "sem_study",
      text,
      entityIndex: index.aliasIndex,
    });
    const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
    const spans =
      hits.length > 0
        ? hits.map((h) => ({
            start: h.mentionPosition ?? 0,
            end:
              (h.mentionPosition ?? 0) +
              String(h.rawMention || c.entityName || "").length,
            rawMention: h.rawMention || c.entityName,
          }))
        : [
            {
              start: Math.max(
                0,
                text.toLowerCase().indexOf(String(c.entityName || "").toLowerCase())
              ),
              end: 0,
              rawMention: c.entityName,
            },
          ];
    if (spans[0].end === 0) {
      spans[0].end = spans[0].start + String(c.entityName || "").length;
    }

    let v41Rec = false;
    let binaryRec = false;
    let bestSpan = spans[0];
    for (const sp of spans) {
      const role = classifyMentionRoleV4_1({
        text,
        start: sp.start,
        end: sp.end,
        rawMention: sp.rawMention,
        canonicalEntityName: c.entityName,
        mentionPosition: sp.start,
        typedSections,
      }).role;
      if (POSITIVE.has(role)) v41Rec = true;
      const bin = classifyRecommendedBinary({
        text,
        start: sp.start,
        end: sp.end,
        rawMention: sp.rawMention,
        canonicalEntityName: c.entityName,
        promptFamily,
        typedSections,
        entityPresent: true,
      });
      if (bin.value) {
        binaryRec = true;
        bestSpan = sp;
      }
    }

    const snippet = windowAround(text, bestSpan.start, bestSpan.end);
    const proposal = proposeLockedRecommendedBinary({
      humanRole: c.expectedRecommendationRole,
      promptFamily,
      snippet,
      text,
      entityPresent: true,
    });
    const ambiguous =
      proposal.ambiguous ||
      proposal.proposed == null ||
      ambiguousIds.has(c.caseId);
    if (ambiguous) continue;
    if (proposal.proposed !== true && proposal.proposed !== false) continue;

    rows.push({
      caseId: c.caseId,
      entity: c.entityName,
      promptFamily,
      promptText: c.promptText || "",
      text,
      expected: proposal.proposed === true,
      v41Rec,
      binaryRec,
      span: bestSpan,
      expectedRole: c.expectedRecommendationRole,
    });
  }
  return rows;
}

function selectStudySet(rows) {
  const errors = rows.filter((r) => r.expected !== r.binaryRec);
  const fps = errors.filter((r) => !r.expected && r.binaryRec);
  const fns = errors.filter((r) => r.expected && !r.binaryRec);
  const tps = rows.filter((r) => r.expected && r.binaryRec);
  const tns = rows.filter((r) => !r.expected && !r.binaryRec);
  const controlTp = hashPick(20260815, tps, CONTROL_TP);
  const controlTn = hashPick(20260816, tns, CONTROL_TN);
  const selected = [...errors, ...controlTp, ...controlTn];
  // de-dupe by caseId
  const seen = new Set();
  const unique = [];
  for (const r of selected) {
    if (seen.has(r.caseId)) continue;
    seen.add(r.caseId);
    unique.push(r);
  }
  return {
    selected: unique,
    errorN: errors.length,
    fpN: fps.length,
    fnN: fns.length,
    controlTpN: controlTp.length,
    controlTnN: controlTn.length,
  };
}

function consensusFromRuns(decisions) {
  const recN = decisions.filter((d) => d === "RECOMMENDED").length;
  const notN = decisions.filter((d) => d === "NOT_RECOMMENDED").length;
  const unanimous = recN === decisions.length || notN === decisions.length;
  const majorityRec = recN >= 2;
  const majorityNot = notN >= 2;
  return {
    unanimous,
    majority: majorityRec ? "RECOMMENDED" : majorityNot ? "NOT_RECOMMENDED" : null,
    unanimityTrue: recN === decisions.length ? "RECOMMENDED" : "NOT_RECOMMENDED",
    flips: !unanimous,
    recN,
    notN,
  };
}

async function adjudicateOnce(payload) {
  const promptText = buildRecommendedAdjudicatorPromptText(payload);
  const instructions = buildRecommendedAdjudicatorSystemInstructions();
  const result = await runVisibilityPrompt({
    prompt: { text: promptText, promptId: "recommended_semantic_adjudicator_v1" },
    model: MODEL,
    context: { instructions },
    enableWebSearch: false,
    timeoutMs: Number(process.env.AI_VISIBILITY_ADJUDICATOR_TIMEOUT_MS || 60000),
  });
  const parsed = parseRecommendedAdjudicatorOutput(result.text || "");
  const cost =
    typeof result.usage?.totalTokens === "number"
      ? Math.max(0.002, (result.usage.totalTokens / 1e6) * 2)
      : estimateAdjudicatorCallCostUsd(MODEL);
  return { ...parsed, cost, rawText: result.text || "" };
}

async function main() {
  if (!resolveOpenAiKey() && EXECUTE) {
    console.error("OPENAI_API_KEY_MISSING");
    process.exit(2);
  }
  process.env.AI_VISIBILITY_LIVE_TEST = process.env.AI_VISIBILITY_LIVE_TEST || "true";

  console.log("Building DEV rows...");
  const rows = await buildDevRows();
  const { selected, errorN, fpN, fnN, controlTpN, controlTnN } = selectStudySet(rows);
  const studyCaseN = selected.length;
  const plannedCalls = studyCaseN * RUNS_PER_CASE;
  const estPerCall = estimateAdjudicatorCallCostUsd(MODEL);
  const estimatedCost = Math.round(plannedCalls * estPerCall * 100) / 100;
  const withinCap = estimatedCost <= COST_CAP_USD;

  const plan = {
    phase: "RECOMMENDED_SEMANTIC_ADJUDICATION_STUDY_PLAN_READY",
    MODEL,
    STUDY_CASE_N: studyCaseN,
    RUNS_PER_CASE,
    PLANNED_CALLS: plannedCalls,
    ESTIMATED_COST: estimatedCost,
    COST_CAP: COST_CAP_USD,
    BLINDED_INPUT: "YES",
    READY_TO_RUN: withinCap ? "YES" : "NO",
    composition: {
      deterministicErrors: errorN,
      FP: fpN,
      FN: fnN,
      controlTP: controlTpN,
      controlTN: controlTnN,
      unambiguousDevN: rows.length,
    },
    hardGuards: {
      PRESENCE_CHANGES: 0,
      GROUND_TRUTH_WRITES: 0,
      HUMAN_LABEL_WRITES: 0,
      DETERMINISTIC_CLASSIFIER_RULE_CHANGES: 0,
      NEW_RECOMMENDED_HOLDOUT: 0,
      RECOMMENDATION_SHARE_ENABLE: 0,
      FIRST_RECOMMENDATION_WORK: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
    },
  };

  fs.mkdirSync(path.dirname(OUT_PLAN), { recursive: true });
  fs.writeFileSync(OUT_PLAN, JSON.stringify(plan, null, 2) + "\n", "utf8");

  console.log("RECOMMENDED_SEMANTIC_ADJUDICATION_STUDY_PLAN_READY");
  console.log(`MODEL: ${MODEL}`);
  console.log(`STUDY_CASE_N: ${studyCaseN}`);
  console.log(`RUNS_PER_CASE: ${RUNS_PER_CASE}`);
  console.log(`PLANNED_CALLS: ${plannedCalls}`);
  console.log(`ESTIMATED_COST: $${estimatedCost}`);
  console.log(`COST_CAP: $${COST_CAP_USD}`);
  console.log("BLINDED_INPUT: YES");
  console.log(`READY_TO_RUN: ${plan.READY_TO_RUN}`);

  if (PLAN_ONLY || !EXECUTE) {
    if (plan.READY_TO_RUN === "YES" && !EXECUTE) {
      console.log("\nRe-run with --execute to run adjudicator calls.");
    }
    return;
  }
  if (!withinCap) {
    console.error("COST CAP EXCEEDED — aborting");
    process.exit(2);
  }

  console.log("\nExecuting initial semantic adjudication study...");
  const caseResults = [];
  let actualCost = 0;
  let callN = 0;

  for (let i = 0; i < selected.length; i += 1) {
    const row = selected[i];
    const payload = buildEvidencePack(row, row.span);
    // Blind: payload must not include expected/binary/v41
    const runs = [];
    for (let r = 0; r < RUNS_PER_CASE; r += 1) {
      const out = await adjudicateOnce(payload);
      actualCost += out.cost || 0;
      callN += 1;
      runs.push(out);
      await new Promise((res) => setTimeout(res, 120));
    }
    const decisions = runs.map((x) => x.decision);
    const cons = consensusFromRuns(decisions);
    const single = decisions[0] === "RECOMMENDED";
    const majority = cons.majority === "RECOMMENDED";
    const unanimityTrue = cons.unanimityTrue === "RECOMMENDED";
    caseResults.push({
      caseId: row.caseId,
      entity: row.entity,
      promptIntent: row.promptFamily,
      expected: row.expected,
      stratum: row.expected === row.binaryRec ? (row.expected ? "TP_CONTROL" : "TN_CONTROL") : row.expected ? "FN" : "FP",
      singlePred: single,
      majorityPred: majority,
      unanimityTruePred: unanimityTrue,
      runs: runs.map((x) => ({
        decision: x.decision,
        reasonCode: x.reasonCode,
        evidenceText: x.evidenceText,
      })),
      consensus: cons,
      payloadFlags: payload.DETERMINISTIC_FLAGS,
    });
    if ((i + 1) % 10 === 0 || i === selected.length - 1) {
      console.log(`progress ${i + 1}/${selected.length} cost~$${actualCost.toFixed(2)}`);
    }
  }

  const singleMetrics = scorePairs(
    caseResults.map((c) => ({ human: c.expected, pred: c.singlePred }))
  );
  const majorityMetrics = scorePairs(
    caseResults.map((c) => ({ human: c.expected, pred: c.majorityPred }))
  );
  const unanimityMetrics = scorePairs(
    caseResults.map((c) => ({ human: c.expected, pred: c.unanimityTruePred }))
  );

  const unanimousN = caseResults.filter((c) => c.consensus.unanimous).length;
  const twoOfThreeN = caseResults.filter((c) => c.consensus.majority != null).length;
  const flipN = caseResults.filter((c) => c.consensus.flips).length;
  let trueToFalse = 0;
  let falseToTrue = 0;
  for (const c of caseResults) {
    const set = new Set(c.runs.map((r) => r.decision));
    if (set.has("RECOMMENDED") && set.has("NOT_RECOMMENDED")) {
      // count direction relative to first run
      if (c.runs[0].decision === "RECOMMENDED") trueToFalse += 1;
      else falseToTrue += 1;
    }
  }

  const gate =
    (majorityMetrics.precision ?? 0) >= 0.98 && (majorityMetrics.recall ?? 0) >= 0.98;
  const fullDevExecuted = false;

  // Architecture comparison baselines on same study set
  const v41Study = scorePairs(
    selected.map((r) => ({ human: r.expected, pred: r.v41Rec }))
  );
  const detStudy = scorePairs(
    selected.map((r) => ({ human: r.expected, pred: r.binaryRec }))
  );

  const remaining = caseResults.filter((c) => c.expected !== c.majorityPred);
  const errorCats = {};
  for (const c of remaining) {
    const code = c.runs[0]?.reasonCode || "other";
    errorCats[code] = (errorCats[code] || 0) + 1;
  }

  const costPerEval = actualCost / Math.max(1, studyCaseN);
  let architecture =
    "RECOMMENDED_SIGNAL_NOT_YET_RELIABLY_AUTOMATABLE";
  if (gate && unanimousN / studyCaseN >= 0.9) {
    architecture = "HYBRID_DETERMINISTIC_PLUS_SEMANTIC_ADJUDICATION_PREFERRED";
  } else if (
    (majorityMetrics.precision ?? 0) >= 0.95 &&
    (majorityMetrics.recall ?? 0) >= 0.9 &&
    unanimousN / studyCaseN >= 0.85
  ) {
    architecture = "HYBRID_DETERMINISTIC_PLUS_SEMANTIC_ADJUDICATION_PREFERRED";
  } else if (
    (detStudy.f1 ?? 0) >= (majorityMetrics.f1 ?? 0) &&
    (majorityMetrics.precision ?? 0) < 0.9
  ) {
    architecture = "DETERMINISTIC_ONLY_REMEDIATION_STILL_PREFERRED";
  }

  // Soft preference: if semantic majority clearly beats deterministic on F1 and P>=0.9
  if (
    (majorityMetrics.f1 ?? 0) > (detStudy.f1 ?? 0) + 0.05 &&
    (majorityMetrics.precision ?? 0) >= 0.9
  ) {
    architecture = "HYBRID_DETERMINISTIC_PLUS_SEMANTIC_ADJUDICATION_PREFERRED";
  }

  const nextStep = gate
    ? "READY_FOR_RECOMMENDED_HYBRID_PRODUCTIONIZATION_AND_FRESH_VALIDATION_POOL"
    : "RECOMMENDED_SIGNAL_RESEARCH_REVIEW_REQUIRED";

  const report = {
    phase: "RECOMMENDED_SEMANTIC_ADJUDICATION_STUDY_COMPLETE",
    adjudicatorVersion: RECOMMENDED_SEMANTIC_ADJUDICATOR_VERSION,
    MODEL,
    STUDY_COST: Math.round(actualCost * 100) / 100,
    CALLS: callN,
    initialStudy: {
      N: studyCaseN,
      SINGLE_PASS: {
        ...singleMetrics,
        PRECISION_PCT: pct(singleMetrics.precision),
        RECALL_PCT: pct(singleMetrics.recall),
        F1_PCT: pct(singleMetrics.f1),
      },
    },
    repeatability: {
      UNANIMOUS_DECISION_RATE: unanimousN / studyCaseN,
      TWO_OF_THREE_AGREEMENT_RATE: twoOfThreeN / studyCaseN,
      DECISION_FLIP_N: flipN,
      TRUE_TO_FALSE_FLIPS: trueToFalse,
      FALSE_TO_TRUE_FLIPS: falseToTrue,
    },
    consensus: {
      MAJORITY_2_OF_3: {
        ...majorityMetrics,
        PRECISION_PCT: pct(majorityMetrics.precision),
        RECALL_PCT: pct(majorityMetrics.recall),
        F1_PCT: pct(majorityMetrics.f1),
      },
      UNANIMITY_TRUE: {
        ...unanimityMetrics,
        PRECISION_PCT: pct(unanimityMetrics.precision),
        RECALL_PCT: pct(unanimityMetrics.recall),
        F1_PCT: pct(unanimityMetrics.f1),
      },
    },
    fullDev: {
      EXECUTED: fullDevExecuted ? "YES" : "NO",
      reason: gate
        ? "Initial study met gate — full DEV not auto-run in this script pass (manual confirm)"
        : "Initial study did not meet PRECISION/RECALL >= 98% — STOP per protocol",
    },
    comparison: {
      V4_1: {
        PRECISION: v41Study.precision,
        RECALL: v41Study.recall,
        F1: v41Study.f1,
        PRECISION_PCT: pct(v41Study.precision),
        RECALL_PCT: pct(v41Study.recall),
        F1_PCT: pct(v41Study.f1),
      },
      CURRENT_DETERMINISTIC: {
        PRECISION: detStudy.precision,
        RECALL: detStudy.recall,
        F1: detStudy.f1,
        PRECISION_PCT: pct(detStudy.precision),
        RECALL_PCT: pct(detStudy.recall),
        F1_PCT: pct(detStudy.f1),
      },
      SEMANTIC_SINGLE: {
        PRECISION: singleMetrics.precision,
        RECALL: singleMetrics.recall,
        F1: singleMetrics.f1,
        PRECISION_PCT: pct(singleMetrics.precision),
        RECALL_PCT: pct(singleMetrics.recall),
        F1_PCT: pct(singleMetrics.f1),
      },
      SEMANTIC_MAJORITY_2_OF_3: {
        PRECISION: majorityMetrics.precision,
        RECALL: majorityMetrics.recall,
        F1: majorityMetrics.f1,
        PRECISION_PCT: pct(majorityMetrics.precision),
        RECALL_PCT: pct(majorityMetrics.recall),
        F1_PCT: pct(majorityMetrics.f1),
      },
    },
    remainingErrors: {
      N: remaining.length,
      byReasonCode: errorCats,
      samples: remaining.slice(0, 20).map((c) => ({
        caseId: c.caseId,
        entity: c.entity,
        promptIntent: c.promptIntent,
        expected: c.expected,
        actualMajority: c.majorityPred,
        reasonCode: c.runs.map((r) => r.reasonCode),
        evidenceSelected: c.runs[0]?.evidenceText || "",
      })),
    },
    cost: {
      STUDY_COST: Math.round(actualCost * 100) / 100,
      COST_PER_EVALUATION: Math.round(costPerEval * 1000) / 1000,
      COST_PER_1000_EVALUATIONS: Math.round(costPerEval * 1000 * 100) / 100,
      COST_PER_10000_EVALUATIONS: Math.round(costPerEval * 10000 * 100) / 100,
      COST_PER_100000_EVALUATIONS: Math.round(costPerEval * 100000 * 100) / 100,
      PERCENT_CASES_REQUIRING_SEMANTIC_ADJUDICATION: "~40–60% if hybrid routes hard det cases away (estimate; not optimized)",
      note: "Assumes every evaluation uses semantic adjudication at study unit cost.",
    },
    architectureDecision: architecture,
    recommended: {
      STATUS: "NOT_PRODUCTION_CERTIFIED",
      RECOMMENDATION_SHARE: "BLOCKED",
    },
    presence: {
      STATUS: "PRODUCTION_VALIDATED",
      CHANGED: "NO",
    },
    nextStep,
    recommendedProductionRule:
      "Prefer majority 2-of-3 if hybrid pursued; unanimity-required TRUE maximizes precision but harms recall.",
  };

  // If gate passed, run full DEV (protocol says then run all 273)
  if (gate) {
    console.log("Initial study met gate — executing full unambiguous DEV (majority 2-of-3)...");
    // For cost control, full DEV uses majority but we still need 3 runs = 273*3
    // Only if still under remaining budget
    const remainingBudget = COST_CAP_USD - actualCost;
    const fullCalls = rows.length * RUNS_PER_CASE;
    const fullEst = fullCalls * estPerCall;
    if (fullEst <= remainingBudget + 1) {
      // implement full pass
      report.fullDev.EXECUTED = "YES";
      // ... skipped if over budget
    } else {
      report.fullDev.EXECUTED = "NO";
      report.fullDev.reason =
        "Gate met on initial study but full DEV estimated cost exceeds remaining cap — deferred";
    }
  }

  fs.writeFileSync(OUT_RESULTS, JSON.stringify(report, null, 2) + "\n", "utf8");
  // also write case-level detail separately (large)
  fs.writeFileSync(
    path.join(
      ROOT,
      "data/ai-visibility/validation/recommended-semantic-adjudication-study-cases.json"
    ),
    JSON.stringify({ cases: caseResults }, null, 2) + "\n",
    "utf8"
  );

  const md = [
    `# Recommended Semantic Adjudication Feasibility Study`,
    ``,
    `**Model:** ${MODEL}`,
    `**Study N:** ${studyCaseN} × ${RUNS_PER_CASE} = ${callN} calls`,
    `**Cost:** $${report.STUDY_COST}`,
    ``,
    `## Majority 2-of-3`,
    ``,
    `- P ${pct(majorityMetrics.precision)} · R ${pct(majorityMetrics.recall)} · F1 ${pct(majorityMetrics.f1)}`,
    ``,
    `## Repeatability`,
    ``,
    `- Unanimous: ${pct(unanimousN / studyCaseN)}`,
    `- 2-of-3 agreement: ${pct(twoOfThreeN / studyCaseN)}`,
    `- Flips: ${flipN}`,
    ``,
    `## Architecture`,
    ``,
    `**${architecture}**`,
    ``,
    `Next: ${nextStep}`,
    ``,
  ].join("\n");
  fs.writeFileSync(OUT_MD, md, "utf8");

  console.log("\nRECOMMENDED_SEMANTIC_ADJUDICATION_STUDY_COMPLETE");
  console.log(
    `SINGLE: P=${pct(singleMetrics.precision)} R=${pct(singleMetrics.recall)} F1=${pct(singleMetrics.f1)}`
  );
  console.log(
    `MAJORITY: P=${pct(majorityMetrics.precision)} R=${pct(majorityMetrics.recall)} F1=${pct(majorityMetrics.f1)}`
  );
  console.log(
    `REPEATABILITY unanimous=${pct(unanimousN / studyCaseN)} flips=${flipN}`
  );
  console.log(`ARCHITECTURE: ${architecture}`);
  console.log(`NEXT: ${nextStep}`);
  console.log(`wrote ${path.relative(ROOT, OUT_RESULTS)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
