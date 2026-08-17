#!/usr/bin/env node
/**
 * Clean DEV + Hardening 4 resume evaluation (DEV only).
 * HOLDOUT untouched. No provider calls.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet, scoreGoldenSetHydrated } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { RECOMMENDATION_CLASSIFIER_VERSION } from "../lib/ai-visibility/recommendation-classifier-v3.js";
import { V2_PATH } from "../lib/ai-visibility/validation/golden-set-human-review.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BASELINE = path.join(
  __dirname,
  "../data/ai-visibility/validation/clean-dev-ground-truth-baseline-hardening-4.json"
);
const OUT = path.join(
  __dirname,
  "../data/ai-visibility/validation/clean-dev-hardening-4-resume-result.json"
);

const ROLES = [
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "negative_or_qualified",
  "source_only",
  "no_mention",
];

function prf(tp, fp, fn) {
  const p = tp + fp ? tp / (tp + fp) : null;
  const r = tp + fn ? tp / (tp + fn) : null;
  const f1 = p != null && r != null && p + r ? (2 * p * r) / (p + r) : null;
  return { precision: p, recall: r, f1, tp, fp, fn };
}

function macroAvg(classMetrics) {
  const vals = Object.values(classMetrics).filter((x) => x.tp + x.fp + x.fn > 0);
  const avg = (a) => (a.length ? a.reduce((s, x) => s + x, 0) / a.length : null);
  return {
    MACRO_P: avg(vals.map((v) => v.precision).filter((x) => x != null)),
    MACRO_R: avg(vals.map((v) => v.recall).filter((x) => x != null)),
    MACRO_F1: avg(vals.map((v) => v.f1).filter((x) => x != null)),
  };
}

const baseline = JSON.parse(fs.readFileSync(BASELINE, "utf8"));
const v2 = JSON.parse(fs.readFileSync(V2_PATH, "utf8"));
const amendedTaxonomy = (v2.cases || []).filter((c) =>
  (c.labelAmendmentHistory || []).some((h) =>
    String(h.AMENDMENT_REASON || "").includes("Taxonomy review")
  )
).length;
const invalidated = (v2.cases || []).filter(
  (c) => c.groundTruthInvalidated || c.excludeFromClassificationDenominator
).length;
const deferred = (v2.cases || []).filter(
  (c) => c.taxonomyReviewDeferred || c.excludeFromRecommendationTuning
).length;
const holdoutN = (v2.cases || []).filter((c) => c.holdoutSplit === "holdout").length;

const golden = loadGoldenSet();
const score = await scoreGoldenSetHydrated(golden, { holdoutPolicy: "exclude" });
if (score.HOLDOUT_ACCESSED || score.HOLDOUT_METRICS_RUN) {
  console.error("BLOCKED: holdout accessed");
  process.exit(2);
}

const index = buildGoldenSetScoringEntityIndex({});
const roleRank = new Map(ROLES.map((r, i) => [r, i]));
const { cases: hydrated } = await hydrateGoldenSetCasesForScoring(
  (golden.cases || []).filter((c) => c.holdoutSplit !== "holdout"),
  {}
);

const cm = Object.fromEntries(ROLES.map((r) => [r, { tp: 0, fp: 0, fn: 0 }]));
const matrix = Object.fromEntries(
  ROLES.map((r) => [r, Object.fromEntries(ROLES.map((c) => [c, 0]))])
);
const subgroups = { PROVIDER: {}, LANGUAGE: {}, GEOGRAPHY: {} };
function ens(dim, key) {
  const k = key || "unspecified";
  if (!subgroups[dim][k]) subgroups[dim][k] = { n: 0, ok: 0 };
  return subgroups[dim][k];
}

const inventory = [];
const beforeErrors = new Set(
  (baseline.trueClassifierErrors?.ERRORS || []).map(
    (e) => `${e.CASE_ID}|${e.HUMAN_LABEL}|${e.CLASSIFIER_LABEL}`
  )
);
const afterKeys = new Set();

for (const c of hydrated) {
  if (c.expectedRecommendationRole == null) continue;
  const mentions = extractMentions({
    responseId: "x",
    text: c.text || "",
    entityIndex: index.aliasIndex,
    promptIntentTerritory: c.promptIntentTerritory || c.promptFamily,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
  let role = null;
  let reason = null;
  if (hits.length) {
    const best = hits
      .slice()
      .sort(
        (a, b) =>
          (roleRank.get(a.role) ?? 99) - (roleRank.get(b.role) ?? 99) ||
          a.mentionPosition - b.mentionPosition
      )[0];
    role = best.role;
    reason = best.classificationReason;
  }
  const exp = c.expectedRecommendationRole;
  const got = role || "no_mention";
  if (!matrix[exp]) matrix[exp] = Object.fromEntries(ROLES.map((r) => [r, 0]));
  if (matrix[exp][got] == null) matrix[exp][got] = 0;
  matrix[exp][got] += 1;

  if (exp === role) cm[exp].tp += 1;
  else {
    if (cm[exp]) cm[exp].fn += 1;
    if (role && cm[role]) cm[role].fp += 1;
    const key = `${c.caseId}|${exp}|${role}`;
    afterKeys.add(key);
    inventory.push({
      CASE_ID: c.caseId,
      ENTITY: c.entityName,
      PROVIDER: c.provider || null,
      LANGUAGE: c.language || null,
      GEOGRAPHY: c.geography || null,
      PROMPT_FAMILY: c.promptFamily || c.promptIntentTerritory || null,
      HUMAN_LABEL: exp,
      CLASSIFIER_LABEL: role,
      REASON: reason,
      ROOT_CAUSE: `${exp} => ${role}`,
    });
  }

  for (const [dim, key] of [
    ["PROVIDER", c.provider],
    ["LANGUAGE", c.language],
    ["GEOGRAPHY", c.geography],
  ]) {
    const row = ens(dim, key);
    row.n += 1;
    if (exp === role) row.ok += 1;
  }
}

const classMetrics = Object.fromEntries(
  ROLES.map((r) => [r, prf(cm[r].tp, cm[r].fp, cm[r].fn)])
);
const macro = macroAvg(classMetrics);

const byPair = {};
const byClass = {};
const byReason = {};
for (const e of inventory) {
  byPair[e.ROOT_CAUSE] = (byPair[e.ROOT_CAUSE] || 0) + 1;
  byClass[e.HUMAN_LABEL] = (byClass[e.HUMAN_LABEL] || 0) + 1;
  byReason[e.REASON || "unknown"] = (byReason[e.REASON || "unknown"] || 0) + 1;
}

const fixed = [...beforeErrors].filter((k) => {
  const [id, human] = k.split("|");
  return !inventory.some((e) => e.CASE_ID === id && e.HUMAN_LABEL === human);
}).length;
const newErrors = inventory.filter((e) => {
  const prefix = `${e.CASE_ID}|${e.HUMAN_LABEL}|`;
  return ![...beforeErrors].some((k) => k.startsWith(prefix));
}).length;

const recGate =
  score.RECOMMENDATION_CLASSIFICATION_ACCURACY >= 0.98 &&
  score.RECOMMENDATION_PRECISION >= 0.98 &&
  score.RECOMMENDATION_RECALL >= 0.98;
const firstGate = score.FIRST_RECOMMENDATION_ACCURACY >= 0.98;
const entityGate = score.ENTITY_RESOLUTION_F1 >= 0.999;

const result = {
  version: "clean_dev_hardening_4_resume_v1",
  generatedAt: new Date().toISOString(),
  CLASSIFIER_VERSION_AFTER: RECOMMENDATION_CLASSIFIER_VERSION,
  cleanDev: {
    DEV_N: score.CASE_COUNT,
    GROUND_TRUTH_AMENDMENTS: amendedTaxonomy,
    INVALIDATED_EXCLUDED: invalidated,
    DEFERRED_EXCLUDED: deferred,
    HOLDOUT_N: holdoutN,
  },
  groundTruthEffectOnly: {
    CLASSIFIER_VERSION_BEFORE: baseline.CLASSIFIER_VERSION_BEFORE,
    ...baseline.metrics,
    CLASSIFIER_LOGIC_CHANGE: 0,
  },
  trueClassifierErrorsBeforeHardening: {
    TOTAL: baseline.trueClassifierErrors?.TOTAL,
    BY_PAIR: baseline.trueClassifierErrors?.BY_PAIR,
  },
  before: {
    ACCURACY: baseline.metrics.ACCURACY,
    PRECISION: baseline.metrics.PRECISION,
    RECALL: baseline.metrics.RECALL,
    F1: baseline.metrics.F1,
    MACRO_P: baseline.metrics.MACRO_P,
    MACRO_R: baseline.metrics.MACRO_R,
    MACRO_F1: baseline.metrics.MACRO_F1,
    FIRST_REC: baseline.metrics.FIRST_REC,
    QUESTION_STATUS: baseline.metrics.QUESTION_STATUS,
  },
  after: {
    ACCURACY: score.RECOMMENDATION_CLASSIFICATION_ACCURACY,
    PRECISION: score.RECOMMENDATION_PRECISION,
    RECALL: score.RECOMMENDATION_RECALL,
    F1: score.RECOMMENDATION_F1,
    ...macro,
    FIRST_REC: score.FIRST_RECOMMENDATION_ACCURACY,
    QUESTION_STATUS: score.QUESTION_STATUS_ACCURACY,
    ENTITY_F1: score.ENTITY_RESOLUTION_F1,
  },
  FIXED_ERRORS: fixed,
  NEW_ERRORS: newErrors,
  classMetrics,
  confusionMatrix: matrix,
  subgroups: Object.fromEntries(
    Object.entries(subgroups).map(([dim, rows]) => [
      dim,
      Object.fromEntries(
        Object.entries(rows).map(([k, v]) => [
          k,
          { CASE_COUNT: v.n, ACCURACY: v.n ? v.ok / v.n : null },
        ])
      ),
    ])
  ),
  remainingErrors: {
    TOTAL: inventory.length,
    BY_PAIR: Object.entries(byPair)
      .sort((a, b) => b[1] - a[1])
      .map(([pattern, count]) => ({ pattern, count })),
    BY_PATTERN: Object.entries(byReason)
      .sort((a, b) => b[1] - a[1])
      .map(([pattern, count]) => ({ pattern, count })),
    BY_CLASS: byClass,
    ERRORS: inventory,
  },
  gates: {
    ENTITY_GATE: entityGate ? "PASS" : "FAIL",
    RECOMMENDATION_GATE: recGate ? "PASS" : "FAIL",
    FIRST_REC_GATE: firstGate ? "PASS" : "FAIL",
    CLASS_BALANCE_STATUS: "REVIEW",
    QUESTION_STATUS_STATUS:
      score.QUESTION_STATUS_ACCURACY >= 0.98 ? "PASS" : "BELOW_TARGET",
  },
  HOLDOUT_ACCESSED: false,
  HOLDOUT_CASES_INSPECTED: 0,
  HOLDOUT_METRICS_RUN: false,
  NEXT_STEP:
    recGate && firstGate && entityGate
      ? "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION"
      : "MORE_DEVELOPMENT_HARDENING_REQUIRED",
};

fs.writeFileSync(OUT, JSON.stringify(result, null, 2));
console.log(
  JSON.stringify(
    {
      OUT,
      before: result.before,
      after: result.after,
      FIXED_ERRORS: fixed,
      NEW_ERRORS: newErrors,
      remaining: inventory.length,
      TOP_PAIRS: result.remainingErrors.BY_PAIR.slice(0, 12),
      gates: result.gates,
      NEXT_STEP: result.NEXT_STEP,
    },
    null,
    2
  )
);
