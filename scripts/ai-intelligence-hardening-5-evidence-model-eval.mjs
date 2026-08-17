#!/usr/bin/env node
/**
 * Hardening 5 — evidence-model DEV eval + prior-error audit.
 * HOLDOUT untouched. No provider calls.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet, scoreGoldenSetHydrated } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { findEntitySpans } from "../lib/ai-visibility/normalize-entities.js";
import { detectResponseSections } from "../lib/ai-visibility/recommendation-classifier-v3.js";
import {
  extractEntityLocalEvidence,
  aggregateEntityEvidence,
} from "../lib/ai-visibility/recommendation-evidence-v4.js";
import {
  decideRecommendationRoleFromEvidence,
  RECOMMENDATION_CLASSIFIER_VERSION,
} from "../lib/ai-visibility/recommendation-classifier-v4.js";
import { RECOMMENDATION_EVIDENCE_VERSION } from "../lib/ai-visibility/recommendation-evidence-v4.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PRIOR = path.join(
  __dirname,
  "../data/ai-visibility/validation/clean-dev-hardening-4-resume-result.json"
);
const OUT = path.join(
  __dirname,
  "../data/ai-visibility/validation/hardening-5-evidence-model-result.json"
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

const prior = JSON.parse(fs.readFileSync(PRIOR, "utf8"));
const before = prior.after;
const beforeErrors = prior.remainingErrors?.ERRORS || [];

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
const promotionAudit = {
  "discussed => associated_option": [],
  "discussed => explicit_recommendation": [],
  "discussed => ranked_recommendation": [],
  "discussed => first_recommendation": [],
  "discussed => comparator": [],
  "explicit_recommendation => first_recommendation": [],
};

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
    inventory.push({
      CASE_ID: c.caseId,
      ENTITY: c.entityName,
      HUMAN_LABEL: exp,
      CLASSIFIER_LABEL: role,
      REASON: reason,
      ROOT_CAUSE: `${exp} => ${role}`,
      PROVIDER: c.provider || null,
      LANGUAGE: c.language || null,
      GEOGRAPHY: c.geography || null,
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
const byReason = {};
for (const e of inventory) {
  byPair[e.ROOT_CAUSE] = (byPair[e.ROOT_CAUSE] || 0) + 1;
  byReason[e.REASON || "unknown"] = (byReason[e.REASON || "unknown"] || 0) + 1;
}

// Audit prior 48 errors through new evidence model
const priorIds = new Set(beforeErrors.map((e) => e.CASE_ID));
const priorAudit = [];
const triggerCounts = {
  "discussed => associated_option": {},
  "discussed => explicit_recommendation": {},
  "discussed => ranked_recommendation": {},
  "discussed => first_recommendation": {},
  "discussed => comparator": {},
  "explicit_recommendation => first_recommendation": {},
};

for (const pe of beforeErrors) {
  const c = hydrated.find((x) => x.caseId === pe.CASE_ID);
  if (!c) continue;
  const sections = detectResponseSections(c.text || "");
  const spans = findEntitySpans(c.text || "", index.aliasIndex).filter(
    (s) => s.entity.name === c.entityName
  );
  const mentionEv = spans.map((s) =>
    extractEntityLocalEvidence({
      text: c.text,
      start: s.start,
      end: s.end,
      rawMention: s.rawMention,
      canonicalEntityName: c.entityName,
      sections,
    })
  );
  const agg = aggregateEntityEvidence(mentionEv);
  const decided = decideRecommendationRoleFromEvidence(agg, { entityPresent: spans.length > 0 });
  const oldPair = `${pe.HUMAN_LABEL} => ${pe.CLASSIFIER_LABEL}`;
  const oldTrigger = pe.REASON || pe.ROOT_CAUSE || "unknown";
  if (triggerCounts[oldPair]) {
    triggerCounts[oldPair][oldTrigger] = (triggerCounts[oldPair][oldTrigger] || 0) + 1;
  }
  let classification = "GROUND_TRUTH_CONSISTENT";
  if (decided.role === pe.HUMAN_LABEL) classification = "GROUND_TRUTH_CONSISTENT";
  else if (decided.role !== pe.CLASSIFIER_LABEL && decided.role !== pe.HUMAN_LABEL) {
    classification = "ROLE_DECISION_DEFECT";
  } else if (decided.role === pe.CLASSIFIER_LABEL) {
    classification = "EVIDENCE_EXTRACTION_DEFECT";
  }
  if (
    pe.HUMAN_LABEL === "discussed" &&
    decided.role !== "discussed" &&
    ["associated_option", "explicit_recommendation", "ranked_recommendation", "first_recommendation"].includes(
      decided.role
    )
  ) {
    // still over-promoting
  }
  priorAudit.push({
    CASE_ID: pe.CASE_ID,
    ENTITY: pe.ENTITY,
    HUMAN_ROLE: pe.HUMAN_LABEL,
    V3_3_ROLE: pe.CLASSIFIER_LABEL,
    NEW_ROLE: decided.role,
    NEW_REASON: decided.reason,
    OLD_TRIGGER: oldTrigger,
    NEW_EVIDENCE_SCOPE: agg?.evidenceScope || mentionEv[0]?.evidenceScope || null,
    EXPECTED_ROLE_FROM_NEW_TREE: decided.role,
    CLASSIFICATION: classification,
    recommendationEvidence: agg?.recommendationEvidence || null,
  });
  if (promotionAudit[oldPair]) {
    promotionAudit[oldPair].push({
      CASE_ID: pe.CASE_ID,
      OLD_TRIGGER: oldTrigger,
      NEW_RESULT: decided.role,
      NEW_EVIDENCE_SCOPE: agg?.evidenceScope || null,
    });
  }
}

const beforeKeys = new Set(beforeErrors.map((e) => `${e.CASE_ID}|${e.HUMAN_LABEL}`));
const afterKeys = new Set(inventory.map((e) => `${e.CASE_ID}|${e.HUMAN_LABEL}`));
const fixed = [...beforeKeys].filter((k) => !afterKeys.has(k)).length;
const newErrors = inventory.filter((e) => !beforeKeys.has(`${e.CASE_ID}|${e.HUMAN_LABEL}`)).length;

const recGate =
  score.RECOMMENDATION_CLASSIFICATION_ACCURACY >= 0.98 &&
  score.RECOMMENDATION_PRECISION >= 0.98 &&
  score.RECOMMENDATION_RECALL >= 0.98;
const firstGate = score.FIRST_RECOMMENDATION_ACCURACY >= 0.98;

const result = {
  version: "hardening_5_evidence_model_v1",
  generatedAt: new Date().toISOString(),
  architecture: {
    OLD_CLASSIFIER: "ai_visibility_recommendation_classifier_v3_3",
    NEW_CLASSIFIER: RECOMMENDATION_CLASSIFIER_VERSION,
    EVIDENCE_EXTRACTION_LAYER: RECOMMENDATION_EVIDENCE_VERSION,
    ROLE_DECISION_LAYER: "decideRecommendationRoleFromEvidence",
  },
  before,
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
  oldFalsePromotionTriggerCounts: Object.fromEntries(
    Object.entries(triggerCounts).map(([pair, counts]) => [
      pair,
      Object.entries(counts)
        .sort((a, b) => b[1] - a[1])
        .map(([trigger, count]) => ({ trigger, count })),
    ])
  ),
  priorErrorAudit: {
    TOTAL: priorAudit.length,
    BY_CLASSIFICATION: priorAudit.reduce((acc, row) => {
      acc[row.CLASSIFICATION] = (acc[row.CLASSIFICATION] || 0) + 1;
      return acc;
    }, {}),
    CASES: priorAudit,
  },
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
    ERRORS: inventory,
  },
  gates: {
    ENTITY_GATE: score.ENTITY_RESOLUTION_F1 >= 0.999 ? "PASS" : "FAIL",
    RECOMMENDATION_GATE: recGate ? "PASS" : "FAIL",
    FIRST_REC_GATE: firstGate ? "PASS" : "FAIL",
    CLASS_BALANCE_STATUS: "REVIEW",
    QUESTION_STATUS_STATUS:
      score.QUESTION_STATUS_ACCURACY >= 0.98 ? "PASS" : "BELOW_TARGET",
  },
  HOLDOUT_ACCESSED: false,
  HOLDOUT_CASES_INSPECTED: 0,
  HOLDOUT_METRICS_RUN: false,
  NEXT_STEP: recGate && firstGate ? "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION" : "MORE_DEVELOPMENT_HARDENING_REQUIRED",
};

fs.writeFileSync(OUT, JSON.stringify(result, null, 2));
console.log(
  JSON.stringify(
    {
      OUT,
      architecture: result.architecture,
      before: result.before,
      after: result.after,
      FIXED_ERRORS: fixed,
      NEW_ERRORS: newErrors,
      remaining: inventory.length,
      TOP_PAIRS: result.remainingErrors.BY_PAIR.slice(0, 10),
      priorAuditClasses: result.priorErrorAudit.BY_CLASSIFICATION,
      oldTriggers: result.oldFalsePromotionTriggerCounts,
      gates: result.gates,
      NEXT_STEP: result.NEXT_STEP,
    },
    null,
    2
  )
);
