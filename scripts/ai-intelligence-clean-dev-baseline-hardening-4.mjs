#!/usr/bin/env node
/**
 * Clean DEV baseline (Part B) — classifier v3.2, no logic changes.
 * HOLDOUT untouched. CLASSIFIER_LOGIC_CHANGE = 0.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet, scoreGoldenSetHydrated } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import {
  RECOMMENDATION_CLASSIFIER_VERSION,
  detectRankMarker,
  detectBulletLine,
  detectOrderedListContext,
  detectResponseSections,
  sectionAt,
  isDocumentTopicHeading,
} from "../lib/ai-visibility/recommendation-classifier-v3.js";
import { V2_PATH } from "../lib/ai-visibility/validation/golden-set-human-review.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../data/ai-visibility/validation/clean-dev-ground-truth-baseline-hardening-4.json"
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

const POS =
  /\b(recommend(?:ed)?|strong\s+(?:option|choice|options|choices)|best\s+fit|should\s+consider|good\s+(?:option|fit)|recomendad|opci[oó]n\s+(?:fuerte|s[oó]lida)|mejor\s+encaje|buen\s+encaje|issue\s+an\s+rfp)\b/i;
const CONS =
  /\b(options?\s+include|brands?\s+typically|consideration\s+set|se\s+consideran|opciones\s+incluyen|commonly\s+cited|marcas?\s+a\s+considerar|shortlist)\b/i;
const COMP = /\b(versus|vs\.?|compared\s+to|alternative\s+to|frente\s+a)\b/i;
const NEG = /\b(not\s+recommend|avoid|poor\s+fit|no\s+recomend|evitar)\b/i;

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
  if (hits.length) {
    role = hits
      .slice()
      .sort(
        (a, b) =>
          (roleRank.get(a.role) ?? 99) - (roleRank.get(b.role) ?? 99) ||
          a.mentionPosition - b.mentionPosition
      )[0].role;
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

    const text = c.text || "";
    const name = c.entityName || "";
    const idx = name ? text.toLowerCase().indexOf(name.toLowerCase()) : 0;
    const s = idx >= 0 ? idx : 0;
    const end = s + name.length;
    const lineStart = text.lastIndexOf("\n", s - 1) + 1;
    const lineEnd = text.indexOf("\n", s);
    const line = text.slice(lineStart, lineEnd === -1 ? text.length : lineEnd);
    const behind = text.slice(Math.max(0, s - 200), s);
    const after = text.slice(s, Math.min(text.length, end + 120));
    const sec = sectionAt(text, s, detectResponseSections(text));
    const rank = detectRankMarker(text, s);
    inventory.push({
      CASE_ID: c.caseId,
      ENTITY: name,
      PROVIDER: c.provider || null,
      LANGUAGE: c.language || null,
      GEOGRAPHY: c.geography || null,
      PROMPT_FAMILY: c.promptFamily || c.promptIntentTerritory || null,
      HUMAN_LABEL: exp,
      CLASSIFIER_LABEL: role,
      LINE: line.slice(0, 220),
      LOCAL_CONTEXT: text.slice(Math.max(0, s - 220), Math.min(text.length, end + 180)).slice(0, 500),
      STRUCTURAL_CONTEXT: {
        parentHeading: sec?.title || null,
        orderedList: detectOrderedListContext(text, s),
        bullet: detectBulletLine(text, s),
        listPosition: rank,
        tableRank: rank != null && /\|/.test(line),
        recommendationHeading:
          /\b(shortlist|brands?\s+to\s+consider|recommended)\b/i.test(sec?.title || "") ||
          /\b(shortlist|brands?\s+to\s+consider)\b/i.test(behind),
        directPositive: POS.test(line) || POS.test(after),
        considerationSet: CONS.test(behind) || CONS.test(sec?.title || ""),
        comparator: COMP.test(behind.slice(-80)),
        negative: NEG.test(line) || NEG.test(after),
        neutralDescription: !(
          POS.test(line) ||
          POS.test(after) ||
          CONS.test(behind) ||
          COMP.test(behind.slice(-80)) ||
          NEG.test(line)
        ),
        topicSectionNumber: isDocumentTopicHeading(line),
      },
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
const byProv = {};
const byLang = {};
const byGeo = {};
const byFam = {};
for (const e of inventory) {
  const pair = `${e.HUMAN_LABEL} => ${e.CLASSIFIER_LABEL}`;
  byPair[pair] = (byPair[pair] || 0) + 1;
  byClass[e.HUMAN_LABEL] = (byClass[e.HUMAN_LABEL] || 0) + 1;
  byProv[e.PROVIDER || "unspecified"] = (byProv[e.PROVIDER || "unspecified"] || 0) + 1;
  byLang[e.LANGUAGE || "unspecified"] = (byLang[e.LANGUAGE || "unspecified"] || 0) + 1;
  byGeo[e.GEOGRAPHY || "unspecified"] = (byGeo[e.GEOGRAPHY || "unspecified"] || 0) + 1;
  byFam[e.PROMPT_FAMILY || "unspecified"] =
    (byFam[e.PROMPT_FAMILY || "unspecified"] || 0) + 1;
}

const baseline = {
  version: "clean_dev_ground_truth_baseline_v1",
  generatedAt: new Date().toISOString(),
  CLASSIFIER_VERSION_BEFORE: RECOMMENDATION_CLASSIFIER_VERSION,
  CLASSIFIER_LOGIC_CHANGE: 0,
  GROUND_TRUTH_EFFECT_ONLY: true,
  PREVIOUS_DIRTY_DEV_ACCURACY: 0.7448,
  cleanDev: {
    DEV_N_BEFORE: 290,
    DEV_N_AFTER: score.CASE_COUNT,
    GROUND_TRUTH_AMENDMENTS: amendedTaxonomy,
    INVALIDATED_EXCLUDED: invalidated,
    DEFERRED_EXCLUDED: deferred,
    HOLDOUT_N: holdoutN,
  },
  metrics: {
    ACCURACY: score.RECOMMENDATION_CLASSIFICATION_ACCURACY,
    PRECISION: score.RECOMMENDATION_PRECISION,
    RECALL: score.RECOMMENDATION_RECALL,
    F1: score.RECOMMENDATION_F1,
    ...macro,
    FIRST_REC: score.FIRST_RECOMMENDATION_ACCURACY,
    QUESTION_STATUS: score.QUESTION_STATUS_ACCURACY,
    ENTITY_F1: score.ENTITY_RESOLUTION_F1,
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
  trueClassifierErrors: {
    TOTAL: inventory.length,
    BY_PAIR: Object.entries(byPair)
      .sort((a, b) => b[1] - a[1])
      .map(([pattern, count]) => ({ pattern, count })),
    BY_CLASS: byClass,
    BY_PROVIDER: byProv,
    BY_LANGUAGE: byLang,
    BY_GEOGRAPHY: byGeo,
    BY_PROMPT_FAMILY: byFam,
    ERRORS: inventory,
  },
  HOLDOUT_ACCESSED: false,
  HOLDOUT_CASES_INSPECTED: 0,
  HOLDOUT_METRICS_RUN: false,
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(baseline, null, 2));
console.log(
  JSON.stringify(
    {
      OUT,
      cleanDev: baseline.cleanDev,
      metrics: baseline.metrics,
      TOP_PAIRS: baseline.trueClassifierErrors.BY_PAIR.slice(0, 15),
      TOTAL_TRUE_CLASSIFIER_ERRORS: inventory.length,
    },
    null,
    2
  )
);
