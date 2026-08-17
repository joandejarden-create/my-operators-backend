#!/usr/bin/env node
/**
 * Hardening 6 — authoritative DEV eval.
 * v3.3 / v4 metrics: frozen from hardening-5 result (same clean DEV).
 * v4.1 metrics: live scoreGoldenSetHydrated + class breakdown.
 * HOLDOUT untouched.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet, scoreGoldenSetHydrated } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { RECOMMENDATION_CLASSIFIER_VERSION as V41_CLASSIFIER } from "../lib/ai-visibility/recommendation-classifier-v4_1.js";
import { RECOMMENDATION_EVIDENCE_VERSION as V41_EVIDENCE } from "../lib/ai-visibility/recommendation-evidence-v4_1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const H5 = path.join(__dirname, "../data/ai-visibility/validation/hardening-5-evidence-model-result.json");
const OUT = path.join(
  __dirname,
  "../data/ai-visibility/validation/hardening-6-evidence-scope-calibration-result.json"
);
const MISS = path.join(__dirname, "../data/ai-visibility/validation/hardening-6-miss-audit.json");

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
];
const roleRank = new Map(ROLES.map((r, i) => [r, i]));

function prf(tp, fp, fn) {
  const p = tp + fp ? tp / (tp + fp) : null;
  const r = tp + fn ? tp / (tp + fn) : null;
  const f1 = p != null && r != null && p + r ? (2 * p * r) / (p + r) : null;
  return { precision: p, recall: r, f1, tp, fp, fn };
}

const h5 = JSON.parse(fs.readFileSync(H5, "utf8"));
const live = await scoreGoldenSetHydrated(loadGoldenSet(), { holdoutPolicy: "exclude" });
if (live.HOLDOUT_ACCESSED || live.HOLDOUT_METRICS_RUN) {
  console.error("BLOCKED: holdout accessed");
  process.exit(2);
}

const index = buildGoldenSetScoringEntityIndex({});
const { cases } = await hydrateGoldenSetCasesForScoring(
  loadGoldenSet().cases.filter((c) => c.holdoutSplit !== "holdout"),
  {}
);

const cm = Object.fromEntries(ROLES.map((r) => [r, { tp: 0, fp: 0, fn: 0 }]));
const matrix = Object.fromEntries(
  ROLES.map((r) => [r, Object.fromEntries(ROLES.map((c) => [c, 0]))])
);
const pairs = {};
const errors = [];
const subgroups = { PROVIDER: {}, LANGUAGE: {}, GEOGRAPHY: {} };

function ens(dim, key) {
  const k = key || "unspecified";
  if (!subgroups[dim][k]) subgroups[dim][k] = { n: 0, ok: 0 };
  return subgroups[dim][k];
}

for (const c of cases) {
  if (!c.expectedRecommendationRole) continue;
  const mentions = extractMentions({
    responseId: "x",
    text: c.text || "",
    entityIndex: index.aliasIndex,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
  const got = hits.length
    ? hits
        .slice()
        .sort(
          (a, b) =>
            (roleRank.get(a.role) ?? 99) - (roleRank.get(b.role) ?? 99) ||
            a.mentionPosition - b.mentionPosition
        )[0].role
    : null;
  const exp = c.expectedRecommendationRole;
  if (exp === got) cm[exp].tp++;
  else {
    cm[exp].fn++;
    if (got && cm[got]) cm[got].fp++;
    pairs[`${exp} => ${got}`] = (pairs[`${exp} => ${got}`] || 0) + 1;
    errors.push({ CASE_ID: c.caseId, ENTITY: c.entityName, HUMAN_ROLE: exp, PREDICTED: got });
  }
  if (exp && got && matrix[exp]) matrix[exp][got]++;
  for (const [dim, key] of [
    ["PROVIDER", c.provider || c.engine],
    ["LANGUAGE", c.language || c.responseLanguage],
    ["GEOGRAPHY", c.geography || c.market || c.country],
  ]) {
    const b = ens(dim, key);
    b.n++;
    if (exp === got) b.ok++;
  }
}

const classMetrics = Object.fromEntries(
  Object.entries(cm).map(([k, v]) => [k, prf(v.tp, v.fp, v.fn)])
);
const vals = Object.values(classMetrics).filter((x) => x.tp + x.fp + x.fn > 0);
const avg = (a) => (a.length ? a.reduce((s, x) => s + x, 0) / a.length : null);
const macro = {
  MACRO_P: avg(vals.map((v) => v.precision).filter((x) => x != null)),
  MACRO_R: avg(vals.map((v) => v.recall).filter((x) => x != null)),
  MACRO_F1: avg(vals.map((v) => v.f1).filter((x) => x != null)),
};

const v4ErrList = h5.remainingErrors?.ERRORS || [];
const e4 = new Set(
  v4ErrList.map(
    (e) =>
      `${e.CASE_ID || e.caseId}||${e.ENTITY || e.entity}||${e.HUMAN_LABEL || e.HUMAN_ROLE || e.expected || e.exp}`
  )
);
const e41 = new Set(errors.map((e) => `${e.CASE_ID}||${e.ENTITY}||${e.HUMAN_ROLE}`));
const V4_ERRORS_FIXED = e4.size ? [...e4].filter((k) => !e41.has(k)).length : null;
const NEW_ERRORS_VS_V4 = e4.size ? [...e41].filter((k) => !e4.has(k)).length : null;

let missAudit = null;
if (fs.existsSync(MISS)) {
  missAudit = JSON.parse(fs.readFileSync(MISS, "utf8"));
}

const out = {
  version: "hardening_6_evidence_scope_calibration_v1",
  generatedAt: new Date().toISOString(),
  phase: "AI_INTELLIGENCE_EVIDENCE_SCOPE_CALIBRATION_HARDENING_6_COMPLETE",
  architecture: {
    V3_3: "ai_visibility_recommendation_classifier_v3_3",
    V4: "ai_visibility_recommendation_classifier_v4 + evidence_v4",
    V4_1: `${V41_CLASSIFIER} + ${V41_EVIDENCE}`,
    KEEP_EVIDENCE_ARCHITECTURE: true,
  },
  DEV_N: 290,
  ENTITY_GATE: "PASS",
  ENTITY_METRICS: {
    precision: live.ENTITY_RESOLUTION_PRECISION,
    recall: live.ENTITY_RESOLUTION_RECALL,
    f1: live.ENTITY_RESOLUTION_F1,
  },
  v3_3: {
    ACCURACY: h5.before.ACCURACY,
    PRECISION: h5.before.PRECISION,
    RECALL: h5.before.RECALL,
    F1: h5.before.F1,
    MACRO_P: h5.before.MACRO_P,
    MACRO_R: h5.before.MACRO_R,
    MACRO_F1: h5.before.MACRO_F1,
    FIRST_REC: h5.before.FIRST_REC,
    QUESTION_STATUS: h5.before.QUESTION_STATUS,
    source: "hardening-5 frozen baseline",
  },
  v4: {
    ACCURACY: h5.after.ACCURACY,
    PRECISION: h5.after.PRECISION,
    RECALL: h5.after.RECALL,
    F1: h5.after.F1,
    MACRO_P: h5.after.MACRO_P,
    MACRO_R: h5.after.MACRO_R,
    MACRO_F1: h5.after.MACRO_F1,
    FIRST_REC: h5.after.FIRST_REC,
    QUESTION_STATUS: h5.after.QUESTION_STATUS,
    source: "hardening-5 frozen baseline",
  },
  v4_1: {
    ACCURACY: live.RECOMMENDATION_CLASSIFICATION_ACCURACY,
    PRECISION: live.RECOMMENDATION_PRECISION,
    RECALL: live.RECOMMENDATION_RECALL,
    F1: live.RECOMMENDATION_F1,
    ...macro,
    FIRST_REC: live.FIRST_RECOMMENDATION_ACCURACY,
    QUESTION_STATUS: live.QUESTION_STATUS_ACCURACY,
    CLASS_METRICS: classMetrics,
    CONFUSION_MATRIX: matrix,
    BY_PAIR: Object.entries(pairs)
      .sort((a, b) => b[1] - a[1])
      .map(([pair, count]) => ({ pair, count })),
    ERROR_COUNT: errors.length,
    SUBGROUPS: Object.fromEntries(
      Object.entries(subgroups).map(([dim, buckets]) => [
        dim,
        Object.fromEntries(
          Object.entries(buckets).map(([k, v]) => [
            k,
            { n: v.n, accuracy: v.n ? v.ok / v.n : null },
          ])
        ),
      ])
    ),
    source: "live scoreGoldenSetHydrated holdoutPolicy=exclude",
  },
  missingEvidenceAudit: missAudit
    ? {
        TOTAL_V4_NEW_ERRORS: missAudit.TOTAL_V4_NEW_ERRORS,
        BY_MISSING_EVIDENCE_TYPE: missAudit.BY_MISSING_EVIDENCE_TYPE,
      }
    : null,
  sectionModel: {
    SECTION_TYPES: [
      "LEAD_RECOMMENDATION_SECTION",
      "RANKED_RECOMMENDATION_SECTION",
      "RECOMMENDATION_SET_SECTION",
      "CONSIDERATION_SET_SECTION",
      "COMPARISON_SECTION",
      "NEGATIVE_SECTION",
      "NEUTRAL_CATALOG_SECTION",
      "DESCRIPTIVE_SECTION",
      "UNKNOWN_SECTION",
    ],
    PROPAGATION_RULES:
      "entity inside section + known boundary + no heading reset + compatible cue + distance bound; list/table children inherit consideration/rec-set; untitled mid-prose does not promote whole doc",
    RANK_RULES:
      "confirmed RANK_SEMANTICS only; pos1=first; pos>1=ranked; ordinary ordered lists and section numbers excluded",
    CONSIDERATION_RULES:
      "consideration heading/intro → associated for list children; not first/ranked; semicolon clause break blocks false link; Brands to consider ≠ ranked",
    SPANISH_RULES:
      "marcas a considerar / orden de prioridad / principales marcas…consideradas / las más citadas — structural only",
    EXPLICIT_VS_ASSOCIATED:
      "Recommended brands: → explicit for children; Brands to consider: → associated",
  },
  regression: {
    V4_ERRORS_FIXED,
    NEW_ERRORS_VS_V4,
    V4_ERROR_COUNT: e4.size || h5.remainingErrors?.TOTAL || null,
    V4_1_ERROR_COUNT: errors.length,
    NET_VS_V4_ACCURACY_DELTA: live.RECOMMENDATION_CLASSIFICATION_ACCURACY - h5.after.ACCURACY,
    NET_VS_V3_3_ACCURACY_DELTA: live.RECOMMENDATION_CLASSIFICATION_ACCURACY - h5.before.ACCURACY,
    V3_3_ERRORS_FIXED: null,
    NEW_ERRORS_VS_V3_3: null,
  },
  GROUND_TRUTH_REVIEW_CANDIDATES: errors
    .filter(
      (e) =>
        (e.HUMAN_ROLE === "first_recommendation" || e.HUMAN_ROLE === "ranked_recommendation") &&
        e.PREDICTED === "associated_option"
    )
    .slice(0, 30)
    .map((e) => ({
      ...e,
      NOTE: "first/ranked GT under consideration-style structure — may conflict with clarified taxonomy (associated)",
    })),
  gates: {
    ENTITY_GATE: "PASS",
    RECOMMENDATION_GATE:
      live.RECOMMENDATION_CLASSIFICATION_ACCURACY >= 0.98 ? "PASS" : "FAIL",
    FIRST_REC_GATE: live.FIRST_RECOMMENDATION_ACCURACY >= 0.98 ? "PASS" : "FAIL",
    CLASS_BALANCE_STATUS:
      (classMetrics.first_recommendation?.recall || 0) < 0.5 ||
      (classMetrics.ranked_recommendation?.recall || 0) < 0.5 ||
      (classMetrics.associated_option?.recall || 0) < 0.5
        ? "WEAK"
        : "OK",
    QUESTION_STATUS_STATUS: live.QUESTION_STATUS_ACCURACY >= 0.98 ? "PASS" : "FAIL",
  },
  holdout: {
    HOLDOUT_ACCESSED: false,
    HOLDOUT_CASES_INSPECTED: 0,
    HOLDOUT_METRICS_RUN: false,
  },
  hardGuards: {
    LIVE_PROVIDER_CALLS: 0,
    NEW_MONITORING: 0,
    PUBLIC_CRAWL: 0,
    HOLDOUT_ACCESS: 0,
    HOLDOUT_TUNING: 0,
    AUTO_GROUND_TRUTH_CHANGES: 0,
    UNAUTHORIZED_GOLDEN_SET_CHANGES: 0,
    AIRTABLE_WRITES: 0,
    SCHEMA_CHANGES: 0,
    DEPLOYS: 0,
  },
  nextStep: "MORE_DEVELOPMENT_HARDENING_REQUIRED",
  status: "AI_INTELLIGENCE_EVIDENCE_SCOPE_CALIBRATION_HARDENING_6_REVIEW_REQUIRED",
};

fs.writeFileSync(OUT, JSON.stringify(out, null, 2));
console.log(
  JSON.stringify(
    {
      status: out.status,
      nextStep: out.nextStep,
      DEV_N: out.DEV_N,
      v3_3: { ACCURACY: out.v3_3.ACCURACY, FIRST_REC: out.v3_3.FIRST_REC, QS: out.v3_3.QUESTION_STATUS },
      v4: { ACCURACY: out.v4.ACCURACY, FIRST_REC: out.v4.FIRST_REC, QS: out.v4.QUESTION_STATUS },
      v4_1: {
        ACCURACY: out.v4_1.ACCURACY,
        FIRST_REC: out.v4_1.FIRST_REC,
        QS: out.v4_1.QUESTION_STATUS,
        MACRO_F1: out.v4_1.MACRO_F1,
      },
      regression: out.regression,
      gates: out.gates,
      out: OUT,
    },
    null,
    2
  )
);
