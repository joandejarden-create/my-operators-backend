#!/usr/bin/env node
/**
 * Recommendation Hardening 3 — DEV-only error audit.
 * HOLDOUT untouched. No provider calls. No label changes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet, scoreGoldenSetHydrated } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { detectRankMarker, detectOrderedListContext, detectResponseSections, sectionRoleAt } from "../lib/ai-visibility/recommendation-classifier-v3.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "data/ai-visibility/validation/recommendation-hardening-3-dev-audit.json");

function pct(n) {
  return n == null ? null : Math.round(n * 1000) / 1000;
}

function localContext(text, start, end) {
  const s = String(text || "");
  const a = Math.max(0, start - 220);
  const b = Math.min(s.length, end + 220);
  return s.slice(a, b);
}

function lineAt(text, start) {
  const s = String(text || "");
  const ls = s.lastIndexOf("\n", start - 1) + 1;
  const le = s.indexOf("\n", start);
  return s.slice(ls, le === -1 ? s.length : le);
}

function cues(text, start, end) {
  const snip = localContext(text, start, end);
  const behind = String(text || "").slice(Math.max(0, start - 350), start);
  return {
    RANK_CUE: detectRankMarker(text, start),
    ORDERED_LIST: detectOrderedListContext(text, start),
    SECTION_HEADING: (() => {
      const sections = detectResponseSections(text);
      for (const sec of sections) {
        if (start >= sec.start && start < sec.end) return sec.title || "(preamble)";
      }
      return null;
    })(),
    SECTION_ROLE: sectionRoleAt(text, start),
    POSITIVE_CUE: /\b(recommend|strong\s+(?:option|choice)|best\s+fit|should\s+consider|recomendad|mejor\s+encaje|conviene\s+considerar)\b/i.test(snip),
    ASSOCIATION_CUE: /\b(options?\s+include|brands?\s+typically|consideration\s+set|associated|considered\s+alongside|se\s+consideran|suelen\s+considerar|alternativas|opciones)\b/i.test(
      behind + snip
    ),
    COMPARATOR_CUE: /\b(versus|vs\.?|compared\s+to|alternative\s+to|frente\s+a)\b/i.test(snip),
    NEGATIVE_CUE: /\b(not\s+recommend|avoid|poor\s+fit|no\s+recomend|evitar)\b/i.test(snip),
  };
}

const golden = loadGoldenSet();
const holdoutN = (golden.cases || []).filter((c) => c.holdoutSplit === "holdout").length;
if (holdoutN < 1) {
  console.error("BLOCKED: holdout partition missing");
  process.exit(2);
}

const score = await scoreGoldenSetHydrated(golden, { holdoutPolicy: "exclude" });
if (score.HOLDOUT_ACCESSED || score.HOLDOUT_METRICS_RUN) {
  console.error("BLOCKED: holdout accessed");
  process.exit(2);
}

const devCases = (golden.cases || []).filter((c) => c.holdoutSplit !== "holdout");
const { cases: hydrated } = await hydrateGoldenSetCasesForScoring(devCases, {});
const byId = new Map(hydrated.map((c) => [c.caseId || c.id, c]));

const recErrors = (score.errors || []).filter((e) => {
  if (e.ERROR_TYPE === "alias_resolution" || e.ERROR_TYPE === "question_status") return false;
  const roles = new Set([
    "first_recommendation",
    "ranked_recommendation",
    "explicit_recommendation",
    "associated_option",
    "comparator",
    "discussed",
    "passing_mention",
    "negative_or_qualified",
    "source_only",
  ]);
  return roles.has(e.EXPECTED) || roles.has(e.ACTUAL);
});

const errors = [];
const confusion = {};
const mexico = [];
const europe = [];
const firstRecMismatches = [];

for (const e of recErrors) {
  const c = byId.get(e.CASE_ID);
  const text = c?.text || "";
  const name = e.ENTITY || c?.entityName || "";
  const idx = name ? text.toLowerCase().indexOf(String(name).toLowerCase()) : -1;
  const start = idx >= 0 ? idx : 0;
  const end = start + (name ? name.length : 0);
  const cue = cues(text, start, end);
  const geo = e.GEOGRAPHY || c?.geography || null;
  const row = {
    CASE_ID: e.CASE_ID,
    PROVIDER: e.PROVIDER || c?.provider || null,
    LANGUAGE: e.LANGUAGE || c?.language || null,
    GEOGRAPHY: geo,
    PROMPT_FAMILY: e.PROMPT_FAMILY || c?.promptFamily || c?.promptIntentTerritory || null,
    ENTITY: e.ENTITY,
    HUMAN_LABEL: e.EXPECTED,
    CLASSIFIER_LABEL: e.ACTUAL,
    LINE: lineAt(text, start).slice(0, 240),
    LOCAL_ENTITY_CONTEXT: localContext(text, start, end).slice(0, 500),
    ...cue,
  };
  errors.push(row);
  const pair = `${e.EXPECTED} => ${e.ACTUAL}`;
  confusion[pair] = (confusion[pair] || 0) + 1;
  const geoU = String(geo || "").toUpperCase();
  if (geoU.includes("MEXICO")) mexico.push(row);
  if (geoU.includes("EUROPE")) europe.push(row);
  if (e.EXPECTED === "first_recommendation" || e.ACTUAL === "first_recommendation") {
    firstRecMismatches.push(row);
  }
}

const rankedToExplicit = errors.filter(
  (r) => r.HUMAN_LABEL === "ranked_recommendation" && r.CLASSIFIER_LABEL === "explicit_recommendation"
);
const discussedToExplicit = errors.filter(
  (r) => r.HUMAN_LABEL === "discussed" && r.CLASSIFIER_LABEL === "explicit_recommendation"
);
const associatedToDiscussed = errors.filter(
  (r) => r.HUMAN_LABEL === "associated_option" && r.CLASSIFIER_LABEL === "discussed"
);

const report = {
  version: "recommendation_hardening_3_dev_audit_v1",
  generatedAt: new Date().toISOString(),
  HOLDOUT_ACCESSED: false,
  HOLDOUT_CASES_INSPECTED: 0,
  HOLDOUT_METRICS_RUN: false,
  DEV_METRICS: {
    RECOMMENDATION_ACCURACY: pct(score.RECOMMENDATION_CLASSIFICATION_ACCURACY),
    RECOMMENDATION_PRECISION: pct(score.RECOMMENDATION_PRECISION),
    RECOMMENDATION_RECALL: pct(score.RECOMMENDATION_RECALL),
    RECOMMENDATION_F1: pct(score.RECOMMENDATION_F1),
    FIRST_REC_ACCURACY: pct(score.FIRST_RECOMMENDATION_ACCURACY),
    QUESTION_STATUS_ACCURACY: pct(score.QUESTION_STATUS_ACCURACY),
    ENTITY_PRECISION: pct(score.ENTITY_RESOLUTION_PRECISION),
    ENTITY_RECALL: pct(score.ENTITY_RESOLUTION_RECALL),
    ENTITY_F1: pct(score.ENTITY_RESOLUTION_F1),
    CASE_COUNT: score.CASE_COUNT,
  },
  TOTAL_REC_ERRORS: errors.length,
  BY_CONFUSION_PAIR: Object.entries(confusion)
    .sort((a, b) => b[1] - a[1])
    .map(([pattern, count]) => ({ pattern, count })),
  RANKED_TO_EXPLICIT_SAMPLE: rankedToExplicit.slice(0, 15),
  DISCUSSED_TO_EXPLICIT_SAMPLE: discussedToExplicit.slice(0, 10),
  ASSOCIATED_TO_DISCUSSED_SAMPLE: associatedToDiscussed.slice(0, 13),
  FIRST_REC_MISMATCH_SAMPLE: firstRecMismatches.slice(0, 15),
  MEXICO_ERRORS: mexico.length,
  MEXICO_SAMPLE: mexico.slice(0, 12),
  EUROPE_ERRORS: europe.length,
  EUROPE_SAMPLE: europe.slice(0, 12),
  ALL_ERRORS: errors,
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      OUT,
      TOTAL_REC_ERRORS: errors.length,
      BY_CONFUSION_PAIR: report.BY_CONFUSION_PAIR.slice(0, 12),
      MEXICO: mexico.length,
      EUROPE: europe.length,
      METRICS: report.DEV_METRICS,
      HOLDOUT_ACCESSED: false,
    },
    null,
    2
  )
);
