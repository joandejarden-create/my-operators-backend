#!/usr/bin/env node
/**
 * Classifier Hardening 1 — development-only entity FN audit.
 * HOLDOUT must not be inspected. LIVE_PROVIDER_CALLS: 0.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildAiVisibilityEntityIndex,
  extractMentions,
  loadRuntimeAliasOverlay,
  normalizeMatchKey,
} from "../lib/ai-visibility/index.js";
import { loadGoldenSet, scoreGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const UNIVERSE = path.join(ROOT, "fixtures/ai-visibility/phase2c-entity-universe.json");
const OUT = path.join(
  ROOT,
  "data/ai-visibility/validation/classifier-hardening-1-entity-dev-audit.json"
);

function scoreDevOnly(golden) {
  const cases = (golden.cases || []).filter((c) => c.holdoutSplit !== "holdout");
  return scoreGoldenSet({ ...golden, cases, caseCount: cases.length });
}

function classifyFailure({ entityName, excerpt, hits, inUniverse, aliasKeys }) {
  const text = String(excerpt || "");
  const key = normalizeMatchKey(entityName);
  const textKey = normalizeMatchKey(text);
  const reasons = [];

  if (!inUniverse) reasons.push("ENTITY_INDEX_MISSING");
  if (!text.trim()) reasons.push("TRUNCATED_EXTRACTION");

  const surfaceForms = [];
  // Check if any word of canonical appears
  const nameParts = key.split(" ").filter((w) => w.length > 2);
  const anyPart = nameParts.some((p) => textKey.includes(p));
  const fullInText = key && textKey.includes(key);

  if (!fullInText && anyPart) {
    reasons.push("CANONICAL_NAME_VARIANT");
    // find rough surface
    for (const p of nameParts) {
      if (textKey.includes(p)) surfaceForms.push(p);
    }
  }
  if (!fullInText && !anyPart) {
    reasons.push("TRUNCATED_EXTRACTION");
  }
  if (fullInText && hits.length === 0 && inUniverse) {
    // text contains normalized name but matcher missed — formatting / alias / boundary
    if (/[*_`#|\[\]]/.test(text) || /\|/.test(text)) reasons.push("MARKDOWN_FORMATTING");
    if (/[áéíóúñüÁÉÍÓÚÑÜ]/.test(String(excerpt || ""))) reasons.push("ACCENT_OR_DIACRITIC");
    if (!aliasKeys.includes(key)) reasons.push("SHORT_ALIAS_MISSING");
    else reasons.push("OTHER");
  }
  if (hits.length === 0 && fullInText) {
    // boundary issues
    reasons.push("OTHER");
  }

  const unique = [...new Set(reasons)];
  return {
    FAILURE_REASON: unique[0] || "OTHER",
    FAILURE_REASONS: unique,
    SURFACE_HINTS: surfaceForms,
    FULL_CANONICAL_IN_NORMALIZED_TEXT: !!fullInText,
    ANY_NAME_TOKEN_IN_TEXT: !!anyPart,
  };
}

const golden = loadGoldenSet();
const holdoutCount = (golden.cases || []).filter((c) => c.holdoutSplit === "holdout").length;
const devCases = (golden.cases || []).filter((c) => c.holdoutSplit !== "holdout");

if (holdoutCount < 1) {
  console.error("BLOCKED: holdout partition missing");
  process.exit(2);
}

const before = scoreDevOnly(golden);
const universe = JSON.parse(fs.readFileSync(UNIVERSE, "utf8"));
const overlay = loadRuntimeAliasOverlay();
const index = buildAiVisibilityEntityIndex({
  brands: universe.entities.filter((e) => e.entityType === "brand"),
  operators: universe.entities.filter((e) => e.entityType === "operator"),
  applyOverlay: true,
  runtimeOverlay: overlay,
});
const universeNames = new Set(
  (universe.entities || []).map((e) => normalizeMatchKey(e.name))
);
const universeByName = new Map(
  (universe.entities || []).map((e) => [normalizeMatchKey(e.name), e])
);

const falseNegatives = [];
for (const c of before.errors || []) {
  if (c.ERROR_TYPE !== "alias_resolution") continue;
  // only development — scoreGoldenSet already filtered cases
  const full = devCases.find((x) => x.caseId === c.CASE_ID);
  if (!full) continue; // safety: skip if somehow holdout
  if (full.holdoutSplit === "holdout") {
    console.error("BLOCKED: holdout case leaked into audit", c.CASE_ID);
    process.exit(3);
  }

  const entityName = full.entityName || c.ENTITY;
  const text = full.text || "";
  const inUniverse = universeNames.has(normalizeMatchKey(entityName));
  const ent = universeByName.get(normalizeMatchKey(entityName));
  const aliasKeys = [
    normalizeMatchKey(entityName),
    ...((ent?.aliases || []).map((a) => normalizeMatchKey(a))),
  ];

  const mentions = extractMentions({
    responseId: `audit_${full.caseId}`,
    text,
    entityIndex: index.aliasIndex,
    promptIntentTerritory: full.promptIntentTerritory,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === entityName);

  const fail = classifyFailure({
    entityName,
    excerpt: text,
    hits,
    inUniverse,
    aliasKeys,
  });

  // Exact surface form search (case-insensitive original)
  let exactForm = null;
  if (entityName && text) {
    const re = new RegExp(
      entityName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/\s+/g, "\\s+"),
      "i"
    );
    const m = text.match(re);
    exactForm = m ? m[0] : null;
  }

  falseNegatives.push({
    CASE_ID: full.caseId,
    PROVIDER: full.provider,
    LANGUAGE: full.language,
    GEOGRAPHY: full.geography,
    PROMPT_FAMILY: full.promptFamily || full.promptIntentTerritory,
    CANONICAL_ENTITY: entityName,
    CANONICAL_ID: full.canonicalEntityId || ent?.id || null,
    EXACT_TEXT_FORM_IN_RESPONSE: exactForm,
    RESOLVER_INPUT_TEXT_LENGTH: text.length,
    RESOLVER_INPUT_PREVIEW: text.slice(0, 180),
    CURRENT_ALIAS_SET: aliasKeys,
    MATCH_ATTEMPT_RESULT: hits.length ? "HIT" : "MISS",
    IN_UNIVERSE: inUniverse,
    ...fail,
  });
}

const clusters = {};
for (const fn of falseNegatives) {
  const p = fn.FAILURE_REASON || "OTHER";
  if (!clusters[p]) {
    clusters[p] = {
      PATTERN: p,
      COUNT: 0,
      CASE_IDS: [],
      PROVIDERS: {},
      LANGUAGES: {},
      EXAMPLE_SURFACE_FORMS: [],
      CANONICAL_ENTITIES: {},
    };
  }
  clusters[p].COUNT += 1;
  clusters[p].CASE_IDS.push(fn.CASE_ID);
  clusters[p].PROVIDERS[fn.PROVIDER || "unspecified"] =
    (clusters[p].PROVIDERS[fn.PROVIDER || "unspecified"] || 0) + 1;
  clusters[p].LANGUAGES[fn.LANGUAGE || "unspecified"] =
    (clusters[p].LANGUAGES[fn.LANGUAGE || "unspecified"] || 0) + 1;
  clusters[p].CANONICAL_ENTITIES[fn.CANONICAL_ENTITY] =
    (clusters[p].CANONICAL_ENTITIES[fn.CANONICAL_ENTITY] || 0) + 1;
  if (fn.EXACT_TEXT_FORM_IN_RESPONSE && clusters[p].EXAMPLE_SURFACE_FORMS.length < 8) {
    clusters[p].EXAMPLE_SURFACE_FORMS.push(fn.EXACT_TEXT_FORM_IN_RESPONSE);
  } else if (fn.SURFACE_HINTS?.length && clusters[p].EXAMPLE_SURFACE_FORMS.length < 8) {
    clusters[p].EXAMPLE_SURFACE_FORMS.push(fn.SURFACE_HINTS.join("+"));
  }
}

// Top missed entities
const byEntity = {};
for (const fn of falseNegatives) {
  byEntity[fn.CANONICAL_ENTITY] = (byEntity[fn.CANONICAL_ENTITY] || 0) + 1;
}
const topMissed = Object.entries(byEntity)
  .sort((a, b) => b[1] - a[1])
  .slice(0, 40);

const report = {
  version: "ai_intelligence_classifier_hardening_1_entity_dev_audit_v1",
  generatedAt: new Date().toISOString(),
  HOLDOUT_ACCESSED: false,
  HOLDOUT_CASES_INSPECTED: 0,
  HOLDOUT_METRICS_RUN: false,
  DEV_N: devCases.length,
  HOLDOUT_N_SEALED: holdoutCount,
  baseline: {
    ENTITY_PRECISION: before.ENTITY_RESOLUTION_PRECISION,
    ENTITY_RECALL: before.ENTITY_RESOLUTION_RECALL,
    ENTITY_F1: before.ENTITY_RESOLUTION_F1,
    ENTITY_FALSE_NEGATIVES: falseNegatives.length,
    RECOMMENDATION_ACCURACY: before.RECOMMENDATION_CLASSIFICATION_ACCURACY,
    FIRST_RECOMMENDATION_ACCURACY: before.FIRST_RECOMMENDATION_ACCURACY,
    QUESTION_STATUS_ACCURACY: before.QUESTION_STATUS_ACCURACY,
    subgroupMetrics: before.subgroupMetrics,
  },
  clusters,
  topMissedEntities: topMissed,
  falseNegatives,
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2), "utf8");
console.log(
  JSON.stringify(
    {
      DEV_N: report.DEV_N,
      HOLDOUT_SEALED: report.HOLDOUT_N_SEALED,
      ENTITY_PRECISION: report.baseline.ENTITY_PRECISION,
      ENTITY_RECALL: report.baseline.ENTITY_RECALL,
      ENTITY_F1: report.baseline.ENTITY_F1,
      FN: report.baseline.ENTITY_FALSE_NEGATIVES,
      clusters: Object.fromEntries(
        Object.entries(clusters).map(([k, v]) => [k, v.COUNT])
      ),
      topMissed: topMissed.slice(0, 15),
      wrote: OUT,
    },
    null,
    2
  )
);
