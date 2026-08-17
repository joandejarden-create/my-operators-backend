#!/usr/bin/env node
/**
 * Export Holdout v3 primary Presence review batch (JSON + Markdown).
 *
 *   node scripts/ai-intelligence-presence-holdout-v3-review-export.mjs
 *
 * No provider calls. No selection/freeze/score. No auto-labels.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildHoldoutV3PrimaryReviewExport,
  HOLDOUT_V3_BATCH_ID,
  HOLDOUT_V3_PRIMARY_EXPORT_VERSION,
} from "../lib/ai-visibility/validation/presence-holdout-v3-review-export.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function normalizeProviderKey(p) {
  const s = String(p || "").toLowerCase();
  if (s.includes("openai")) return "OPENAI";
  if (s.includes("gemini")) return "GEMINI";
  if (s.includes("perplexity")) return "PERPLEXITY";
  if (s.includes("claude")) return "CLAUDE";
  return String(p || "UNSPECIFIED").toUpperCase();
}

function normalizeLanguageKey(l) {
  const s = String(l || "").toLowerCase();
  if (s.startsWith("en")) return "ENGLISH";
  if (s.startsWith("es")) return "SPANISH";
  return String(l || "UNSPECIFIED").toUpperCase();
}

function normalizeGeographyKey(g) {
  const s = String(g || "").toUpperCase().replace(/\s+/g, "_");
  if (s === "GLOBAL") return "GLOBAL";
  if (s === "CALA") return "CALA";
  if (s === "MEXICO" || s === "MX") return "MEXICO";
  if (s === "EUROPE") return "EUROPE";
  if (s === "NORTH_AMERICA" || s === "NORTH AMERICA" || s === "NA") return "NORTH_AMERICA";
  return s;
}

function bucketCounts(cases, normFn, key) {
  const out = {};
  for (const c of cases || []) {
    const k = normFn(c[key]);
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

const result = buildHoldoutV3PrimaryReviewExport({ persist: true });

if (!result.ok) {
  console.log("PRESENCE_HOLDOUT_V3_REVIEW_EXPORT_BLOCKED");
  console.log(JSON.stringify(result, null, 2));
  process.exit(2);
}

const p = result.payload;
const providers = bucketCounts(p.cases, normalizeProviderKey, "provider");
const languages = bucketCounts(p.cases, normalizeLanguageKey, "language");
const geos = bucketCounts(p.cases, normalizeGeographyKey, "geography");

const summary = {
  phase: "PRESENCE_HOLDOUT_V3_REVIEW_EXPORT_READY",
  status: "PRESENCE_HOLDOUT_V3_PRIMARY_REVIEW_EXPORT_PASS",
  export: {
    FILE: p.persisted?.json || null,
    MARKDOWN: p.persisted?.markdown || null,
    EXPORT_VERSION: HOLDOUT_V3_PRIMARY_EXPORT_VERSION,
    BATCH: HOLDOUT_V3_BATCH_ID,
    CASE_COUNT: p.caseCount,
    UNIQUE_CASE_ID_COUNT: p.UNIQUE_CASE_ID_COUNT,
    UNIQUE_ENTITY_RESPONSE_PAIR_COUNT: p.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT,
    UNIQUE_RESPONSE_COUNT: p.uniqueResponseCount,
    MAX_PAIRS_PER_RESPONSE: p.MAX_PAIRS_PER_RESPONSE,
  },
  providers,
  languages,
  geographies: geos,
  integrity: {
    LEAKAGE: p.integrity.LEAKAGE_TO_PRIOR_VALIDATION,
    DUPLICATES:
      p.UNIQUE_CASE_ID_COUNT === p.caseCount &&
      p.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT === p.caseCount
        ? 0
        : "FAIL",
    ALREADY_REVIEWED: p.integrity.ALREADY_HUMAN_REVIEWED,
  },
  governance: {
    SYSTEM_SUGGESTION_ASSISTANCE_ONLY: "YES",
    AUTO_HUMAN_LABELING: "NO",
    HOLDOUT_V3_SELECTED: "NO",
    HOLDOUT_V3_FROZEN: "NO",
    HOLDOUT_V3_SCORED: "NO",
  },
  nextAction: "SEND_EXPORTED_JSON_FOR_ASSISTED_PRESENCE_REVIEW",
  hardGuards: {
    PROVIDER_CALLS: 0,
    ENTITY_RESOLVER_CHANGES: 0,
    ALIAS_CHANGES: 0,
    AUTO_HUMAN_LABELS: 0,
    HUMAN_FINAL_LABELS_WRITTEN: 0,
    HOLDOUT_V3_SELECTION: 0,
    HOLDOUT_V3_FREEZE: 0,
    HOLDOUT_V3_SCORING: 0,
    HOLDOUT_V2_CHANGES: 0,
    REGIONALIZATION_EXECUTION: 0,
    AIRTABLE_WRITES: 0,
    DEPLOYS: 0,
  },
};

const summaryPath = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-holdout-v3-primary-review-export-summary.json"
);
fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2) + "\n");

console.log("PRESENCE_HOLDOUT_V3_REVIEW_EXPORT_READY");
console.log(JSON.stringify(summary, null, 2));
console.log(`\nWrote ${p.persisted?.json}`);
console.log(`Wrote ${p.persisted?.markdown}`);
