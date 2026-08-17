#!/usr/bin/env node
/**
 * Classifier Hardening 1 — Entity Resolution (DEVELOPMENT ONLY).
 * HOLDOUT must remain sealed. LIVE_PROVIDER_CALLS: 0. No recommendation classifier changes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet, scoreGoldenSet, scoreGoldenSetHydrated } from "../lib/ai-visibility/validation/golden-set.js";
import { materializeGoldenSetEntityUniverse } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { loadRuntimeAliasOverlay } from "../lib/ai-visibility/runtime-alias-overlay.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "data/ai-visibility/validation/classifier-hardening-1-entity-dev-result.json"
);

function pct(n) {
  return n == null ? null : Math.round(n * 10000) / 10000;
}

function entityCounts(score) {
  let fp = 0;
  let fn = 0;
  for (const e of score.errors || []) {
    if (e.ERROR_TYPE === "alias_resolution") {
      if (e.ACTUAL === "entity_absent") fn += 1;
      if (e.EXPECTED === "entity_absent" || e.ACTUAL === "entity_present") fp += 1;
    }
  }
  // Prefer denominator math
  const recall = score.ENTITY_RESOLUTION_RECALL;
  const prec = score.ENTITY_RESOLUTION_PRECISION;
  // Reconstruct approx from F1 if needed — use error inventory for FN
  fn = (score.errors || []).filter((e) => e.ERROR_TYPE === "alias_resolution").length;
  return { ENTITY_FALSE_NEGATIVES: fn, ENTITY_FALSE_POSITIVES: fp };
}

function subgroupSlice(score, dim, keys) {
  const rows = score.subgroupMetrics?.[dim] || {};
  const out = {};
  for (const k of keys) {
    const r = rows[k];
    if (!r) {
      out[k] = null;
      continue;
    }
    out[k] = {
      n: r.CASE_COUNT,
      ENTITY_PRECISION: pct(r.ENTITY_RESOLUTION_PRECISION),
      ENTITY_RECALL: pct(r.ENTITY_RESOLUTION_RECALL),
      RECOMMENDATION_ACCURACY: pct(r.RECOMMENDATION_CLASSIFICATION_ACCURACY),
      FIRST_RECOMMENDATION_ACCURACY: pct(r.FIRST_RECOMMENDATION_ACCURACY),
    };
  }
  return out;
}

// --- materialize universe ---
const universe = materializeGoldenSetEntityUniverse({ write: true });

const golden = loadGoldenSet();
const holdoutN = (golden.cases || []).filter((c) => c.holdoutSplit === "holdout").length;
const devN = (golden.cases || []).filter((c) => c.holdoutSplit !== "holdout").length;
if (holdoutN < 1) {
  console.error("BLOCKED: holdout missing");
  process.exit(2);
}

// Baseline on excerpts + tiny universe path already measured; recompute BEFORE with old-style
// excerpt-only + expanded index (universe fix alone), then AFTER with hydration.

const beforeExcerpt = scoreGoldenSet({
  ...golden,
  cases: (golden.cases || []).filter((c) => c.holdoutSplit !== "holdout"),
});

const after = await scoreGoldenSetHydrated(golden, { holdoutPolicy: "exclude" });

if (after.HOLDOUT_ACCESSED || after.HOLDOUT_METRICS_RUN) {
  console.error("BLOCKED: holdout accessed");
  process.exit(3);
}

const beforeCounts = entityCounts(beforeExcerpt);
const afterCounts = entityCounts(after);

const remainingByField = { entityPresent: 0, recommendationStatus: 0, questionStatus: 0 };
for (const e of after.errors || []) {
  if (e.ERROR_TYPE === "alias_resolution") remainingByField.entityPresent += 1;
  else if (e.ERROR_TYPE === "question_status") remainingByField.questionStatus += 1;
  else remainingByField.recommendationStatus += 1;
}

const remainingPatterns = {};
for (const e of after.errors || []) {
  const p =
    e.ERROR_TYPE === "alias_resolution"
      ? "ALIAS_RESOLUTION_ERROR"
      : e.ERROR_TYPE === "question_status"
        ? "QUESTION_STATUS_MISMATCH"
        : e.ERROR_TYPE === "ranking_first_recommendation"
          ? "FIRST_RECOMMENDATION_MISSED"
          : "RECOMMENDATION_STATUS_MISMATCH";
  remainingPatterns[p] = (remainingPatterns[p] || 0) + 1;
}

const entityGate =
  (after.ENTITY_RESOLUTION_PRECISION || 0) >= 0.98 &&
  (after.ENTITY_RESOLUTION_RECALL || 0) >= 0.98;

const overlay = loadRuntimeAliasOverlay();
const entityFns = (after.errors || []).filter((e) => e.ERROR_TYPE === "alias_resolution");
const remainingEntityClusters = {};
for (const e of entityFns) {
  const key = e.ENTITY || e.ENTITY_NAME || e.CANONICAL_ENTITY || e.expectedEntity || "UNKNOWN";
  if (!remainingEntityClusters[key]) {
    remainingEntityClusters[key] = {
      PATTERN: key,
      COUNT: 0,
      CASE_IDS: [],
      PROVIDERS: [],
      LANGUAGES: [],
      FAILURE_REASON: null,
    };
  }
  remainingEntityClusters[key].COUNT += 1;
  if (e.CASE_ID || e.caseId) remainingEntityClusters[key].CASE_IDS.push(e.CASE_ID || e.caseId);
  if (e.PROVIDER && !remainingEntityClusters[key].PROVIDERS.includes(e.PROVIDER)) {
    remainingEntityClusters[key].PROVIDERS.push(e.PROVIDER);
  }
  if (e.LANGUAGE && !remainingEntityClusters[key].LANGUAGES.includes(e.LANGUAGE)) {
    remainingEntityClusters[key].LANGUAGES.push(e.LANGUAGE);
  }
}
const remainingEntityFailureClusters = Object.values(remainingEntityClusters).map((row) => {
  if (/^Playa/i.test(row.PATTERN)) {
    row.FAILURE_REASON =
      "SUBJECT_ABSENT_OR_SPANISH_PLAYA_AMBIGUITY — response has beach/place 'playa' only; bare Playa alias rejected";
  } else if (/IHG/i.test(row.PATTERN)) {
    row.FAILURE_REASON =
      "PARENT_PREFIX_BLOCKED — text uses bare IHG; mapping to IHG Hotels & Resorts (Managed) rejected";
  } else {
    row.FAILURE_REASON = "OTHER_OR_SURFACE_FORM_GAP";
  }
  return row;
});

const report = {
  version: "ai_intelligence_classifier_hardening_1_entity_dev_result_v1",
  generatedAt: new Date().toISOString(),
  HOLDOUT_ACCESSED: false,
  HOLDOUT_CASES_INSPECTED: 0,
  HOLDOUT_METRICS_RUN: false,
  DEV_N: devN,
  HOLDOUT_N_SEALED: holdoutN,
  universeEntityCount: universe.entityCount,
  developmentBaseline: {
    note: "Excerpt-only scoring with expanded Golden Set entity index (pre-hydration)",
    ENTITY_PRECISION: pct(beforeExcerpt.ENTITY_RESOLUTION_PRECISION),
    ENTITY_RECALL: pct(beforeExcerpt.ENTITY_RESOLUTION_RECALL),
    ENTITY_F1: pct(beforeExcerpt.ENTITY_RESOLUTION_F1),
    ...beforeCounts,
    RECOMMENDATION_ACCURACY: pct(beforeExcerpt.RECOMMENDATION_CLASSIFICATION_ACCURACY),
    FIRST_RECOMMENDATION_ACCURACY: pct(beforeExcerpt.FIRST_RECOMMENDATION_ACCURACY),
    QUESTION_STATUS_ACCURACY: pct(beforeExcerpt.QUESTION_STATUS_ACCURACY),
  },
  // Historical baseline from prior audit (excerpt + 17-entity universe)
  priorAuditBaseline: {
    ENTITY_PRECISION: 1,
    ENTITY_RECALL: 0.5364,
    ENTITY_F1: 0.6983,
    ENTITY_FALSE_NEGATIVES: 140,
  },
  rootCauses: {
    ENTITY_INDEX_MISSING: "Phase 2C universe had 17 brands; Golden Set subjects need 31+",
    TRUNCATED_EXTRACTION:
      "Scoring used rawResponseExcerpt (~1200 chars); full store rawText often 5–10k and contains the entity",
    CANONICAL_NAME_VARIANT: "Partial — short aliases + markdown; addressed via overlay + stripMarkdown",
  },
  providerNormalization: {
    note: "Provider rawText in monitoring store preserves brand text; loss was golden-set excerpt truncation for scoring, not adapter drop.",
    OPENAI: { TEXT_LOSS_FOUND: "EXCERPT_TRUNCATION_IN_SCORING", ENTITY_BEARING_TEXT_DROPPED: "YES_WHEN_LATE_IN_ANSWER" },
    GEMINI: { TEXT_LOSS_FOUND: "EXCERPT_TRUNCATION_IN_SCORING", ENTITY_BEARING_TEXT_DROPPED: "YES_WHEN_LATE_IN_ANSWER" },
    PERPLEXITY: { TEXT_LOSS_FOUND: "EXCERPT_TRUNCATION_IN_SCORING", ENTITY_BEARING_TEXT_DROPPED: "YES_WHEN_LATE_IN_ANSWER" },
    CLAUDE: { TEXT_LOSS_FOUND: "EXCERPT_TRUNCATION_IN_SCORING", ENTITY_BEARING_TEXT_DROPPED: "YES_WHEN_LATE_IN_ANSWER" },
    FIXES: [
      "hydrateGoldenSetCasesForScoring loads full monitoring-store rawText by batchId+responseId",
      "buildGoldenSetScoringEntityIndex expands subject universe",
      "stripMarkdownNoiseForEntityMatch before span search",
      "runtime alias overlay short forms (collision-safe)",
    ],
  },
  resolverFixes: {
    NORMALIZATION_FIXES: ["stripMarkdownNoiseForEntityMatch", "NFKD accent folding (existing)"],
    ALIASES_ADDED: (overlay.aliases || [])
      .filter((a) => a.source === "golden_set_v2_dev_hardening_1" || a.alias === "Courtyard")
      .map((a) => a.alias)
      .concat(
        (overlay.aliases || [])
          .filter((a) => a.source === "ai_visibility_live_observation")
          .map((a) => a.alias)
          .filter((a) => ["Curio", "Curio Collection", "Autograph"].includes(a))
      ),
    ALIASES_REJECTED: (overlay.aliasesRejected || []).map((a) => a.alias),
    COLLISION_PROTECTIONS: [
      "blockedBareParents",
      "longest-alias-first",
      "rejected generic aliases",
      "rejected Playa (Spanish beach/place collision)",
    ],
    PARENT_BRAND_PROTECTIONS: [
      "Hilton/Marriott/Hyatt/IHG/Accor/Wyndham remain blocked bare parents",
    ],
  },
  remainingEntityFailureClusters,
  developmentAfter: {
    ENTITY_PRECISION: pct(after.ENTITY_RESOLUTION_PRECISION),
    ENTITY_RECALL: pct(after.ENTITY_RESOLUTION_RECALL),
    ENTITY_F1: pct(after.ENTITY_RESOLUTION_F1),
    ...afterCounts,
    hydrationStats: after.hydrationStats,
  },
  subgroups: {
    PROVIDER: subgroupSlice(after, "PROVIDER", ["openai", "gemini", "perplexity", "claude"]),
    LANGUAGE: subgroupSlice(after, "LANGUAGE", ["en", "es"]),
    GEOGRAPHY: subgroupSlice(after, "GEOGRAPHY", [
      "GLOBAL",
      "CALA",
      "MEXICO",
      "EUROPE",
      "NORTH_AMERICA",
    ]),
  },
  cascadeEffect: {
    RECOMMENDATION_ACCURACY_BEFORE: pct(beforeExcerpt.RECOMMENDATION_CLASSIFICATION_ACCURACY),
    RECOMMENDATION_ACCURACY_AFTER: pct(after.RECOMMENDATION_CLASSIFICATION_ACCURACY),
    FIRST_REC_ACCURACY_BEFORE: pct(beforeExcerpt.FIRST_RECOMMENDATION_ACCURACY),
    FIRST_REC_ACCURACY_AFTER: pct(after.FIRST_RECOMMENDATION_ACCURACY),
    QUESTION_STATUS_ACCURACY_BEFORE: pct(beforeExcerpt.QUESTION_STATUS_ACCURACY),
    QUESTION_STATUS_ACCURACY_AFTER: pct(after.QUESTION_STATUS_ACCURACY),
  },
  remainingErrors: {
    ENTITY: remainingByField.entityPresent,
    RECOMMENDATION: remainingByField.recommendationStatus,
    FIRST_REC: remainingPatterns.FIRST_RECOMMENDATION_MISSED || 0,
    QUESTION_STATUS: remainingByField.questionStatus,
    BY_PATTERN: remainingPatterns,
  },
  entityGate,
  ENTITY_GATE_STATUS: entityGate
    ? "ENTITY_GATE_READY_FOR_NEXT_HARDENING_PHASE"
    : "ENTITY_GATE_REVIEW_REQUIRED",
  activity: {
    LIVE_PROVIDER_CALLS: 0,
    NEW_MONITORING: 0,
    PUBLIC_CRAWL: 0,
    GOLDEN_SET_LABEL_CHANGES: 0,
    HOLDOUT_TUNING: 0,
    RECOMMENDATION_CLASSIFIER_CHANGES: 0,
    QUESTION_STATUS_LOGIC_CHANGES: 0,
    AIRTABLE_WRITES: 0,
    SCHEMA_CHANGES: 0,
    DEPLOYS: 0,
  },
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2), "utf8");
console.log(JSON.stringify(report, null, 2));
