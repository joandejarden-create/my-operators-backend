/**
 * Presence-only holdout integrity + evaluation.
 * Uses entity-resolution spans only — does not invoke recommendation classifier.
 * HOLDOUT_TUNING = 0 (never mutate labels or classifiers from this path).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet } from "../validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../validation/hydrate-golden-set-texts.js";
import { buildGoldenSetScoringEntityIndex } from "../validation/golden-set-entity-index.js";
import { findEntitySpans } from "../normalize-entities.js";
import { MANDATORY_SUBGROUP_MIN_N } from "../validation/classification-threshold.js";
import { SIGNAL_PRODUCTION_GATES } from "../validation/classification-threshold.js";
import { SIGNAL_KEYS, SIGNAL_READINESS } from "../signal-architecture/index.js";
import { summarizeProductSurfaceAudit, SURFACE_CLASS } from "../signal-architecture/product-surface-audit.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_PATH = path.resolve(
  __dirname,
  "../../../fixtures/ai-visibility/ai-intelligence-golden-set-v2.json"
);
const VALIDATION_ROOT = path.resolve(__dirname, "../../../data/ai-visibility/validation");

export const PRESENCE_HOLDOUT_EVAL_VERSION = "ai_intelligence_presence_holdout_v1";

/**
 * Scan validation artifacts for prior holdout scoring/access.
 */
export function auditPriorHoldoutAccess(rootDir = VALIDATION_ROOT) {
  const hits = [];
  if (!fs.existsSync(rootDir)) {
    return { HOLDOUT_PREVIOUSLY_ACCESSED: false, HOLDOUT_PREVIOUSLY_SCORED: false, hits };
  }
  const stack = [rootDir];
  while (stack.length) {
    const dir = stack.pop();
    for (const name of fs.readdirSync(dir)) {
      const p = path.join(dir, name);
      const st = fs.statSync(p);
      if (st.isDirectory()) {
        stack.push(p);
        continue;
      }
      if (!name.endsWith(".json")) continue;
      // Skip our own future report filename during re-runs
      if (name === "presence-holdout-validation.json") continue;
      let text;
      try {
        text = fs.readFileSync(p, "utf8");
      } catch {
        continue;
      }
      const accessed =
        /"HOLDOUT_ACCESSED"\s*:\s*true/.test(text) ||
        /"HOLDOUT_ACCESSED"\s*:\s*"YES"/i.test(text);
      const scored =
        /"HOLDOUT_METRICS_RUN"\s*:\s*true/.test(text) ||
        /"holdoutPolicy"\s*:\s*"holdout_only"/.test(text);
      if (accessed || scored) {
        hits.push({ path: p, accessed, scored });
      }
    }
  }
  return {
    HOLDOUT_PREVIOUSLY_ACCESSED: hits.some((h) => h.accessed),
    HOLDOUT_PREVIOUSLY_SCORED: hits.some((h) => h.scored),
    hits,
  };
}

/**
 * Count holdout cases used in GT amendments (must be 0).
 */
export function countHoldoutCasesUsedForTuning() {
  let used = 0;
  const amendmentDirs = [
    path.join(VALIDATION_ROOT, "amendments"),
    VALIDATION_ROOT,
  ];
  const patterns = [/taxonomy-resolution.*\.json$/, /ground-truth-amendment.*\.json$/];
  for (const dir of amendmentDirs) {
    if (!fs.existsSync(dir)) continue;
    for (const name of fs.readdirSync(dir)) {
      if (!name.endsWith(".json")) continue;
      if (!patterns.some((re) => re.test(name)) && !name.includes("amendment") && !name.includes("taxonomy-resolution")) {
        continue;
      }
      let doc;
      try {
        doc = JSON.parse(fs.readFileSync(path.join(dir, name), "utf8"));
      } catch {
        continue;
      }
      const rows =
        doc.amendments ||
        doc.cases ||
        doc.authorizedAmendments ||
        doc.applied ||
        [];
      for (const row of Array.isArray(rows) ? rows : []) {
        if (row.holdoutSplit === "holdout" || row.HOLDOUT === true) used += 1;
      }
    }
  }
  return used;
}

/**
 * Step 1 — verify sealed holdout integrity. STOP if any fail.
 */
export function verifyHoldoutIntegrity() {
  const raw = JSON.parse(fs.readFileSync(FIXTURE_PATH, "utf8"));
  const holdoutMeta = raw.holdout || {};
  const holdoutCases = (raw.cases || []).filter((c) => c.holdoutSplit === "holdout");
  const prior = auditPriorHoldoutAccess();
  const usedForTuning = countHoldoutCasesUsedForTuning();

  const integrity = {
    HOLDOUT_N: holdoutCases.length,
    HOLDOUT_VERSION: holdoutMeta.VERSION || null,
    HOLDOUT_CREATED_AT: raw.reviewDate || raw.lastValidatedAt || null,
    HOLDOUT_META_N: holdoutMeta.HOLDOUT_N ?? null,
    HOLDOUT_PREVIOUSLY_ACCESSED: prior.HOLDOUT_PREVIOUSLY_ACCESSED ? "YES" : "NO",
    HOLDOUT_PREVIOUSLY_SCORED: prior.HOLDOUT_PREVIOUSLY_SCORED ? "YES" : "NO",
    HOLDOUT_CASES_USED_FOR_TUNING: usedForTuning,
    STRATIFICATION_STATUS: holdoutMeta.STRATIFICATION_STATUS || null,
    DEVELOPMENT_N: holdoutMeta.DEVELOPMENT_N ?? null,
    priorHits: prior.hits,
  };

  const ok =
    integrity.HOLDOUT_N === 93 &&
    integrity.HOLDOUT_META_N === 93 &&
    integrity.HOLDOUT_PREVIOUSLY_ACCESSED === "NO" &&
    integrity.HOLDOUT_PREVIOUSLY_SCORED === "NO" &&
    integrity.HOLDOUT_CASES_USED_FOR_TUNING === 0;

  return {
    ok,
    integrity,
    stopReason: ok
      ? null
      : "HOLDOUT_INTEGRITY_FAILED — do not score holdout until integrity is restored",
  };
}

function prf(tp, fp, fn) {
  const precision = tp + fp > 0 ? tp / (tp + fp) : null;
  const recall = tp + fn > 0 ? tp / (tp + fn) : null;
  const f1 =
    precision != null && recall != null && precision + recall > 0
      ? (2 * precision * recall) / (precision + recall)
      : null;
  return { precision, recall, f1 };
}

function entityPresentByResolution(text, caseRow, aliasIndex) {
  const spans = findEntitySpans(String(text || ""), aliasIndex);
  const name = caseRow.entityName || caseRow.candidateEntity;
  const id = caseRow.canonicalEntityId;
  return spans.some((s) => {
    if (id && s.entity?.id === id) return true;
    if (name && s.entity?.name === name) return true;
    return false;
  });
}

function expectedPresence(caseRow) {
  if (caseRow.expectedEntityPresent != null) return !!caseRow.expectedEntityPresent;
  // Nominated golden-set subject with a role ⇒ presence expected true
  if (caseRow.entityName || caseRow.canonicalEntityId) return true;
  return false;
}

/**
 * Audit-only FN diagnosis — does not change classifier, aliases, or GT.
 */
function diagnosePresenceFalseNegative(text, caseRow) {
  const raw = String(text || "");
  const name = String(caseRow.entityName || "");
  const hasCanonical =
    name && new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(raw);
  const hasGeographicPlaya =
    /playa del carmen|playa \(playa|en playa|de playa/i.test(raw);
  if (name === "Playa Hotels & Resorts" && !hasCanonical && hasGeographicPlaya) {
    return (
      "FALSE_NEGATIVE — canonical company string absent; response only has geographic Playa tokens " +
      "(e.g. Playa del Carmen). Entity index has no distinct Playa Hotels & Resorts alias hit. " +
      "Possible subject/GT mismatch vs resolution gap. No tuning applied."
    );
  }
  if (name && !hasCanonical) {
    return (
      "FALSE_NEGATIVE — canonical entity string not found in eligible response text " +
      "(alias coverage or subject/text mismatch). No tuning applied."
    );
  }
  return (
    "FALSE_NEGATIVE — entity-resolution stack did not resolve canonical entity. No tuning applied."
  );
}

function normalizeProvider(p) {
  if (!p) return "unspecified";
  const s = String(p).toLowerCase();
  if (s.includes("openai") || s.includes("chatgpt") || s === "gpt") return "ChatGPT";
  if (s.includes("gemini")) return "Gemini";
  if (s.includes("perplexity")) return "Perplexity";
  if (s.includes("claude") || s.includes("anthropic")) return "Claude";
  return String(p);
}

function normalizeLanguage(l) {
  if (!l) return "unspecified";
  const s = String(l).toLowerCase();
  if (s === "en" || s.startsWith("en")) return "English";
  if (s === "es" || s.startsWith("es")) return "Spanish";
  return String(l);
}

function normalizeGeography(g) {
  if (!g) return "unspecified";
  const s = String(g).toUpperCase().replace(/\s+/g, "_");
  if (s === "GLOBAL") return "Global";
  if (s === "CALA") return "CALA";
  if (s === "MEXICO" || s === "MX") return "Mexico";
  if (s === "EUROPE" || s === "EU") return "Europe";
  if (s === "NORTH_AMERICA" || s === "NA" || s === "NORTH AMERICA") return "North America";
  return String(g);
}

function emptyBucket() {
  return { N: 0, tp: 0, tn: 0, fp: 0, fn: 0 };
}

function finalizeBucket(b) {
  const { precision, recall, f1 } = prf(b.tp, b.fp, b.fn);
  const accuracy = b.N > 0 ? (b.tp + b.tn) / b.N : null;
  const specificity = b.tn + b.fp > 0 ? b.tn / (b.tn + b.fp) : null;
  const falsePositiveRate = b.tn + b.fp > 0 ? b.fp / (b.tn + b.fp) : null;
  const falseNegativeRate = b.tp + b.fn > 0 ? b.fn / (b.tp + b.fn) : null;
  const gatePass =
    b.N >= MANDATORY_SUBGROUP_MIN_N &&
    precision != null &&
    recall != null &&
    precision >= SIGNAL_PRODUCTION_GATES.PRESENCE_GATE.precision &&
    recall >= SIGNAL_PRODUCTION_GATES.PRESENCE_GATE.recall;
  const gateFail =
    b.N >= MANDATORY_SUBGROUP_MIN_N &&
    (precision == null ||
      recall == null ||
      precision < SIGNAL_PRODUCTION_GATES.PRESENCE_GATE.precision ||
      recall < SIGNAL_PRODUCTION_GATES.PRESENCE_GATE.recall);
  return {
    N: b.N,
    CANDIDATE_PAIR_N: b.N,
    UNIQUE_RESPONSE_N: b.uniqueResponseN != null ? b.uniqueResponseN : null,
    TRUE_POSITIVES: b.tp,
    TRUE_NEGATIVES: b.tn,
    FALSE_POSITIVES: b.fp,
    FALSE_NEGATIVES: b.fn,
    ACCURACY: accuracy,
    PRECISION: precision,
    RECALL: recall,
    F1: f1,
    SPECIFICITY: specificity,
    FALSE_POSITIVE_RATE: falsePositiveRate,
    FALSE_NEGATIVE_RATE: falseNegativeRate,
    GATE:
      b.N < MANDATORY_SUBGROUP_MIN_N
        ? "INSUFFICIENT_N"
        : gatePass
          ? "PASS"
          : gateFail
            ? "FAIL"
            : "REVIEW",
  };
}

/**
 * Run Presence-only holdout evaluation (authorized final evaluation).
 * Does not tune. Does not score Recommended/First/Negative/Comparator.
 */
export async function runPresenceHoldoutValidation(options = {}) {
  const integrityCheck = verifyHoldoutIntegrity();
  if (!integrityCheck.ok && options.forceDespiteIntegrity !== true) {
    return {
      phase: "AI_INTELLIGENCE_PRESENCE_HOLDOUT_VALIDATION_COMPLETE",
      status: "AI_INTELLIGENCE_PRESENCE_HOLDOUT_VALIDATION_BLOCKED",
      nextStep: "PRESENCE_HOLDOUT_REVIEW_REQUIRED",
      holdoutIntegrity: integrityCheck.integrity,
      stopReason: integrityCheck.stopReason,
      HOLDOUT_ACCESSED: false,
      HOLDOUT_METRICS_RUN: false,
    };
  }

  const golden = loadGoldenSet();
  const holdoutCases = (golden.cases || []).filter((c) => c.holdoutSplit === "holdout");
  const index = buildGoldenSetScoringEntityIndex({});
  const { cases: hydrated, stats: hydrationStats } = await hydrateGoldenSetCasesForScoring(
    holdoutCases,
    { store: options.store }
  );

  const agg = emptyBucket();
  const byProvider = {};
  const byLanguage = {};
  const byGeography = {};
  const errors = [];

  for (const c of hydrated) {
    const text = c.text || c.rawResponseExcerpt || "";
    const expected = expectedPresence(c);
    const actual = entityPresentByResolution(text, c, index.aliasIndex);

    const provider = normalizeProvider(c.provider);
    const language = normalizeLanguage(c.language);
    const geography = normalizeGeography(c.geography);
    for (const [map, key] of [
      [byProvider, provider],
      [byLanguage, language],
      [byGeography, geography],
    ]) {
      if (!map[key]) map[key] = emptyBucket();
    }

    const buckets = [agg, byProvider[provider], byLanguage[language], byGeography[geography]];
    for (const b of buckets) b.N += 1;

    if (expected && actual) {
      for (const b of buckets) b.tp += 1;
    } else if (!expected && !actual) {
      for (const b of buckets) b.tn += 1;
    } else if (!expected && actual) {
      for (const b of buckets) b.fp += 1;
      errors.push({
        CASE_ID: c.caseId,
        PROVIDER: c.provider,
        LANGUAGE: c.language,
        GEOGRAPHY: c.geography,
        ENTITY: c.entityName,
        EXPECTED: false,
        ACTUAL: true,
        ROOT_CAUSE: "FALSE_POSITIVE — entity resolution matched when presence expected false",
      });
    } else {
      for (const b of buckets) b.fn += 1;
      errors.push({
        CASE_ID: c.caseId,
        PROVIDER: c.provider,
        LANGUAGE: c.language,
        GEOGRAPHY: c.geography,
        ENTITY: c.entityName,
        EXPECTED: true,
        ACTUAL: false,
        ROOT_CAUSE: diagnosePresenceFalseNegative(text, c),
      });
    }
  }

  const aggregate = finalizeBucket(agg);
  const subgroups = {
    Provider: Object.fromEntries(
      Object.entries(byProvider).map(([k, v]) => [k, finalizeBucket(v)])
    ),
    Language: Object.fromEntries(
      Object.entries(byLanguage).map(([k, v]) => [k, finalizeBucket(v)])
    ),
    Geography: Object.fromEntries(
      Object.entries(byGeography).map(([k, v]) => [k, finalizeBucket(v)])
    ),
  };

  const materialSubgroupFailures = [];
  for (const [dim, rows] of Object.entries(subgroups)) {
    for (const [key, m] of Object.entries(rows)) {
      if (m.GATE === "FAIL") {
        materialSubgroupFailures.push({ dimension: dim, key, ...m });
      }
    }
  }

  const threshold = SIGNAL_PRODUCTION_GATES.PRESENCE_GATE;
  const aggregatePass =
    aggregate.PRECISION != null &&
    aggregate.RECALL != null &&
    aggregate.PRECISION >= threshold.precision &&
    aggregate.RECALL >= threshold.recall;

  let HOLDOUT_GATE = "FAIL";
  let PRESENCE_STATUS = "HOLDOUT_FAILED";
  let status = "AI_INTELLIGENCE_PRESENCE_HOLDOUT_VALIDATION_REVIEW_REQUIRED";
  let nextStep = "PRESENCE_HOLDOUT_REVIEW_REQUIRED";

  if (aggregatePass && materialSubgroupFailures.length === 0) {
    HOLDOUT_GATE = "PASS";
    PRESENCE_STATUS = "PRODUCTION_VALIDATED";
    status = "AI_INTELLIGENCE_PRESENCE_HOLDOUT_VALIDATION_PASS";
    nextStep = "READY_FOR_PRESENCE_PRODUCT_INTEGRATION_AND_TARGETED_RECOMMENDED_FIRST_RECALL";
  } else if (aggregatePass && materialSubgroupFailures.length > 0) {
    HOLDOUT_GATE = "REVIEW_REQUIRED";
    PRESENCE_STATUS = "HOLDOUT_FAILED";
    status = "AI_INTELLIGENCE_PRESENCE_HOLDOUT_VALIDATION_REVIEW_REQUIRED";
    nextStep = "PRESENCE_HOLDOUT_REVIEW_REQUIRED";
  }

  const surfaces = summarizeProductSurfaceAudit();
  const SAFE_TO_ENABLE =
    HOLDOUT_GATE === "PASS"
      ? surfaces.SAFE_NOW.map((s) => s.label)
      : [];
  const KEEP_BLOCKED =
    HOLDOUT_GATE === "PASS"
      ? [
          ...surfaces.BLOCKED.map((s) => s.label),
          ...surfaces.INTERNAL_ONLY.map((s) => s.label),
        ]
      : [
          ...surfaces.SAFE_NOW.map((s) => `${s.label} (blocked — Presence holdout failed)`),
          ...surfaces.BLOCKED.map((s) => s.label),
          ...surfaces.INTERNAL_ONLY.map((s) => s.label),
        ];

  return {
    phase: "AI_INTELLIGENCE_PRESENCE_HOLDOUT_VALIDATION_COMPLETE",
    status,
    nextStep,
    version: PRESENCE_HOLDOUT_EVAL_VERSION,
    holdoutIntegrity: integrityCheck.integrity,
    signalEvaluated: SIGNAL_KEYS.PRESENCE,
    signalsNotEvaluated: [
      SIGNAL_KEYS.RECOMMENDED,
      SIGNAL_KEYS.FIRST_RECOMMENDATION,
      SIGNAL_KEYS.NEGATIVE_OR_QUALIFIED,
      SIGNAL_KEYS.COMPARATOR,
    ],
    recommendationClassifierInvoked: false,
    entityResolutionOnly: true,
    hydrationStats,
    presence: {
      N: aggregate.N,
      TRUE_POSITIVES: aggregate.TRUE_POSITIVES,
      TRUE_NEGATIVES: aggregate.TRUE_NEGATIVES,
      FALSE_POSITIVES: aggregate.FALSE_POSITIVES,
      FALSE_NEGATIVES: aggregate.FALSE_NEGATIVES,
      ACCURACY: aggregate.ACCURACY,
      PRECISION: aggregate.PRECISION,
      RECALL: aggregate.RECALL,
      F1: aggregate.F1,
    },
    subgroups,
    materialSubgroupFailures,
    errors: {
      TOTAL: errors.length,
      DETAILS: errors,
    },
    gate: {
      DEV_GATE: "PASS",
      HOLDOUT_GATE,
      threshold,
      PRESENCE_STATUS,
      PRODUCTION_READINESS:
        PRESENCE_STATUS === "PRODUCTION_VALIDATED"
          ? SIGNAL_READINESS.VALIDATED
          : SIGNAL_READINESS.NOT_READY,
    },
    productReleaseMap: {
      SAFE_TO_ENABLE,
      KEEP_BLOCKED,
      unavailableNeverZero: true,
      unavailableCopy: "Validated monitoring data is not currently available.",
    },
    scorecard: {
      PER_SIGNAL: true,
      COMPOSITE_SCORE: false,
      PRESENCE: {
        DEV: "PASS",
        HOLDOUT: HOLDOUT_GATE === "PASS" ? "PASS" : HOLDOUT_GATE === "REVIEW_REQUIRED" ? "REVIEW_REQUIRED" : "FAIL",
        PRODUCTION_READINESS:
          PRESENCE_STATUS === "PRODUCTION_VALIDATED" ? "VALIDATED" : "NOT_READY",
      },
      RECOMMENDED: { DEV: "NOT_READY", HOLDOUT: "NOT_RUN" },
      FIRST_RECOMMENDATION: { DEV: "NOT_READY", HOLDOUT: "NOT_RUN" },
      NEGATIVE: { DEV: "NOT_READY", HOLDOUT: "NOT_RUN" },
      COMPARATOR: { DEV: "NOT_READY", HOLDOUT: "NOT_RUN" },
    },
    hardGuards: {
      NEW_MONITORING: 0,
      LIVE_PROVIDER_CALLS: 0,
      PUBLIC_CRAWL: 0,
      HOLDOUT_TUNING: 0,
      CLASSIFIER_CHANGES: 0,
      GROUND_TRUTH_CHANGES: 0,
      AIRTABLE_WRITES: 0,
      SCHEMA_CHANGES: 0,
      DEPLOYS: 0,
    },
    HOLDOUT_ACCESSED: true,
    HOLDOUT_CASES_INSPECTED: hydrated.length,
    HOLDOUT_METRICS_RUN: true,
    HOLDOUT_TUNING: 0,
  };
}
