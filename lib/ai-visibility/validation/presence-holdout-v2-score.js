/**
 * One-time Presence Holdout v2 score — AI_SIGNAL_PRESENCE only.
 *
 * Hard guards: no provider calls, no alias/resolver/GT changes, no tuning,
 * no Recommended/First/Negative/Comparator evaluation, no second score run.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { findEntitySpans } from "../normalize-entities.js";
import { buildGoldenSetScoringEntityIndex } from "./golden-set-entity-index.js";
import { sha256Hex } from "./presence-validation-pool-governance.js";
import { MANDATORY_SUBGROUP_MIN_N } from "./classification-threshold.js";
import { SIGNAL_PRODUCTION_GATES } from "./classification-threshold.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const PRESENCE_HOLDOUT_V2_VERSION = "ai_intelligence_presence_holdout_v2";
export const EXPECTED_MANIFEST_HASH =
  "ef45678fb550b5d702dd06e597e6b1cb67d5cc52533b6db2737db07adf567090";
export const EXPECTED_CONTENT_HASH =
  "cbcc3003ad6a43171e33c68a887b73811a75e3b1c777cdaeee9a90bb2e734cf6";

export const DEFAULT_HOLDOUT_V2_MANIFEST = path.join(
  ROOT,
  "data/ai-visibility/validation/ai-intelligence-presence-holdout-v2.json"
);
export const DEFAULT_CANDIDATES_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-candidates/candidates/candidates.json"
);

const PRECISION_THRESHOLD = SIGNAL_PRODUCTION_GATES.PRESENCE_GATE.precision;
const RECALL_THRESHOLD = SIGNAL_PRODUCTION_GATES.PRESENCE_GATE.recall;

function stableStringify(obj) {
  if (obj === null || typeof obj !== "object") return JSON.stringify(obj);
  if (Array.isArray(obj)) return `[${obj.map(stableStringify).join(",")}]`;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(",")}}`;
}

function pct(ratio) {
  if (ratio == null || Number.isNaN(ratio)) return "NOT_MEANINGFUL / INSUFFICIENT_DENOMINATOR";
  return `${(ratio * 100).toFixed(6)}%`;
}

function metricOrInsufficient(numer, denom) {
  if (!(denom > 0)) return { value: null, display: "NOT_MEANINGFUL / INSUFFICIENT_DENOMINATOR" };
  const value = numer / denom;
  return { value, display: pct(value) };
}

function emptyBucket() {
  return { N: 0, positiveN: 0, negativeN: 0, tp: 0, tn: 0, fp: 0, fn: 0, responseIds: new Set() };
}

function finalizeBucket(b, { sparseLabel = false } = {}) {
  const precision = metricOrInsufficient(b.tp, b.tp + b.fp);
  const recall = metricOrInsufficient(b.tp, b.tp + b.fn);
  const specificity = metricOrInsufficient(b.tn, b.tn + b.fp);
  const fpr = metricOrInsufficient(b.fp, b.tn + b.fp);
  const fnr = metricOrInsufficient(b.fn, b.tp + b.fn);
  const accuracy = metricOrInsufficient(b.tp + b.tn, b.N);
  let f1 = { value: null, display: "NOT_MEANINGFUL / INSUFFICIENT_DENOMINATOR" };
  if (precision.value != null && recall.value != null && precision.value + recall.value > 0) {
    const v = (2 * precision.value * recall.value) / (precision.value + recall.value);
    f1 = { value: v, display: pct(v) };
  }

  const sparse = sparseLabel || b.N < MANDATORY_SUBGROUP_MIN_N;
  const material = b.N >= MANDATORY_SUBGROUP_MIN_N;
  let gate = "INSUFFICIENT_N";
  if (material) {
    const pOk = precision.value != null && precision.value >= PRECISION_THRESHOLD;
    const rOk = recall.value != null && recall.value >= RECALL_THRESHOLD;
    gate = pOk && rOk ? "PASS" : "FAIL";
  }

  return {
    N: b.N,
    positiveN: b.positiveN,
    negativeN: b.negativeN,
    UNIQUE_RESPONSE_N: b.responseIds.size,
    TP: b.tp,
    TN: b.tn,
    FP: b.fp,
    FN: b.fn,
    TRUE_POSITIVES: b.tp,
    TRUE_NEGATIVES: b.tn,
    FALSE_POSITIVES: b.fp,
    FALSE_NEGATIVES: b.fn,
    ACCURACY: accuracy.value,
    ACCURACY_PCT: accuracy.display,
    PRECISION: precision.value,
    PRECISION_PCT: precision.display,
    RECALL: recall.value,
    RECALL_PCT: recall.display,
    F1: f1.value,
    F1_PCT: f1.display,
    SPECIFICITY: specificity.value,
    SPECIFICITY_PCT: specificity.display,
    FPR: fpr.value,
    FPR_PCT: fpr.display,
    FNR: fnr.value,
    FNR_PCT: fnr.display,
    SPARSE: sparse,
    MATERIAL: material,
    GATE: gate,
  };
}

function normalizeProvider(p) {
  const s = String(p || "").toLowerCase();
  if (s.includes("openai") || s === "gpt") return "OPENAI";
  if (s.includes("gemini")) return "GEMINI";
  if (s.includes("perplexity")) return "PERPLEXITY";
  if (s.includes("claude") || s.includes("anthropic")) return "CLAUDE";
  return String(p || "UNSPECIFIED").toUpperCase();
}

function normalizeLanguage(l) {
  const s = String(l || "").toLowerCase();
  if (s === "en" || s.startsWith("en")) return "ENGLISH";
  if (s === "es" || s.startsWith("es")) return "SPANISH";
  return String(l || "UNSPECIFIED").toUpperCase();
}

function normalizeGeography(g) {
  const s = String(g || "")
    .toUpperCase()
    .replace(/\s+/g, "_");
  if (s === "GLOBAL") return "GLOBAL";
  if (s === "CALA") return "CALA";
  if (s === "MEXICO" || s === "MX") return "MEXICO";
  if (s === "EUROPE" || s === "EU") return "EUROPE";
  if (s === "NORTH_AMERICA" || s === "NA" || s === "NORTH AMERICA") return "NORTH_AMERICA";
  return String(g || "UNSPECIFIED").toUpperCase();
}

function entityPresentByResolution(text, caseRow, aliasIndex) {
  const spans = findEntitySpans(String(text || ""), aliasIndex);
  const name = caseRow.canonicalEntityName || caseRow.entityName;
  const id = caseRow.canonicalEntityId;
  const matching = spans.filter((s) => {
    if (id && s.entity?.id === id) return true;
    if (name && s.entity?.name === name) return true;
    return false;
  });
  return { present: matching.length > 0, spans, matching };
}

function excerptAround(text, start, end, pad = 80) {
  const t = String(text || "");
  const a = Math.max(0, start - pad);
  const b = Math.min(t.length, end + pad);
  return t.slice(a, b).replace(/\s+/g, " ").trim();
}

function diagnoseRootCause({ text, caseRow, expected, actual, matching, allSpans }) {
  const raw = String(text || "");
  const name = String(caseRow.canonicalEntityName || "");
  const rationale = String(caseRow.systemSuggestionRationale || "").toLowerCase();
  const negType = String(caseRow.negativeControlType || "");
  const nameRe = name
    ? new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i")
    : null;
  const hasCanonicalString = !!(nameRe && nameRe.test(raw));

  if (!expected && actual) {
    if (/playa/i.test(name) || /playa/.test(rationale) || negType.includes("PLAYA")) {
      return "geographic false friend";
    }
    if (/sibling|parent|hilton|marriott/.test(rationale) || /PARENT|SIBLING/.test(negType)) {
      return /sibling/i.test(rationale) || negType.includes("SIBLING")
        ? "sibling ambiguity"
        : "parent/child ambiguity";
    }
    if (/generic collection|autograph|curio|tapestry|tribute|luxury collection/i.test(rationale + name)) {
      return "generic false friend";
    }
    if (matching?.[0]?.matchedAlias && matching[0].matchedAlias !== name) {
      return "alias gap";
    }
    return "other";
  }

  if (expected && !actual) {
    if (/playa/i.test(name) && /playa del carmen|en playa|de playa/i.test(raw) && !hasCanonicalString) {
      return "geographic false friend";
    }
    if (hasCanonicalString) {
      return "normalization gap";
    }
    if (!hasCanonicalString && allSpans?.some((s) => /hilton|marriott/i.test(s.entity?.name || ""))) {
      return "parent/child ambiguity";
    }
    if (!hasCanonicalString) return "alias gap";
    return "other";
  }

  return "other";
}

function surfaceForError({ text, caseRow, expected, actual, matching, allSpans }) {
  const raw = String(text || "");
  if (!expected && actual && matching?.[0]) {
    return excerptAround(raw, matching[0].start, matching[0].end);
  }
  if (expected && !actual) {
    const name = String(caseRow.canonicalEntityName || "");
    if (name) {
      const m = raw.match(new RegExp(`.{0,60}${name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}.{0,60}`, "i"));
      if (m) return m[0].replace(/\s+/g, " ").trim();
    }
    // nearest sibling/false-friend span if any
    const hit = (allSpans || []).find((s) =>
      /playa|hilton|marriott|autograph|curio|collection/i.test(s.rawMention || s.entity?.name || "")
    );
    if (hit) return excerptAround(raw, hit.start, hit.end);
  }
  return raw.slice(0, 200).replace(/\s+/g, " ").trim();
}

/**
 * Recompute seal hashes from sealed manifest (pre-score core).
 */
export function verifyHoldoutV2Seal(manifest, options = {}) {
  const expectedManifestHash = options.expectedManifestHash || EXPECTED_MANIFEST_HASH;
  const expectedContentHash = options.expectedContentHash || EXPECTED_CONTENT_HASH;

  const cases = [...(manifest.cases || [])].sort((a, b) =>
    String(a.caseId).localeCompare(String(b.caseId))
  );
  const contentBody = {
    caseIds: cases.map((c) => c.caseId),
    sourceResponseIds: [...(manifest.sourceResponseIds || [])].sort(),
    humanFinalLabels: cases.map((c) => ({
      caseId: c.caseId,
      label: c.humanFinalLabel,
    })),
    presentCount: manifest.presentCount,
    notPresentCount: manifest.notPresentCount,
  };
  const recomputedContentHash = sha256Hex(stableStringify(contentBody));

  const {
    manifestHash: _mh,
    HOLDOUT_V2_SEALED: _sealed,
    sealedAt: _at,
    scoredAt: _scoredAt,
    scoreReportPath: _srp,
    scoreResults: _sr,
    postScoreSealNote: _psn,
    oneTimeScoreGuards: _otg,
    ...coreCandidate
  } = manifest;

  // Seal was computed on READY_UNSCORED / UNTOUCHED=true / SCORED=false core.
  // If file was already mutated post-score, reconstruct pre-score core for check.
  const coreForHash = {
    ...coreCandidate,
    STATUS: "READY_UNSCORED",
    UNTOUCHED: true,
    SCORED: false,
    USED_FOR_TUNING: false,
    PREDICTIONS_EXPOSED: false,
    HOLDOUT_V2_SCORING: 0,
    scorecard: {
      PRESENCE_DEV: "PASS",
      HOLDOUT_V1: "INSPECTED_DIAGNOSTIC",
      HOLDOUT_V2: "READY_UNSCORED",
      PRESENCE_PRODUCTION_READINESS: "NOT_READY",
      RECOMMENDED: "NOT_READY",
      FIRST_RECOMMENDATION: "NOT_READY",
      NEGATIVE: "NOT_READY",
      COMPARATOR: "NOT_READY",
    },
    regionalization: {
      STATUS: "PLANNED_AFTER_PRESENCE_CERTIFICATION",
      EXECUTED: false,
      PROVIDER_CALLS: 0,
    },
    hardGuards: {
      HOLDOUT_V2_SCORING: 0,
      PREDICTIONS_EXPOSED: 0,
      ENTITY_RESOLVER_CHANGES: 0,
      ALIAS_CHANGES: 0,
      GROUND_TRUTH_CHANGES: 0,
      HUMAN_LABEL_CHANGES: 0,
      PROVIDER_CALLS: 0,
    },
  };

  const recomputedManifestHash = sha256Hex(stableStringify(coreForHash));

  const sealedBeforeScore =
    manifest.UNTOUCHED === true &&
    manifest.SCORED === false &&
    manifest.USED_FOR_TUNING === false &&
    manifest.PREDICTIONS_EXPOSED === false &&
    (manifest.HOLDOUT_V2_SEALED === true || manifest.HOLDOUT_V2_SEALED === "YES");

  const contentMatch = recomputedContentHash === expectedContentHash;
  const manifestMatch = recomputedManifestHash === expectedManifestHash;
  const storedContentMatch = manifest.contentHash === expectedContentHash;
  const storedManifestMatch = manifest.manifestHash === expectedManifestHash;

  const ok =
    contentMatch &&
    manifestMatch &&
    storedContentMatch &&
    storedManifestMatch &&
    sealedBeforeScore;

  return {
    ok,
    MANIFEST_HASH_MATCH: manifestMatch && storedManifestMatch ? "YES" : "NO",
    CONTENT_HASH_MATCH: contentMatch && storedContentMatch ? "YES" : "NO",
    SEALED_BEFORE_SCORE: sealedBeforeScore ? "YES" : "NO",
    recomputedManifestHash,
    recomputedContentHash,
    expectedManifestHash,
    expectedContentHash,
    storedManifestHash: manifest.manifestHash || null,
    storedContentHash: manifest.contentHash || null,
    UNTOUCHED: manifest.UNTOUCHED === true ? "YES" : "NO",
    SCORED: manifest.SCORED === true ? "YES" : "NO",
    USED_FOR_TUNING: manifest.USED_FOR_TUNING === true ? "YES" : "NO",
    PREDICTIONS_EXPOSED: manifest.PREDICTIONS_EXPOSED === true ? "YES" : "NO",
    stopReason: ok ? null : "HOLDOUT_SEAL_VERIFICATION_FAILED",
  };
}

/**
 * One-time Presence Holdout v2 score.
 */
export function runPresenceHoldoutV2OneTimeScore(options = {}) {
  const manifestPath = options.manifestPath || DEFAULT_HOLDOUT_V2_MANIFEST;
  const candidatesPath = options.candidatesPath || DEFAULT_CANDIDATES_PATH;
  const persist = options.persist !== false;

  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));

  if (manifest.SCORED === true) {
    return {
      phase: "PRESENCE_HOLDOUT_V2_ONE_TIME_SCORE_COMPLETE",
      status: "HOLDOUT_SEAL_VERIFICATION_FAILED",
      stopReason: "SECOND_SCORE_RUN_FORBIDDEN — Holdout v2 already SCORED=YES",
      SEALED_BEFORE_SCORE: "NO",
      hardGuards: { SECOND_SCORE_RUN: 1 },
    };
  }

  const seal = verifyHoldoutV2Seal(manifest);
  if (!seal.ok) {
    return {
      phase: "PRESENCE_HOLDOUT_V2_ONE_TIME_SCORE_COMPLETE",
      status: "HOLDOUT_SEAL_VERIFICATION_FAILED",
      stopReason: seal.stopReason,
      seal,
      hardGuards: {
        PROVIDER_CALLS: 0,
        ENTITY_RESOLVER_CHANGES: 0,
        ALIAS_CHANGES: 0,
        GROUND_TRUTH_CHANGES: 0,
        HUMAN_LABEL_CHANGES: 0,
        HOLDOUT_RESELECTION: 0,
        SECOND_SCORE_RUN: 0,
        RECOMMENDED_WORK: 0,
        FIRST_RECOMMENDATION_WORK: 0,
        REGIONALIZATION_EXECUTION: 0,
        AIRTABLE_WRITES: 0,
        DEPLOYS: 0,
      },
    };
  }

  const candDoc = JSON.parse(fs.readFileSync(candidatesPath, "utf8"));
  const byId = new Map((candDoc.cases || []).map((c) => [c.caseId, c]));
  const index = buildGoldenSetScoringEntityIndex({});

  const cases = [...(manifest.cases || [])].sort((a, b) =>
    String(a.caseId).localeCompare(String(b.caseId))
  );

  const agg = emptyBucket();
  const byProvider = {};
  const byLanguage = {};
  const byGeography = {};
  const errors = [];
  const missingText = [];

  for (const row of cases) {
    const live = byId.get(row.caseId);
    const text = live?.rawText || "";
    if (!text) {
      missingText.push(row.caseId);
      continue;
    }
    if (row.responseHash && live.responseHash && row.responseHash !== live.responseHash) {
      missingText.push(`${row.caseId}:RESPONSE_HASH_MISMATCH`);
      continue;
    }

    const expected = row.humanFinalLabel === "PRESENT";
    const { present: actual, spans, matching } = entityPresentByResolution(text, row, index.aliasIndex);

    const provider = normalizeProvider(row.provider);
    const language = normalizeLanguage(row.language);
    const geography = normalizeGeography(row.geography);

    for (const [map, key] of [
      [byProvider, provider],
      [byLanguage, language],
      [byGeography, geography],
    ]) {
      if (!map[key]) map[key] = emptyBucket();
    }

    const buckets = [agg, byProvider[provider], byLanguage[language], byGeography[geography]];
    for (const b of buckets) {
      b.N += 1;
      if (expected) b.positiveN += 1;
      else b.negativeN += 1;
      if (row.sourceResponseId) b.responseIds.add(row.sourceResponseId);
    }

    if (expected && actual) {
      for (const b of buckets) b.tp += 1;
    } else if (!expected && !actual) {
      for (const b of buckets) b.tn += 1;
    } else if (!expected && actual) {
      for (const b of buckets) b.fp += 1;
      errors.push({
        CASE_ID: row.caseId,
        SOURCE_RESPONSE_ID: row.sourceResponseId,
        PROVIDER: provider,
        LANGUAGE: language,
        GEOGRAPHY: geography,
        CANONICAL_ENTITY: row.canonicalEntityName,
        EXPECTED: "NOT_PRESENT",
        ACTUAL: "PRESENT",
        EXPECTED_BOOL: false,
        ACTUAL_BOOL: true,
        ERROR_TYPE: "FALSE_POSITIVE",
        EXACT_RELEVANT_SURFACE_TEXT: surfaceForError({
          text,
          caseRow: row,
          expected,
          actual,
          matching,
          allSpans: spans,
        }),
        ROOT_CAUSE_CATEGORY: diagnoseRootCause({
          text,
          caseRow: row,
          expected,
          actual,
          matching,
          allSpans: spans,
        }),
        MATCHED_ALIAS: matching?.[0]?.matchedAlias || null,
      });
    } else {
      for (const b of buckets) b.fn += 1;
      errors.push({
        CASE_ID: row.caseId,
        SOURCE_RESPONSE_ID: row.sourceResponseId,
        PROVIDER: provider,
        LANGUAGE: language,
        GEOGRAPHY: geography,
        CANONICAL_ENTITY: row.canonicalEntityName,
        EXPECTED: "PRESENT",
        ACTUAL: "NOT_PRESENT",
        EXPECTED_BOOL: true,
        ACTUAL_BOOL: false,
        ERROR_TYPE: "FALSE_NEGATIVE",
        EXACT_RELEVANT_SURFACE_TEXT: surfaceForError({
          text,
          caseRow: row,
          expected,
          actual,
          matching,
          allSpans: spans,
        }),
        ROOT_CAUSE_CATEGORY: diagnoseRootCause({
          text,
          caseRow: row,
          expected,
          actual,
          matching,
          allSpans: spans,
        }),
        MATCHED_ALIAS: null,
      });
    }
  }

  if (missingText.length || agg.N !== 100) {
    return {
      phase: "PRESENCE_HOLDOUT_V2_ONE_TIME_SCORE_COMPLETE",
      status: "HOLDOUT_SEAL_VERIFICATION_FAILED",
      stopReason: `SCORING_INPUT_INTEGRITY_FAILED missingOrMismatch=${missingText.length} scoredN=${agg.N}`,
      missingText,
      seal,
    };
  }

  // Lock aggregate before error audit (audit already collected without mutating logic)
  const aggregate = finalizeBucket(agg);
  const confusionSum = aggregate.TP + aggregate.TN + aggregate.FP + aggregate.FN;

  const providers = Object.fromEntries(
    ["OPENAI", "GEMINI", "PERPLEXITY", "CLAUDE"].map((k) => [
      k,
      finalizeBucket(byProvider[k] || emptyBucket()),
    ])
  );
  const languages = Object.fromEntries(
    ["ENGLISH", "SPANISH"].map((k) => [k, finalizeBucket(byLanguage[k] || emptyBucket())])
  );
  const geographies = Object.fromEntries(
    ["GLOBAL", "CALA", "MEXICO", "EUROPE", "NORTH_AMERICA"].map((k) => [
      k,
      finalizeBucket(byGeography[k] || emptyBucket(), {
        sparseLabel: (byGeography[k]?.N || 0) < MANDATORY_SUBGROUP_MIN_N,
      }),
    ])
  );

  const materialSubgroupFailures = [];
  for (const [dim, rows] of [
    ["Provider", providers],
    ["Language", languages],
    ["Geography", geographies],
  ]) {
    for (const [key, m] of Object.entries(rows)) {
      if (m.MATERIAL && m.GATE === "FAIL") {
        materialSubgroupFailures.push({ dimension: dim, key, N: m.N, GATE: m.GATE });
      }
    }
  }

  // Meaningful pattern flags (do not auto-fail solely on sparse cells)
  const reviewFlags = [];
  for (const [key, m] of Object.entries(geographies)) {
    if (m.SPARSE && (m.FP > 0 || m.FN > 0)) {
      reviewFlags.push({
        type: "SPARSE_GEO_ERROR",
        geography: key,
        N: m.N,
        FP: m.FP,
        FN: m.FN,
        note: "Sparse subgroup error — flag only; not sole global fail reason",
      });
    }
  }

  const aggregatePass =
    aggregate.PRECISION != null &&
    aggregate.RECALL != null &&
    aggregate.PRECISION >= PRECISION_THRESHOLD &&
    aggregate.RECALL >= RECALL_THRESHOLD &&
    confusionSum === 100;

  let PRESENCE_HOLDOUT_V2_GATE = "FAIL";
  let certification = "PRESENCE_HOLDOUT_V2_CERTIFICATION_FAIL";
  let nextStep = "PRESENCE_HOLDOUT_V2_FAILURE_REMEDIATION_REQUIRED";
  let HOLDOUT_V2_SCORECARD = "FAIL";
  let PRESENCE_PRODUCTION_READINESS = "NOT_READY";
  let regionalizationStatus = "PLANNED_AFTER_PRESENCE_CERTIFICATION_BLOCKED";

  if (aggregatePass && materialSubgroupFailures.length === 0) {
    PRESENCE_HOLDOUT_V2_GATE = "PASS";
    certification = "PRESENCE_HOLDOUT_V2_CERTIFICATION_PASS";
    nextStep = "READY_FOR_PRESENCE_PRODUCT_INTEGRATION_AND_STAGE_1_REGIONALIZATION";
    HOLDOUT_V2_SCORECARD = "PASS";
    PRESENCE_PRODUCTION_READINESS = "PRODUCTION_VALIDATED";
    regionalizationStatus = "READY_FOR_STAGE_1_OPENAI_REGIONALIZATION_EXPERIMENT";
  } else if (aggregatePass && materialSubgroupFailures.length > 0) {
    PRESENCE_HOLDOUT_V2_GATE = "REVIEW_REQUIRED";
    certification = "PRESENCE_HOLDOUT_V2_CERTIFICATION_REVIEW_REQUIRED";
    nextStep = "PRESENCE_HOLDOUT_V2_FAILURE_REMEDIATION_REQUIRED";
    HOLDOUT_V2_SCORECARD = "REVIEW_REQUIRED";
    PRESENCE_PRODUCTION_READINESS = "NOT_READY";
    regionalizationStatus = "PLANNED_AFTER_PRESENCE_CERTIFICATION_BLOCKED";
  }

  const SAFE_TO_ENABLE =
    PRESENCE_HOLDOUT_V2_GATE === "PASS"
      ? [
          "AI Presence",
          "Regional Presence",
          "Competitive Position based on Presence",
          "Questions Missing",
          "Presence trends/change where enough real comparable monitoring periods exist",
        ]
      : [];
  const KEEP_BLOCKED =
    PRESENCE_HOLDOUT_V2_GATE === "PASS"
      ? [
          "Recommendation Share",
          "First Recommendation Rate",
          "Questions Won",
          "Recommended claims",
          "First Recommendation claims",
          "Negative claims",
          "Comparator claims",
        ]
      : [
          "AI Presence",
          "Regional Presence",
          "Competitive Position based on Presence",
          "Questions Missing",
          "Presence trends/change",
          "Recommendation Share",
          "First Recommendation Rate",
          "Questions Won",
          "Recommended claims",
          "First Recommendation claims",
          "Negative claims",
          "Comparator claims",
        ];

  const falsePositives = errors.filter((e) => e.ERROR_TYPE === "FALSE_POSITIVE");
  const falseNegatives = errors.filter((e) => e.ERROR_TYPE === "FALSE_NEGATIVE");

  const scoredAt = new Date().toISOString();
  const report = {
    phase: "PRESENCE_HOLDOUT_V2_ONE_TIME_SCORE_COMPLETE",
    status: certification,
    nextStep,
    VERSION: PRESENCE_HOLDOUT_V2_VERSION,
    scoredAt,
    seal: {
      MANIFEST_HASH_MATCH: seal.MANIFEST_HASH_MATCH,
      CONTENT_HASH_MATCH: seal.CONTENT_HASH_MATCH,
      SEALED_BEFORE_SCORE: seal.SEALED_BEFORE_SCORE,
      MANIFEST_HASH: seal.expectedManifestHash,
      CONTENT_HASH: seal.expectedContentHash,
    },
    holdout: {
      PAIR_N: 100,
      UNIQUE_RESPONSE_N: manifest.uniqueResponseCount,
      PRESENT_N: manifest.presentCount,
      NOT_PRESENT_N: manifest.notPresentCount,
    },
    confusionMatrix: {
      TP: aggregate.TP,
      TN: aggregate.TN,
      FP: aggregate.FP,
      FN: aggregate.FN,
      SUM: confusionSum,
      SUM_OK: confusionSum === 100,
    },
    metrics: {
      ACCURACY: aggregate.ACCURACY,
      ACCURACY_PCT: aggregate.ACCURACY_PCT,
      PRECISION: aggregate.PRECISION,
      PRECISION_PCT: aggregate.PRECISION_PCT,
      RECALL: aggregate.RECALL,
      RECALL_PCT: aggregate.RECALL_PCT,
      F1: aggregate.F1,
      F1_PCT: aggregate.F1_PCT,
      SPECIFICITY: aggregate.SPECIFICITY,
      SPECIFICITY_PCT: aggregate.SPECIFICITY_PCT,
      FPR: aggregate.FPR,
      FPR_PCT: aggregate.FPR_PCT,
      FNR: aggregate.FNR,
      FNR_PCT: aggregate.FNR_PCT,
    },
    productionGate: {
      PRECISION_THRESHOLD: "98%",
      RECALL_THRESHOLD: "98%",
      PRECISION_THRESHOLD_RAW: PRECISION_THRESHOLD,
      RECALL_THRESHOLD_RAW: RECALL_THRESHOLD,
      PRESENCE_HOLDOUT_V2_GATE,
      materialSubgroupFailures,
      reviewFlags,
    },
    providers,
    languages,
    geographies,
    errors: {
      TOTAL_ERRORS: errors.length,
      FALSE_POSITIVES: falsePositives,
      FALSE_NEGATIVES: falseNegatives,
      DETAILS: errors,
    },
    scorecard: {
      PRESENCE_DEV: "PASS",
      HOLDOUT_V1: "INSPECTED_DIAGNOSTIC",
      HOLDOUT_V2: HOLDOUT_V2_SCORECARD,
      PRESENCE_PRODUCTION_READINESS,
      RECOMMENDED: "NOT_READY",
      FIRST_RECOMMENDATION: "NOT_READY",
      NEGATIVE: "NOT_READY",
      COMPARATOR: "NOT_READY",
    },
    productReleaseMap: {
      SAFE_TO_ENABLE,
      KEEP_BLOCKED,
      unavailableNeverZero: true,
    },
    regionalization: {
      STATUS: regionalizationStatus,
      EXECUTED: false,
      PROVIDER_CALLS: 0,
      note:
        PRESENCE_HOLDOUT_V2_GATE === "PASS"
          ? "May advance to Stage 1 OpenAI regionalization experiment — NOT executed in this scoring phase"
          : "Remains planned but blocked from client-facing use until Presence remediation",
    },
    holdoutStateAfterScore: {
      SCORED: "YES",
      UNTOUCHED: "NO",
      USED_FOR_TUNING: "NO",
      PREDICTIONS_EXPOSED: "NO",
    },
    hardGuards: {
      PROVIDER_CALLS: 0,
      ENTITY_RESOLVER_CHANGES: 0,
      ALIAS_CHANGES: 0,
      GROUND_TRUTH_CHANGES: 0,
      HUMAN_LABEL_CHANGES: 0,
      HOLDOUT_RESELECTION: 0,
      SECOND_SCORE_RUN: 0,
      RECOMMENDED_WORK: 0,
      FIRST_RECOMMENDATION_WORK: 0,
      REGIONALIZATION_EXECUTION: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
      HOLDOUT_TUNING: 0,
    },
    signalEvaluated: "AI_SIGNAL_PRESENCE",
    signalsNotEvaluated: [
      "Recommended",
      "First Recommendation",
      "Negative",
      "Comparator",
      "10-class recommendation taxonomy",
    ],
  };

  if (persist) {
    const reportPath = path.join(
      ROOT,
      "data/ai-visibility/validation/presence-holdout-v2-one-time-score.json"
    );
    fs.mkdirSync(path.dirname(reportPath), { recursive: true });
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2) + "\n", "utf8");

    const updatedManifest = {
      ...manifest,
      STATUS: HOLDOUT_V2_SCORECARD === "PASS" ? "SCORED_PASS" : `SCORED_${HOLDOUT_V2_SCORECARD}`,
      UNTOUCHED: false,
      SCORED: true,
      USED_FOR_TUNING: false,
      PREDICTIONS_EXPOSED: false,
      HOLDOUT_V2_SCORING: 1,
      scoredAt,
      scoreReportPath: path.relative(ROOT, reportPath).replace(/\\/g, "/"),
      scoreResults: {
        PRESENCE_HOLDOUT_V2_GATE,
        TP: aggregate.TP,
        TN: aggregate.TN,
        FP: aggregate.FP,
        FN: aggregate.FN,
        PRECISION: aggregate.PRECISION,
        RECALL: aggregate.RECALL,
        ACCURACY: aggregate.ACCURACY,
        F1: aggregate.F1,
      },
      scorecard: report.scorecard,
      regionalization: {
        STATUS: regionalizationStatus,
        EXECUTED: false,
        PROVIDER_CALLS: 0,
      },
      postScoreSealNote:
        "Original manifestHash/contentHash remain the pre-score seal. After this one-time score, UNTOUCHED=NO and SCORED=YES permanently.",
      oneTimeScoreGuards: report.hardGuards,
      // Preserve original seal hashes
      contentHash: EXPECTED_CONTENT_HASH,
      manifestHash: EXPECTED_MANIFEST_HASH,
      HOLDOUT_V2_SEALED: true,
    };
    fs.writeFileSync(manifestPath, JSON.stringify(updatedManifest, null, 2) + "\n", "utf8");
    report.persisted = {
      reportPath: path.relative(ROOT, reportPath).replace(/\\/g, "/"),
      manifestPath: path.relative(ROOT, manifestPath).replace(/\\/g, "/"),
    };
  }

  return report;
}
