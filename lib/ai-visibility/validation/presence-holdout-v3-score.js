/**
 * One-time Presence Holdout v3 score — AI_SIGNAL_PRESENCE only.
 *
 * Hard guards: no provider calls, no alias/resolver/GT changes, no tuning,
 * no Recommended/First/Negative/Comparator evaluation, no second score run.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { findEntitySpans, RESOLVER_VERSION } from "../normalize-entities.js";
import { buildGoldenSetScoringEntityIndex } from "./golden-set-entity-index.js";
import { sha256Hex } from "./presence-validation-pool-governance.js";
import { validateHoldoutManifestIntegrity } from "./holdout-manifest-integrity.js";
import { MANDATORY_SUBGROUP_MIN_N, SIGNAL_PRODUCTION_GATES } from "./classification-threshold.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const PRESENCE_HOLDOUT_V3_VERSION = "ai_intelligence_presence_holdout_v3";
export const EXPECTED_MANIFEST_HASH =
  "a67b79dee7716b086cf30a88b35feb66e04c4737b1f3128a4673dd173b47dcae";
export const EXPECTED_CONTENT_HASH =
  "155a6ad121809c38de29afdaba96fab4f891836e5bddd26907f450bf9fb7ba44";

export const DEFAULT_HOLDOUT_V3_MANIFEST = path.join(
  ROOT,
  "data/ai-visibility/validation/ai-intelligence-presence-holdout-v3.json"
);
export const DEFAULT_CANDIDATES_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-candidates/candidates/candidates.json"
);
export const DEFAULT_SCORE_LOCK_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-holdout-v3-score-lock.json"
);
export const DEFAULT_SCORE_REPORT_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-holdout-v3-one-time-score.json"
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
    if (/canopy|short.?name|bare\b/.test(rationale + name.toLowerCase())) {
      return "short-name ambiguity";
    }
    return "other";
  }

  if (expected && !actual) {
    if (/playa/i.test(name) && /playa del carmen|en playa|de playa/i.test(raw) && !hasCanonicalString) {
      return "geographic false friend";
    }
    if (hasCanonicalString) return "normalization gap";
    if (!hasCanonicalString && allSpans?.some((s) => /hilton|marriott/i.test(s.entity?.name || ""))) {
      return "parent/child ambiguity";
    }
    if (/canopy|curio|tapestry/i.test(name) && !hasCanonicalString) {
      return "contextual identity gap";
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
      const m = raw.match(
        new RegExp(`.{0,60}${name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}.{0,60}`, "i")
      );
      if (m) return m[0].replace(/\s+/g, " ").trim();
    }
    const hit = (allSpans || []).find((s) =>
      /playa|hilton|marriott|autograph|curio|collection|canopy/i.test(
        s.rawMention || s.entity?.name || ""
      )
    );
    if (hit) return excerptAround(raw, hit.start, hit.end);
  }
  return raw.slice(0, 200).replace(/\s+/g, " ").trim();
}

function isShortenedNameCase(row, text) {
  if (row.humanFinalLabel !== "PRESENT") return false;
  const name = String(row.canonicalEntityName || "");
  const raw = String(text || "");
  const shortToken = name
    .replace(/\s+by\s+(Hilton|Marriott|IHG|Hyatt).*$/i, "")
    .replace(/\s+Collection.*$/i, "")
    .replace(/\s+Hotels.*$/i, "")
    .trim();
  const hasFull = name
    ? new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(raw)
    : false;
  const hasShort =
    shortToken.length >= 4 &&
    new RegExp(`\\b${shortToken.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i").test(raw);
  return hasShort && !hasFull;
}

function isParentContextCase(row) {
  const rat = String(row.systemSuggestionRationale || "").toLowerCase();
  return /sibling|parent|family context|without this specific/.test(rat);
}

function isBrandFamilyListCase(text) {
  const raw = String(text || "");
  return (
    /(hilton|marriott|ihg|hyatt).{0,80}(canopy|curio|tapestry|autograph|kimpton|indigo)/i.test(
      raw
    ) ||
    /(canopy|curio|tapestry|autograph|kimpton|indigo).{0,80}(hilton|marriott|ihg)/i.test(raw)
  );
}

function isCommonLanguageCollisionCase(row, text) {
  const name = String(row.canonicalEntityName || "");
  const raw = String(text || "");
  return (
    (/\bplaya\b/i.test(raw) && !/playa\s+hotels/i.test(raw)) ||
    (/\bcanopy\b/i.test(raw) && !/canopy\s+by\s+hilton/i.test(raw) && /canopy/i.test(name))
  );
}

/**
 * Verify Holdout v3 seal before any prediction.
 */
export function verifyHoldoutV3Seal(manifest, options = {}) {
  const expectedManifestHash = options.expectedManifestHash || EXPECTED_MANIFEST_HASH;
  const expectedContentHash = options.expectedContentHash || EXPECTED_CONTENT_HASH;

  const cases = [...(manifest.cases || [])].sort((a, b) =>
    String(a.caseId).localeCompare(String(b.caseId))
  );
  const integrity = validateHoldoutManifestIntegrity(cases);

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
    HOLDOUT_V3_SEALED: _sealed,
    sealedAt: _at,
    scoredAt: _scoredAt,
    scoreReportPath: _srp,
    scoreResults: _sr,
    scoreLockPath: _slp,
    postScoreSealNote: _psn,
    oneTimeScoreGuards: _otg,
    ...coreCandidate
  } = manifest;

  const coreForHash = {
    ...coreCandidate,
    STATUS: "READY_UNSCORED",
    SCORED: false,
    USED_FOR_TUNING: false,
    PREDICTIONS_EXPOSED: false,
    FRESH_RESPONSES: true,
    NOT_USED_FOR_TUNING: true,
    UNSCORED: true,
    HOLDOUT_V3_SCORING: 0,
    scorecard: {
      PRESENCE_DEV: "PASS",
      HOLDOUT_V1: "INSPECTED_DIAGNOSTIC",
      HOLDOUT_V2: "SCORED_FAIL",
      HOLDOUT_V3: "READY_UNSCORED",
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
      HOLDOUT_V3_SCORING: 0,
      PREDICTIONS_EXPOSED: 0,
      ENTITY_RESOLVER_CHANGES: 0,
      ALIAS_CHANGES: 0,
      GROUND_TRUTH_CHANGES: 0,
      HUMAN_LABEL_CHANGES: 0,
      HOLDOUT_V2_CHANGES: 0,
      HOLDOUT_V2_RESCORE: 0,
      PROVIDER_CALLS: 0,
      REGIONALIZATION_EXECUTION: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
    },
  };

  const recomputedManifestHash = sha256Hex(stableStringify(coreForHash));

  const sealedBeforeScore =
    manifest.FRESH_RESPONSES === true &&
    manifest.SCORED === false &&
    manifest.USED_FOR_TUNING === false &&
    manifest.PREDICTIONS_EXPOSED === false &&
    (manifest.HOLDOUT_V3_SEALED === true || manifest.HOLDOUT_V3_SEALED === "YES") &&
    manifest.pairCount === 100 &&
    integrity.UNIQUE_CASE_ID_COUNT === 100 &&
    integrity.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT === 100 &&
    integrity.ok;

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
    MANIFEST_INTEGRITY: integrity.ok ? "PASS" : "FAIL",
    SEALED_BEFORE_SCORE: sealedBeforeScore ? "YES" : "NO",
    recomputedManifestHash,
    recomputedContentHash,
    expectedManifestHash,
    expectedContentHash,
    storedManifestHash: manifest.manifestHash || null,
    storedContentHash: manifest.contentHash || null,
    PAIR_N: manifest.pairCount,
    UNIQUE_CASE_ID_COUNT: integrity.UNIQUE_CASE_ID_COUNT,
    UNIQUE_ENTITY_RESPONSE_PAIR_COUNT: integrity.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT,
    FRESH_RESPONSES: manifest.FRESH_RESPONSES === true ? "YES" : "NO",
    USED_FOR_TUNING: manifest.USED_FOR_TUNING === true ? "YES" : "NO",
    PREDICTIONS_EXPOSED: manifest.PREDICTIONS_EXPOSED === true ? "YES" : "NO",
    SCORED: manifest.SCORED === true ? "YES" : "NO",
    HOLDOUT_V3_SEALED:
      manifest.HOLDOUT_V3_SEALED === true || manifest.HOLDOUT_V3_SEALED === "YES"
        ? "YES"
        : "NO",
    stopReason: ok ? null : "HOLDOUT_V3_SEAL_VERIFICATION_FAILED",
  };
}

/**
 * One-time Presence Holdout v3 score.
 */
export function runPresenceHoldoutV3OneTimeScore(options = {}) {
  const manifestPath = options.manifestPath || DEFAULT_HOLDOUT_V3_MANIFEST;
  const candidatesPath = options.candidatesPath || DEFAULT_CANDIDATES_PATH;
  const persist = options.persist !== false;
  const scoreLockPath = options.scoreLockPath || DEFAULT_SCORE_LOCK_PATH;
  const reportPath = options.reportPath || DEFAULT_SCORE_REPORT_PATH;

  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));

  if (manifest.SCORED === true) {
    return {
      phase: "PRESENCE_HOLDOUT_V3_ONE_TIME_SCORE_COMPLETE",
      status: "HOLDOUT_V3_SEAL_VERIFICATION_FAILED",
      stopReason: "SECOND_SCORE_RUN_FORBIDDEN — Holdout v3 already SCORED=YES",
      SEALED_BEFORE_SCORE: "NO",
      hardGuards: { SECOND_SCORE_RUN: 1, HOLDOUT_V3_RESCORE: 1 },
    };
  }

  const seal = verifyHoldoutV3Seal(manifest);
  if (!seal.ok) {
    return {
      phase: "PRESENCE_HOLDOUT_V3_ONE_TIME_SCORE_COMPLETE",
      status: "HOLDOUT_V3_SEAL_VERIFICATION_FAILED",
      stopReason: seal.stopReason,
      seal,
      hardGuards: {
        PROVIDER_CALLS: 0,
        ENTITY_RESOLVER_CHANGES: 0,
        ALIAS_CHANGES: 0,
        GROUND_TRUTH_CHANGES: 0,
        HUMAN_LABEL_CHANGES: 0,
        HOLDOUT_V3_RESELECTION: 0,
        HOLDOUT_V3_RESCORE: 0,
        SECOND_SCORE_RUN: 0,
        HOLDOUT_V2_CHANGES: 0,
        HOLDOUT_V2_RESCORE: 0,
        RECOMMENDED_WORK: 0,
        FIRST_RECOMMENDATION_WORK: 0,
        REGIONALIZATION_EXECUTION: 0,
        AIRTABLE_WRITES: 0,
        DEPLOYS: 0,
      },
    };
  }

  if (RESOLVER_VERSION !== "ai_visibility_entity_resolver_v2_1_contextual") {
    return {
      phase: "PRESENCE_HOLDOUT_V3_ONE_TIME_SCORE_COMPLETE",
      status: "HOLDOUT_V3_SEAL_VERIFICATION_FAILED",
      stopReason: `RESOLVER_VERSION_MISMATCH:${RESOLVER_VERSION}`,
      seal,
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
  const rowOutcomes = [];
  const missingText = [];

  // Pass 1 — score only (collect outcomes; do not inspect error narratives yet)
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
    const { present: actual, spans, matching } = entityPresentByResolution(
      text,
      row,
      index.aliasIndex
    );

    const provider = normalizeProvider(row.provider);
    const language = normalizeLanguage(row.language);
    const geography = normalizeGeography(row.geography);
    const model = live?.model || row.model || null;

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

    let outcome = "TN";
    if (expected && actual) {
      for (const b of buckets) b.tp += 1;
      outcome = "TP";
    } else if (!expected && !actual) {
      for (const b of buckets) b.tn += 1;
      outcome = "TN";
    } else if (!expected && actual) {
      for (const b of buckets) b.fp += 1;
      outcome = "FP";
    } else {
      for (const b of buckets) b.fn += 1;
      outcome = "FN";
    }

    rowOutcomes.push({
      row,
      live,
      text,
      expected,
      actual,
      spans,
      matching,
      provider,
      language,
      geography,
      model,
      outcome,
      correct: outcome === "TP" || outcome === "TN",
    });
  }

  if (missingText.length || agg.N !== 100) {
    return {
      phase: "PRESENCE_HOLDOUT_V3_ONE_TIME_SCORE_COMPLETE",
      status: "HOLDOUT_V3_SEAL_VERIFICATION_FAILED",
      stopReason: `SCORING_INPUT_INTEGRITY_FAILED missingOrMismatch=${missingText.length} scoredN=${agg.N}`,
      missingText,
      seal,
    };
  }

  // Lock aggregate BEFORE error case inspection
  const aggregate = finalizeBucket(agg);
  const confusionSum = aggregate.TP + aggregate.TN + aggregate.FP + aggregate.FN;
  const lockedAt = new Date().toISOString();
  const scoreLock = {
    lockedAt,
    VERSION: PRESENCE_HOLDOUT_V3_VERSION,
    resolverVersion: RESOLVER_VERSION,
    seal: {
      MANIFEST_HASH: EXPECTED_MANIFEST_HASH,
      CONTENT_HASH: EXPECTED_CONTENT_HASH,
      MANIFEST_HASH_MATCH: seal.MANIFEST_HASH_MATCH,
      CONTENT_HASH_MATCH: seal.CONTENT_HASH_MATCH,
    },
    confusionMatrix: {
      TP: aggregate.TP,
      TN: aggregate.TN,
      FP: aggregate.FP,
      FN: aggregate.FN,
      SUM: confusionSum,
    },
    metrics: {
      ACCURACY: aggregate.ACCURACY,
      PRECISION: aggregate.PRECISION,
      RECALL: aggregate.RECALL,
      F1: aggregate.F1,
      SPECIFICITY: aggregate.SPECIFICITY,
      FPR: aggregate.FPR,
      FNR: aggregate.FNR,
    },
    SCORE_LOCKED_BEFORE_ERROR_INSPECTION: true,
  };

  if (persist) {
    fs.mkdirSync(path.dirname(scoreLockPath), { recursive: true });
    fs.writeFileSync(scoreLockPath, JSON.stringify(scoreLock, null, 2) + "\n", "utf8");
  }

  // Pass 2 — error audit AFTER lock
  const errors = [];
  for (const o of rowOutcomes) {
    if (o.outcome !== "FP" && o.outcome !== "FN") continue;
    errors.push({
      CASE_ID: o.row.caseId,
      SOURCE_RESPONSE_ID: o.row.sourceResponseId,
      PROVIDER: o.provider,
      MODEL: o.model,
      LANGUAGE: o.language,
      GEOGRAPHY: o.geography,
      CANONICAL_ENTITY: o.row.canonicalEntityName,
      EXPECTED: o.expected ? "PRESENT" : "NOT_PRESENT",
      ACTUAL: o.actual ? "PRESENT" : "NOT_PRESENT",
      PROMPT_ID: o.row.promptId || o.live?.promptId || null,
      EXACT_RELEVANT_SURFACE_TEXT: surfaceForError({
        text: o.text,
        caseRow: o.row,
        expected: o.expected,
        actual: o.actual,
        matching: o.matching,
        allSpans: o.spans,
      }),
      ROOT_CAUSE_CATEGORY: diagnoseRootCause({
        text: o.text,
        caseRow: o.row,
        expected: o.expected,
        actual: o.actual,
        matching: o.matching,
        allSpans: o.spans,
      }),
      ERROR_TYPE: o.outcome === "FP" ? "FALSE_POSITIVE" : "FALSE_NEGATIVE",
      MATCHED_ALIAS: o.matching?.[0]?.matchedAlias || null,
    });
  }

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

  // Negative control category audit (NOT_PRESENT only)
  const negCats = {
    SIBLING_TARGET_ABSENT: { N: 0, correctN: 0, errorN: 0 },
    PARENT_CHILD: { N: 0, correctN: 0, errorN: 0 },
    GENERIC_COLLECTION: { N: 0, correctN: 0, errorN: 0 },
    GEOGRAPHIC_PLAYA: { N: 0, correctN: 0, errorN: 0 },
    NO_ENTITY: { N: 0, correctN: 0, errorN: 0 },
  };
  for (const o of rowOutcomes) {
    if (o.row.humanFinalLabel !== "NOT_PRESENT") continue;
    const cat = String(o.row.negativeControlType || "");
    if (!negCats[cat]) continue;
    negCats[cat].N += 1;
    if (o.correct) negCats[cat].correctN += 1;
    else negCats[cat].errorN += 1;
  }

  // Contextual coverage audit
  const contextual = {
    SHORTENED_NAMES: { N: 0, correctN: 0, errorN: 0 },
    PARENT_CONTEXT: { N: 0, correctN: 0, errorN: 0 },
    BRAND_FAMILY_LISTS: { N: 0, correctN: 0, errorN: 0 },
    COMMON_LANGUAGE_COLLISIONS: { N: 0, correctN: 0, errorN: 0 },
  };
  for (const o of rowOutcomes) {
    if (isShortenedNameCase(o.row, o.text)) {
      contextual.SHORTENED_NAMES.N += 1;
      if (o.correct) contextual.SHORTENED_NAMES.correctN += 1;
      else contextual.SHORTENED_NAMES.errorN += 1;
    }
    if (isParentContextCase(o.row)) {
      contextual.PARENT_CONTEXT.N += 1;
      if (o.correct) contextual.PARENT_CONTEXT.correctN += 1;
      else contextual.PARENT_CONTEXT.errorN += 1;
    }
    if (isBrandFamilyListCase(o.text)) {
      contextual.BRAND_FAMILY_LISTS.N += 1;
      if (o.correct) contextual.BRAND_FAMILY_LISTS.correctN += 1;
      else contextual.BRAND_FAMILY_LISTS.errorN += 1;
    }
    if (isCommonLanguageCollisionCase(o.row, o.text)) {
      contextual.COMMON_LANGUAGE_COLLISIONS.N += 1;
      if (o.correct) contextual.COMMON_LANGUAGE_COLLISIONS.correctN += 1;
      else contextual.COMMON_LANGUAGE_COLLISIONS.errorN += 1;
    }
  }

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

  const PRECISION_GATE =
    aggregate.PRECISION != null && aggregate.PRECISION >= PRECISION_THRESHOLD
      ? "PASS"
      : "FAIL";
  const RECALL_GATE =
    aggregate.RECALL != null && aggregate.RECALL >= RECALL_THRESHOLD ? "PASS" : "FAIL";

  const aggregatePass =
    PRECISION_GATE === "PASS" && RECALL_GATE === "PASS" && confusionSum === 100;

  let PRESENCE_HOLDOUT_V3_GATE = "FAIL";
  let certification = "PRESENCE_HOLDOUT_V3_CERTIFICATION_FAIL";
  let nextStep = "PRESENCE_HOLDOUT_V3_FAILURE_REVIEW_REQUIRED";
  let HOLDOUT_V3_SCORECARD = "FAIL";
  let PRESENCE_PRODUCTION_READINESS = "NOT_READY";
  let regionalizationStatus = "PLANNED_AFTER_PRESENCE_CERTIFICATION_BLOCKED";

  if (aggregatePass && materialSubgroupFailures.length === 0) {
    PRESENCE_HOLDOUT_V3_GATE = "PASS";
    certification = "PRESENCE_HOLDOUT_V3_CERTIFICATION_PASS";
    nextStep = "READY_FOR_PRESENCE_PRODUCT_INTEGRATION_AND_STAGE_1_REGIONALIZATION";
    HOLDOUT_V3_SCORECARD = "PASS";
    PRESENCE_PRODUCTION_READINESS = "PRODUCTION_VALIDATED";
    regionalizationStatus = "READY_FOR_STAGE_1_OPENAI_REGIONALIZATION_EXPERIMENT";
  } else if (aggregatePass && materialSubgroupFailures.length > 0) {
    PRESENCE_HOLDOUT_V3_GATE = "REVIEW_REQUIRED";
    certification = "PRESENCE_HOLDOUT_V3_CERTIFICATION_REVIEW_REQUIRED";
    nextStep = "PRESENCE_HOLDOUT_V3_FAILURE_REVIEW_REQUIRED";
    HOLDOUT_V3_SCORECARD = "REVIEW_REQUIRED";
    PRESENCE_PRODUCTION_READINESS = "NOT_READY";
    regionalizationStatus = "PLANNED_AFTER_PRESENCE_CERTIFICATION_BLOCKED";
  }

  const SAFE_TO_ENABLE =
    PRESENCE_HOLDOUT_V3_GATE === "PASS"
      ? [
          "AI Presence",
          "Regional Presence",
          "Competitive Position based on Presence",
          "Questions Missing",
          "Presence trends/change where enough real comparable monitoring periods exist",
        ]
      : [];
  const KEEP_BLOCKED =
    PRESENCE_HOLDOUT_V3_GATE === "PASS"
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
  const scoredAt = lockedAt;

  const report = {
    phase: "PRESENCE_HOLDOUT_V3_ONE_TIME_SCORE_COMPLETE",
    status: certification,
    nextStep,
    VERSION: PRESENCE_HOLDOUT_V3_VERSION,
    scoredAt,
    resolverVersion: RESOLVER_VERSION,
    SCORE_LOCKED_BEFORE_ERROR_INSPECTION: true,
    scoreLockPath: persist
      ? path.relative(ROOT, scoreLockPath).replace(/\\/g, "/")
      : null,
    seal: {
      MANIFEST_HASH_MATCH: seal.MANIFEST_HASH_MATCH,
      CONTENT_HASH_MATCH: seal.CONTENT_HASH_MATCH,
      MANIFEST_INTEGRITY: seal.MANIFEST_INTEGRITY,
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
      ACTUAL_POSITIVES: 60,
      ACTUAL_NEGATIVES: 40,
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
      PRECISION_GATE,
      RECALL_GATE,
      PRESENCE_HOLDOUT_V3_GATE,
      materialSubgroupFailures,
      reviewFlags,
    },
    providers,
    languages,
    geographies,
    negativeControls: negCats,
    contextualCoverage: contextual,
    errors: {
      TOTAL_ERRORS: errors.length,
      FALSE_POSITIVES_N: falsePositives.length,
      FALSE_NEGATIVES_N: falseNegatives.length,
      FALSE_POSITIVES: falsePositives,
      FALSE_NEGATIVES: falseNegatives,
      DETAILS: errors,
    },
    scorecard: {
      PRESENCE_DEV: "PASS",
      HOLDOUT_V1: "INSPECTED_DIAGNOSTIC",
      HOLDOUT_V2: "SCORED_FAIL",
      HOLDOUT_V3: HOLDOUT_V3_SCORECARD,
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
        PRESENCE_HOLDOUT_V3_GATE === "PASS"
          ? "May advance to Stage 1 OpenAI regionalization experiment — NOT executed in this scoring phase"
          : "Remains planned but blocked until Presence Holdout v3 remediation",
    },
    holdoutStateAfterScore: {
      SCORED: "YES",
      PREDICTIONS_EXPOSED: "YES",
      USED_FOR_TUNING: "NO",
      FRESH_RESPONSES_HISTORICAL: "YES",
      NEVER_AGAIN_UNSEEN_FOR_TUNING: true,
    },
    hardGuards: {
      PROVIDER_CALLS: 0,
      ENTITY_RESOLVER_CHANGES: 0,
      ALIAS_CHANGES: 0,
      GROUND_TRUTH_CHANGES: 0,
      HUMAN_LABEL_CHANGES: 0,
      HOLDOUT_V3_RESELECTION: 0,
      HOLDOUT_V3_RESCORE: 0,
      SECOND_SCORE_RUN: 0,
      HOLDOUT_V2_CHANGES: 0,
      HOLDOUT_V2_RESCORE: 0,
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
    fs.mkdirSync(path.dirname(reportPath), { recursive: true });
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2) + "\n", "utf8");

    const scorecardPath = path.join(
      ROOT,
      "data/ai-visibility/validation/presence-holdout-v3-scorecard.json"
    );
    fs.writeFileSync(
      scorecardPath,
      JSON.stringify(
        {
          updatedAt: scoredAt,
          ...report.scorecard,
          relatedHoldout: PRESENCE_HOLDOUT_V3_VERSION,
          relatedManifestHash: EXPECTED_MANIFEST_HASH,
          PRESENCE_HOLDOUT_V3_GATE,
          note:
            PRESENCE_HOLDOUT_V3_GATE === "PASS"
              ? "Holdout v3 certification PASS — Presence production-validated. Holdout v2 remains SCORED_FAIL. Holdout v3 must never again be treated as unseen for tuning."
              : "Holdout v3 certification did not PASS — production NOT_READY. Holdout v2 remains SCORED_FAIL.",
        },
        null,
        2
      ) + "\n",
      "utf8"
    );

    const updatedManifest = {
      ...manifest,
      STATUS:
        HOLDOUT_V3_SCORECARD === "PASS"
          ? "SCORED_PASS"
          : `SCORED_${HOLDOUT_V3_SCORECARD}`,
      SCORED: true,
      USED_FOR_TUNING: false,
      PREDICTIONS_EXPOSED: true,
      UNSCORED: false,
      FRESH_RESPONSES: true,
      NOT_USED_FOR_TUNING: true,
      HOLDOUT_V3_SCORING: 1,
      scoredAt,
      scoreLockPath: path.relative(ROOT, scoreLockPath).replace(/\\/g, "/"),
      scoreReportPath: path.relative(ROOT, reportPath).replace(/\\/g, "/"),
      scoreResults: {
        PRESENCE_HOLDOUT_V3_GATE,
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
        "Original manifestHash/contentHash remain the pre-score seal. After this one-time score, SCORED=YES and PREDICTIONS_EXPOSED=YES permanently. Holdout v3 must never again be treated as unseen for future tuning.",
      oneTimeScoreGuards: report.hardGuards,
      contentHash: EXPECTED_CONTENT_HASH,
      manifestHash: EXPECTED_MANIFEST_HASH,
      HOLDOUT_V3_SEALED: true,
    };
    fs.writeFileSync(manifestPath, JSON.stringify(updatedManifest, null, 2) + "\n", "utf8");

    // Touch candidates holdoutV3 status only (no label changes)
    if (candDoc.holdoutV3) {
      candDoc.holdoutV3.SCORED = true;
      candDoc.holdoutV3.STATUS = updatedManifest.STATUS;
      candDoc.holdoutV3.scoredAt = scoredAt;
      candDoc.holdoutV3.PRESENCE_HOLDOUT_V3_GATE = PRESENCE_HOLDOUT_V3_GATE;
      fs.writeFileSync(candidatesPath, JSON.stringify(candDoc, null, 2) + "\n", "utf8");
    }

    report.persisted = {
      reportPath: path.relative(ROOT, reportPath).replace(/\\/g, "/"),
      scoreLockPath: path.relative(ROOT, scoreLockPath).replace(/\\/g, "/"),
      manifestPath: path.relative(ROOT, manifestPath).replace(/\\/g, "/"),
      scorecardPath: path.relative(ROOT, scorecardPath).replace(/\\/g, "/"),
    };
  }

  return report;
}
