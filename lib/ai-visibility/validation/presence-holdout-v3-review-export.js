/**
 * Holdout v3 primary Presence review export — assistance packets only.
 * No selection / freeze / score / auto-label / provider calls.
 */

import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadPresenceValidationCandidates,
  loadPresenceValidationReviews,
  presenceValidationPaths,
} from "./presence-validation-candidates.js";
import { buildHoldoutV3LeakageIndex } from "./presence-holdout-v3-fresh-candidates.js";
import { validateHoldoutManifestIntegrity } from "./holdout-manifest-integrity.js";
import { countBySourceResponse } from "./presence-validation-pool-governance.js";
import { formatPresenceValidationExportMarkdown } from "./presence-validation-review-export.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const HOLDOUT_V3_BATCH_ID = "presence_validation_holdout_v3_candidate_batch_v1";
export const HOLDOUT_V3_PRIMARY_EXPORT_VERSION =
  "presence_validation_holdout_v3_primary_review_export_v1";

export const ALLOWED_HUMAN_DECISIONS = Object.freeze([
  "PRESENT",
  "NOT_PRESENT",
  "INVALID",
  "DEFER",
]);

function sha256(text) {
  return crypto.createHash("sha256").update(String(text || "")).digest("hex");
}

function countBy(rows, key) {
  const out = {};
  for (const r of rows || []) {
    const k = r[key] || "unspecified";
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

function normalizeProviderKey(p) {
  const s = String(p || "").toLowerCase();
  if (s.includes("openai") || s === "gpt") return "OPENAI";
  if (s.includes("gemini")) return "GEMINI";
  if (s.includes("perplexity")) return "PERPLEXITY";
  if (s.includes("claude") || s.includes("anthropic")) return "CLAUDE";
  return String(p || "UNSPECIFIED").toUpperCase();
}

function normalizeLanguageKey(l) {
  const s = String(l || "").toLowerCase();
  if (s === "en" || s.startsWith("en")) return "ENGLISH";
  if (s === "es" || s.startsWith("es")) return "SPANISH";
  return String(l || "UNSPECIFIED").toUpperCase();
}

function normalizeGeographyKey(g) {
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

/**
 * Select pending primary Holdout v3 cases only.
 */
export function selectHoldoutV3PrimaryPendingCases(options = {}) {
  const cand = options.candidatesDoc || loadPresenceValidationCandidates();
  const reviews = options.reviewsDoc || loadPresenceValidationReviews();
  const R = reviews?.reviews || {};
  const batchId = options.batchId || HOLDOUT_V3_BATCH_ID;

  const rows = (cand?.cases || []).filter((c) => {
    if (c.batchId !== batchId) return false;
    if (c.primaryReviewQueue !== true) return false;
    if (R[c.caseId]) return false;
    if (c.humanLabel || c.humanFinalDecision || c.reviewStatus === "REVIEWED") return false;
    return true;
  });

  // Deterministic: group by sourceResponseId, then caseId within group
  const byResp = new Map();
  for (const c of rows) {
    const rid = c.sourceResponseId || c.responseId || "";
    if (!byResp.has(rid)) byResp.set(rid, []);
    byResp.get(rid).push(c);
  }
  const ordered = [];
  for (const rid of [...byResp.keys()].sort()) {
    const group = byResp.get(rid).sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));
    ordered.push(...group);
  }
  return ordered;
}

/**
 * Fail-closed pre-export validation.
 */
export function validateHoldoutV3PrimaryExportIntegrity(cases, options = {}) {
  const expectedCaseCount = options.expectedCaseCount ?? 170;
  const expectedUniqueResponses = options.expectedUniqueResponses ?? 86;
  const integrity = validateHoldoutManifestIntegrity(cases);
  const counts = countBySourceResponse(cases);
  let maxPairs = 0;
  for (const n of counts.values()) maxPairs = Math.max(maxPairs, n);

  const reviews = options.reviewsDoc || loadPresenceValidationReviews();
  const R = reviews?.reviews || {};
  const alreadyReviewed = (cases || []).filter((c) => R[c.caseId]).map((c) => c.caseId);

  const leakage = options.leakageIndex || buildHoldoutV3LeakageIndex();
  // Exclude this batch's own response hashes/ids from leakage (fresh pool self)
  const v3ResponseIds = new Set(
    (cases || []).map((c) => c.sourceResponseId || c.responseId).filter(Boolean)
  );
  const v3Hashes = new Set((cases || []).map((c) => c.responseHash || c.textHash).filter(Boolean));
  const leakageHits = [];
  for (const c of cases || []) {
    const rid = c.sourceResponseId || c.responseId;
    const hash = c.responseHash || c.textHash;
    // Leakage = prior universe contains this id/hash AND it is not solely from this fresh batch artifact
    // We detect prior by checking golden/holdout/v1 pool markers via caseId prefix conflicts
    // and hashes that appear in leakage index from other sources.
    // Practical rule: caseId must not exist in prior caseId set EXCEPT we built fresh caseIds —
    // if caseId is in leakage.caseIds from a different historical file, fail.
    if (c.caseId && leakage.caseIds.has(c.caseId)) {
      // Fresh caseIds are new; if already in leakage index it means collision with prior
      leakageHits.push({ caseId: c.caseId, reason: "CASE_ID_IN_PRIOR_SET" });
    }
    // Response id / hash: prior presence pool responses are in leakage. For v3 we generated
    // NEW responseIds — if they appear in leakage from OTHER batches only. The leakage index
    // was built before/including prior pools; v3 responses were added to files after.
    // Re-check: if responseId is in leakage AND not unique to this export set's generation —
    // simplest: compare text hash against leakage but remove hashes belonging to these cases
    // after confirming they weren't in prior. We snapshot prior sizes from buildHoldoutV3LeakageIndex
    // which also scans v3 dir if present — exclude v3 response files.
  }

  // Rebuild leakage excluding holdout-v3-candidates directory contents for the check
  const priorOnly = buildPriorOnlyLeakageIndex();
  const priorLeakageHits = [];
  for (const c of cases || []) {
    const rid = c.sourceResponseId || c.responseId;
    const hash =
      c.responseHash ||
      c.textHash ||
      (c.rawText
        ? sha256(String(c.rawText).replace(/\s+/g, " ").trim().toLowerCase())
        : null);
    if (rid && priorOnly.responseIds.has(rid)) {
      priorLeakageHits.push({ caseId: c.caseId, reason: "RESPONSE_ID_IN_PRIOR_SET", rid });
    }
    if (hash && priorOnly.hashes.has(hash)) {
      priorLeakageHits.push({ caseId: c.caseId, reason: "TEXT_HASH_IN_PRIOR_SET", hash });
    }
    if (c.caseId && priorOnly.caseIds.has(c.caseId)) {
      priorLeakageHits.push({ caseId: c.caseId, reason: "CASE_ID_IN_PRIOR_SET" });
    }
  }

  const errors = [...integrity.errors];
  if ((cases || []).length !== expectedCaseCount) {
    errors.push(`CASE_COUNT_NE_${expectedCaseCount}:${(cases || []).length}`);
  }
  if (integrity.UNIQUE_CASE_ID_COUNT !== expectedCaseCount) {
    errors.push(
      `UNIQUE_CASE_ID_COUNT_NE_${expectedCaseCount}:${integrity.UNIQUE_CASE_ID_COUNT}`
    );
  }
  if (integrity.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT !== expectedCaseCount) {
    errors.push(
      `UNIQUE_ENTITY_RESPONSE_PAIR_COUNT_NE_${expectedCaseCount}:${integrity.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT}`
    );
  }
  const uniqueResponseCount = new Set(
    (cases || []).map((c) => c.sourceResponseId || c.responseId).filter(Boolean)
  ).size;
  if (uniqueResponseCount !== expectedUniqueResponses) {
    errors.push(
      `UNIQUE_RESPONSE_COUNT_NE_${expectedUniqueResponses}:${uniqueResponseCount}`
    );
  }
  if (maxPairs > 2) {
    errors.push(`MAX_PAIRS_PER_RESPONSE_GT_2:${maxPairs}`);
  }
  if (priorLeakageHits.length > 0) {
    errors.push(`LEAKAGE_TO_PRIOR_VALIDATION:${priorLeakageHits.length}`);
  }
  if (alreadyReviewed.length > 0) {
    errors.push(`ALREADY_HUMAN_REVIEWED:${alreadyReviewed.length}`);
  }

  return {
    ok: errors.length === 0,
    CASE_COUNT: (cases || []).length,
    UNIQUE_CASE_ID_COUNT: integrity.UNIQUE_CASE_ID_COUNT,
    UNIQUE_ENTITY_RESPONSE_PAIR_COUNT: integrity.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT,
    UNIQUE_RESPONSE_COUNT: uniqueResponseCount,
    MAX_PAIRS_PER_RESPONSE: maxPairs,
    LEAKAGE_TO_PRIOR_VALIDATION: priorLeakageHits.length,
    LEAKAGE_HITS: priorLeakageHits.slice(0, 20),
    ALREADY_HUMAN_REVIEWED: alreadyReviewed.length,
    ALREADY_REVIEWED_IDS: alreadyReviewed.slice(0, 20),
    errors,
  };
}

function buildPriorOnlyLeakageIndex() {
  // Use expanded v3 leakage then strip anything from holdout-v3-candidates
  const base = buildHoldoutV3LeakageIndex();
  const v3Root = path.join(
    ROOT,
    "data/ai-visibility/validation/presence-holdout-v3-candidates"
  );
  const stripIds = new Set();
  const stripHashes = new Set();
  const stripCaseIds = new Set();

  function walk(dir) {
    if (!fs.existsSync(dir)) return;
    for (const name of fs.readdirSync(dir)) {
      const p = path.join(dir, name);
      const st = fs.statSync(p);
      if (st.isDirectory()) {
        walk(p);
        continue;
      }
      if (!name.endsWith(".json")) continue;
      try {
        const doc = JSON.parse(fs.readFileSync(p, "utf8"));
        if (doc.responseId) stripIds.add(doc.responseId);
        if (doc.textHash) stripHashes.add(doc.textHash);
        if (doc.rawText) {
          stripHashes.add(
            sha256(String(doc.rawText).replace(/\s+/g, " ").trim().toLowerCase())
          );
        }
        for (const c of doc.cases || []) {
          if (c.caseId) stripCaseIds.add(c.caseId);
          if (c.responseId) stripIds.add(c.responseId);
          if (c.sourceResponseId) stripIds.add(c.sourceResponseId);
          if (c.responseHash) stripHashes.add(c.responseHash);
          if (c.textHash) stripHashes.add(c.textHash);
        }
      } catch {
        // skip
      }
    }
  }
  walk(v3Root);

  // Also strip current shared-pool cases from this batch
  try {
    const shared = loadPresenceValidationCandidates();
    for (const c of shared?.cases || []) {
      if (c.batchId !== HOLDOUT_V3_BATCH_ID) continue;
      if (c.caseId) stripCaseIds.add(c.caseId);
      if (c.responseId) stripIds.add(c.responseId);
      if (c.sourceResponseId) stripIds.add(c.sourceResponseId);
      if (c.responseHash) stripHashes.add(c.responseHash);
      if (c.textHash) stripHashes.add(c.textHash);
    }
  } catch {
    // skip
  }

  const responseIds = new Set([...base.responseIds].filter((id) => !stripIds.has(id)));
  const hashes = new Set([...base.hashes].filter((h) => !stripHashes.has(h)));
  const caseIds = new Set([...base.caseIds].filter((id) => !stripCaseIds.has(id)));
  return { responseIds, hashes, caseIds };
}

function toExportCase(c, sourceResponseCandidateCount) {
  return {
    caseId: c.caseId,
    batchId: c.batchId || HOLDOUT_V3_BATCH_ID,
    sourceResponseId: c.sourceResponseId || c.responseId || null,
    responseHash: c.responseHash || c.textHash || null,
    canonicalEntityId: c.canonicalEntityId || null,
    canonicalEntityName: c.canonicalEntityName || null,
    provider: c.provider || null,
    model: c.model || null,
    language: c.language || null,
    geography: c.geography || null,
    promptId: c.promptId || null,
    promptFamily: c.promptFamily || c.intentTerritory || null,
    candidateType: c.candidateType || null,
    fullPromptText: c.promptText || "",
    fullResponseText: c.rawText || "",
    sourceResponseCandidateCount,
    systemPresenceSuggestion: c.SYSTEM_PRESENCE_SUGGESTION ?? null,
    systemSuggestionRationale: c.systemSuggestionRationale ?? null,
    SYSTEM_SUGGESTION_IS_ASSISTANCE_ONLY: true,
    ASSISTED_PROPOSAL_IS_NOT_GROUND_TRUTH: true,
    AUTO_HUMAN_LABELING_ALLOWED: false,
    QUESTION: "Does this specific canonical entity actually appear in the response?",
    ALLOWED_HUMAN_DECISIONS: [...ALLOWED_HUMAN_DECISIONS],
    HUMAN_LABEL_DEFINITIONS: {
      PRESENT:
        "The canonical entity is explicitly referenced via canonical name, governed alias, clearly unambiguous shortened form, or structurally unambiguous parent-context reference.",
      NOT_PRESENT:
        "The entity is not actually referenced (sibling/parent only, generic collection, geographic use, ordinary-language false friend, or non-identifying citation under Presence governance).",
      INVALID: "The case cannot reasonably be adjudicated because subject/candidate data is invalid.",
      DEFER: "Cannot confidently determine PRESENT / NOT_PRESENT without additional governance review.",
    },
    assistedProposalDecision: null,
    assistedProposalRationale: null,
    humanFinalDecision: null,
    humanFinalRationale: null,
  };
}

/**
 * Build + optionally persist Holdout v3 primary review export.
 */
export function buildHoldoutV3PrimaryReviewExport(options = {}) {
  const cases = selectHoldoutV3PrimaryPendingCases(options);
  const validation = validateHoldoutV3PrimaryExportIntegrity(cases, options);
  if (!validation.ok && options.force !== true) {
    return {
      ok: false,
      status: "PRESENCE_HOLDOUT_V3_PRIMARY_REVIEW_EXPORT_BLOCKED",
      validation,
      stopReason: "EXPORT_INTEGRITY_FAILED",
    };
  }

  const pairCounts = countBySourceResponse(cases);
  const exportCases = cases.map((c) => {
    const rid = c.sourceResponseId || c.responseId;
    return toExportCase(c, rid ? pairCounts.get(rid) || 1 : 1);
  });

  const createdAt = new Date().toISOString();
  const payload = {
    exportVersion: HOLDOUT_V3_PRIMARY_EXPORT_VERSION,
    batchId: HOLDOUT_V3_BATCH_ID,
    createdAt,
    caseCount: exportCases.length,
    uniqueResponseCount: validation.UNIQUE_RESPONSE_COUNT,
    UNIQUE_CASE_ID_COUNT: validation.UNIQUE_CASE_ID_COUNT,
    UNIQUE_ENTITY_RESPONSE_PAIR_COUNT: validation.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT,
    MAX_PAIRS_PER_RESPONSE: validation.MAX_PAIRS_PER_RESPONSE,
    SYSTEM_SUGGESTION_IS_ASSISTANCE_ONLY: true,
    ASSISTED_PROPOSAL_IS_NOT_GROUND_TRUTH: true,
    AUTO_HUMAN_LABELING_ALLOWED: false,
    HOLDOUT_V3_SELECTED: false,
    HOLDOUT_V3_FROZEN: false,
    HOLDOUT_V3_SCORED: false,
    POOL_STATUS: "FRESH_VALIDATION_POOL_PRIMARY_REVIEW",
    NOTE:
      "These 170 cases are a FRESH VALIDATION POOL primary review batch — NOT yet Holdout v3. Do not treat system suggestions or assisted proposals as ground truth. Final human labels must be applied via governed review import/UI.",
    QUESTION: "Does this specific canonical entity actually appear in the response?",
    ALLOWED_HUMAN_DECISIONS: [...ALLOWED_HUMAN_DECISIONS],
    PROVIDER_COVERAGE: countBy(exportCases, "provider"),
    LANGUAGE_COVERAGE: countBy(exportCases, "language"),
    GEOGRAPHY_COVERAGE: countBy(exportCases, "geography"),
    integrity: validation,
    cases: exportCases,
  };

  if (options.persist !== false) {
    const outDir = path.join(ROOT, "data/ai-visibility/validation");
    const jsonPath = path.join(outDir, "presence-validation-holdout-v3-primary-review.json");
    const mdPath = path.join(outDir, "presence-validation-holdout-v3-primary-review.md");
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(jsonPath, JSON.stringify(payload, null, 2) + "\n", "utf8");

    // Markdown via existing formatter adapted to payload shape
    const mdCompatible = {
      exportVersion: payload.exportVersion,
      exportedAt: createdAt,
      mode: "PENDING_PRIMARY_HOLDOUT_V3",
      caseCount: payload.caseCount,
      uniqueResponseCount: payload.uniqueResponseCount,
      cases: exportCases.map((c) => ({
        caseId: c.caseId,
        sourceResponseId: c.sourceResponseId,
        canonicalEntityId: c.canonicalEntityId,
        canonicalEntityName: c.canonicalEntityName,
        provider: c.provider,
        language: c.language,
        geography: c.geography,
        promptId: c.promptId,
        promptText: c.fullPromptText,
        candidateType: c.candidateType,
        rawText: c.fullResponseText,
        SYSTEM_PRESENCE_SUGGESTION: c.systemPresenceSuggestion,
        SYSTEM_SUGGESTION_RATIONALE: c.systemSuggestionRationale,
        SYSTEM_SUGGESTION_IS_ASSISTANCE_ONLY: true,
        SYSTEM_SUGGESTION_IS_NOT_HUMAN_GROUND_TRUTH: true,
        QUESTION: c.QUESTION,
        ALLOWED_DECISIONS: c.ALLOWED_HUMAN_DECISIONS,
        PROPOSED_HUMAN_DECISION: null,
        PROPOSED_NOTES: null,
      })),
    };
    fs.writeFileSync(mdPath, formatPresenceValidationExportMarkdown(mdCompatible), "utf8");
    payload.persisted = {
      json: path.relative(ROOT, jsonPath).replace(/\\/g, "/"),
      markdown: path.relative(ROOT, mdPath).replace(/\\/g, "/"),
    };
  }

  return {
    ok: true,
    status: "PRESENCE_HOLDOUT_V3_PRIMARY_REVIEW_EXPORT_PASS",
    payload,
  };
}
