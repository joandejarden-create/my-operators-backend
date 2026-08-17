/**
 * Presence validation pool governance — response-level partitioning,
 * unique-response tracking, Holdout v2 selection caps, OpenAI freeze gate.
 *
 * Does not regenerate responses, change entity resolution, or score Holdout v2.
 */

import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  dedupeHoldoutSelectionByCaseId,
  resolvePresenceSelectionLabel,
  validateHoldoutManifestIntegrity,
} from "./holdout-manifest-integrity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function detectProviderAvailabilityLocal() {
  return {
    openai: !!(
      process.env.OPENAI_API_KEY || process.env.AI_VISIBILITY_OPENAI_API_KEY
    ),
    gemini: !!process.env.GEMINI_API_KEY,
    perplexity: !!process.env.PERPLEXITY_API_KEY,
    claude: !!(process.env.ANTHROPIC_API_KEY || process.env.CLAUDE_API_KEY),
  };
}

/** Max candidate pairs from one source response allowed into final Holdout v2. */
export const CANDIDATE_CAP_PER_RESPONSE_HOLDOUT = 2;

export const VALIDATION_PARTITIONS = Object.freeze({
  UNASSIGNED: "UNASSIGNED",
  DEV: "DEV",
  RESERVE: "RESERVE",
  HOLDOUT: "HOLDOUT",
});

export const PRESENCE_HOLDOUT_V2_METRIC_CONTRACT = Object.freeze({
  requiredMetrics: [
    "Accuracy",
    "Precision",
    "Recall",
    "F1",
    "Specificity",
    "FalsePositiveRate",
    "FalseNegativeRate",
    "CANDIDATE_PAIR_N",
    "UNIQUE_RESPONSE_N",
  ],
  requiredBreakdowns: ["provider", "language", "geography"],
  forbidCompositeScore: true,
  note: "Report metrics independently — no single composite score.",
});

/**
 * Confusion + rate metrics for Presence (when eventually scored).
 * @param {{ tp: number, tn: number, fp: number, fn: number }} c
 */
export function computePresenceValidationMetrics(c) {
  const tp = Number(c.tp) || 0;
  const tn = Number(c.tn) || 0;
  const fp = Number(c.fp) || 0;
  const fn = Number(c.fn) || 0;
  const total = tp + tn + fp + fn;
  const precision = tp + fp > 0 ? tp / (tp + fp) : null;
  const recall = tp + fn > 0 ? tp / (tp + fn) : null;
  const specificity = tn + fp > 0 ? tn / (tn + fp) : null;
  const f1 =
    precision != null && recall != null && precision + recall > 0
      ? (2 * precision * recall) / (precision + recall)
      : null;
  return {
    Accuracy: total > 0 ? (tp + tn) / total : null,
    Precision: precision,
    Recall: recall,
    F1: f1,
    Specificity: specificity,
    FalsePositiveRate: tn + fp > 0 ? fp / (tn + fp) : null,
    FalseNegativeRate: tp + fn > 0 ? fn / (tp + fn) : null,
    CANDIDATE_PAIR_N: total,
    // UNIQUE_RESPONSE_N must be supplied by caller from case metadata
  };
}

export function uniqueResponseIds(cases) {
  return new Set(
    (cases || [])
      .map((c) => c.sourceResponseId || c.responseId)
      .filter(Boolean)
  );
}

export function countBySourceResponse(cases) {
  const map = new Map();
  for (const c of cases || []) {
    const id = c.sourceResponseId || c.responseId;
    if (!id) continue;
    map.set(id, (map.get(id) || 0) + 1);
  }
  return map;
}

/**
 * Enrich candidate rows with response-level governance fields.
 * Preserves caseId / human labels / primaryReviewQueue when present.
 */
export function enrichCandidatesWithResponseGovernance(cases) {
  const counts = countBySourceResponse(cases);
  return (cases || []).map((c) => {
    const sourceResponseId = c.sourceResponseId || c.responseId || null;
    const responseHash = c.responseHash || c.textHash || null;
    return {
      ...c,
      sourceResponseId,
      responseHash,
      responseId: c.responseId || sourceResponseId,
      provider: c.provider || null,
      language: c.language || null,
      geography: c.geography || null,
      promptId: c.promptId || null,
      sourceResponseCandidateCount: sourceResponseId
        ? counts.get(sourceResponseId) || 1
        : 1,
      validationPartition: c.validationPartition || VALIDATION_PARTITIONS.UNASSIGNED,
    };
  });
}

/**
 * Assert (and optionally repair) that all pairs from one response share one partition.
 * @returns {{ ok: boolean, violations: array, repaired: array }}
 */
export function enforceResponseLevelPartitioning(cases, { repair = false } = {}) {
  const byResp = new Map();
  for (const c of cases || []) {
    const id = c.sourceResponseId || c.responseId;
    if (!id) continue;
    if (!byResp.has(id)) byResp.set(id, []);
    byResp.get(id).push(c);
  }
  const violations = [];
  const repaired = [];
  for (const [respId, group] of byResp) {
    const parts = new Set(
      group.map((c) => c.validationPartition || VALIDATION_PARTITIONS.UNASSIGNED)
    );
    if (parts.size <= 1) continue;
    violations.push({
      sourceResponseId: respId,
      partitions: [...parts],
      caseIds: group.map((c) => c.caseId),
    });
    if (repair) {
      // Prefer HOLDOUT > RESERVE > DEV > UNASSIGNED if mixed (should not happen in governed selection)
      const priority = ["HOLDOUT", "RESERVE", "DEV", "UNASSIGNED"];
      const chosen =
        priority.find((p) => parts.has(p)) || VALIDATION_PARTITIONS.UNASSIGNED;
      for (const c of group) {
        if (c.validationPartition !== chosen) {
          c.validationPartition = chosen;
          repaired.push(c.caseId);
        }
      }
    }
  }
  return { ok: violations.length === 0, violations, repaired };
}

/**
 * Primary review queue: maximize unique-response coverage.
 * Caps pairs per response in the primary queue (default = holdout cap)
 * so ~150 pairs span ~75 responses. Remaining pairs stay reviewable via "All".
 * Partition atomicity (DEV/reserve/holdout) is separate and still response-level.
 */
export function selectPrimaryReviewQueueByResponse(
  candidates,
  targetPairN = 150,
  { pairsPerResponseCap = CANDIDATE_CAP_PER_RESPONSE_HOLDOUT } = {}
) {
  const enriched = enrichCandidatesWithResponseGovernance(candidates);
  const byResp = new Map();
  for (const c of enriched) {
    const id = c.sourceResponseId;
    if (!id) continue;
    if (!byResp.has(id)) byResp.set(id, []);
    byResp.get(id).push(c);
  }

  const groups = [...byResp.entries()]
    .map(([sourceResponseId, cases]) => {
      const sample = cases[0];
      const sorted = cases.sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));
      const trues = sorted.filter((c) => c.candidateType === "PRESENCE_TRUE");
      const falses = sorted.filter((c) => c.candidateType === "PRESENCE_FALSE");
      // Prefer 1 TRUE + 1 FALSE when capping
      const capped = [];
      if (trues.length) capped.push(trues[0]);
      if (falses.length && capped.length < pairsPerResponseCap) capped.push(falses[0]);
      for (const row of sorted) {
        if (capped.length >= pairsPerResponseCap) break;
        if (!capped.some((p) => p.caseId === row.caseId)) capped.push(row);
      }
      return {
        sourceResponseId,
        cases: capped,
        provider: sample.provider,
        language: sample.language,
        geography: sample.geography,
        trueN: trues.length,
        falseN: falses.length,
        diversityScore: (trues.length > 0 ? 2 : 0) + (falses.length > 0 ? 2 : 0),
        sortKey: [
          sample.provider || "",
          sample.language || "",
          sample.geography || "",
          sourceResponseId,
        ].join("|"),
      };
    })
    .sort((a, b) => {
      if (b.diversityScore !== a.diversityScore) return b.diversityScore - a.diversityScore;
      return a.sortKey.localeCompare(b.sortKey);
    });

  const buckets = new Map();
  for (const g of groups) {
    const key = [g.provider, g.language, g.geography].join("|");
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(g);
  }
  const keys = [...buckets.keys()].sort();
  const selectedCaseIds = new Set();
  const selectedResponseIds = new Set();
  let pairCount = 0;
  let progressed = true;
  while (pairCount < targetPairN && progressed) {
    progressed = false;
    for (const key of keys) {
      if (pairCount >= targetPairN) break;
      const bucket = buckets.get(key);
      if (!bucket.length) continue;
      const g = bucket.shift();
      for (const c of g.cases) {
        if (pairCount >= targetPairN) break;
        selectedCaseIds.add(c.caseId);
        selectedResponseIds.add(g.sourceResponseId);
        pairCount += 1;
      }
      progressed = true;
    }
  }

  return {
    selectedResponseIds,
    selectedCases: enriched.filter((c) => selectedCaseIds.has(c.caseId)),
    UNIQUE_RESPONSE_N: selectedResponseIds.size,
    CANDIDATE_PAIR_N: pairCount,
  };
}

/**
 * Deterministic Holdout v2 candidate selection from eligible labelled cases.
 * Response-atomic partitions; cap pairs per response; returns counts only — does not freeze.
 *
 * @param {object[]} eligibleCases — human-labelled PRESENT / NOT_PRESENT only
 * @param {object} design — { TOTAL_N, PRESENCE_TRUE_N, PRESENCE_FALSE_N, CANDIDATE_CAP_PER_RESPONSE }
 */
export function selectHoldoutV2WithResponseGovernance(eligibleCases, design = {}) {
  const TOTAL_N = design.TOTAL_N ?? 100;
  const TRUE_N = design.PRESENCE_TRUE_N ?? 75;
  const FALSE_N = design.PRESENCE_FALSE_N ?? 25;
  const cap = design.CANDIDATE_CAP_PER_RESPONSE ?? CANDIDATE_CAP_PER_RESPONSE_HOLDOUT;

  const enriched = enrichCandidatesWithResponseGovernance(eligibleCases);
  const byResp = new Map();
  for (const c of enriched) {
    const id = c.sourceResponseId;
    if (!id) continue;
    if (!byResp.has(id)) byResp.set(id, []);
    byResp.get(id).push(c);
  }

  // Within each response, prefer at most `cap` pairs (prefer 1 TRUE + 1 FALSE when available).
  // Human final label ALWAYS wins over candidateType (Holdout v2 bug: OR-filter double-bucketed
  // CHANGED rows that were PRESENCE_FALSE system candidates but human PRESENT).
  const cappedRows = [];
  for (const [, group] of [...byResp.entries()].sort((a, b) =>
    String(a[0]).localeCompare(String(b[0]))
  )) {
    const trues = group
      .filter((c) => resolvePresenceSelectionLabel(c) === "PRESENT")
      .sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));
    const falses = group
      .filter((c) => resolvePresenceSelectionLabel(c) === "NOT_PRESENT")
      .sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));
    const picks = [];
    const pushUnique = (row) => {
      if (!row?.caseId) return;
      if (picks.some((p) => p.caseId === row.caseId)) return;
      picks.push(row);
    };
    if (trues.length) pushUnique(trues[0]);
    if (falses.length && picks.length < cap) pushUnique(falses[0]);
    for (const row of [...trues, ...falses]) {
      if (picks.length >= cap) break;
      pushUnique(row);
    }
    for (const p of picks) cappedRows.push(p);
  }

  function take(rows, label, n) {
    const pool = rows
      .filter((c) => resolvePresenceSelectionLabel(c) === label)
      .sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));
    // Stratify by provider|language|geography
    const buckets = new Map();
    for (const c of pool) {
      const key = [c.provider, c.language, c.geography].join("|");
      if (!buckets.has(key)) buckets.set(key, []);
      buckets.get(key).push(c);
    }
    const keys = [...buckets.keys()].sort();
    const out = [];
    let progressed = true;
    while (out.length < n && progressed) {
      progressed = false;
      for (const key of keys) {
        if (out.length >= n) break;
        const b = buckets.get(key);
        if (b?.length) {
          out.push(b.shift());
          progressed = true;
        }
      }
    }
    return out;
  }

  // Response already used in one label class must not split — track assigned responses
  const assignedResponses = new Set();
  const selectedCaseIds = new Set();
  const selected = [];

  function takeRespectingResponseAtomicity(label, n) {
    const picks = [];
    const pool = take(cappedRows, label, cappedRows.length);
    for (const c of pool) {
      if (picks.length >= n) break;
      if (!c?.caseId || selectedCaseIds.has(c.caseId)) continue;
      const rid = c.sourceResponseId;
      if (assignedResponses.has(rid)) {
        // Only allow if this response is already in HOLDOUT selection (same partition)
        if (!selected.some((s) => s.sourceResponseId === rid)) continue;
      }
      // Cap already applied; also ensure we don't exceed remaining slots for this response
      const alreadyFromResp = selected.filter((s) => s.sourceResponseId === rid).length;
      if (alreadyFromResp >= cap) continue;
      picks.push(c);
      assignedResponses.add(rid);
      selectedCaseIds.add(c.caseId);
      selected.push(c);
    }
    return picks;
  }

  const present = takeRespectingResponseAtomicity("PRESENT", TRUE_N);
  const absent = takeRespectingResponseAtomicity("NOT_PRESENT", FALSE_N);

  // If under TOTAL_N, fill from remaining capped rows without splitting responses wrongly
  while (selected.length < TOTAL_N) {
    const remaining = cappedRows.filter(
      (c) =>
        c?.caseId &&
        !selectedCaseIds.has(c.caseId) &&
        (resolvePresenceSelectionLabel(c) === "PRESENT" ||
          resolvePresenceSelectionLabel(c) === "NOT_PRESENT")
    );
    if (!remaining.length) break;
    let added = false;
    for (const c of remaining.sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)))) {
      if (selected.length >= TOTAL_N) break;
      const rid = c.sourceResponseId;
      const fromResp = selected.filter((s) => s.sourceResponseId === rid).length;
      if (fromResp >= cap) continue;
      if (assignedResponses.has(rid) && fromResp === 0) continue;
      if (selectedCaseIds.has(c.caseId)) continue;
      selected.push(c);
      selectedCaseIds.add(c.caseId);
      assignedResponses.add(rid);
      added = true;
    }
    if (!added) break;
  }

  const deduped = dedupeHoldoutSelectionByCaseId(selected);
  const integrity = validateHoldoutManifestIntegrity(deduped);
  const uniq = uniqueResponseIds(deduped);
  return {
    selected: deduped,
    CANDIDATE_PAIR_N: deduped.length,
    UNIQUE_RESPONSE_N: uniq.size,
    PRESENT_N: deduped.filter((c) => resolvePresenceSelectionLabel(c) === "PRESENT").length,
    NOT_PRESENT_N: deduped.filter((c) => resolvePresenceSelectionLabel(c) === "NOT_PRESENT")
      .length,
    CANDIDATE_CAP_PER_RESPONSE: cap,
    RESPONSE_LEVEL_PARTITIONING: true,
    presentPreview: present.length,
    absentPreview: absent.length,
    manifestIntegrity: integrity,
    SELECTION_INTEGRITY_OK: integrity.ok,
  };
}

/**
 * Assign validationPartition atomically by sourceResponseId.
 */
export function assignPartitionByResponse(cases, responseIds, partition) {
  const set = new Set(responseIds);
  for (const c of cases || []) {
    const id = c.sourceResponseId || c.responseId;
    if (set.has(id)) c.validationPartition = partition;
  }
  return cases;
}

export function evaluateOpenAiFreezeGate(availability = null, cases = []) {
  const avail = availability || detectProviderAvailabilityLocal();
  const openaiAvailable = !!avail.openai;
  const openaiResponsesInPool = (cases || []).some(
    (c) => String(c.provider || "").toLowerCase() === "openai"
  );
  const freezeAllowed = openaiResponsesInPool;
  return {
    OPENAI_IN_PRODUCTION_CONTRACT: true,
    OPENAI_CREDENTIAL_AVAILABLE: openaiAvailable,
    OPENAI_FRESH_RESPONSES_IN_POOL: openaiResponsesInPool,
    OPENAI_REQUIRED_BEFORE_FINAL_FREEZE: true,
    HOLDOUT_V2_FINAL_FREEZE_ALLOWED: freezeAllowed ? "PENDING_HUMAN_REVIEW_COMPLETE" : "NO",
    reason: openaiResponsesInPool
      ? "OpenAI fresh validation responses present — final freeze still awaits completed human review + selection"
      : openaiAvailable
        ? "OpenAI credential present but no OpenAI responses in this validation pool yet — do not substitute"
        : "OPENAI_API_KEY missing; OpenAI remains in production provider contract — do not substitute another provider",
  };
}

/**
 * Apply governance to existing candidates.json (no provider regeneration).
 */
export function applyPresenceValidationPoolGovernance(options = {}) {
  const candidatesPath =
    options.candidatesPath ||
    path.join(
      ROOT,
      "data/ai-visibility/validation/presence-validation-candidates/candidates/candidates.json"
    );
  if (!fs.existsSync(candidatesPath)) {
    return { ok: false, error: "CANDIDATES_MISSING", path: candidatesPath };
  }

  const doc = JSON.parse(fs.readFileSync(candidatesPath, "utf8"));
  let cases = enrichCandidatesWithResponseGovernance(doc.cases || []);

  // Rebuild primary queue response-atomically (reviews empty / safe)
  const primary = selectPrimaryReviewQueueByResponse(cases, options.primaryTargetN || 150);
  const primaryIds = new Set(primary.selectedCases.map((c) => c.caseId));
  for (const c of cases) {
    c.primaryReviewQueue = primaryIds.has(c.caseId);
  }

  const partitionCheck = enforceResponseLevelPartitioning(cases, { repair: true });
  const openaiGate = evaluateOpenAiFreezeGate(null, cases);
  const uniqAll = uniqueResponseIds(cases);
  const uniqPrimary = uniqueResponseIds(cases.filter((c) => c.primaryReviewQueue));

  const updated = {
    ...doc,
    governanceVersion: "presence_validation_pool_governance_v1",
    governedAt: new Date().toISOString(),
    RESPONSE_LEVEL_PARTITIONING: true,
    CANDIDATE_CAP_PER_RESPONSE: CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
    UNIQUE_RESPONSE_N: uniqAll.size,
    CANDIDATE_PAIR_N: cases.length,
    PRIMARY_REVIEW_QUEUE: primaryIds.size,
    PRIMARY_UNIQUE_RESPONSES: uniqPrimary.size,
    OPENAI_REQUIRED_BEFORE_FINAL_FREEZE: true,
    HOLDOUT_V2_FINAL_FREEZE_ALLOWED: openaiGate.HOLDOUT_V2_FINAL_FREEZE_ALLOWED,
    PRESENCE_HOLDOUT_V2_METRIC_CONTRACT,
    cases,
  };

  fs.writeFileSync(candidatesPath, JSON.stringify(updated, null, 2) + "\n");

  const reportPath =
    options.reportPath ||
    path.join(
      ROOT,
      "data/ai-visibility/validation/presence-validation-pool-governance.json"
    );
  const report = {
    phase: "PRESENCE_VALIDATION_POOL_GOVERNANCE_UPDATED",
    CURRENT_RESPONSES: uniqAll.size,
    CURRENT_CANDIDATES: cases.length,
    RESPONSE_LEVEL_PARTITIONING: "YES",
    CANDIDATE_CAP_PER_RESPONSE: CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
    UNIQUE_RESPONSE_TRACKING: "YES",
    PRIMARY_REVIEW_QUEUE_PAIRS: primaryIds.size,
    PRIMARY_UNIQUE_RESPONSES: uniqPrimary.size,
    PARTITION_VIOLATIONS_REPAIRED: partitionCheck.repaired.length,
    OPENAI_REQUIRED_BEFORE_FINAL_FREEZE: "YES",
    HOLDOUT_V2_FINAL_FREEZE_ALLOWED: openaiGate.HOLDOUT_V2_FINAL_FREEZE_ALLOWED,
    openaiGate,
    metricContract: PRESENCE_HOLDOUT_V2_METRIC_CONTRACT,
    HUMAN_REVIEW_READY: "YES",
    NEXT_ACTION: "CONTINUE_HUMAN_PRESENCE_REVIEW",
    HOLDOUT_V2_SCORED: false,
    REGENERATED_BATCH: false,
  };
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2) + "\n");

  return { ok: true, report, candidatesPath, reportPath };
}

export function sha256Hex(text) {
  return crypto.createHash("sha256").update(String(text || "")).digest("hex");
}
