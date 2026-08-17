#!/usr/bin/env node
/**
 * Presence Holdout v2 — deterministic selection + freeze (NO scoring).
 *
 *   node scripts/ai-intelligence-presence-holdout-v2-select-and-freeze.mjs
 *   node scripts/ai-intelligence-presence-holdout-v2-select-and-freeze.mjs --dry-run
 *
 * HOLDOUT_V2_SCORING=0 PREDICTIONS_EXPOSED=0 ENTITY_RESOLVER_CHANGES=0
 */
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import {
  selectHoldoutV2WithResponseGovernance,
  enrichCandidatesWithResponseGovernance,
  enforceResponseLevelPartitioning,
  uniqueResponseIds,
  countBySourceResponse,
  CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
  PRESENCE_HOLDOUT_V2_METRIC_CONTRACT,
  VALIDATION_PARTITIONS,
  sha256Hex,
} from "../lib/ai-visibility/validation/presence-validation-pool-governance.js";
import { validateHoldoutManifestIntegrity } from "../lib/ai-visibility/validation/holdout-manifest-integrity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DRY_RUN = process.argv.includes("--dry-run");

const SELECTION_VERSION = "presence_holdout_v2_response_governed_select_v1";
const SELECTION_ALGORITHM =
  "selectHoldoutV2WithResponseGovernance — stratified PRESENT/NOT_PRESENT by provider|language|geography; response-atomic; caseId lexicographic tie-break";
const SELECTION_SEED = "presence_holdout_v2_freeze_20260815_readiness_pass";

const OUT_MANIFEST = path.join(
  ROOT,
  "data/ai-visibility/validation/ai-intelligence-presence-holdout-v2.json"
);
const OUT_RESERVE = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-reserve.json"
);
const OUT_REPORT = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-holdout-v2-selection-freeze-report.json"
);
const CANDIDATES_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-candidates/candidates/candidates.json"
);
const REVIEWS_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-candidates/reviews/reviews.json"
);

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function stableStringify(obj) {
  if (obj === null || typeof obj !== "object") return JSON.stringify(obj);
  if (Array.isArray(obj)) return `[${obj.map(stableStringify).join(",")}]`;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(",")}}`;
}

function countBy(rows, key) {
  const out = {};
  for (const r of rows || []) {
    const k = r[key] || "unspecified";
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

function classifyNegative(c) {
  const rat = String(c.systemSuggestionRationale || "").toLowerCase();
  const name = String(c.canonicalEntityName || "");
  if (/hilton context without|sibling\/parent hilton/.test(rat)) return "PARENT_CHILD_OR_SIBLING_HILTON";
  if (/marriott family|marriott context without/.test(rat)) return "PARENT_CHILD_OR_SIBLING_MARRIOTT";
  if (/generic collection/.test(rat)) return "GENERIC_COLLECTION";
  if (/playa/.test(rat) || /playa hotels/i.test(name)) return "GEOGRAPHIC_PLAYA";
  if (/hard negative|absent from response|canonical brand absent/.test(rat)) return "NO_SUBJECT_OR_HARD_NEGATIVE";
  return "OTHER";
}

function main() {
  const candDoc = loadJson(CANDIDATES_PATH);
  const reviews = loadJson(REVIEWS_PATH);
  const R = reviews.reviews || {};

  const eligible = [];
  for (const c of candDoc.cases || []) {
    const r = R[c.caseId];
    if (!r) continue;
    if (r.action !== "PRESENT" && r.action !== "NOT_PRESENT") continue;
    if (!c.canonicalEntityId) continue;
    if (!(c.sourceResponseId || c.responseId)) continue;
    if (!(c.rawText && String(c.rawText).trim())) continue;
    eligible.push({
      ...c,
      humanLabel: r.action,
      humanFinalDecision: r.humanFinalDecision || r.action,
      humanAction: r.humanAction || null,
      reviewer: r.reviewer || null,
      reviewedAt: r.reviewedAt || null,
    });
  }

  const enriched = enrichCandidatesWithResponseGovernance(eligible);
  const selection = selectHoldoutV2WithResponseGovernance(enriched, {
    TOTAL_N: 100,
    PRESENCE_TRUE_N: 75,
    PRESENCE_FALSE_N: 25,
    CANDIDATE_CAP_PER_RESPONSE: CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
  });

  const selected = selection.selected || [];
  const sealIntegrity = validateHoldoutManifestIntegrity(selected);
  if (!sealIntegrity.ok || selection.SELECTION_INTEGRITY_OK === false) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          error: "HOLDOUT_MANIFEST_INTEGRITY_FAIL_DO_NOT_SEAL",
          sealIntegrity,
          selectionIntegrity: selection.manifestIntegrity || null,
        },
        null,
        2
      )
    );
    process.exit(2);
  }
  const selectedIds = new Set(selected.map((c) => c.caseId));
  const holdoutResponseIds = new Set(
    selected.map((c) => c.sourceResponseId || c.responseId).filter(Boolean)
  );

  // Atomicity: any eligible pair from a holdout response must also be in holdout
  // (or left out of reserve). If a response is in holdout, all of its selected pairs
  // are already capped; remaining eligible pairs from same response go to neither
  // holdout nor reserve as "same-response remainder" OR we put them in HOLD if under
  // cap — selection already capped at 2. Unselected pairs from holdout responses:
  // must NOT go to RESERVE (would split). Put them in HOLD_OUT_OF_CAP / same HOLDOUT
  // partition but not in the 100 scored set — design says partition atomicity for
  // DEV|RESERVE|HOLDOUT. So assign validationPartition=HOLDOUT for ALL cases sharing
  // a holdout sourceResponseId; only the 100 caseIds are the scored holdout set.

  const reserve = enriched.filter((c) => {
    const rid = c.sourceResponseId || c.responseId;
    if (selectedIds.has(c.caseId)) return false;
    if (holdoutResponseIds.has(rid)) return false; // same response → HOLDOUT partition, not reserve
    return true;
  });

  const holdoutSameResponseRemainder = enriched.filter((c) => {
    const rid = c.sourceResponseId || c.responseId;
    return !selectedIds.has(c.caseId) && holdoutResponseIds.has(rid);
  });

  // Response-level checks on selected 100
  const counts = countBySourceResponse(selected);
  let maxPairs = 0;
  for (const n of counts.values()) maxPairs = Math.max(maxPairs, n);
  const overCap = [...counts.entries()].filter(([, n]) => n > CANDIDATE_CAP_PER_RESPONSE_HOLDOUT);
  const partCheck = enforceResponseLevelPartitioning(
    selected.map((c) => ({ ...c, validationPartition: VALIDATION_PARTITIONS.HOLDOUT })),
    { repair: false }
  );

  const presentN = selected.filter((c) => c.humanLabel === "PRESENT").length;
  const notPresentN = selected.filter((c) => c.humanLabel === "NOT_PRESENT").length;
  const uniqN = uniqueResponseIds(selected).size;

  const negSelected = selected.filter((c) => c.humanLabel === "NOT_PRESENT");
  const negativeControlCounts = {
    PARENT_CHILD: 0,
    SIBLING: 0,
    GENERIC_COLLECTION: 0,
    GEOGRAPHIC_PLAYA: 0,
    SIMILAR_NAME: 0,
    OTHER: 0,
  };
  for (const c of negSelected) {
    const t = classifyNegative(c);
    if (t === "PARENT_CHILD_OR_SIBLING_HILTON" || t === "PARENT_CHILD_OR_SIBLING_MARRIOTT") {
      // Split hilton/marriott: parent/sibling family context
      if (/sibling/.test(String(c.systemSuggestionRationale || "").toLowerCase())) {
        negativeControlCounts.SIBLING += 1;
      } else {
        negativeControlCounts.PARENT_CHILD += 1;
      }
    } else if (t === "GENERIC_COLLECTION") negativeControlCounts.GENERIC_COLLECTION += 1;
    else if (t === "GEOGRAPHIC_PLAYA") negativeControlCounts.GEOGRAPHIC_PLAYA += 1;
    else if (t === "NO_SUBJECT_OR_HARD_NEGATIVE") negativeControlCounts.SIMILAR_NAME += 1;
    else negativeControlCounts.OTHER += 1;
  }

  const createdAt = new Date().toISOString();
  const casePayload = selected
    .map((c) => ({
      caseId: c.caseId,
      sourceResponseId: c.sourceResponseId || c.responseId,
      responseHash: c.responseHash || c.textHash || null,
      canonicalEntityId: c.canonicalEntityId,
      canonicalEntityName: c.canonicalEntityName,
      humanFinalLabel: c.humanLabel,
      humanAction: c.humanAction,
      provider: c.provider,
      language: c.language,
      geography: c.geography,
      candidateType: c.candidateType,
      batchId: c.batchId || null,
      promptId: c.promptId || null,
      systemSuggestionRationale: c.systemSuggestionRationale || null,
      negativeControlType:
        c.humanLabel === "NOT_PRESENT" ? classifyNegative(c) : null,
    }))
    .sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));

  const eligibilityFingerprint = sha256Hex(
    stableStringify({
      eligibleCaseIds: enriched.map((c) => c.caseId).sort(),
      selectionVersion: SELECTION_VERSION,
      selectionSeed: SELECTION_SEED,
      TOTAL_N: 100,
      PRESENCE_TRUE_N: 75,
      PRESENCE_FALSE_N: 25,
      cap: CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
    })
  );

  const contentBody = {
    caseIds: casePayload.map((c) => c.caseId),
    sourceResponseIds: [...holdoutResponseIds].sort(),
    humanFinalLabels: casePayload.map((c) => ({
      caseId: c.caseId,
      label: c.humanFinalLabel,
    })),
    presentCount: presentN,
    notPresentCount: notPresentN,
  };
  const contentHash = sha256Hex(stableStringify(contentBody));

  const manifestCore = {
    version: "ai_intelligence_presence_holdout_v2",
    holdoutVersion: "ai_intelligence_presence_holdout_v2",
    createdAt,
    STATUS: "READY_UNSCORED",
    HOLDOUT_V2_FINAL_FREEZE_ALLOWED: "YES_FROZEN",
    UNTOUCHED: true,
    SCORED: false,
    USED_FOR_TUNING: false,
    PREDICTIONS_EXPOSED: false,
    HOLDOUT_V2_SCORING: 0,
    selectionAlgorithm: SELECTION_ALGORITHM,
    selectionVersion: SELECTION_VERSION,
    selectionSeed: SELECTION_SEED,
    eligibilityRules: [
      "HUMAN_FINALIZED PRESENT|NOT_PRESENT",
      "canonicalEntityId present",
      "sourceResponseId present",
      "rawText present",
      "not Golden/DEV/Holdout v1/classifier-lab (fresh presence validation pool only)",
      "response-level atomicity",
      "max 2 pairs per sourceResponseId",
    ],
    tieBreakRules: [
      "caseId lexicographic within provider|language|geography buckets",
      "prefer 1 PRESENT + 1 NOT_PRESENT per response when capping",
    ],
    design: {
      TOTAL_N: 100,
      PRESENCE_TRUE_N: 75,
      PRESENCE_FALSE_N: 25,
      CANDIDATE_CAP_PER_RESPONSE: CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
    },
    pairCount: selected.length,
    uniqueResponseCount: uniqN,
    presentCount: presentN,
    notPresentCount: notPresentN,
    providerCounts: countBy(selected, "provider"),
    languageCounts: countBy(selected, "language"),
    geographyCounts: countBy(selected, "geography"),
    negativeControlCounts,
    caseIds: casePayload.map((c) => c.caseId),
    sourceResponseIds: [...holdoutResponseIds].sort(),
    responseHashes: casePayload.map((c) => c.responseHash),
    canonicalEntityIds: casePayload.map((c) => c.canonicalEntityId),
    humanFinalLabels: casePayload.map((c) => ({
      caseId: c.caseId,
      label: c.humanFinalLabel,
    })),
    cases: casePayload,
    holdoutSameResponseRemainderCaseIds: holdoutSameResponseRemainder
      .map((c) => c.caseId)
      .sort(),
    RESPONSE_LEVEL_ATOMICITY: overCap.length === 0 && partCheck.ok ? "PASS" : "FAIL",
    MAX_PAIRS_PER_RESPONSE: maxPairs,
    metricContract: PRESENCE_HOLDOUT_V2_METRIC_CONTRACT,
    eligibilityFingerprint,
    contentHash,
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

  const manifestHash = sha256Hex(stableStringify(manifestCore));
  const manifest = {
    ...manifestCore,
    manifestHash,
    HOLDOUT_V2_SEALED: true,
    sealedAt: createdAt,
  };

  const reserveDoc = {
    version: "presence_validation_reserve_v1",
    partition: "RESERVE",
    createdAt,
    relatedHoldout: "ai_intelligence_presence_holdout_v2",
    relatedManifestHash: manifestHash,
    PAIR_N: reserve.length,
    UNIQUE_RESPONSE_N: uniqueResponseIds(reserve).size,
    PRESENT_N: reserve.filter((c) => c.humanLabel === "PRESENT").length,
    NOT_PRESENT_N: reserve.filter((c) => c.humanLabel === "NOT_PRESENT").length,
    caseIds: reserve.map((c) => c.caseId).sort(),
    sourceResponseIds: [...uniqueResponseIds(reserve)].sort(),
    note: "Eligible human-labelled Presence pairs not in Holdout v2 scored set and not sharing a Holdout sourceResponseId. May support later DEV/regression/drift — never mutate frozen Holdout v2.",
  };

  const report = {
    phase: "PRESENCE_HOLDOUT_V2_SELECTION_AND_FREEZE_COMPLETE",
    status: "PRESENCE_HOLDOUT_V2_SELECTION_AND_FREEZE_PASS",
    DRY_RUN: DRY_RUN,
    VERSION: manifest.holdoutVersion,
    PAIR_N: selected.length,
    UNIQUE_RESPONSE_N: uniqN,
    PRESENT: presentN,
    NOT_PRESENT: notPresentN,
    PROVIDERS: manifest.providerCounts,
    LANGUAGES: manifest.languageCounts,
    GEOGRAPHIES: manifest.geographyCounts,
    NEGATIVE_CONTROLS: negativeControlCounts,
    SELECTION: {
      ALGORITHM: SELECTION_ALGORITHM,
      VERSION: SELECTION_VERSION,
      SEED: SELECTION_SEED,
    },
    RESPONSE_LEVEL_ATOMICITY: manifest.RESPONSE_LEVEL_ATOMICITY,
    MAX_PAIRS_PER_RESPONSE: maxPairs,
    SEAL: {
      MANIFEST: path.relative(ROOT, OUT_MANIFEST).replace(/\\/g, "/"),
      MANIFEST_HASH: manifestHash,
      CONTENT_HASH: contentHash,
      UNTOUCHED: true,
      SCORED: false,
      USED_FOR_TUNING: false,
      PREDICTIONS_EXPOSED: false,
      HOLDOUT_V2_SEALED: true,
    },
    RESERVE: {
      PAIR_N: reserveDoc.PAIR_N,
      UNIQUE_RESPONSE_N: reserveDoc.UNIQUE_RESPONSE_N,
      PRESENT: reserveDoc.PRESENT_N,
      NOT_PRESENT: reserveDoc.NOT_PRESENT_N,
      HOLDOUT_SAME_RESPONSE_REMAINDER: holdoutSameResponseRemainder.length,
    },
    SCORECARD: manifest.scorecard,
    REGIONALIZATION: manifest.regionalization,
    NEXT_STEP: "READY_FOR_ONE_TIME_PRESENCE_HOLDOUT_V2_SCORE",
  };

  if (selected.length !== 100 || presentN !== 75 || notPresentN !== 25) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          error: "COMPOSITION_MISMATCH",
          selected: selected.length,
          presentN,
          notPresentN,
        },
        null,
        2
      )
    );
    process.exit(2);
  }
  if (overCap.length || !partCheck.ok) {
    console.error(JSON.stringify({ ok: false, error: "ATOMICITY_FAIL", overCap, partCheck }, null, 2));
    process.exit(2);
  }

  console.log(JSON.stringify(report, null, 2));

  if (DRY_RUN) {
    console.log("\nDry-run only — nothing written.");
    return;
  }

  // Persist partitions on candidates (labels unchanged)
  const holdoutCaseSet = selectedIds;
  const holdoutRespSet = holdoutResponseIds;
  for (const c of candDoc.cases || []) {
    const rid = c.sourceResponseId || c.responseId;
    if (holdoutCaseSet.has(c.caseId) || holdoutRespSet.has(rid)) {
      c.validationPartition = "HOLDOUT";
      c.holdoutV2Member = holdoutCaseSet.has(c.caseId);
    } else if (R[c.caseId] && (R[c.caseId].action === "PRESENT" || R[c.caseId].action === "NOT_PRESENT")) {
      // human-labelled eligible not in holdout response family → RESERVE
      const inReserve = reserve.some((x) => x.caseId === c.caseId);
      c.validationPartition = inReserve ? "RESERVE" : c.validationPartition || "UNASSIGNED";
      c.holdoutV2Member = false;
    } else {
      c.holdoutV2Member = false;
      if (!c.validationPartition || c.validationPartition === "UNASSIGNED") {
        c.validationPartition = "UNASSIGNED";
      }
    }
  }
  candDoc.holdoutV2 = {
    sealed: true,
    sealedAt: createdAt,
    manifestHash,
    contentHash,
    pairN: selected.length,
    uniqueResponseN: uniqN,
  };
  candDoc.HOLDOUT_V2_FINAL_FREEZE_ALLOWED = "YES_FROZEN";
  candDoc.HOLDOUT_V2_SEALED = true;

  fs.writeFileSync(OUT_MANIFEST, JSON.stringify(manifest, null, 2) + "\n");
  fs.writeFileSync(OUT_RESERVE, JSON.stringify(reserveDoc, null, 2) + "\n");
  fs.writeFileSync(CANDIDATES_PATH, JSON.stringify(candDoc, null, 2) + "\n");
  fs.writeFileSync(OUT_REPORT, JSON.stringify(report, null, 2) + "\n");

  console.log(`\nWrote ${OUT_MANIFEST}`);
  console.log(`Wrote ${OUT_RESERVE}`);
  console.log(`Wrote ${OUT_REPORT}`);
  console.log("HOLDOUT_V2_SEALED = YES — do not score until authorized.");
}

main();
