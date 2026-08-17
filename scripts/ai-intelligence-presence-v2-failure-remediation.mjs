#!/usr/bin/env node
/**
 * Presence v2 failure remediation — DEV/Reserve only.
 *
 * HOLDOUT_V2_CHANGES=0 · HOLDOUT_V2_RESCORE=0 · HOLDOUT_V2_TUNING_FIXTURES=0
 * PROVIDER_CALLS=0 · NEW_HOLDOUT_GENERATION=0
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { findEntitySpans, RESOLVER_VERSION } from "../lib/ai-visibility/normalize-entities.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { loadGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { validateHoldoutManifestIntegrity } from "../lib/ai-visibility/validation/holdout-manifest-integrity.js";
import { computePresenceValidationMetrics } from "../lib/ai-visibility/validation/presence-validation-pool-governance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const HOLDOUT_V2 = path.join(
  ROOT,
  "data/ai-visibility/validation/ai-intelligence-presence-holdout-v2.json"
);
const RESERVE = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-reserve.json"
);
const CANDIDATES = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-candidates/candidates/candidates.json"
);
const REVIEWS = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-candidates/reviews/reviews.json"
);
const REGRESSION = path.join(
  ROOT,
  "fixtures/ai-visibility/contextual-canopy-regression.json"
);
const OUT = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-v2-failure-remediation.json"
);

const PRECISION_GATE = 0.98;
const RECALL_GATE = 0.98;

function entityPresent(text, caseRow, aliasIndex, applyContextualAliases) {
  const spans = findEntitySpans(String(text || ""), aliasIndex, {
    applyContextualAliases,
  });
  const name = caseRow.canonicalEntityName || caseRow.entityName;
  const id = caseRow.canonicalEntityId || caseRow.entityId;
  return spans.some((s) => {
    if (id && s.entity?.id === id) return true;
    if (name && s.entity?.name === name) return true;
    return false;
  });
}

function scorePairs(pairs, aliasIndex, applyContextualAliases) {
  let tp = 0;
  let tn = 0;
  let fp = 0;
  let fn = 0;
  const errors = [];
  for (const row of pairs) {
    const expected = row.expected === true || row.expected === "PRESENT";
    const actual = entityPresent(row.text, row, aliasIndex, applyContextualAliases);
    if (expected && actual) tp += 1;
    else if (!expected && !actual) tn += 1;
    else if (!expected && actual) {
      fp += 1;
      errors.push({
        caseId: row.caseId,
        type: "FP",
        entity: row.canonicalEntityName || row.entityName,
      });
    } else {
      fn += 1;
      errors.push({
        caseId: row.caseId,
        type: "FN",
        entity: row.canonicalEntityName || row.entityName,
      });
    }
  }
  const metrics = computePresenceValidationMetrics({ tp, tn, fp, fn });
  return { tp, tn, fp, fn, metrics, errors, N: pairs.length };
}

function fmt(m) {
  return {
    N: m.N,
    TP: m.tp,
    TN: m.tn,
    FP: m.fp,
    FN: m.fn,
    PRECISION: m.metrics.Precision,
    RECALL: m.metrics.Recall,
    F1: m.metrics.F1,
    ACCURACY: m.metrics.Accuracy,
  };
}

async function main() {
  const holdout = JSON.parse(fs.readFileSync(HOLDOUT_V2, "utf8"));
  const holdoutCaseIds = new Set(holdout.caseIds || []);
  const holdoutIntegrity = validateHoldoutManifestIntegrity(holdout.cases || []);

  // Historical artifact audit only — do not mutate
  const holdoutPreserved = {
    STATUS: holdout.STATUS,
    UNTOUCHED: holdout.UNTOUCHED === false ? "NO" : holdout.UNTOUCHED === true ? "YES" : holdout.UNTOUCHED,
    SCORED: holdout.SCORED === true ? "YES" : "NO",
    USED_FOR_TUNING: holdout.USED_FOR_TUNING === true ? "YES" : "NO",
    MODIFIED: "NO",
    RESCORED: "NO",
    DUPLICATE_CASE_ID_IN_SEAL: holdoutIntegrity.duplicateCaseIds,
    NOTE: "Holdout v2 left sealed as SCORED_FAIL; duplicate not repaired.",
  };

  const duplicateBug = {
    ROOT_CAUSE:
      "selectHoldoutV2WithResponseGovernance response-cap stage OR-filtered humanLabel with candidateType, so CHANGED rows (human PRESENT + candidateType PRESENCE_FALSE) entered BOTH true and false buckets; first two picks pushed the same caseId twice without dedupe; takeRespectingResponseAtomicity also lacked caseId uniqueness; freeze script had no UNIQUE_CASE_ID_COUNT==PAIR_N seal guard.",
    SELECTION_STAGE:
      "selectHoldoutV2WithResponseGovernance → per-response cap picks (trues/falses OR-filter) → PRESENT take into selected[]",
    WHY_CASE_ID_UNIQUENESS_DID_NOT_BLOCK:
      "No selectedCaseIds set; only late fill loop checked caseId; duplicate already inserted during PRESENT take from double-bucketed cappedRows.",
    WHY_MANIFEST_VALIDATION_DID_NOT_BLOCK:
      "Freeze path checked composition counts and response atomicity/cap only — no validateHoldoutManifestIntegrity / DO_NOT_SEAL on duplicate caseIds.",
    DUPLICATE_CASE_ID: "presval_260089d8b1bc",
    OTHER_MANIFESTS_WITH_DUPLICATE_CASE_IDS: "NONE (audit of data/ai-visibility/validation/*.json)",
    FUTURE_DUPLICATE_GUARD:
      "validateHoldoutManifestIntegrity fail-closed before seal; selection humanLabel-wins + caseId dedupe",
  };

  const index = buildGoldenSetScoringEntityIndex({});
  const candDoc = JSON.parse(fs.readFileSync(CANDIDATES, "utf8"));
  const reviews = JSON.parse(fs.readFileSync(REVIEWS, "utf8")).reviews || {};
  const byId = new Map((candDoc.cases || []).map((c) => [c.caseId, c]));
  const reserveDoc = JSON.parse(fs.readFileSync(RESERVE, "utf8"));
  const reserveIds = new Set(reserveDoc.caseIds || []);

  // Reserve pairs — never Holdout v2
  const reservePairs = [];
  for (const caseId of reserveIds) {
    if (holdoutCaseIds.has(caseId)) continue;
    const c = byId.get(caseId);
    const r = reviews[caseId];
    if (!c?.rawText || !r) continue;
    if (r.action !== "PRESENT" && r.action !== "NOT_PRESENT") continue;
    reservePairs.push({
      caseId,
      text: c.rawText,
      expected: r.action === "PRESENT",
      canonicalEntityId: c.canonicalEntityId,
      canonicalEntityName: c.canonicalEntityName,
      sourceResponseId: c.sourceResponseId || c.responseId,
    });
  }

  // DEV = Golden Set development split (not holdout) — Presence labels only
  const golden = loadGoldenSet();
  const devCases = (golden.cases || []).filter((c) => c.holdoutSplit !== "holdout");
  const { cases: hydratedDev } = await hydrateGoldenSetCasesForScoring(devCases, {});
  const devPairs = hydratedDev
    .map((c) => {
      const expected =
        c.expectedEntityPresent != null
          ? !!c.expectedEntityPresent
          : !!(c.entityName || c.canonicalEntityId);
      return {
        caseId: c.caseId,
        text: c.text || c.rawResponseExcerpt || "",
        expected,
        canonicalEntityId: c.canonicalEntityId,
        canonicalEntityName: c.entityName || c.canonicalEntityName,
        entityName: c.entityName,
      };
    })
    .filter((c) => c.text && c.caseId);

  // Plus non-holdout human-labelled presence pool rows not in Reserve (DEV pool remainder)
  // excluding Holdout — for Canopy PRESENT companion case
  const poolDevExtra = [];
  for (const c of candDoc.cases || []) {
    if (holdoutCaseIds.has(c.caseId)) continue;
    if (reserveIds.has(c.caseId)) continue;
    const r = reviews[c.caseId];
    if (!r || (r.action !== "PRESENT" && r.action !== "NOT_PRESENT")) continue;
    if (!c.rawText) continue;
    poolDevExtra.push({
      caseId: c.caseId,
      text: c.rawText,
      expected: r.action === "PRESENT",
      canonicalEntityId: c.canonicalEntityId,
      canonicalEntityName: c.canonicalEntityName,
      sourceResponseId: c.sourceResponseId || c.responseId,
    });
  }

  const devCombined = [...devPairs, ...poolDevExtra];

  const reserveBefore = scorePairs(reservePairs, index.aliasIndex, false);
  const reserveAfter = scorePairs(reservePairs, index.aliasIndex, true);
  const devBefore = scorePairs(devCombined, index.aliasIndex, false);
  const devAfter = scorePairs(devCombined, index.aliasIndex, true);

  const regression = JSON.parse(fs.readFileSync(REGRESSION, "utf8"));
  let posPass = 0;
  let negPass = 0;
  const posFail = [];
  const negFail = [];
  for (const row of regression.positive || []) {
    const spans = findEntitySpans(row.text, index.aliasIndex, { applyContextualAliases: true });
    const ok = spans.some((s) => s.entity?.name === row.expectEntity);
    if (ok) posPass += 1;
    else posFail.push(row.id);
  }
  for (const row of regression.negative || []) {
    const spans = findEntitySpans(row.text, index.aliasIndex, { applyContextualAliases: true });
    const bad = spans.some((s) => s.entity?.name === row.expectEntityAbsent);
    if (!bad) negPass += 1;
    else negFail.push(row.id);
  }
  const regressionPass =
    posFail.length === 0 &&
    negFail.length === 0 &&
    posPass === (regression.positive || []).length &&
    negPass === (regression.negative || []).length;

  const fpsCreated = Math.max(0, reserveAfter.fp - reserveBefore.fp) + Math.max(0, devAfter.fp - devBefore.fp);
  const canopyFps = [...reserveAfter.errors, ...devAfter.errors].filter(
    (e) => e.type === "FP" && /canopy/i.test(e.entity || "")
  );

  const reserveMeets =
    reserveAfter.metrics.Precision != null &&
    reserveAfter.metrics.Recall != null &&
    reserveAfter.metrics.Precision >= PRECISION_GATE &&
    reserveAfter.metrics.Recall >= RECALL_GATE;
  const devMeets =
    devAfter.metrics.Precision != null &&
    devAfter.metrics.Recall != null &&
    devAfter.metrics.Precision >= PRECISION_GATE &&
    devAfter.metrics.Recall >= RECALL_GATE;

  const remediationReady =
    regressionPass &&
    fpsCreated === 0 &&
    canopyFps.length === 0 &&
    // Require no precision regression on Reserve (Presence precision essential)
    (reserveAfter.metrics.Precision ?? 0) >= (reserveBefore.metrics.Precision ?? 0) &&
    (reserveAfter.metrics.Recall ?? 0) >= (reserveBefore.metrics.Recall ?? 0) &&
    (devAfter.metrics.Precision ?? 0) >= (devBefore.metrics.Precision ?? 0);

  // Material FP rejection rule
  const rejectedForFp = fpsCreated > 0 || canopyFps.length > 0;

  const readyForHoldoutV3 = remediationReady && !rejectedForFp;

  const report = {
    phase: "PRESENCE_V2_FAILURE_REMEDIATION_COMPLETE",
    status: readyForHoldoutV3
      ? "READY_FOR_FRESH_PRESENCE_HOLDOUT_V3_GENERATION"
      : "PRESENCE_RESOLVER_REMEDIATION_REVIEW_REQUIRED",
    scoredAt: new Date().toISOString(),
    holdoutV2: holdoutPreserved,
    duplicateIntegrityBug: duplicateBug,
    canopy: {
      CURRENT_FAILURE:
        "Bare Canopy in Hilton brand-family context did not resolve to Canopy by Hilton (alias gap).",
      CONTEXTUAL_RULE_IMPLEMENTED: "YES — CONTEXTUAL_ALIAS canopy_by_hilton_contextual_v1",
      GLOBAL_BARE_ALIAS_ADDED: "NO",
      requiredParentContext: "Hilton",
      rejectOrdinaryLanguage: true,
    },
    resolver: {
      NEW_VERSION: RESOLVER_VERSION,
      PREVIOUS_VERSION: "ai_visibility_entity_resolver_v2",
      changeReason:
        "Add narrow CONTEXTUAL_ALIAS for Canopy→Canopy by Hilton only with Hilton brand-family/parent context; reject ordinary-language canopy.",
      REMEDIATION_READY: readyForHoldoutV3 ? "YES" : "NO",
      PRODUCTION_CERTIFIED: "NO",
    },
    regression: {
      POSITIVE_CASES: (regression.positive || []).length,
      NEGATIVE_CASES: (regression.negative || []).length,
      POSITIVE_PASS: posPass,
      NEGATIVE_PASS: negPass,
      POSITIVE_FAIL: posFail,
      NEGATIVE_FAIL: negFail,
      RESULT: regressionPass ? "PASS" : "FAIL",
      holdoutV2FixturesUsed: 0,
    },
    DEV: {
      BEFORE: fmt(devBefore),
      AFTER: fmt(devAfter),
      SOURCE:
        "Golden Set development split + non-holdout/non-reserve human-labelled presence pool",
      N_GOLDEN_DEV: devPairs.length,
      N_POOL_EXTRA: poolDevExtra.length,
      N_COMBINED: devCombined.length,
      meetsGate: devMeets,
    },
    RESERVE: {
      BEFORE: fmt(reserveBefore),
      AFTER: fmt(reserveAfter),
      N: reservePairs.length,
      meetsGate: reserveMeets,
    },
    FALSE_POSITIVES_CREATED: fpsCreated,
    CANOPY_FALSE_POSITIVES: canopyFps,
    productionGate: {
      PRECISION_THRESHOLD: "98%",
      RECALL_THRESHOLD: "98%",
      THRESHOLD_LOWERING: 0,
    },
    freshCertification: {
      NEW_UNSEEN_RESPONSES_REQUIRED: "YES",
      READY_FOR_HOLDOUT_V3_GENERATION: readyForHoldoutV3 ? "YES" : "NO",
      HOLDOUT_V3_GENERATED: "NO",
      mustInclude: [
        "PRESENT and NOT_PRESENT",
        "all four production providers",
        "English + Spanish",
        "governed geographies",
        "hard negative controls",
        "unique case IDs",
        "broad unique-response coverage",
        "contextual short-brand-name cases",
        "no duplicate rows",
        "UNIQUE_CASE_ID_COUNT == PAIR_N seal guard",
      ],
      doNotReuse: [
        "Holdout v1",
        "Holdout v2",
        "DEV",
        "reviewed reserve responses",
        "classifier-lab responses",
        "previously inspected validation responses",
      ],
    },
    regionalization: {
      STATUS: "PLANNED_AFTER_PRESENCE_CERTIFICATION",
      EXECUTED: false,
    },
    hardGuards: {
      HOLDOUT_V2_CHANGES: 0,
      HOLDOUT_V2_RESCORE: 0,
      HOLDOUT_V2_TUNING_FIXTURES: 0,
      GROUND_TRUTH_CHANGES: 0,
      THRESHOLD_LOWERING: 0,
      PROVIDER_CALLS: 0,
      NEW_HOLDOUT_GENERATION: 0,
      RECOMMENDED_WORK: 0,
      FIRST_RECOMMENDATION_WORK: 0,
      REGIONALIZATION_EXECUTION: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
    },
    nextStep: readyForHoldoutV3
      ? "READY_FOR_FRESH_PRESENCE_HOLDOUT_V3_GENERATION"
      : "PRESENCE_RESOLVER_REMEDIATION_REVIEW_REQUIRED",
  };

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(report, null, 2) + "\n", "utf8");
  console.log("PRESENCE_V2_FAILURE_REMEDIATION_COMPLETE");
  console.log(`status=${report.status}`);
  console.log(`resolver=${RESOLVER_VERSION}`);
  console.log(
    `DEV before P=${devBefore.metrics.Precision} R=${devBefore.metrics.Recall} → after P=${devAfter.metrics.Precision} R=${devAfter.metrics.Recall}`
  );
  console.log(
    `RESERVE before P=${reserveBefore.metrics.Precision} R=${reserveBefore.metrics.Recall} → after P=${reserveAfter.metrics.Precision} R=${reserveAfter.metrics.Recall} FP=${reserveAfter.fp}`
  );
  console.log(`regression=${report.regression.RESULT} fpsCreated=${fpsCreated}`);
  console.log(`wrote ${path.relative(ROOT, OUT)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
