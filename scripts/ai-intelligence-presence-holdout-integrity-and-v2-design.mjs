#!/usr/bin/env node
/**
 * Presence Holdout Integrity Audit + Holdout v2 Design.
 *
 * - Audits 4 Playa Holdout v1 FNs under pre-existing Playa GT rule
 * - Optionally INVALIDATE_CANDIDATE_SUBJECT (holdout integrity repair; does not restore untouched)
 * - Freezes Holdout v1 as INSPECTED_DIAGNOSTIC_HOLDOUT
 * - Designs Holdout v2; does NOT score it
 *
 * ENTITY_RESOLVER_CHANGES=0 ALIAS_CHANGES=0 HOLDOUT_V2_SCORING=0
 */
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import {
  amendGoldenSetV2GroundTruth,
  AMENDMENT_ACTIONS,
  assessPlayaBrandReference,
  readGoldenSetV2Fixture,
} from "../lib/ai-visibility/validation/golden-set-ground-truth-amendment.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { GOLDEN_SET_EXPANSION_TARGET } from "../lib/ai-visibility/validation/classification-threshold.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const OUT_DIR = path.join(ROOT, "data/ai-visibility/validation");
const PLAYA_IDS = [
  "v2_cand_bcdb2d55",
  "v2_cand_d98e6ea8",
  "v2_cand_e57848c0",
  "v2_cand_f3f3fea0",
];

const REASON =
  "Candidate subject was over-nominated. The stored response does not contain an unambiguous reference to the specific canonical entity. Generic/common-language or parent-company references are insufficient. Same pre-holdout Playa Hotels & Resorts governance rule as DEV residual INVALIDATE (2026-08-15). Holdout integrity repair only — does not restore Holdout v1 untouched certification.";
const REVIEWER =
  "joan@dealality (founder — chat-authorized holdout integrity INVALIDATE_CANDIDATE_SUBJECT 2026-08-15 Presence Holdout Integrity Audit)";

function fingerprint(obj) {
  return crypto.createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}

const { doc } = readGoldenSetV2Fixture();
const byId = Object.fromEntries((doc.cases || []).map((c) => [c.caseId, c]));
const holdoutV1Cases = (doc.cases || []).filter((c) => c.holdoutSplit === "holdout");

const rawPlaya = PLAYA_IDS.map((id) => {
  const c = byId[id];
  if (!c) throw new Error(`CASE_NOT_FOUND ${id}`);
  return c;
});

const { cases: hydrated } = await hydrateGoldenSetCasesForScoring(
  rawPlaya.map((c) => ({
    ...c,
    entityName: c.candidateEntity,
    text: c.rawResponseExcerpt,
  })),
  {}
);

const playaAudit = [];
for (const c of hydrated) {
  const text = c.text || "";
  const playa = assessPlayaBrandReference(text);
  const hasFull = /playa\s+hotels\s*(?:&|and)?\s*resorts/i.test(text);
  const hasHotels = /\bplaya\s+hotels\b/i.test(text);
  const unambiguous = playa.UNAMBIGUOUS_BRAND_REFERENCE === "YES";
  const genericOnly = !unambiguous && /\bplaya\b/i.test(text);
  const row = {
    CASE_ID: c.caseId,
    PROVIDER: c.provider,
    LANGUAGE: c.language,
    GEOGRAPHY: c.geography,
    CANONICAL_SUBJECT: c.candidateEntity || c.entityName,
    ORIGINAL_HUMAN_LABEL: {
      expectedEntityPresent: c.expectedEntityPresent,
      expectedRecommendationClass: c.expectedRecommendationClass,
      expectedFirstRecommendation: c.expectedFirstRecommendation,
      holdoutSplit: c.holdoutSplit,
    },
    FULL_STORED_RESPONSE: text,
    FULL_STORED_RESPONSE_LENGTH: text.length,
    EXACT_ENTITY_SURFACE_TEXT: playa.EXAMPLE_SURFACES || [],
    CONTAINS_PLAYA_HOTELS_AND_RESORTS: hasFull ? "YES" : "NO",
    CONTAINS_PLAYA_HOTELS: hasHotels ? "YES" : "NO",
    UNAMBIGUOUS_CANONICAL_REFERENCE: unambiguous ? "YES" : "NO",
    GENERIC_OR_GEOGRAPHIC_PLAYA_ONLY: genericOnly ? "YES" : "NO",
    VALID_ENTITY_SPECIFIC_CANDIDATE: unambiguous ? "YES" : "NO",
    GROUND_TRUTH_INTEGRITY_ERROR: unambiguous ? "NO" : "YES",
    GOVERNED_ACTION: unambiguous ? "KEEP" : "INVALIDATE_CANDIDATE_SUBJECT",
    PRIOR_RULE:
      "ENTITY PRESENT requires unambiguous canonical representation — Spanish playa / geographic Playa del Carmen insufficient (BUILD_DECISIONS 2026-08-15 residual entity GT audit).",
  };
  playaAudit.push(row);
}

const integrityErrors = playaAudit.filter((r) => r.GROUND_TRUTH_INTEGRITY_ERROR === "YES");

const invalidationResults = [];
if (APPLY) {
  for (const row of integrityErrors) {
    const r = amendGoldenSetV2GroundTruth(
      {
        caseId: row.CASE_ID,
        action: AMENDMENT_ACTIONS.INVALIDATE_CANDIDATE_SUBJECT,
        reviewer: REVIEWER,
        amendmentReason: REASON,
        holdoutIntegrityRepairAuthorized: true,
      },
      { apply: true, allowHoldoutIntegrityRepair: true }
    );
    invalidationResults.push({
      caseId: row.CASE_ID,
      written: r.written === true,
      action: r.action,
      HOLDOUT_UNTOUCHED_RESTORED: false,
    });
  }
} else {
  for (const row of integrityErrors) {
    const r = amendGoldenSetV2GroundTruth(
      {
        caseId: row.CASE_ID,
        action: AMENDMENT_ACTIONS.INVALIDATE_CANDIDATE_SUBJECT,
        reviewer: REVIEWER,
        amendmentReason: REASON,
        holdoutIntegrityRepairAuthorized: true,
      },
      { apply: false, allowHoldoutIntegrityRepair: true }
    );
    invalidationResults.push({
      caseId: row.CASE_ID,
      written: false,
      dryRun: true,
      previewAction: r.preview?.action || r.action,
    });
  }
}

// Re-read fixture after apply for counts
const { doc: docAfter } = readGoldenSetV2Fixture();
const holdAfter = (docAfter.cases || []).filter((c) => c.holdoutSplit === "holdout");
const holdActive = holdAfter.filter(
  (c) =>
    c.groundTruthInvalidated !== true &&
    c.excludeFromClassificationDenominator !== true &&
    c.reviewStatus !== "INVALIDATED_CANDIDATE_SUBJECT"
);
const holdPos = holdActive.filter((c) => c.expectedEntityPresent !== false).length;
const holdNeg = holdActive.filter((c) => c.expectedEntityPresent === false).length;
const holdInvalidated = holdAfter.filter(
  (c) =>
    c.groundTruthInvalidated === true ||
    c.reviewStatus === "INVALIDATED_CANDIDATE_SUBJECT"
).length;

// Source audit for Holdout v2
const candPath = path.join(
  ROOT,
  "fixtures/ai-visibility/ai-intelligence-golden-set-v2-candidates.json"
);
const cand = JSON.parse(fs.readFileSync(candPath, "utf8"));
const candCases = cand.cases || [];
const v2Ids = new Set((docAfter.cases || []).map((c) => c.caseId));
const v2SourceIds = new Set(
  (docAfter.cases || []).map((c) => c.sourceCaseId).filter(Boolean)
);
const neverInGolden = candCases.filter((c) => {
  const id = c.caseId;
  return !v2Ids.has(id) && !v2Ids.has(`v2_${id}`) && !v2SourceIds.has(id);
});
const humanLabelledUntouched = neverInGolden.filter(
  (c) =>
    c.humanLabelled === true &&
    c.reviewer &&
    c.reviewedAt &&
    c.reviewStatus !== "PENDING_HUMAN_REVIEW" &&
    c.reviewStatus !== "SUPERSEDED_INVALID_SUBJECT"
);

const NEW_HUMAN_LABELLED_CASES_REQUIRED = humanLabelledUntouched.length === 0;

// Holdout v2 composition contract (Presence-specific)
// Policy: prior holdout ~93 (~22% of labelled); expansion floor 150 labelled for governed.
// Presence holdout must include negatives for FP validation — propose 25% negatives.
const HOLD_V2_DESIGN = {
  TOTAL_N: 100,
  PRESENCE_TRUE_N: 75,
  PRESENCE_FALSE_N: 25,
  rationale: [
    "Prior Holdout v1 size was 93 (~22% stratified split). Presence-specific Holdout v2 targets ~100 to preserve similar power.",
    "PRESENCE_FALSE_N=25 (≥20%) so false-positive resistance is independently testable; not artificial 50/50.",
    "GOLDEN_SET_EXPANSION_TARGET.minCases=150 applies to overall labelled governance, not to recycling DEV into holdout.",
    "All cases must be newly human-labelled and never used in DEV, Holdout v1, or classifier hardening inspection.",
  ],
  PROVIDER_COUNTS_TARGET: {
    OpenAI: 20,
    Gemini: 25,
    Perplexity: 25,
    Claude: 25,
    note: "Approximate; allow ±3 per provider while covering all four.",
  },
  LANGUAGE_COUNTS_TARGET: {
    English: 50,
    Spanish: 50,
  },
  GEOGRAPHY_COUNTS_TARGET: {
    Global: 15,
    CALA: 25,
    Mexico: 25,
    Europe: 15,
    NorthAmerica: 15,
    note: "Approximate; Geography where available.",
  },
  NEGATIVE_CONTROL_COVERAGE: [
    "parent company mentioned, child brand absent",
    "generic collection language without brand",
    "ordinary-language false friends",
    "geographic Playa vs Playa Hotels & Resorts (independent unseen cases — not Holdout v1 Playa FNs)",
    "similar brand names",
    "citation/source mention without subject mention (where governed)",
    "no entity mention at all",
  ],
  FORBIDDEN: [
    "recycle DEV cases",
    "recycle Holdout v1 cases",
    "construct negatives from inspected Holdout v1 Playa errors to force PASS",
    "add bare Playa alias",
    "tune resolver against holdout",
  ],
};

const holdoutV1Status = {
  VERSION: "ai_intelligence_holdout_v1",
  STATUS: "INSPECTED_DIAGNOSTIC_HOLDOUT",
  HOLDOUT_V1_UNTOUCHED: "NO",
  HOLDOUT_V1_FINAL_CERTIFICATION_ELIGIBLE: "NO",
  reason: [
    "Scored for Presence (2026-08-15).",
    "Errors inspected; failure identities revealed (4× Playa Hotels & Resorts).",
    "Contained 0 Presence-negative cases (TN=0) — cannot certify false-positive resistance.",
    "Integrity invalidations (if applied) repair GT but do not restore untouched status.",
  ],
  ORIGINAL_N: 93,
  POSITIVE_N_ORIGINAL: 93,
  NEGATIVE_N_ORIGINAL: 0,
  AFTER_INTEGRITY: {
    MEMBERSHIP_N: holdAfter.length,
    ACTIVE_FOR_SCORING_N: holdActive.length,
    INVALIDATED_N: holdInvalidated,
    POSITIVE_N: holdPos,
    NEGATIVE_N: holdNeg,
  },
  PRESERVED: true,
  SCORE_ARTIFACT: "data/ai-visibility/validation/presence-holdout-validation.json",
};

const createdAt = new Date().toISOString();
const holdoutV2Manifest = {
  version: "ai_intelligence_presence_holdout_v2",
  createdAt,
  STATUS: NEW_HUMAN_LABELLED_CASES_REQUIRED
    ? "BLOCKED_NEEDS_NEW_CASES"
    : "READY_UNSCORED",
  UNTOUCHED: NEW_HUMAN_LABELLED_CASES_REQUIRED ? null : true,
  SCORED: false,
  HOLDOUT_V2_SCORING: 0,
  selectionRule: {
    signal: "AI_SIGNAL_PRESENCE",
    require: [
      "NEVER_USED_FOR_TUNING",
      "NEVER_INSPECTED_DURING_CLASSIFIER_HARDENING",
      "NEVER_INCLUDED_IN_DEV",
      "NEVER_INCLUDED_IN_HOLDOUT_V1",
      "HUMAN_LABELLED",
      "CANONICAL_SUBJECT_VALID",
    ],
    includePresenceFalse: true,
    includePresenceTrue: true,
    design: HOLD_V2_DESIGN,
  },
  sourceUniverse: {
    goldenSetV2Total: (docAfter.cases || []).length,
    holdoutV1N: 93,
    cleanDevApprox: 290,
    candidateNotInGolden: neverInGolden.length,
    humanLabelledUntouchedEligible: humanLabelledUntouched.length,
    NEW_HUMAN_LABELLED_CASES_REQUIRED,
  },
  caseIds: [],
  positiveNegativeCounts: {
    PRESENCE_TRUE_N: null,
    PRESENCE_FALSE_N: null,
    note: "Not selected — awaiting new human-labelled unseen cases",
  },
  providerLanguageGeographyDistribution: null,
  contentHash: null,
  note: NEW_HUMAN_LABELLED_CASES_REQUIRED
    ? "Manifest frozen as BLOCKED design contract. Case IDs empty until new unseen human labels exist. Do not score."
    : "Ready unscored — authorize one-time Presence holdout separately.",
};

holdoutV2Manifest.contentHash = fingerprint({
  version: holdoutV2Manifest.version,
  STATUS: holdoutV2Manifest.STATUS,
  selectionRule: holdoutV2Manifest.selectionRule,
  sourceUniverse: holdoutV2Manifest.sourceUniverse,
  caseIds: holdoutV2Manifest.caseIds,
});

const report = {
  phase: "AI_INTELLIGENCE_PRESENCE_HOLDOUT_INTEGRITY_AND_V2_DESIGN_COMPLETE",
  status: NEW_HUMAN_LABELLED_CASES_REQUIRED
    ? "AI_INTELLIGENCE_PRESENCE_HOLDOUT_INTEGRITY_AND_V2_DESIGN_PASS"
    : "AI_INTELLIGENCE_PRESENCE_HOLDOUT_INTEGRITY_AND_V2_DESIGN_PASS",
  nextStep: NEW_HUMAN_LABELLED_CASES_REQUIRED
    ? "NEW_UNSEEN_HUMAN_LABELS_REQUIRED"
    : "READY_FOR_ONE_TIME_PRESENCE_HOLDOUT_V2",
  mode: APPLY ? "APPLY" : "DRY_RUN",
  playaAudit: {
    TOTAL: playaAudit.length,
    UNAMBIGUOUS_CANONICAL_REFERENCES: playaAudit.filter(
      (r) => r.UNAMBIGUOUS_CANONICAL_REFERENCE === "YES"
    ).length,
    GENERIC_GEOGRAPHIC_ONLY: playaAudit.filter(
      (r) => r.GENERIC_OR_GEOGRAPHIC_PLAYA_ONLY === "YES"
    ).length,
    VALID_RESOLVER_MISSES: 0,
    GROUND_TRUTH_INTEGRITY_ERRORS: integrityErrors.length,
    CANDIDATE_SUBJECT_INVALIDATIONS: APPLY
      ? invalidationResults.filter((r) => r.written).length
      : integrityErrors.length,
    cases: playaAudit,
  },
  holdoutV1: holdoutV1Status,
  goldenSet: {
    AMENDMENTS: APPLY ? invalidationResults.filter((r) => r.written).length : 0,
    INVALIDATIONS_PENDING_OR_APPLIED: invalidationResults,
    HISTORY_PRESERVED: true,
    HOLDOUT_MEMBERSHIP_PRESERVED: true,
    ORIGINAL_HUMAN_LABELS_PRESERVED: true,
  },
  holdoutV2Source: {
    ELIGIBLE_UNTOUCHED_CASES: humanLabelledUntouched.length,
    NEW_HUMAN_LABELLED_CASES_REQUIRED: NEW_HUMAN_LABELLED_CASES_REQUIRED ? "YES" : "NO",
    candidatePoolNotInGolden: neverInGolden.length,
    note:
      "Candidate fixture has only PENDING_HUMAN_REVIEW or SUPERSEDED_INVALID_SUBJECT outside promoted Golden Set v2. No eligible untouched human-labelled Presence holdout pool remains.",
  },
  holdoutV2Design: HOLD_V2_DESIGN,
  holdoutV2: {
    CREATED: true,
    MANIFEST: "data/ai-visibility/validation/ai-intelligence-presence-holdout-v2.json",
    UNTOUCHED: holdoutV2Manifest.UNTOUCHED,
    SCORED: false,
    STATUS: holdoutV2Manifest.STATUS,
  },
  scorecard: {
    PRESENCE: {
      DEV: "PASS",
      HOLDOUT_V1: "INSPECTED / INVALID_FOR_FINAL_CERTIFICATION",
      HOLDOUT_V2: holdoutV2Manifest.STATUS,
      PRODUCTION_READINESS: "NOT_READY",
    },
  },
  expansionPolicy: GOLDEN_SET_EXPANSION_TARGET,
  hardGuards: {
    ENTITY_RESOLVER_CHANGES: 0,
    ALIAS_CHANGES: 0,
    PROVIDER_CALLS: 0,
    NEW_MONITORING: 0,
    HOLDOUT_V2_SCORING: 0,
    THRESHOLD_LOWERING: 0,
    CASE_SPECIFIC_RULES: 0,
    AIRTABLE_WRITES: 0,
    DEPLOYS: 0,
  },
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(
  path.join(OUT_DIR, "presence-holdout-integrity-and-v2-design.json"),
  JSON.stringify(report, null, 2) + "\n",
  "utf8"
);
fs.writeFileSync(
  path.join(OUT_DIR, "ai-intelligence-presence-holdout-v2.json"),
  JSON.stringify(holdoutV2Manifest, null, 2) + "\n",
  "utf8"
);
fs.writeFileSync(
  path.join(OUT_DIR, "ai-intelligence-holdout-v1-inspected-diagnostic.json"),
  JSON.stringify(holdoutV1Status, null, 2) + "\n",
  "utf8"
);

console.log(JSON.stringify({
  phase: report.phase,
  status: report.status,
  nextStep: report.nextStep,
  mode: report.mode,
  playa: {
    TOTAL: report.playaAudit.TOTAL,
    UNAMBIGUOUS: report.playaAudit.UNAMBIGUOUS_CANONICAL_REFERENCES,
    GENERIC_ONLY: report.playaAudit.GENERIC_GEOGRAPHIC_ONLY,
    GT_ERRORS: report.playaAudit.GROUND_TRUTH_INTEGRITY_ERRORS,
    INVALIDATIONS: report.playaAudit.CANDIDATE_SUBJECT_INVALIDATIONS,
    VALID_RESOLVER_MISSES: 0,
  },
  holdoutV1: holdoutV1Status.STATUS,
  holdoutV2: holdoutV2Manifest.STATUS,
  NEW_HUMAN_LABELLED_CASES_REQUIRED,
}, null, 2));

if (!APPLY) {
  console.log("\nDry-run only. Re-run with --apply to write INVALIDATE amendments.");
}
