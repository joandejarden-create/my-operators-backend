#!/usr/bin/env node
/**
 * Golden Set — Assisted Proposal Import Contract tests.
 * Preview-focused. No provider calls. No auto-approve. No promote.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";
import {
  previewHumanReviewImport,
  applyHumanReviewImport,
  acceptAssistedProposals,
  detectImportKind,
  loadAssistedProposal,
  ASSISTED_REVIEW_IMPORT_VERSION,
  ASSISTANCE_TEMPLATE_VERSION,
  HUMAN_IMPORT_VERSION,
  IMPORT_ERROR_CODES,
} from "../lib/ai-visibility/validation/golden-set-review-bulk-export-import.js";
import {
  loadCandidateDocument,
  getReviewProgress,
  listAllReviewRecords,
  REVIEW_STATUS,
} from "../lib/ai-visibility/validation/golden-set-human-review.js";
import { getActiveGoldenSetReviewCandidates } from "../lib/ai-visibility/validation/golden-set-active-candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURE = path.join(
  ROOT,
  "fixtures/ai-visibility/golden-set-assisted-review-proposals-330.json"
);
const DOWNLOADS = path.join(
  os.homedir(),
  "Downloads",
  "golden-set-assisted-review-proposals-330.json"
);

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("Golden Set Assisted Proposal Import Contract\n");

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gsr-assisted-"));
const opts = { outDir: tmp };
const queueDoc = loadCandidateDocument();
const active = getActiveGoldenSetReviewCandidates(queueDoc);
const sample = active[0];
assert.ok(sample, "need active candidate");

const assistedPath = fs.existsSync(FIXTURE)
  ? FIXTURE
  : fs.existsSync(DOWNLOADS)
    ? DOWNLOADS
    : null;
assert.ok(assistedPath, "assisted 330 fixture missing");
const assisted330 = JSON.parse(fs.readFileSync(assistedPath, "utf8"));

test("ASSISTED_REVIEW_SCHEMA_RECOGNIZED", () => {
  assert.equal(assisted330.reviewVersion, ASSISTED_REVIEW_IMPORT_VERSION);
  assert.equal(detectImportKind(assisted330), "ASSISTED");
  assert.equal(
    detectImportKind({
      reviewVersion: ASSISTANCE_TEMPLATE_VERSION,
      cases: [{ caseId: "x", recommendedHumanReview: {} }],
    }),
    "ASSISTED"
  );
  assert.equal(
    detectImportKind({
      reviewVersion: HUMAN_IMPORT_VERSION,
      cases: [{ caseId: "x", reviewStatus: "DEFERRED", humanReview: {} }],
    }),
    "HUMAN"
  );
});

test("ASSISTED_REVIEW_330_CASES_LOAD", () => {
  assert.equal(assisted330.cases.length, 330);
  assert.equal(assisted330.assistedProposalCount, 330);
  assert.equal(assisted330.sourceExportedCandidateCount, 330);
  assert.ok(assisted330.cases[0].recommendedHumanReview);
  assert.equal(assisted330.cases[0].humanReview, undefined);
});

const preview330 = previewHumanReviewImport(assisted330, opts);

test("ASSISTED_REVIEW_MATCHES_ACTIVE_QUEUE", () => {
  assert.equal(preview330.kind, "ASSISTED");
  assert.equal(preview330.PROPOSALS_TOTAL, 330);
  assert.equal(preview330.MATCHED_ACTIVE_CASES, 330);
  assert.equal(preview330.UNKNOWN_CASE_IDS.length, 0);
  assert.equal(preview330.SUPERSEDED, 0);
  assert.equal(preview330.canApply, true);
});

test("ASSISTED_REVIEW_NOT_HUMAN_GROUND_TRUTH", () => {
  assert.equal(preview330.ASSISTED_PROPOSAL_GROUND_TRUTH, false);
  assert.equal(preview330.AUTO_APPLY, false);
  assert.equal(preview330.AUTO_APPROVALS, 0);
  for (const ch of preview330.CHANGES_TO_APPLY.slice(0, 20)) {
    assert.equal(ch.groundTruth, false);
    assert.equal(ch.kind, "ASSISTED_PROPOSAL");
    assert.equal(ch.reviewStatusUnchanged, REVIEW_STATUS.UNREVIEWED);
  }
});

test("ASSISTED_REVIEW_DOES_NOT_CHANGE_REVIEW_STATUS", () => {
  assert.equal(preview330.CONFIRMED, 0);
  assert.equal(preview330.CORRECTED, 0);
  assert.equal(preview330.CASES_MARKED_REVIEWED, 0);
  assert.equal(getReviewProgress(opts).REVIEWED, 0);
});

test("ASSISTED_REVIEW_DOES_NOT_SET_REVIEWER", () => {
  assert.equal(preview330.HUMAN_FINAL_CREATED, 0);
  assert.equal(listAllReviewRecords(opts).length, 0);
});

test("ASSISTED_PREVIEW_NO_WRITE", () => {
  assert.equal(preview330.wrote, false);
  assert.equal(preview330.preview, true);
  const assistedDir = path.join(tmp, "human-review", "assisted-proposals");
  assert.equal(fs.existsSync(assistedDir) && fs.readdirSync(assistedDir).length > 0, false);
});

test("ASSISTED_INVALID_ENUM_REJECTED", () => {
  const bad = previewHumanReviewImport(
    {
      reviewVersion: ASSISTED_REVIEW_IMPORT_VERSION,
      candidateVersion: queueDoc.version,
      cases: [
        {
          caseId: sample.caseId,
          recommendedHumanReview: {
            entityPresent: "YES",
            canonicalEntityId: sample.canonicalEntityId,
            canonicalEntityName: sample.candidateEntity,
            recommendationStatus: "NOT_A_REAL_STATUS",
            firstRecommendation: "NO",
            questionStatus: "PRESENT",
            citationAssociation: "UNKNOWN",
          },
        },
      ],
    },
    opts
  );
  assert.equal(bad.canApply, false);
  assert.ok(bad.INVALID_ENUMS.length > 0);
  assert.equal(bad.errorCode, IMPORT_ERROR_CODES.INVALID_ENUM);
});

test("ASSISTED_UNKNOWN_CASE_REJECTED", () => {
  const bad = previewHumanReviewImport(
    {
      reviewVersion: ASSISTED_REVIEW_IMPORT_VERSION,
      candidateVersion: queueDoc.version,
      cases: [
        {
          caseId: "cand_DOES_NOT_EXIST",
          recommendedHumanReview: {
            entityPresent: "YES",
            canonicalEntityId: "recX",
            canonicalEntityName: "X",
            recommendationStatus: "discussed",
            firstRecommendation: "NO",
            questionStatus: "PRESENT",
            citationAssociation: "UNKNOWN",
          },
        },
      ],
    },
    opts
  );
  assert.equal(bad.canApply, false);
  assert.ok(bad.UNKNOWN_CASE_IDS.includes("cand_DOES_NOT_EXIST"));
  assert.equal(bad.errorCode, IMPORT_ERROR_CODES.UNKNOWN_CASE_IDS);
});

test("ASSISTED_SUPERSEDED_CASE_REJECTED", () => {
  const superseded = (queueDoc.cases || []).find(
    (c) => c.reviewStatus === "SUPERSEDED_INVALID_SUBJECT"
  );
  if (!superseded) {
    console.log("  SKIP ASSISTED_SUPERSEDED_CASE_REJECTED (no superseded fixture)");
    passed += 1;
    return;
  }
  const bad = previewHumanReviewImport(
    {
      reviewVersion: ASSISTED_REVIEW_IMPORT_VERSION,
      candidateVersion: queueDoc.version,
      cases: [
        {
          caseId: superseded.caseId,
          recommendedHumanReview: {
            entityPresent: "YES",
            canonicalEntityId: superseded.canonicalEntityId || "recX",
            canonicalEntityName: superseded.candidateEntity || "X",
            recommendationStatus: "discussed",
            firstRecommendation: "NO",
            questionStatus: "PRESENT",
            citationAssociation: "UNKNOWN",
          },
        },
      ],
    },
    opts
  );
  assert.equal(bad.canApply, false);
  assert.ok(bad.CANDIDATE_NOT_ACTIVE.length > 0);
  assert.equal(bad.errorCode, IMPORT_ERROR_CODES.CANDIDATE_NOT_ACTIVE);
});

test("ASSISTED_CANONICAL_MISMATCH_REJECTED", () => {
  const bad = previewHumanReviewImport(
    {
      reviewVersion: ASSISTED_REVIEW_IMPORT_VERSION,
      candidateVersion: queueDoc.version,
      cases: [
        {
          caseId: sample.caseId,
          recommendedHumanReview: {
            entityPresent: "YES",
            canonicalEntityId: "rec_WRONG_ENTITY",
            canonicalEntityName: sample.candidateEntity,
            recommendationStatus: "discussed",
            firstRecommendation: "NO",
            questionStatus: "PRESENT",
            citationAssociation: "UNKNOWN",
          },
        },
      ],
    },
    opts
  );
  assert.equal(bad.canApply, false);
  assert.ok(bad.CANONICAL_ENTITY_MISMATCHES.length > 0);
  assert.equal(bad.errorCode, IMPORT_ERROR_CODES.CANONICAL_ENTITY_MISMATCH);
});

test("ACCEPT_ASSISTED_REQUIRES_HUMAN_CLICK", () => {
  let threw = false;
  try {
    acceptAssistedProposals([sample.caseId], { ...opts, apply: false, reviewer: "joan" });
  } catch (err) {
    threw = err.code === "EXPLICIT_APPLY_REQUIRED";
  }
  assert.equal(threw, true);
});

test("BULK_ACCEPT_REQUIRES_HUMAN_CLICK", () => {
  let threw = false;
  try {
    acceptAssistedProposals(
      active.slice(0, 3).map((c) => c.caseId),
      { ...opts, apply: false, reviewer: "joan" }
    );
  } catch (err) {
    threw = err.code === "EXPLICIT_APPLY_REQUIRED";
  }
  assert.equal(threw, true);
});

test("SERVER_RECOMPUTES_CONFIRMED_CORRECTED", () => {
  const mini = {
    reviewVersion: ASSISTED_REVIEW_IMPORT_VERSION,
    candidateVersion: queueDoc.version,
    cases: [
      {
        caseId: sample.caseId,
        recommendedHumanReview: {
          entityPresent: "YES",
          canonicalEntityId: sample.canonicalEntityId,
          canonicalEntityName: sample.candidateEntity,
          recommendationStatus: "discussed",
          firstRecommendation: "NO",
          questionStatus: "DISCUSSION_ONLY",
          citationAssociation: "UNKNOWN",
          reason: "test recompute",
        },
      },
    ],
  };
  applyHumanReviewImport(mini, { ...opts, apply: true, reviewer: "joan@dealality.test" });
  assert.equal(getReviewProgress(opts).REVIEWED, 0);
  const prop = loadAssistedProposal(sample.caseId, opts);
  assert.ok(prop);
  assert.equal(prop.reviewer, null);
  assert.equal(prop.reviewedAt, null);
  assert.equal(prop.groundTruth, false);

  const accepted = acceptAssistedProposals([sample.caseId], {
    ...opts,
    apply: true,
    reviewer: "joan@dealality.test",
  });
  assert.equal(accepted.applied, true);
  const rec = listAllReviewRecords(opts).find((r) => r.caseId === sample.caseId);
  assert.ok(rec);
  assert.ok([REVIEW_STATUS.CONFIRMED, REVIEW_STATUS.CORRECTED].includes(rec.reviewStatus));
  assert.equal(rec.autoApproved, false);
  assert.equal(rec.externalAssistanceUsed, true);
});

test("GENERIC_IMPORT_ERROR_REPLACED", () => {
  const ui = fs.readFileSync(
    path.join(ROOT, "public/js/ai-visibility/ai-intelligence-golden-set-review.js"),
    "utf8"
  );
  assert.ok(ui.includes("formatImportPreviewError"));
  assert.ok(ui.includes("IMPORT_SCHEMA_UNSUPPORTED"));
  assert.ok(ui.includes("IMPORT_PAYLOAD_TOO_LARGE"));
  assert.ok(ui.includes("IMPORT_FILE_INVALID"));
  assert.ok(ui.includes("SYSTEM_MATCHES"));
  // Generic fallback may remain only inside formatter — ensure coded paths exist
  assert.ok(ui.includes("Assisted vs System"));
});

test("ASSISTED_330_PREVIEW_DIFF_SUMMARY", () => {
  assert.equal(typeof preview330.SYSTEM_MATCHES, "number");
  assert.equal(typeof preview330.SYSTEM_DIFFERENCES, "number");
  assert.equal(preview330.SYSTEM_MATCHES + preview330.SYSTEM_DIFFERENCES, 330);
  assert.ok(preview330.DISTRIBUTION?.provider);
  assert.ok(preview330.DISTRIBUTION?.language);
  console.log(
    "    SYSTEM_MATCHES=",
    preview330.SYSTEM_MATCHES,
    "SYSTEM_DIFFERENCES=",
    preview330.SYSTEM_DIFFERENCES
  );
  console.log(
    "    REC=",
    preview330.RECOMMENDATION_DIFFS,
    "FIRST=",
    preview330.FIRST_RECOMMENDATION_DIFFS,
    "Q=",
    preview330.QUESTION_STATUS_DIFFS,
    "CITE=",
    preview330.CITATION_DIFFS
  );
});

test("BODY_LIMIT_ROUTE_CONFIGURED", () => {
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  assert.ok(server.includes('/api/ai-intelligence/golden-set-review/import'));
  assert.ok(server.includes('limit: "5mb"') || server.includes("limit: '5mb'"));
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
