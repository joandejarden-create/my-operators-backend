#!/usr/bin/env node
/**
 * Golden Set entity-nomination remediation tests.
 * No provider calls. No auto-review. No auto-promotion.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  hasSubjectEntity,
  isReviewableCandidate,
  selectEntitiesForCandidate,
  validateActiveCandidatesForExport,
  auditCandidateSubjects,
  SUPERSEDED_INVALID_SUBJECT,
  MAX_ENTITIES_PER_RESPONSE,
} from "../lib/ai-visibility/validation/golden-set-candidate-entity-remediation.js";
import {
  loadCandidateDocument,
  buildReviewQueue,
  submitHumanReview,
  isPromotableReview,
  REVIEW_STATUS,
} from "../lib/ai-visibility/validation/golden-set-human-review.js";
import {
  exportAllReviewCandidates,
  EXPORT_MODE,
} from "../lib/ai-visibility/validation/golden-set-review-bulk-export-import.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

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

console.log("Golden Set Candidate Entity Nomination\n");

const doc = loadCandidateDocument();
const audit = auditCandidateSubjects(doc);
const active = (doc.cases || []).filter(isReviewableCandidate);
const superseded = (doc.cases || []).filter(
  (c) => c.reviewStatus === SUPERSEDED_INVALID_SUBJECT
);

test("GOLDEN_CASE_REQUIRES_SUBJECT_ENTITY / ALL_ACTIVE_HAVE_CANONICAL", () => {
  assert.ok(active.length > 0);
  for (const c of active) {
    assert.equal(hasSubjectEntity(c), true, c.caseId);
    assert.ok(c.canonicalEntityId);
    assert.ok(c.candidateEntity || c.canonicalEntityName);
    assert.ok(c.systemSuggestion);
  }
  assert.equal(audit.NULL_SUBJECT_CASES, 0);
});

test("NULL_SUBJECT_NOT_REVIEWABLE / NOT_IN_QUEUE", () => {
  const queue = buildReviewQueue({});
  assert.equal(
    queue.cases.filter((c) => !c.candidateEntity || !c.canonicalEntityId).length,
    0
  );
  assert.equal(
    queue.cases.filter((c) => c.reviewStatus === SUPERSEDED_INVALID_SUBJECT).length,
    0
  );
});

test("NULL_SUBJECT_NOT_PROMOTABLE / SUPERSEDED_PRESERVED", () => {
  assert.ok(superseded.length > 0);
  for (const s of superseded.slice(0, 5)) {
    assert.equal(isReviewableCandidate(s), false);
    assert.equal(isPromotableReview({ ...s, reviewStatus: "CONFIRMED", humanLabels: {} }), false);
  }
  const withLineage = superseded.find((s) => (s.supersededBy || []).length);
  assert.ok(withLineage, "expected lineage on superseded null cases");
  assert.ok(withLineage.sourceCandidateId == null || withLineage.caseId);
  const childId = withLineage.supersededBy[0];
  const child = active.find((c) => c.caseId === childId);
  assert.ok(child);
  assert.equal(child.sourceCandidateId, withLineage.caseId);
});

test("ENTITY_NOMINATION_USES_CANONICAL_RESOLVER / NO_LLM / NO_FABRICATION", () => {
  const expansion = fs.readFileSync(
    path.join(ROOT, "lib/ai-visibility/validation/golden-set-expansion.js"),
    "utf8"
  );
  assert.ok(expansion.includes("extractMentions"));
  assert.ok(!expansion.includes('canonicalEntityId: null,\n        canonicalEntityName: null'));
  const rem = fs.readFileSync(
    path.join(ROOT, "lib/ai-visibility/validation/golden-set-candidate-entity-remediation.js"),
    "utf8"
  );
  assert.ok(rem.includes("extractMentions"));
  assert.ok(rem.includes("deterministic_extractMentions_v3"));
  assert.ok(!/openai\.com|anthropic|generateContent|chat\.completions/i.test(rem));
});

test("MULTI_ENTITY_RESPONSE_CREATES_ENTITY_SPECIFIC_CASES / CAP", () => {
  const mentions = [
    { canonicalEntityId: "a", canonicalEntityName: "A", role: "first_recommendation", mentionPosition: 1 },
    { canonicalEntityId: "b", canonicalEntityName: "B", role: "ranked_recommendation", mentionPosition: 2 },
    { canonicalEntityId: "c", canonicalEntityName: "C", role: "discussed", mentionPosition: 3 },
    { canonicalEntityId: "d", canonicalEntityName: "D", role: "discussed", mentionPosition: 4 },
    { canonicalEntityId: "e", canonicalEntityName: "E", role: "passing_mention", mentionPosition: 5 },
    { canonicalEntityId: "f", canonicalEntityName: "F", role: "associated_option", mentionPosition: 6 },
  ];
  const selected = selectEntitiesForCandidate(mentions);
  assert.ok(selected.length <= MAX_ENTITIES_PER_RESPONSE);
  assert.ok(selected.some((m) => m.role === "first_recommendation"));
  const created = active.filter((c) => c.sourceCandidateId);
  assert.ok(created.length > 0);
});

test("SOURCE_CASE_LINEAGE_PRESERVED / OLD_NULL_CASE_SUPERSEDED", () => {
  assert.ok(doc.remediationVersion);
  assert.equal(doc.after?.NULL_SUBJECT_ACTIVE_CASES ?? audit.NULL_SUBJECT_CASES, 0);
  assert.ok(superseded.every((s) => s.reviewStatus === SUPERSEDED_INVALID_SUBJECT));
});

test("EXPORT_ALL_HAS_ZERO_NULL_SUBJECTS", () => {
  const exp = exportAllReviewCandidates({ mode: EXPORT_MODE.ALL, requireExportGate: true });
  assert.equal(exp.invalidSubjectCaseCount, 0);
  assert.equal(exp.totalCandidates, active.length);
  assert.ok(exp.cases.every((c) => c.subjectEntityName && c.canonicalEntityId));
});

test("EXPORT_GATE_VALIDATES", () => {
  const gate = validateActiveCandidatesForExport(doc);
  assert.equal(gate.ok, true);
  assert.equal(gate.NO_NULL_SUBJECTS, true);
});

test("NULL_SUBJECT_SUBMIT_REJECTED", () => {
  const bad = superseded[0];
  let threw = false;
  try {
    submitHumanReview({
      caseId: bad.caseId,
      reviewStatus: REVIEW_STATUS.CONFIRMED,
      humanLabels: {
        entityPresent: true,
        canonicalEntityId: "x",
        canonicalEntityName: "X",
        recommendationStatus: "discussed",
        firstRecommendation: false,
        questionStatus: "DISCUSSION_ONLY",
        citationAssociation: "UNKNOWN",
      },
      reviewer: "tester",
    });
  } catch (err) {
    threw =
      err.code === "CANDIDATE_NOT_ACTIVE" ||
      err.code === "CASE_SUPERSEDED_NOT_REVIEWABLE" ||
      err.code === "INVALID_REVIEW_CASE_MISSING_SUBJECT";
  }
  assert.equal(threw, true);
});

test("NO_AUTO_REVIEW / NO_AUTO_PROMOTION", () => {
  assert.equal(doc.humanLabelled, 0);
  assert.equal(doc.llmLabelledAsGroundTruth, 0);
  assert.ok(active.every((c) => c.humanLabelled === false));
  assert.ok(active.every((c) => c.llmLabelledAsGroundTruth === false));
  const ui = fs.readFileSync(
    path.join(ROOT, "public/js/ai-visibility/ai-intelligence-golden-set-review.js"),
    "utf8"
  );
  assert.ok(ui.includes("INVALID_REVIEW_CASE_MISSING_SUBJECT"));
});

test("SAMPLER_NO_LONGER_EMITS_NULL_PLACEHOLDER", () => {
  const expansion = fs.readFileSync(
    path.join(ROOT, "lib/ai-visibility/validation/golden-set-expansion.js"),
    "utf8"
  );
  assert.ok(expansion.includes("Skip response entirely"));
  assert.ok(expansion.includes("extractMentions"));
});

console.log(`\n${passed} passed, ${failed} failed`);
console.log(
  JSON.stringify(
    {
      ACTIVE: active.length,
      SUPERSEDED: superseded.length,
      NULL_ACTIVE: audit.NULL_SUBJECT_CASES,
    },
    null,
    2
  )
);
if (failed) process.exit(1);
