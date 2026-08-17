#!/usr/bin/env node
/**
 * Active queue / export purity tests.
 * Superseded history preserved; never exported or queued.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getActiveGoldenSetReviewCandidates,
  isActiveReviewCandidate,
  isSupersededCandidate,
  summarizeCandidatePopulation,
  SUPERSEDED_INVALID_SUBJECT,
} from "../lib/ai-visibility/validation/golden-set-active-candidates.js";
import {
  loadCandidateDocument,
  buildReviewQueue,
  getReviewProgress,
  promoteGoldenSetV2,
  submitHumanReview,
  REVIEW_STATUS,
} from "../lib/ai-visibility/validation/golden-set-human-review.js";
import {
  exportAllReviewCandidates,
  exportReviewPacketsJson,
  buildAssistanceReturnTemplate,
  previewHumanReviewImport,
  EXPORT_MODE,
} from "../lib/ai-visibility/validation/golden-set-review-bulk-export-import.js";
import { buildNextReviewPackets, COPY_NEXT_MAX } from "../lib/ai-visibility/validation/golden-set-review-packet.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    const r = fn();
    if (r && typeof r.then === "function") throw new Error("Use sync tests");
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}
async function testAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("Golden Set Active Export Purity\n");

const doc = loadCandidateDocument();
const pop = summarizeCandidatePopulation(doc);
const active = getActiveGoldenSetReviewCandidates(doc);
const superseded = (doc.cases || []).filter(isSupersededCandidate);

test("ACTIVE_REVIEW_RESOLVER_EXCLUDES_SUPERSEDED / NULL_SUBJECT", () => {
  assert.equal(pop.storedCandidateCount, (doc.cases || []).length);
  assert.equal(pop.activeReviewCandidateCount, active.length);
  assert.equal(pop.supersededCandidateCount, superseded.length);
  assert.equal(pop.nullSubjectActive, 0);
  assert.ok(pop.nullSubjectTotal >= superseded.filter((c) => !c.candidateEntity).length);
  assert.ok(active.every(isActiveReviewCandidate));
  assert.ok(active.every((c) => c.reviewStatus !== SUPERSEDED_INVALID_SUBJECT));
  assert.ok(active.every((c) => c.candidateEntity && c.canonicalEntityId));
  assert.equal(active.filter((c) => !c.candidateEntity).length, 0);
});

test("QUEUE_EXCLUDES_SUPERSEDED / NULL_SUBJECT", () => {
  const q = buildReviewQueue({});
  assert.equal(q.cases.length, pop.activeReviewCandidateCount);
  assert.equal(q.activeReviewCandidateCount, pop.activeReviewCandidateCount);
  assert.equal(q.storedCandidateCount, pop.storedCandidateCount);
  assert.equal(q.supersededCandidateCount, pop.supersededCandidateCount);
  assert.equal(q.cases.filter((c) => c.reviewStatus === SUPERSEDED_INVALID_SUBJECT).length, 0);
  assert.equal(q.cases.filter((c) => !c.candidateEntity || !c.canonicalEntityId).length, 0);
});

test("EXPORT_ALL_MEANS_ALL_ACTIVE / EXCLUDES_SUPERSEDED / NULL", () => {
  const exp = exportAllReviewCandidates({ mode: EXPORT_MODE.ALL });
  assert.equal(exp.storedCandidateCount, pop.storedCandidateCount);
  assert.equal(exp.activeReviewCandidateCount, pop.activeReviewCandidateCount);
  assert.equal(exp.supersededCandidateCount, pop.supersededCandidateCount);
  assert.equal(exp.exportedCandidateCount, pop.activeReviewCandidateCount);
  assert.equal(exp.totalCandidates, pop.activeReviewCandidateCount);
  assert.equal(exp.queueTotal, pop.activeReviewCandidateCount);
  assert.equal(exp.invalidSubjectCaseCount, 0);
  assert.equal(
    exp.cases.filter((c) => !c.subjectEntityName || !c.canonicalEntityId || !c.canonicalEntityName)
      .length,
    0
  );
  assert.equal(exp.cases.filter((c) => c.reviewStatus === SUPERSEDED_INVALID_SUBJECT).length, 0);
  assert.notEqual(exp.storedCandidateCount, exp.exportedCandidateCount);
});

test("EXPORT_COUNT_MATCHES_ACTIVE_QUEUE", () => {
  const q = buildReviewQueue({});
  const exp = exportAllReviewCandidates({ mode: EXPORT_MODE.ALL });
  assert.equal(exp.exportedCandidateCount, q.cases.length);
  assert.equal(exp.queueTotal, q.progress.TOTAL);
});

test("FILTERED_EXPORT / PACKET_EXPORT / ASSISTANCE_TEMPLATE active-only", () => {
  const filt = exportAllReviewCandidates({
    mode: EXPORT_MODE.FILTERED_CURRENT_VIEW,
    filters: { provider: "openai" },
  });
  assert.ok(filt.cases.every((c) => c.provider === "openai"));
  assert.ok(filt.cases.every((c) => c.subjectEntityName && c.canonicalEntityId));
  const packets = exportReviewPacketsJson({ mode: EXPORT_MODE.ALL });
  assert.equal(packets.totalCandidates, pop.activeReviewCandidateCount);
  const tpl = buildAssistanceReturnTemplate({ mode: EXPORT_MODE.ALL });
  assert.ok(tpl.cases.every((c) => active.some((a) => a.caseId === c.caseId)));
});

await testAsync("COPY_REVIEW / BATCH_COPY_EXCLUDES_SUPERSEDED", async () => {
  const q = buildReviewQueue({ filters: { reviewStatus: "UNREVIEWED" } });
  const ids = q.cases.slice(0, COPY_NEXT_MAX).map((c) => c.caseId);
  assert.ok(ids.every((id) => active.some((a) => a.caseId === id)));
  const batch = await buildNextReviewPackets(ids, {});
  assert.ok(batch.count <= 5);
  for (const id of ids) {
    assert.ok(batch.combinedText.includes(id));
  }
  assert.ok(!batch.combinedText.includes(superseded[0]?.caseId || "___none___") || !superseded.length);
});

test("IMPORT_REJECTS_SUPERSEDED_CASE", () => {
  const badId = superseded[0]?.caseId;
  assert.ok(badId);
  const preview = previewHumanReviewImport(
    {
      candidateVersion: doc.version,
      cases: [{ caseId: badId, reviewStatus: "DEFERRED" }],
    },
    {}
  );
  assert.equal(preview.canApply, false);
  assert.ok((preview.CANDIDATE_NOT_ACTIVE || []).some((x) => x.caseId === badId || x === badId));
});

test("PROMOTION_REJECTS_SUPERSEDED_CASE", () => {
  const promo = promoteGoldenSetV2({ apply: false });
  assert.equal(promo.written, false);
  assert.equal(promo.casesPromotedFromReview, 0);
  // Even if a review existed for superseded IDs, active resolver would exclude them
  const activeIds = new Set(active.map((a) => a.caseId));
  assert.ok(superseded.every((s) => !activeIds.has(s.caseId)));
});

test("REVIEW_PROGRESS_COUNTS_ACTIVE_ONLY / COVERAGE", () => {
  const p = getReviewProgress({});
  assert.equal(p.TOTAL, pop.activeReviewCandidateCount);
  assert.equal(p.activeReviewCandidateCount, pop.activeReviewCandidateCount);
  assert.equal(p.storedCandidateCount, pop.storedCandidateCount);
  assert.equal(p.REMAINING, pop.activeReviewCandidateCount - p.REVIEWED);
  assert.notEqual(p.TOTAL, pop.storedCandidateCount);
});

test("SUPERSEDED_HISTORY_PRESERVED", () => {
  assert.ok(superseded.length > 0);
  assert.ok(superseded.every((s) => s.reviewStatus === SUPERSEDED_INVALID_SUBJECT));
  assert.ok(fs.existsSync(path.join(ROOT, "fixtures/ai-visibility/ai-intelligence-golden-set-v2-candidates.json")));
  const raw = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "fixtures/ai-visibility/ai-intelligence-golden-set-v2-candidates.json"),
      "utf8"
    )
  );
  assert.ok((raw.cases || []).some((c) => c.reviewStatus === SUPERSEDED_INVALID_SUBJECT));
});

test("CONVENIENCE_EXPORT_FILE_IS_ACTIVE_ONLY", () => {
  const p = path.join(
    ROOT,
    "data/ai-visibility/validation/human-review/exports/golden-set-review-candidates-all.json"
  );
  assert.ok(fs.existsSync(p));
  const exp = JSON.parse(fs.readFileSync(p, "utf8"));
  assert.equal(exp.exportedCandidateCount || exp.totalCandidates, pop.activeReviewCandidateCount);
  assert.equal(exp.invalidSubjectCaseCount, 0);
  assert.equal(
    (exp.cases || []).filter((c) => !c.subjectEntityName || !c.canonicalEntityId).length,
    0
  );
});

console.log(`\n${passed} passed, ${failed} failed`);
console.log(JSON.stringify(pop, null, 2));
if (failed) process.exit(1);
