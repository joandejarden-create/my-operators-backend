#!/usr/bin/env node
/**
 * Golden Set Review Packet + learning-loop tests.
 * No provider calls. No auto-labels. No classifier changes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildReviewPacket,
  buildNextReviewPackets,
  COPY_NEXT_MAX,
  diffHumanVsSystem,
  systemSuggestionAsLabels,
  TAXONOMY_HELP,
} from "../lib/ai-visibility/validation/golden-set-review-packet.js";
import {
  buildLearningReport,
  exportReviewedCases,
  IMPROVEMENT_STATUS,
} from "../lib/ai-visibility/validation/golden-set-review-learning.js";
import {
  loadCandidateDocument,
  submitHumanReview,
  REVIEW_STATUS,
} from "../lib/ai-visibility/validation/golden-set-human-review.js";
import os from "os";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    const r = fn();
    if (r && typeof r.then === "function") throw new Error("Use testAsync");
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

console.log("Golden Set Review Assistance\n");

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gsr-assist-"));
const opts = { outDir: tmp };

await testAsync("REVIEW_PACKET_CONTAINS_required_fields", async () => {
  const pkt = await buildReviewPacket("cand_238deb1e", {});
  const t = pkt.packetText;
  assert.ok(t.includes("CASE ID:"));
  assert.ok(t.includes("cand_238deb1e"));
  assert.ok(t.includes("PROMPT:"));
  assert.ok(t.includes("STORED AI RESPONSE:"));
  assert.ok(t.includes("Autograph Collection"));
  assert.ok(t.includes("SYSTEM SUGGESTION — NOT GROUND TRUTH"));
  assert.ok(t.includes("AVAILABLE HUMAN LABEL OPTIONS"));
  assert.ok(t.includes("first_recommendation"));
  assert.ok(t.includes("DISCUSSION_ONLY"));
  assert.ok(t.includes("External review assistance is NOT ground truth"));
  assert.equal(pkt.caseId, "cand_238deb1e");
});

await testAsync("COPY_FIVE_REVIEW_PACKETS_MAX", async () => {
  const doc = loadCandidateDocument();
  const ids = doc.cases.slice(0, 10).map((c) => c.caseId);
  const batch = await buildNextReviewPackets(ids, {});
  assert.ok(batch.count <= COPY_NEXT_MAX);
  assert.equal(COPY_NEXT_MAX, 5);
  assert.ok(batch.combinedText.includes("BATCH INSTRUCTIONS"));
  assert.ok(batch.combinedText.includes("NOT ground truth"));
});

test("CONFIRM_WHEN_LABELS_MATCH / CORRECT_WHEN_LABELS_DIFFER", () => {
  const doc = loadCandidateDocument();
  const c = doc.cases.find((x) => x.caseId === "cand_238deb1e");
  const sys = systemSuggestionAsLabels(c);
  const same = diffHumanVsSystem(sys, sys);
  assert.equal(same.suggestedAction, "CONFIRM");
  assert.equal(same.matches, true);
  const human = {
    ...sys,
    recommendationStatus: "first_recommendation",
    firstRecommendation: true,
    questionStatus: "FIRST_RECOMMENDED",
  };
  const diff = diffHumanVsSystem(human, sys);
  assert.equal(diff.suggestedAction, "CORRECT");
  assert.ok(diff.fieldsChanged.includes("recommendationStatus"));
});

test("SYSTEM_SUGGESTION_PRESERVED_AND_CORRECTION_DIFF", () => {
  const doc = loadCandidateDocument();
  const c = doc.cases[3];
  const result = submitHumanReview(
    {
      caseId: c.caseId,
      reviewStatus: REVIEW_STATUS.CORRECTED,
      humanLabels: {
        entityPresent: true,
        canonicalEntityId: c.canonicalEntityId || "recX",
        canonicalEntityName: c.candidateEntity || "X",
        recommendationStatus: "first_recommendation",
        firstRecommendation: true,
        questionStatus: "FIRST_RECOMMENDED",
        citationAssociation: "ASSOCIATED",
      },
      reviewer: "assist_test@dealality.test",
    },
    opts
  );
  assert.ok(result.record.systemSuggestion);
  assert.ok(result.record.humanLabels);
  assert.ok(Array.isArray(result.record.differences));
  assert.ok(result.record.fieldsChanged.length >= 1);
  assert.equal(result.record.autoApproved, false);
  assert.equal(result.record.externalAssistanceGroundTruth, false);
});

test("CORRECTED_CASE_ENTERS_ERROR_ANALYSIS", () => {
  const report = buildLearningReport({ ...opts, write: true });
  assert.equal(report.AUTO_RULE_CHANGES, false);
  assert.ok(report.learning.CORRECTIONS >= 1);
  assert.ok(Array.isArray(report.patterns));
  for (const cand of report.improvementCandidates) {
    assert.equal(cand.STATUS, IMPROVEMENT_STATUS.REVIEW_REQUIRED);
    assert.equal(cand.DO_NOT_APPLY, true);
  }
});

test("EXPORT_REVIEWED_CASES", () => {
  const exp = exportReviewedCases(opts);
  assert.ok(exp.count >= 1);
  assert.ok(exp.csv.includes("caseId"));
});

test("NO_AUTO_SUBMISSION_IN_UI", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/ai-visibility/ai-intelligence-golden-set-review.js"),
    "utf8"
  );
  assert.ok(js.includes("Copy for Review") || js.includes("gsrCopyPacket"));
  assert.ok(js.includes("No auto-submit") || js.includes("still click"));
  assert.ok(js.includes("Assisted responses are not automatically") || js.includes("NOT ground truth") || js.includes("not auto-applied"));
  assert.ok(!js.includes("autoSubmit(true)"));
});

test("EXTERNAL_ASSISTANCE_NOT_GROUND_TRUTH disclosure", () => {
  const html = fs.readFileSync(
    path.join(ROOT, "public/ai-intelligence-golden-set-review.html"),
    "utf8"
  );
  assert.ok(html.includes("Assisted responses are not automatically treated as ground truth"));
  assert.ok(html.includes("How to Label This Case"));
});

test("TAXONOMY_HELP_PRESENT", () => {
  assert.ok(TAXONOMY_HELP.recommendationStatus.discussed);
  assert.ok(TAXONOMY_HELP.questionStatus.FIRST_RECOMMENDED);
});

test("ROUTES_REGISTERED_for_packets", () => {
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  assert.ok(server.includes("/packet"));
  assert.ok(server.includes("packets/batch"));
  assert.ok(server.includes("golden-set-review/learning"));
  assert.ok(server.includes("golden-set-review/export"));
});

await testAsync("EXAMPLE_PACKET_cand_238deb1e_printed", async () => {
  const pkt = await buildReviewPacket("cand_238deb1e", {});
  assert.ok(pkt.packetText.includes("strong options") || pkt.packetText.includes("Autograph"));
  // Do not assert a human label answer — packet only
  fs.writeFileSync(
    path.join(ROOT, "data/ai-visibility/validation/human-review/example-packet-cand_238deb1e.txt"),
    pkt.packetText,
    "utf8"
  );
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
