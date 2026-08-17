#!/usr/bin/env node
/**
 * Residual entity ground-truth audit + amendment regression tests.
 * No holdout. No auto labels. No classifier changes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import {
  assessPlayaBrandReference,
  assessIhgManagedReference,
  amendGoldenSetV2GroundTruth,
  AMENDMENT_ACTIONS,
  GROUND_TRUTH_AMENDMENT_VERSION,
} from "../lib/ai-visibility/validation/golden-set-ground-truth-amendment.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { isBlockedBareParentMention } from "../lib/ai-visibility/index.js";

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

console.log("Residual Entity Ground-Truth Audit\n");

test("GENERIC_PLAYA_NOT_BRAND_PRESENCE", () => {
  const a = assessPlayaBrandReference(
    "Ideal para resorts de playa y Thompson Playa del Carmen; destinos de playa."
  );
  assert.equal(a.UNAMBIGUOUS_BRAND_REFERENCE, "NO");
  assert.equal(a.GROUND_TRUTH_REVIEW_REQUIRED, "YES");
});

test("UNAMBIGUOUS_PLAYA_HOTELS_CAN_BE_PRESENCE", () => {
  const a = assessPlayaBrandReference(
    "Owners shortlist Playa Hotels & Resorts for all-inclusive Mexico beach assets."
  );
  assert.equal(a.UNAMBIGUOUS_BRAND_REFERENCE, "YES");
  assert.equal(a.GROUND_TRUTH_REVIEW_REQUIRED, "NO");
  assert.match(a.EXACT_TEXT, /Playa Hotels/i);
});

test("PARENT_IHG_NOT_SPECIFIC_CHILD_ENTITY", () => {
  const a = assessIhgManagedReference(
    "### 6. IHG (InterContinental Hotels Group)\nIHG operates Kimpton and Six Senses."
  );
  assert.equal(a.SPECIFIC_CANONICAL_REFERENCE, "NO");
  assert.equal(a.PARENT_ONLY_REFERENCE, "YES");
  assert.equal(a.GROUND_TRUTH_REVIEW_REQUIRED, "YES");
  assert.ok(isBlockedBareParentMention("IHG"));

  const b = assessIhgManagedReference("IHG Hotels & Resorts** InterContinental Hotels Group (IHG)");
  assert.equal(b.SPECIFIC_CANONICAL_REFERENCE, "NO");
  assert.equal(b.PARENT_ONLY_REFERENCE, "YES");
});

test("GROUND_TRUTH_AMENDMENT_REQUIRES_HUMAN", () => {
  let threw = false;
  try {
    amendGoldenSetV2GroundTruth({
      caseId: "v2_cand_1674948a",
      action: AMENDMENT_ACTIONS.CORRECT_HUMAN_LABEL,
      amendedLabels: { expectedEntityPresent: false },
    });
  } catch (err) {
    threw = true;
    assert.equal(err.code, "REVIEWER_REQUIRED");
  }
  assert.ok(threw);

  const dry = amendGoldenSetV2GroundTruth({
    caseId: "v2_cand_1674948a",
    action: AMENDMENT_ACTIONS.CORRECT_HUMAN_LABEL,
    reviewer: "test@dealality",
    amendmentReason: "unit-test dry-run only",
    amendedLabels: { expectedEntityPresent: false },
  });
  assert.equal(dry.apply, false);
  assert.equal(dry.written, false);
  assert.equal(dry.AUTO_LABEL_CHANGES, false);
  assert.equal(dry.CLASSIFIER_LOGIC_CHANGE, false);
});

test("GROUND_TRUTH_AMENDMENT_PRESERVES_HISTORY", () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gs-amend-"));
  const fixture = path.join(tmp, "golden-set-v2.json");
  const caseId = "v2_cand_test_amend_1";
  const doc = {
    version: "ai_intelligence_golden_set_v2",
    cases: [
      {
        caseId,
        holdoutSplit: "development",
        expectedEntityPresent: true,
        expectedRecommendationRole: "discussed",
        expectedRecommendationClass: "discussed",
        expectedFirstRecommendation: false,
        expectedQuestionStatus: "DISCUSSION_ONLY",
        expectedCitationAssociation: "UNKNOWN",
        candidateEntity: "Playa Hotels & Resorts",
        canonicalEntityId: "rec3TUHT9Z4AnFp5P",
        reviewStatus: "CORRECTED",
        reviewer: "original@dealality",
        reviewedAt: "2026-08-14T23:00:00.000Z",
        notes: "original",
        humanLabelled: true,
        llmLabelledAsGroundTruth: false,
      },
    ],
  };
  fs.writeFileSync(fixture, JSON.stringify(doc, null, 2), "utf8");
  const histRoot = path.join(tmp, "amendments");
  const result = amendGoldenSetV2GroundTruth(
    {
      caseId,
      action: AMENDMENT_ACTIONS.CORRECT_HUMAN_LABEL,
      reviewer: "joan@dealality",
      amendmentReason: "Generic Spanish playa is not the brand",
      amendedLabels: { expectedEntityPresent: false },
    },
    { apply: true, filePath: fixture, rootDir: histRoot }
  );
  assert.equal(result.written, true);
  assert.equal(result.amendment.version, GROUND_TRUTH_AMENDMENT_VERSION);
  assert.equal(result.amendment.ORIGINAL_HUMAN_LABEL.expectedEntityPresent, true);
  assert.equal(result.amendment.AMENDED_HUMAN_LABEL.expectedEntityPresent, false);
  assert.ok(result.amendment.ORIGINAL_REVIEW_AUDIT.reviewer);

  const updated = JSON.parse(fs.readFileSync(fixture, "utf8"));
  const c = updated.cases[0];
  assert.equal(c.expectedEntityPresent, false);
  assert.equal(c.labelAmendmentHistory.length, 1);
  assert.equal(c.labelAmendmentHistory[0].ORIGINAL_HUMAN_LABEL.expectedEntityPresent, true);
  const histFiles = fs.readdirSync(path.join(histRoot, "ground-truth-amendments", caseId));
  assert.ok(histFiles.some((f) => f.endsWith(".json")));
  assert.ok(histFiles.some((f) => f.includes("prior-case")));
});

test("INVALID_CANDIDATE_NOT_USED_AS_POSITIVE_LABEL", () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gs-inv-"));
  const fixture = path.join(tmp, "golden-set-v2.json");
  const caseId = "v2_cand_test_inv_1";
  fs.writeFileSync(
    fixture,
    JSON.stringify({
      version: "ai_intelligence_golden_set_v2",
      cases: [
        {
          caseId,
          holdoutSplit: "development",
          expectedEntityPresent: true,
          candidateEntity: "IHG Hotels & Resorts (Managed)",
          canonicalEntityId: "rec7IXYQYpKMYsrDl",
          reviewStatus: "CORRECTED",
          reviewer: "original@dealality",
          reviewedAt: "2026-08-14T23:00:00.000Z",
          humanLabelled: true,
        },
      ],
    }),
    "utf8"
  );
  const result = amendGoldenSetV2GroundTruth(
    {
      caseId,
      action: AMENDMENT_ACTIONS.INVALIDATE_CANDIDATE_SUBJECT,
      reviewer: "joan@dealality",
      amendmentReason: "Parent IHG only — not Managed subject",
    },
    { apply: true, filePath: fixture, rootDir: path.join(tmp, "h") }
  );
  assert.equal(result.written, true);
  const c = JSON.parse(fs.readFileSync(fixture, "utf8")).cases[0];
  assert.equal(c.expectedEntityPresent, false);
  assert.equal(c.groundTruthInvalidated, true);
  assert.equal(c.reviewStatus, "INVALIDATED_CANDIDATE_SUBJECT");
});

test("HOLDOUT_NOT_ACCESSED", () => {
  const auditPath = path.join(
    ROOT,
    "data/ai-visibility/validation/human-review/residual-entity-ground-truth/audit-summary.json"
  );
  assert.ok(fs.existsSync(auditPath), "run residual audit before this test suite");
  const summary = JSON.parse(fs.readFileSync(auditPath, "utf8"));
  assert.equal(summary.HOLDOUT_ACCESSED, false);
  assert.equal(summary.HOLDOUT_CASES_INSPECTED, 0);
  assert.equal(summary.HOLDOUT_METRICS_RUN, false);
  assert.equal(summary.AUTO_LABEL_CHANGES, 0);
  assert.equal(summary.CLASSIFIER_CHANGES, 0);
  assert.equal(summary.ALIAS_CHANGES, 0);

  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gs-hold-"));
  const fixture = path.join(tmp, "golden-set-v2.json");
  fs.writeFileSync(
    fixture,
    JSON.stringify({
      version: "ai_intelligence_golden_set_v2",
      cases: [
        {
          caseId: "v2_cand_holdout_x",
          holdoutSplit: "holdout",
          expectedEntityPresent: true,
          humanLabelled: true,
        },
      ],
    }),
    "utf8"
  );
  let blocked = false;
  try {
    amendGoldenSetV2GroundTruth(
      {
        caseId: "v2_cand_holdout_x",
        action: AMENDMENT_ACTIONS.CORRECT_HUMAN_LABEL,
        reviewer: "joan@dealality",
        amendmentReason: "should block",
        amendedLabels: { expectedEntityPresent: false },
      },
      { apply: true, filePath: fixture, rootDir: path.join(tmp, "h") }
    );
  } catch (err) {
    blocked = err.code === "HOLDOUT_CASE_AMENDMENT_BLOCKED";
  }
  assert.ok(blocked);
});

test("SCORING_INDEX_DOES_NOT_MAP_GENERIC_PLAYA", () => {
  const index = buildGoldenSetScoringEntityIndex({});
  const mentions = extractMentions({
    responseId: "t",
    text: "destinos de playa y club de playa; Thompson Playa del Carmen.",
    entityIndex: index.aliasIndex,
  });
  assert.ok(!mentions.some((m) => /Playa Hotels/i.test(m.canonicalEntityName)));
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
