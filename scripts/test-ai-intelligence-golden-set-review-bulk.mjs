#!/usr/bin/env node
/**
 * Golden Set Review — bulk export + import tests.
 * No provider calls. No auto-labels. No auto-promotion. No classifier changes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";
import {
  exportAllReviewCandidates,
  exportCandidatesToCsv,
  exportReviewPacketsJson,
  buildAssistanceReturnTemplate,
  previewHumanReviewImport,
  applyHumanReviewImport,
  acceptAssistedProposals,
  loadAssistedProposal,
  EXPORT_MODE,
  ASSISTANCE_TEMPLATE_VERSION,
} from "../lib/ai-visibility/validation/golden-set-review-bulk-export-import.js";
import {
  loadCandidateDocument,
  getReviewProgress,
  listAllReviewRecords,
  REVIEW_STATUS,
  isPromotableReview,
} from "../lib/ai-visibility/validation/golden-set-human-review.js";
import { systemSuggestionAsLabels } from "../lib/ai-visibility/validation/golden-set-review-packet.js";

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

console.log("Golden Set Bulk Review Export/Import\n");

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gsr-bulk-"));
const opts = { outDir: tmp };

const queueDoc = loadCandidateDocument();
const queueTotal = (queueDoc.cases || []).filter(
  (c) =>
    c.reviewStatus !== "SUPERSEDED_INVALID_SUBJECT" &&
    c.candidateEntity &&
    c.canonicalEntityId
).length;

const exp = exportAllReviewCandidates({ mode: EXPORT_MODE.ALL, ...opts });

test("EXPORT_ALL_RETURNS_ALL_CANDIDATES / COUNT_MATCHES_QUEUE", () => {
  assert.equal(exp.totalCandidates, queueTotal);
  assert.equal(exp.queueTotal, queueTotal);
  assert.ok(exp.totalCandidates > 0);
  assert.equal(exp.cases.length, queueTotal);
  assert.equal(exp.invalidSubjectCaseCount, 0);
});

test("EXPORT_ALL_INCLUDES_UNREVIEWED", () => {
  assert.ok(exp.unreviewedCount >= 0);
  assert.equal(exp.reviewedCount + exp.unreviewedCount, exp.totalCandidates);
  const unreviewed = exp.cases.filter((c) => c.reviewStatus === "UNREVIEWED");
  assert.ok(unreviewed.length > 0 || exp.reviewedCount === exp.totalCandidates);
});

test("EXPORT_ALL_INCLUDES_SYSTEM_SUGGESTION / ENUMS / PROMPT / RESPONSE", () => {
  const c = exp.cases[0];
  assert.ok(c.caseId);
  assert.ok(c.systemSuggestion);
  assert.ok(c.systemSuggestion.recommendationStatus || c.systemSuggestion.note);
  assert.ok(Array.isArray(c.allowedHumanLabels.recommendationStatus));
  assert.ok(c.allowedHumanLabels.recommendationStatus.includes("first_recommendation"));
  assert.ok(c.promptText);
  assert.ok(c.storedResponse);
  assert.ok(Array.isArray(exp.reviewInstructions));
  assert.equal(exp.AUTO_LABEL, false);
  assert.equal(exp.AUTO_APPROVAL, false);
});

test("EXPORT_ALL_COVERAGE_MATCHES_QUEUE", () => {
  const cov = exp.coverage;
  let openai = 0;
  let gemini = 0;
  let perplexity = 0;
  let claude = 0;
  let en = 0;
  let es = 0;
  const geos = { GLOBAL: 0, CALA: 0, MEXICO: 0, EUROPE: 0, NORTH_AMERICA: 0 };
  const active = (queueDoc.cases || []).filter(
    (c) =>
      c.reviewStatus !== "SUPERSEDED_INVALID_SUBJECT" &&
      c.candidateEntity &&
      c.canonicalEntityId
  );
  for (const c of active) {
    const p = String(c.provider || "").toLowerCase();
    if (p === "openai") openai += 1;
    if (p === "gemini") gemini += 1;
    if (p === "perplexity") perplexity += 1;
    if (p === "claude") claude += 1;
    if (String(c.language).toLowerCase() === "en") en += 1;
    if (String(c.language).toLowerCase() === "es") es += 1;
    const g = String(c.geography || "").toUpperCase();
    if (geos[g] != null) geos[g] += 1;
  }
  assert.equal(cov.OPENAI, openai);
  assert.equal(cov.GEMINI, gemini);
  assert.equal(cov.PERPLEXITY, perplexity);
  assert.equal(cov.CLAUDE, claude);
  assert.equal(cov.ENGLISH, en);
  assert.equal(cov.SPANISH, es);
  assert.equal(cov.GLOBAL, geos.GLOBAL);
  assert.equal(cov.CALA, geos.CALA);
  assert.equal(cov.MEXICO, geos.MEXICO);
  assert.equal(cov.EUROPE, geos.EUROPE);
  assert.equal(cov.NORTH_AMERICA, geos.NORTH_AMERICA);
  assert.equal(exp.cases[0].caseId, active[0].caseId);
  assert.equal(exp.cases[exp.cases.length - 1].caseId, active[active.length - 1].caseId);
});

test("CSV_EXPORT_VALID / JSON_EXPORT_VALID", () => {
  const csv = exportCandidatesToCsv(exp);
  assert.ok(csv.includes("caseId,provider,model"));
  assert.ok(csv.split("\n").length > queueTotal);
  assert.equal(exp.exportVersion.includes("bulk_export"), true);
});

test("CHATGPT_REVIEW_EXPORT / ASSISTANCE_TEMPLATE_VALID", () => {
  const packets = exportReviewPacketsJson({ mode: EXPORT_MODE.ALL, ...opts });
  assert.equal(packets.totalCandidates, queueTotal);
  assert.ok(packets.cases[0].prompt);
  assert.ok(packets.cases[0].storedResponse);
  assert.ok(packets.disclosure.includes("not automatically treated as ground truth"));
  const tpl = buildAssistanceReturnTemplate({ mode: EXPORT_MODE.NEXT_10, ...opts });
  assert.equal(tpl.reviewVersion, ASSISTANCE_TEMPLATE_VERSION);
  assert.ok(tpl.cases.length <= 10);
  assert.ok(tpl.cases[0].recommendedHumanReview);
  assert.ok(!("groundTruth" in tpl.cases[0]));
});

test("EXPORT_NEXT_10_AND_FILTERED", () => {
  const n10 = exportAllReviewCandidates({ mode: EXPORT_MODE.NEXT_10, ...opts });
  assert.ok(n10.totalCandidates <= 10);
  const filt = exportAllReviewCandidates({
    mode: EXPORT_MODE.FILTERED_CURRENT_VIEW,
    filters: { provider: "openai" },
    ...opts,
  });
  assert.ok(filt.cases.every((c) => c.provider === "openai"));
});

const sample =
  queueDoc.cases.find(
    (c) =>
      c.caseId === "cand_238deb1e" &&
      c.reviewStatus !== "SUPERSEDED_INVALID_SUBJECT" &&
      c.candidateEntity
  ) ||
  queueDoc.cases.find(
    (c) => c.reviewStatus !== "SUPERSEDED_INVALID_SUBJECT" && c.candidateEntity && c.canonicalEntityId
  );
const sys = systemSuggestionAsLabels(sample);

test("IMPORT_PREVIEW_NO_WRITE", () => {
  const humanDoc = {
    reviewVersion: "ai_intelligence_golden_set_human_import_v1",
    candidateVersion: queueDoc.version,
    cases: [
      {
        caseId: sample.caseId,
        reviewStatus: "CONFIRMED",
        humanReview: {
          entityPresent: "YES",
          canonicalEntityId: sample.canonicalEntityId,
          canonicalEntityName: sample.candidateEntity,
          recommendationStatus: sys.recommendationStatus,
          firstRecommendation: sys.firstRecommendation === true ? "YES" : "NO",
          questionStatus: sys.questionStatus,
          citationAssociation: sys.citationAssociation || "UNKNOWN",
          parentBrandNote: "",
          notes: "preview only",
        },
        reviewReason: "preview only",
      },
    ],
  };
  const preview = previewHumanReviewImport(humanDoc, opts);
  assert.equal(preview.preview, true);
  assert.equal(preview.wrote, false);
  assert.equal(preview.AUTO_APPLY, false);
  assert.equal(listAllReviewRecords(opts).length, 0);
});

test("IMPORT_INVALID_CASE_REJECTED / ENUM / DUPLICATE / VERSION", () => {
  const badCase = previewHumanReviewImport(
    {
      candidateVersion: queueDoc.version,
      cases: [{ caseId: "cand_DOES_NOT_EXIST", reviewStatus: "DEFERRED" }],
    },
    opts
  );
  assert.equal(badCase.canApply, false);
  assert.ok(badCase.INVALID_CASE_IDS.includes("cand_DOES_NOT_EXIST"));

  const badEnum = previewHumanReviewImport(
    {
      candidateVersion: queueDoc.version,
      cases: [
        {
          caseId: sample.caseId,
          reviewStatus: "CORRECTED",
          humanReview: {
            entityPresent: "YES",
            canonicalEntityId: sample.canonicalEntityId,
            canonicalEntityName: sample.candidateEntity,
            recommendationStatus: "NOT_A_REAL_STATUS",
            firstRecommendation: "YES",
            questionStatus: "DISCUSSION_ONLY",
            citationAssociation: "UNKNOWN",
          },
        },
      ],
    },
    opts
  );
  assert.equal(badEnum.canApply, false);
  assert.ok(badEnum.INVALID_ENUMS.length > 0);

  const dup = previewHumanReviewImport(
    {
      candidateVersion: queueDoc.version,
      cases: [
        { caseId: sample.caseId, reviewStatus: "DEFERRED" },
        { caseId: sample.caseId, reviewStatus: "DEFERRED" },
      ],
    },
    opts
  );
  assert.equal(dup.canApply, false);
  assert.ok(dup.DUPLICATES.includes(sample.caseId));

  const ver = previewHumanReviewImport(
    {
      candidateVersion: "wrong_version",
      cases: [{ caseId: sample.caseId, reviewStatus: "DEFERRED" }],
    },
    opts
  );
  assert.equal(ver.canApply, false);
  assert.ok(ver.VERSION_MISMATCHES.length > 0);
});

test("IMPORT_REQUIRES_EXPLICIT_APPLY / AUTHORIZED_HUMAN", () => {
  let threw = false;
  try {
    applyHumanReviewImport(
      {
        candidateVersion: queueDoc.version,
        cases: [{ caseId: sample.caseId, reviewStatus: "DEFERRED" }],
      },
      { ...opts, apply: false, reviewer: "tester" }
    );
  } catch (err) {
    threw = err.code === "EXPLICIT_APPLY_REQUIRED";
  }
  assert.equal(threw, true);

  threw = false;
  try {
    applyHumanReviewImport(
      {
        candidateVersion: queueDoc.version,
        cases: [{ caseId: sample.caseId, reviewStatus: "DEFERRED" }],
      },
      { ...opts, apply: true, reviewer: "" }
    );
  } catch (err) {
    threw = err.code === "AUTHORIZED_HUMAN_REQUIRED";
  }
  assert.equal(threw, true);
});

test("IMPORT_CONFIRM_MATCH_RECOMPUTED / CORRECT_DIFF / DEFERRED", () => {
  const matchDoc = {
    candidateVersion: queueDoc.version,
    cases: [
      {
        caseId: sample.caseId,
        reviewStatus: "CONFIRMED",
        humanReview: {
          entityPresent: "YES",
          canonicalEntityId: sample.canonicalEntityId,
          canonicalEntityName: sample.candidateEntity,
          recommendationStatus: sys.recommendationStatus,
          firstRecommendation: sys.firstRecommendation === true ? "YES" : "NO",
          questionStatus: sys.questionStatus,
          citationAssociation: sys.citationAssociation || "UNKNOWN",
        },
        reviewReason: "matches system",
      },
    ],
  };
  const prevMatch = previewHumanReviewImport(matchDoc, opts);
  assert.equal(prevMatch.CHANGES_TO_APPLY[0].finalComputedReviewStatus, "CONFIRMED");

  const other = queueDoc.cases.find((c) => c.caseId !== sample.caseId) || sample;
  const otherSys = systemSuggestionAsLabels(other);
  const corrDoc = {
    candidateVersion: queueDoc.version,
    cases: [
      {
        caseId: other.caseId,
        reviewStatus: "CONFIRMED",
        humanReview: {
          entityPresent: "YES",
          canonicalEntityId: other.canonicalEntityId,
          canonicalEntityName: other.candidateEntity,
          recommendationStatus: "first_recommendation",
          firstRecommendation: "YES",
          questionStatus: "FIRST_RECOMMENDED",
          citationAssociation: otherSys.citationAssociation || "UNKNOWN",
        },
        reviewReason: "ranked first explicitly",
      },
    ],
  };
  const prevCorr = previewHumanReviewImport(corrDoc, opts);
  assert.equal(prevCorr.CHANGES_TO_APPLY[0].finalComputedReviewStatus, "CORRECTED");

  const defDoc = {
    candidateVersion: queueDoc.version,
    cases: [{ caseId: sample.caseId, reviewStatus: "DEFERRED", reviewReason: "need more context" }],
  };
  const prevDef = previewHumanReviewImport(defDoc, opts);
  assert.equal(prevDef.DEFERRED, 1);
  assert.equal(prevDef.CHANGES_TO_APPLY[0].finalComputedReviewStatus, "DEFERRED");
});

test("ASSISTED_PROPOSAL_NOT_GROUND_TRUTH / NOT_GOLDEN_SET", () => {
  const assistedDoc = {
    reviewVersion: ASSISTANCE_TEMPLATE_VERSION,
    candidateVersion: queueDoc.version,
    cases: [
      {
        caseId: sample.caseId,
        recommendedHumanReview: {
          entityPresent: "YES",
          canonicalEntityId: sample.canonicalEntityId,
          canonicalEntityName: sample.candidateEntity,
          recommendationStatus: "first_recommendation",
          firstRecommendation: "YES",
          questionStatus: "FIRST_RECOMMENDED",
          citationAssociation: "UNKNOWN",
          parentBrandNote: "",
          reason: "strong options ranked #1",
        },
      },
    ],
  };
  const preview = previewHumanReviewImport(assistedDoc, opts);
  assert.equal(preview.kind, "ASSISTED");
  assert.equal(preview.ASSISTED_PROPOSAL_GROUND_TRUTH, false);
  const applied = applyHumanReviewImport(assistedDoc, {
    ...opts,
    apply: true,
    reviewer: "joan@dealality.test",
  });
  assert.equal(applied.kind, "ASSISTED");
  assert.equal(applied.ASSISTED_PROPOSAL_GROUND_TRUTH, false);
  const prop = loadAssistedProposal(sample.caseId, opts);
  assert.ok(prop);
  assert.equal(prop.groundTruth, false);
  assert.equal(getReviewProgress(opts).REVIEWED, 0);
  assert.equal(listAllReviewRecords(opts).filter(isPromotableReview).length, 0);
});

test("ACCEPT_ASSISTED_REQUIRES_EXPLICIT_APPLY_THEN_HUMAN_FINAL", () => {
  const accepted = acceptAssistedProposals([sample.caseId], {
    ...opts,
    apply: true,
    reviewer: "joan@dealality.test",
  });
  assert.equal(accepted.applied, true);
  const progress = getReviewProgress(opts);
  assert.ok(progress.REVIEWED >= 1);
  assert.ok(progress.PROMOTABLE >= 1);
  const records = listAllReviewRecords(opts);
  const rec = records.find((r) => r.caseId === sample.caseId);
  assert.ok(rec);
  assert.ok([REVIEW_STATUS.CONFIRMED, REVIEW_STATUS.CORRECTED].includes(rec.reviewStatus));
  assert.ok(rec.systemSuggestion);
  assert.equal(rec.autoApproved, false);
  assert.equal(rec.llmLabelledAsGroundTruth, false);
  assert.ok(rec.importBatchId);
  assert.ok(rec.externalAssistanceUsed === true);
});

test("SYSTEM_SUGGESTION_PRESERVED / IMPORT_AUDIT_TRAIL", () => {
  const rec = listAllReviewRecords(opts).find((r) => r.caseId === sample.caseId);
  assert.deepEqual(rec.systemSuggestion, sample.systemSuggestion);
  assert.ok(rec.reviewReason || rec.notes);
  const importsDir = path.join(tmp, "human-review", "imports");
  assert.ok(fs.existsSync(importsDir));
  const audits = fs.readdirSync(importsDir).filter((f) => f.startsWith("imp_"));
  assert.ok(audits.length >= 1);
});

test("NO_AUTO_LABEL / NO_AUTO_PROMOTION / NO_CLASSIFIER_CHANGE", () => {
  assert.equal(exp.AUTO_LABEL, false);
  assert.equal(exp.AUTO_PROMOTION, false);
  const ui = fs.readFileSync(
    path.join(ROOT, "public/js/ai-visibility/ai-intelligence-golden-set-review.js"),
    "utf8"
  );
  assert.ok(ui.includes("Export All Review Candidates") || ui.includes("gsrExportAll"));
  assert.ok(ui.includes("import/preview"));
  assert.ok(ui.includes("apply: true"));
  assert.ok(!ui.includes("autoSubmit"));
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  assert.ok(server.includes("/export/all"));
  assert.ok(server.includes("/import/preview"));
  assert.ok(server.includes("/import/apply"));
});

test("ROUTES_REGISTERED_bulk_export_import", () => {
  const api = fs.readFileSync(
    path.join(ROOT, "api/ai-intelligence-golden-set-review.js"),
    "utf8"
  );
  assert.ok(api.includes("getGoldenSetReviewExportAll"));
  assert.ok(api.includes("postGoldenSetReviewImportPreview"));
  assert.ok(api.includes("postGoldenSetReviewImportApply"));
  const bulk = fs.readFileSync(
    path.join(ROOT, "lib/ai-visibility/validation/golden-set-review-bulk-export-import.js"),
    "utf8"
  );
  assert.ok(bulk.includes("recommendedHumanReview"));
  assert.ok(bulk.includes("ASSISTED_PROPOSAL_GROUND_TRUTH"));
});

console.log(`\n${passed} passed, ${failed} failed`);
if (failed) process.exit(1);

// Write coverage snapshot for founder report (tmp only)
fs.writeFileSync(
  path.join(tmp, "export-coverage-snapshot.json"),
  JSON.stringify(
    {
      QUEUE_TOTAL: queueTotal,
      EXPORTED_ALL_TOTAL: exp.totalCandidates,
      coverage: exp.coverage,
      firstCase: exp.cases[0]?.caseId,
      lastCase: exp.cases[exp.cases.length - 1]?.caseId,
    },
    null,
    2
  ),
  "utf8"
);
console.log("Coverage snapshot:", JSON.stringify(exp.coverage));
