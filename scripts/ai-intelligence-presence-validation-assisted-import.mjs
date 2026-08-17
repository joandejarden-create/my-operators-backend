#!/usr/bin/env node
/**
 * Import ChatGPT Presence assisted proposals (ASSISTED_PROPOSAL only).
 * Never writes human ground truth.
 *
 *   node scripts/ai-intelligence-presence-validation-assisted-import.mjs --file path.json
 *   node scripts/ai-intelligence-presence-validation-assisted-import.mjs --file path.json --confirm
 */
import fs from "fs";
import path from "path";
import {
  validateAssistedProposalDocument,
  importAssistedProposals,
  classifyAssistedBulkApproval,
} from "../lib/ai-visibility/validation/presence-validation-assisted-proposals.js";

const args = process.argv.slice(2);
const fileIdx = args.indexOf("--file");
const confirm = args.includes("--confirm");
const filePath =
  (fileIdx >= 0 && args[fileIdx + 1]) ||
  path.resolve("C:/Users/joand/Downloads/presence-validation-chatgpt-proposals-v1.json");

if (!fs.existsSync(filePath)) {
  console.error(JSON.stringify({ ok: false, error: "FILE_NOT_FOUND", filePath }, null, 2));
  process.exit(2);
}

const doc = JSON.parse(fs.readFileSync(filePath, "utf8"));
const validation = validateAssistedProposalDocument(doc);

if (!validation.ok) {
  console.log(
    JSON.stringify(
      {
        phase: "PRESENCE_VALIDATION_ASSISTED_PROPOSALS_IMPORT_STOPPED",
        IMPORT_STATUS: "STOPPED",
        validation,
        HUMAN_FINAL_LABELS_CHANGED: 0,
        AUTO_APPLIED: 0,
      },
      null,
      2
    )
  );
  process.exit(2);
}

if (!confirm) {
  console.log(
    JSON.stringify(
      {
        phase: "PRESENCE_VALIDATION_ASSISTED_PROPOSALS_PREVIEW",
        IMPORT_STATUS: "PREVIEW_ONLY",
        validation,
        note: "Re-run with --confirm to write assisted proposals (still not human GT).",
        HUMAN_FINAL_LABELS_CHANGED: 0,
        AUTO_APPLIED: 0,
      },
      null,
      2
    )
  );
  process.exit(0);
}

const result = importAssistedProposals(doc, { sourceFile: filePath });
const bulk = classifyAssistedBulkApproval({
  caseIds: result.importedCaseIds,
  proposalVersion: doc.proposalVersion,
});

const isOpenAi =
  String(doc.provider || "").toLowerCase() === "openai" ||
  /openai/i.test(String(doc.proposalVersion || ""));

const report = {
  phase: isOpenAi
    ? "OPENAI_PRESENCE_ASSISTED_BULK_APPROVAL_READY"
    : "PRESENCE_VALIDATION_ASSISTED_PROPOSALS_IMPORTED",
  SOURCE_FILE: filePath,
  PROPOSAL_VERSION: doc.proposalVersion,
  TOTAL_PROPOSALS: result.TOTAL_PROPOSALS,
  MATCHED: validation.MATCHED_CASES,
  UNKNOWN: validation.UNKNOWN_CASES.length,
  DUPLICATES: validation.DUPLICATE_CASES.length,
  ALREADY_REVIEWED: validation.ALREADY_REVIEWED_CASES.length,
  PROPOSED_PRESENT: validation.PROPOSED_PRESENT,
  PROPOSED_NOT_PRESENT: validation.PROPOSED_NOT_PRESENT,
  PROPOSED_INVALID: validation.PROPOSED_INVALID,
  PROPOSED_DEFER: validation.PROPOSED_DEFER,
  SYSTEM_DISAGREEMENTS: validation.SYSTEM_DISAGREEMENTS,
  BULK_APPROVAL_ELIGIBLE: bulk.BULK_APPROVAL_ELIGIBLE,
  MANUAL_REVIEW_REQUIRED: bulk.MANUAL_REVIEW_REQUIRED,
  MANUAL_CASE_IDS: bulk.MANUAL_CASE_IDS,
  HUMAN_CONFIRMATION_REQUIRED: "YES",
  AUTO_APPLIED: 0,
  HUMAN_FINAL_LABELS_CHANGED: 0,
  HOLDOUT_V2_FREEZE_ALLOWED: "NO",
  NEXT_ACTION: isOpenAi ? "CONFIRM_OPENAI_BULK_APPROVAL" : "HUMAN_REVIEW_ASSISTED_PROPOSALS",
  IMPORT_STATUS: result.IMPORT_STATUS,
  REVIEW_ROUTE: "/ai-intelligence-presence-validation-review",
  storePath: result.storePath,
  STORE_PROPOSAL_COUNT: result.STORE_PROPOSAL_COUNT,
  status:
    bulk.MANUAL_REVIEW_REQUIRED === 0 && bulk.BULK_APPROVAL_ELIGIBLE === result.TOTAL_PROPOSALS
      ? "OPENAI_PRESENCE_ASSISTED_IMPORT_PASS"
      : "OPENAI_PRESENCE_ASSISTED_IMPORT_REVIEW_REQUIRED",
};

const out = path.resolve(
  isOpenAi
    ? "data/ai-visibility/validation/presence-validation-openai-assisted-import-report.json"
    : "data/ai-visibility/validation/presence-validation-assisted-import-report.json"
);
fs.mkdirSync(path.dirname(out), { recursive: true });
fs.writeFileSync(out, JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(report, null, 2));
console.log(`\nWrote ${out}`);
