#!/usr/bin/env node
/**
 * Import Holdout v3 ChatGPT assisted Presence proposals (ASSISTANCE ONLY).
 *
 *   node scripts/ai-intelligence-presence-holdout-v3-assisted-import.mjs
 *   node scripts/ai-intelligence-presence-holdout-v3-assisted-import.mjs --file path.json
 *
 * Never writes human ground truth. Never selects/freezes/scores Holdout v3.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  validateAssistedProposalDocument,
  importAssistedProposals,
  classifyAssistedBulkApproval,
  mapSystemSuggestionToDecision,
} from "../lib/ai-visibility/validation/presence-validation-assisted-proposals.js";
import {
  loadPresenceValidationCandidates,
  loadPresenceValidationReviews,
} from "../lib/ai-visibility/validation/presence-validation-candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const EXPECTED_VERSION =
  "presence_validation_holdout_v3_chatgpt_assisted_proposals_v1";
const EXPECTED_BATCH = "presence_validation_holdout_v3_candidate_batch_v1";
const EXPECTED_CASES = 170;
const EXPECTED_UNIQUE_RESPONSES = 86;

const args = process.argv.slice(2);
const fileIdx = args.indexOf("--file");
const filePath =
  (fileIdx >= 0 && args[fileIdx + 1]) ||
  path.resolve(
    "C:/Users/joand/Downloads/presence-validation-holdout-v3-chatgpt-proposals-v1.json"
  );

function stop(reason, extra = {}) {
  const report = {
    phase: "PRESENCE_HOLDOUT_V3_ASSISTED_IMPORT_STOPPED",
    status: "STOPPED",
    stopReason: reason,
    HUMAN_FINAL_LABELS_WRITTEN: 0,
    AUTO_APPLIED: 0,
    HOLDOUT_V3_SELECTED: "NO",
    HOLDOUT_V3_FROZEN: "NO",
    HOLDOUT_V3_SCORED: "NO",
    ...extra,
  };
  console.log(JSON.stringify(report, null, 2));
  const out = path.join(
    ROOT,
    "data/ai-visibility/validation/presence-holdout-v3-assisted-import-report.json"
  );
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(report, null, 2) + "\n");
  process.exit(2);
}

if (!fs.existsSync(filePath)) {
  stop("FILE_NOT_FOUND", { filePath });
}

const doc = JSON.parse(fs.readFileSync(filePath, "utf8"));

// --- Header governance ---
if (doc.proposalVersion !== EXPECTED_VERSION) {
  stop("PROPOSAL_VERSION_MISMATCH", {
    expected: EXPECTED_VERSION,
    actual: doc.proposalVersion,
  });
}
if (Number(doc.sourceCaseCount) !== EXPECTED_CASES) {
  stop("SOURCE_CASE_COUNT_MISMATCH", {
    expected: EXPECTED_CASES,
    actual: doc.sourceCaseCount,
  });
}
if (Number(doc.sourceUniqueResponseCount) !== EXPECTED_UNIQUE_RESPONSES) {
  stop("SOURCE_UNIQUE_RESPONSE_COUNT_MISMATCH", {
    expected: EXPECTED_UNIQUE_RESPONSES,
    actual: doc.sourceUniqueResponseCount,
  });
}
if (doc.assistanceOnly !== true) {
  stop("ASSISTANCE_ONLY_REQUIRED");
}
if (doc.autoHumanLabelingAllowed !== false) {
  stop("AUTO_HUMAN_LABELING_MUST_BE_FALSE");
}
const allowed = new Set(["PRESENT", "NOT_PRESENT", "INVALID", "DEFER"]);
const allowedDoc = Array.isArray(doc.allowedDecisions) ? doc.allowedDecisions : [];
if (
  allowedDoc.length !== 4 ||
  allowedDoc.some((d) => !allowed.has(String(d).toUpperCase()))
) {
  stop("ALLOWED_DECISIONS_MISMATCH", { allowedDecisions: allowedDoc });
}
if (!Array.isArray(doc.proposals) || doc.proposals.length !== EXPECTED_CASES) {
  stop("PROPOSALS_LENGTH_MISMATCH", {
    expected: EXPECTED_CASES,
    actual: doc.proposals?.length,
  });
}

// --- Match only to v3 primary pending ---
const cand = loadPresenceValidationCandidates();
const reviews = loadPresenceValidationReviews();
const byId = new Map((cand?.cases || []).map((c) => [c.caseId, c]));
const matched = [];
const unknown = [];
const wrongBatch = [];
const notPrimary = [];
const alreadyReviewed = [];
const seen = new Set();
const duplicates = [];

for (const row of doc.proposals) {
  const caseId = row.caseId;
  if (seen.has(caseId)) {
    duplicates.push(caseId);
    continue;
  }
  seen.add(caseId);
  const c = byId.get(caseId);
  if (!c) {
    unknown.push(caseId);
    continue;
  }
  if (c.batchId !== EXPECTED_BATCH) {
    wrongBatch.push(caseId);
    continue;
  }
  if (c.primaryReviewQueue !== true) {
    notPrimary.push(caseId);
    continue;
  }
  if (reviews.reviews?.[caseId]) {
    alreadyReviewed.push(caseId);
    continue;
  }
  matched.push(caseId);
}

if (
  unknown.length ||
  wrongBatch.length ||
  notPrimary.length ||
  alreadyReviewed.length ||
  duplicates.length ||
  matched.length !== EXPECTED_CASES
) {
  stop("BATCH_MATCH_FAILED", {
    MATCHED: matched.length,
    UNKNOWN: unknown.length,
    WRONG_BATCH: wrongBatch.length,
    NOT_PRIMARY: notPrimary.length,
    ALREADY_REVIEWED: alreadyReviewed.length,
    DUPLICATES: duplicates.length,
    unknownSample: unknown.slice(0, 10),
    wrongBatchSample: wrongBatch.slice(0, 10),
  });
}

// Generic validator (decisions, duplicates, etc.)
const validation = validateAssistedProposalDocument(doc);
if (!validation.ok) {
  stop(validation.stopReason || "VALIDATION_FAILED", { validation });
}

// Agreement audit (assisted vs system on matched rows)
let agreements = 0;
let disagreements = 0;
const disagreementIds = [];
for (const row of doc.proposals) {
  const c = byId.get(row.caseId);
  const decision = String(row.proposedDecision || "").toUpperCase();
  const sys = mapSystemSuggestionToDecision(c?.SYSTEM_PRESENCE_SUGGESTION);
  if (
    sys &&
    (decision === "PRESENT" || decision === "NOT_PRESENT") &&
    decision !== sys
  ) {
    disagreements += 1;
    disagreementIds.push(row.caseId);
  } else if (sys && (decision === "PRESENT" || decision === "NOT_PRESENT")) {
    agreements += 1;
  }
}

// Import assistance only (merge store; no human GT)
const imported = importAssistedProposals(doc, { sourceFile: filePath });
if (!imported.ok) {
  stop("IMPORT_FAILED", { imported });
}

const bulk = classifyAssistedBulkApproval({
  caseIds: imported.importedCaseIds,
  proposalVersion: EXPECTED_VERSION,
});

const report = {
  phase: "PRESENCE_HOLDOUT_V3_ASSISTED_IMPORT_COMPLETE",
  status: "PRESENCE_HOLDOUT_V3_ASSISTED_BULK_APPROVAL_READY",
  file: {
    PROPOSAL_VERSION: doc.proposalVersion,
    SOURCE_CASE_COUNT: doc.sourceCaseCount,
    SOURCE_UNIQUE_RESPONSE_COUNT: doc.sourceUniqueResponseCount,
    sourceFile: filePath,
  },
  import: {
    TOTAL_PROPOSALS: imported.TOTAL_PROPOSALS,
    MATCHED: matched.length,
    UNKNOWN: unknown.length,
    DUPLICATES: duplicates.length,
    ALREADY_REVIEWED: alreadyReviewed.length,
    PROPOSED_PRESENT: validation.PROPOSED_PRESENT,
    PROPOSED_NOT_PRESENT: validation.PROPOSED_NOT_PRESENT,
    PROPOSED_INVALID: validation.PROPOSED_INVALID,
    PROPOSED_DEFER: validation.PROPOSED_DEFER,
  },
  agreement: {
    SYSTEM_AGREEMENTS: agreements,
    SYSTEM_DISAGREEMENTS: disagreements,
    DISAGREEMENT_CASE_IDS: disagreementIds,
  },
  approval: {
    BULK_APPROVAL_ELIGIBLE: bulk.BULK_APPROVAL_ELIGIBLE,
    MANUAL_REVIEW_REQUIRED: bulk.MANUAL_REVIEW_REQUIRED,
    ELIGIBLE_PRESENT: bulk.ELIGIBLE_PRESENT,
    ELIGIBLE_NOT_PRESENT: bulk.ELIGIBLE_NOT_PRESENT,
    MANUAL_CASE_IDS: bulk.MANUAL_CASE_IDS,
    HUMAN_CONFIRMATION_REQUIRED: "YES",
    AUTO_APPLIED: 0,
    UI_ACTION: "APPROVE ELIGIBLE ASSISTED PROPOSALS",
    REVIEW_ROUTE: "/ai-intelligence-presence-validation-review",
  },
  holdoutV3: {
    SELECTED: "NO",
    FROZEN: "NO",
    SCORED: "NO",
  },
  humanFinals: {
    HUMAN_FINALS_WRITTEN: 0,
  },
  nextAction: "CONFIRM_HOLDOUT_V3_BULK_APPROVAL",
  hardGuards: {
    AUTO_HUMAN_LABELING: 0,
    HUMAN_FINAL_LABELS_WRITTEN: 0,
    HOLDOUT_V3_SELECTION: 0,
    HOLDOUT_V3_FREEZE: 0,
    HOLDOUT_V3_SCORING: 0,
    ENTITY_RESOLVER_CHANGES: 0,
    ALIAS_CHANGES: 0,
    PROVIDER_CALLS: 0,
    REGIONALIZATION_EXECUTION: 0,
    AIRTABLE_WRITES: 0,
    DEPLOYS: 0,
    HOLDOUT_V2_CHANGES: 0,
  },
  storePath: imported.storePath,
};

const out = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-holdout-v3-assisted-import-report.json"
);
fs.writeFileSync(out, JSON.stringify(report, null, 2) + "\n");
console.log("PRESENCE_HOLDOUT_V3_ASSISTED_IMPORT_COMPLETE");
console.log(JSON.stringify(report, null, 2));
console.log(`\nWrote ${path.relative(ROOT, out)}`);
console.log("STOP — human must confirm bulk approval in the review UI.");
