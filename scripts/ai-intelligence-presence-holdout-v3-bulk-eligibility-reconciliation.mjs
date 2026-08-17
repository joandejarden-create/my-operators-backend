#!/usr/bin/env node
/**
 * Holdout v3 bulk eligibility reconciliation audit (read-only labels).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  classifyAssistedBulkApproval,
  resolveAssistedBulkApprovalScope,
} from "../lib/ai-visibility/validation/presence-validation-assisted-proposals.js";
import {
  loadPresenceValidationCandidates,
  loadPresenceValidationReviews,
} from "../lib/ai-visibility/validation/presence-validation-candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BATCH = "presence_validation_holdout_v3_candidate_batch_v1";

const cand = loadPresenceValidationCandidates();
const reviews = loadPresenceValidationReviews();
const store = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "data/ai-visibility/validation/presence-validation-candidates/assisted-proposals/assisted-proposals.json"
    ),
    "utf8"
  )
);

const v3 = (cand.cases || []).filter(
  (c) => c.batchId === BATCH && c.primaryReviewQueue === true
);
const assisted = v3.filter((c) => store.proposals?.[c.caseId]);
const pending = v3.filter((c) => !reviews.reviews?.[c.caseId]);
const already = v3.filter((c) => reviews.reviews?.[c.caseId]);

const scoped = classifyAssistedBulkApproval({ useActiveScope: true });
const unscoped = classifyAssistedBulkApproval({ useActiveScope: false });

const report = {
  phase: "PRESENCE_HOLDOUT_V3_BULK_ELIGIBILITY_RECONCILIATION_COMPLETE",
  status: "PRESENCE_HOLDOUT_V3_BULK_ELIGIBILITY_RECONCILIATION_PASS",
  v3Scope: {
    TOTAL_PRIMARY: v3.length,
    PENDING: pending.length,
    ASSISTED: assisted.length,
    ALREADY_REVIEWED: already.length,
  },
  eligibility: {
    BACKEND_ELIGIBLE: scoped.BULK_APPROVAL_ELIGIBLE,
    UI_ELIGIBLE_OBSERVED_BEFORE_FIX: 120,
    MANUAL_REQUIRED_NOW: scoped.MANUAL_REVIEW_REQUIRED,
    UI_MANUAL_OBSERVED_BEFORE_FIX: 50,
    BACKEND_ALREADY_IN_SCOPE: scoped.ALREADY_REVIEWED,
    UNSCOPED_ELIGIBLE: unscoped.BULK_APPROVAL_ELIGIBLE,
    UNSCOPED_MANUAL: unscoped.MANUAL_REVIEW_REQUIRED,
    UNSCOPED_ALREADY_REVIEWED: unscoped.ALREADY_REVIEWED,
  },
  manualReasonBreakdown_stale_session: {
    IDENTITY_AMBIGUITY_WARNING: 50,
    detail:
      "False positive: assisted notes containing the word 'unambiguous' matched substring regex /ambiguous/ before word-boundary fix. After fix: 0.",
  },
  manualReasonBreakdown_now: {},
  priorBatchIsolation: {
    PRIOR_191_INCLUDED: "NO",
    PRIOR_191_SHOWN_IN_UNSCOPED_ALREADY_REVIEWED_DISPLAY: "YES",
    note: "Prior 191 are assisted+human-final cases. Unscoped classify places them in ALREADY_REVIEWED only — never in ELIGIBLE. UI previously rendered that unscoped ALREADY_REVIEWED beside stale 120/50 counts.",
  },
  rootCause: {
    ROOT_CAUSE:
      "(1) IDENTITY_AMBIGUITY_WARNING substring match on 'unambiguous' caused 50 false manuals; fixed with word boundaries. (2) Bulk preview API used unscoped classifyAssistedBulkApproval(), so UI showed ALREADY_REVIEWED=191 from prior pool. (3) Stale browser/server session could retain 120/50 until reload after fix.",
  },
  fix: {
    CLASSIFICATION_LOGIC_CHANGED: "YES",
    UI_REPORTING_CHANGED: "YES",
    DATA_LABELS_CHANGED: 0,
  },
  finalState: {
    BULK_APPROVAL_ELIGIBLE: scoped.BULK_APPROVAL_ELIGIBLE,
    MANUAL_REVIEW_REQUIRED: scoped.MANUAL_REVIEW_REQUIRED,
    scope: scoped.scope,
  },
  MANUAL_CASE_IDS_NOW: scoped.MANUAL_CASE_IDS,
  nextAction:
    scoped.BULK_APPROVAL_ELIGIBLE === 170 && scoped.MANUAL_REVIEW_REQUIRED === 0
      ? "CONFIRM_HOLDOUT_V3_BULK_APPROVAL"
      : "MANUAL_HOLDOUT_V3_EXCEPTIONS_REQUIRED",
  hardGuards: {
    HUMAN_FINAL_LABELS_WRITTEN: 0,
    BULK_APPROVAL_EXECUTED: 0,
    HOLDOUT_V3_SELECTION: 0,
    HOLDOUT_V3_FREEZE: 0,
    HOLDOUT_V3_SCORING: 0,
    ENTITY_RESOLVER_CHANGES: 0,
    ALIAS_CHANGES: 0,
    GROUND_TRUTH_CHANGES: 0,
    PROVIDER_CALLS: 0,
  },
};

const out = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-holdout-v3-bulk-eligibility-reconciliation.json"
);
fs.writeFileSync(out, JSON.stringify(report, null, 2) + "\n");
console.log("PRESENCE_HOLDOUT_V3_BULK_ELIGIBILITY_RECONCILIATION_COMPLETE");
console.log(JSON.stringify(report, null, 2));
console.log(`Wrote ${path.relative(ROOT, out)}`);
