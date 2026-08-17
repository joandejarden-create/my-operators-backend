/**
 * Golden Set v2 governed ground-truth label amendment / versioning.
 *
 * Human labels may change only via explicit authorized human review.
 * Original review audit is preserved. No auto-label. No holdout access.
 * Classifier / resolver / aliases are never modified here.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { V2_PATH, GOLDEN_SET_V2_VERSION } from "./golden-set-human-review.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const GROUND_TRUTH_AMENDMENT_VERSION = "ai_intelligence_golden_set_ground_truth_amendment_v1";

export const AMENDMENT_ACTIONS = Object.freeze({
  KEEP_CURRENT_HUMAN_LABEL: "KEEP_CURRENT_HUMAN_LABEL",
  CORRECT_HUMAN_LABEL: "CORRECT_HUMAN_LABEL",
  INVALIDATE_CANDIDATE_SUBJECT: "INVALIDATE_CANDIDATE_SUBJECT",
  DEFER: "DEFER",
});

export function amendmentHistoryDir(options = {}) {
  const rootDir =
    options.rootDir ||
    path.join(ROOT, "data/ai-visibility/validation/human-review");
  return path.join(rootDir, "ground-truth-amendments");
}

export function readGoldenSetV2Fixture(options = {}) {
  const filePath = options.filePath || V2_PATH;
  if (!fs.existsSync(filePath)) {
    const err = new Error("GOLDEN_SET_V2_MISSING");
    err.code = "GOLDEN_SET_V2_MISSING";
    throw err;
  }
  return {
    filePath,
    doc: JSON.parse(fs.readFileSync(filePath, "utf8")),
  };
}

function snapshotLabels(c) {
  return {
    expectedEntityPresent: c.expectedEntityPresent ?? null,
    expectedRecommendationRole:
      c.expectedRecommendationRole || c.expectedRecommendationClass || null,
    expectedRecommendationClass:
      c.expectedRecommendationClass || c.expectedRecommendationRole || null,
    expectedFirstRecommendation:
      c.expectedFirstRecommendation === true
        ? true
        : c.expectedFirstRecommendation === false
          ? false
          : c.expectedFirstRecommendation ?? null,
    expectedQuestionStatus: c.expectedQuestionStatus ?? null,
    expectedCitationAssociation: c.expectedCitationAssociation ?? null,
    candidateEntity: c.candidateEntity || c.entityName || null,
    canonicalEntityId: c.canonicalEntityId || null,
    reviewStatus: c.reviewStatus || null,
    groundTruthInvalidated: c.groundTruthInvalidated === true,
    reviewer: c.reviewer || null,
    reviewedAt: c.reviewedAt || null,
  };
}

/**
 * Build a versioned amendment record. Does not write unless apply=true.
 * AUTO_LABEL_CHANGES forbidden — apply requires explicit human action + reviewer + reason.
 */
export function amendGoldenSetV2GroundTruth(payload, options = {}) {
  const apply = options.apply === true;
  const caseId = payload?.caseId;
  if (!caseId) {
    const err = new Error("CASE_ID_REQUIRED");
    err.code = "CASE_ID_REQUIRED";
    throw err;
  }

  const action = payload.action;
  if (!Object.values(AMENDMENT_ACTIONS).includes(action)) {
    const err = new Error("INVALID_AMENDMENT_ACTION");
    err.code = "INVALID_AMENDMENT_ACTION";
    throw err;
  }

  if (action === AMENDMENT_ACTIONS.KEEP_CURRENT_HUMAN_LABEL || action === AMENDMENT_ACTIONS.DEFER) {
    return {
      apply: false,
      written: false,
      caseId,
      action,
      CLASSIFIER_LOGIC_CHANGE: false,
      AUTO_LABEL_CHANGES: false,
      note:
        action === AMENDMENT_ACTIONS.DEFER
          ? "Deferred — no ground-truth change"
          : "Keep current human label — no ground-truth change",
    };
  }

  if (!payload.reviewer || !String(payload.reviewer).trim()) {
    const err = new Error("REVIEWER_REQUIRED");
    err.code = "REVIEWER_REQUIRED";
    throw err;
  }
  if (!payload.amendmentReason || !String(payload.amendmentReason).trim()) {
    const err = new Error("AMENDMENT_REASON_REQUIRED");
    err.code = "AMENDMENT_REASON_REQUIRED";
    throw err;
  }

  const { filePath, doc } = readGoldenSetV2Fixture(options);
  const cases = Array.isArray(doc.cases) ? doc.cases : [];
  const idx = cases.findIndex((c) => c.caseId === caseId || `v2_${c.sourceCaseId}` === caseId);
  if (idx < 0) {
    const err = new Error("CASE_NOT_FOUND");
    err.code = "CASE_NOT_FOUND";
    throw err;
  }

  const current = cases[idx];
  // Holdout cases: CORRECT/label edits remain blocked.
  // INVALIDATE_CANDIDATE_SUBJECT may run only with explicit integrity-repair authorization
  // (does NOT restore untouched holdout certification eligibility).
  if (current.holdoutSplit === "holdout") {
    const integrityRepair =
      action === AMENDMENT_ACTIONS.INVALIDATE_CANDIDATE_SUBJECT &&
      options.allowHoldoutIntegrityRepair === true &&
      payload.holdoutIntegrityRepairAuthorized === true;
    if (!integrityRepair) {
      const err = new Error("HOLDOUT_CASE_AMENDMENT_BLOCKED");
      err.code = "HOLDOUT_CASE_AMENDMENT_BLOCKED";
      throw err;
    }
  }

  const originalHumanLabel = snapshotLabels(current);
  const now = new Date().toISOString();
  const amendedLabels = { ...originalHumanLabel };

  if (action === AMENDMENT_ACTIONS.CORRECT_HUMAN_LABEL) {
    const next = payload.amendedLabels || {};
    if (next.expectedEntityPresent != null) {
      amendedLabels.expectedEntityPresent = !!next.expectedEntityPresent;
    }
    if (next.expectedRecommendationRole != null || next.recommendationStatus != null) {
      const role = next.expectedRecommendationRole || next.recommendationStatus;
      amendedLabels.expectedRecommendationRole = role;
      amendedLabels.expectedRecommendationClass = role;
    }
    if (next.expectedFirstRecommendation != null || next.firstRecommendation != null) {
      const fr = next.expectedFirstRecommendation ?? next.firstRecommendation;
      amendedLabels.expectedFirstRecommendation =
        fr === "NOT_APPLICABLE" ? null : fr === true || fr === false ? fr : !!fr;
    }
    if (next.expectedQuestionStatus != null || next.questionStatus != null) {
      amendedLabels.expectedQuestionStatus =
        next.expectedQuestionStatus || next.questionStatus;
    }
    if (next.expectedCitationAssociation != null || next.citationAssociation != null) {
      amendedLabels.expectedCitationAssociation =
        next.expectedCitationAssociation || next.citationAssociation;
    }
    amendedLabels.groundTruthInvalidated = false;
  } else if (action === AMENDMENT_ACTIONS.INVALIDATE_CANDIDATE_SUBJECT) {
    // Keep historically auditable; exclude from classification denominators.
    amendedLabels.expectedEntityPresent = false;
    amendedLabels.groundTruthInvalidated = true;
    amendedLabels.excludeFromClassificationDenominator = true;
    amendedLabels.reviewStatus = "INVALIDATED_CANDIDATE_SUBJECT";
  }

  const originalCandidateSubject =
    current.candidateEntity ||
    current.entityName ||
    originalHumanLabel.candidateEntity ||
    null;

  const amendmentRecord = {
    version: GROUND_TRUTH_AMENDMENT_VERSION,
    caseId,
    action,
    AMENDMENT_ACTION: action,
    ORIGINAL_HUMAN_LABEL: originalHumanLabel,
    ORIGINAL_CANDIDATE_SUBJECT: originalCandidateSubject,
    ORIGINAL_CANONICAL_ID: current.canonicalEntityId || originalHumanLabel.canonicalEntityId || null,
    AMENDED_HUMAN_LABEL: amendedLabels,
    AMENDMENT_REASON: String(payload.amendmentReason).trim(),
    REVIEWER: String(payload.reviewer).trim(),
    AMENDED_AT: now,
    ORIGINAL_REVIEW_AUDIT: {
      reviewer: current.reviewer || null,
      reviewedAt: current.reviewedAt || null,
      reviewStatus: current.reviewStatus || null,
      notes: current.notes || null,
      source: current.source || null,
      systemSuggestionPreserved: current.systemSuggestionPreserved || null,
    },
    CLASSIFIER_LOGIC_CHANGE: false,
    AUTO_LABEL_CHANGES: false,
    HOLDOUT_ACCESSED: current.holdoutSplit === "holdout",
    HOLDOUT_INTEGRITY_REPAIR: current.holdoutSplit === "holdout",
    HOLDOUT_UNTOUCHED_RESTORED: false,
    note:
      current.holdoutSplit === "holdout"
        ? "Holdout integrity repair only — does not restore untouched certification eligibility"
        : null,
  };

  if (!apply) {
    return {
      apply: false,
      written: false,
      caseId,
      action,
      preview: amendmentRecord,
      CLASSIFIER_LOGIC_CHANGE: false,
      AUTO_LABEL_CHANGES: false,
      note: "Dry-run only — pass apply:true after explicit human authorization",
    };
  }

  const histRoot = amendmentHistoryDir(options);
  fs.mkdirSync(histRoot, { recursive: true });
  const caseHist = path.join(histRoot, caseId);
  fs.mkdirSync(caseHist, { recursive: true });
  const stamp = now.replace(/[:.]/g, "-");
  fs.writeFileSync(
    path.join(caseHist, `${stamp}.json`),
    JSON.stringify(amendmentRecord, null, 2),
    "utf8"
  );

  // Snapshot prior case blob (never erase)
  fs.writeFileSync(
    path.join(caseHist, `${stamp}.prior-case.json`),
    JSON.stringify(current, null, 2),
    "utf8"
  );

  const priorHistory = Array.isArray(current.labelAmendmentHistory)
    ? current.labelAmendmentHistory
    : [];

  const nextCase = {
    ...current,
    expectedEntityPresent: amendedLabels.expectedEntityPresent,
    expectedRecommendationRole: amendedLabels.expectedRecommendationRole,
    expectedRecommendationClass: amendedLabels.expectedRecommendationClass,
    expectedFirstRecommendation: amendedLabels.expectedFirstRecommendation,
    expectedQuestionStatus: amendedLabels.expectedQuestionStatus,
    expectedCitationAssociation: amendedLabels.expectedCitationAssociation,
    reviewStatus: amendedLabels.reviewStatus || current.reviewStatus,
    groundTruthInvalidated: amendedLabels.groundTruthInvalidated === true,
    excludeFromClassificationDenominator:
      amendedLabels.excludeFromClassificationDenominator === true ||
      amendedLabels.groundTruthInvalidated === true,
    originalCandidateSubjectPreserved:
      current.originalCandidateSubjectPreserved || originalCandidateSubject,
    originalCanonicalIdPreserved:
      current.originalCanonicalIdPreserved || current.canonicalEntityId || null,
    // Retain candidateEntity/canonicalEntityId for audit trail; scoring excludes invalidated.
    caseType: amendedLabels.expectedRecommendationRole || current.caseType,
    reviewer: amendmentRecord.REVIEWER,
    reviewedAt: now,
    notes: [
      current.notes,
      `GROUND_TRUTH_AMENDMENT:${action}:${amendmentRecord.AMENDMENT_REASON}`,
    ]
      .filter(Boolean)
      .join(" | "),
    labelAmendmentHistory: [...priorHistory, amendmentRecord],
    goldenSetVersion: GOLDEN_SET_V2_VERSION,
    groundTruthAmendmentVersion: GROUND_TRUTH_AMENDMENT_VERSION,
  };

  const nextCases = [...cases];
  nextCases[idx] = nextCase;
  const nextDoc = {
    ...doc,
    cases: nextCases,
    lastGroundTruthAmendmentAt: now,
    groundTruthAmendmentVersion: GROUND_TRUTH_AMENDMENT_VERSION,
  };

  // Versioned fixture snapshot before overwrite
  const fixtureSnapDir = path.join(histRoot, "_v2-snapshots");
  fs.mkdirSync(fixtureSnapDir, { recursive: true });
  fs.copyFileSync(filePath, path.join(fixtureSnapDir, `golden-set-v2-before-${stamp}.json`));

  fs.writeFileSync(filePath, JSON.stringify(nextDoc, null, 2), "utf8");

  return {
    apply: true,
    written: true,
    caseId,
    action,
    amendment: amendmentRecord,
    CLASSIFIER_LOGIC_CHANGE: false,
    AUTO_LABEL_CHANGES: false,
    path: filePath,
  };
}

/**
 * Governance helpers used by residual entity audits (no writes).
 */
export function assessPlayaBrandReference(text) {
  const t = String(text || "");
  const unambiguous =
    /playa\s+hotels\s*(?:&|and)?\s*resorts/i.test(t) || /\bplaya\s+hotels\b/i.test(t);
  const bareOrGeneric = /\bplaya\b/i.test(t);
  const exactUnambiguous = (() => {
    const m = t.match(/playa\s+hotels\s*(?:&|and)?\s*resorts/i) || t.match(/\bplaya\s+hotels\b/i);
    return m ? m[0] : null;
  })();
  const exactGeneric = (() => {
    const windows = [];
    const lower = t.toLowerCase();
    let i = 0;
    let n = 0;
    while ((i = lower.indexOf("playa", i)) >= 0 && n < 6) {
      windows.push(t.slice(Math.max(0, i - 40), Math.min(t.length, i + 55)).replace(/\s+/g, " "));
      i += 5;
      n += 1;
    }
    return windows;
  })();
  return {
    UNAMBIGUOUS_BRAND_REFERENCE: unambiguous ? "YES" : "NO",
    EXACT_TEXT: unambiguous ? exactUnambiguous : exactGeneric[0] || null,
    EXAMPLE_SURFACES: exactGeneric,
    GROUND_TRUTH_REVIEW_REQUIRED: unambiguous ? "NO" : "YES",
  };
}

export function assessIhgManagedReference(text) {
  const t = String(text || "");
  const specificManaged = /ihg\s+hotels\s*(?:&|and)?\s*resorts\s*\(\s*managed\s*\)/i.test(t);
  const parentFull = /ihg\s+hotels\s*(?:&|and)?\s*resorts\b/i.test(t);
  const bareParent = /\bihg\b/i.test(t) || /intercontinental\s+hotels\s+group/i.test(t);
  const exact = (() => {
    const m =
      t.match(/ihg\s+hotels\s*(?:&|and)?\s*resorts\s*\(\s*managed\s*\)/i) ||
      t.match(/ihg\s+hotels\s*(?:&|and)?\s*resorts\b/i) ||
      t.match(/\bihg\b/i);
    return m ? m[0] : null;
  })();
  return {
    SPECIFIC_CANONICAL_REFERENCE: specificManaged ? "YES" : "NO",
    PARENT_ONLY_REFERENCE: !specificManaged && (parentFull || bareParent) ? "YES" : "NO",
    EXACT_TEXT: exact,
    GROUND_TRUTH_REVIEW_REQUIRED: specificManaged ? "NO" : "YES",
  };
}
