/**
 * Recommendation Taxonomy Human Review (52 flagged DEV cases).
 *
 * Separate from the 318-case Golden Set review queue.
 * No auto-apply. Apply requires explicit human authorization.
 * Holdout cases blocked. No provider calls.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  amendGoldenSetV2GroundTruth,
  AMENDMENT_ACTIONS,
  readGoldenSetV2Fixture,
} from "./golden-set-ground-truth-amendment.js";
import {
  RECOMMENDATION_STATUS_TAXONOMY,
} from "./golden-set-human-review.js";
import { questionStatusFromRecommendationRole } from "../recommendation-classifier-v3.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const TAXONOMY_REVIEW_VERSION = "ai_intelligence_recommendation_taxonomy_review_v1";
export const TAXONOMY_REVIEW_STORE_VERSION = "recommendation_taxonomy_human_decisions_v1";

export const TAXONOMY_REVIEW_ACTIONS = Object.freeze({
  KEEP_HUMAN_LABEL: "KEEP_HUMAN_LABEL",
  ACCEPT_TAXONOMY_PROPOSAL: "ACCEPT_TAXONOMY_PROPOSAL",
  EDIT_LABEL: "EDIT_LABEL",
  DEFER: "DEFER",
});

export const TAXONOMY_DECISION_TREE = Object.freeze([
  { step: 1, when: "entity absent", role: "no_mention" },
  { step: 2, when: "materially discouraged / excluded", role: "negative_or_qualified" },
  {
    step: 3,
    when: "explicit lead recommendation or #1 in meaningful ranked structure",
    role: "first_recommendation",
  },
  {
    step: 4,
    when: "non-first position in meaningful ordered recommendation structure",
    role: "ranked_recommendation",
  },
  {
    step: 5,
    when: "direct entity-linked positive recommendation language",
    role: "explicit_recommendation",
  },
  {
    step: 6,
    when: "viable option / consideration-set membership without direct evaluation",
    role: "associated_option",
  },
  { step: 7, when: "principally comparison/reference", role: "comparator" },
  { step: 8, when: "substantive neutral description", role: "discussed" },
  { step: 9, when: "incidental mention", role: "passing_mention" },
  { step: 10, when: "source/citation only", role: "source_only" },
]);

export const TAXONOMY_BOUNDARY_NOTES = Object.freeze([
  "Document section numbering alone ≠ ranking",
  "Shortlist/consideration membership alone ≠ explicit recommendation",
  "Neutral description ≠ associated option",
  "First textual mention alone ≠ first recommendation",
]);

function defaultArtifactPath() {
  return path.join(
    ROOT,
    "data/ai-visibility/validation/recommendation-taxonomy-ground-truth-review.json"
  );
}

function decisionsStorePath(options = {}) {
  return (
    options.decisionsPath ||
    path.join(
      ROOT,
      "data/ai-visibility/validation/human-review/recommendation-taxonomy-decisions.json"
    )
  );
}

export function loadTaxonomyReviewArtifact(options = {}) {
  const filePath = options.artifactPath || defaultArtifactPath();
  if (!fs.existsSync(filePath)) {
    const err = new Error("TAXONOMY_REVIEW_ARTIFACT_MISSING");
    err.code = "TAXONOMY_REVIEW_ARTIFACT_MISSING";
    throw err;
  }
  const doc = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return { filePath, doc };
}

export function loadTaxonomyDecisions(options = {}) {
  const filePath = decisionsStorePath(options);
  if (!fs.existsSync(filePath)) {
    return {
      filePath,
      doc: {
        version: TAXONOMY_REVIEW_STORE_VERSION,
        decisions: {},
        appliedAt: null,
        appliedBy: null,
      },
    };
  }
  return { filePath, doc: JSON.parse(fs.readFileSync(filePath, "utf8")) };
}

export function saveTaxonomyDecisions(doc, options = {}) {
  const filePath = decisionsStorePath(options);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(doc, null, 2), "utf8");
  return filePath;
}

/**
 * Validate artifact against Golden Set v2 — DEV only, no holdout.
 */
export function validateTaxonomyReviewArtifact(options = {}) {
  const { filePath, doc } = loadTaxonomyReviewArtifact(options);
  const cases = Array.isArray(doc.cases) ? doc.cases : [];
  const { doc: v2 } = readGoldenSetV2Fixture(options);
  const byId = new Map((v2.cases || []).map((c) => [c.caseId, c]));

  const valid = [];
  const invalid = [];
  const unknownCaseIds = [];
  const holdoutCases = [];

  for (const row of cases) {
    const id = row.CASE_ID;
    const gs = byId.get(id);
    if (!gs) {
      unknownCaseIds.push(id);
      invalid.push({ CASE_ID: id, reason: "UNKNOWN_CASE_ID" });
      continue;
    }
    if (gs.holdoutSplit === "holdout") {
      holdoutCases.push(id);
      invalid.push({ CASE_ID: id, reason: "HOLDOUT_CASE" });
      continue;
    }
    if (gs.groundTruthInvalidated === true || gs.excludeFromClassificationDenominator === true) {
      invalid.push({ CASE_ID: id, reason: "INVALIDATED_SUBJECT" });
      continue;
    }
    if (!RECOMMENDATION_STATUS_TAXONOMY.includes(row.CURRENT_HUMAN_LABEL)) {
      invalid.push({ CASE_ID: id, reason: "INVALID_CURRENT_LABEL" });
      continue;
    }
    if (!RECOMMENDATION_STATUS_TAXONOMY.includes(row.TAXONOMY_DECISION_PROPOSED)) {
      invalid.push({ CASE_ID: id, reason: "INVALID_PROPOSED_LABEL" });
      continue;
    }
    valid.push({
      ...row,
      holdoutSplit: gs.holdoutSplit || "development",
      goldenCurrentRole:
        gs.expectedRecommendationRole || gs.expectedRecommendationClass || null,
    });
  }

  const ok =
    cases.length === 52 &&
    valid.length === 52 &&
    holdoutCases.length === 0 &&
    unknownCaseIds.length === 0 &&
    invalid.length === 0;

  return {
    VALIDATION: ok ? "VALID" : "INVALID",
    TOTAL_CASES: cases.length,
    VALID: valid.length,
    INVALID: invalid.length,
    UNKNOWN_CASE_IDS: unknownCaseIds,
    HOLDOUT_CASES: holdoutCases.length,
    HOLDOUT_CASE_IDS: holdoutCases,
    INVALID_DETAILS: invalid,
    artifactPath: filePath,
    cases: valid,
    READY_FOR_HUMAN_APPLY: ok,
  };
}

function filterKey(row) {
  const cur = row.CURRENT_HUMAN_LABEL;
  const prop = row.TAXONOMY_DECISION_PROPOSED;
  if (cur === prop) return "KEEP_LIKELY";
  if (cur === "ranked_recommendation" || prop === "ranked_recommendation") return "RANKED_BOUNDARY";
  if (
    cur === "associated_option" ||
    prop === "associated_option" ||
    cur === "discussed" ||
    prop === "discussed"
  ) {
    if (
      (cur === "associated_option" && prop === "discussed") ||
      (cur === "discussed" && prop === "associated_option") ||
      cur === "associated_option" ||
      prop === "associated_option"
    ) {
      return "ASSOCIATED_DISCUSSED";
    }
  }
  if (cur === "first_recommendation" || prop === "first_recommendation") return "FIRST_REC";
  if (cur === "explicit_recommendation" || prop === "explicit_recommendation") return "EXPLICIT";
  return "PROPOSED_AMENDMENT";
}

/**
 * Build review queue with human decisions merged.
 */
export function buildTaxonomyReviewQueue(options = {}) {
  const validation = validateTaxonomyReviewArtifact(options);
  if (validation.HOLDOUT_CASES > 0) {
    const err = new Error("HOLDOUT_CASES_IN_TAXONOMY_REVIEW");
    err.code = "HOLDOUT_CASES_IN_TAXONOMY_REVIEW";
    err.details = validation;
    throw err;
  }

  const { doc: decisionsDoc } = loadTaxonomyDecisions(options);
  const decisions = decisionsDoc.decisions || {};
  const filter = String(options.filter || "ALL").toUpperCase();

  let rows = validation.cases.map((c) => {
    const d = decisions[c.CASE_ID] || null;
    const action = d?.action || null;
    const finalLabel =
      action === TAXONOMY_REVIEW_ACTIONS.KEEP_HUMAN_LABEL
        ? c.CURRENT_HUMAN_LABEL
        : action === TAXONOMY_REVIEW_ACTIONS.ACCEPT_TAXONOMY_PROPOSAL
          ? c.TAXONOMY_DECISION_PROPOSAL || c.TAXONOMY_DECISION_PROPOSED
          : action === TAXONOMY_REVIEW_ACTIONS.EDIT_LABEL
            ? d.editedLabel
            : action === TAXONOMY_REVIEW_ACTIONS.DEFER
              ? null
              : null;
    const isAmendProposal = c.CURRENT_HUMAN_LABEL !== c.TAXONOMY_DECISION_PROPOSED;
    return {
      ...c,
      filterBucket: filterKey(c),
      isAmendProposal,
      humanDecision: action,
      editedLabel: d?.editedLabel || null,
      decisionNotes: d?.notes || null,
      decidedAt: d?.decidedAt || null,
      decidedBy: d?.decidedBy || null,
      resolvedFinalLabel: finalLabel,
      reviewStatus: action
        ? action === TAXONOMY_REVIEW_ACTIONS.DEFER
          ? "DEFERRED"
          : "DECIDED"
        : "UNREVIEWED",
    };
  });

  if (filter && filter !== "ALL" && filter !== "ALL_52") {
    if (filter === "PROPOSED_AMENDMENT") rows = rows.filter((r) => r.isAmendProposal);
    else if (filter === "KEEP_LIKELY") rows = rows.filter((r) => !r.isAmendProposal);
    else if (filter === "UNREVIEWED") rows = rows.filter((r) => !r.humanDecision);
    else if (filter === "DEFERRED")
      rows = rows.filter((r) => r.humanDecision === TAXONOMY_REVIEW_ACTIONS.DEFER);
    else rows = rows.filter((r) => r.filterBucket === filter || filter.includes(r.filterBucket));
  }

  const summary = summarizeTaxonomyDecisions(validation.cases, decisions);

  return {
    version: TAXONOMY_REVIEW_VERSION,
    validation: {
      VALIDATION: validation.VALIDATION,
      TOTAL_CASES: validation.TOTAL_CASES,
      VALID: validation.VALID,
      INVALID: validation.INVALID,
      HOLDOUT_CASES: validation.HOLDOUT_CASES,
      READY_FOR_HUMAN_APPLY: validation.READY_FOR_HUMAN_APPLY,
    },
    decisionTree: TAXONOMY_DECISION_TREE,
    boundaryNotes: TAXONOMY_BOUNDARY_NOTES,
    recommendationTaxonomy: [...RECOMMENDATION_STATUS_TAXONOMY],
    summary,
    filter,
    cases: rows,
    appliedAt: decisionsDoc.appliedAt || null,
    AUTO_GROUND_TRUTH_CHANGES: 0,
  };
}

export function summarizeTaxonomyDecisions(cases, decisions) {
  let unreviewed = 0;
  let keep = 0;
  let amend = 0;
  let defer = 0;
  let edit = 0;
  const byTransition = {};

  for (const c of cases) {
    const d = decisions[c.CASE_ID];
    const cur = c.CURRENT_HUMAN_LABEL;
    const prop = c.TAXONOMY_DECISION_PROPOSED;
    const key = `${cur} => ${prop}`;
    byTransition[key] = (byTransition[key] || 0) + 1;

    if (!d?.action) {
      unreviewed += 1;
      continue;
    }
    if (d.action === TAXONOMY_REVIEW_ACTIONS.KEEP_HUMAN_LABEL) keep += 1;
    else if (d.action === TAXONOMY_REVIEW_ACTIONS.ACCEPT_TAXONOMY_PROPOSAL) amend += 1;
    else if (d.action === TAXONOMY_REVIEW_ACTIONS.EDIT_LABEL) edit += 1;
    else if (d.action === TAXONOMY_REVIEW_ACTIONS.DEFER) defer += 1;
  }

  const proposedKeep = cases.filter((c) => c.CURRENT_HUMAN_LABEL === c.TAXONOMY_DECISION_PROPOSED)
    .length;
  const proposedAmend = cases.length - proposedKeep;

  return {
    TOTAL: cases.length,
    UNREVIEWED: unreviewed,
    DECIDED_KEEP: keep,
    DECIDED_ACCEPT_PROPOSAL: amend,
    DECIDED_EDIT: edit,
    DEFERRED: defer,
    REMAINING: unreviewed,
    PROPOSED_KEEP: proposedKeep,
    PROPOSED_AMEND: proposedAmend,
    PROPOSED_DEFER: 0,
    BY_TRANSITION: Object.entries(byTransition)
      .sort((a, b) => b[1] - a[1])
      .map(([transition, count]) => ({ transition, count })),
  };
}

/**
 * Save per-case human decision (does not amend Golden Set).
 */
export function setTaxonomyReviewDecision(payload, options = {}) {
  const caseId = payload?.caseId;
  const action = payload?.action;
  if (!caseId) {
    const err = new Error("CASE_ID_REQUIRED");
    err.code = "CASE_ID_REQUIRED";
    throw err;
  }
  if (!Object.values(TAXONOMY_REVIEW_ACTIONS).includes(action)) {
    const err = new Error("INVALID_TAXONOMY_REVIEW_ACTION");
    err.code = "INVALID_TAXONOMY_REVIEW_ACTION";
    throw err;
  }

  const validation = validateTaxonomyReviewArtifact(options);
  const row = validation.cases.find((c) => c.CASE_ID === caseId);
  if (!row) {
    const err = new Error("CASE_NOT_IN_TAXONOMY_REVIEW");
    err.code = "CASE_NOT_IN_TAXONOMY_REVIEW";
    throw err;
  }

  if (action === TAXONOMY_REVIEW_ACTIONS.EDIT_LABEL) {
    if (!RECOMMENDATION_STATUS_TAXONOMY.includes(payload.editedLabel)) {
      const err = new Error("INVALID_EDITED_LABEL");
      err.code = "INVALID_EDITED_LABEL";
      throw err;
    }
  }

  const { filePath, doc } = loadTaxonomyDecisions(options);
  if (doc.appliedAt) {
    const err = new Error("TAXONOMY_REVIEW_ALREADY_APPLIED");
    err.code = "TAXONOMY_REVIEW_ALREADY_APPLIED";
    throw err;
  }

  doc.decisions = doc.decisions || {};
  doc.decisions[caseId] = {
    action,
    editedLabel: action === TAXONOMY_REVIEW_ACTIONS.EDIT_LABEL ? payload.editedLabel : null,
    notes: payload.notes || null,
    decidedAt: new Date().toISOString(),
    decidedBy: payload.reviewer || null,
  };
  doc.version = TAXONOMY_REVIEW_STORE_VERSION;
  saveTaxonomyDecisions(doc, { ...options, decisionsPath: filePath });

  return {
    caseId,
    decision: doc.decisions[caseId],
    summary: summarizeTaxonomyDecisions(validation.cases, doc.decisions),
    AUTO_GROUND_TRUTH_CHANGES: 0,
  };
}

/**
 * Seed ACCEPT_TAXONOMY_PROPOSAL for all amend-proposal cases (or all if includeKeep).
 * Does NOT apply to Golden Set — decisions only.
 */
export function acceptAllValidTaxonomyProposals(payload, options = {}) {
  const reviewer = payload?.reviewer;
  if (!reviewer || !String(reviewer).trim()) {
    const err = new Error("REVIEWER_REQUIRED");
    err.code = "REVIEWER_REQUIRED";
    throw err;
  }
  const validation = validateTaxonomyReviewArtifact(options);
  if (!validation.READY_FOR_HUMAN_APPLY) {
    const err = new Error("TAXONOMY_REVIEW_NOT_VALID");
    err.code = "TAXONOMY_REVIEW_NOT_VALID";
    throw err;
  }

  const { filePath, doc } = loadTaxonomyDecisions(options);
  if (doc.appliedAt) {
    const err = new Error("TAXONOMY_REVIEW_ALREADY_APPLIED");
    err.code = "TAXONOMY_REVIEW_ALREADY_APPLIED";
    throw err;
  }

  const includeKeep = payload?.includeKeep === true;
  const now = new Date().toISOString();
  doc.decisions = doc.decisions || {};
  let seeded = 0;
  for (const c of validation.cases) {
    const isAmend = c.CURRENT_HUMAN_LABEL !== c.TAXONOMY_DECISION_PROPOSED;
    if (!isAmend && !includeKeep) continue;
    doc.decisions[c.CASE_ID] = {
      action: isAmend
        ? TAXONOMY_REVIEW_ACTIONS.ACCEPT_TAXONOMY_PROPOSAL
        : TAXONOMY_REVIEW_ACTIONS.KEEP_HUMAN_LABEL,
      editedLabel: null,
      notes: "Seeded via ACCEPT ALL VALID TAXONOMY PROPOSALS (not yet applied)",
      decidedAt: now,
      decidedBy: String(reviewer).trim(),
    };
    seeded += 1;
  }
  saveTaxonomyDecisions(doc, { ...options, decisionsPath: filePath });
  return {
    seeded,
    summary: summarizeTaxonomyDecisions(validation.cases, doc.decisions),
    note: "Decisions seeded only — Golden Set unchanged until explicit Apply",
    AUTO_GROUND_TRUTH_CHANGES: 0,
  };
}

function resolveFinalLabel(caseRow, decision) {
  if (!decision?.action) return null;
  if (decision.action === TAXONOMY_REVIEW_ACTIONS.KEEP_HUMAN_LABEL) {
    return caseRow.CURRENT_HUMAN_LABEL;
  }
  if (decision.action === TAXONOMY_REVIEW_ACTIONS.ACCEPT_TAXONOMY_PROPOSAL) {
    return caseRow.TAXONOMY_DECISION_PROPOSED;
  }
  if (decision.action === TAXONOMY_REVIEW_ACTIONS.EDIT_LABEL) {
    return decision.editedLabel;
  }
  return null; // DEFER
}

/**
 * Preview apply — no writes to Golden Set.
 */
export function previewTaxonomyReviewApply(options = {}) {
  const validation = validateTaxonomyReviewArtifact(options);
  if (validation.HOLDOUT_CASES > 0) {
    const err = new Error("HOLDOUT_CASES_IN_TAXONOMY_REVIEW");
    err.code = "HOLDOUT_CASES_IN_TAXONOMY_REVIEW";
    throw err;
  }
  const { doc: decisionsDoc } = loadTaxonomyDecisions(options);
  const decisions = decisionsDoc.decisions || {};

  const keep = [];
  const amend = [];
  const defer = [];
  const unreviewed = [];
  const byCurrentToFinal = {};

  for (const c of validation.cases) {
    const d = decisions[c.CASE_ID];
    if (!d?.action) {
      unreviewed.push(c.CASE_ID);
      continue;
    }
    if (d.action === TAXONOMY_REVIEW_ACTIONS.DEFER) {
      defer.push(c.CASE_ID);
      continue;
    }
    const finalLabel = resolveFinalLabel(c, d);
    const key = `${c.CURRENT_HUMAN_LABEL} => ${finalLabel}`;
    byCurrentToFinal[key] = (byCurrentToFinal[key] || 0) + 1;
    if (finalLabel === c.CURRENT_HUMAN_LABEL) {
      keep.push({ CASE_ID: c.CASE_ID, label: finalLabel });
    } else {
      amend.push({
        CASE_ID: c.CASE_ID,
        FROM: c.CURRENT_HUMAN_LABEL,
        TO: finalLabel,
        action: d.action,
      });
    }
  }

  return {
    TOTAL: validation.TOTAL_CASES,
    KEEP_PROPOSED: keep.length,
    AMEND_PROPOSED: amend.length,
    DEFER_PROPOSED: defer.length,
    UNREVIEWED: unreviewed.length,
    BREAKDOWN_BY_CURRENT_TO_PROPOSED: Object.entries(byCurrentToFinal)
      .sort((a, b) => b[1] - a[1])
      .map(([transition, count]) => ({ transition, count })),
    KEEP_CASES: keep,
    AMEND_CASES: amend,
    DEFER_CASES: defer,
    UNREVIEWED_CASES: unreviewed,
    CAN_APPLY:
      unreviewed.length === 0 &&
      validation.READY_FOR_HUMAN_APPLY &&
      !decisionsDoc.appliedAt,
    BLOCKERS: [
      unreviewed.length ? `UNREVIEWED:${unreviewed.length}` : null,
      decisionsDoc.appliedAt ? "ALREADY_APPLIED" : null,
      !validation.READY_FOR_HUMAN_APPLY ? "VALIDATION_INVALID" : null,
    ].filter(Boolean),
    AUTO_GROUND_TRUTH_CHANGES: 0,
    note: "Preview only — pass explicitApply:true to mutate Golden Set",
  };
}

/**
 * Apply taxonomy review decisions to Golden Set v2 via governed amendment API.
 * Requires explicitApply:true + reviewer + confirmToken matching APPLY_TAXONOMY_REVIEW.
 */
export function applyTaxonomyReviewDecisions(payload, options = {}) {
  if (payload?.explicitApply !== true) {
    const err = new Error("EXPLICIT_APPLY_REQUIRED");
    err.code = "EXPLICIT_APPLY_REQUIRED";
    throw err;
  }
  if (payload?.confirmToken !== "APPLY_TAXONOMY_REVIEW") {
    const err = new Error("CONFIRM_TOKEN_REQUIRED");
    err.code = "CONFIRM_TOKEN_REQUIRED";
    throw err;
  }
  const reviewer = payload?.reviewer;
  if (!reviewer || !String(reviewer).trim()) {
    const err = new Error("REVIEWER_REQUIRED");
    err.code = "REVIEWER_REQUIRED";
    throw err;
  }

  const preview = previewTaxonomyReviewApply(options);
  if (!preview.CAN_APPLY) {
    const err = new Error("TAXONOMY_APPLY_BLOCKED");
    err.code = "TAXONOMY_APPLY_BLOCKED";
    err.details = preview;
    throw err;
  }

  const validation = validateTaxonomyReviewArtifact(options);
  const { filePath, doc: decisionsDoc } = loadTaxonomyDecisions(options);
  const decisions = decisionsDoc.decisions || {};

  const results = {
    KEPT: 0,
    AMENDED: 0,
    DEFERRED: 0,
    FAILED: [],
    BY_TRANSITION: {},
    BY_PROVIDER: {},
    BY_LANGUAGE: {},
    BY_GEOGRAPHY: {},
  };

  for (const c of validation.cases) {
    const d = decisions[c.CASE_ID];
    const action = d.action;

    if (action === TAXONOMY_REVIEW_ACTIONS.DEFER) {
      // Mark deferred on golden set via DEFER amendment (no label change)
      amendGoldenSetV2GroundTruth(
        {
          caseId: c.CASE_ID,
          action: AMENDMENT_ACTIONS.DEFER,
          reviewer: String(reviewer).trim(),
          amendmentReason: "Taxonomy review deferred",
        },
        { ...options, apply: true }
      );
      // Also stamp taxonomyDeferred on case via a no-op keep path — DEFER returns early.
      // Persist defer flag through a CORRECT that only adds metadata is not available.
      // Use local decisions + a light fixture patch for exclude-from-tuning.
      results.DEFERRED += 1;
      continue;
    }

    const finalLabel = resolveFinalLabel(c, d);
    if (finalLabel === c.CURRENT_HUMAN_LABEL) {
      amendGoldenSetV2GroundTruth(
        {
          caseId: c.CASE_ID,
          action: AMENDMENT_ACTIONS.KEEP_CURRENT_HUMAN_LABEL,
          reviewer: String(reviewer).trim(),
          amendmentReason: "Taxonomy review KEEP_HUMAN_LABEL",
        },
        { ...options, apply: false }
      );
      results.KEPT += 1;
    } else {
      const questionStatus = questionStatusFromRecommendationRole(finalLabel, true);
      const firstRec = finalLabel === "first_recommendation";
      try {
        amendGoldenSetV2GroundTruth(
          {
            caseId: c.CASE_ID,
            action: AMENDMENT_ACTIONS.CORRECT_HUMAN_LABEL,
            reviewer: String(reviewer).trim(),
            amendmentReason: `Taxonomy review ${action}: ${c.CURRENT_HUMAN_LABEL} → ${finalLabel}. ${c.REASON || ""}`.trim(),
            amendedLabels: {
              expectedRecommendationRole: finalLabel,
              recommendationStatus: finalLabel,
              expectedFirstRecommendation: firstRec,
              expectedQuestionStatus: questionStatus,
            },
          },
          { ...options, apply: true }
        );
        results.AMENDED += 1;
        const t = `${c.CURRENT_HUMAN_LABEL} => ${finalLabel}`;
        results.BY_TRANSITION[t] = (results.BY_TRANSITION[t] || 0) + 1;
        const p = c.PROVIDER || "unspecified";
        const l = c.LANGUAGE || "unspecified";
        const g = c.GEOGRAPHY || "unspecified";
        results.BY_PROVIDER[p] = (results.BY_PROVIDER[p] || 0) + 1;
        results.BY_LANGUAGE[l] = (results.BY_LANGUAGE[l] || 0) + 1;
        results.BY_GEOGRAPHY[g] = (results.BY_GEOGRAPHY[g] || 0) + 1;
      } catch (err) {
        results.FAILED.push({ CASE_ID: c.CASE_ID, error: err.message, code: err.code });
      }
    }
  }

  // Stamp deferred cases on v2 fixture for evaluation exclusion
  if (results.DEFERRED > 0) {
    const { filePath: v2Path, doc: v2 } = readGoldenSetV2Fixture(options);
    let changed = false;
    for (const c of validation.cases) {
      const d = decisions[c.CASE_ID];
      if (d?.action !== TAXONOMY_REVIEW_ACTIONS.DEFER) continue;
      const idx = (v2.cases || []).findIndex((x) => x.caseId === c.CASE_ID);
      if (idx < 0) continue;
      v2.cases[idx] = {
        ...v2.cases[idx],
        taxonomyReviewDeferred: true,
        excludeFromClassificationDenominator:
          v2.cases[idx].excludeFromClassificationDenominator === true
            ? true
            : false,
        // Deferred taxonomy cases excluded from tuning denominators until resolved
        excludeFromRecommendationTuning: true,
        notes: [
          v2.cases[idx].notes,
          "TAXONOMY_REVIEW_DEFERRED",
        ]
          .filter(Boolean)
          .join(" | "),
      };
      changed = true;
    }
    if (changed) {
      fs.writeFileSync(v2Path, JSON.stringify(v2, null, 2), "utf8");
    }
  }

  decisionsDoc.appliedAt = new Date().toISOString();
  decisionsDoc.appliedBy = String(reviewer).trim();
  decisionsDoc.applyResult = {
    REVIEWED_52: validation.TOTAL_CASES,
    ...results,
  };
  saveTaxonomyDecisions(decisionsDoc, { ...options, decisionsPath: filePath });

  return {
    REVIEWED_52: validation.TOTAL_CASES,
    KEPT: results.KEPT,
    AMENDED: results.AMENDED,
    DEFERRED: results.DEFERRED,
    REMAINING: 0,
    FAILED: results.FAILED,
    BY_TRANSITION: results.BY_TRANSITION,
    BY_PROVIDER: results.BY_PROVIDER,
    BY_LANGUAGE: results.BY_LANGUAGE,
    BY_GEOGRAPHY: results.BY_GEOGRAPHY,
    AUTO_GROUND_TRUTH_CHANGES: 0,
    UNAUTHORIZED_GOLDEN_SET_CHANGES: 0,
    HOLDOUT_ACCESSED: false,
  };
}

/**
 * Ready summary for human apply gate (no mutations).
 */
export function getTaxonomyReviewReadySummary(options = {}) {
  const validation = validateTaxonomyReviewArtifact(options);
  const summary = summarizeTaxonomyDecisions(
    validation.cases,
    loadTaxonomyDecisions(options).doc.decisions || {}
  );
  return {
    status: "AI_INTELLIGENCE_RECOMMENDATION_TAXONOMY_REVIEW_READY",
    TOTAL_CASES: validation.TOTAL_CASES,
    VALID: validation.VALID,
    INVALID: validation.INVALID,
    HOLDOUT_CASES: validation.HOLDOUT_CASES,
    UNKNOWN_CASE_IDS: validation.UNKNOWN_CASE_IDS,
    PROPOSED_KEEP: summary.PROPOSED_KEEP,
    PROPOSED_AMEND: summary.PROPOSED_AMEND,
    PROPOSED_DEFER: summary.PROPOSED_DEFER,
    BY_TRANSITION: summary.BY_TRANSITION,
    READY_FOR_HUMAN_APPLY: validation.READY_FOR_HUMAN_APPLY ? "YES" : "NO",
    VALIDATION: validation.VALIDATION,
    route: "/ai-intelligence-recommendation-taxonomy-review",
    AUTO_GROUND_TRUTH_CHANGES: 0,
    note: "Do not apply on behalf of Joan — explicit Apply required in UI",
  };
}
