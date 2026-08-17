/**
 * Presence validation ChatGPT-assisted proposals.
 * ASSISTED_PROPOSAL only — never auto-applies human ground truth.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadPresenceValidationCandidates,
  loadPresenceValidationReviews,
  presenceValidationPaths,
  savePresenceValidationReview,
} from "./presence-validation-candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const ALLOWED_ASSISTED_DECISIONS = Object.freeze([
  "PRESENT",
  "NOT_PRESENT",
  "INVALID",
  "DEFER",
]);

export function assistedProposalsPath() {
  return path.join(
    presenceValidationPaths().root,
    "assisted-proposals",
    "assisted-proposals.json"
  );
}

export function mapSystemSuggestionToDecision(suggestion) {
  const s = String(suggestion || "").trim().toUpperCase();
  if (s === "YES" || s === "PRESENT" || s === "TRUE") return "PRESENT";
  if (s === "NO" || s === "NOT_PRESENT" || s === "ABSENT" || s === "FALSE") {
    return "NOT_PRESENT";
  }
  return null;
}

export function loadAssistedProposals() {
  const p = assistedProposalsPath();
  if (!fs.existsSync(p)) {
    return {
      version: "presence_validation_assisted_proposals_v1",
      ASSISTED_PROPOSAL_IS_NOT_GROUND_TRUTH: true,
      AUTO_APPLY: false,
      proposals: {},
    };
  }
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function validateAssistedProposalDocument(doc) {
  const result = {
    ok: false,
    MATCHED_CASES: 0,
    UNKNOWN_CASES: [],
    DUPLICATE_CASES: [],
    ALREADY_REVIEWED_CASES: [],
    INVALID_DECISIONS: [],
    PROVIDER_MISMATCH_CASES: [],
    PROPOSED_PRESENT: 0,
    PROPOSED_NOT_PRESENT: 0,
    PROPOSED_INVALID: 0,
    PROPOSED_DEFER: 0,
    SYSTEM_DISAGREEMENTS: 0,
    CASE_COUNT: 0,
    proposalVersion: null,
    sourceExport: null,
    provider: null,
    stopReason: null,
  };

  if (!doc || typeof doc !== "object" || Array.isArray(doc)) {
    result.stopReason = "MALFORMED_JSON";
    return result;
  }
  if (!Array.isArray(doc.proposals)) {
    result.stopReason = "MISSING_PROPOSALS_ARRAY";
    return result;
  }

  result.proposalVersion = doc.proposalVersion || null;
  result.sourceExport = doc.sourceExport || null;
  result.provider = doc.provider || null;
  result.CASE_COUNT = doc.proposals.length;

  if (doc.caseCount != null && Number(doc.caseCount) !== doc.proposals.length) {
    result.ok = false;
    result.stopReason = "CASE_COUNT_MISMATCH";
    result.caseCountNote = `caseCount=${doc.caseCount} proposals=${doc.proposals.length}`;
    return result;
  }

  const cand = loadPresenceValidationCandidates();
  if (!cand?.cases?.length) {
    result.stopReason = "CANDIDATES_MISSING";
    return result;
  }
  const knownIds = new Set(cand.cases.map((c) => c.caseId));
  const candById = new Map(cand.cases.map((c) => [c.caseId, c]));
  const reviews = loadPresenceValidationReviews();
  const seen = new Set();
  const requireOpenAi =
    String(doc.provider || "").toLowerCase() === "openai" ||
    /openai/i.test(String(doc.proposalVersion || ""));

  for (const row of doc.proposals) {
    const caseId = row?.caseId;
    if (!caseId) {
      result.INVALID_DECISIONS.push({ caseId: null, reason: "MISSING_CASE_ID" });
      continue;
    }
    if (seen.has(caseId)) {
      result.DUPLICATE_CASES.push(caseId);
      continue;
    }
    seen.add(caseId);

    if (!knownIds.has(caseId)) {
      result.UNKNOWN_CASES.push(caseId);
      continue;
    }

    const decision = String(row.proposedDecision || "").trim().toUpperCase();
    if (!ALLOWED_ASSISTED_DECISIONS.includes(decision)) {
      result.INVALID_DECISIONS.push({ caseId, decision: row.proposedDecision });
      continue;
    }

    const candRow = candById.get(caseId);
    if (requireOpenAi) {
      const isOpenAi =
        String(candRow?.provider || "").toLowerCase() === "openai" ||
        candRow?.batchId === "presence_validation_openai_batch_v1";
      if (!isOpenAi) {
        result.PROVIDER_MISMATCH_CASES.push(caseId);
        continue;
      }
    }

    if (reviews.reviews?.[caseId]) {
      result.ALREADY_REVIEWED_CASES.push(caseId);
    }

    result.MATCHED_CASES += 1;
    if (decision === "PRESENT") result.PROPOSED_PRESENT += 1;
    else if (decision === "NOT_PRESENT") result.PROPOSED_NOT_PRESENT += 1;
    else if (decision === "INVALID") result.PROPOSED_INVALID += 1;
    else if (decision === "DEFER") result.PROPOSED_DEFER += 1;

    const sysMapped = mapSystemSuggestionToDecision(
      row.systemSuggestion ?? candRow?.SYSTEM_PRESENCE_SUGGESTION
    );
    if (
      sysMapped &&
      (decision === "PRESENT" || decision === "NOT_PRESENT") &&
      decision !== sysMapped
    ) {
      result.SYSTEM_DISAGREEMENTS += 1;
    }
  }

  if (result.UNKNOWN_CASES.length > 0) {
    result.ok = false;
    result.stopReason = "UNKNOWN_CASES";
    return result;
  }
  if (result.DUPLICATE_CASES.length > 0) {
    result.ok = false;
    result.stopReason = "DUPLICATE_CASES";
    return result;
  }
  if (result.INVALID_DECISIONS.length > 0) {
    result.ok = false;
    result.stopReason = "INVALID_DECISIONS";
    return result;
  }
  if (result.PROVIDER_MISMATCH_CASES.length > 0) {
    result.ok = false;
    result.stopReason = "PROVIDER_MISMATCH";
    return result;
  }
  if (result.ALREADY_REVIEWED_CASES.length > 0) {
    result.ok = false;
    result.stopReason = "ALREADY_REVIEWED";
    return result;
  }
  if (result.MATCHED_CASES !== result.CASE_COUNT || result.CASE_COUNT < 1) {
    result.ok = false;
    result.stopReason = "MATCH_COUNT_MISMATCH";
    return result;
  }

  result.ok = true;
  return result;
}

/**
 * Import proposals into assisted store only. Never writes humanReview.
 */
export function importAssistedProposals(doc, options = {}) {
  const validation = validateAssistedProposalDocument(doc);
  if (!validation.ok) {
    return {
      ok: false,
      IMPORT_STATUS: "STOPPED",
      validation,
      HUMAN_FINAL_LABELS_CHANGED: 0,
      AUTO_APPLIED: 0,
    };
  }

  const importedAt = new Date().toISOString();
  const existing = loadAssistedProposals();
  const priorProposals =
    existing?.proposals && typeof existing.proposals === "object"
      ? { ...existing.proposals }
      : {};

  // Merge: preserve prior assisted proposals; upsert this import batch only.
  const store = {
    version: "presence_validation_assisted_proposals_v1",
    ASSISTED_PROPOSAL_IS_NOT_GROUND_TRUTH: true,
    AUTO_APPLY: false,
    proposalVersion: doc.proposalVersion,
    sourceExport: doc.sourceExport || null,
    sourceFile: options.sourceFile || null,
    provider: doc.provider || existing.provider || null,
    warning: doc.warning || existing.warning || null,
    importedAt,
    latestImport: {
      proposalVersion: doc.proposalVersion,
      sourceExport: doc.sourceExport || null,
      sourceFile: options.sourceFile || null,
      provider: doc.provider || null,
      importedAt,
      CASE_COUNT: validation.CASE_COUNT,
    },
    importHistory: [
      ...((Array.isArray(existing.importHistory) && existing.importHistory) || []),
      {
        proposalVersion: doc.proposalVersion,
        sourceExport: doc.sourceExport || null,
        provider: doc.provider || null,
        importedAt,
        CASE_COUNT: validation.CASE_COUNT,
      },
    ],
    validationSummary: {
      MATCHED_CASES: validation.MATCHED_CASES,
      ALREADY_REVIEWED_CASES: validation.ALREADY_REVIEWED_CASES.length,
      SYSTEM_DISAGREEMENTS: validation.SYSTEM_DISAGREEMENTS,
      PROPOSED_PRESENT: validation.PROPOSED_PRESENT,
      PROPOSED_NOT_PRESENT: validation.PROPOSED_NOT_PRESENT,
      PROPOSED_INVALID: validation.PROPOSED_INVALID,
      PROPOSED_DEFER: validation.PROPOSED_DEFER,
    },
    proposals: priorProposals,
  };

  const importedCaseIds = [];
  for (const row of doc.proposals) {
    const decision = String(row.proposedDecision).trim().toUpperCase();
    const rationale =
      row.proposedRationale ||
      row.proposedNotes ||
      row.rationale ||
      "";
    store.proposals[row.caseId] = {
      caseId: row.caseId,
      source: "ChatGPT",
      proposalVersion: doc.proposalVersion,
      proposedDecision: decision,
      proposedNotes: rationale,
      proposedRationale: rationale,
      assistedProposalDecision: decision,
      assistedProposalRationale: rationale,
      assistedProposalVersion: doc.proposalVersion,
      assistedProposalImportedAt: importedAt,
      reviewSource: row.reviewSource || null,
      systemSuggestionFromProposal: row.systemSuggestion ?? null,
      canonicalEntity: row.canonicalEntityName || row.canonicalEntity || null,
      canonicalEntityId: row.canonicalEntityId || null,
      sourceResponseId: row.sourceResponseId || null,
      importedAt,
      sourceExport: doc.sourceExport || doc.sourceExportVersion || null,
      provider: doc.provider || row.provider || null,
      ASSISTED_PROPOSAL: true,
      NOT_HUMAN_GROUND_TRUTH: true,
      AUTO_HUMAN_LABELING_ALLOWED: false,
    };
    importedCaseIds.push(row.caseId);
  }

  const outPath = assistedProposalsPath();
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(store, null, 2) + "\n", "utf8");

  // Also copy source into project audit folder
  if (options.sourceFile && fs.existsSync(options.sourceFile)) {
    const auditDir = path.join(presenceValidationPaths().root, "assisted-proposals");
    const dest = path.join(auditDir, path.basename(options.sourceFile));
    fs.copyFileSync(options.sourceFile, dest);
  }

  return {
    ok: true,
    IMPORT_STATUS: "IMPORTED_AS_ASSISTED_PROPOSALS",
    validation,
    storePath: outPath,
    TOTAL_PROPOSALS: importedCaseIds.length,
    STORE_PROPOSAL_COUNT: Object.keys(store.proposals).length,
    importedCaseIds,
    HUMAN_FINAL_LABELS_CHANGED: 0,
    AUTO_APPLIED: 0,
  };
}

export function summarizeAssistedProposalProgress() {
  const store = loadAssistedProposals();
  const reviews = loadPresenceValidationReviews();
  const proposals = store.proposals || {};
  let accepted = 0;
  let changed = 0;
  let deferred = 0;
  let remaining = 0;
  let disagreements = 0;
  const cand = loadPresenceValidationCandidates();
  const byId = new Map((cand?.cases || []).map((c) => [c.caseId, c]));
  const humanActionCounts = {};

  for (const [caseId, prop] of Object.entries(proposals)) {
    const human = reviews.reviews?.[caseId];
    const candRow = byId.get(caseId);
    const sysMapped = mapSystemSuggestionToDecision(
      prop.systemSuggestionFromProposal ?? candRow?.SYSTEM_PRESENCE_SUGGESTION
    );
    if (
      sysMapped &&
      (prop.proposedDecision === "PRESENT" || prop.proposedDecision === "NOT_PRESENT") &&
      prop.proposedDecision !== sysMapped
    ) {
      disagreements += 1;
    }
    if (!human) {
      remaining += 1;
      continue;
    }
    const actionKey = human.humanAction || human.action || "UNKNOWN";
    humanActionCounts[actionKey] = (humanActionCounts[actionKey] || 0) + 1;

    if (human.action === "DEFER" || human.humanAction === "DEFERRED") {
      deferred += 1;
      continue;
    }

    // Accepted vs Changed = human final vs ASSISTED proposal (not system suggestion).
    // BULK_ACCEPTED_ASSISTED_PROPOSAL and ACCEPTED_ASSISTED_PROPOSAL count as accepted
    // when the final matches the assisted proposal. Canopy-style cases that match the
    // ChatGPT proposal but differ from system suggestion are ACCEPTED, not CHANGED.
    const proposed = String(prop.proposedDecision || "").toUpperCase();
    const finalDecision = String(human.humanFinalDecision || human.action || "").toUpperCase();
    if (finalDecision && proposed && finalDecision === proposed) {
      accepted += 1;
    } else {
      changed += 1;
    }
  }

  return {
    TOTAL_ASSISTED: Object.keys(proposals).length,
    ACCEPTED: accepted,
    CHANGED: changed,
    DEFERRED: deferred,
    REMAINING: remaining,
    ALREADY_REVIEWED: accepted + changed + deferred,
    SYSTEM_DISAGREEMENTS: disagreements,
    HUMAN_ACTION_COUNTS: humanActionCounts,
  };
}

export function attachAssistedProposalToCase(c, store, reviews) {
  const prop = store?.proposals?.[c.caseId] || null;
  const systemSuggestion = c.SYSTEM_PRESENCE_SUGGESTION ?? null;
  const sysMapped = mapSystemSuggestionToDecision(systemSuggestion);
  let disagreement = false;
  if (prop) {
    const pDec = prop.proposedDecision;
    if (
      sysMapped &&
      (pDec === "PRESENT" || pDec === "NOT_PRESENT") &&
      pDec !== sysMapped
    ) {
      disagreement = true;
    }
  }
  return {
    ...c,
    assistedProposal: prop
      ? {
          ...prop,
          ASSISTED_PROPOSAL_NOT_GROUND_TRUTH: true,
        }
      : null,
    systemSuggestionMapped: sysMapped,
    assistedDisagreesWithSystem: disagreement,
    humanReview: reviews?.reviews?.[c.caseId] || c.humanReview || null,
  };
}

function countBreakdown(rows, key) {
  const out = {};
  for (const r of rows || []) {
    const k = r[key] || "unspecified";
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

/**
 * Default bulk-approval scope = latest assisted import (avoids mixing prior reviewed pools).
 * Override with explicit caseIds / proposalVersion / batchId.
 */
export function resolveAssistedBulkApprovalScope(options = {}) {
  const store = loadAssistedProposals();
  const cand = loadPresenceValidationCandidates();
  let proposalVersion =
    options.proposalVersion != null
      ? options.proposalVersion
      : store.latestImport?.proposalVersion || store.proposalVersion || null;

  let batchId = options.batchId || null;
  try {
    const markerPath = path.join(
      ROOT,
      "data/ai-visibility/validation/presence-validation-active-pool.json"
    );
    if (!batchId && fs.existsSync(markerPath)) {
      const marker = JSON.parse(fs.readFileSync(markerPath, "utf8"));
      batchId = marker.holdoutV3BatchId || marker.activeBatchId || null;
    }
  } catch {
    // ignore marker read errors
  }

  let caseIds = options.caseIds || null;
  if (!caseIds && batchId && (cand?.cases || []).length) {
    const ids = (cand.cases || [])
      .filter((c) => c.batchId === batchId && c.primaryReviewQueue === true)
      .map((c) => c.caseId);
    if (ids.length) caseIds = ids;
  }

  return {
    proposalVersion,
    batchId,
    caseIds,
    SCOPE_NOTE:
      proposalVersion || batchId
        ? "Scoped to latest import / active review batch — prior assisted+reviewed pools excluded from eligibility counts"
        : "Unscoped — all assisted proposals in store",
  };
}

/**
 * Classify assisted proposals into BULK_APPROVAL_ELIGIBLE vs MANUAL_REVIEW_REQUIRED.
 * Does not write human labels.
 * @param {{ caseIds?: string[]|Set<string>, proposalVersion?: string, batchId?: string, useActiveScope?: boolean }} [options]
 *        When useActiveScope!==false (default true for API), scopes to latest import / active batch.
 */
export function classifyAssistedBulkApproval(options = {}) {
  const scope =
    options.useActiveScope === true
      ? resolveAssistedBulkApprovalScope(options)
      : {
          proposalVersion: options.proposalVersion ?? null,
          batchId: options.batchId ?? null,
          caseIds: options.caseIds ?? null,
          SCOPE_NOTE: options.proposalVersion || options.caseIds || options.batchId
            ? "Explicit scope"
            : "Unscoped — all assisted proposals in store",
        };

  const cand = loadPresenceValidationCandidates();
  const reviews = loadPresenceValidationReviews();
  const store = loadAssistedProposals();
  const byId = new Map((cand?.cases || []).map((c) => [c.caseId, c]));
  const caseIdFilter = scope.caseIds
    ? new Set(Array.isArray(scope.caseIds) ? scope.caseIds : [...scope.caseIds])
    : null;
  const versionFilter = scope.proposalVersion ? String(scope.proposalVersion) : null;

  const eligible = [];
  const manual = [];
  const alreadyReviewedRows = [];
  let alreadyReviewed = 0;
  let scopedTotal = 0;

  for (const [caseId, prop] of Object.entries(store.proposals || {})) {
    if (caseIdFilter && !caseIdFilter.has(caseId)) continue;
    if (versionFilter && String(prop.proposalVersion || "") !== versionFilter) continue;
    scopedTotal += 1;
    const row = byId.get(caseId);
    const human = reviews.reviews?.[caseId];
    const decision = String(prop.proposedDecision || "").toUpperCase();
    const sysMapped = mapSystemSuggestionToDecision(
      prop.systemSuggestionFromProposal ?? row?.SYSTEM_PRESENCE_SUGGESTION
    );

    const baseEntry = {
      caseId,
      proposedDecision: decision,
      systemSuggestionMapped: sysMapped,
      canonicalEntityName: row?.canonicalEntityName || prop.canonicalEntity || null,
      canonicalEntityId: row?.canonicalEntityId || null,
      provider: row?.provider || null,
      language: row?.language || null,
      geography: row?.geography || null,
      sourceResponseId: row?.sourceResponseId || row?.responseId || null,
      proposalVersion: prop.proposalVersion || store.proposalVersion,
    };

    // Mutually exclusive: already reviewed never enters eligible or manual.
    if (human) {
      alreadyReviewed += 1;
      alreadyReviewedRows.push({
        ...baseEntry,
        reasons: ["ALREADY_REVIEWED"],
        humanFinalDecision: human.humanFinalDecision || human.action || null,
        humanAction: human.humanAction || null,
      });
      continue;
    }

    const reasons = [];
    if (!row) reasons.push("MISSING_CANDIDATE");
    if (decision === "INVALID") reasons.push("PROPOSED_INVALID");
    if (decision === "DEFER") reasons.push("PROPOSED_DEFER");
    if (decision !== "PRESENT" && decision !== "NOT_PRESENT") {
      reasons.push("DECISION_NOT_BINARY");
    }
    if (!sysMapped) reasons.push("SYSTEM_SUGGESTION_UNMAPPED");
    if (sysMapped && decision !== sysMapped) reasons.push("SYSTEM_DISAGREEMENT");
    if (!row?.canonicalEntityName && !prop.canonicalEntity) {
      reasons.push("MISSING_CANONICAL_ENTITY");
    }
    if (!row?.canonicalEntityId) reasons.push("MISSING_CANONICAL_ENTITY_ID");
    if (!(row?.sourceResponseId || row?.responseId)) {
      reasons.push("MISSING_SOURCE_RESPONSE");
    }
    if (!(row?.rawText && String(row.rawText).trim())) {
      reasons.push("MISSING_RESPONSE_TEXT");
    }
    const notes = String(prop.proposedNotes || prop.proposedRationale || "").toLowerCase();
    if (
      /\bidentity\b|\bambiguous\b|\bcollision\b|\bunclear\b|invalid subject/.test(notes) ||
      /\bidentity\b|\bambiguous\b|\bcollision\b/.test(
        String(row?.systemSuggestionRationale || "").toLowerCase()
      )
    ) {
      reasons.push("IDENTITY_AMBIGUITY_WARNING");
    }

    const entry = { ...baseEntry, reasons };
    if (reasons.length === 0) eligible.push(entry);
    else manual.push(entry);
  }

  const eligiblePresent = eligible.filter((e) => e.proposedDecision === "PRESENT");
  const eligibleNotPresent = eligible.filter((e) => e.proposedDecision === "NOT_PRESENT");
  const uniqueResponses = new Set(eligible.map((e) => e.sourceResponseId).filter(Boolean));

  return {
    TOTAL_ASSISTED: caseIdFilter || versionFilter ? scopedTotal : Object.keys(store.proposals || {}).length,
    BULK_APPROVAL_ELIGIBLE: eligible.length,
    MANUAL_REVIEW_REQUIRED: manual.length,
    ALREADY_REVIEWED: alreadyReviewed,
    ELIGIBLE_PRESENT: eligiblePresent.length,
    ELIGIBLE_NOT_PRESENT: eligibleNotPresent.length,
    UNIQUE_RESPONSES: uniqueResponses.size,
    providerBreakdown: countBreakdown(eligible, "provider"),
    languageBreakdown: countBreakdown(eligible, "language"),
    geographyBreakdown: countBreakdown(eligible, "geography"),
    MANUAL_CASE_IDS: manual.map((m) => m.caseId).sort(),
    MANUAL_CASES: manual.sort((a, b) => String(a.caseId).localeCompare(String(b.caseId))),
    ELIGIBLE_CASE_IDS: eligible.map((e) => e.caseId).sort(),
    ALREADY_REVIEWED_CASE_IDS: alreadyReviewedRows.map((e) => e.caseId).sort(),
    eligible,
    alreadyReviewedRows,
    statesAreMutuallyExclusive: true,
    proposalVersion: versionFilter || store.proposalVersion || null,
    sourceExport: store.sourceExport || null,
    scope: {
      proposalVersion: versionFilter,
      batchId: scope.batchId || null,
      caseIdFilterN: caseIdFilter ? caseIdFilter.size : null,
      SCOPE_NOTE: scope.SCOPE_NOTE,
      useActiveScope: options.useActiveScope === true,
    },
    PRIOR_REVIEWED_EXCLUDED_FROM_ELIGIBLE: true,
    HUMAN_CONFIRMATION_REQUIRED: eligible.length > 0,
    AUTO_APPLIED: 0,
  };
}

/**
 * Apply bulk approval ONLY after explicit confirmation.
 * humanAction = BULK_ACCEPTED_ASSISTED_PROPOSAL
 */
export function applyAssistedBulkApproval({
  reviewer,
  confirmToken,
  caseIds = null,
  proposalVersion = null,
  useActiveScope = true,
} = {}) {
  if (confirmToken !== "CONFIRM_BULK_APPROVAL") {
    const err = new Error("EXPLICIT_CONFIRMATION_REQUIRED");
    err.code = "EXPLICIT_CONFIRMATION_REQUIRED";
    throw err;
  }
  if (!reviewer || !String(reviewer).trim()) {
    const err = new Error("REVIEWER_REQUIRED");
    err.code = "REVIEWER_REQUIRED";
    throw err;
  }

  const classification = classifyAssistedBulkApproval({
    useActiveScope,
    proposalVersion,
    caseIds: Array.isArray(caseIds) && caseIds.length ? caseIds : undefined,
  });
  const eligibleIds = new Set(classification.ELIGIBLE_CASE_IDS);
  const requested =
    Array.isArray(caseIds) && caseIds.length ? caseIds : classification.ELIGIBLE_CASE_IDS;
  const toApply = requested.filter((id) => eligibleIds.has(id));
  const skipped = requested.filter((id) => !eligibleIds.has(id));

  const approvedAt = new Date().toISOString();
  const applied = [];
  for (const caseId of toApply) {
    const entry = classification.eligible.find((e) => e.caseId === caseId);
    if (!entry) continue;
    const saved = savePresenceValidationReview(caseId, {
      action: entry.proposedDecision,
      reviewer: String(reviewer).trim(),
      notes: null,
      humanAction: "BULK_ACCEPTED_ASSISTED_PROPOSAL",
      acceptAssisted: true,
    });
    applied.push({
      caseId,
      humanFinalDecision: saved.humanFinalDecision,
      humanAction: saved.humanAction,
      sourceResponseId: entry.sourceResponseId,
      systemSuggestion: saved.systemSuggestion,
      assistedProposalVersion: entry.proposalVersion,
    });
  }

  const audit = {
    bulkApprovalId: `bulk_presval_${approvedAt.replace(/[:.]/g, "-")}`,
    approvedAt,
    reviewer: String(reviewer).trim(),
    proposalVersion: classification.proposalVersion,
    caseCount: applied.length,
    caseIds: applied.map((a) => a.caseId),
    Present: applied.filter((a) => a.humanFinalDecision === "PRESENT").length,
    NotPresent: applied.filter((a) => a.humanFinalDecision === "NOT_PRESENT").length,
    uniqueResponseCount: new Set(applied.map((a) => a.sourceResponseId).filter(Boolean)).size,
    providerBreakdown: classification.providerBreakdown,
    languageBreakdown: classification.languageBreakdown,
    geographyBreakdown: classification.geographyBreakdown,
    explicitHumanConfirmation: true,
    confirmToken: "CONFIRM_BULK_APPROVAL",
    skippedIneligibleCaseIds: skipped,
    DISAGREEMENTS_BULK_APPROVED: 0,
    INVALID_BULK_APPROVED: 0,
    DEFER_BULK_APPROVED: 0,
    AUTO_GROUND_TRUTH: 0,
  };

  const auditDir = path.join(presenceValidationPaths().root, "bulk-approvals");
  fs.mkdirSync(auditDir, { recursive: true });
  const auditPath = path.join(auditDir, `${audit.bulkApprovalId}.json`);
  fs.writeFileSync(auditPath, JSON.stringify(audit, null, 2) + "\n", "utf8");

  return {
    ok: true,
    appliedCount: applied.length,
    skippedCount: skipped.length,
    audit,
    auditPath,
    MANUAL_CASE_IDS: classification.MANUAL_CASE_IDS,
  };
}

