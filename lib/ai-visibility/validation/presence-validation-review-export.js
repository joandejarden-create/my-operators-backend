/**
 * Presence validation review export — ChatGPT-assist packets.
 * SYSTEM SUGGESTION = ASSISTANCE ONLY. Never auto-label. Export is read-only.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadPresenceValidationCandidates,
  loadPresenceValidationReviews,
  presenceValidationPaths,
} from "./presence-validation-candidates.js";
import { classifyAssistedBulkApproval } from "./presence-validation-assisted-proposals.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const PRESENCE_VALIDATION_EXPORT_VERSION =
  "presence_validation_review_export_v1";

export const ALLOWED_PROPOSED_DECISIONS = Object.freeze([
  "PRESENT",
  "NOT_PRESENT",
  "INVALID",
  "DEFER",
]);

/**
 * Shared filter for queue + export (no mutation).
 */
export function filterPresenceValidationCases(allCases, filters = {}) {
  let cases = [...(allCases || [])];
  const status = String(filters.status || "pending").toLowerCase();
  const provider = filters.provider || null;
  const language = filters.language || null;
  const geography = filters.geography || null;
  const candidateType = filters.candidateType || null;
  const primaryOnly = filters.primary === false || filters.primary === "0" || filters.primary === 0
    ? false
    : String(filters.primary ?? "1") !== "0";

  if (provider) cases = cases.filter((c) => c.provider === provider);
  if (language) cases = cases.filter((c) => c.language === language);
  if (geography) cases = cases.filter((c) => c.geography === geography);
  if (candidateType) cases = cases.filter((c) => c.candidateType === candidateType);
  if (primaryOnly) cases = cases.filter((c) => c.primaryReviewQueue === true);

  if (status === "pending") cases = cases.filter((c) => !c.humanReview);
  else if (status === "reviewed") cases = cases.filter((c) => !!c.humanReview);
  else if (status === "present")
    cases = cases.filter((c) => c.humanReview?.action === "PRESENT");
  else if (status === "not_present")
    cases = cases.filter((c) => c.humanReview?.action === "NOT_PRESENT");
  // status === "all" → no status filter

  const assisted = String(filters.assisted || "all").toLowerCase();
  if (assisted === "agreement") {
    cases = cases.filter(
      (c) => c.assistedProposal && c.assistedDisagreesWithSystem === false
    );
  } else if (assisted === "disagreement") {
    cases = cases.filter((c) => c.assistedDisagreesWithSystem === true);
  } else if (assisted === "none") {
    cases = cases.filter((c) => !c.assistedProposal);
  } else if (assisted === "has" || assisted === "assisted") {
    cases = cases.filter((c) => !!c.assistedProposal);
  } else if (assisted === "manual" || assisted === "manual_review") {
    cases = cases.filter(
      (c) =>
        c.assistedProposal &&
        (c.assistedDisagreesWithSystem === true ||
          c.assistedProposal.proposedDecision === "INVALID" ||
          c.assistedProposal.proposedDecision === "DEFER" ||
          c.bulkApprovalClass === "MANUAL_REVIEW_REQUIRED")
    );
  } else if (assisted === "bulk_eligible") {
    cases = cases.filter((c) => c.bulkApprovalClass === "BULK_APPROVAL_ELIGIBLE");
  }

  return cases;
}

export function loadPresenceValidationCasesForExport() {
  const cand = loadPresenceValidationCandidates();
  if (!cand) return null;
  const reviews = loadPresenceValidationReviews();
  let assistedStore = { proposals: {} };
  try {
    const p = path.join(
      presenceValidationPaths().root,
      "assisted-proposals",
      "assisted-proposals.json"
    );
    if (fs.existsSync(p)) assistedStore = JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    assistedStore = { proposals: {} };
  }

  function mapSys(suggestion) {
    const s = String(suggestion || "").trim().toUpperCase();
    if (s === "YES" || s === "PRESENT" || s === "TRUE") return "PRESENT";
    if (s === "NO" || s === "NOT_PRESENT" || s === "ABSENT" || s === "FALSE") {
      return "NOT_PRESENT";
    }
    return null;
  }

  // Optional classification stamp for filters (lazy, no label writes)
  let eligibleSet = new Set();
  let manualSet = new Set();
  try {
    const cls = classifyAssistedBulkApproval({ useActiveScope: true });
    eligibleSet = new Set(cls.ELIGIBLE_CASE_IDS || []);
    manualSet = new Set(cls.MANUAL_CASE_IDS || []);
  } catch {
    eligibleSet = new Set();
    manualSet = new Set();
  }

  const cases = (cand.cases || []).map((c) => {
    const r = reviews.reviews?.[c.caseId] || null;
    const prop = assistedStore.proposals?.[c.caseId] || null;
    const sysMapped = mapSys(c.SYSTEM_PRESENCE_SUGGESTION);
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
    let bulkApprovalClass = null;
    if (prop) {
      if (eligibleSet.has(c.caseId)) bulkApprovalClass = "BULK_APPROVAL_ELIGIBLE";
      else if (manualSet.has(c.caseId)) bulkApprovalClass = "MANUAL_REVIEW_REQUIRED";
      else if (disagreement) bulkApprovalClass = "MANUAL_REVIEW_REQUIRED";
    }
    return {
      ...c,
      humanReview: r,
      assistance: {
        SYSTEM_PRESENCE_SUGGESTION: c.SYSTEM_PRESENCE_SUGGESTION,
        rationale: c.systemSuggestionRationale,
      },
      assistedProposal: prop
        ? { ...prop, ASSISTED_PROPOSAL_NOT_GROUND_TRUTH: true }
        : null,
      systemSuggestionMapped: sysMapped,
      assistedDisagreesWithSystem: disagreement,
      bulkApprovalClass,
    };
  });
  return { doc: cand, cases };
}

function uniqueResponseCount(cases) {
  return new Set(
    (cases || []).map((c) => c.sourceResponseId || c.responseId).filter(Boolean)
  ).size;
}

function toExportCase(c) {
  return {
    caseId: c.caseId,
    sourceResponseId: c.sourceResponseId || c.responseId || null,
    canonicalEntityId: c.canonicalEntityId || null,
    canonicalEntityName: c.canonicalEntityName || null,
    provider: c.provider || null,
    language: c.language || null,
    geography: c.geography || null,
    promptId: c.promptId || null,
    promptText: c.promptText || null,
    candidateType: c.candidateType || null,
    rawText: c.rawText || "",
    SYSTEM_PRESENCE_SUGGESTION: c.SYSTEM_PRESENCE_SUGGESTION ?? null,
    SYSTEM_SUGGESTION_RATIONALE: c.systemSuggestionRationale ?? c.assistance?.rationale ?? null,
    SYSTEM_SUGGESTION_IS_ASSISTANCE_ONLY: true,
    SYSTEM_SUGGESTION_IS_NOT_HUMAN_GROUND_TRUTH: true,
    QUESTION: "Does this specific canonical entity actually appear in the response?",
    ALLOWED_DECISIONS: [...ALLOWED_PROPOSED_DECISIONS],
    PROPOSED_HUMAN_DECISION: null,
    PROPOSED_NOTES: null,
  };
}

function applyBatchLimit(cases, limit) {
  if (limit == null || limit === "all" || limit === "ALL" || limit === "") {
    return cases;
  }
  const n = Number(limit);
  if (!Number.isFinite(n) || n <= 0) return cases;
  return cases.slice(0, Math.floor(n));
}

/**
 * Build export document (JSON shape). Read-only; optional audit write.
 */
export function buildPresenceValidationReviewExport(options = {}) {
  const loaded = loadPresenceValidationCasesForExport();
  if (!loaded) {
    const err = new Error("CANDIDATES_MISSING");
    err.code = "CANDIDATES_MISSING";
    throw err;
  }

  const mode = String(options.mode || "filter").toLowerCase();
  const filters = {
    status: mode === "pending" ? "pending" : options.status || "pending",
    provider: options.provider || null,
    language: options.language || null,
    geography: options.geography || null,
    candidateType: options.candidateType || null,
    primary: options.primary ?? "1",
  };
  // EXPORT PENDING forces pending regardless of status query
  if (mode === "pending") filters.status = "pending";

  let filtered = filterPresenceValidationCases(loaded.cases, filters);
  // Stable order for reproducible batches
  filtered = filtered.sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));
  const limited = applyBatchLimit(filtered, options.limit);
  const exportCases = limited.map(toExportCase);
  const exportedAt = new Date().toISOString();

  const payload = {
    exportVersion: PRESENCE_VALIDATION_EXPORT_VERSION,
    exportedAt,
    mode: mode === "pending" ? "PENDING" : "CURRENT_FILTER",
    filters,
    caseCount: exportCases.length,
    uniqueResponseCount: uniqueResponseCount(exportCases),
    SYSTEM_SUGGESTION_MARKED_ASSISTANCE_ONLY: true,
    AUTO_LABELING: false,
    ASSISTED_IMPORT_SUPPORTED: false,
    note:
      "SYSTEM SUGGESTION = ASSISTANCE ONLY. NOT HUMAN GROUND TRUTH. Fill PROPOSED_HUMAN_DECISION for drafting only — final labels must be applied in the review UI.",
    ALLOWED_DECISIONS: [...ALLOWED_PROPOSED_DECISIONS],
    cases: exportCases,
  };

  if (options.writeAudit !== false) {
    writeExportAudit(payload);
  }

  return payload;
}

export function formatPresenceValidationExportMarkdown(payload) {
  const lines = [];
  lines.push("# Presence Validation Review Export");
  lines.push("");
  lines.push(`exportVersion: ${payload.exportVersion}`);
  lines.push(`exportedAt: ${payload.exportedAt}`);
  lines.push(`mode: ${payload.mode}`);
  lines.push(`caseCount: ${payload.caseCount}`);
  lines.push(`uniqueResponseCount: ${payload.uniqueResponseCount}`);
  lines.push("");
  lines.push("## HARD RULES");
  lines.push("");
  lines.push("- SYSTEM SUGGESTION = ASSISTANCE ONLY");
  lines.push("- SYSTEM SUGGESTION IS NOT HUMAN GROUND TRUTH");
  lines.push("- Do not treat ChatGPT output as final labels");
  lines.push("- Allowed PROPOSED_HUMAN_DECISION values: PRESENT | NOT_PRESENT | INVALID | DEFER");
  lines.push("- Final labels must be applied explicitly in /ai-intelligence-presence-validation-review");
  lines.push("");
  lines.push("## Filters");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(payload.filters, null, 2));
  lines.push("```");
  lines.push("");

  payload.cases.forEach((c, i) => {
    const n = String(i + 1).padStart(3, "0");
    lines.push("--------------------------------------------------");
    lines.push(`CASE ${n}`);
    lines.push("");
    lines.push(`CASE_ID: ${c.caseId}`);
    lines.push(`SOURCE_RESPONSE_ID: ${c.sourceResponseId || ""}`);
    lines.push("");
    lines.push(`CANONICAL_ENTITY: ${c.canonicalEntityName || ""}`);
    lines.push(`CANONICAL_ENTITY_ID: ${c.canonicalEntityId || ""}`);
    lines.push("");
    lines.push(`PROVIDER: ${c.provider || ""}`);
    lines.push(`LANGUAGE: ${c.language || ""}`);
    lines.push(`GEOGRAPHY: ${c.geography || ""}`);
    lines.push(`CANDIDATE_TYPE: ${c.candidateType || ""}`);
    lines.push(`PROMPT_ID: ${c.promptId || ""}`);
    lines.push("");
    lines.push("PROMPT:");
    lines.push(c.promptText || "");
    lines.push("");
    lines.push("FULL_RESPONSE:");
    lines.push(c.rawText || "");
    lines.push("");
    lines.push("SYSTEM_SUGGESTION:");
    lines.push(String(c.SYSTEM_PRESENCE_SUGGESTION ?? ""));
    lines.push("");
    lines.push("SYSTEM_RATIONALE:");
    lines.push(String(c.SYSTEM_SUGGESTION_RATIONALE ?? ""));
    lines.push("");
    lines.push("SYSTEM_SUGGESTION_IS_ASSISTANCE_ONLY: YES");
    lines.push("SYSTEM_SUGGESTION_IS_NOT_HUMAN_GROUND_TRUTH: YES");
    lines.push("");
    lines.push("QUESTION:");
    lines.push("Does this specific canonical entity actually appear in the response?");
    lines.push("");
    lines.push("ALLOWED_DECISIONS:");
    lines.push("PRESENT");
    lines.push("NOT_PRESENT");
    lines.push("INVALID");
    lines.push("DEFER");
    lines.push("");
    lines.push("PROPOSED_HUMAN_DECISION:");
    lines.push("");
    lines.push("PROPOSED_NOTES:");
    lines.push("");
  });

  lines.push("--------------------------------------------------");
  lines.push("");
  return lines.join("\n");
}

function writeExportAudit(payload) {
  const dir = path.join(
    presenceValidationPaths().root,
    "exports"
  );
  fs.mkdirSync(dir, { recursive: true });
  const stamp = String(payload.exportedAt || new Date().toISOString()).replace(/[:.]/g, "-");
  const audit = {
    exportedAt: payload.exportedAt,
    exportVersion: payload.exportVersion,
    mode: payload.mode,
    filters: payload.filters,
    caseIds: (payload.cases || []).map((c) => c.caseId),
    caseCount: payload.caseCount,
    uniqueResponseCount: payload.uniqueResponseCount,
    AUTO_LABELING: false,
    HUMAN_LABEL_CHANGES: 0,
    GROUND_TRUTH_CHANGES: 0,
  };
  const auditPath = path.join(dir, `export-audit-${stamp}.json`);
  fs.writeFileSync(auditPath, JSON.stringify(audit, null, 2) + "\n", "utf8");
  return auditPath;
}

export function parseExportLimit(raw) {
  if (raw == null || raw === "" || String(raw).toLowerCase() === "all") return "all";
  const n = Number(raw);
  if ([25, 50, 100].includes(n)) return n;
  if (Number.isFinite(n) && n > 0) return Math.floor(n);
  return "all";
}
