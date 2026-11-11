/**
 * Persistent HPC field-research ledger V1.
 * Survives restarts; linked to canonical HPC record + ADP propertyId.
 * Heuristic UNAVAILABLE dispositions are NEVER imported as completed research.
 */

import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  renameSync,
} from "fs";
import { join, dirname } from "path";
import { isFieldEmpty } from "./hpc-full-onboarding-contract-v1.js";
import {
  classifyHpcFieldImportance,
  HPC_FIELD_IMPORTANCE,
  isCriticalHpcBlocker,
} from "./hpc-field-importance-v1.js";

export const HPC_PERSISTENT_FIELD_RESEARCH_LEDGER_V1 =
  "HPC_PERSISTENT_FIELD_RESEARCH_LEDGER_V1";

export const HPC_RESEARCH_STATUS = Object.freeze({
  VERIFIED_COMPLETE: "VERIFIED_COMPLETE",
  RECOVER_EXISTING_DATA: "RECOVER_EXISTING_DATA",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
  RESEARCH_IN_PROGRESS: "RESEARCH_IN_PROGRESS",
  EVIDENCE_FOUND: "EVIDENCE_FOUND",
  SOURCE_CONFLICT: "SOURCE_CONFLICT",
  WAITING_FOR_AUTHORIZATION: "WAITING_FOR_AUTHORIZATION",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  TEMPORARILY_UNAVAILABLE: "TEMPORARILY_UNAVAILABLE",
  VERIFIED_UNAVAILABLE: "VERIFIED_UNAVAILABLE",
  AIRTABLE_WRITE_PENDING: "AIRTABLE_WRITE_PENDING",
  AIRTABLE_READBACK_PENDING: "AIRTABLE_READBACK_PENDING",
  COMPLETED_AND_VERIFIED: "COMPLETED_AND_VERIFIED",
  /** Heuristic disposition from prior CSV — not genuine research */
  HEURISTIC_UNRESOLVED: "HEURISTIC_UNRESOLVED",
});

export const HPC_ENRICHMENT_READINESS = Object.freeze({
  HPC_BLOCKED_CRITICAL: "HPC_BLOCKED_CRITICAL",
  HPC_ADP_READY_WITH_ENRICHMENT_BACKLOG:
    "HPC_ADP_READY_WITH_ENRICHMENT_BACKLOG",
  HPC_FULLY_ENRICHED: "HPC_FULLY_ENRICHED",
  HPC_IDENTITY_UNRESOLVED: "HPC_IDENTITY_UNRESOLVED",
});

/** Retry backoff (ms) by status */
export const HPC_RESEARCH_RETRY_POLICY = Object.freeze({
  RESEARCH_REQUIRED_MS: 0,
  TEMPORARILY_UNAVAILABLE_MS: 7 * 24 * 60 * 60 * 1000,
  HEURISTIC_UNRESOLVED_MS: 14 * 24 * 60 * 60 * 1000,
  SOURCE_CONFLICT_MS: 30 * 24 * 60 * 60 * 1000,
  VERIFIED_UNAVAILABLE_MS: 90 * 24 * 60 * 60 * 1000,
  WAITING_FOR_AUTHORIZATION_MS: 24 * 60 * 60 * 1000,
  maxAttemptsBeforeCooldown: 8,
});

function ledgerRoot(cwd = process.cwd()) {
  return join(cwd, "data/ai-demand-positioning");
}

function indexPath(cwd = process.cwd()) {
  return join(ledgerRoot(cwd), "hpc-field-research-ledger-v1.json");
}

function hotelLedgerPath(propertyId, cwd = process.cwd()) {
  return join(
    ledgerRoot(cwd),
    "hpc-field-research",
    propertyId,
    "latest.json"
  );
}

function emptyIndex() {
  return {
    ledgerVersion: HPC_PERSISTENT_FIELD_RESEARCH_LEDGER_V1,
    updatedAt: null,
    hotels: {},
  };
}

function atomicWriteJson(filePath, doc) {
  mkdirSync(dirname(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.tmp`;
  writeFileSync(tmp, JSON.stringify(doc, null, 2) + "\n", "utf8");
  renameSync(tmp, filePath);
}

export function loadHpcResearchLedgerIndex(cwd = process.cwd()) {
  const p = indexPath(cwd);
  if (!existsSync(p)) return emptyIndex();
  try {
    const doc = JSON.parse(readFileSync(p, "utf8"));
    if (!doc.hotels || typeof doc.hotels !== "object") return emptyIndex();
    return doc;
  } catch {
    return emptyIndex();
  }
}

export function saveHpcResearchLedgerIndex(doc, cwd = process.cwd()) {
  const next = {
    ...doc,
    ledgerVersion: HPC_PERSISTENT_FIELD_RESEARCH_LEDGER_V1,
    updatedAt: new Date().toISOString(),
  };
  atomicWriteJson(indexPath(cwd), next);
  return next;
}

export function loadHotelHpcResearchLedger(propertyId, cwd = process.cwd()) {
  const p = hotelLedgerPath(propertyId, cwd);
  if (!existsSync(p)) return null;
  try {
    return JSON.parse(readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

export function saveHotelHpcResearchLedger(ledger, cwd = process.cwd()) {
  if (!ledger?.propertyId) throw new Error("ledger.propertyId required");
  const next = {
    ...ledger,
    ledgerVersion: HPC_PERSISTENT_FIELD_RESEARCH_LEDGER_V1,
    updatedAt: new Date().toISOString(),
  };
  atomicWriteJson(hotelLedgerPath(ledger.propertyId, cwd), next);
  const index = loadHpcResearchLedgerIndex(cwd);
  index.hotels[ledger.propertyId] = {
    hpcRecordId: next.hpcRecordId || null,
    enrichmentReadiness: next.enrichmentReadiness || null,
    criticalOpen: next.summary?.criticalOpen ?? null,
    importantOpen: next.summary?.importantOpen ?? null,
    enrichmentOpen: next.summary?.enrichmentOpen ?? null,
    lastCycleAt: next.lastCycleAt || next.updatedAt,
    lastResearchAttemptAt: next.lastResearchAttemptAt || null,
  };
  saveHpcResearchLedgerIndex(index, cwd);
  return next;
}

/**
 * @returns {object} field task row
 */
export function createFieldResearchTask({
  fieldName,
  airtableFieldId = null,
  valueType = "unknown",
  currentValue = null,
  importance = null,
  status = HPC_RESEARCH_STATUS.RESEARCH_REQUIRED,
} = {}) {
  const now = new Date().toISOString();
  const imp = importance || classifyHpcFieldImportance(fieldName);
  return {
    fieldName,
    airtableFieldId,
    expectedValueType: valueType,
    currentValue: currentValue ?? null,
    importance: imp,
    importanceToAdp: imp,
    requiredResearchSource: null,
    researchStatus: status,
    lastResearchAttemptAt: null,
    sourcesExamined: [],
    evidenceFound: [],
    evidenceConfidence: null,
    sourceConflicts: [],
    attemptCount: 0,
    lastSuccessfulVerificationAt: null,
    nextResearchAction: status === HPC_RESEARCH_STATUS.VERIFIED_COMPLETE
      ? null
      : "RESEARCH_OR_RECOVER",
    nextEligibleRetryAt: now,
    blockingReason: null,
    resolutionStatus: status,
    provenance: {
      importedFromHeuristicUnavailable: false,
      genuineResearchEvidence: false,
    },
  };
}

export function isTerminalCompleteStatus(status) {
  return (
    status === HPC_RESEARCH_STATUS.VERIFIED_COMPLETE ||
    status === HPC_RESEARCH_STATUS.COMPLETED_AND_VERIFIED ||
    status === HPC_RESEARCH_STATUS.NOT_APPLICABLE
  );
}

export function isEligibleForResearchAttempt(task, nowMs = Date.now()) {
  if (!task) return false;
  if (isTerminalCompleteStatus(task.researchStatus)) return false;
  if (task.researchStatus === HPC_RESEARCH_STATUS.VERIFIED_UNAVAILABLE) {
    const next = Date.parse(task.nextEligibleRetryAt || 0);
    return Number.isFinite(next) ? nowMs >= next : false;
  }
  if (
    task.researchStatus === HPC_RESEARCH_STATUS.WAITING_FOR_AUTHORIZATION
  ) {
    return false;
  }
  const next = Date.parse(task.nextEligibleRetryAt || 0);
  if (Number.isFinite(next) && nowMs < next) return false;
  return true;
}

export function nextRetryAtForStatus(status, fromMs = Date.now()) {
  const p = HPC_RESEARCH_RETRY_POLICY;
  let delta = p.RESEARCH_REQUIRED_MS;
  if (status === HPC_RESEARCH_STATUS.TEMPORARILY_UNAVAILABLE) {
    delta = p.TEMPORARILY_UNAVAILABLE_MS;
  } else if (status === HPC_RESEARCH_STATUS.HEURISTIC_UNRESOLVED) {
    delta = p.HEURISTIC_UNRESOLVED_MS;
  } else if (status === HPC_RESEARCH_STATUS.SOURCE_CONFLICT) {
    delta = p.SOURCE_CONFLICT_MS;
  } else if (status === HPC_RESEARCH_STATUS.VERIFIED_UNAVAILABLE) {
    delta = p.VERIFIED_UNAVAILABLE_MS;
  } else if (status === HPC_RESEARCH_STATUS.WAITING_FOR_AUTHORIZATION) {
    delta = p.WAITING_FOR_AUTHORIZATION_MS;
  }
  return new Date(fromMs + delta).toISOString();
}

/**
 * Record a real research attempt (only when sources were actually examined).
 */
export function recordResearchAttempt(task, attempt = {}) {
  const now = new Date().toISOString();
  const sources = Array.isArray(attempt.sourcesExamined)
    ? attempt.sourcesExamined
    : attempt.sourceUrl
      ? [attempt.sourceUrl]
      : [];
  const next = {
    ...task,
    lastResearchAttemptAt: now,
    attemptCount: (task.attemptCount || 0) + 1,
    sourcesExamined: uniqueStrings([
      ...(task.sourcesExamined || []),
      ...sources,
    ]),
    evidenceFound: uniqueStrings([
      ...(task.evidenceFound || []),
      ...(attempt.evidenceFound || []),
      ...(attempt.finding ? [attempt.finding] : []),
    ]),
    evidenceConfidence:
      attempt.evidenceConfidence ?? task.evidenceConfidence ?? null,
    sourceConflicts: uniqueStrings([
      ...(task.sourceConflicts || []),
      ...(attempt.sourceConflicts || []),
    ]),
    blockingReason: attempt.blockingReason ?? task.blockingReason ?? null,
    researchStatus: attempt.status || task.researchStatus,
    resolutionStatus: attempt.status || task.resolutionStatus,
    nextResearchAction: attempt.nextResearchAction ?? task.nextResearchAction,
    nextEligibleRetryAt: nextRetryAtForStatus(
      attempt.status || task.researchStatus,
      Date.now()
    ),
    provenance: {
      ...(task.provenance || {}),
      genuineResearchEvidence:
        (task.provenance?.genuineResearchEvidence === true) ||
        sources.length > 0 ||
        !!(attempt.evidenceFound && attempt.evidenceFound.length),
      importedFromHeuristicUnavailable: false,
    },
  };
  return next;
}

function uniqueStrings(arr) {
  return [...new Set((arr || []).map((s) => String(s).trim()).filter(Boolean))];
}

/**
 * Reconcile ledger tasks with live Airtable field values.
 */
export function reconcileLedgerWithLiveRecord({
  ledger,
  liveFields = {},
  schemaFields = [],
} = {}) {
  const byName = Object.fromEntries(
    (schemaFields || []).map((f) => [f.name, f])
  );
  const tasks = { ...(ledger.fields || {}) };
  const schemaNames = (schemaFields || []).map((f) => f.name).filter(Boolean);
  const names =
    schemaNames.length > 0
      ? schemaNames
      : [...new Set([...Object.keys(tasks), ...Object.keys(liveFields || {})])];

  for (const fieldName of names) {
    const schemaField = byName[fieldName];
    if (schemaField?.computed || schemaField?.writable === false) {
      if (tasks[fieldName]) {
        tasks[fieldName] = {
          ...tasks[fieldName],
          researchStatus: HPC_RESEARCH_STATUS.NOT_APPLICABLE,
          resolutionStatus: HPC_RESEARCH_STATUS.NOT_APPLICABLE,
          blockingReason: "COMPUTED_OR_NON_WRITABLE",
          nextResearchAction: null,
        };
      }
      continue;
    }
    const liveVal = liveFields?.[fieldName];
    const existing = tasks[fieldName] || createFieldResearchTask({
      fieldName,
      airtableFieldId: schemaField?.id || null,
      valueType: schemaField?.type || "unknown",
      currentValue: liveVal ?? null,
    });
    const populated = !isFieldEmpty(liveVal) || liveVal === false || liveVal === 0;
    if (populated) {
      const valueChanged =
        existing.currentValue != null &&
        String(existing.currentValue) !== String(liveVal);
      tasks[fieldName] = {
        ...existing,
        airtableFieldId: schemaField?.id || existing.airtableFieldId,
        expectedValueType: schemaField?.type || existing.expectedValueType,
        previousValue: valueChanged ? existing.currentValue : existing.previousValue,
        currentValue: liveVal,
        researchStatus: HPC_RESEARCH_STATUS.COMPLETED_AND_VERIFIED,
        resolutionStatus: HPC_RESEARCH_STATUS.COMPLETED_AND_VERIFIED,
        lastSuccessfulVerificationAt: new Date().toISOString(),
        nextResearchAction: null,
        blockingReason: null,
        valueChangeDetected: valueChanged === true,
      };
    } else {
      // Keep prior research memory; do not wipe sourcesExamined
      const status =
        existing.researchStatus === HPC_RESEARCH_STATUS.COMPLETED_AND_VERIFIED ||
        existing.researchStatus === HPC_RESEARCH_STATUS.VERIFIED_COMPLETE
          ? HPC_RESEARCH_STATUS.RESEARCH_REQUIRED
          : existing.researchStatus === HPC_RESEARCH_STATUS.HEURISTIC_UNRESOLVED
            ? HPC_RESEARCH_STATUS.HEURISTIC_UNRESOLVED
            : existing.researchStatus || HPC_RESEARCH_STATUS.RESEARCH_REQUIRED;
      tasks[fieldName] = {
        ...existing,
        airtableFieldId: schemaField?.id || existing.airtableFieldId,
        expectedValueType: schemaField?.type || existing.expectedValueType,
        currentValue: null,
        researchStatus: isTerminalCompleteStatus(status)
          ? HPC_RESEARCH_STATUS.RESEARCH_REQUIRED
          : status,
        resolutionStatus: status,
        importance: classifyHpcFieldImportance(fieldName),
        nextResearchAction: existing.nextResearchAction || "RESEARCH_OR_RECOVER",
        nextEligibleRetryAt:
          existing.nextEligibleRetryAt || new Date().toISOString(),
      };
    }
  }

  return summarizeHotelLedger({
    ...ledger,
    fields: tasks,
    lastReconciledAt: new Date().toISOString(),
  });
}

export function summarizeHotelLedger(ledger) {
  const fields = ledger.fields || {};
  let criticalOpen = 0;
  let importantOpen = 0;
  let enrichmentOpen = 0;
  let heuristicUnresolved = 0;
  let verifiedUnavailable = 0;
  let completed = 0;
  const criticalBlockers = [];
  const importantMissing = [];
  const enrichmentBacklog = [];

  for (const [name, task] of Object.entries(fields)) {
    if (isTerminalCompleteStatus(task.researchStatus)) {
      completed += 1;
      continue;
    }
    if (task.researchStatus === HPC_RESEARCH_STATUS.HEURISTIC_UNRESOLVED) {
      heuristicUnresolved += 1;
    }
    if (task.researchStatus === HPC_RESEARCH_STATUS.VERIFIED_UNAVAILABLE) {
      verifiedUnavailable += 1;
    }
    const imp = task.importance || classifyHpcFieldImportance(name);
    if (imp === HPC_FIELD_IMPORTANCE.CRITICAL) {
      criticalOpen += 1;
      criticalBlockers.push(name);
    } else if (imp === HPC_FIELD_IMPORTANCE.IMPORTANT) {
      importantOpen += 1;
      importantMissing.push(name);
    } else if (
      imp === HPC_FIELD_IMPORTANCE.ENRICHMENT ||
      imp === HPC_FIELD_IMPORTANCE.INTERNAL_WORKFLOW ||
      imp === HPC_FIELD_IMPORTANCE.PROVIDER_SPECIFIC
    ) {
      enrichmentOpen += 1;
      enrichmentBacklog.push(name);
    }
  }

  let enrichmentReadiness = HPC_ENRICHMENT_READINESS.HPC_FULLY_ENRICHED;
  if (!ledger.hpcRecordId) {
    enrichmentReadiness = HPC_ENRICHMENT_READINESS.HPC_IDENTITY_UNRESOLVED;
  } else if (criticalOpen > 0) {
    enrichmentReadiness = HPC_ENRICHMENT_READINESS.HPC_BLOCKED_CRITICAL;
  } else if (importantOpen + enrichmentOpen > 0) {
    enrichmentReadiness =
      HPC_ENRICHMENT_READINESS.HPC_ADP_READY_WITH_ENRICHMENT_BACKLOG;
  }

  return {
    ...ledger,
    enrichmentReadiness,
    summary: {
      fieldCount: Object.keys(fields).length,
      completed,
      criticalOpen,
      importantOpen,
      enrichmentOpen,
      heuristicUnresolved,
      verifiedUnavailable,
      criticalBlockers,
      importantMissing: importantMissing.slice(0, 40),
      enrichmentBacklog: enrichmentBacklog.slice(0, 80),
    },
  };
}

/**
 * Import heuristic UNAVAILABLE rows without marking them as completed research.
 */
export function importHeuristicUnresolved({
  ledger,
  fieldName,
  reason = null,
  sourcePass = null,
} = {}) {
  const existing =
    ledger.fields?.[fieldName] ||
    createFieldResearchTask({
      fieldName,
      status: HPC_RESEARCH_STATUS.HEURISTIC_UNRESOLVED,
    });
  if (isTerminalCompleteStatus(existing.researchStatus)) {
    return ledger;
  }
  const task = {
    ...existing,
    researchStatus: HPC_RESEARCH_STATUS.HEURISTIC_UNRESOLVED,
    resolutionStatus: HPC_RESEARCH_STATUS.HEURISTIC_UNRESOLVED,
    blockingReason: reason || existing.blockingReason || "HEURISTIC_DISPOSITION_PRIOR_PASS",
    nextEligibleRetryAt: nextRetryAtForStatus(
      HPC_RESEARCH_STATUS.HEURISTIC_UNRESOLVED
    ),
    nextResearchAction: "ELIGIBLE_AFTER_RETRY_WINDOW_OR_NEW_EVIDENCE",
    provenance: {
      ...(existing.provenance || {}),
      importedFromHeuristicUnavailable: true,
      genuineResearchEvidence: false,
      sourcePass: sourcePass || "dealality-hpc-remaining-field-closure-v1",
    },
  };
  return summarizeHotelLedger({
    ...ledger,
    fields: { ...(ledger.fields || {}), [fieldName]: task },
  });
}

/**
 * Mark field complete after authorized write + matching readback.
 */
export function markFieldCompletedAndVerified(task, { value, sourceUrl, confidence } = {}) {
  const now = new Date().toISOString();
  return {
    ...task,
    currentValue: value,
    researchStatus: HPC_RESEARCH_STATUS.COMPLETED_AND_VERIFIED,
    resolutionStatus: HPC_RESEARCH_STATUS.COMPLETED_AND_VERIFIED,
    lastSuccessfulVerificationAt: now,
    nextResearchAction: null,
    blockingReason: null,
    evidenceConfidence: confidence ?? task.evidenceConfidence,
    sourcesExamined: uniqueStrings([
      ...(task.sourcesExamined || []),
      ...(sourceUrl ? [sourceUrl] : []),
    ]),
    provenance: {
      ...(task.provenance || {}),
      genuineResearchEvidence: true,
      importedFromHeuristicUnavailable: false,
      airtableReadbackVerified: true,
    },
  };
}

/**
 * Compact AI Admin / status drawer projection.
 */
export function hotelLedgerAdminProjection(propertyId, cwd = process.cwd()) {
  const index = loadHpcResearchLedgerIndex(cwd);
  const summary = index.hotels?.[propertyId] || null;
  const full = loadHotelHpcResearchLedger(propertyId, cwd);
  return {
    propertyId,
    hpcRecordId: full?.hpcRecordId || summary?.hpcRecordId || null,
    enrichmentReadiness:
      full?.enrichmentReadiness || summary?.enrichmentReadiness || null,
    criticalOpen: full?.summary?.criticalOpen ?? summary?.criticalOpen ?? null,
    importantOpen: full?.summary?.importantOpen ?? summary?.importantOpen ?? null,
    enrichmentOpen:
      full?.summary?.enrichmentOpen ?? summary?.enrichmentOpen ?? null,
    criticalBlockers: full?.summary?.criticalBlockers || [],
    importantMissing: (full?.summary?.importantMissing || []).slice(0, 12),
    enrichmentBacklogPreview: (full?.summary?.enrichmentBacklog || []).slice(
      0,
      12
    ),
    lastCycleAt: full?.lastCycleAt || summary?.lastCycleAt || null,
    lastResearchAttemptAt:
      full?.lastResearchAttemptAt || summary?.lastResearchAttemptAt || null,
    heuristicUnresolved: full?.summary?.heuristicUnresolved ?? null,
    nextEligibleAction: deriveNextAdminAction(full),
  };
}

function deriveNextAdminAction(ledger) {
  if (!ledger) return "INITIALIZE_HPC_RESEARCH_LEDGER";
  if (ledger.enrichmentReadiness === HPC_ENRICHMENT_READINESS.HPC_BLOCKED_CRITICAL) {
    return "RESOLVE_CRITICAL_HPC_FIELDS";
  }
  if (
    ledger.enrichmentReadiness ===
    HPC_ENRICHMENT_READINESS.HPC_ADP_READY_WITH_ENRICHMENT_BACKLOG
  ) {
    return "RESUME_ENRICHMENT_BACKLOG";
  }
  if (ledger.enrichmentReadiness === HPC_ENRICHMENT_READINESS.HPC_FULLY_ENRICHED) {
    return "NONE_HPC_ENRICHED";
  }
  return "RESOLVE_HPC_IDENTITY";
}

export function emptyHotelLedger({ propertyId, hpcRecordId = null, hotelName = null } = {}) {
  return summarizeHotelLedger({
    ledgerVersion: HPC_PERSISTENT_FIELD_RESEARCH_LEDGER_V1,
    propertyId,
    hpcRecordId,
    hotelName,
    fields: {},
    lastCycleAt: null,
    lastResearchAttemptAt: null,
    cycleHistory: [],
  });
}

export { isCriticalHpcBlocker };
