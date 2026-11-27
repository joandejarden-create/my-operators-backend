/**
 * Governed commercial research-completion queue.
 * Persists maturity + queue state; resumes unfinished cases; never marks COMPLETE
 * while eligible unresolved candidates remain.
 */

import fs from "node:fs";
import path from "node:path";
import {
  COMMERCIAL_SCREENING_DISPOSITION,
  ACTIVE_RESEARCH_QUEUE_DISPOSITIONS,
  mapToCommercialScreeningDisposition,
} from "./gdi-commercial-deep-research-screening-dispositions-v1.js";
import {
  DEEP_RESEARCH_OPERATING_MODE,
  deriveResearchCompletionStatus,
} from "./gdi-commercial-deep-research-operating-modes-v1.js";
import { SELECTION_DECISION } from "./gdi-deep-research-selection-register-v1.js";

export const GDI_COMMERCIAL_RESEARCH_COMPLETION_QUEUE_V1 =
  "gdi-commercial-research-completion-queue-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Build a lossless queue row for one unique candidate.
 */
export function buildQueueRow({
  candidate = {},
  preDeep = null,
  selection = null,
  deepAlreadyComplete = false,
  deepPartialRefreshNeeded = false,
  competitorCommittedVerified = false,
} = {}) {
  const id = candidate.id || candidate.opportunityId || candidate.candidateId || null;
  const mapped = mapToCommercialScreeningDisposition({
    preDeepOutcome: preDeep?.outcome || candidate.preDeepScreen?.outcome || null,
    pursuitDecision:
      preDeep?.pursuitDecision || candidate.preDeepScreen?.pursuitDecision || null,
    selectionDecision: selection?.selectionDecision || null,
    deepAlreadyComplete,
    deepPartialRefreshNeeded,
    competitorCommittedVerified,
    researchContinuation:
      preDeep?.outcome === "ELIGIBLE_FOR_TARGETED_RESEARCH" ||
      selection?.selectionDecision === SELECTION_DECISION.SELECTED_FOR_TARGETED_TIER3,
  });
  return {
    opportunityId: id,
    organizationName: text(candidate.organizationName) || null,
    title: text(candidate.title || candidate.programmeName) || null,
    segmentId: candidate.researchSegment || candidate.segmentId || null,
    preDeepOutcome: preDeep?.outcome || candidate.preDeepScreen?.outcome || null,
    selectionDecision: selection?.selectionDecision || null,
    selectionScore: selection?.preliminaryScore ?? null,
    disposition: mapped.disposition,
    activeQueue: mapped.activeQueue === true,
    paidDeepRequired: mapped.paidDeepRequired === true,
    dispositionReason: mapped.reason,
    deepAlreadyComplete: deepAlreadyComplete === true,
    queueState: deepAlreadyComplete
      ? "COMPLETED_REUSE"
      : mapped.activeQueue
        ? selection?.selectionDecision?.startsWith("SELECTED")
          ? "SELECTED_THIS_BATCH"
          : "PENDING"
        : "INACTIVE",
  };
}

/**
 * Assemble full queue from screening + selection register + optional deep-complete set.
 */
export function buildCommercialResearchCompletionQueue({
  hotelId = null,
  operatingMode = DEEP_RESEARCH_OPERATING_MODE.COMMERCIAL_FULL_E2E,
  discoveryCandidates = [],
  preDeepDecisions = [],
  selectionRegister = null,
  completedDeepIds = [],
  partialRefreshIds = [],
  competitorCommittedIds = [],
  budgetExhausted = false,
  paidAuthorized = true,
  batchSize = null,
} = {}) {
  const completed = new Set((completedDeepIds || []).map(String));
  const partial = new Set((partialRefreshIds || []).map(String));
  const competitor = new Set((competitorCommittedIds || []).map(String));
  const preById = new Map();
  for (const d of preDeepDecisions || []) {
    const id = String(d.candidateId || d.opportunityId || "");
    if (!id) continue;
    // Prefer last decision (cross-seg remaps write DUPLICATE after ELIGIBLE).
    preById.set(id, d);
  }
  const selById = new Map();
  for (const row of selectionRegister?.register || []) {
    const id = String(row.candidateId || row.opportunity?.id || "");
    if (id) selById.set(id, row);
  }

  const unique = new Map();
  for (const c of discoveryCandidates || []) {
    const id = String(c.id || c.opportunityId || c.candidateId || "");
    if (id && !unique.has(id)) unique.set(id, c);
  }
  // Also include any decision-only IDs (should not happen for lossless discovery).
  for (const id of preById.keys()) {
    if (!unique.has(id)) {
      unique.set(id, {
        id,
        organizationName: preById.get(id)?.organizationName,
        segmentId: preById.get(id)?.segmentId,
      });
    }
  }

  const rows = [...unique.values()].map((c) => {
    const id = String(c.id || c.opportunityId || c.candidateId || "");
    return buildQueueRow({
      candidate: c,
      preDeep: preById.get(id) || c.preDeepScreen || null,
      selection: selById.get(id) || null,
      deepAlreadyComplete: completed.has(id),
      deepPartialRefreshNeeded: partial.has(id),
      competitorCommittedVerified: competitor.has(id),
    });
  });

  const byDisposition = {};
  for (const r of rows) {
    byDisposition[r.disposition] = (byDisposition[r.disposition] || 0) + 1;
  }

  const active = rows.filter((r) => r.activeQueue);
  const pendingPaid = active.filter(
    (r) => r.paidDeepRequired && r.queueState !== "COMPLETED_REUSE",
  );
  const deferredEligible = pendingPaid.filter(
    (r) => r.queueState === "PENDING" || r.queueState === "SELECTED_THIS_BATCH",
  );

  // Rank pending by selection score then source order for next batch.
  const rankedPending = pendingPaid
    .slice()
    .sort((a, b) => {
      const as = Number(a.selectionScore || 0);
      const bs = Number(b.selectionScore || 0);
      if (bs !== as) return bs - as;
      return String(a.opportunityId).localeCompare(String(b.opportunityId));
    });

  const nextBatch =
    batchSize != null && Number.isFinite(Number(batchSize)) && Number(batchSize) > 0
      ? rankedPending.slice(0, Number(batchSize))
      : rankedPending;

  const completion = deriveResearchCompletionStatus({
    operatingMode,
    eligibleCount: active.length,
    deepCompletedCount: rows.filter((r) => completed.has(String(r.opportunityId))).length,
    deferredEligibleCount: rankedPending.length,
    budgetExhausted,
    paidAuthorized,
  });

  // Certification sample must never be reported as commercial COMPLETE.
  if (
    operatingMode === DEEP_RESEARCH_OPERATING_MODE.CERTIFICATION_SAMPLE &&
    rankedPending.length > 0
  ) {
    completion.comprehensiveResearchComplete = false;
    completion.certificationSampleOnly = true;
    if (completion.status === "COMPLETE") {
      completion.status = "PARTIAL_QUEUE_REMAINING";
      completion.reason = "certification_sample_eligible_queue_remains";
    }
  }

  return {
    version: GDI_COMMERCIAL_RESEARCH_COMPLETION_QUEUE_V1,
    at: new Date().toISOString(),
    hotelId,
    operatingMode,
    totalUniqueCandidates: rows.length,
    dispositionCounts: byDisposition,
    activeQueueCount: active.length,
    pendingPaidDeepCount: rankedPending.length,
    completedReuseCount: rows.filter(
      (r) => r.disposition === COMMERCIAL_SCREENING_DISPOSITION.DEEP_RESEARCH_ALREADY_COMPLETE_REUSE,
    ).length,
    duplicateReuseCount: rows.filter(
      (r) => r.disposition === COMMERCIAL_SCREENING_DISPOSITION.DUPLICATE_REUSE,
    ).length,
    nextBatchIds: nextBatch.map((r) => r.opportunityId),
    nextBatchCount: nextBatch.length,
    completion,
    activeDispositionSet: ACTIVE_RESEARCH_QUEUE_DISPOSITIONS,
    rows,
  };
}

export function writeCommercialResearchCompletionQueue(queue, outPath) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, `${JSON.stringify(queue, null, 2)}\n`);
  return outPath;
}

/**
 * Research-completeness reporting matrix (leisure / CAD / FQO honesty).
 */
export function buildResearchCompletenessReport({
  standardStageAssessed = false,
  liveResearchExecuted = false,
  deepResearchCompletedCount = 0,
  deepResearchRequiredCount = 0,
  competitiveClassificationCompleted = false,
  strictFqoCompleted = false,
  remainingResearchContinuation = 0,
  providerAuthorizationPending = false,
  leisureMode = null,
  predictiveSourcingStatus = null,
  operatingMode = null,
  comprehensiveResearchComplete = false,
} = {}) {
  return {
    version: "gdi-research-completeness-report-v1",
    operatingMode,
    standardStageAssessed: standardStageAssessed === true,
    liveResearchExecuted: liveResearchExecuted === true,
    deepResearchCompletedCount: Number(deepResearchCompletedCount || 0),
    deepResearchRequiredCount: Number(deepResearchRequiredCount || 0),
    competitiveClassificationCompleted: competitiveClassificationCompleted === true,
    strictFqoCompleted: strictFqoCompleted === true,
    remainingResearchContinuation: Number(remainingResearchContinuation || 0),
    providerAuthorizationPending: providerAuthorizationPending === true,
    leisureMode: leisureMode || null,
    leisureLivePaid:
      leisureMode === "live" || leisureMode === "paid_live" || leisureMode === "LIVE",
    leisureMockOnly: /mock/i.test(String(leisureMode || "")),
    predictiveSourcingStatus: predictiveSourcingStatus || null,
    comprehensiveResearchComplete: comprehensiveResearchComplete === true,
    notes: [
      leisureMode && /mock/i.test(String(leisureMode))
        ? "Applicable leisure remains mock_live — do not claim full commercial Full E2E completion."
        : null,
      predictiveSourcingStatus === "PARTIAL" ||
      predictiveSourcingStatus === "PARTIAL_THROUGH_CAD_AND_DEEP_RESEARCH"
        ? "CAD classification alone is not complete predictive sourcing."
        : null,
      comprehensiveResearchComplete !== true
        ? "Comprehensive commercial research not complete while eligible unresolved candidates remain."
        : null,
    ].filter(Boolean),
  };
}

export default {
  GDI_COMMERCIAL_RESEARCH_COMPLETION_QUEUE_V1,
  buildQueueRow,
  buildCommercialResearchCompletionQueue,
  writeCommercialResearchCompletionQueue,
  buildResearchCompletenessReport,
};
