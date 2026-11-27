#!/usr/bin/env node
/**
 * $0 — Reconcile Wyndham paid-live 161 discoveries into a commercial research-
 * completion queue + spend plan. NO_PAID_RESEARCH.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildCommercialResearchCompletionQueue } from "../lib/group-demand-intelligence/research/gdi-commercial-research-completion-queue-v1.js";
import { DEEP_RESEARCH_OPERATING_MODE } from "../lib/group-demand-intelligence/research/gdi-commercial-deep-research-operating-modes-v1.js";
import { quoteExaAgentRun, EXA_AGENT_EFFORT } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";
import { mapToCommercialScreeningDisposition } from "../lib/group-demand-intelligence/research/gdi-commercial-deep-research-screening-dispositions-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const CERT =
  path.join(ROOT, "reports/gdi-wyndham-sd-bayside-paid-live-cert-25usd-v1");
const OUT = path.join(
  ROOT,
  "reports/gdi-wyndham-commercial-full-e2e-completion-plan-v1",
);

const matrix = JSON.parse(
  fs.readFileSync(path.join(CERT, "fourteen-segment-discovery-matrix.json"), "utf8"),
);
const screen = JSON.parse(
  fs.readFileSync(path.join(CERT, "pre-deep-candidate-screening.json"), "utf8"),
);
const selection = JSON.parse(
  fs.readFileSync(path.join(CERT, "deep-research-selection-register.json"), "utf8"),
);
const ledger = JSON.parse(
  fs.readFileSync(path.join(CERT, "paid-budget-ledger.json"), "utf8"),
);

const discoveryCandidates = [];
for (const cell of matrix.cells || []) {
  for (const c of cell.candidates || []) {
    discoveryCandidates.push({
      id: c.candidateId || c.id,
      opportunityId: c.candidateId || c.id,
      organizationName: c.organizationName,
      title: c.title,
      researchSegment: cell.segmentId,
      segmentId: cell.segmentId,
      hotelId: "recnPB3Q02NO8Ma1o",
      preDeepScreen: null,
    });
  }
}

const completedDeepIds = [
  ...new Set(
    (ledger.operations || [])
      .filter(
        (op) =>
          op.stage === "DEEP_RESEARCH" ||
          op.kind === "EXA_AGENT_DEEP_RESEARCH",
      )
      .map((op) => op.opportunityId)
      .filter(Boolean),
  ),
];

const queue = buildCommercialResearchCompletionQueue({
  hotelId: "recnPB3Q02NO8Ma1o",
  operatingMode: DEEP_RESEARCH_OPERATING_MODE.COMMERCIAL_FULL_E2E,
  discoveryCandidates,
  preDeepDecisions: screen.decisions || [],
  selectionRegister: selection,
  completedDeepIds,
  paidAuthorized: false, // plan only
  batchSize: 20,
});

const unit = Number(quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM }).maxUsd || 0.1);
const remainingDeep = queue.pendingPaidDeepCount;
// Exclude already completed from remaining
const remainingAfterCert = queue.rows.filter(
  (r) =>
    r.paidDeepRequired &&
    r.queueState !== "COMPLETED_REUSE" &&
    !completedDeepIds.includes(r.opportunityId),
).length;

const bySegment = {};
for (const r of queue.rows) {
  const seg = r.segmentId || "unknown";
  if (!bySegment[seg]) {
    bySegment[seg] = {
      total: 0,
      active: 0,
      pendingDeep: 0,
      completedReuse: 0,
      duplicate: 0,
    };
  }
  bySegment[seg].total += 1;
  if (r.activeQueue) bySegment[seg].active += 1;
  if (r.paidDeepRequired && !completedDeepIds.includes(r.opportunityId)) {
    bySegment[seg].pendingDeep += 1;
  }
  if (completedDeepIds.includes(r.opportunityId)) bySegment[seg].completedReuse += 1;
  if (r.disposition === "DUPLICATE_REUSE") bySegment[seg].duplicate += 1;
}

const batchSize = 20;
const batches = Math.ceil(remainingAfterCert / batchSize) || 0;
const expectedSpendUsd = Number((remainingAfterCert * unit).toFixed(2));

// Lossless unique disposition register
const uniqueRegister = queue.rows.map((r) => ({
  opportunityId: r.opportunityId,
  organizationName: r.organizationName,
  title: r.title,
  segmentId: r.segmentId,
  preDeepOutcome: r.preDeepOutcome,
  selectionDecision: r.selectionDecision,
  disposition: r.disposition,
  activeQueue: r.activeQueue,
  paidDeepRequired: r.paidDeepRequired,
  deepAlreadyComplete: completedDeepIds.includes(r.opportunityId),
  dispositionReason: r.dispositionReason,
}));

const accounting = {
  discoveredUnique: discoveryCandidates.length,
  screenedDecisions: (screen.decisions || []).length,
  uniqueScreenIds: new Set((screen.decisions || []).map((d) => d.candidateId)).size,
  duplicateNoNewEvidenceUnique: (screen.outcomeCounts?.DUPLICATE_NO_NEW_EVIDENCE || 0),
  // Note: outcomeCounts may count remaps; unique dupes from final decisions:
  uniqueFinalDuplicate: uniqueRegister.filter((r) => r.disposition === "DUPLICATE_REUSE").length,
  eligibleBeforeCrossSegment: screen.eligibleForDeepCountBeforeCrossSegmentDedupe,
  crossSegmentSuppressed: screen.crossSegmentDedupe?.suppressedCount || 0,
  eligibleAfterCrossSegment: screen.eligibleForDeepCount,
  deepSelectedThisCert: selection.selectedCount,
  deferredThisCert: selection.deferredCount,
  founderGapExplained: {
    formula: "161 = 32 DUPLICATE_REUSE (final unique) + 129 eligible; 129 = 8 selected + 121 deferred",
    duplicatePopulation: 32,
    selected: 8,
    deferred: 121,
    overlapNote:
      "The 32 are NOT in the 8 or 121. screenedCount 172 = 161 unique + 11 cross-segment remap decision rows.",
  },
};

const plan = {
  version: "gdi-wyndham-commercial-full-e2e-completion-plan-v1",
  at: new Date().toISOString(),
  hotelId: "recnPB3Q02NO8Ma1o",
  hotelName: "Wyndham San Diego Bayside",
  sourceCertDir: CERT,
  noPaidResearch: true,
  accounting,
  queueSummary: {
    totalUniqueCandidates: queue.totalUniqueCandidates,
    dispositionCounts: queue.dispositionCounts,
    alreadyDeepResearched: completedDeepIds.length,
    remainingFullDeepResearch: remainingAfterCert,
    partialRefresh: 0,
    competitiveCommittedMonitor: queue.dispositionCounts.VERIFIED_COMPETITOR_COMMITTED || 0,
    historicalOrIrrelevant:
      (queue.dispositionCounts.HISTORICAL_MONITOR || 0) +
      (queue.dispositionCounts.NOT_RELEVANT_WITH_EVIDENCE || 0) +
      (queue.dispositionCounts.DUPLICATE_REUSE || 0) +
      (queue.dispositionCounts.INSUFFICIENT_INITIAL_EVIDENCE || 0),
    activeQueue: queue.activeQueueCount,
  },
  economics: {
    quoteUnitMaxUsd: unit,
    effort: EXA_AGENT_EFFORT.MEDIUM,
    remainingDeepTargets: remainingAfterCert,
    expectedAgentSpendUsd: expectedSpendUsd,
    suggestedBatchSize: batchSize,
    estimatedBatchCount: batches,
    note: "Estimate assumes one medium Exa Agent deep per remaining eligible unique candidate. Contact-only refresh may be cheaper. Does not guarantee FQO yield.",
    commercialQualificationPotential:
      "Unknown — strict six-rule FQO after deep; prior cert converted 1/8 (12.5%) of sampled deeps. Do not claim 25–35 FQO as a cap or guarantee.",
  },
  bySegment,
  nextBatchIds: queue.nextBatchIds,
  completedDeepIds,
  completion: queue.completion,
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "01-LOSSLESS_CANDIDATE_REGISTER.json"),
  `${JSON.stringify({ version: "gdi-wyndham-lossless-candidate-register-v1", accounting, rows: uniqueRegister }, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "02-COMMERCIAL_RESEARCH_COMPLETION_QUEUE.json"),
  `${JSON.stringify(queue, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "03-COMPLETION_PLAN.json"),
  `${JSON.stringify(plan, null, 2)}\n`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      out: OUT,
      discovered: accounting.discoveredUnique,
      duplicate32: accounting.founderGapExplained.duplicatePopulation,
      eligible129: accounting.eligibleAfterCrossSegment,
      selected8: accounting.deepSelectedThisCert,
      deferred121: accounting.deferredThisCert,
      remainingDeep: remainingAfterCert,
      expectedSpendUsd,
      batches,
    },
    null,
    2,
  ),
);
