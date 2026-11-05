/**
 * Shared aggregate $20 campaign budget — Waterstone + Chapultepec live FullE2E.
 *
 * Soft segment allocations sum to exactly $20. Children only receive atomically
 * reserved slices from ONE authoritative aggregate ledger. Proposed until founder
 * authorization; refuse paid execution without explicit GO.
 *
 * Reuses deep-research ledger primitives (reserve/commit/fail/cancel) and the
 * same settle/hold semantics as the Bethesda/Wyndham operational campaign.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertLiveDeepResearchLedger,
  loadOrCreateLiveDeepResearchLedger,
  reserveLiveDeepResearchSpend,
  commitLiveDeepResearchSpend,
  failLiveDeepResearchSpend,
  cancelLiveDeepResearchSpend,
  withLiveDeepResearchLedgerLock,
} from "./deep-research-gdi-live-certification-v1.js";
import { verifyChildProviderBudgetEnforcementActive as verifyChildProviderBudgetEnforcementActiveOps } from "./operational-campaign-shared-budget-v1.js";

export const WATERSTONE_CHAPULTEPEC_SHARED_BUDGET_V1 =
  "gdi-waterstone-chapultepec-shared-budget-v1";

export const AGGREGATE_CEILING_USD = 20;

export const WATERSTONE_HOTEL_ID = "recgMYovrrZDJMqzX";
export const CHAPULTEPEC_HOTEL_ID = "recterKmNPdntCm0v";

export const FOUNDER_APPROVAL_PHRASE =
  "AUTHORIZED: Waterstone + Grand Fiesta Americana Chapultepec live GDI research, maximum aggregate spending USD $20.";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

/** Soft allocations — sum must equal AGGREGATE_CEILING_USD ($20). */
export const SEGMENT_SOFT_ALLOCATIONS = Object.freeze([
  { hotelId: WATERSTONE_HOTEL_ID, segment: "scaling_companies", softUsd: 8 },
  { hotelId: WATERSTONE_HOTEL_ID, segment: "association_events", softUsd: 2 },
  { hotelId: CHAPULTEPEC_HOTEL_ID, segment: "scaling_companies", softUsd: 6 },
  { hotelId: CHAPULTEPEC_HOTEL_ID, segment: "association_events", softUsd: 4 },
]);

export const AUTHORIZATION_STATUS = Object.freeze({
  PROPOSED: "PROPOSED_NOT_AUTHORIZED",
  AUTHORIZED: "AUTHORIZED",
  HALTED: "HALTED",
});

function writeLedgerFile(ledgerPath, ledger) {
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  const tmp = `${ledgerPath}.${process.pid}.tmp`;
  fs.writeFileSync(
    tmp,
    `${JSON.stringify({ ...ledger, updatedAt: new Date().toISOString() }, null, 2)}\n`,
  );
  fs.rmSync(ledgerPath, { force: true });
  fs.renameSync(tmp, ledgerPath);
}

export function assertAllocationsSumToCeiling(ceiling = AGGREGATE_CEILING_USD) {
  const sum = SEGMENT_SOFT_ALLOCATIONS.reduce((s, a) => s + a.softUsd, 0);
  if (Math.abs(sum - ceiling) > 1e-9) {
    const err = new Error(`segment_allocations_sum_mismatch:${sum}!=${ceiling}`);
    err.code = "ALLOCATION_SUM_MISMATCH";
    throw err;
  }
  return { ok: true, sum, ceiling };
}

export function campaignAggregateLedgerPath(
  campaignRootRel = "reports/gdi-waterstone-chapultepec-live-campaign-v1",
) {
  if (process.env.GDI_WC_AGGREGATE_LEDGER) {
    const p = process.env.GDI_WC_AGGREGATE_LEDGER;
    return path.isAbsolute(p) ? p : path.join(ROOT, p);
  }
  return path.join(ROOT, campaignRootRel, "ledgers", "campaign-aggregate-20usd.json");
}

export function campaignStatePath(
  campaignRootRel = "reports/gdi-waterstone-chapultepec-live-campaign-v1",
) {
  if (process.env.GDI_WC_CAMPAIGN_STATE) {
    const p = process.env.GDI_WC_CAMPAIGN_STATE;
    return path.isAbsolute(p) ? p : path.join(ROOT, p);
  }
  return path.join(ROOT, campaignRootRel, "campaign-execution-state.json");
}

export function authorizationPath(
  campaignRootRel = "reports/gdi-waterstone-chapultepec-live-campaign-v1",
) {
  return path.join(ROOT, campaignRootRel, "authorization-status.json");
}

export function jobKeyFor(hotelId, segment) {
  return `wc_${hotelId}_${segment}`;
}

export function loadAuthorizationStatus(campaignRootRel) {
  const p = authorizationPath(campaignRootRel);
  if (!fs.existsSync(p)) {
    return {
      status: AUTHORIZATION_STATUS.PROPOSED,
      aggregateCeilingUsd: AGGREGATE_CEILING_USD,
      authorizedAt: null,
      authorizedBy: null,
      note: "PROPOSED — do not execute metered research until founder GO",
    };
  }
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function writeProposedAuthorization(campaignRootRel) {
  assertAllocationsSumToCeiling();
  const p = authorizationPath(campaignRootRel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const existing = fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, "utf8")) : null;
  // Never downgrade AUTHORIZED → PROPOSED on ledger ensure
  if (existing?.status === AUTHORIZATION_STATUS.AUTHORIZED) {
    return existing;
  }
  const doc = {
    status: AUTHORIZATION_STATUS.PROPOSED,
    aggregateCeilingUsd: AGGREGATE_CEILING_USD,
    softAllocations: SEGMENT_SOFT_ALLOCATIONS,
    authorizedAt: null,
    authorizedBy: null,
    note: "PROPOSED — shared hard cap $20 across four pathways; not four independent budgets",
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`);
  return doc;
}

/**
 * Record founder authorization for the shared $20 campaign.
 * Accepts the explicit GO phrase from the founder directive (or the shorter legacy phrase).
 */
export function markCampaignAuthorized({
  campaignRootRel = "reports/gdi-waterstone-chapultepec-live-campaign-v1",
  authorizedBy = "founder",
  approvalPhrase = "",
  source = "chat_directive",
} = {}) {
  assertAllocationsSumToCeiling();
  const normalized = String(approvalPhrase || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
  const accepted =
    normalized.includes("authorized") &&
    normalized.includes("20") &&
    (normalized.includes("waterstone") || normalized.includes("chapultepec"));
  if (!accepted) {
    const err = new Error("founder_approval_phrase_rejected");
    err.code = "APPROVAL_PHRASE_REJECTED";
    throw err;
  }
  const p = authorizationPath(campaignRootRel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const doc = {
    status: AUTHORIZATION_STATUS.AUTHORIZED,
    aggregateCeilingUsd: AGGREGATE_CEILING_USD,
    softAllocations: SEGMENT_SOFT_ALLOCATIONS,
    authorizedAt: new Date().toISOString(),
    authorizedBy,
    approvalPhrase: String(approvalPhrase).trim(),
    providersAllowed: ["exa", "firecrawl", "direct_public"],
    providersDenied: ["serpapi", "surfe", "pdl", "parallel"],
    publication: false,
    productionAirtableWrites: false,
    source,
    note: "AUTHORIZED — shared hard cap $20; Exa/Firecrawl/direct_public only",
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`);
  return doc;
}

/**
 * Refuse paid dispatch unless authorization status is AUTHORIZED.
 */
export function assertPaidExecutionAuthorized(campaignRootRel) {
  const auth = loadAuthorizationStatus(campaignRootRel);
  if (auth.status !== AUTHORIZATION_STATUS.AUTHORIZED) {
    const err = new Error("campaign_not_authorized_refuse_paid");
    err.code = "CAMPAIGN_NOT_AUTHORIZED";
    err.authorization = auth;
    throw err;
  }
  return auth;
}

export function ensureCampaignAggregateLedger({
  ceilingUsd = AGGREGATE_CEILING_USD,
  campaignRootRel = "reports/gdi-waterstone-chapultepec-live-campaign-v1",
  allowCreate = true,
} = {}) {
  assertAllocationsSumToCeiling(ceilingUsd);
  const ledgerPath = campaignAggregateLedgerPath(campaignRootRel);
  assertLiveDeepResearchLedger(ledgerPath);
  if (!fs.existsSync(ledgerPath) && !allowCreate) {
    const err = new Error("aggregate_ledger_missing_refuse_reset");
    err.code = "AGGREGATE_LEDGER_MISSING";
    throw err;
  }
  const ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath, {
    budgetUsd: ceilingUsd,
    version: WATERSTONE_CHAPULTEPEC_SHARED_BUDGET_V1,
  });
  if (ledger.unreconciledObligationsUsd == null) ledger.unreconciledObligationsUsd = 0;
  if (ledger.haltedPaidExecution == null) ledger.haltedPaidExecution = false;
  if (!Array.isArray(ledger.incidents)) ledger.incidents = [];
  writeLedgerFile(ledgerPath, ledger);
  // Preserve AUTHORIZED — only seed PROPOSED when missing
  writeProposedAuthorization(campaignRootRel);
  return ledgerPath;
}

export function assertSafeLedgerReuse(ledgerPath, { forceReset = false } = {}) {
  if (!fs.existsSync(ledgerPath)) return { ok: true, created: true };
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  const used =
    Number(ledger.estimatedSpentUsd || 0) +
    Number(ledger.reservedUsd || 0) +
    Number(ledger.unreconciledObligationsUsd || 0);
  if (used > 1e-9 && !forceReset) {
    return {
      ok: true,
      reused: true,
      refuseReset: true,
      usedUsd: used,
      estimatedSpentUsd: ledger.estimatedSpentUsd,
      reservedUsd: ledger.reservedUsd,
      unreconciledObligationsUsd: ledger.unreconciledObligationsUsd,
      haltedPaidExecution: ledger.haltedPaidExecution === true,
      note: "Existing ledger has spend/holds — reuse only; no destructive reset",
    };
  }
  return { ok: true, reused: false, usedUsd: used };
}

export function readLedgerSnapshot(ledgerPath) {
  if (!fs.existsSync(ledgerPath)) return null;
  return JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
}

export function obligationsUsd(ledger) {
  return Number(
    (
      Number(ledger?.estimatedSpentUsd || 0) +
      Number(ledger?.reservedUsd || 0) +
      Number(ledger?.unreconciledObligationsUsd || 0)
    ).toFixed(6),
  );
}

export function remainingAggregateUsd(ledgerPath) {
  const ledger = readLedgerSnapshot(ledgerPath);
  if (!ledger) return 0;
  const ceiling = Number(ledger.budgetUsd || 0);
  return Number((ceiling - obligationsUsd(ledger)).toFixed(6));
}

export function isCampaignHalted(ledgerPath) {
  const ledger = readLedgerSnapshot(ledgerPath);
  if (!ledger) return false;
  if (ledger.haltedPaidExecution === true) return true;
  const holds = (ledger.entries || []).filter((e) => e.status === "unreconciled_hold");
  return holds.length > 0;
}

function assertAggregateWithinCeilingUnlocked(ledger, ceiling = AGGREGATE_CEILING_USD) {
  const spent = Number(ledger?.estimatedSpentUsd || 0);
  const reserved = Number(ledger?.reservedUsd || 0);
  const unrec = Number(ledger?.unreconciledObligationsUsd || 0);
  const total = Number((spent + reserved + unrec).toFixed(6));
  const cap = Number(ledger?.budgetUsd || ceiling);
  if (total - cap > 1e-9) {
    return { ok: false, spent, reserved, unreconciledObligationsUsd: unrec, total, ceiling: cap };
  }
  return { ok: true, spent, reserved, unreconciledObligationsUsd: unrec, total, ceiling: cap };
}

export function assertAggregateWithinCeiling(ledgerPath, ceiling = AGGREGATE_CEILING_USD) {
  const ledger = readLedgerSnapshot(ledgerPath);
  return assertAggregateWithinCeilingUnlocked(ledger, ceiling);
}

export function reconcileUnresolvedHolds(ledgerPath) {
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  const reserved = Number(ledger.reservedUsd || 0);
  const unreconciled = Number(ledger.unreconciledObligationsUsd || 0);
  const holds = (ledger.entries || []).filter((e) => e.status === "unreconciled_hold");
  return {
    ok: reserved < 1e-9 && unreconciled < 1e-9 && holds.length === 0 && ledger.haltedPaidExecution !== true,
    reservedUsd: reserved,
    unreconciledObligationsUsd: unreconciled,
    holdCount: holds.length,
    haltedPaidExecution: ledger.haltedPaidExecution === true,
    incidents: ledger.incidents || [],
  };
}

export function assertPaidExecutionAllowed(ledgerPath) {
  const ledger = readLedgerSnapshot(ledgerPath);
  if (!ledger) return { ok: false, reason: "missing_ledger" };
  if (ledger.haltedPaidExecution === true) {
    return {
      ok: false,
      reason: "halted_paid_execution",
      incidents: ledger.incidents || [],
      note: "Reconcile unreconciled holds / overspend incidents before further paid runs.",
    };
  }
  const holds = (ledger.entries || []).filter((e) => e.status === "unreconciled_hold");
  if (holds.length) {
    return {
      ok: false,
      reason: "unreconciled_holds_present",
      holds: holds.map((h) => ({
        reservationId: h.reservationId,
        jobKey: h.jobKey,
        quotedUsd: h.quotedUsd,
        reportedSpentUsd: h.reportedSpentUsd,
      })),
    };
  }
  const check = assertAggregateWithinCeiling(ledgerPath);
  if (!check.ok) return { ok: false, reason: "ceiling_breached", check };
  return { ok: true };
}

export function reserveSegmentBudget({
  ledgerPath,
  hotelId,
  segment,
  softUsd,
  kind = "segment_slice",
}) {
  const allowed = assertPaidExecutionAllowed(ledgerPath);
  if (!allowed.ok) {
    return { ok: false, reason: allowed.reason, jobKey: jobKeyFor(hotelId, segment), detail: allowed };
  }
  const key = jobKeyFor(hotelId, segment);
  const remaining = remainingAggregateUsd(ledgerPath);
  const quote = Number(Math.min(Number(softUsd), Math.max(0, remaining)).toFixed(6));
  if (!(quote > 0) && remaining <= 0) {
    return { ok: false, reason: "budget_exhausted", jobKey: key, quotedUsd: 0, remainingUsd: remaining };
  }
  if (!(quote > 0)) {
    return { ok: false, reason: "zero_quote", jobKey: key, quotedUsd: 0, remainingUsd: remaining };
  }
  const result = reserveLiveDeepResearchSpend(ledgerPath, {
    jobKey: key,
    quotedUsd: quote,
    kind,
  });
  return {
    ...result,
    jobKey: key,
    quotedUsd: quote,
    remainingUsdAfter: remainingAggregateUsd(ledgerPath),
  };
}

export function commitSegmentSpend(ledgerPath, reservationId, actualUsd) {
  return commitLiveDeepResearchSpend(ledgerPath, reservationId, actualUsd);
}

export function failSegmentSpend(ledgerPath, reservationId, reason) {
  return failLiveDeepResearchSpend(ledgerPath, reservationId, reason);
}

export function cancelSegmentSpend(ledgerPath, reservationId, reason) {
  return cancelLiveDeepResearchSpend(ledgerPath, reservationId, reason);
}

export function markUnreconciledHold(
  ledgerPath,
  reservationId,
  { reason, reportedSpentUsd = null, exitCode = null, halt = true } = {},
) {
  return withLiveDeepResearchLedgerLock(ledgerPath, () => {
    const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
    const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
    if (!entry) return { ok: false, reason: "reservation_missing", ledger };
    if (entry.status === "spent") return { ok: true, idempotent: true, ledger };
    if (entry.status === "failed" || entry.status === "cancelled") {
      return { ok: false, reason: `entry_${entry.status}_already_released`, ledger };
    }
    entry.status = "unreconciled_hold";
    entry.holdReason = reason || "unreconciled_child_failure";
    entry.reportedSpentUsd =
      reportedSpentUsd == null ? null : Number(reportedSpentUsd);
    entry.exitCode = exitCode;
    entry.heldAt = new Date().toISOString();
    if (halt) ledger.haltedPaidExecution = true;
    ledger.incidents = ledger.incidents || [];
    ledger.incidents.push({
      type: "UNRECONCILED_HOLD",
      reservationId,
      jobKey: entry.jobKey,
      quotedUsd: entry.quotedUsd,
      reportedSpentUsd: entry.reportedSpentUsd,
      reason: entry.holdReason,
      at: entry.heldAt,
    });
    writeLedgerFile(ledgerPath, ledger);
    return { ok: true, ledger, heldUsd: Number(entry.quotedUsd) };
  });
}

export function settleChildSpend(ledgerPath, reservationId, quotedUsd, reportedSpentUsd) {
  if (reportedSpentUsd == null || !Number.isFinite(Number(reportedSpentUsd))) {
    return markUnreconciledHold(ledgerPath, reservationId, {
      reason: "missing_child_spend_record",
      reportedSpentUsd: null,
      halt: true,
    });
  }
  const reported = Number(reportedSpentUsd);
  const quoted = Number(quotedUsd);

  if (reported - quoted > 1e-9) {
    return withLiveDeepResearchLedgerLock(ledgerPath, () => {
      const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
      const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
      if (!entry) return { ok: false, reason: "reservation_missing" };
      ledger.reservedUsd = Number(
        Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd)).toFixed(6),
      );
      ledger.estimatedSpentUsd = Number(
        (Number(ledger.estimatedSpentUsd || 0) + reported).toFixed(6),
      );
      entry.status = "spent";
      entry.actualUsd = reported;
      entry.quotedUsd = quoted;
      entry.overspendUsd = Number((reported - quoted).toFixed(6));
      entry.spentAt = new Date().toISOString();
      ledger.haltedPaidExecution = true;
      ledger.incidents = ledger.incidents || [];
      ledger.incidents.push({
        type: "OVERSPEND_INCIDENT",
        open: true,
        reservationId,
        jobKey: entry.jobKey,
        quotedUsd: quoted,
        reportedSpentUsd: reported,
        overspendUsd: entry.overspendUsd,
        at: entry.spentAt,
        action: "halt_further_paid_execution",
      });
      writeLedgerFile(ledgerPath, ledger);
      return {
        ok: false,
        reason: "overspend_halt",
        spentUsd: reported,
        quotedUsd: quoted,
        overspendUsd: entry.overspendUsd,
        halt: true,
        ledger,
        ceilingCheck: assertAggregateWithinCeilingUnlocked(ledger),
      };
    });
  }

  const committed = commitSegmentSpend(ledgerPath, reservationId, reported);
  return {
    ok: committed.ok === true,
    spentUsd: reported,
    quotedUsd: quoted,
    overspendUsd: 0,
    halt: false,
    ledger: committed.ledger,
    reason: committed.ok ? "committed" : committed.reason,
  };
}

export function loadCampaignState(statePath = campaignStatePath()) {
  if (!fs.existsSync(statePath)) {
    return {
      version: WATERSTONE_CHAPULTEPEC_SHARED_BUDGET_V1,
      completedJobs: [],
      interruptedJobs: [],
      unreconciledJobs: [],
      shadowRuns: [],
      updatedAt: null,
    };
  }
  return JSON.parse(fs.readFileSync(statePath, "utf8"));
}

export function saveCampaignState(state, statePath = campaignStatePath()) {
  fs.mkdirSync(path.dirname(statePath), { recursive: true });
  const next = { ...state, updatedAt: new Date().toISOString() };
  fs.writeFileSync(statePath, `${JSON.stringify(next, null, 2)}\n`);
  return next;
}

export function isJobCompleted(state, hotelId, segment) {
  const key = jobKeyFor(hotelId, segment);
  return (state.completedJobs || []).some((j) => j.jobKey === key && j.status === "completed");
}

export function markJobCompleted(state, row) {
  const jobs = [...(state.completedJobs || []).filter((j) => j.jobKey !== row.jobKey), row];
  return { ...state, completedJobs: jobs };
}

export function markJobInterrupted(state, row) {
  const jobs = [...(state.interruptedJobs || []).filter((j) => j.jobKey !== row.jobKey), row];
  return { ...state, interruptedJobs: jobs };
}

export function markJobUnreconciled(state, row) {
  const jobs = [...(state.unreconciledJobs || []).filter((j) => j.jobKey !== row.jobKey), row];
  return { ...state, unreconciledJobs: jobs };
}

export async function reserveSharedCampaignSpend({
  hotelId,
  segment,
  amountUsd,
  campaignRootRel = "reports/gdi-waterstone-chapultepec-live-campaign-v1",
  meta = {},
} = {}) {
  assertPaidExecutionAuthorized(campaignRootRel);
  const ledgerPath = campaignAggregateLedgerPath(campaignRootRel);
  const holds = reconcileUnresolvedHolds(ledgerPath);
  if (holds.haltedPaidExecution) {
    const err = new Error("campaign_halted_refuse_reserve");
    err.code = "CAMPAIGN_HALTED";
    throw err;
  }
  return withLiveDeepResearchLedgerLock(ledgerPath, async () => {
    return reserveLiveDeepResearchSpend(ledgerPath, {
      amountUsd,
      jobKey: jobKeyFor(hotelId, segment),
      meta: { hotelId, segment, ...meta, sharedCeilingUsd: AGGREGATE_CEILING_USD },
    });
  });
}

export {
  commitLiveDeepResearchSpend as commitSharedCampaignSpend,
  failLiveDeepResearchSpend as failSharedCampaignSpend,
  cancelLiveDeepResearchSpend as cancelSharedCampaignSpend,
};

/** Reuse the same provider-level enforcement checks as the operational campaign. */
export function verifyChildProviderBudgetEnforcementActive({ root = ROOT } = {}) {
  return verifyChildProviderBudgetEnforcementActiveOps({ root });
}

export function preflightSharedBudgetGate(campaignRootRel) {
  assertAllocationsSumToCeiling();
  const ledgerPath = ensureCampaignAggregateLedger({ campaignRootRel });
  const reuse = assertSafeLedgerReuse(ledgerPath);
  const holds = reconcileUnresolvedHolds(ledgerPath);
  const auth = loadAuthorizationStatus(campaignRootRel);
  const softSum = SEGMENT_SOFT_ALLOCATIONS.reduce((s, a) => s + a.softUsd, 0);
  const proposedReady =
    softSum === AGGREGATE_CEILING_USD &&
    holds.ok &&
    !holds.haltedPaidExecution &&
    auth.status === AUTHORIZATION_STATUS.PROPOSED;
  const liveReady =
    softSum === AGGREGATE_CEILING_USD &&
    holds.ok &&
    !holds.haltedPaidExecution &&
    auth.status === AUTHORIZATION_STATUS.AUTHORIZED;
  return {
    ok: proposedReady || liveReady,
    proposedReady,
    liveReady,
    aggregateCeilingUsd: AGGREGATE_CEILING_USD,
    softAllocationsSumUsd: softSum,
    softAllocations: SEGMENT_SOFT_ALLOCATIONS,
    authorization: auth,
    ledgerPath,
    ledgerReuse: reuse,
    holds,
    remainingUsd: remainingAggregateUsd(ledgerPath),
    paidExecutionAllowed: liveReady,
    note: liveReady
      ? "Shared $20 gate AUTHORIZED — paid FullE2E dispatch permitted under soft caps"
      : auth.status === AUTHORIZATION_STATUS.PROPOSED
        ? "Shared $20 gate ready — awaiting founder authorization before any metered call"
        : "Check authorization / holds before dispatch",
  };
}
