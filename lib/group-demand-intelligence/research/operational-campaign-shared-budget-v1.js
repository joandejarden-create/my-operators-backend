/**
 * Shared aggregate campaign budget for Bethesda + Wyndham operational live research.
 *
 * Soft segment allocations sum to exactly $10. Children only receive atomically
 * reserved slices. Failed/timed-out children keep a CONSERVATIVE HOLD (do not
 * release reservation). Overspend is recorded as an incident and halts further
 * paid execution — never silently capped away.
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

export const OPERATIONAL_CAMPAIGN_SHARED_BUDGET_V1 = "gdi-operational-campaign-shared-budget-v1";
export const AGGREGATE_CEILING_USD = 10;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

/** Soft allocations — sum must equal AGGREGATE_CEILING_USD. */
export const SEGMENT_SOFT_ALLOCATIONS = Object.freeze([
  { hotelId: "recLuxvwwxID7U2B8", segment: "scaling_companies", softUsd: 2.5 },
  { hotelId: "recLuxvwwxID7U2B8", segment: "association_events", softUsd: 1.5 },
  { hotelId: "recLuxvwwxID7U2B8", segment: "corporate_meetings_training", softUsd: 0.75 },
  { hotelId: "recLuxvwwxID7U2B8", segment: "corporate_projects_extended_stay", softUsd: 0.75 },
  { hotelId: "recnPB3Q02NO8Ma1o", segment: "scaling_companies", softUsd: 2.0 },
  { hotelId: "recnPB3Q02NO8Ma1o", segment: "association_events", softUsd: 1.25 },
  { hotelId: "recnPB3Q02NO8Ma1o", segment: "corporate_meetings_training", softUsd: 0.625 },
  { hotelId: "recnPB3Q02NO8Ma1o", segment: "corporate_projects_extended_stay", softUsd: 0.625 },
]);

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

export function campaignAggregateLedgerPath(campaignRootRel = "reports/gdi-operational-live-research-v1") {
  if (process.env.GDI_OPS_AGGREGATE_LEDGER) {
    const p = process.env.GDI_OPS_AGGREGATE_LEDGER;
    return path.isAbsolute(p) ? p : path.join(ROOT, p);
  }
  return path.join(ROOT, campaignRootRel, "ledgers", "campaign-aggregate-10usd.json");
}

export function campaignStatePath(campaignRootRel = "reports/gdi-operational-live-research-v1") {
  if (process.env.GDI_OPS_CAMPAIGN_STATE) {
    const p = process.env.GDI_OPS_CAMPAIGN_STATE;
    return path.isAbsolute(p) ? p : path.join(ROOT, p);
  }
  return path.join(ROOT, campaignRootRel, "campaign-execution-state.json");
}

export function jobKeyFor(hotelId, segment) {
  return `ops_${hotelId}_${segment}`;
}

export function ensureCampaignAggregateLedger({
  ceilingUsd = AGGREGATE_CEILING_USD,
  campaignRootRel = "reports/gdi-operational-live-research-v1",
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
    version: "gdi-operational-live-research-v1-aggregate-ledger",
  });
  // Ensure campaign safety fields exist
  if (ledger.unreconciledObligationsUsd == null) ledger.unreconciledObligationsUsd = 0;
  if (ledger.haltedPaidExecution == null) ledger.haltedPaidExecution = false;
  if (!Array.isArray(ledger.incidents)) ledger.incidents = [];
  writeLedgerFile(ledgerPath, ledger);
  return ledgerPath;
}

/**
 * Refuse destructive reset of a live aggregate ledger that already has spend/holds
 * unless explicitly forced (tests only).
 */
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
      estimatedSpentUsd: ledger.estimatedSpentUsd,
      reservedUsd: ledger.reservedUsd,
      unreconciledObligationsUsd: ledger.unreconciledObligationsUsd,
      haltedPaidExecution: ledger.haltedPaidExecution === true,
      note: "Existing aggregate ledger preserved — restart cannot wipe prior obligations.",
    };
  }
  return { ok: true, reused: true, refuseReset: false };
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

/** Only for dry-spawn / never-metered paths — releases reservation. */
export function failSegmentSpend(ledgerPath, reservationId, reason) {
  return failLiveDeepResearchSpend(ledgerPath, reservationId, reason);
}

export function cancelSegmentSpend(ledgerPath, reservationId, reason) {
  return cancelLiveDeepResearchSpend(ledgerPath, reservationId, reason);
}

/**
 * Failed / timed-out / crashed child may have billed providers.
 * Keep full quote as reserved hold; do NOT release.
 */
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
    // reserved or already unreconciled_hold — keep reservedUsd
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

/**
 * After operator/verifier confirms actual spend for a hold.
 * verifiedUsd may be less than quote; if greater, records overspend incident.
 */
export function reconcileHold(ledgerPath, reservationId, verifiedUsd) {
  return withLiveDeepResearchLedgerLock(ledgerPath, () => {
    const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
    const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
    if (!entry) return { ok: false, reason: "reservation_missing" };
    if (entry.status !== "unreconciled_hold" && entry.status !== "reserved") {
      return { ok: false, reason: `cannot_reconcile_${entry.status}` };
    }
    const verified = Number(verifiedUsd);
    const quoted = Number(entry.quotedUsd);
    if (!Number.isFinite(verified) || verified < 0) {
      return { ok: false, reason: "invalid_verified" };
    }

    // Release reserved quote, then apply verified accounting
    ledger.reservedUsd = Number(
      Math.max(0, Number(ledger.reservedUsd || 0) - quoted).toFixed(6),
    );

    if (verified - quoted > 1e-9) {
      // Overspend: charge full verified against spent; halt
      const nextSpent = Number((Number(ledger.estimatedSpentUsd || 0) + verified).toFixed(6));
      ledger.estimatedSpentUsd = nextSpent;
      entry.status = "spent";
      entry.actualUsd = verified;
      entry.overspendUsd = Number((verified - quoted).toFixed(6));
      entry.spentAt = new Date().toISOString();
      ledger.haltedPaidExecution = true;
      ledger.incidents = ledger.incidents || [];
      ledger.incidents.push({
        type: "OVERSPEND_ON_RECONCILE",
        reservationId,
        jobKey: entry.jobKey,
        quotedUsd: quoted,
        verifiedUsd: verified,
        overspendUsd: entry.overspendUsd,
        at: entry.spentAt,
      });
      writeLedgerFile(ledgerPath, ledger);
      return {
        ok: true,
        overspend: true,
        spentUsd: verified,
        halt: true,
        ledger,
        ceilingCheck: assertAggregateWithinCeilingUnlocked(ledger),
      };
    }

    const nextSpent = Number((Number(ledger.estimatedSpentUsd || 0) + verified).toFixed(6));
    ledger.estimatedSpentUsd = nextSpent;
    entry.status = "spent";
    entry.actualUsd = verified;
    entry.spentAt = new Date().toISOString();
    entry.reconciledAt = entry.spentAt;
    // Clear halt if no remaining holds/incidents requiring halt
    const remainingHolds = (ledger.entries || []).filter((e) => e.status === "unreconciled_hold");
    const openOverspend = (ledger.incidents || []).some(
      (i) => i.type === "OVERSPEND_INCIDENT" && i.open === true,
    );
    if (!remainingHolds.length && !openOverspend) {
      ledger.haltedPaidExecution = false;
    }
    writeLedgerFile(ledgerPath, ledger);
    return { ok: true, overspend: false, spentUsd: verified, halt: false, ledger };
  });
}

/**
 * Settle a successful child report WITHOUT silent capping.
 * - missing report → unreconciled hold (full quote)
 * - reported <= quote → commit reported
 * - reported > quote → commit quote + record excess obligation + HALT
 */
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
      // Commit full reported amount (true expenditure), release reservation quote from reserved
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

export function loadCampaignState(statePath = campaignStatePath()) {
  if (!fs.existsSync(statePath)) {
    return {
      version: OPERATIONAL_CAMPAIGN_SHARED_BUDGET_V1,
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

export function isJobUnreconciled(state, hotelId, segment) {
  const key = jobKeyFor(hotelId, segment);
  return (state.unreconciledJobs || []).some((j) => j.jobKey === key);
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

/**
 * Offline helper — prefer settleChildSpend for production semantics.
 * Overshoot now HALTS rather than silently capping as success.
 */
export function simulateChildSpendAgainstReservation({
  ledgerPath,
  reservationId,
  quotedUsd,
  attemptUsd,
  crashAfterSpend = false,
}) {
  const attempt = Number(attemptUsd);
  if (!Number.isFinite(attempt) || attempt < 0) {
    return markUnreconciledHold(ledgerPath, reservationId, {
      reason: "invalid_attempt",
      reportedSpentUsd: null,
    });
  }
  if (crashAfterSpend) {
    // Spent then crashed — hold full quote conservatively (or reported if known)
    return markUnreconciledHold(ledgerPath, reservationId, {
      reason: "child_crash_after_spend",
      reportedSpentUsd: attempt,
      halt: true,
    });
  }
  return settleChildSpend(ledgerPath, reservationId, quotedUsd, attempt);
}

export function withAggregateLock(ledgerPath, fn) {
  return withLiveDeepResearchLedgerLock(ledgerPath, fn);
}

/**
 * Static verification that child runners enforce budget BEFORE metered calls.
 * Distinguishes code-path enforcement from ledger-only arithmetic.
 */
export function verifyChildProviderBudgetEnforcementActive({ root = ROOT } = {}) {
  const checks = [];
  const deepPath = path.join(
    root,
    "lib/group-demand-intelligence/research/deep-research-gdi-live-certification-v1.js",
  );
  const deepSrc = fs.readFileSync(deepPath, "utf8");
  const httpCallRe = /(?:providerFetch|fetch)\s*\(/;
  checks.push({
    id: "maybeExaSearch_reserves_before_fetch",
    ok:
      /export async function maybeExaSearch[\s\S]*?reserveLiveDeepResearchSpend[\s\S]*?(?:providerFetch|fetch)\s*\(/.test(
        deepSrc,
      ) || /reserveLiveDeepResearchSpend[\s\S]{0,400}api\.exa\.ai/.test(deepSrc),
    path: deepPath,
  });
  checks.push({
    id: "paid_provider_hold_after_ambiguous",
    ok:
      deepSrc.includes("holdAmbiguousPaidProviderSpend")
      && deepSrc.includes("markPaidProviderDispatched")
      && deepSrc.includes("unreconciled_hold"),
  });
  checks.push({
    id: "dispatch_mark_return_checked_before_http",
    ok:
      /markPaidProviderDispatched[\s\S]{0,400}!dispatched\.ok[\s\S]{0,200}(?:providerFetch|fetch)\s*\(/.test(
        deepSrc,
      )
      || (
        deepSrc.includes("if (!dispatched.ok)")
        && deepSrc.includes("already_dispatched")
        && deepSrc.includes("job_reservation_in_progress")
      ),
  });
  // Read maybeExaSearch more carefully
  const exaIdx = deepSrc.indexOf("export async function maybeExaSearch");
  if (exaIdx >= 0) {
    const slice = deepSrc.slice(exaIdx, exaIdx + 3500);
    const reserveAt = slice.indexOf("reserveLiveDeepResearchSpend");
    const dispatchAt = slice.indexOf("markPaidProviderDispatched");
    const checkDispatch = slice.indexOf("!dispatched.ok");
    const fetchAt = slice.search(httpCallRe);
    checks.push({
      id: "exa_reserve_before_fetch_order",
      ok:
        reserveAt >= 0
        && fetchAt >= 0
        && reserveAt < fetchAt
        && dispatchAt >= 0
        && dispatchAt < fetchAt
        && checkDispatch >= 0
        && checkDispatch < fetchAt,
      reserveAt,
      dispatchAt,
      checkDispatch,
      fetchAt,
    });
  }
  const fcMatch = deepSrc.match(/async function (maybeFirecrawl\w*|firecrawlScrape\w*)/);
  if (fcMatch) {
    const start = deepSrc.indexOf(fcMatch[0]);
    const slice = deepSrc.slice(start, start + 3500);
    const reserveAt = slice.indexOf("reserveLiveDeepResearchSpend");
    const dispatchAt = slice.indexOf("markPaidProviderDispatched");
    const checkDispatch = slice.indexOf("!dispatched.ok");
    const fetchAt = slice.search(httpCallRe);
    checks.push({
      id: "firecrawl_reserve_before_fetch_order",
      ok:
        reserveAt >= 0
        && fetchAt >= 0
        && reserveAt < fetchAt
        && dispatchAt >= 0
        && dispatchAt < fetchAt
        && checkDispatch >= 0
        && checkDispatch < fetchAt,
      reserveAt,
      dispatchAt,
      checkDispatch,
      fetchAt,
    });
  } else {
    const scrapeIdx = deepSrc.indexOf("firecrawl");
    checks.push({
      id: "firecrawl_reserve_present_in_module",
      ok: scrapeIdx >= 0 && deepSrc.includes("reserveLiveDeepResearchSpend"),
    });
  }

  const cmtPath = path.join(root, "scripts/gdi-operational-cmt-cpe-live-segment-v1.mjs");
  const cmtSrc = fs.readFileSync(cmtPath, "utf8");
  checks.push({
    id: "cmt_cpe_budget_ceiling_before_live_search",
    ok:
      cmtSrc.includes("searchIndexBudgetCeilingUsd") &&
      cmtSrc.includes("authorizeLiveMeteredSearch") &&
      /budgetUsd|budget-usd/.test(cmtSrc),
  });

  const mockPath = path.join(root, "scripts/gdi-operational-mocked-metered-child-v1.mjs");
  checks.push({
    id: "mocked_metered_child_exists",
    ok: fs.existsSync(mockPath),
    path: mockPath,
  });

  return {
    ok: checks.every((c) => c.ok),
    checks,
    distinction:
      "Provider-level prevention = reserve/budget check before HTTP fetch in child modules. Ledger arithmetic alone is insufficient.",
  };
}
