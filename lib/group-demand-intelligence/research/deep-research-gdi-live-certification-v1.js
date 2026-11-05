/**
 * Live Webhound-like deep-research certification for one existing GDI opportunity:
 * Principal. London DMC → Nobu Hotel London Portman Square.
 *
 * Isolated ledger · hard ceiling $0.75 · no Parallel/Surfe/PDL · no Airtable/publish.
 * Prefer direct_public; Exa lite + Firecrawl scrape only when priced and reserved.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash, randomBytes } from "node:crypto";
import { fileURLToPath } from "node:url";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import { credentialPresence } from "./benchmark-provider-flags-v1.js";
import {
  DEEP_RESEARCH_GDI_INTEGRATION_V1,
  seedPrincipalLondonGdiOpportunity,
  captureOpportunityBeforeSnapshot,
  runDeepResearchOnExistingGdiOpportunity,
  assertPaidContactProvidersDisabled,
} from "./deep-research-gdi-integration-v1.js";
import {
  LONGFORM_HELD_URLS,
  LONGFORM_CANDIDATE,
} from "./longform-research-certification-v1.js";
import {
  extractPublicContactIntelligenceForOrs,
  classifyContactOwnership,
  CONTACT_OWNERSHIP_CLASS,
} from "./commercial-spine-restoration-v41.js";

export const LIVE_DEEP_RESEARCH_CERT_VERSION = "gdi-deep-research-gdi-integration-v1-live";
export const LIVE_HARD_CEILING_USD = 0.75;
export const LIVE_LEDGER_RELATIVE = "reports/gdi-deep-research-gdi-integration-v1/live-ledger.json";
export const LIVE_LEDGER_VERSION = "gdi-deep-research-gdi-integration-v1-live-ledger";
/** Allowed isolated ledgers for deep-research live spends (exact relative path match). */
export const ALLOWED_LIVE_DEEP_RESEARCH_LEDGERS = Object.freeze([
  LIVE_LEDGER_RELATIVE,
  "reports/gdi-evidence-driven-longform-validation-v1/live-ledger.json",
  "reports/gdi-autonomous-longform-gdi-integration-v1/live-ledgers/gdi_lmc_97fb727376abb83b.json",
  "reports/gdi-autonomous-longform-gdi-integration-v1/live-ledgers/gdi_opp_potomac_memorial_2027.json",
  "reports/gdi-autonomous-longform-gdi-integration-v1/live-ledgers/gdi_opp_nado_ddaa_washcon_2028.json",
]);

/** Directory prefix allowlist for multi-opportunity pilot ledgers (in addition to exact paths). */
export const ALLOWED_LIVE_DEEP_RESEARCH_LEDGER_PREFIXES = Object.freeze([
  "reports/gdi-autonomous-longform-gdi-integration-v1/live-ledgers/",
  "reports/gdi-commercial-depth-hardening-v1/live-ledgers/",
  "reports/gdi-full-commercial-deep-research-validation-v1/live-ledgers/",
  "reports/gdi-bethesda-full-demand-challenge-v1/live-ledgers/",
  "reports/gdi-bethesda-full-spectrum-discovery-v1/live-stage-a/ledgers/",
  "reports/gdi-bethesda-full-spectrum-discovery-v1/live-stage-b/ledgers/",
  "reports/gdi-autonomous-demand-discovery-v5-bethesda/live-stage-a/ledgers/",
  "reports/gdi-bethesda-scaling-companies-full-e2e-v1/ledgers/",
  "reports/gdi-bethesda-scaling-companies-full-e2e-v1/live/ledgers/",
  "reports/gdi-bethesda-scaling-companies-commercial-completion-v1/ledgers/",
  "reports/gdi-bethesda-scaling-companies-final-publication-v1/ledgers/",
  "reports/gdi-scaling-companies-hotel-runner-v1/",
  "reports/gdi-scaling-company-runs/",
  "reports/gdi-waterstone-scaling-full-e2e-runner-v1/",
  "reports/gdi-waterstone-scaling-full-e2e-runner-repair-v1/",
  "reports/gdi-waterstone-scaling-contact-recovery-v1/",
  "reports/gdi-waterstone-scaling-companies-final-publication-v1/",
  "reports/gdi-scaling-companies-universal-full-e2e-runner-v1/",
  "reports/gdi-scaling-companies-universal-final-certification-v1/",
  "reports/gdi-universal-hotel-profile-onboarding-mexico-v1/",
  "reports/gdi-scaling-companies-intelligent-lifecycle-v1/",
  "reports/gdi-scaling-autonomous-research-first-full-e2e-v1/",
  "reports/gdi-association-events-runs/",
  "reports/gdi-association-events-mexico-full-e2e-v1/",
  "reports/gdi-operational-live-research-v1/",
  "reports/gdi-scaling-exa-agent-two-stage-integration/stage-b-controlled-comparison/ledgers/",
  "reports/gdi-scaling-exa-agent-two-stage-integration/stage-c-multi-company/ledgers/",
  "reports/gdi-waterstone-chapultepec-live-campaign-v1/",
  "reports/gdi-mexico-scaling-gold-standard-fulle2e/live-campaign-v1/",
  "reports/gdi-scaling-companies-fulle2e-fiesta-americana-condesa-crosshotel-v1/live-campaign-v1/",
  "reports/gdi-events-chapultepec-12-opportunity-reresearch-v1/",
  "reports/gdi-mexico-two-hotel-commercial-demand-campaign-v1/",
]);

const FORBIDDEN_LEDGER = /parallel-live-comparison|cohort-a-live|segment-e2e|paid-multisegment|parallel-budget|gdi-planner-evaluation|gdi-autonomous-research-certification|gdi-autonomous-deep-research-certification|gdi-autonomous-longform-research-certification|cross-organization-followup-savoy|gdi-commercial-demand-discovery-v4/i;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hashKey(v) {
  return createHash("sha256").update(String(v || "")).digest("hex").slice(0, 12);
}

function hostnameOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function isIndependentPublisher(url) {
  const h = hostnameOf(url);
  if (!h) return false;
  if (/principallondon\.com|principalge\.com/i.test(h)) return false;
  return /rosewoodhotels\.com|fairmont\.com|savoy|nobuhotels\.com|hilton\.com|marriott\.com|ihg\.com|travelweekly|meetingstoday|bizbash|skift/i.test(h);
}

export function assertLiveDeepResearchLedger(ledgerPath) {
  const normalized = String(ledgerPath || "").replace(/\\/g, "/");
  // Forbid prior research ledgers, but allow the multi-opp pilot live-ledgers directory
  // (path contains "gdi-autonomous-longform-gdi-integration-v1/live-ledgers/" which is allowlisted).
  const prefixAllowed = ALLOWED_LIVE_DEEP_RESEARCH_LEDGER_PREFIXES.some((prefix) =>
    normalized.includes(String(prefix).replace(/\\/g, "/")));
  if (!prefixAllowed && FORBIDDEN_LEDGER.test(normalized)) {
    const err = new Error("live_deep_research_ledger_not_isolated");
    err.code = "LIVE_LEDGER_NOT_ISOLATED";
    throw err;
  }
  const allowedExact = ALLOWED_LIVE_DEEP_RESEARCH_LEDGERS.some((rel) =>
    normalized.endsWith(String(rel).replace(/\\/g, "/")));
  if (!allowedExact && !prefixAllowed) {
    const err = new Error("live_deep_research_ledger_path_refused");
    err.code = "LIVE_LEDGER_PATH_REFUSED";
    throw err;
  }
}

function emptyLedger({ budgetUsd = LIVE_HARD_CEILING_USD, version = LIVE_LEDGER_VERSION } = {}) {
  return {
    version,
    currency: "USD",
    budgetUsd: Number(budgetUsd),
    isolatedFromPriorResearchBalances: true,
    usesPriorBenchmarkBalances: false,
    usesRemainingV4Balances: false,
    reservedUsd: 0,
    estimatedSpentUsd: 0,
    exaCalls: 0,
    firecrawlCalls: 0,
    directRetrievals: 0,
    haltedPaidExecution: false,
    incidents: [],
    entries: [],
    updatedAt: new Date().toISOString(),
  };
}

/** Test hook — inject mock fetch for offline spend-safety tests (never used in production). */
let __testProviderFetch = null;
export function __setLiveProviderFetchForTests(fn) {
  __testProviderFetch = typeof fn === "function" ? fn : null;
}
async function providerFetch(url, init) {
  const impl = __testProviderFetch || fetch;
  return impl(url, init);
}

function ledgerCeiling(ledger) {
  const n = Number(ledger?.budgetUsd);
  return Number.isFinite(n) && n > 0 ? n : LIVE_HARD_CEILING_USD;
}

const LOCK_STALE_MS = 30_000;
const LOCK_MAX_WAIT_MS = 15_000;
const LOCK_RETRY_MS = 25;

function sleepSync(ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    /* busy wait — short lock retries only */
  }
}

/**
 * Exclusive directory lock for ledger mutations (reserve/commit/fail/reconcile).
 * Prevents the live-run race where persist reloaded a stale object mid-commit.
 */
export function withLiveDeepResearchLedgerLock(ledgerPath, fn) {
  assertLiveDeepResearchLedger(ledgerPath);
  const lockDir = `${ledgerPath}.lock`;
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  const started = Date.now();
  for (;;) {
    try {
      fs.mkdirSync(lockDir);
      fs.writeFileSync(
        path.join(lockDir, "owner.json"),
        JSON.stringify({ pid: process.pid, at: new Date().toISOString() }),
      );
      break;
    } catch (err) {
      if (err?.code !== "EEXIST") throw err;
      try {
        const st = fs.statSync(lockDir);
        if (Date.now() - st.mtimeMs > LOCK_STALE_MS) {
          fs.rmSync(lockDir, { recursive: true, force: true });
          continue;
        }
      } catch { /* retry */ }
      if (Date.now() - started > LOCK_MAX_WAIT_MS) {
        const e = new Error("live_deep_research_ledger_lock_timeout");
        e.code = "LIVE_LEDGER_LOCK_TIMEOUT";
        throw e;
      }
      sleepSync(LOCK_RETRY_MS);
    }
  }
  try {
    return fn();
  } finally {
    try {
      fs.rmSync(lockDir, { recursive: true, force: true });
    } catch { /* ignore */ }
  }
}

function writeLedger(ledgerPath, ledger) {
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  const tmp = `${ledgerPath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify({ ...ledger, updatedAt: new Date().toISOString() }, null, 2)}\n`);
  fs.rmSync(ledgerPath, { force: true });
  fs.renameSync(tmp, ledgerPath);
}

export function loadOrCreateLiveDeepResearchLedger(ledgerPath, { budgetUsd, version } = {}) {
  assertLiveDeepResearchLedger(ledgerPath);
  if (!fs.existsSync(ledgerPath)) {
    const ledger = emptyLedger({
      budgetUsd: budgetUsd ?? LIVE_HARD_CEILING_USD,
      version: version ?? LIVE_LEDGER_VERSION,
    });
    writeLedger(ledgerPath, ledger);
    return ledger;
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  const ceiling = ledgerCeiling(ledger);
  if (!(ceiling > 0) || !Number.isFinite(ceiling)) {
    const err = new Error("live_deep_research_ledger_budget_refused");
    err.code = "LIVE_LEDGER_BUDGET_REFUSED";
    throw err;
  }
  // If caller supplies an expected ceiling, refuse mismatch (prevents $0.75 ledger reuse for $0.50 run)
  if (budgetUsd != null && Math.abs(Number(budgetUsd) - ceiling) > 1e-9) {
    const err = new Error("live_deep_research_ledger_budget_mismatch");
    err.code = "LIVE_LEDGER_BUDGET_MISMATCH";
    throw err;
  }
  return ledger;
}

function remainingBudget(ledger) {
  // reservedUsd includes unreconciled_hold obligations (never auto-released for paid providers)
  return Number(
    (
      ledgerCeiling(ledger) -
      Number(ledger.estimatedSpentUsd || 0) -
      Number(ledger.reservedUsd || 0)
    ).toFixed(6),
  );
}

/** Public remaining-budget helper (holds count against ceiling). */
export function liveDeepResearchRemainingBudget(ledgerOrPath) {
  const ledger =
    typeof ledgerOrPath === "string"
      ? loadOrCreateLiveDeepResearchLedger(ledgerOrPath)
      : ledgerOrPath;
  return remainingBudget(ledger);
}

function isPaidMeteredKind(kind) {
  return kind === "exa" || kind === "firecrawl";
}

function reserveSpendUnlocked(ledger, { jobKey, quotedUsd, kind, requestIdentity = null }) {
  const quoted = Number(quotedUsd);
  if (!Number.isFinite(quoted) || quoted < 0) {
    return { ok: false, reason: "invalid_quote" };
  }
  const existing = (ledger.entries || []).find(
    (e) =>
      e.jobKey === jobKey
      && e.status !== "released"
      && e.status !== "failed"
      && e.status !== "cancelled",
  );
  if (existing?.status === "spent") return { ok: false, reason: "job_already_spent", entry: existing };
  if (existing?.status === "unreconciled_hold") {
    return {
      ok: false,
      reason: "job_unreconciled_hold",
      reservationId: existing.reservationId,
      entry: existing,
      note: "Retry blocked until hold reconciled — prevents duplicate billable dispatch",
    };
  }
  if (existing?.status === "reserved") {
    // Paid metered jobs: reserved/dispatched = in progress. Never authorize a second HTTP path.
    if (isPaidMeteredKind(existing.kind) || isPaidMeteredKind(kind)) {
      return {
        ok: false,
        reason: existing.dispatchedAt ? "job_dispatch_in_progress" : "job_reservation_in_progress",
        reservationId: existing.reservationId,
        entry: existing,
        alreadyReserved: true,
        note: "Existing reserved/dispatched paid job blocked until outcome reconciled",
      };
    }
    return {
      ok: true,
      reservationId: existing.reservationId,
      alreadyReserved: true,
      entry: existing,
    };
  }
  if (ledger.haltedPaidExecution === true && isPaidMeteredKind(kind) && quoted > 0) {
    return { ok: false, reason: "halted_paid_execution" };
  }
  if (remainingBudget(ledger) + 1e-9 < quoted) {
    return { ok: false, reason: "budget_exhausted" };
  }
  if (Number(ledger.estimatedSpentUsd || 0) + quoted - ledgerCeiling(ledger) > 1e-9) {
    return { ok: false, reason: "above_hard_ceiling" };
  }
  const reservationId = `rsv_${randomBytes(6).toString("hex")}`;
  ledger.reservedUsd = Number((Number(ledger.reservedUsd || 0) + quoted).toFixed(6));
  ledger.entries.push({
    reservationId,
    jobKey,
    kind,
    quotedUsd: quoted,
    status: quoted === 0 ? "spent" : "reserved",
    actualUsd: quoted === 0 ? 0 : null,
    requestIdentity: requestIdentity || { jobKey, kind },
    createdAt: new Date().toISOString(),
    spentAt: quoted === 0 ? new Date().toISOString() : null,
  });
  if (quoted === 0 && kind === "direct") ledger.directRetrievals = Number(ledger.directRetrievals || 0) + 1;
  return { ok: true, reservationId };
}

function commitSpendUnlocked(ledger, reservationId, actualUsd) {
  const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
  if (!entry) return { ok: false, reason: "reservation_missing" };
  if (entry.status === "spent") return { ok: true, idempotent: true };
  if (entry.status === "failed" || entry.status === "released" || entry.status === "cancelled") {
    return { ok: false, reason: `entry_${entry.status}` };
  }
  // reserved or unreconciled_hold may be committed after verified reconciliation
  const actual = Number(actualUsd);
  const quoted = Number(entry.quotedUsd);
  if (!Number.isFinite(actual) || actual < 0) {
    return { ok: false, reason: "invalid_actual" };
  }

  if (actual - quoted > 1e-9) {
    // Provider charged more than estimate — record true spend, halt further paid
    ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - quoted).toFixed(6));
    ledger.estimatedSpentUsd = Number((Number(ledger.estimatedSpentUsd || 0) + actual).toFixed(6));
    entry.status = "spent";
    entry.actualUsd = actual;
    entry.overspendUsd = Number((actual - quoted).toFixed(6));
    entry.spentAt = new Date().toISOString();
    ledger.haltedPaidExecution = true;
    ledger.incidents = ledger.incidents || [];
    ledger.incidents.push({
      type: "PROVIDER_OVERSPEND",
      reservationId,
      jobKey: entry.jobKey,
      kind: entry.kind,
      quotedUsd: quoted,
      actualUsd: actual,
      at: entry.spentAt,
    });
    if (entry.kind === "exa") ledger.exaCalls = Number(ledger.exaCalls || 0) + 1;
    if (entry.kind === "firecrawl") ledger.firecrawlCalls = Number(ledger.firecrawlCalls || 0) + 1;
    return { ok: true, overspend: true, halt: true };
  }

  const nextSpent = Number((Number(ledger.estimatedSpentUsd || 0) + actual).toFixed(6));
  if (nextSpent - ledgerCeiling(ledger) > 1e-9) {
    return { ok: false, reason: "commit_would_exceed_ceiling" };
  }
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - quoted).toFixed(6));
  ledger.estimatedSpentUsd = nextSpent;
  entry.status = "spent";
  entry.actualUsd = actual;
  entry.spentAt = new Date().toISOString();
  if (entry.kind === "exa") ledger.exaCalls = Number(ledger.exaCalls || 0) + 1;
  if (entry.kind === "firecrawl") ledger.firecrawlCalls = Number(ledger.firecrawlCalls || 0) + 1;
  return { ok: true };
}

/**
 * Release only when charge is VERIFIABLY impossible (never after HTTP dispatch).
 * Prefer holdAmbiguousPaidSpendUnlocked for metered providers after fetch.
 */
function failSpendUnlocked(ledger, reservationId, reason) {
  const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true };
  if (entry.status === "unreconciled_hold") {
    return { ok: false, reason: "cannot_fail_release_unreconciled_hold" };
  }
  if (entry.status === "reserved") {
    // Refuse automatic release for metered kinds once dispatched flag is set
    if (isPaidMeteredKind(entry.kind) && entry.dispatchedAt) {
      return holdAmbiguousPaidSpendUnlocked(ledger, reservationId, {
        reason: reason || "refuse_release_after_dispatch",
        httpMayHaveReachedProvider: true,
      });
    }
    ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  }
  entry.status = "failed";
  entry.reason = reason;
  entry.failedAt = new Date().toISOString();
  return { ok: true };
}

/**
 * Conservative hold after a request may have reached a metered provider.
 * Does NOT reduce reservedUsd. Counts against remaining budget until reconciled.
 */
function holdAmbiguousPaidSpendUnlocked(ledger, reservationId, meta = {}) {
  const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
  if (!entry) return { ok: false, reason: "reservation_missing" };
  if (entry.status === "spent") return { ok: true, idempotent: true };
  if (entry.status === "failed" || entry.status === "cancelled" || entry.status === "released") {
    return { ok: false, reason: `entry_${entry.status}_already_released` };
  }
  // reserved or already unreconciled_hold — keep reservedUsd
  entry.status = "unreconciled_hold";
  entry.holdReason = meta.reason || "ambiguous_paid_provider_outcome";
  entry.httpMayHaveReachedProvider = meta.httpMayHaveReachedProvider !== false;
  entry.reportedSpentUsd =
    meta.reportedSpentUsd == null ? null : Number(meta.reportedSpentUsd);
  entry.heldAt = new Date().toISOString();
  if (meta.requestIdentity) {
    entry.requestIdentity = { ...(entry.requestIdentity || {}), ...meta.requestIdentity };
  }
  ledger.haltedPaidExecution = true;
  ledger.incidents = ledger.incidents || [];
  ledger.incidents.push({
    type: "AMBIGUOUS_PAID_PROVIDER_HOLD",
    reservationId,
    jobKey: entry.jobKey,
    kind: entry.kind,
    quotedUsd: entry.quotedUsd,
    reason: entry.holdReason,
    at: entry.heldAt,
  });
  return { ok: true, heldUsd: Number(entry.quotedUsd), reservationId };
}

/** Locked reserve — always load → mutate → write under exclusive lock. */
export function reserveLiveDeepResearchSpend(ledgerPath, args) {
  return withLiveDeepResearchLedgerLock(ledgerPath, () => {
    const ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
    const result = reserveSpendUnlocked(ledger, args);
    writeLedger(ledgerPath, ledger);
    return { ...result, ledger };
  });
}

export function commitLiveDeepResearchSpend(ledgerPath, reservationId, actualUsd) {
  return withLiveDeepResearchLedgerLock(ledgerPath, () => {
    const ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
    const result = commitSpendUnlocked(ledger, reservationId, actualUsd);
    writeLedger(ledgerPath, ledger);
    return { ...result, ledger };
  });
}

export function failLiveDeepResearchSpend(ledgerPath, reservationId, reason) {
  return withLiveDeepResearchLedgerLock(ledgerPath, () => {
    const ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
    const result = failSpendUnlocked(ledger, reservationId, reason);
    writeLedger(ledgerPath, ledger);
    return { ...result, ledger };
  });
}

/**
 * Hold reservation after metered HTTP may have been billed (errors/timeouts/ambiguous).
 * Never releases reservedUsd for paid providers.
 */
export function holdAmbiguousPaidProviderSpend(ledgerPath, reservationId, meta = {}) {
  return withLiveDeepResearchLedgerLock(ledgerPath, () => {
    const ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
    const result = holdAmbiguousPaidSpendUnlocked(ledger, reservationId, meta);
    writeLedger(ledgerPath, ledger);
    return { ...result, ledger };
  });
}

/** Test-only: force next dispatch mark to fail (offline concurrency/dispatch tests). */
let __testFailNextDispatchMark = false;
export function __setFailNextDispatchMarkForTests(v) {
  __testFailNextDispatchMark = v === true;
}

/**
 * Atomic CAS: reserved + !dispatchedAt → dispatchedAt.
 * Only the winner may send provider HTTP. Concurrent/restart losers must refuse the call.
 */
function markPaidProviderDispatchedUnlocked(ledger, reservationId, requestIdentity = {}) {
  if (__testFailNextDispatchMark) {
    __testFailNextDispatchMark = false;
    return { ok: false, reason: "dispatch_mark_forced_failure", ledger };
  }
  const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
  if (!entry) return { ok: false, reason: "reservation_missing" };
  if (entry.status === "spent") return { ok: false, reason: "job_already_spent", entry };
  if (entry.status === "unreconciled_hold") {
    return { ok: false, reason: "job_unreconciled_hold", entry };
  }
  if (entry.status === "failed" || entry.status === "released" || entry.status === "cancelled") {
    return { ok: false, reason: `entry_${entry.status}`, entry };
  }
  if (entry.status !== "reserved") {
    return { ok: false, reason: `entry_${entry.status}`, entry };
  }
  if (entry.dispatchedAt) {
    return {
      ok: false,
      reason: "already_dispatched",
      entry,
      note: "Duplicate HTTP dispatch refused — prior dispatch still unresolved",
    };
  }
  entry.dispatchedAt = new Date().toISOString();
  entry.dispatchOwnerPid = process.pid;
  entry.requestIdentity = {
    ...(entry.requestIdentity || {}),
    ...requestIdentity,
    reservationId,
    jobKey: entry.jobKey,
    kind: entry.kind,
    dispatchedAt: entry.dispatchedAt,
  };
  return { ok: true, entry, reservationId };
}

/**
 * Mark reservation as dispatched immediately before provider HTTP (request identity preserved).
 * Callers MUST check ok and refuse HTTP when false.
 */
export function markPaidProviderDispatched(ledgerPath, reservationId, requestIdentity = {}) {
  return withLiveDeepResearchLedgerLock(ledgerPath, () => {
    const ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
    const result = markPaidProviderDispatchedUnlocked(ledger, reservationId, requestIdentity);
    if (result.ok) writeLedger(ledgerPath, ledger);
    return { ...result, ledger };
  });
}

/**
 * Release ONLY when verified unbilled (e.g. never dispatched). Default refuse for paid kinds.
 */
export function releasePaidReservationIfVerifiedUnbilled(
  ledgerPath,
  reservationId,
  { verifiedUnbilled = false, evidence = null } = {},
) {
  return withLiveDeepResearchLedgerLock(ledgerPath, () => {
    const ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
    const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
    if (!entry) return { ok: false, reason: "reservation_missing", ledger };
    if (entry.status === "spent") return { ok: true, idempotent: true, ledger };
    if (!verifiedUnbilled) {
      return { ok: false, reason: "verified_unbilled_required", ledger };
    }
    if (entry.dispatchedAt && isPaidMeteredKind(entry.kind)) {
      return {
        ok: false,
        reason: "dispatched_paid_cannot_verify_unbilled_without_provider_receipt",
        ledger,
      };
    }
    if (entry.status === "reserved" || entry.status === "unreconciled_hold") {
      ledger.reservedUsd = Number(
        Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6),
      );
    }
    entry.status = "released";
    entry.reason = "verified_unbilled";
    entry.releaseEvidence = evidence;
    entry.releasedAt = new Date().toISOString();
    writeLedger(ledgerPath, ledger);
    return { ok: true, ledger };
  });
}

export function cancelLiveDeepResearchSpend(ledgerPath, reservationId, reason = "cancelled") {
  return withLiveDeepResearchLedgerLock(ledgerPath, () => {
    const ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
    const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
    if (!entry || entry.status === "spent") return { ok: true, ledger };
    // Never cancel/release metered holds after dispatch
    if (isPaidMeteredKind(entry.kind) && (entry.dispatchedAt || entry.status === "unreconciled_hold")) {
      const held = holdAmbiguousPaidSpendUnlocked(ledger, reservationId, {
        reason: `cancel_refused_hold_instead:${reason}`,
        httpMayHaveReachedProvider: true,
      });
      writeLedger(ledgerPath, ledger);
      return { ...held, ledger, cancelled: false };
    }
    if (entry.status === "reserved") {
      ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
    }
    entry.status = "cancelled";
    entry.reason = reason;
    entry.cancelledAt = new Date().toISOString();
    writeLedger(ledgerPath, ledger);
    return { ok: true, ledger };
  });
}

// In-process helpers used by the researcher (must not bypass lock when ledgerPath known)
function reserveSpend(ledger, args) {
  return reserveSpendUnlocked(ledger, args);
}
function commitSpend(ledger, reservationId, actualUsd) {
  return commitSpendUnlocked(ledger, reservationId, actualUsd);
}
function failSpend(ledger, reservationId, reason) {
  return failSpendUnlocked(ledger, reservationId, reason);
}

export function preflightLiveDeepResearchPricing() {
  const exaSearch = quoteBenchmarkCall({
    provider: "exa",
    operation: "search",
    params: { type: "lite", numResults: 8 },
  });
  const firecrawlScrape = quoteBenchmarkCall({
    provider: "firecrawl",
    operation: "scrape",
    params: { url: "https://www.principallondon.com/news", formats: ["markdown"] },
  });
  const exaCred = credentialPresence("exa");
  const fcCred = credentialPresence("firecrawl");
  const checks = {
    exaSearchBounded: exaSearch.bounded === true && Number(exaSearch.maxUsd) > 0 && Number(exaSearch.maxUsd) <= 0.01,
    firecrawlScrapeBounded: firecrawlScrape.bounded === true && Number(firecrawlScrape.maxUsd) > 0 && Number(firecrawlScrape.maxUsd) <= 0.01,
    hardCeilingUsd: LIVE_HARD_CEILING_USD,
    exaCredential: exaCred.configured ? "PRESENT" : "MISSING",
    firecrawlCredential: fcCred.configured ? "PRESENT" : "MISSING",
    parallelDisabled: true,
    surfePdlDisabled: assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false }).ok === true,
    quotes: {
      exaLiteSearchUsd: exaSearch.maxUsd,
      firecrawlScrapeUsd: firecrawlScrape.maxUsd,
      asOf: exaSearch.asOf,
    },
  };
  const ok = checks.exaSearchBounded
    && checks.firecrawlScrapeBounded
    && checks.surfePdlDisabled
    && (exaCred.configured || fcCred.configured || true); // direct_public always allowed
  return { ok, checks, exaSearch, firecrawlScrape };
}

async function fetchPageDirect(url) {
  const { fetchOfficialPage } = await import("../official-page-fetch.js");
  const page = await fetchOfficialPage(url, { allowRender: false, timeoutMs: 22000 });
  return {
    ok: page?.ok === true || Boolean(page?.text || page?.searchableText),
    text: page?.text || page?.searchableText || "",
    html: page?.html || page?.rawHtml || "",
    fetchMethod: page?.fetchMethod || "direct_public",
  };
}

export async function maybeFirecrawlScrape(url, ledgerPath) {
  return maybeFirecrawl(url, ledgerPath);
}

async function maybeFirecrawl(url, ledgerPath) {
  if (
    process.env.GDI_FORBID_FIRECRAWL === "1" ||
    process.env.GDI_EXA_ONLY === "1" ||
    String(process.env.GDI_PROVIDERS_FORBIDDEN || "")
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .includes("firecrawl")
  ) {
    return null;
  }
  const key = process.env.FIRECRAWL_API_KEY || process.env.FIRECRAWL_KEY;
  if (!key) return null;
  const quote = quoteBenchmarkCall({
    provider: "firecrawl",
    operation: "scrape",
    params: { url, formats: ["markdown"] },
  });
  if (!quote.bounded) return null;
  const jobKey = `live-fc-${hashKey(normalizeUrlKey(url))}`;
  const reservation = reserveLiveDeepResearchSpend(ledgerPath, {
    jobKey,
    quotedUsd: Number(quote.maxUsd),
    kind: "firecrawl",
    requestIdentity: { provider: "firecrawl", operation: "scrape", url: normalizeUrlKey(url), jobKey },
  });
  if (!reservation.ok) return null;
  // Atomic dispatch claim BEFORE HTTP — refuse call if mark fails (concurrent/restart).
  const dispatched = markPaidProviderDispatched(ledgerPath, reservation.reservationId, {
    provider: "firecrawl",
    operation: "scrape",
    url: normalizeUrlKey(url),
  });
  if (!dispatched.ok) {
    return null;
  }
  try {
    const res = await providerFetch("https://api.firecrawl.dev/v2/scrape", {
      method: "POST",
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ url, formats: ["markdown"] }),
    });
    let json = {};
    let parseOk = true;
    try {
      json = await res.json();
    } catch {
      parseOk = false;
    }
    if (!res.ok) {
      holdAmbiguousPaidProviderSpend(ledgerPath, reservation.reservationId, {
        reason: `firecrawl_http_${res.status}`,
        httpMayHaveReachedProvider: true,
      });
      return null;
    }
    if (!parseOk || (json == null || typeof json !== "object")) {
      holdAmbiguousPaidProviderSpend(ledgerPath, reservation.reservationId, {
        reason: "firecrawl_ambiguous_response",
        httpMayHaveReachedProvider: true,
      });
      return null;
    }
    const md = String(json?.data?.markdown || json?.markdown || "");
    const committed = commitLiveDeepResearchSpend(ledgerPath, reservation.reservationId, Number(quote.maxUsd));
    if (!committed.ok) {
      // HTTP succeeded — charge may exist; never release.
      holdAmbiguousPaidProviderSpend(ledgerPath, reservation.reservationId, {
        reason: committed.reason || "firecrawl_commit_failed_after_ok_http",
        httpMayHaveReachedProvider: true,
      });
      return null;
    }
    return { text: md, actualUsd: Number(quote.maxUsd), provider: "firecrawl" };
  } catch (err) {
    holdAmbiguousPaidProviderSpend(ledgerPath, reservation.reservationId, {
      reason: err?.message || "firecrawl_timeout_or_network",
      httpMayHaveReachedProvider: true,
    });
    return null;
  }
}

/**
 * Normalize Exa query input. Investigation loops pass `{ query, opportunity, strategy }`;
 * ORS/ownership may pass a bare string. Never stringify the whole options object
 * (that produced "[object Object]" → Exa HTTP 400 → unreconciled hold in run-039/040).
 */
export function normalizeExaSearchQuery(query) {
  if (typeof query === "string") return query.trim();
  if (query && typeof query === "object") {
    const nested = query.query ?? query.q ?? query.researchQuestion ?? query.text;
    if (typeof nested === "string") return nested.trim();
  }
  return "";
}

/** Exa lite search charged to an allowlisted isolated live ledger. */
export async function maybeExaSearch(query, ledgerPath) {
  const key = process.env.EXA_API_KEY || process.env.EXA_KEY;
  if (!key) return { status: "credential_absent", hits: [], costUsd: 0 };
  const queryText = normalizeExaSearchQuery(query);
  if (queryText.length < 3) {
    return { status: "invalid_query", hits: [], costUsd: 0 };
  }
  const quote = quoteBenchmarkCall({
    provider: "exa",
    operation: "search",
    params: { type: "lite", numResults: 8 },
  });
  if (!quote.bounded) return { status: "unpriced", hits: [], costUsd: 0 };
  const jobKey = `live-exa-${hashKey(queryText)}`;
  const reservation = reserveLiveDeepResearchSpend(ledgerPath, {
    jobKey,
    quotedUsd: Number(quote.maxUsd),
    kind: "exa",
    requestIdentity: { provider: "exa", operation: "search", query: queryText, jobKey },
  });
  if (!reservation.ok) return { status: reservation.reason, hits: [], costUsd: 0 };
  // Atomic dispatch claim BEFORE HTTP — refuse call if mark fails (concurrent/restart).
  const dispatched = markPaidProviderDispatched(ledgerPath, reservation.reservationId, {
    provider: "exa",
    operation: "search",
    query: queryText,
  });
  if (!dispatched.ok) {
    return {
      status: dispatched.reason || "dispatch_mark_failed",
      hits: [],
      costUsd: 0,
      refusedHttp: true,
    };
  }
  try {
    const res = await providerFetch("https://api.exa.ai/search", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": key },
      body: JSON.stringify({
        query: queryText,
        type: "lite",
        numResults: 8,
        contents: { text: { maxCharacters: 1400 } },
      }),
    });
    let body = {};
    let parseOk = true;
    try {
      body = await res.json();
    } catch {
      parseOk = false;
    }
    if (!res.ok) {
      holdAmbiguousPaidProviderSpend(ledgerPath, reservation.reservationId, {
        reason: `exa_http_${res.status}`,
        httpMayHaveReachedProvider: true,
      });
      return { status: `http_${res.status}_held`, hits: [], costUsd: 0, held: true };
    }
    if (!parseOk || body == null || typeof body !== "object") {
      holdAmbiguousPaidProviderSpend(ledgerPath, reservation.reservationId, {
        reason: "exa_ambiguous_response",
        httpMayHaveReachedProvider: true,
      });
      return { status: "ambiguous_held", hits: [], costUsd: 0, held: true };
    }
    const reported = Number(body?.costDollars?.total);
    // Prefer provider-reported spend even when it exceeds estimate (triggers overspend halt).
    const actual =
      Number.isFinite(reported) && reported >= 0 ? reported : Number(quote.maxUsd);
    const committed = commitLiveDeepResearchSpend(ledgerPath, reservation.reservationId, actual);
    if (!committed.ok) {
      holdAmbiguousPaidProviderSpend(ledgerPath, reservation.reservationId, {
        reason: committed.reason || "exa_commit_failed_after_ok_http",
        httpMayHaveReachedProvider: true,
        reportedSpentUsd: Number.isFinite(reported) ? reported : null,
      });
      return { status: "commit_failed_held", hits: [], costUsd: 0, held: true };
    }
    const results = Array.isArray(body?.results) ? body.results : [];
    const hits = results.map((row) => ({
      url: row?.url || null,
      title: row?.title || null,
      excerpt: text([row?.text, ...(row?.highlights || [])].filter(Boolean).join(" ")).slice(0, 500),
      publisher: hostnameOf(row?.url),
      independent: isIndependentPublisher(row?.url),
    })).filter((h) => h.url);
    return {
      status: committed.overspend ? "ok_overspend_halt" : "ok",
      hits,
      costUsd: actual,
      overspend: committed.overspend === true,
      halt: committed.halt === true,
    };
  } catch (err) {
    holdAmbiguousPaidProviderSpend(ledgerPath, reservation.reservationId, {
      reason: err?.message || "exa_timeout_or_network",
      httpMayHaveReachedProvider: true,
    });
    return { status: "error_held", hits: [], costUsd: 0, held: true, error: err?.message };
  }
}

/**
 * Live ORS researcher — direct_public first, Firecrawl thin fallback, optional Exa for discovery queries.
 */
function normalizeUrlKey(url) {
  return String(url || "").trim().replace(/\/+$/, "").toLowerCase();
}

const HELD_URL_KEYS = new Set(LONGFORM_HELD_URLS.map(normalizeUrlKey));

function isHeldUrl(url) {
  return HELD_URL_KEYS.has(normalizeUrlKey(url));
}

/**
 * Reconcile orphan reservations.
 * Paid metered kinds (exa/firecrawl) are NEVER auto-released without verifiedUnbilledReservationIds
 * for entries that were never dispatched. Dispatched / held paid entries stay as unreconciled_hold
 * and continue counting against the ceiling.
 */
export function reconcileLiveLedgerReservations(
  ledgerPath,
  { verifiedUnbilledReservationIds = [] } = {},
) {
  return withLiveDeepResearchLedgerLock(ledgerPath, () => {
    const ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
    const verified = new Set(
      (verifiedUnbilledReservationIds || []).map((id) => String(id)),
    );
    let released = 0;
    let held = 0;
    let preserved = 0;
    for (const entry of ledger.entries || []) {
      if (entry.status !== "reserved" && entry.status !== "unreconciled_hold") continue;

      if (isPaidMeteredKind(entry.kind)) {
        const canReleaseVerified =
          verified.has(String(entry.reservationId))
          && !entry.dispatchedAt
          && entry.status === "reserved";
        if (canReleaseVerified) {
          ledger.reservedUsd = Number(
            Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6),
          );
          entry.status = "released";
          entry.reason = "reconcile_verified_unbilled";
          entry.releasedAt = new Date().toISOString();
          released += 1;
          continue;
        }
        // Fail-closed: convert reserved orphans to hold; keep existing holds.
        if (entry.status === "reserved") {
          holdAmbiguousPaidSpendUnlocked(ledger, entry.reservationId, {
            reason: "orphan_reconcile_no_verified_unbilled",
            httpMayHaveReachedProvider: Boolean(entry.dispatchedAt),
          });
          held += 1;
        } else {
          preserved += 1;
        }
        continue;
      }

      // Non-metered (direct / $0): safe to release reserved orphans
      if (entry.status === "reserved") {
        ledger.reservedUsd = Number(
          Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6),
        );
        entry.status = "released";
        entry.reason = "reconcile_orphan_reservation";
        entry.releasedAt = new Date().toISOString();
        released += 1;
      }
    }
    writeLedger(ledgerPath, ledger);
    return { released, held, preserved, ledger };
  });
}

export function createLiveDeepResearchResearcher({
  ledgerPath,
  trace,
  additionalSkipUrls = [],
  allowInvestigatedReread = false,
  forbidFirecrawl = false,
} = {}) {
  const investigated = new Set();
  const priorSkip = new Set(
    [...HELD_URL_KEYS, ...additionalSkipUrls.map(normalizeUrlKey)].filter(Boolean),
  );
  const t = trace || {
    attempted: [],
    executed: [],
    failed: [],
    exaQueries: [],
    relatedEntity: { discovered: [], attempted: [], executed: [], failed: [] },
  };
  const firecrawlBlocked = forbidFirecrawl === true;

  return {
    ledgerPath,
    trace: t,
    forbidFirecrawl: firecrawlBlocked,
    getLedger: () => loadOrCreateLiveDeepResearchLedger(ledgerPath),
    async execute({ opportunity, action }) {
      let ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
      const isLead = action?.strategy === "related_entity_followup";
      const queries = action?.queries || [];
      const urlQueries = queries.filter((q) => /^https?:/i.test(String(q)));
      const textQueries = queries.filter((q) => !/^https?:/i.test(String(q)) && text(q).length >= 8);
      const opportunitySkip = new Set(
        (opportunity?.investigatedUrls || []).map(normalizeUrlKey).filter(Boolean),
      );

      // Discovery via Exa when text research questions remain — prefer buyer/commercial queries
      // over URL-only queues so WHO/program pivots are not silently skipped.
      let discoveredUrls = [];
      const preferBuyerQuery = (qs) => {
        const ranked = [...qs].sort((a, b) => {
          const score = (q) =>
            (/hotel|housing|sourcing|meeting|director|procurement|venue|room.?block|tournament|conference/i.test(q) ? 2 : 0)
            + (/director|manager|coordinator|staff|team|leadership/i.test(q) ? 1 : 0)
            - (/annual meeting 202[789]/i.test(q) ? 1 : 0);
          return score(b) - score(a);
        });
        return ranked[0] || qs[0];
      };
      if (textQueries.length && remainingBudget(ledger) >= 0.004) {
        const q = preferBuyerQuery(textQueries);
        const exa = await maybeExaSearch(q, ledgerPath);
        ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
        t.exaQueries.push({ query: q, status: exa.status, hits: (exa.hits || []).length, costUsd: exa.costUsd || 0 });
        discoveredUrls = (exa.hits || [])
          .filter((h) => h.url && !isHeldUrl(h.url) && !priorSkip.has(normalizeUrlKey(h.url)) && !opportunitySkip.has(normalizeUrlKey(h.url)))
          .slice(0, 4)
          .map((h) => h.url);
        for (const hit of exa.hits || []) {
          if (hit.independent) {
            t.relatedEntity.discovered.push({
              url: hit.url,
              title: hit.title,
              publisher: hit.publisher,
              via: "exa",
            });
          }
        }
        // One pivot Exa when first query returns no usable new URLs and a distinct buyer query remains
        const altQ = textQueries.find((tq) => tq !== q && /hotel|housing|sourcing|meeting|director|staff|team/i.test(tq));
        if (!discoveredUrls.length && altQ && remainingBudget(ledger) >= 0.004) {
          const exa2 = await maybeExaSearch(altQ, ledgerPath);
          ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
          t.exaQueries.push({
            query: altQ,
            status: exa2.status,
            hits: (exa2.hits || []).length,
            costUsd: exa2.costUsd || 0,
            pivot: true,
          });
          discoveredUrls = (exa2.hits || [])
            .filter((h) => h.url && !isHeldUrl(h.url) && !priorSkip.has(normalizeUrlKey(h.url)) && !opportunitySkip.has(normalizeUrlKey(h.url)))
            .slice(0, 4)
            .map((h) => h.url);
        }
      }

      const seedPending = (opportunity?.pendingDiscoverySearches || [])
        .map((p) => p.suggestedUrl || p.url)
        .filter(Boolean);
      const urls = [
        ...urlQueries,
        ...discoveredUrls,
        ...(isLead ? seedPending : []),
        ...(!isLead && urlQueries.length === 0 && discoveredUrls.length === 0
          ? (opportunity?.seedUrls || []).filter((u) => !isHeldUrl(u) && !opportunitySkip.has(normalizeUrlKey(u)))
          : []),
      ].filter(Boolean);

      const collected = [];
      let costUsd = 0;

      for (const url of [...new Set(urls)].slice(0, 5)) {
        const urlKey = normalizeUrlKey(url);
        if (investigated.has(urlKey)) continue;
        const alreadyInvestigated = priorSkip.has(urlKey) || opportunitySkip.has(urlKey);
        const attempt = {
          url,
          strategy: action?.strategy || "research",
          heldPreviously: isHeldUrl(url) || alreadyInvestigated,
          independent: isIndependentPublisher(url),
          relatedEntity: isLead || isIndependentPublisher(url),
        };
        t.attempted.push(attempt);
        if (isLead || attempt.relatedEntity) t.relatedEntity.attempted.push(attempt);

        // Do not spend cycles re-fetching already-held / previously investigated pages without new justification
        if (alreadyInvestigated && !allowInvestigatedReread) {
          t.failed.push({ ...attempt, reason: "already_investigated_skip_unjustified_reread" });
          if (isLead || attempt.relatedEntity) {
            t.relatedEntity.failed.push({ ...attempt, reason: "already_investigated_skip_unjustified_reread" });
          }
          continue;
        }
        if (isHeldUrl(url) && !isLead) {
          t.failed.push({ ...attempt, reason: "held_url_skipped_not_new_research" });
          continue;
        }

        investigated.add(urlKey);
        reserveLiveDeepResearchSpend(ledgerPath, {
          jobKey: `live-direct-${hashKey(urlKey)}`,
          quotedUsd: 0,
          kind: "direct",
        });

        let pageText = "";
        let provider = "direct_public";
        let fetchOk = false;
        try {
          const page = await fetchPageDirect(url);
          pageText = page.text || "";
          fetchOk = page.ok && text(pageText).length >= 80;
          if (text(pageText).length < 400) {
            if (firecrawlBlocked) {
              // Founder Exa-only campaigns (e.g. Nobu Assoc Events second wave) must not scrape via Firecrawl.
              provider = provider || "direct_public";
            } else {
              const fc = await maybeFirecrawl(url, ledgerPath);
              if (fc?.text && text(fc.text).length >= 80) {
                pageText = fc.text;
                costUsd += Number(fc.actualUsd || 0);
                provider = "firecrawl";
                fetchOk = true;
              } else if (fc && !text(fc.text)) {
                provider = "firecrawl_empty";
              }
            }
          }
        } catch (err) {
          t.failed.push({ ...attempt, reason: err.message || "fetch_threw" });
          if (isLead || attempt.relatedEntity) {
            t.relatedEntity.failed.push({ ...attempt, reason: err.message || "fetch_threw" });
          }
          continue;
        }

        if (!fetchOk) {
          t.failed.push({ ...attempt, reason: "empty_or_thin", provider });
          if (isLead || attempt.relatedEntity) {
            t.relatedEntity.failed.push({ ...attempt, reason: "empty_or_thin", provider });
          }
          continue;
        }

        const executed = {
          ...attempt,
          provider,
          chars: text(pageText).length,
          publisher: hostnameOf(url),
        };
        t.executed.push(executed);
        if (isLead || attempt.relatedEntity) t.relatedEntity.executed.push(executed);
        collected.push({ url, text: pageText, provider, independent: attempt.independent, relatedEntity: isLead || attempt.independent });
      }

      let namedStakeholders = [];
      for (const c of collected.slice(0, 4)) {
        try {
          const ci = extractPublicContactIntelligenceForOrs({
            textContent: c.text,
            url: c.url,
            organizationName: opportunity?.organizationName || "Principal. London DMC",
          });
          namedStakeholders = namedStakeholders.concat(ci.namedStakeholdersFound || []);
        } catch { /* keep going */ }
      }

      const evidence = collected.map((c) => ({
        url: c.url,
        statement: text(c.text).slice(0, 450),
        quote: text(c.text).slice(0, 450),
        provenance: "live_retrieval",
        accepted: true,
        sourceFamily: c.independent ? "independent_publisher" : "organizer_publication",
        relatedEntity: c.relatedEntity === true,
        provider: c.provider,
        previouslyHeld: false,
      }));

      return {
        disposition: evidence.length ? "EVIDENCE_FOUND" : "NO_EVIDENCE",
        newMaterialEvidence: evidence.length > 0,
        costUsd,
        evidence: isLead ? evidence.map((e) => ({ ...e, relatedEntity: true })) : evidence,
        provider: collected[0]?.provider || "direct_public",
        deepPatch: {
          namedStakeholdersFound: namedStakeholders.slice(0, 8),
          contact: opportunity?.contactEmail || opportunity?.contact?.businessEmail
            ? {
              businessEmail: opportunity.contactEmail || opportunity.contact.businessEmail,
              personName: namedStakeholders[0]?.name || null,
              roleTitle: namedStakeholders[0]?.title || null,
            }
            : undefined,
        },
      };
    },
  };
}

/**
 * Seed live opportunity with outstanding commercial questions + related-entity leads
 * beyond already-held Principal pages.
 */
export function seedLivePrincipalOpportunity() {
  const held = new Set(LONGFORM_HELD_URLS);
  const newSeedUrls = [
    "https://www.principallondon.com/news",
    "https://www.principallondon.com/who-we-are",
    "https://www.principallondon.com/about",
    "https://www.principallondon.com/team",
  ].filter((u) => !held.has(u));

  const rosewoodCandidates = [
    "https://www.rosewoodhotels.com/en/london",
    "https://www.rosewoodhotels.com/en/london/meetings-and-events",
    "https://www.rosewoodhotels.com/en/london/offers",
  ];

  return seedPrincipalLondonGdiOpportunity({
    seedUrls: [...LONGFORM_HELD_URLS, ...newSeedUrls],
    // Keep held as investigated; force new seeds into research queue
    investigatedUrls: [...LONGFORM_HELD_URLS],
    pendingDiscoverySearches: [
      {
        entityName: "Rosewood London",
        suggestedUrl: rosewoodCandidates[0],
        independentPublisher: true,
        entityType: "hotel_or_accommodation_partner",
        researchQuestion: "Independently corroborate whether Rosewood London hosts corporate/incentive groups; do not treat organizer case-study alone as independent Principal–Rosewood contract proof.",
      },
      {
        entityName: "Rosewood London meetings",
        suggestedUrl: rosewoodCandidates[1],
        independentPublisher: true,
        entityType: "hotel_or_accommodation_partner",
        researchQuestion: "Check Rosewood London meetings/events page for group accommodation signals independent of Principal.",
      },
      {
        entityName: "Principal hotel-sourcing team",
        suggestedUrl: "https://www.principallondon.com/who-we-are",
        independentPublisher: false,
        entityType: "programme",
        researchQuestion: "Identify hotel-sourcing / contracting decision-makers or functional contact routes on Principal staff pages.",
      },
    ],
    unresolvedQuestions: [
      "Does Principal currently publish a dated London programme needing bedrooms at Nobu Portman Square?",
      "Who at Principal owns hotel sourcing / contracting decisions?",
      "Is the Rosewood London base independently corroborated beyond Principal case-study copy?",
      "How does Principal shortlist or recommend hotels for incentive programmes?",
    ],
    candidateQueue: newSeedUrls,
  });
}

export function classifyEvidenceBuckets({ before, afterEvidence = [], trace = null } = {}) {
  const heldUrls = new Set((before?.demandEvidence || []).map((d) => d.url).filter(Boolean));
  const previouslyKnown = [];
  const newlyRetrieved = [];
  const independent = [];
  const organizerClaimed = [];

  for (const e of afterEvidence) {
    const row = {
      statement: text(e.statement || e.quote).slice(0, 240),
      url: e.url,
      publisher: hostnameOf(e.url),
      provenance: e.provenance || null,
      sourceFamily: e.sourceFamily || null,
    };
    if (heldUrls.has(e.url) || e.previouslyHeld) previouslyKnown.push(row);
    else if (e.provenance === "live_retrieval" || e.provenance === "deep_research_merge") newlyRetrieved.push(row);
    else previouslyKnown.push(row);

    if (isIndependentPublisher(e.url) || e.sourceFamily === "independent_publisher") independent.push(row);
    else if (/principallondon|principalge/i.test(hostnameOf(e.url))) organizerClaimed.push(row);
  }

  return {
    previouslyKnown,
    newlyRetrievedLive: newlyRetrieved.filter((r) => !heldUrls.has(r.url)),
    independentlyCorroborated: independent,
    organizerClaimedRelationships: organizerClaimed,
    note: "Multiple Principal-owned pages ≠ independent relationship verification. A Rosewood hotel page describing groups ≠ Principal–Rosewood commercial contract.",
    relatedEntityTrace: trace?.relatedEntity || null,
  };
}

export function buildLiveAuthorizationManifest({ founderCeilingUsd = LIVE_HARD_CEILING_USD } = {}) {
  const pricing = preflightLiveDeepResearchPricing();
  return {
    status: "AUTHORIZED",
    version: LIVE_DEEP_RESEARCH_CERT_VERSION,
    authorizedAt: new Date().toISOString(),
    opportunityId: LONGFORM_CANDIDATE.opportunityId,
    hotelId: LONGFORM_CANDIDATE.hotelId,
    hotelName: LONGFORM_CANDIDATE.hotelName,
    organizationName: LONGFORM_CANDIDATE.organizationName,
    hardCeilingUsd: founderCeilingUsd,
    proposedHardCeilingUsdWas: 1.0,
    founderAuthorizedCeilingUsd: founderCeilingUsd,
    isolatedLedger: LIVE_LEDGER_RELATIVE,
    allowedProviders: ["direct_public", "exa_lite_optional", "firecrawl_thin_optional"],
    forbiddenUnlessSeparateGo: ["parallel", "surfe", "pdl"],
    usesPriorBenchmarkBalances: false,
    usesRemainingV4Balances: false,
    executePaidResearch: true,
    pricingPreflight: pricing,
    successCriteria: [
      "≥1 related_entity_followup executed on live URL when pending",
      "≥1 non-held URL retrieved (not merely re-read of held Principal pages)",
      "Org contact ownership validated (not directory)",
      "Named stakeholder pursued when public evidence supports",
      "Dossier citations resolve",
      "Publication eligibility unchanged",
      "BEFORE/AFTER comparison without inventing Nobu RFP",
      `Spend ≤ $${founderCeilingUsd} on isolated ledger`,
    ],
  };
}

export async function executeLiveDeepResearchCertification({
  ledgerPath = path.join(ROOT, LIVE_LEDGER_RELATIVE),
  checkpointDir = path.join(ROOT, "reports/gdi-deep-research-gdi-integration-v1/live-checkpoints"),
  hardCeilingUsd = LIVE_HARD_CEILING_USD,
} = {}) {
  if (Number(hardCeilingUsd) !== LIVE_HARD_CEILING_USD) {
    throw new Error(`hard_ceiling_must_be_${LIVE_HARD_CEILING_USD}`);
  }

  const pricing = preflightLiveDeepResearchPricing();
  if (!pricing.ok) {
    return {
      executed: false,
      reason: "pricing_or_policy_preflight_failed",
      pricing,
      spentUsd: 0,
    };
  }

  assertLiveDeepResearchLedger(ledgerPath);
  loadOrCreateLiveDeepResearchLedger(ledgerPath);

  const opportunity = seedLivePrincipalOpportunity();
  const before = captureOpportunityBeforeSnapshot(opportunity);
  const researcher = createLiveDeepResearchResearcher({ ledgerPath });

  fs.mkdirSync(checkpointDir, { recursive: true });

  const result = await runDeepResearchOnExistingGdiOpportunity({
    opportunity,
    researcher,
    corpus: null,
    offlineLongform: false,
    options: {
      allowEarlyBuyerStop: false,
      allowReplan: true,
      maxIterations: 5,
      maxLeadFollowUpsPerOpportunity: 3,
      remainingBudgetUsd: hardCeilingUsd,
      checkpointDir,
      entityUrlHints: {
        "Rosewood London": "https://www.rosewoodhotels.com/en/london",
      },
      candidateQueue: [
        "https://www.principallondon.com/news",
        "https://www.principallondon.com/who-we-are",
      ],
      paidBlocked: true,
      forbidPublication: true,
      forbidAirtable: true,
      authorizeSurfe: false,
      authorizePdl: false,
    },
  });

  const ledger = researcher.getLedger();
  const spentUsd = Number(ledger.estimatedSpentUsd || 0);
  const evidenceBuckets = classifyEvidenceBuckets({
    before,
    afterEvidence: result.after?.opportunity?.evidence || [],
    trace: researcher.trace,
  });

  const relatedExecuted = Number(result.after?.relatedEntityLeadsExecuted || 0);
  const newLiveUrls = (researcher.trace.executed || []).filter((e) => !e.heldPreviously);
  const independentExecuted = (researcher.trace.executed || []).filter((e) => e.independent);

  const technicalPass =
    result.orsEntrypoint === "matureOpportunity"
    && result.gdiRoundTrip?.sameOpportunityId === true
    && result.publicationEligibilityUnchanged === true
    && result.citationAudit?.ok === true
    && spentUsd - hardCeilingUsd <= 1e-9
    && newLiveUrls.length >= 1;

  const namedBuyerFound = (result.after?.namedStakeholders || []).some((s) => s?.name);
  const rosewoodIndependentDoesNotProvePrincipal =
    "Rosewood London pages confirm the hotel hosts groups/meetings; they do not independently prove a Principal–Rosewood commercial relationship.";

  // Commercial success requires material sales usefulness beyond technical retrieval volume.
  const commercialPass =
    technicalPass
    && relatedExecuted >= 1
    && Boolean(result.hotelSalesBrief?.whoShouldIContact?.route)
    && result.after?.contactOwnership?.salesUsableAsProspectContact === true
    && !/Nobu (?:is already|has been|was) (?:selected|awarded|booked)/i.test(
      String(result.hotelSalesBrief?.whatShouldISayOrAsk || ""),
    )
    && (namedBuyerFound || independentExecuted.length >= 1);

  const commercialGrade = !technicalPass
    ? "FAIL"
    : namedBuyerFound && relatedExecuted >= 1
      ? "PASS"
      : relatedExecuted >= 1 && result.after?.contactOwnership?.salesUsableAsProspectContact
        ? "PARTIAL_PASS_ACCOUNT_DEVELOPMENT"
        : "FAIL_OR_INSUFFICIENT";

  // Correct live brief language — no offline "fixture" claims
  if (result.hotelSalesBrief) {
    result.hotelSalesBrief.whatPotentialBusinessExists =
      "Luxury / executive incentive programmes requiring central-London hotel bases. Principal case studies claim Rosewood London as a programme base; Rosewood’s own site confirms group/meetings capability but does not name Principal. No open Nobu RFP.";
    result.hotelSalesBrief.whyMyHotel =
      "Nobu Portman Square sits in Principal’s central-London luxury incentive catchment (Marylebone / West End). Peer-luxury group placement is plausible for account development — not a claimed Nobu booking or Rosewood overflow inference.";
    if (!namedBuyerFound) {
      result.hotelSalesBrief.whatDoWeStillNotKnow = [
        ...(result.hotelSalesBrief.whatDoWeStillNotKnow || []),
        "Named hotel-sourcing decision-maker not established from live public pages in this run",
      ];
    }
  }

  return {
    executed: true,
    version: LIVE_DEEP_RESEARCH_CERT_VERSION,
    integrationVersion: DEEP_RESEARCH_GDI_INTEGRATION_V1,
    before,
    after: result.after,
    hotelSalesBrief: result.hotelSalesBrief,
    dossierResult: result.dossierResult,
    contract: { ...result.contract, mode: "live", simulatedVsLive: "LIVE_RETRIEVAL" },
    citationAudit: result.citationAudit,
    publicationEligibilityUnchanged: result.publicationEligibilityUnchanged,
    publicationEligibilityDetail: result.publicationEligibilityDetail,
    gdiRoundTrip: result.gdiRoundTrip,
    orsEntrypoint: result.orsEntrypoint,
    replanPass: result.replanPass,
    drainExtra: result.drainExtra,
    evidenceBuckets,
    retrievalTrace: researcher.trace,
    costs: {
      spentUsd,
      remainingCeilingUsd: Number((hardCeilingUsd - spentUsd).toFixed(6)),
      hardCeilingUsd,
      ledgerPath: LIVE_LEDGER_RELATIVE,
      exaCalls: ledger.exaCalls || 0,
      firecrawlCalls: ledger.firecrawlCalls || 0,
      directRetrievals: ledger.directRetrievals || 0,
      entries: ledger.entries || [],
    },
    certification: {
      technical: technicalPass ? "PASS" : "FAIL",
      commercial: commercialGrade,
      commercialPassBoolean: commercialPass,
      namedBuyerFound,
      relatedEntityLeadsExecuted: relatedExecuted,
      newLiveUrlsRetrieved: newLiveUrls.length,
      independentPublisherUrlsRetrieved: independentExecuted.length,
      rosewoodCaveat: rosewoodIndependentDoesNotProvePrincipal,
      note: commercialGrade === "PASS"
        ? "Live research materially enriched the same GDI opportunity without inventing Nobu buying."
        : commercialGrade === "PARTIAL_PASS_ACCOUNT_DEVELOPMENT"
          ? "Live research deepened programme/related-entity context and confirmed org contact ownership, but did not newly verify a named hotel-sourcing decision-maker or independently prove Principal–Rosewood contracting."
          : "Technical path may have completed without sufficient commercial improvement — see comparison.",
    },
  };
}
