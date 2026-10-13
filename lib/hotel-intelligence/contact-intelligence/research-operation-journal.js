/**
 * Operation journal helpers for ownership research execution boundaries.
 * Statuses: RESERVED | IN_FLIGHT | COMPLETED | FAILED | IN_FLIGHT_UNKNOWN
 *
 * v2: collision-resistant keys, durable sanitized results, replay on resume,
 * explicit provider-level failures (not assumed free).
 */

import crypto from "node:crypto";
import {
  resolveContextDevCreditReservation,
  readProviderConfirmedCredits,
  resolveDocumentedScrapeBillingGuarantee,
} from "../../context-dev/operation-credit-bounds.js";
import { computeAuthoritativeContextDevSpendFromJournal } from "./context-dev-journal-spend.js";

export const RESEARCH_OP_JOURNAL_VERSION = "research-op-journal-v2";
export {
  resolveContextDevCreditReservation,
  readProviderConfirmedCredits,
  resolveDocumentedScrapeBillingGuarantee,
};
export { computeAuthoritativeContextDevSpendFromJournal } from "./context-dev-journal-spend.js";

export const OP_STATUS = Object.freeze({
  RESERVED: "RESERVED",
  IN_FLIGHT: "IN_FLIGHT",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
  IN_FLIGHT_UNKNOWN: "IN_FLIGHT_UNKNOWN",
});

/**
 * Collision-resistant work key over provider, operation, and request params.
 * Preserves case-sensitive URL paths and query values (no lowercasing of URLs).
 */
export function buildOperationWorkKey({
  provider,
  operation,
  query = null,
  url = null,
  extra = null,
} = {}) {
  const canonical = {
    provider: String(provider || "unknown"),
    operation: String(operation || "unknown"),
    // Queries: normalize whitespace only — preserve case for distinctiveness hash input
    // but include both raw and folded forms so "Foo" vs "foo" still collide intentionally
    // for search dedupe of equivalent ownership queries:
    query: query == null ? null : String(query).replace(/\s+/g, " ").trim(),
    // URLs: preserve case-sensitive path/query exactly
    url: url == null ? null : String(url).trim(),
    extra: extra == null ? null : extra,
  };
  const payload = JSON.stringify(canonical);
  const digest = crypto.createHash("sha256").update(payload, "utf8").digest("hex").slice(0, 24);
  const label = [canonical.provider, canonical.operation, digest].join(":");
  return label.slice(0, 180);
}

/** @deprecated Use buildOperationWorkKey — kept for reading old journal entries */
export function normalizeWorkKey(kind, value) {
  return buildOperationWorkKey({
    provider: "legacy",
    operation: kind,
    query: kind === "search" || kind === "serpapi" ? value : null,
    url: kind === "scrape" ? value : null,
  });
}

/**
 * True when provider result / source / evidence row is usage-rights BLOCKED.
 * Used at collection + staging boundaries — not only journal sanitization.
 */
export function isUsageRightsBlocked(obj = {}, meta = {}) {
  const resultRights = String(
    obj?.usage_rights || obj?.usageRights || obj?.license || ""
  ).toUpperCase();
  const metaRights = String(meta.usage_rights || "").toUpperCase();
  return (
    metaRights === "BLOCKED" ||
    meta.usage_rights_blocked === true ||
    resultRights === "BLOCKED" ||
    obj?.restricted === true ||
    obj?.usage_rights_blocked === true ||
    meta.trusted_metadata?.usage_rights === "BLOCKED" ||
    String(meta.trusted_metadata?.usage_rights || "").toUpperCase() === "BLOCKED"
  );
}

/**
 * Restore cumulative budget counters from case + journal for a resumed caller.
 * Replay/resume must never start from zeros when prior spend exists.
 */
/**
 * Outstanding reservations derived from last operation status per work_key.
 * Durable results may be replayable while billing remains unsettled —
 * do not treat durable_result alone as settled spend.
 */
export function outstandingReservationsFromJournal(operationJournal = []) {
  const lastByKey = new Map();
  for (const e of operationJournal || []) {
    if (!e?.work_key) continue;
    lastByKey.set(e.work_key, e);
  }
  let open_reservations = 0;
  let serpapi_reserved = 0;
  let context_dev_outstanding = 0;
  for (const [, e] of lastByKey) {
    const billingUnsettled =
      e.reservation?.unsettled === true ||
      e.billing_settled === false ||
      e.status === OP_STATUS.IN_FLIGHT_UNKNOWN ||
      e.event === "provider_failed_billing_unknown" ||
      e.event === "provider_success_billing_unknown" ||
      e.event === "call_interrupted_or_failed" ||
      e.event === "persistence_failed_after_provider_success" ||
      e.settle_source === "unknown_retain_liability" ||
      (e.settle_source &&
        String(e.settle_source).includes("KEY_METADATA") &&
        e.settled_credits == null);
    const inFlightUnsettled =
      e.status === OP_STATUS.RESERVED ||
      (e.status === OP_STATUS.IN_FLIGHT &&
        e.event !== "durable_result_written" &&
        e.settled_credits == null &&
        !e.durable_result);
    if (!billingUnsettled && !inFlightUnsettled) continue;
    // Settled terminal with confirmed/documented credits — not outstanding
    if (
      !billingUnsettled &&
      (e.status === OP_STATUS.COMPLETED || e.status === OP_STATUS.FAILED) &&
      e.settled_credits != null
    ) {
      continue;
    }
    open_reservations += 1;
    const provider = String(e.provider || e.reservation?.provider || "").toLowerCase();
    const credits = Number(e.reservation_credits ?? e.reservation?.credits ?? 1);
    if (provider === "serpapi") {
      serpapi_reserved += 1;
    } else if (provider === "context_dev" || /context/.test(provider) || !provider) {
      context_dev_outstanding += Number.isFinite(credits) ? credits : 1;
    }
  }
  return { open_reservations, serpapi_reserved, context_dev_outstanding };
}

/**
 * Authoritative Context.dev settled spend from the operation journal.
 * Prefers the journaler's own budget_usage snapshots (updated only on settleSpend
 * for real dispatches — replays/skips do not increment). Unique work-key
 * enumeration is a cross-check; outstanding liability stays separate.
 *
 * Legacy journals without budget_usage snapshots: hasCompleteJournal=false.
 */
export function summarizeContextDevJournalAccounting(operationJournal = []) {
  const journal = Array.isArray(operationJournal) ? operationJournal : [];
  let snapshotSpent = 0;
  let snapshotCalls = 0;
  let hasBudgetSnapshots = false;
  let hasOpKeys = false;
  const lastByKey = new Map();
  let replay_events = 0;

  for (const e of journal) {
    if (e?.work_key) {
      hasOpKeys = true;
      lastByKey.set(e.work_key, e);
    }
    if (/replay/i.test(String(e?.event || ""))) replay_events += 1;
    const bu = e?.budget_usage;
    if (bu && typeof bu === "object") {
      hasBudgetSnapshots = true;
      snapshotSpent = Math.max(snapshotSpent, Number(bu.context_dev_spent || 0));
      snapshotCalls = Math.max(snapshotCalls, Number(bu.context_dev_calls || 0));
    }
  }

  // Cross-check: unique context_dev work keys that reached a billed terminal state
  let uniqueSettledCalls = 0;
  let uniqueSettledCredits = 0;
  const settled_work_keys = [];
  for (const [workKey, e] of lastByKey) {
    const provider = String(e.provider || e.reservation?.provider || "").toLowerCase();
    const isContext =
      provider === "context_dev" ||
      (/context/.test(provider) && provider !== "serpapi") ||
      /^context_dev:/.test(String(workKey));
    if (!isContext) continue;
    if (
      e.kind === "skip" ||
      e.event === "replay_stored_result" ||
      e.event === "already_completed_missing_result" ||
      e.event === "replay_restricted_blocked" ||
      e.event === "dispatch_blocked_by_gate" ||
      e.event === "dispatch_blocked_persistence_failed"
    ) {
      continue;
    }
    const billed =
      e.event === "call_completed" ||
      e.event === "provider_failed" ||
      e.event === "provider_blocked_restricted" ||
      e.event === "durable_result_written" ||
      e.status === OP_STATUS.COMPLETED ||
      e.status === OP_STATUS.FAILED;
    if (!billed) continue;
    // Prefer last settlement-like row; skip pure RESERVED/IN_FLIGHT without durable result
    if (
      e.status === OP_STATUS.RESERVED ||
      (e.status === OP_STATUS.IN_FLIGHT && !e.durable_result && e.event !== "durable_result_written")
    ) {
      continue;
    }
    const credits = (() => {
      const confirmed = readProviderConfirmedCredits(e.durable_result || e);
      if (confirmed.ok) return confirmed.credits;
      if (e.settled_credits != null && Number.isFinite(Number(e.settled_credits))) {
        return Number(e.settled_credits);
      }
      // Do not invent spend from reservation when provider metadata is absent.
      return NaN;
    })();
    if (!Number.isFinite(credits) || credits < 0) continue;
    uniqueSettledCredits += credits;
    uniqueSettledCalls += 1;
    settled_work_keys.push(workKey);
  }

  const authoritative = computeAuthoritativeContextDevSpendFromJournal(journal);
  // Prefer complete operation-level provider evidence over inflated snapshots.
  const settled_credits =
    authoritative.ok && authoritative.complete
      ? authoritative.settled_credits
      : hasBudgetSnapshots
        ? snapshotSpent
        : uniqueSettledCredits;
  const settled_calls =
    authoritative.ok && authoritative.complete
      ? authoritative.settled_calls
      : hasBudgetSnapshots
        ? snapshotCalls
        : uniqueSettledCalls;

  const outstanding = outstandingReservationsFromJournal(journal);
  return {
    hasCompleteJournal: hasBudgetSnapshots || hasOpKeys,
    settled_credits,
    settled_calls,
    settled_work_keys:
      authoritative.ok && authoritative.complete
        ? authoritative.ops.filter((o) => o.credited != null).map((o) => o.work_key)
        : settled_work_keys,
    unique_op_credits: authoritative.ok ? authoritative.settled_credits : uniqueSettledCredits,
    unique_op_calls: authoritative.ok ? authoritative.settled_calls : uniqueSettledCalls,
    snapshot_spent: snapshotSpent,
    snapshot_calls: snapshotCalls,
    replay_events,
    outstanding_credits: Math.max(
      outstanding.context_dev_outstanding,
      authoritative.outstanding_credits || 0
    ),
    open_reservations: outstanding.open_reservations,
    authoritative,
  };
}

/**
 * Reconcile case Context.dev spend without mixing absolute journal totals and
 * incremental call-log deltas. Call-log length is never a spend proxy.
 *
 * @param {object} args
 * @param {number} args.priorSpent
 * @param {number} args.priorCalls
 * @param {"remaining"|"cumulative"} args.allowanceMode
 * @param {object|null} args.ledgerSnapshot - createContextDevCreditLedger.snapshot()
 * @param {Array} args.operationJournal
 * @param {number} [args.orchestrationCallCount] - informational only
 */
export function reconcileContextDevCaseSpend({
  priorSpent = 0,
  priorCalls = 0,
  allowanceMode = "cumulative",
  ledgerSnapshot = null,
  operationJournal = [],
  orchestrationCallCount = null,
} = {}) {
  const prior = Math.max(0, Number(priorSpent) || 0);
  const priorCallN = Math.max(0, Number(priorCalls) || 0);
  const mode = String(allowanceMode || "cumulative");
  const journalAcct = summarizeContextDevJournalAccounting(operationJournal);
  const ledgerSpent = Number(
    ledgerSnapshot?.spent_credits ?? ledgerSnapshot?.spent ?? NaN
  );
  const ledgerIncrementalOk = Number.isFinite(ledgerSpent);

  let settled_absolute;
  let settled_calls_absolute;
  let source;

  if (journalAcct.hasCompleteJournal && journalAcct.settled_credits >= prior) {
    // Journal unique ops cover (at least) prior settled spend — authoritative absolute.
    settled_absolute = journalAcct.settled_credits;
    settled_calls_absolute = journalAcct.settled_calls;
    source = "operation_journal_unique_ops";
  } else if (mode === "remaining" && ledgerIncrementalOk) {
    // Remaining-mode ledger is this-run incremental; prior counted once.
    settled_absolute = prior + ledgerSpent;
    settled_calls_absolute = priorCallN + (journalAcct.hasCompleteJournal
      ? Math.max(0, journalAcct.settled_calls - priorCallN)
      : Number(ledgerSnapshot?.entries?.filter((e) => Number(e.cost) > 0).length || 0));
    source = "remaining_prior_plus_ledger_incremental";
  } else if (journalAcct.hasCompleteJournal) {
    // Journal below prior ⇒ prior includes legacy spend absent from journal keys.
    settled_absolute = prior + journalAcct.settled_credits;
    settled_calls_absolute = priorCallN + journalAcct.settled_calls;
    source = "prior_plus_journal_legacy_gap";
  } else if (ledgerIncrementalOk) {
    settled_absolute = mode === "remaining" ? prior + ledgerSpent : Math.max(prior, ledgerSpent);
    settled_calls_absolute = Math.max(
      priorCallN,
      Number(ledgerSnapshot?.entries?.filter((e) => Number(e.cost) > 0).length || ledgerSpent)
    );
    source = "legacy_ledger_fallback";
  } else {
    settled_absolute = prior;
    settled_calls_absolute = priorCallN;
    source = "prior_only_conservative";
  }

  return {
    context_dev_spent: settled_absolute,
    context_dev_calls: settled_calls_absolute,
    outstanding_credits: journalAcct.outstanding_credits,
    open_reservations: journalAcct.open_reservations,
    reconciliation_source: source,
    journal: journalAcct,
    ledger_spent_credits: ledgerIncrementalOk ? ledgerSpent : null,
    orchestration_call_count:
      orchestrationCallCount == null ? null : Number(orchestrationCallCount) || 0,
    note:
      "Settled spend derives from uniquely identified provider operations; orchestration/replay call-log length is not a spend proxy.",
  };
}

/**
 * Restore budget counters from case + journal for a resumed caller.
 * When operation-level provider credits are complete (incl. explicit 0),
 * they override inflated aggregate budget_usage snapshots.
 * Outstanding reservations are NOT cumulative — derived from operation state
 * (preferred) or the latest journal budget_usage snapshot (including zeros).
 */
export function restoreBudgetUsageFromJournal(priorBudget = {}, operationJournal = []) {
  const out = {
    context_dev_reserved: Number(priorBudget.context_dev_reserved || 0),
    context_dev_spent: Number(priorBudget.context_dev_spent || 0),
    context_dev_calls: Number(priorBudget.context_dev_calls || 0),
    context_dev_estimated_unconfirmed: Number(priorBudget.context_dev_estimated_unconfirmed || 0),
    context_dev_dispatch_blocked: priorBudget.context_dev_dispatch_blocked === true,
    context_dev_block_reason: priorBudget.context_dev_block_reason || null,
    context_dev_overrun_credits: Number(priorBudget.context_dev_overrun_credits || 0),
    context_dev_overrun_work_key: priorBudget.context_dev_overrun_work_key || null,
    serpapi_calls: Number(priorBudget.serpapi_calls || 0),
    serpapi_usd: Number(priorBudget.serpapi_usd || 0),
    serpapi_reserved: Number(priorBudget.serpapi_reserved || 0),
    enrichment_calls: Number(priorBudget.enrichment_calls || 0),
    model_calls: Number(priorBudget.model_calls || 0),
    model_usd: Number(priorBudget.model_usd || 0),
    open_reservations: Number(priorBudget.open_reservations || 0),
  };
  const journal = Array.isArray(operationJournal) ? operationJournal : [];
  let latestBu = null;
  let snapshotSpent = Number(out.context_dev_spent || 0);
  let snapshotCalls = Number(out.context_dev_calls || 0);
  for (const entry of journal) {
    const bu = entry?.budget_usage;
    if (!bu || typeof bu !== "object") continue;
    latestBu = bu;
    snapshotSpent = Math.max(snapshotSpent, Number(bu.context_dev_spent || 0));
    snapshotCalls = Math.max(snapshotCalls, Number(bu.context_dev_calls || 0));
    out.context_dev_estimated_unconfirmed = Math.max(
      out.context_dev_estimated_unconfirmed,
      Number(bu.context_dev_estimated_unconfirmed || 0)
    );
    out.context_dev_overrun_credits = Math.max(
      out.context_dev_overrun_credits,
      Number(bu.context_dev_overrun_credits || 0)
    );
    out.serpapi_calls = Math.max(out.serpapi_calls, Number(bu.serpapi_calls || 0));
    out.serpapi_usd = Math.max(out.serpapi_usd, Number(bu.serpapi_usd || 0));
    out.enrichment_calls = Math.max(out.enrichment_calls, Number(bu.enrichment_calls || 0));
    out.model_calls = Math.max(out.model_calls, Number(bu.model_calls || 0));
    out.model_usd = Math.max(out.model_usd, Number(bu.model_usd || 0));
    // Overrun block is sticky — never cleared by a later zero/default snapshot
    if (bu.context_dev_dispatch_blocked === true) {
      out.context_dev_dispatch_blocked = true;
      out.context_dev_block_reason =
        bu.context_dev_block_reason ||
        out.context_dev_block_reason ||
        "CONTEXT_DEV_CONSUMPTION_EXCEEDED_RESERVATION";
      if (bu.context_dev_overrun_work_key) {
        out.context_dev_overrun_work_key = bu.context_dev_overrun_work_key;
      }
    }
  }
  // Also restore block from overrun settlement events (even if budget_usage omitted flag)
  for (const entry of journal) {
    if (entry?.overrun === true || entry?.event === "context_dev_overrun_block") {
      out.context_dev_dispatch_blocked = true;
      out.context_dev_block_reason =
        entry.context_dev_block_reason ||
        entry.reason ||
        out.context_dev_block_reason ||
        "CONTEXT_DEV_CONSUMPTION_EXCEEDED_RESERVATION";
      if (entry.work_key) out.context_dev_overrun_work_key = entry.work_key;
      const reported = Number(entry.settled_credits ?? entry.reported_credits);
      const reserved = Number(entry.reservation_credits ?? 0);
      if (Number.isFinite(reported) && Number.isFinite(reserved) && reported > reserved) {
        out.context_dev_overrun_credits = Math.max(
          out.context_dev_overrun_credits,
          reported - reserved
        );
      }
    }
  }

  const authoritative = computeAuthoritativeContextDevSpendFromJournal(journal);
  if (authoritative.ok && authoritative.complete) {
    // Complete provider evidence wins — do not resurrect inflated snapshot spend.
    out.context_dev_spent = authoritative.settled_credits;
    out.context_dev_calls = authoritative.settled_calls;
    out.context_dev_spend_source = authoritative.source;
    out.context_dev_spend_corrections = authoritative.corrections;
    if (snapshotSpent !== authoritative.settled_credits) {
      out.context_dev_snapshot_spent_superseded = snapshotSpent;
    }
  } else if (authoritative.ok && authoritative.confirmed_ops > 0) {
    out.context_dev_spent = authoritative.settled_credits;
    out.context_dev_calls = authoritative.settled_calls;
    out.context_dev_spend_source = authoritative.source;
    out.context_dev_spend_corrections = authoritative.corrections;
  } else {
    out.context_dev_spent = snapshotSpent;
    out.context_dev_calls = snapshotCalls;
    out.context_dev_spend_source = "budget_usage_snapshot_max";
  }

  const fromOps = outstandingReservationsFromJournal(journal);
  const hasOpKeys = journal.some((e) => e?.work_key && e?.status);
  if (hasOpKeys) {
    out.open_reservations = fromOps.open_reservations;
    out.serpapi_reserved = fromOps.serpapi_reserved;
    out.context_dev_reserved = Math.max(
      fromOps.context_dev_outstanding,
      authoritative.outstanding_credits || 0
    );
  } else if (latestBu) {
    if (latestBu.open_reservations != null) {
      out.open_reservations = Number(latestBu.open_reservations || 0);
    }
    if (latestBu.serpapi_reserved != null) {
      out.serpapi_reserved = Number(latestBu.serpapi_reserved || 0);
    }
    if (latestBu.context_dev_reserved != null) {
      out.context_dev_reserved = Number(latestBu.context_dev_reserved || 0);
    }
  }
  return out;
}

/**
 * Merge cumulative budget snapshots without letting a fresh zero wipe prior spend.
 * Outstanding reservation fields prefer explicit next (including zero).
 */
export function mergeCumulativeBudgetUsage(prior = {}, next = {}) {
  const p = prior || {};
  const n = next || {};
  const maxNum = (a, b) => Math.max(Number(a || 0), Number(b || 0));
  const blocked =
    p.context_dev_dispatch_blocked === true || n.context_dev_dispatch_blocked === true;
  return {
    ...p,
    ...n,
    context_dev_reserved: maxNum(p.context_dev_reserved, n.context_dev_reserved),
    context_dev_spent: maxNum(p.context_dev_spent, n.context_dev_spent),
    context_dev_calls: maxNum(p.context_dev_calls, n.context_dev_calls),
    context_dev_estimated_unconfirmed: maxNum(
      p.context_dev_estimated_unconfirmed,
      n.context_dev_estimated_unconfirmed
    ),
    context_dev_overrun_credits: maxNum(p.context_dev_overrun_credits, n.context_dev_overrun_credits),
    context_dev_dispatch_blocked: blocked,
    context_dev_block_reason: blocked
      ? n.context_dev_block_reason ||
        p.context_dev_block_reason ||
        "CONTEXT_DEV_CONSUMPTION_EXCEEDED_RESERVATION"
      : n.context_dev_block_reason ?? p.context_dev_block_reason ?? null,
    context_dev_overrun_work_key:
      n.context_dev_overrun_work_key || p.context_dev_overrun_work_key || null,
    serpapi_calls: maxNum(p.serpapi_calls, n.serpapi_calls),
    serpapi_usd: maxNum(p.serpapi_usd, n.serpapi_usd),
    enrichment_calls: maxNum(p.enrichment_calls, n.enrichment_calls),
    model_calls: maxNum(p.model_calls, n.model_calls),
    model_usd: maxNum(p.model_usd, n.model_usd),
    external_usd: maxNum(p.external_usd, n.external_usd),
    open_reservations:
      n.open_reservations != null ? Number(n.open_reservations || 0) : Number(p.open_reservations || 0),
    serpapi_reserved:
      n.serpapi_reserved != null ? Number(n.serpapi_reserved || 0) : Number(p.serpapi_reserved || 0),
  };
}

/**
 * Strip restricted / secret-bearing fields before durable persistence.
 */
/** Prefer structured Context.dev error fields over String(object) → "[object Object]". */
export function serializeProviderFailureReason(result) {
  if (result?.reason != null && typeof result.reason !== "object") {
    return String(result.reason).slice(0, 200);
  }
  const err = result?.error;
  if (err == null) return "PROVIDER_FAILED";
  if (typeof err === "string") return err.slice(0, 200);
  if (typeof err === "object") {
    const parts = [
      err.class || err.error_class || null,
      err.status != null ? `status=${err.status}` : null,
      err.message || err.error_detail_sanitized || null,
    ].filter(Boolean);
    if (parts.length) return parts.join(":").slice(0, 200);
    try {
      return JSON.stringify(err).slice(0, 200);
    } catch {
      return "PROVIDER_FAILED";
    }
  }
  return String(err).slice(0, 200);
}

export function sanitizeProviderResultForStorage(result, meta = {}) {
  if (result == null) return { ok: false, sanitized: true, empty: true };

  const blocked = isUsageRightsBlocked(result, meta);

  if (blocked) {
    // Stub only — never persist restricted payload content
    return {
      ok: false,
      sanitized: true,
      restricted: true,
      usage_rights: "BLOCKED",
      note: "Restricted payload omitted from case journal",
    };
  }

  const usage_rights = meta.usage_rights || result?.usage_rights || "INTERNAL_ONLY";

  const key_metadata = (() => {
    const km =
      result?.key_metadata ||
      result?.data?.key_metadata ||
      result?.error?.key_metadata ||
      null;
    if (!km || typeof km !== "object") return null;
    return {
      credits_consumed:
        km.credits_consumed != null ? Number(km.credits_consumed) : null,
      credits_remaining:
        km.credits_remaining != null ? Number(km.credits_remaining) : null,
    };
  })();

  // Provider-level failure (HTTP/transport may have succeeded but payload invalid)
  if (result && result.ok === false) {
    const err = result.error && typeof result.error === "object" ? result.error : null;
    return {
      ok: false,
      sanitized: true,
      provider_failed: true,
      reason: serializeProviderFailureReason(result),
      ...(err?.class || err?.error_class
        ? { error_class: err.class || err.error_class }
        : {}),
      ...(err?.status != null ? { http_status: Number(err.status) } : {}),
      usage_rights,
      ...(key_metadata ? { key_metadata } : {}),
    };
  }

  const out = {
    ok: result.ok !== false,
    sanitized: true,
    usage_rights,
    ...(key_metadata ? { key_metadata } : {}),
  };

  // Search-shaped
  if (result.data?.results || result.results) {
    const rows = result.data?.results || result.results || [];
    out.kind = "search";
    out.data = {
      results: rows.slice(0, 20).map((r) => ({
        title: r.title || null,
        url: r.url || r.link || null,
        snippet: String(r.snippet || r.description || "").slice(0, 400),
        provider: r.provider || meta.provider || null,
      })),
    };
    return out;
  }

  // Serp organic
  if (Array.isArray(result.organic)) {
    out.kind = "serpapi";
    out.organic = result.organic.slice(0, 20).map((r) => ({
      title: r.title || null,
      link: r.link || r.url || null,
      snippet: String(r.snippet || "").slice(0, 400),
    }));
    return out;
  }

  // Scrape / document
  const md =
    typeof result.data === "string"
      ? result.data
      : result.data?.markdown || result.data?.content || result.markdown || "";
  if (md || result.kind === "scrape" || meta.op === "scrape") {
    out.kind = "scrape";
    out.data = String(md).slice(0, 50_000);
    out.characters = String(md).length;
    return out;
  }

  // Generic shallow copy without nested secrets
  out.kind = "generic";
  out.summary = {
    keys: Object.keys(result || {}).filter((k) => !/secret|token|key|password|raw/i.test(k)).slice(0, 20),
  };
  return out;
}

/** Estimated Context.dev credits for an operation (numeric reservation upper bound).
 * SerpAPI and other non-credit providers return 0 — they use call/usd units.
 * Prefer resolveContextDevCreditReservation for ok/block semantics.
 */
export function estimateOperationReservationCredits(meta = {}) {
  const provider = String(meta.provider || "").toLowerCase();
  if (provider === "serpapi") return 0;
  const op = String(meta.op || meta.operation || "").toLowerCase();
  if (op === "serpapi") return 0;
  const bound = resolveContextDevCreditReservation({ ...meta, provider: provider || "context_dev" });
  if (bound.ok) return Number(bound.credits);
  const explicit = Number(meta.estimated_credits);
  if (Number.isFinite(explicit) && explicit > 0) return explicit;
  return null;
}

function rehydrateSearchResult(stored) {
  return {
    ok: stored.ok !== false,
    skipped: false,
    replayed: true,
    data: stored.data || { results: [] },
  };
}

function rehydrateScrapeResult(stored) {
  return {
    ok: stored.ok !== false,
    skipped: false,
    replayed: true,
    data: stored.data || "",
  };
}

function rehydrateSerpResult(stored) {
  return {
    ok: stored.ok !== false,
    replayed: true,
    organic: stored.organic || [],
  };
}

export function replayStoredResult(stored) {
  if (!stored || stored.restricted) {
    return { ok: false, replayed: true, restricted: true, data: stored?.kind === "scrape" ? "" : { results: [] } };
  }
  if (stored.provider_failed || stored.ok === false) {
    return {
      ok: false,
      replayed: true,
      provider_failed: true,
      reason: stored.reason,
      data: stored.kind === "scrape" ? "" : { results: [] },
      organic: [],
    };
  }
  if (stored.kind === "search") return rehydrateSearchResult(stored);
  if (stored.kind === "scrape") return rehydrateScrapeResult(stored);
  if (stored.kind === "serpapi") return rehydrateSerpResult(stored);
  return { ok: true, replayed: true, data: stored.data ?? stored };
}

/**
 * Build a journal-aware wrapper around an external call.
 */
export function createJournaledCaller({
  onOperationCheckpoint,
  completedWorkKeys,
  pendingUnknownKeys,
  resultByWorkKey = null,
  forceRetryUnknown = false,
  budgetUsage = null,
  /** Optional gate checked immediately before RESERVED→dispatch. Return { ok:false, reason } to block. */
  preDispatchGate = null,
  /** Optional clock for deadline-aware gates (tests). */
  nowFn = () => Date.now(),
} = {}) {
  const completed = completedWorkKeys instanceof Set ? completedWorkKeys : new Set(completedWorkKeys || []);
  const unknown = pendingUnknownKeys instanceof Set ? pendingUnknownKeys : new Set(pendingUnknownKeys || []);
  const results =
    resultByWorkKey instanceof Map
      ? resultByWorkKey
      : new Map(Object.entries(resultByWorkKey || {}));
  const budget = budgetUsage || {
    context_dev_reserved: 0,
    context_dev_spent: 0,
    context_dev_calls: 0,
    serpapi_calls: 0,
    serpapi_usd: 0,
    serpapi_reserved: 0,
    enrichment_calls: 0,
    model_calls: 0,
    model_usd: 0,
    open_reservations: 0,
  };

  async function checkpoint(entry) {
    if (typeof onOperationCheckpoint !== "function") return { ok: true };
    // budget_usage last — entry must never overwrite cumulative usage with zeros/absent
    let ack;
    try {
      ack = await onOperationCheckpoint({
        version: RESEARCH_OP_JOURNAL_VERSION,
        at: new Date().toISOString(),
        ...entry,
        budget_usage: { ...budget },
      });
    } catch (err) {
      return {
        ok: false,
        reason: err?.code || err?.message || "CHECKPOINT_THREW",
        threw: true,
        error: err,
      };
    }
    if (ack && typeof ack === "object") return ack;
    // Mandatory persistence: absent acknowledgement is not durable success
    if (entry?.requires_persistence_ack === true) {
      return { ok: false, reason: "PERSISTENCE_ACK_MISSING" };
    }
    return { ok: true };
  }

  /**
   * Persist a required pre-dispatch / in-flight checkpoint. Failure blocks provider call.
   */
  async function requireDurableAck(entry) {
    const ack = await checkpoint({ ...entry, requires_persistence_ack: true });
    if (!ack || ack.ok !== true) {
      return {
        ok: false,
        reason: ack?.reason || "PERSISTENCE_FAILED",
        ack,
      };
    }
    return { ok: true, ack };
  }

  function releaseOpenReservation(provider, reservationCredits) {
    budget.open_reservations = Math.max(0, Number(budget.open_reservations || 0) - 1);
    if (provider === "serpapi") {
      budget.serpapi_reserved = Math.max(0, Number(budget.serpapi_reserved || 0) - 1);
    } else if (provider === "context_dev" || provider === "" || /context/.test(provider)) {
      budget.context_dev_reserved = Math.max(
        0,
        Number(budget.context_dev_reserved || 0) - reservationCredits
      );
    }
  }

  /**
   * @returns {{ skip: boolean, reason?: string, result?: any }}
   */
  async function run(workKey, meta, fn) {
    if (completed.has(workKey)) {
      const stored = results.get(workKey);
      if (stored?.restricted) {
        await checkpoint({
          kind: "skip",
          work_key: workKey,
          status: OP_STATUS.FAILED,
          event: "replay_restricted_blocked",
          ...meta,
        });
        return {
          skip: true,
          reason: "RESTRICTED_BLOCKED",
          result: { ok: false, replayed: true, restricted: true, data: { results: [] } },
        };
      }
      if (stored) {
        await checkpoint({
          kind: "skip",
          work_key: workKey,
          status: OP_STATUS.COMPLETED,
          event: "replay_stored_result",
          ...meta,
        });
        return { skip: true, reason: "REPLAY_STORED_RESULT", result: replayStoredResult(stored) };
      }
      await checkpoint({
        kind: "skip",
        work_key: workKey,
        status: OP_STATUS.COMPLETED,
        event: "already_completed_missing_result",
        ...meta,
      });
      return {
        skip: true,
        reason: "COMPLETED_WITHOUT_STORED_RESULT",
        result: { ok: false, replayed: true, missing_stored_result: true, data: { results: [] } },
      };
    }

    if (unknown.has(workKey) && !forceRetryUnknown) {
      await checkpoint({
        kind: "skip",
        work_key: workKey,
        status: OP_STATUS.IN_FLIGHT_UNKNOWN,
        event: "pending_reconcile_no_blind_retry",
        ...meta,
      });
      return { skip: true, reason: "IN_FLIGHT_UNKNOWN_PENDING_RECONCILE" };
    }

    if (typeof preDispatchGate === "function") {
      const gate = await preDispatchGate({
        work_key: workKey,
        meta,
        now: nowFn(),
        budget_usage: { ...budget },
      });
      if (gate && gate.ok === false) {
        await checkpoint({
          kind: "skip",
          work_key: workKey,
          status: OP_STATUS.FAILED,
          event: "dispatch_blocked_by_gate",
          reason: gate.reason || "DISPATCH_BLOCKED",
          ...meta,
        });
        return {
          skip: true,
          reason: gate.reason || "DISPATCH_BLOCKED",
          result: { ok: false, skipped: true, reason: gate.reason || "DISPATCH_BLOCKED", data: { results: [] } },
        };
      }
    }

    const provider = String(meta.provider || "").toLowerCase();
    if (budget.context_dev_dispatch_blocked === true) {
      await checkpoint({
        kind: "skip",
        work_key: workKey,
        status: OP_STATUS.FAILED,
        event: "dispatch_blocked_by_gate",
        reason: budget.context_dev_block_reason || "CONTEXT_DEV_OVERRUN_BLOCK",
        ...meta,
      });
      return {
        skip: true,
        reason: budget.context_dev_block_reason || "CONTEXT_DEV_OVERRUN_BLOCK",
        result: {
          ok: false,
          skipped: true,
          reason: budget.context_dev_block_reason || "CONTEXT_DEV_OVERRUN_BLOCK",
          data: { results: [] },
        },
      };
    }

    const creditBound =
      provider === "serpapi"
        ? { ok: true, credits: 0, bound_source: "serpapi" }
        : resolveContextDevCreditReservation({ ...meta, provider: provider || "context_dev" });
    if (!creditBound.ok || !Number.isFinite(Number(creditBound.credits)) || Number(creditBound.credits) < 0) {
      await checkpoint({
        kind: "skip",
        work_key: workKey,
        status: OP_STATUS.FAILED,
        event: "dispatch_blocked_unbounded_credits",
        reason: creditBound.reason || "CONTEXT_DEV_OPERATION_UNBOUNDED",
        ...meta,
      });
      return {
        skip: true,
        reason: creditBound.reason || "CONTEXT_DEV_OPERATION_UNBOUNDED",
        result: {
          ok: false,
          skipped: true,
          reason: creditBound.reason || "CONTEXT_DEV_OPERATION_UNBOUNDED",
          data: { results: [] },
        },
      };
    }
    const reservationCredits = Number(creditBound.credits);
    if (provider === "serpapi") {
      budget.serpapi_reserved = Number(budget.serpapi_reserved || 0) + 1;
    } else if (provider === "context_dev" || provider === "" || /context/.test(provider)) {
      budget.context_dev_reserved = Number(budget.context_dev_reserved || 0) + reservationCredits;
    }
    budget.open_reservations = Number(budget.open_reservations || 0) + 1;
    budget.last_reservation_bound_source = creditBound.bound_source || null;
    budget.last_reservation_credits = reservationCredits;

    const preDispatchAck = await requireDurableAck({
      kind: "reservation",
      work_key: workKey,
      status: OP_STATUS.RESERVED,
      event: "pre_dispatch",
      reservation_credits: reservationCredits,
      reservation: { credits: reservationCredits, provider: meta.provider || null, op: meta.op || null },
      ...meta,
    });
    if (!preDispatchAck.ok) {
      releaseOpenReservation(provider, reservationCredits);
      await checkpoint({
        kind: "skip",
        work_key: workKey,
        status: OP_STATUS.FAILED,
        event: "dispatch_blocked_persistence_failed",
        reason: preDispatchAck.reason,
        reservation_credits: reservationCredits,
        ...meta,
      });
      return {
        skip: true,
        reason: preDispatchAck.reason || "PERSISTENCE_FAILED",
        persistence_failed_before_dispatch: true,
        result: {
          ok: false,
          skipped: true,
          reason: preDispatchAck.reason || "PERSISTENCE_FAILED",
          data: { results: [] },
        },
      };
    }

    const callStartedAck = await requireDurableAck({
      kind: "dispatch",
      work_key: workKey,
      status: OP_STATUS.IN_FLIGHT,
      event: "call_started",
      reservation_credits: reservationCredits,
      reservation: { credits: reservationCredits, provider: meta.provider || null, op: meta.op || null },
      ...meta,
    });
    if (!callStartedAck.ok) {
      releaseOpenReservation(provider, reservationCredits);
      await checkpoint({
        kind: "skip",
        work_key: workKey,
        status: OP_STATUS.FAILED,
        event: "dispatch_blocked_persistence_failed",
        reason: callStartedAck.reason,
        reservation_credits: reservationCredits,
        ...meta,
      });
      return {
        skip: true,
        reason: callStartedAck.reason || "PERSISTENCE_FAILED",
        persistence_failed_before_dispatch: true,
        result: {
          ok: false,
          skipped: true,
          reason: callStartedAck.reason || "PERSISTENCE_FAILED",
          data: { results: [] },
        },
      };
    }

    try {
      const result = await fn();
      const sanitized = sanitizeProviderResultForStorage(result, meta);

      const settleSpend = (sanitizedResult = null) => {
        budget.open_reservations = Math.max(0, Number(budget.open_reservations || 0) - 1);
        if (provider === "serpapi") {
          budget.serpapi_reserved = Math.max(0, Number(budget.serpapi_reserved || 0) - 1);
          budget.serpapi_calls = Number(budget.serpapi_calls || 0) + 1;
          const usd = Number(meta.estimated_usd ?? meta.usd_per_call ?? 0.01);
          budget.serpapi_usd = Number((Number(budget.serpapi_usd || 0) + usd).toFixed(6));
          return { settled: true, credits: 0, source: "serpapi" };
        }
        if (provider === "enrichment" || /enrich|fullenrich|surfe/i.test(provider)) {
          budget.enrichment_calls = Number(budget.enrichment_calls || 0) + 1;
          return { settled: true, credits: 0, source: "enrichment" };
        }
        if (provider === "model" || provider === "planner" || /model|openai|anthropic/i.test(provider)) {
          budget.model_calls = Number(budget.model_calls || 0) + 1;
          budget.model_usd = Number(budget.model_usd || 0) + Number(meta.estimated_usd || 0);
          return { settled: true, credits: 0, source: "model" };
        }
        // Context.dev — settle provider-confirmed per-request credits when present.
        // Never clamp reported consumption to the reservation.
        const confirmed = readProviderConfirmedCredits(sanitizedResult);
        let settleAmount;
        let settleSource;
        if (confirmed.ok) {
          settleAmount = confirmed.credits;
          settleSource = "provider_key_metadata.credits_consumed";
        } else {
          const guarantee = resolveDocumentedScrapeBillingGuarantee(sanitizedResult, {
            ...meta,
            op: meta.op || (String(creditBound.bound_source || "").includes("scrape") ? "scrape" : meta.op),
          });
          if (guarantee.ok && guarantee.billed === false) {
            settleAmount = 0;
            settleSource = guarantee.evidence || "documented_http_not_billed";
          } else {
            // Missing billing metadata and no documented non-billable guarantee:
            // retain the full reservation as outstanding liability. Track estimate
            // separately — do not settle/release.
            const documented =
              creditBound.documented_estimate != null
                ? Number(creditBound.documented_estimate)
                : null;
            budget.context_dev_estimated_unconfirmed =
              Number(budget.context_dev_estimated_unconfirmed || 0) +
              (Number.isFinite(documented) ? documented : 0);
            budget.last_settle_source = "unknown_retain_liability";
            budget.last_billing_unsettled = true;
            budget.open_reservations = Number(budget.open_reservations || 0) + 1;
            // Re-increment open_reservations because we decremented at settleSpend entry
            return {
              settled: false,
              keep_outstanding: true,
              credits: null,
              estimated_credits: Number.isFinite(documented) ? documented : null,
              source: confirmed.reason || "KEY_METADATA_ABSENT",
              billing_settled: false,
            };
          }
        }
        budget.context_dev_reserved = Math.max(
          0,
          Number(budget.context_dev_reserved || 0) - reservationCredits
        );
        budget.context_dev_spent = Number(budget.context_dev_spent || 0) + settleAmount;
        budget.context_dev_calls = Number(budget.context_dev_calls || 0) + 1;
        budget.last_settle_source = settleSource;
        budget.last_settled_credits = settleAmount;
        budget.last_billing_unsettled = false;
        budget.last_reservation_released_unused = Math.max(0, reservationCredits - settleAmount);
        if (settleAmount > reservationCredits) {
          budget.context_dev_overrun_credits =
            Number(budget.context_dev_overrun_credits || 0) + (settleAmount - reservationCredits);
          budget.context_dev_dispatch_blocked = true;
          budget.context_dev_block_reason = "CONTEXT_DEV_CONSUMPTION_EXCEEDED_RESERVATION";
          budget.context_dev_overrun_work_key = workKey;
        }
        return {
          settled: true,
          credits: settleAmount,
          source: settleSource,
          overrun: settleAmount > reservationCredits,
          billing_settled: true,
        };
      };

      if (sanitized.restricted) {
        const settle = settleSpend(sanitized);
        completed.add(workKey);
        unknown.delete(workKey);
        const stub = {
          ok: false,
          restricted: true,
          usage_rights: "BLOCKED",
          note: "Restricted payload omitted from case journal",
        };
        results.set(workKey, stub);
        await checkpoint({
          kind: "settlement",
          work_key: workKey,
          status: OP_STATUS.FAILED,
          event: "provider_blocked_restricted",
          reservation_credits: reservationCredits,
          settled_credits: settle.credits,
          settle_source: settle.source,
          result_ref: { stored: false, restricted: true },
          durable_result: stub,
          ...meta,
        });
        return {
          skip: false,
          provider_failed: true,
          restricted: true,
          result: { ok: false, restricted: true, data: { results: [] } },
        };
      }

      if (sanitized.provider_failed || sanitized.ok === false) {
        const settle = settleSpend(sanitized);
          if (settle.keep_outstanding) {
          unknown.add(workKey);
          await checkpoint({
            kind: "settlement",
            work_key: workKey,
            status: OP_STATUS.IN_FLIGHT_UNKNOWN,
            event: "provider_failed_billing_unknown",
            reservation_credits: reservationCredits,
            reservation: { credits: reservationCredits, unsettled: true },
            settle_source: settle.source,
            billing_settled: false,
            estimated_credits: settle.estimated_credits ?? null,
            result_ref: { stored: true, kind: sanitized.kind || "generic" },
            durable_result: sanitized,
            ...meta,
          });
          results.set(workKey, sanitized);
          return { skip: false, result, provider_failed: true, billing_unknown: true };
        }
        results.set(workKey, sanitized);
        completed.add(workKey);
        unknown.delete(workKey);
        await checkpoint({
          kind: "settlement",
          work_key: workKey,
          status: OP_STATUS.FAILED,
          event: "provider_failed",
          reservation_credits: reservationCredits,
          settled_credits: settle.credits,
          settle_source: settle.source,
          overrun: settle.overrun === true,
          billing_settled: settle.billing_settled !== false,
          result_ref: { stored: true, kind: sanitized.kind || "generic" },
          durable_result: sanitized,
          key_metadata: sanitized.key_metadata || null,
          ...meta,
        });
        if (settle.overrun === true) {
          await checkpoint({
            kind: "control",
            work_key: workKey,
            status: OP_STATUS.FAILED,
            event: "context_dev_overrun_block",
            reason: "CONTEXT_DEV_CONSUMPTION_EXCEEDED_RESERVATION",
            reservation_credits: reservationCredits,
            settled_credits: settle.credits,
            reported_credits: settle.credits,
            overrun: true,
            context_dev_block_reason: "CONTEXT_DEV_CONSUMPTION_EXCEEDED_RESERVATION",
            ...meta,
          });
        }
        return { skip: false, result, provider_failed: true, overrun: settle.overrun === true };
      }

      results.set(workKey, sanitized);
      // Account credits before durable checkpoint so a kill after successful put
      // still preserves cumulative spend on disk. Roll back if persistence fails.
      const settle = settleSpend(sanitized);
      if (settle.keep_outstanding) {
        unknown.add(workKey);
        await checkpoint({
          kind: "failure",
          work_key: workKey,
          status: OP_STATUS.IN_FLIGHT_UNKNOWN,
          event: "provider_success_billing_unknown",
          reservation_credits: reservationCredits,
          reservation: { credits: reservationCredits, unsettled: true },
          settle_source: settle.source,
          durable_result: sanitized,
          ...meta,
        });
        // Still attempt to persist result for replay, but liability remains outstanding
      }
      const persistAck = await checkpoint({
        kind: "result_persisted",
        work_key: workKey,
        status: OP_STATUS.IN_FLIGHT,
        event: "durable_result_written",
        reservation_credits: reservationCredits,
        settled_credits: settle.credits,
        settle_source: settle.source,
        result_ref: { stored: true, kind: sanitized.kind || "generic" },
        durable_result: sanitized,
        requires_persistence_ack: true,
        ...meta,
      });
      // Failed required write must not become a durable-success claim; keep liability open
      if (!persistAck || persistAck.ok !== true) {
        // Roll back spend settlement; retain open reservation liability
        if (settle.settled && settle.credits != null) {
          budget.context_dev_spent = Math.max(
            0,
            Number(budget.context_dev_spent || 0) - Number(settle.credits)
          );
          budget.context_dev_calls = Math.max(0, Number(budget.context_dev_calls || 0) - 1);
          budget.context_dev_reserved =
            Number(budget.context_dev_reserved || 0) + reservationCredits;
        }
        budget.open_reservations = Number(budget.open_reservations || 0) + 1;
        unknown.add(workKey);
        await checkpoint({
          kind: "failure",
          work_key: workKey,
          status: OP_STATUS.IN_FLIGHT_UNKNOWN,
          event: "persistence_failed_after_provider_success",
          error_code: persistAck.reason || "PERSISTENCE_FAILED",
          reservation_credits: reservationCredits,
          reservation: { credits: reservationCredits, unsettled: true },
          ...meta,
        });
        const err = new Error(persistAck.reason || "PERSISTENCE_FAILED");
        err.code = "PERSISTENCE_FAILED";
        err.preserve_unknown_liability = true;
        throw err;
      }

      if (!settle.keep_outstanding) {
        completed.add(workKey);
        unknown.delete(workKey);
      }
      await checkpoint({
        kind: "settlement",
        work_key: workKey,
        status: settle.keep_outstanding ? OP_STATUS.IN_FLIGHT_UNKNOWN : OP_STATUS.COMPLETED,
        event: "call_completed",
        reservation_credits: reservationCredits,
        settled_credits: settle.credits,
        settle_source: settle.source,
        overrun: settle.overrun === true,
        billing_settled: settle.billing_settled !== false && !settle.keep_outstanding,
        reservation: settle.keep_outstanding
          ? { credits: reservationCredits, unsettled: true }
          : undefined,
        result_ref: { stored: true, kind: sanitized.kind || "generic" },
        key_metadata: sanitized.key_metadata || null,
        ...meta,
      });
      if (settle.overrun === true) {
        await checkpoint({
          kind: "control",
          work_key: workKey,
          status: OP_STATUS.COMPLETED,
          event: "context_dev_overrun_block",
          reason: "CONTEXT_DEV_CONSUMPTION_EXCEEDED_RESERVATION",
          reservation_credits: reservationCredits,
          settled_credits: settle.credits,
          reported_credits: settle.credits,
          overrun: true,
          context_dev_block_reason: "CONTEXT_DEV_CONSUMPTION_EXCEEDED_RESERVATION",
          ...meta,
        });
      }
      return { skip: false, result, overrun: settle.overrun === true };
    } catch (err) {
      unknown.add(workKey);
      // Do not settle/release uncertain charges — keep reservation outstanding
      await checkpoint({
        kind: "failure",
        work_key: workKey,
        status: OP_STATUS.IN_FLIGHT_UNKNOWN,
        event: "call_interrupted_or_failed",
        error_code: err?.code || "IN_FLIGHT_UNKNOWN",
        reservation_credits: reservationCredits,
        reservation: { credits: reservationCredits, unsettled: true },
        ...meta,
      });
      throw err;
    }
  }

  return {
    run,
    completed,
    unknown,
    results,
    budget,
    checkpoint,
    snapshot() {
      return {
        completed_work_keys: [...completed],
        pending_unknown_keys: [...unknown],
        result_by_work_key: Object.fromEntries(results),
        budget_usage: { ...budget },
      };
    },
  };
}

/**
 * Recover pending unknown keys and durable results from case operation_journal.
 */
export function pendingUnknownFromJournal(journal = []) {
  const lastByKey = new Map();
  const durableKeys = new Set();
  for (const e of journal || []) {
    if (!e?.work_key) continue;
    lastByKey.set(e.work_key, e);
    if (e.durable_result || e.event === "durable_result_written") {
      durableKeys.add(e.work_key);
    }
  }
  const unknown = new Set();
  for (const [k, e] of lastByKey) {
    // Durable result already persisted ⇒ known outcome (do not treat as unknown)
    if (durableKeys.has(k)) continue;
    if (
      e.status === OP_STATUS.IN_FLIGHT ||
      e.status === OP_STATUS.IN_FLIGHT_UNKNOWN ||
      e.status === OP_STATUS.RESERVED
    ) {
      unknown.add(k);
    }
  }
  for (const [k, e] of lastByKey) {
    if (e.status === OP_STATUS.COMPLETED || e.status === OP_STATUS.FAILED) {
      unknown.delete(k);
    }
  }
  return unknown;
}

export function resultsFromJournal(journal = []) {
  const map = new Map();
  for (const e of journal || []) {
    if (!e?.work_key) continue;
    if (e.durable_result && (e.status === OP_STATUS.COMPLETED || e.status === OP_STATUS.FAILED || e.event === "durable_result_written")) {
      map.set(e.work_key, e.durable_result);
    }
  }
  return map;
}

export function completedKeysFromJournal(journal = []) {
  const lastByKey = new Map();
  const durableKeys = new Set();
  const persistenceFailed = new Set();
  for (const e of journal || []) {
    if (!e?.work_key) continue;
    lastByKey.set(e.work_key, e);
    if (e.event === "persistence_failed_after_provider_success") {
      persistenceFailed.add(e.work_key);
      durableKeys.delete(e.work_key);
    }
    if (
      (e.durable_result || e.event === "durable_result_written") &&
      !persistenceFailed.has(e.work_key)
    ) {
      durableKeys.add(e.work_key);
    }
  }
  const keys = new Set();
  for (const [k, e] of lastByKey) {
    if (persistenceFailed.has(k)) continue;
    if (e.status === OP_STATUS.COMPLETED || e.status === OP_STATUS.FAILED) keys.add(k);
    // Crash after persist-before-complete: durable result is authoritative settlement
    if (durableKeys.has(k)) keys.add(k);
  }
  return keys;
}

/**
 * Wrap Context.dev search/scrape/extract through the operation journal.
 * Pass estimated_credits explicitly (credits, not call counts).
 */
export function createJournaledContextProviders({
  journaler,
  search,
  scrape,
  extract,
  creditCosts = { search: 1, scrape: 1, extract: 10 },
} = {}) {
  if (!journaler) {
    return { search, scrape, extract, journaled: false };
  }
  const wrap = (op, rawFn, creditKey) => {
    if (typeof rawFn !== "function") return null;
    return async (args) => {
      const workKey = buildOperationWorkKey({
        provider: "context_dev",
        operation: op,
        query: args?.query,
        url: args?.url || args?.urls?.[0],
      });
      const bound = resolveContextDevCreditReservation({
        provider: "context_dev",
        op,
        query: args?.query,
        url: args?.url || args?.urls?.[0],
        actions: args?.actions,
        estimated_credits: args?.estimated_credits,
        num_results: args?.numResults ?? args?.num_results,
        enriched_images: args?.enriched_images,
      });
      const estimated = bound.ok
        ? Number(bound.credits)
        : Number(args?.estimated_credits) || Number(creditCosts[creditKey] ?? creditCosts[op] ?? 1);
      const outcome = await journaler.run(
        workKey,
        {
          provider: "context_dev",
          op,
          query: args?.query || null,
          url: args?.url || args?.urls?.[0] || null,
          actions: args?.actions || null,
          estimated_credits: estimated,
          reservation_bound_source: bound.bound_source || null,
        },
        () => rawFn(args)
      );
      if (outcome.skip) {
        if (outcome.reason === "IN_FLIGHT_UNKNOWN_PENDING_RECONCILE") {
          return {
            ok: false,
            skipped: true,
            replayed: true,
            provider_billed: false,
            reason: outcome.reason,
            data: op === "search" ? { results: [] } : op === "scrape" || op === "scrape_markdown" ? "" : {},
          };
        }
        if (outcome.result) {
          return {
            ...outcome.result,
            skipped: true,
            replayed: outcome.result.replayed !== false,
            provider_billed: false,
            reason: outcome.reason || outcome.result.reason || "JOURNAL_SKIP",
          };
        }
        return {
          ok: false,
          skipped: true,
          replayed: true,
          provider_billed: false,
          reason: outcome.reason,
          data: op === "search" ? { results: [] } : {},
        };
      }
      return outcome.result;
    };
  };
  return {
    search: wrap("search", search, "search"),
    scrape: wrap("scrape", scrape, "scrape"),
    extract: wrap("extract", extract, "extract"),
    journaled: true,
  };
}
