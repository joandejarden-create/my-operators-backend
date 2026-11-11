/**
 * SerpAPI intent settlement. No network calls and no invented search IDs.
 */
import crypto from "node:crypto";

export function accountDrop(before = {}, after = {}) {
  const plan = Number(before.plan_searches_left) - Number(after.plan_searches_left);
  const extra = Number(before.extra_credits) - Number(after.extra_credits);
  if (!Number.isFinite(plan) || !Number.isFinite(extra)) return null;
  return plan + extra;
}

/**
 * Execution and billing are separate. A later balance change is not
 * settlement proof unless this request is the only one that can explain it.
 * Elapsed time is not evidence.
 */
export function classifySearchExecution({ responseOk = false, searchId = "", timedOut = false, interrupted = false } = {}) {
  if (timedOut || interrupted) return { execution: "ambiguous_execution", retry: false };
  if (responseOk && searchId) return { execution: "succeeded", retry: false };
  if (!searchId && responseOk === false) return { execution: "failed_non_consumption", retry: false };
  return { execution: "ambiguous_execution", retry: false };
}

export function classifyBillingSettlement({ execution, planDelta = null, extraDelta = null } = {}) {
  if (execution !== "succeeded") return { settlement: "disputed", reservedCredits: 0 };
  if (extraDelta > 0 || planDelta > 1) return { settlement: "unattributed_usage", reservedCredits: 1 };
  if (planDelta === 1 && extraDelta === 0) return { settlement: "confirmed_billed", reservedCredits: 0 };
  if (planDelta === 0 && extraDelta === 0) return { settlement: "pending_delayed", reservedCredits: 1 };
  return { settlement: "disputed", reservedCredits: 1 };
}

export function assessIsolatedSettlement({
  pendingSearchIds = [],
  planBefore,
  planAfter,
  extraBefore,
  extraAfter,
  hourBefore = null,
  hourAfter = null,
  otherInFlight = 0,
} = {}) {
  const planDrop = Number(planBefore) - Number(planAfter);
  const extraDrop = Number(extraBefore) - Number(extraAfter);
  if (otherInFlight > 0) return { ok: false, continuePermitted: false, reason: "IN_FLIGHT_REQUEST" };
  if (!Number.isFinite(planDrop) || !Number.isFinite(extraDrop) || extraDrop !== 0) {
    return { ok: false, continuePermitted: false, reason: "UNATTRIBUTED_USAGE" };
  }
  if (planDrop === 0) return { ok: false, continuePermitted: true, reason: "STILL_PENDING" };
  if (planDrop < 0 || planDrop > 1 || pendingSearchIds.length !== 1) {
    return { ok: false, continuePermitted: false, reason: planDrop > 1 || pendingSearchIds.length !== 1 ? "MULTIPLE_OR_UNEXPECTED_MOVEMENT" : "UNATTRIBUTED_USAGE" };
  }
  const hourIncreased = hourBefore == null || hourAfter == null ? null : Number(hourAfter) - Number(hourBefore);
  if (hourIncreased !== 1) return { ok: false, continuePermitted: false, reason: "ISOLATION_NOT_PROVEN" };
  return { ok: true, continuePermitted: true, reason: "ISOLATED_DELAYED_CREDIT", searchId: pendingSearchIds[0] };
}

export function classifyProviderOutcome({
  responseOk = false,
  searchId = "",
  before = null,
  after = null,
  timedOut = false,
  duplicate = false,
  interrupted = false,
} = {}) {
  if (duplicate) {
    return { status: "duplicate", confidence: "confirmed_receipt", retry: false, halt: true, suspectedCredits: 0, reason: "DUPLICATE_RESPONSE" };
  }
  const drop = before && after ? accountDrop(before, after) : null;
  if (searchId && responseOk) {
    return {
      status: "billed",
      confidence: "confirmed_receipt",
      retry: false,
      halt: drop !== 1,
      suspectedCredits: 0,
      reason: drop === 1 ? "CONFIRMED_RECEIPT" : "CONFIRMED_RECEIPT_USAGE_UNCERTAIN",
    };
  }
  if (!searchId && drop != null && drop > 0) {
    return { status: "quarantined", confidence: "suspected_consumption", retry: false, halt: true, suspectedCredits: drop, reason: "MISSING_SEARCH_ID_WITH_USAGE_DROP" };
  }
  if (timedOut || interrupted || drop == null) {
    return { status: "quarantined", confidence: "unknown", retry: false, halt: true, suspectedCredits: null, reason: timedOut ? "TIMEOUT" : "USAGE_UNKNOWN" };
  }
  if (!searchId && drop === 0 && responseOk === false) {
    return { status: "failed", confidence: "confirmed_non_consumption", retry: false, halt: true, suspectedCredits: 0, reason: "REJECTED_BEFORE_CONSUMPTION" };
  }
  return { status: "quarantined", confidence: "unknown", retry: false, halt: true, suspectedCredits: null, reason: "UNCERTAIN_OUTCOME" };
}

export function createSearchIntent({ hotelId, workstream, pathway, query, language, authorizationId } = {}) {
  return {
    operationId: `gdi_intent_${crypto.randomBytes(8).toString("hex")}`,
    hotelId,
    workstream: workstream || "",
    pathway: pathway || "",
    query,
    language: language || "",
    authorizationId: authorizationId || "",
    requestedAt: new Date().toISOString(),
    status: "reserved",
    confidence: "pending",
    searchId: null,
    before: null,
    after: null,
    requestsSent: 0,
  };
}

export function markIntentSent(intent, before) {
  if (intent.requestsSent >= 1) return { ok: false, reason: "ONE_REQUEST_PER_INTENT", intent };
  return { ok: true, intent: { ...intent, requestsSent: 1, before, status: "sent" } };
}

export function settleIntent(intent, outcome) {
  const settled = classifyProviderOutcome(outcome);
  return {
    ...intent,
    status: settled.status,
    confidence: settled.confidence,
    searchId: outcome.searchId || null,
    after: outcome.after || null,
    suspectedCredits: settled.suspectedCredits,
    retry: false,
    reason: settled.reason,
  };
}

export function canOpenAnotherIntent({ entries = [], hotelId, authorizationId, hotelCeiling = 1, globalCeiling = 2 } = {}) {
  const scoped = entries.filter((row) => row.authorizationId === authorizationId && row.confidence !== "confirmed_non_consumption");
  const hotelCount = scoped.filter((row) => row.hotelId === hotelId).length;
  if (scoped.some((row) => row.status === "reserved" || row.status === "sent")) {
    return { ok: false, reason: "INTERRUPTED_INTENT_OPEN" };
  }
  if (hotelCount >= hotelCeiling) return { ok: false, reason: "HOTEL_LIMIT" };
  if (scoped.length >= globalCeiling) return { ok: false, reason: "GLOBAL_LIMIT" };
  return { ok: true };
}
