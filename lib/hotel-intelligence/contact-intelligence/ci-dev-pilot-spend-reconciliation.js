/**
 * Offline reconciliation of Context.dev pilot spend from durable journals.
 * No provider calls. Idempotent corrections via ledger applySpendCorrection.
 *
 * Semantics (OpenAPI KeyMetadata): credits_consumed = credits charged for THIS request.
 */

import fs from "node:fs";
import path from "node:path";
import { readProviderConfirmedCredits } from "../../context-dev/operation-credit-bounds.js";

export const CI_DEV_PILOT_SPEND_RECONCILIATION_VERSION = "ci-dev-pilot-spend-reconciliation-v1";

const AUTHORIZED_FIVE = [
  "rec07GLNG5oNBkAj8",
  "rec00Cp2AgpZAtp9Z",
  "rec08YF1M2BOFBcYG",
  "rec00eC8DN3Ow0hFo",
  "rec09k8RdHgiAAKsG",
];

/**
 * Extract unique Context.dev operations from a case journal (exclude replays).
 */
export function extractUniqueContextDevOps(caseRecord, meta = {}) {
  const journal = Array.isArray(caseRecord?.operation_journal) ? caseRecord.operation_journal : [];
  const byKey = new Map();

  for (const e of journal) {
    const wk = e?.work_key;
    if (!wk || !/^context_dev:/.test(String(wk))) continue;
    if (
      e.event === "replay_stored_result" ||
      e.event === "already_completed_missing_result" ||
      e.kind === "skip"
    ) {
      continue;
    }

    const prev = byKey.get(wk) || {
      work_key: wk,
      hotel_id: meta.hotel_id || caseRecord.hotel_id,
      case_id: caseRecord.case_id,
      op: e.op || null,
      reserved_credits: null,
      previously_settled_credits: null,
      provider_reported: null,
      provider_request_id: e.provider_request_id || e.request_id || null,
      events: [],
      durable_result: null,
      execution_status: "unknown",
    };
    prev.events.push({ event: e.event, at: e.at, status: e.status });
    if (e.reservation_credits != null) prev.reserved_credits = Number(e.reservation_credits);
    if (e.settled_credits != null) prev.previously_settled_credits = Number(e.settled_credits);
    if (e.op) prev.op = e.op;
    if (e.durable_result) prev.durable_result = e.durable_result;
    if (e.key_metadata && !prev.durable_result?.key_metadata) {
      prev.durable_result = { ...(prev.durable_result || {}), key_metadata: e.key_metadata };
    }
    if (e.event === "call_completed") prev.execution_status = "success";
    if (e.event === "provider_failed" || e.event === "provider_failed_billing_unknown") {
      prev.execution_status = e.event === "provider_failed_billing_unknown" ? "failure_billing_unknown" : "failure";
    }
    if (e.event === "durable_result_written" && prev.execution_status === "unknown") {
      prev.execution_status = "dispatched";
    }
    byKey.set(wk, prev);
  }

  const ops = [];
  for (const op of byKey.values()) {
    const confirmed = readProviderConfirmedCredits(op.durable_result);
    const reserved = Number(op.reserved_credits);
    let corrected = null;
    let meaning = null;
    let evidence = null;
    let outstanding = 0;

    if (confirmed.ok) {
      corrected = confirmed.credits;
      meaning = "per_request_credits_consumed";
      evidence = confirmed.evidence;
      // Previous ledger settled reservation amount; correction replaces with confirmed.
    } else {
      corrected = null;
      meaning = "UNKNOWN";
      evidence = confirmed.reason || "KEY_METADATA_ABSENT";
      outstanding = Number.isFinite(reserved) ? reserved : 0;
    }

    ops.push({
      ...op,
      provider_reported: confirmed.ok
        ? {
            credits_consumed: confirmed.credits,
            credits_remaining: confirmed.credits_remaining,
            meaning: "per_request",
          }
        : null,
      corrected_consumption: corrected,
      corrected_meaning: meaning,
      correction_evidence: evidence,
      outstanding_liability_credits: outstanding,
      previously_settled_estimate: Number.isFinite(reserved) ? reserved : op.previously_settled_credits,
    });
  }
  return ops;
}

/**
 * Build full pilot reconciliation report (read-only until apply).
 */
export function buildPilotSpendReconciliation({
  pilotId,
  caseStore,
  hotelCaseBindings = [],
  accountWideConsumedCredits = 669,
} = {}) {
  const perHotel = [];
  const allOps = [];
  let totalPreviouslySettled = 0;
  let totalCorrected = 0;
  let totalOutstanding = 0;
  let totalUnknownOps = 0;
  let dispatchCount = 0;

  for (const binding of hotelCaseBindings) {
    const caseRec =
      binding.case_id && caseStore?.getCase ? caseStore.getCase(binding.case_id) : null;
    const ops = extractUniqueContextDevOps(caseRec, {
      hotel_id: binding.hotel_id,
      pilot_id: pilotId,
    });
    dispatchCount += ops.length;
    const hotelPrev = Number(binding.previously_settled || caseRec?.budget_usage?.context_dev_spent || 0);
    totalPreviouslySettled += hotelPrev;

    let hotelCorrected = 0;
    let hotelOutstanding = 0;
    let hotelUnknown = 0;
    for (const op of ops) {
      allOps.push({ ...op, pilot_id: pilotId, run_id: pilotId });
      if (op.corrected_consumption != null) hotelCorrected += op.corrected_consumption;
      else {
        hotelUnknown += 1;
        hotelOutstanding += Number(op.outstanding_liability_credits || 0);
      }
    }
    totalCorrected += hotelCorrected;
    totalOutstanding += hotelOutstanding;
    totalUnknownOps += hotelUnknown;

    perHotel.push({
      hotel_id: binding.hotel_id,
      case_id: binding.case_id,
      hotel_name: binding.hotel_name || caseRec?.hotel_seed?.name || null,
      unique_ops: ops.length,
      previously_settled_credits: hotelPrev,
      corrected_provider_confirmed_credits: hotelCorrected,
      outstanding_liability_credits: hotelOutstanding,
      unknown_ops: hotelUnknown,
      delta_corrected_minus_previous: hotelCorrected - hotelPrev,
      ops,
    });
  }

  return {
    version: CI_DEV_PILOT_SPEND_RECONCILIATION_VERSION,
    pilot_id: pilotId,
    semantics: {
      credits_consumed:
        "Per-request credits charged for that API call (OpenAPI KeyMetadata: 'credits consumed by this request'). Not cumulative account consumption.",
      credits_remaining: "Organization balance after the request (account-wide; not pilot-scoped).",
      account_wide_consumed_observation: accountWideConsumedCredits,
      account_wide_excluded_from_pilot: true,
      note: "Account-wide cycle consumption must not enter the pilot ledger.",
    },
    dispatch_count_unique_work_keys: dispatchCount,
    previously_settled_total: totalPreviouslySettled,
    corrected_provider_confirmed_total: totalCorrected,
    outstanding_liability_total: totalOutstanding,
    unknown_ops: totalUnknownOps,
    discrepancy_notes: {
      thirty_nine_vs_forty:
        "39 unique Context.dev work keys with durable provider outcomes; ledger previously settled 40 because one work key (Alles Blau scrape:09dba55f…) completed two settlement cycles — unique-key corrected accounting counts once.",
      provider_vs_ledger:
        "Ledger settled reservation amounts (typically 1/op). Provider key_metadata.credits_consumed is per-request and was higher on some billable scrape failures (reported 10).",
    },
    per_hotel: perHotel,
    operations: allOps,
    authorized_five_ids: AUTHORIZED_FIVE,
  };
}

/**
 * Apply reconciliation to durable ledger + case budget_usage (idempotent).
 */
export function applyPilotSpendCorrection({
  reconciliation,
  spendTracker,
  caseStore,
  hardCap = 500,
  nowFn = () => Date.now(),
} = {}) {
  if (!reconciliation?.pilot_id) {
    return { ok: false, reason: "RECONCILIATION_MISSING" };
  }
  const correctionId = `corr_${reconciliation.pilot_id}_${reconciliation.corrected_provider_confirmed_total}`;
  const snap = spendTracker.snapshot?.() || {};
  if (snap.spend_correction?.correction_id === correctionId) {
    return {
      ok: true,
      idempotent: true,
      correction_id: correctionId,
      spent: snap.context_dev_spent,
      remaining: Math.max(0, hardCap - Number(snap.context_dev_spent || 0) - Number(snap.outstanding_liability_credits || 0)),
    };
  }

  if (typeof spendTracker.applySpendCorrection !== "function") {
    return { ok: false, reason: "LEDGER_APPLY_NOT_SUPPORTED" };
  }

  const applied = spendTracker.applySpendCorrection({
    correction_id: correctionId,
    corrected_spent: reconciliation.corrected_provider_confirmed_total,
    outstanding_liability: reconciliation.outstanding_liability_total,
    hard_cap: hardCap,
    per_hotel: reconciliation.per_hotel.map((h) => ({
      hotel_id: h.hotel_id,
      case_id: h.case_id,
      spent: h.corrected_provider_confirmed_credits,
      journal_outstanding: h.outstanding_liability_credits,
    })),
    reconciliation_summary: {
      version: reconciliation.version,
      previously_settled_total: reconciliation.previously_settled_total,
      corrected_provider_confirmed_total: reconciliation.corrected_provider_confirmed_total,
      dispatch_count_unique_work_keys: reconciliation.dispatch_count_unique_work_keys,
      semantics: reconciliation.semantics,
    },
    at: new Date(nowFn()).toISOString(),
  });

  // Update case budget_usage snapshots absolutely (do not add again)
  for (const h of reconciliation.per_hotel) {
    if (!h.case_id || !caseStore?.getCase || !caseStore?.putCase) continue;
    const rec = caseStore.getCase(h.case_id);
    if (!rec) continue;
    const priorSpent = Number(rec.budget_usage?.context_dev_spent || 0);
    const bu = { ...(rec.budget_usage || {}) };
    bu.context_dev_spent = h.corrected_provider_confirmed_credits;
    bu.context_dev_outstanding = h.outstanding_liability_credits;
    bu.context_dev_reconciliation_source = "provider_key_metadata_per_request";
    bu.context_dev_spend_source = "provider_key_metadata_per_request";
    bu.context_dev_reconciliation_id = correctionId;
    bu.context_dev_absolute_spent_correction = true;
    rec.budget_usage = bu;

    // Auditable per-op corrections (idempotent by correction_id)
    const journal = Array.isArray(rec.operation_journal) ? rec.operation_journal.slice() : [];
    const already = journal.some(
      (e) => e?.event === "accounting_correction" && e?.correction_id === correctionId
    );
    if (!already) {
      for (const op of h.ops || []) {
        if (op.corrected_consumption == null) continue;
        const prev =
          op.previously_settled_estimate != null
            ? Number(op.previously_settled_estimate)
            : op.reserved_credits;
        if (prev === op.corrected_consumption) continue;
        journal.push({
          kind: "accounting_correction",
          version: CI_DEV_PILOT_SPEND_RECONCILIATION_VERSION,
          at: new Date(nowFn()).toISOString(),
          event: "accounting_correction",
          correction_id: correctionId,
          work_key: op.work_key,
          previous_value: prev,
          corrected_value: op.corrected_consumption,
          evidence: op.correction_evidence || "provider_key_metadata.credits_consumed",
          hotel_id: h.hotel_id,
          case_id: h.case_id,
        });
      }
      if (priorSpent !== h.corrected_provider_confirmed_credits) {
        journal.push({
          kind: "accounting_correction",
          version: CI_DEV_PILOT_SPEND_RECONCILIATION_VERSION,
          at: new Date(nowFn()).toISOString(),
          event: "accounting_correction_case_aggregate",
          correction_id: correctionId,
          work_key: `case_aggregate:${h.case_id}`,
          previous_value: priorSpent,
          corrected_value: h.corrected_provider_confirmed_credits,
          evidence: "sum_provider_key_metadata_per_request",
          hotel_id: h.hotel_id,
          case_id: h.case_id,
        });
      }
      rec.operation_journal = journal;
    }

    try {
      caseStore.putCase(rec, { skip_lock: true, allow_unlocked: true });
    } catch {
      try {
        caseStore.putCase(rec);
      } catch (err) {
        if (!applied.case_update_errors) applied.case_update_errors = [];
        applied.case_update_errors.push({ case_id: h.case_id, error: String(err?.message || err) });
      }
    }
  }

  return {
    ok: true,
    idempotent: false,
    correction_id: correctionId,
    applied,
    remaining: Math.max(
      0,
      hardCap -
        Number(reconciliation.corrected_provider_confirmed_total || 0) -
        Number(reconciliation.outstanding_liability_total || 0)
    ),
  };
}

export function writeReconciliationArtifacts(reconciliation, applyResult, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const reconPath = path.join(outDir, "pilot-spend-reconciliation.json");
  const applyPath = path.join(outDir, "pilot-spend-correction-apply.json");
  fs.writeFileSync(reconPath, JSON.stringify(reconciliation, null, 2));
  fs.writeFileSync(applyPath, JSON.stringify(applyResult, null, 2));
  return { reconPath, applyPath };
}
