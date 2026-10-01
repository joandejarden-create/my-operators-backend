/**
 * Operation-level Context.dev spend from durable journals.
 * Explicit provider credits_consumed=0 is a valid settled zero.
 * Missing metadata retains outstanding liability (does not invent spend).
 */

import { readProviderConfirmedCredits } from "../../context-dev/operation-credit-bounds.js";

/**
 * @param {Array} operationJournal
 * @returns {{
 *   ok: boolean,
 *   complete: boolean,
 *   settled_credits: number,
 *   settled_calls: number,
 *   outstanding_credits: number,
 *   unknown_ops: number,
 *   confirmed_ops: number,
 *   unique_ops: number,
 *   corrections: Array<object>,
 *   ops: Array<object>,
 *   source: string,
 * }}
 */
export function computeAuthoritativeContextDevSpendFromJournal(operationJournal = []) {
  const journal = Array.isArray(operationJournal) ? operationJournal : [];
  const byKey = new Map();

  for (const e of journal) {
    const wk = e?.work_key;
    if (!wk || !/^context_dev:/.test(String(wk))) continue;
    if (
      e.event === "replay_stored_result" ||
      e.event === "already_completed_missing_result" ||
      e.event === "replay_restricted_blocked" ||
      e.kind === "skip"
    ) {
      continue;
    }
    const prev = byKey.get(wk) || {
      work_key: wk,
      reserved_credits: null,
      settled_credits: null,
      durable_result: null,
      billing_settled: null,
      keep_outstanding: false,
      events: [],
    };
    prev.events.push(e.event);
    if (e.reservation_credits != null) prev.reserved_credits = Number(e.reservation_credits);
    if (e.settled_credits != null && Number.isFinite(Number(e.settled_credits))) {
      prev.settled_credits = Number(e.settled_credits);
    }
    if (e.durable_result) prev.durable_result = e.durable_result;
    if (e.key_metadata && !prev.durable_result?.key_metadata) {
      prev.durable_result = { ...(prev.durable_result || {}), key_metadata: e.key_metadata };
    }
    if (e.billing_settled === false || e.keep_outstanding === true) {
      prev.keep_outstanding = true;
    }
    if (e.billing_settled === true) prev.billing_settled = true;
    byKey.set(wk, prev);
  }

  const ops = [];
  let settled_credits = 0;
  let settled_calls = 0;
  let outstanding_credits = 0;
  let unknown_ops = 0;
  let confirmed_ops = 0;
  const corrections = [];

  for (const op of byKey.values()) {
    const confirmed = readProviderConfirmedCredits(op.durable_result);
    const reserved = Number(op.reserved_credits);
    let credited = null;
    let source = null;

    if (confirmed.ok) {
      credited = confirmed.credits; // may be 0 — explicit zero is valid settlement
      source = "provider_key_metadata.credits_consumed";
      confirmed_ops += 1;
      if (
        op.settled_credits != null &&
        Number.isFinite(op.settled_credits) &&
        op.settled_credits !== credited
      ) {
        corrections.push({
          work_key: op.work_key,
          previous_value: op.settled_credits,
          corrected_value: credited,
          evidence: source,
        });
      } else if (
        credited === 0 &&
        Number.isFinite(reserved) &&
        reserved > 0 &&
        (op.settled_credits == null || op.settled_credits !== 0)
      ) {
        // Snapshot/reservation may have counted this op; provider reported zero.
        corrections.push({
          work_key: op.work_key,
          previous_value: op.settled_credits != null ? op.settled_credits : reserved,
          corrected_value: 0,
          evidence: source,
        });
      }
    } else if (op.keep_outstanding || op.billing_settled === false) {
      unknown_ops += 1;
      outstanding_credits += Number.isFinite(reserved) ? reserved : 0;
      source = "billing_unsettled_retain_liability";
    } else if (op.settled_credits != null && Number.isFinite(op.settled_credits)) {
      credited = op.settled_credits;
      source = "journal_settled_credits";
      confirmed_ops += 1;
    } else if (Number.isFinite(reserved) && reserved >= 0) {
      unknown_ops += 1;
      outstanding_credits += reserved;
      source = "missing_metadata_retain_liability";
    } else {
      unknown_ops += 1;
      source = "unrepresented_operation";
    }

    if (credited != null && Number.isFinite(credited)) {
      settled_credits += credited;
      settled_calls += 1;
    }

    ops.push({
      work_key: op.work_key,
      reserved_credits: Number.isFinite(reserved) ? reserved : null,
      credited,
      source,
      provider_credits_consumed: confirmed.ok ? confirmed.credits : null,
    });
  }

  const complete = byKey.size > 0 && unknown_ops === 0;
  return {
    ok: byKey.size > 0,
    complete,
    settled_credits,
    settled_calls,
    outstanding_credits,
    unknown_ops,
    confirmed_ops,
    unique_ops: byKey.size,
    corrections,
    ops,
    source: complete
      ? "provider_key_metadata_per_request"
      : byKey.size > 0
        ? "partial_provider_metadata_plus_liability"
        : "none",
  };
}
