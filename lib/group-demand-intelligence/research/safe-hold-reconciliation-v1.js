/**
 * Safe provider-hold reconciliation V1 (run-050).
 *
 * Never release unreconciled_hold entries at $0 without evidence that the
 * provider charged nothing (or the reservation was never submitted).
 */

import fs from "node:fs";
import { reconcileHold } from "./operational-campaign-shared-budget-v1.js";

export const SAFE_HOLD_RECONCILIATION_V1 = "gdi-safe-hold-reconciliation-v1";

/**
 * @param {string} ledgerPath
 * @param {object} [opts]
 * @param {boolean} [opts.allowZeroWithoutEvidence=false] — forbidden in campaigns
 * @param {Array<{reservationId:string,verifiedUsd:number,evidence?:string}>} [opts.verifiedOutcomes]
 */
export function reconcileHoldsSafely(ledgerPath, opts = {}) {
  const {
    allowZeroWithoutEvidence = false,
    verifiedOutcomes = [],
  } = opts;

  if (!fs.existsSync(ledgerPath)) {
    return { ok: false, reason: "missing_ledger", cleared: [], retained: [] };
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  const holds = (ledger.entries || []).filter(
    (e) => e.status === "unreconciled_hold" || e.status === "reserved",
  );
  const byId = new Map(
    (verifiedOutcomes || []).map((o) => [String(o.reservationId), o]),
  );
  const cleared = [];
  const retained = [];
  const refused = [];

  for (const h of holds) {
    const verified = byId.get(String(h.reservationId));
    if (!verified) {
      if (allowZeroWithoutEvidence) {
        refused.push({
          reservationId: h.reservationId,
          reason: "allowZeroWithoutEvidence_is_true_but_campaigns_must_not_use_it",
        });
      }
      retained.push({
        reservationId: h.reservationId,
        jobKey: h.jobKey,
        quotedUsd: h.quotedUsd,
        reason: "no_verified_provider_outcome",
        action: "keep_reserved_fail_closed",
      });
      continue;
    }
    const amount = Number(verified.verifiedUsd);
    if (!Number.isFinite(amount) || amount < 0) {
      retained.push({
        reservationId: h.reservationId,
        reason: "invalid_verified_amount",
      });
      continue;
    }
    if (amount === 0 && !verified.evidence && !verified.providerConfirmation) {
      retained.push({
        reservationId: h.reservationId,
        reason: "zero_dollar_without_evidence_refused",
        action: "keep_reserved",
      });
      refused.push({
        reservationId: h.reservationId,
        reason: "zero_dollar_requires_provider_confirmation_or_evidence",
      });
      continue;
    }
    const r = reconcileHold(ledgerPath, h.reservationId, amount);
    cleared.push({
      reservationId: h.reservationId,
      verifiedUsd: amount,
      evidence: verified.evidence || verified.providerConfirmation || null,
      ok: r.ok,
      result: r,
    });
  }

  return {
    ok: refused.length === 0,
    version: SAFE_HOLD_RECONCILIATION_V1,
    cleared,
    retained,
    refused,
    note: "Unknown provider charges remain reserved until verified — never auto-classified as free.",
  };
}

/**
 * Campaign helper: replace unsafe clearHolds() that reconciled all holds at $0.
 */
export function clearHoldsOnlyWhenVerified(ledgerPath, verifiedOutcomes = []) {
  return reconcileHoldsSafely(ledgerPath, {
    allowZeroWithoutEvidence: false,
    verifiedOutcomes,
  });
}
