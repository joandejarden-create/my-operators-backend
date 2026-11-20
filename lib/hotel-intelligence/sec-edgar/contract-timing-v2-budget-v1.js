/**
 * Contract Timing V2 — $25 incremental paid ceiling across Exa + SEC-API.io.
 * Free official SEC EDGAR retrieval is not charged against this ceiling.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

export const CONTRACT_TIMING_V2_BUDGET_ID = "contract-timing-v2-budget-v1";
export const CONTRACT_TIMING_V2_PAID_CEILING_USD = 25;

/** Conservative unit costs for accounting (SEC-API.io is subscription; calls counted separately). */
export const PROVIDER_UNIT_COST_USD = Object.freeze({
  EXA_SEARCH_LITE: 0.005,
  EXA_CONTENTS: 0.001,
  /** Subscription call — track count; charge $0 against incremental ceiling unless overage known */
  SEC_API_IO_FULL_TEXT: 0,
  SEC_API_IO_FILING_QUERY: 0,
  SEC_API_IO_DOWNLOAD_GB: 0.3,
});

export function defaultBudgetLedgerPath(cwd = process.cwd()) {
  return path.join(cwd, "data", "sec-edgar", "contract-timing-v2-budget-ledger.json");
}

export function createContractTimingV2Budget(opts = {}) {
  const ceilingUsd = opts.ceilingUsd ?? CONTRACT_TIMING_V2_PAID_CEILING_USD;
  const ledgerPath = opts.ledgerPath || defaultBudgetLedgerPath(opts.cwd || process.cwd());
  let state = {
    version: CONTRACT_TIMING_V2_BUDGET_ID,
    spentUsd: 0,
    byProvider: { EXA: 0, SEC_API_IO: 0, FREE_SEC: 0 },
    requestCounts: { EXA: 0, SEC_API_IO: 0, FREE_SEC: 0 },
    operations: [],
    fingerprints: {},
    stoppedAtBudget: false,
    updatedAt: null,
  };
  if (fs.existsSync(ledgerPath)) {
    try {
      state = { ...state, ...JSON.parse(fs.readFileSync(ledgerPath, "utf8")) };
    } catch {
      /* keep default */
    }
  }

  function persist() {
    fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
    state.updatedAt = new Date().toISOString();
    fs.writeFileSync(ledgerPath, JSON.stringify(state, null, 2), "utf8");
  }

  function remaining() {
    return Math.max(0, Number(ceilingUsd) - Number(state.spentUsd || 0));
  }

  function fingerprint(provider, operation, payload) {
    const raw = JSON.stringify({ provider, operation, payload });
    return crypto.createHash("sha1").update(raw).digest("hex");
  }

  function canSpend(usd) {
    const n = Number(usd) || 0;
    if (state.stoppedAtBudget && n > 0) {
      return { ok: false, error: "BUDGET_STOPPED", remaining: remaining() };
    }
    if (n <= 0) return { ok: true, remaining: remaining() };
    if (n > remaining()) {
      return {
        ok: false,
        error: "CONTRACT_TIMING_V2_PAID_CEILING",
        ceilingUsd,
        spentUsd: state.spentUsd,
        remaining: remaining(),
      };
    }
    return { ok: true, remaining: remaining() - n };
  }

  function record(meta = {}) {
    const usd = Number(meta.usd) || 0;
    const provider = String(meta.provider || "UNKNOWN").toUpperCase();
    const gate = canSpend(usd);
    if (!gate.ok) {
      state.stoppedAtBudget = true;
      persist();
      return gate;
    }
    if (meta.fingerprint && state.fingerprints[meta.fingerprint] && meta.skipIfCached) {
      return { ok: true, cached: true, remaining: remaining() };
    }
    state.spentUsd = Number((Number(state.spentUsd || 0) + usd).toFixed(6));
    if (!state.byProvider[provider]) state.byProvider[provider] = 0;
    state.byProvider[provider] = Number((state.byProvider[provider] + usd).toFixed(6));
    if (!state.requestCounts[provider]) state.requestCounts[provider] = 0;
    state.requestCounts[provider] += 1;
    if (meta.fingerprint) {
      state.fingerprints[meta.fingerprint] = {
        at: new Date().toISOString(),
        usd,
        provider,
        operation: meta.operation || null,
      };
    }
    state.operations.push({
      at: new Date().toISOString(),
      usd,
      provider,
      operation: meta.operation || null,
      fingerprint: meta.fingerprint || null,
      note: meta.note || null,
    });
    if (remaining() <= 0 && usd > 0) state.stoppedAtBudget = true;
    persist();
    return { ok: true, spentUsd: state.spentUsd, remaining: remaining() };
  }

  return {
    version: CONTRACT_TIMING_V2_BUDGET_ID,
    ceilingUsd,
    ledgerPath,
    spentUsd: () => state.spentUsd,
    remaining,
    canSpend,
    record,
    fingerprint,
    hasFingerprint: (fp) => Boolean(state.fingerprints[fp]),
    stopPaid: () => {
      state.stoppedAtBudget = true;
      persist();
    },
    snapshot: () => ({
      ...state,
      ceilingUsd,
      remaining: remaining(),
    }),
  };
}

export default { createContractTimingV2Budget, CONTRACT_TIMING_V2_PAID_CEILING_USD };
