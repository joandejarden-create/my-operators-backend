/**
 * Paid-processing ceiling for SEC EDGAR pilot (SEC HTTP is free).
 */

import fs from "node:fs";
import path from "node:path";
import { SEC_EDGAR_PILOT_PAID_CEILING_USD } from "./contract.js";

export const BUDGET_GUARD_VERSION = "sec-edgar-budget-guard-v1";

export function createSecEdgarBudgetGuard(opts = {}) {
  const ceilingUsd = opts.ceilingUsd ?? SEC_EDGAR_PILOT_PAID_CEILING_USD;
  const ledgerPath =
    opts.ledgerPath ||
    path.join(process.cwd(), "data", "sec-edgar", "budget-ledger.json");
  let state = { spentUsd: 0, operations: [], updatedAt: null };
  if (fs.existsSync(ledgerPath)) {
    try {
      state = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
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

  function canSpend(usd) {
    const n = Number(usd) || 0;
    if (n <= 0) return { ok: true, remaining: remaining() };
    if (n > remaining()) {
      return {
        ok: false,
        error: "SEC_EDGAR_PAID_CEILING",
        ceilingUsd,
        spentUsd: state.spentUsd,
        remaining: remaining(),
        message: "Pilot paid ceiling exhausted — request founder authorization to continue.",
      };
    }
    return { ok: true, remaining: remaining() - n };
  }

  function recordSpend(usd, meta = {}) {
    const n = Number(usd) || 0;
    const gate = canSpend(n);
    if (!gate.ok) return gate;
    state.spentUsd = Number((Number(state.spentUsd || 0) + n).toFixed(4));
    state.operations.push({
      at: new Date().toISOString(),
      usd: n,
      ...meta,
    });
    persist();
    return { ok: true, spentUsd: state.spentUsd, remaining: remaining() };
  }

  return {
    version: BUDGET_GUARD_VERSION,
    ceilingUsd,
    spentUsd: () => state.spentUsd,
    remaining,
    canSpend,
    recordSpend,
    snapshot: () => ({ ...state, ceilingUsd, remaining: remaining() }),
  };
}
