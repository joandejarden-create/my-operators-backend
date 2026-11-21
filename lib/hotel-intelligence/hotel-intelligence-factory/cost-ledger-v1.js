/**
 * Provider-level + cumulative cost ledger for factory runs.
 */

import fs from "node:fs";
import path from "node:path";
import { runDir } from "./checkpoint-v1.js";

export const HIF_COST_LEDGER_V1 = "hotel-intelligence-factory-cost-ledger-v1";

export function createCostLedger(opts = {}) {
  const ceilingUsd = Number(opts.ceilingUsd != null ? opts.ceilingUsd : 25);
  const entries = [];
  let spentUsd = 0;

  function remaining() {
    return Math.max(0, ceilingUsd - spentUsd);
  }

  function canSpend(usd) {
    const n = Number(usd) || 0;
    if (spentUsd + n > ceilingUsd + 1e-9) {
      return { ok: false, error: "BUDGET_EXCEEDED", spentUsd, ceilingUsd, requested: n };
    }
    return { ok: true };
  }

  function record({ provider, operation, usd = 0, note = null, fingerprint = null }) {
    const gate = canSpend(usd);
    if (!gate.ok) return gate;
    spentUsd += Number(usd) || 0;
    entries.push({
      at: new Date().toISOString(),
      provider,
      operation,
      usd: Number(usd) || 0,
      note,
      fingerprint,
    });
    return { ok: true, spentUsd, remaining: remaining() };
  }

  function snapshot() {
    const byProvider = {};
    for (const e of entries) {
      byProvider[e.provider] = byProvider[e.provider] || { usd: 0, ops: 0 };
      byProvider[e.provider].usd += e.usd;
      byProvider[e.provider].ops += 1;
    }
    return {
      version: HIF_COST_LEDGER_V1,
      ceilingUsd,
      spentUsd,
      remainingUsd: remaining(),
      byProvider,
      entries,
    };
  }

  function persist(durableRootOrCwd, runId) {
    const dir = runDir(durableRootOrCwd, runId);
    fs.mkdirSync(dir, { recursive: true });
    const snap = snapshot();
    fs.writeFileSync(path.join(dir, "cost-ledger.json"), JSON.stringify(snap, null, 2), "utf8");
    // Cumulative ledger across runs (durable)
    try {
      const cumPath = path.join(path.dirname(dir), "..", "ledgers", "cumulative-cost.json");
      fs.mkdirSync(path.dirname(cumPath), { recursive: true });
      let cum = { spentUsd: 0, runs: {} };
      if (fs.existsSync(cumPath)) {
        cum = JSON.parse(fs.readFileSync(cumPath, "utf8"));
      }
      cum.runs[runId] = { spentUsd: snap.spentUsd, at: new Date().toISOString() };
      cum.spentUsd = Object.values(cum.runs).reduce((a, r) => a + (r.spentUsd || 0), 0);
      fs.writeFileSync(cumPath, JSON.stringify(cum, null, 2), "utf8");
    } catch {
      /* non-fatal */
    }
    return snap;
  }

  return { ceilingUsd, remaining, canSpend, record, snapshot, persist };
}

export default { createCostLedger };
