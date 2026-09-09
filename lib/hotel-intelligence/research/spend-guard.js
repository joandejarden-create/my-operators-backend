/**
 * Packet 2.6C-R2 — concurrent + daily spend guards for live Webhound.
 */

import fs from "node:fs";
import path from "node:path";
import { resolveDataRoot, ensureDir, readJsonFile, writeJsonFile } from "../local-store.js";
import {
  getDailyExternalBudgetUsd,
  getMaxConcurrentWebhoundRuns,
  PILOT_MAX_PROVIDER_BUDGET_USD,
} from "./policy.js";
import { isActiveStatus } from "./statuses.js";

function utcDayKey(d = new Date()) {
  return d.toISOString().slice(0, 10);
}

function spendLedgerPath(env) {
  const root = path.join(resolveDataRoot(env), "research");
  ensureDir(root);
  return path.join(root, "spend-ledger.json");
}

export function readSpendLedger(env = process.env) {
  return readJsonFile(spendLedgerPath(env), {
    version: "hi-research-spend-ledger-v1",
    days: {},
    updated_at: null,
  });
}

export function recordExternalSpend(input = {}, env = process.env) {
  const day = input.day || utcDayKey();
  const amount = Number(input.amount_usd);
  if (!Number.isFinite(amount) || amount < 0) return readSpendLedger(env);
  const ledger = readSpendLedger(env);
  const row = ledger.days[day] || { reserved_usd: 0, spent_usd: 0, runs: [] };
  if (input.kind === "reserve") {
    row.reserved_usd = Number(((row.reserved_usd || 0) + amount).toFixed(4));
  } else if (input.kind === "release_reserve") {
    row.reserved_usd = Math.max(0, Number(((row.reserved_usd || 0) - amount).toFixed(4)));
  } else {
    row.spent_usd = Number(((row.spent_usd || 0) + amount).toFixed(4));
    if (input.kind === "settle_reserve") {
      row.reserved_usd = Math.max(0, Number(((row.reserved_usd || 0) - (input.reserved_usd ?? amount)).toFixed(4)));
    }
  }
  row.runs.push({
    at: new Date().toISOString(),
    hotel_id: input.hotel_id || null,
    request_id: input.request_id || null,
    template_id: input.template_id || null,
    amount_usd: amount,
    kind: input.kind || "spent",
    provider_run_id: input.provider_run_id || null,
  });
  ledger.days[day] = row;
  ledger.updated_at = new Date().toISOString();
  writeJsonFile(spendLedgerPath(env), ledger);
  return ledger;
}

export function getDailySpendSnapshot(env = process.env, day = utcDayKey()) {
  const ledger = readSpendLedger(env);
  const row = ledger.days[day] || { reserved_usd: 0, spent_usd: 0, runs: [] };
  const budget = getDailyExternalBudgetUsd(env);
  const used = Number(row.spent_usd || 0) + Number(row.reserved_usd || 0);
  return {
    day,
    spent_usd: Number(row.spent_usd || 0),
    reserved_usd: Number(row.reserved_usd || 0),
    used_usd: Number(used.toFixed(4)),
    daily_budget_usd: budget,
    remaining_usd: Number(Math.max(0, budget - used).toFixed(4)),
  };
}

/**
 * Count active non-simulation Webhound runs across hotel indexes under research root.
 */
export function countActiveWebhoundRuns(repository, env = process.env) {
  if (typeof repository.countActiveExternalRuns === "function") {
    return repository.countActiveExternalRuns();
  }
  const root = path.join(resolveDataRoot(env), "research", "hotels");
  if (!fs.existsSync(root)) return 0;
  let count = 0;
  for (const name of fs.readdirSync(root)) {
    const idxPath = path.join(root, name, "index.json");
    if (!fs.existsSync(idxPath)) continue;
    try {
      const idx = JSON.parse(fs.readFileSync(idxPath, "utf8"));
      for (const r of idx.requests || []) {
        if (r.simulated || r.is_simulation) continue;
        if (String(r.provider_strategy || "").toUpperCase() !== "WEBHOUND") continue;
        if (isActiveStatus(r.status)) count += 1;
      }
    } catch {
      /* skip corrupt */
    }
  }
  return count;
}

/**
 * Assert a new paid Webhound run may start.
 */
export function assertMayStartPaidWebhoundRun(input = {}) {
  const env = input.env || process.env;
  const maxConcurrent = getMaxConcurrentWebhoundRuns(env);
  const active = countActiveWebhoundRuns(input.repository, env);
  if (active >= maxConcurrent) {
    const err = new Error("max_concurrent_webhound_runs");
    err.code = "max_concurrent_webhound_runs";
    err.customer_safe =
      "Another Deep Research investigation is currently running. Try again when it completes.";
    err.active = active;
    err.max = maxConcurrent;
    throw err;
  }

  const snap = getDailySpendSnapshot(env);
  const need = Number(input.budget_usd ?? PILOT_MAX_PROVIDER_BUDGET_USD);
  if (snap.remaining_usd + 1e-9 < need) {
    const err = new Error("daily_external_research_budget_exhausted");
    err.code = "daily_external_research_budget_exhausted";
    err.customer_safe = "Deep Research is currently unavailable. Try again shortly.";
    err.snapshot = snap;
    throw err;
  }
  return { ok: true, active, maxConcurrent, daily: snap };
}
