/**
 * Parallel provider cost ledger — local only.
 */

import fs from "node:fs";
import path from "node:path";
import { resolveDataRoot, ensureDir, readJsonFile, writeJsonFile } from "../../../local-store.js";

export const PARALLEL_COST_LEDGER_VERSION = "parallel-cost-ledger-v1";

function ledgerPath(env = process.env) {
  return path.join(resolveDataRoot(env), "research", "providers", "parallel", "cost-ledger.json");
}

export function readParallelCostLedger(env = process.env) {
  return readJsonFile(ledgerPath(env), {
    version: PARALLEL_COST_LEDGER_VERSION,
    entries: [],
    totals: {
      provider_cost_usd: 0,
      runs: 0,
      accepted_claims: 0,
      accepted_contacts: 0,
      accepted_critical_facts: 0,
    },
  });
}

export function appendParallelCostEntry(entry, env = process.env) {
  const ledger = readParallelCostLedger(env);
  const cost = Number(entry.provider_cost_usd || 0);
  const rec = {
    recorded_at: new Date().toISOString(),
    provider: "PARALLEL",
    run_id: entry.run_id || null,
    task_type: entry.task_type || entry.template_id || null,
    hotel_id: entry.hotel_id || null,
    owner_entity_id: entry.owner_entity_id || null,
    project_id: entry.project_id || null,
    queries_visible: entry.queries_visible ?? null,
    tokens_visible: entry.tokens_visible ?? null,
    runtime_ms: entry.runtime_ms ?? null,
    provider_cost_usd: Number.isFinite(cost) ? cost : null,
    accepted_claims: Number(entry.accepted_claims || 0),
    accepted_contacts: Number(entry.accepted_contacts || 0),
    accepted_critical_facts: Number(entry.accepted_critical_facts || 0),
    benchmark_case_id: entry.benchmark_case_id || null,
  };

  ledger.entries.push(rec);
  ledger.totals.runs += 1;
  ledger.totals.provider_cost_usd = Number(
    (ledger.totals.provider_cost_usd + (rec.provider_cost_usd || 0)).toFixed(4)
  );
  ledger.totals.accepted_claims += rec.accepted_claims;
  ledger.totals.accepted_contacts += rec.accepted_contacts;
  ledger.totals.accepted_critical_facts += rec.accepted_critical_facts;

  const root = path.dirname(ledgerPath(env));
  ensureDir(root);
  writeJsonFile(ledgerPath(env), ledger);
  return { entry: rec, totals: ledger.totals };
}

export function computeParallelUnitEconomics(entry = {}) {
  const cost = Number(entry.provider_cost_usd || 0);
  const claims = Math.max(0, Number(entry.accepted_claims || 0));
  const facts = Math.max(0, Number(entry.accepted_critical_facts || 0));
  const contacts = Math.max(0, Number(entry.accepted_contacts || 0));
  return {
    cost_per_accepted_claim: claims > 0 ? Number((cost / claims).toFixed(4)) : null,
    cost_per_accepted_critical_fact: facts > 0 ? Number((cost / facts).toFixed(4)) : null,
    cost_per_usable_contact: contacts > 0 ? Number((cost / contacts).toFixed(4)) : null,
  };
}
