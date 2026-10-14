/**
 * Reconcile connectivity probe run ID into the Parallel budget ledger.
 * Does NOT reset the ledger or create a fresh $2 allowance.
 *
 * Actual billing unknown — probe is treated as estimated spend (conservative).
 *
 * Usage:
 *   node scripts/gdi-reconcile-parallel-budget-probe-v1.mjs
 *   node scripts/gdi-reconcile-parallel-budget-probe-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  readParallelBudgetLedger,
  remainingParallelBudgetUsd,
} from "../lib/group-demand-intelligence/parallel/budget-ledger-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const RESEARCH_RUN_ID = "trun_567f3335d0c140ad8e87120211c6f61f";
const PROBE_RUN_ID = "trun_567f3335d0c140ad9269500c06dc2b38";
/** Conservative estimate — actual billed unknown; do not treat probe as free. */
const PROBE_ESTIMATED_USD = 0.5;

const apply = process.argv.includes("--apply");

const ledgerPath =
  process.env.GDI_PARALLEL_BUDGET_LEDGER_PATH ||
  path.join(
    ROOT,
    "reports/gdi-drawer-completeness/_local-repaired-store/parallel-budget/ledger.json"
  );

const env = {
  ...process.env,
  GDI_PARALLEL_BUDGET_LEDGER_PATH: ledgerPath,
  GDI_PARALLEL_ENABLED: "1",
};

let ledger = readParallelBudgetLedger({ env, budgetUsd: 2 });

const hasResearch = (ledger.entries || []).some(
  (e) => e.providerRunId === RESEARCH_RUN_ID && e.status === "spent"
);
const hasProbe = (ledger.entries || []).some(
  (e) => e.providerRunId === PROBE_RUN_ID
);

const report = {
  ledgerPath,
  apply,
  researchRunId: RESEARCH_RUN_ID,
  probeRunId: PROBE_RUN_ID,
  researchLedgered: hasResearch,
  probePreviouslyLedgered: hasProbe,
  before: {
    budgetUsd: ledger.budgetUsd,
    estimatedSpentUsd: ledger.estimatedSpentUsd,
    reservedUsd: ledger.reservedUsd,
    remainingUsd: remainingParallelBudgetUsd(ledger),
  },
  probeAccounting: {
    estimatedUsd: PROBE_ESTIMATED_USD,
    note: "Actual billed unknown — probe counted toward $2 cap conservatively",
  },
};

if (!hasProbe && apply) {
  const entry = {
    reservationId: `rsv_probe_reconcile_${Date.now().toString(36)}`,
    jobKey: "gdi-parallel-recnPB3Q02NO-connectivity-probe",
    hotelId: "recnPB3Q02NO8Ma1o",
    label: "parallel_connectivity_probe",
    processor: "base",
    estimatedUsd: PROBE_ESTIMATED_USD,
    status: "spent",
    reservedAt: "2026-10-14T22:00:00.000Z",
    spentAt: new Date().toISOString(),
    providerRunId: PROBE_RUN_ID,
    note: "Reconciled after-the-fact — probe was not free; actual billed unknown",
  };
  ledger.entries = [...(ledger.entries || []), entry];
  ledger.estimatedSpentUsd =
    Number(ledger.estimatedSpentUsd || 0) + PROBE_ESTIMATED_USD;
  ledger.updatedAt = new Date().toISOString();
  // strip path helper if present
  const { path: _p, ...toWrite } = ledger;
  fs.writeFileSync(ledgerPath, JSON.stringify(toWrite, null, 2) + "\n");
  ledger = readParallelBudgetLedger({ env, budgetUsd: 2 });
}

report.after = {
  budgetUsd: ledger.budgetUsd,
  estimatedSpentUsd: ledger.estimatedSpentUsd,
  reservedUsd: ledger.reservedUsd,
  remainingUsd: remainingParallelBudgetUsd(ledger),
};

const remaining = remainingParallelBudgetUsd(ledger);
const correctedLiveFloor = 0.5; // conservative floor used by live adapter
report.correctedLiveValidation = {
  requiredEstimatedFloorUsd: correctedLiveFloor,
  safeRemainingAllowance: remaining >= correctedLiveFloor,
  remainingUsd: remaining,
  decision:
    remaining >= correctedLiveFloor
      ? "proceed_with_one_identity_lock_live_call"
      : "finish_non_paid_only_budget_exhausted",
};

const outDir = path.join(
  ROOT,
  "reports/gdi-operational-baseline/budget-reconciliation"
);
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "parallel-budget-reconciliation.json"),
  JSON.stringify(report, null, 2) + "\n"
);
console.log(JSON.stringify(report, null, 2));
