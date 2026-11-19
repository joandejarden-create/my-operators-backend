#!/usr/bin/env node
/**
 * HE-INTEL-001 P3A dry-run — $0 paid, staging artifacts only.
 *
 *   node scripts/dry-run-he-intel-001-p3a-research-benchmark-v1.mjs
 *   node scripts/dry-run-he-intel-001-p3a-research-benchmark-v1.mjs --write-artifacts
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runP3ABenchmarkDryRun,
  buildProviderCapabilityMatrix,
  proposedMinimumPaidBenchmarkBudget,
  buildP3BExecutionPlan,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3a/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const WRITE = process.argv.includes("--write-artifacts");

const report = await runP3ABenchmarkDryRun({
  memoryOnly: !WRITE,
  runId: `p3a_dry_${new Date().toISOString().replace(/[:.]/g, "-")}`,
  checkEnv: true,
  artifactRoot: path.join(
    ROOT,
    "reports/hotel-explorer/he-intel-001-research-benchmark/runs"
  ),
});

const outDir = path.join(ROOT, "reports/hotel-explorer");
fs.mkdirSync(outDir, { recursive: true });
const summaryPath = path.join(outDir, "HE-INTEL-001-P3A-dry-run-summary-v1.json");
const summary = {
  ok: report.ok,
  phase: report.phase,
  contract: report.contract,
  contract_version: report.contract_version,
  runId: report.runId,
  cohort: report.cohort,
  authorizationSummary: report.authorizationSummary,
  leakageViolations: report.leakageViolations,
  dryRunExecuted: report.dryRunExecuted,
  sharedBudgetBypassPossible: report.sharedBudgetBypassPossible,
  proposedP3BBudget: proposedMinimumPaidBenchmarkBudget(),
  p3bExecutionPlan: buildP3BExecutionPlan(),
  capabilityMatrix: buildProviderCapabilityMatrix({ checkEnv: true }),
  guarantees: report.guarantees,
  costLedger: {
    runUsd: report.costLedger.runUsd,
    runUsdStatus: report.costLedger.runUsdStatus,
    refused: report.costLedger.refused,
    entryCount: report.costLedger.entries.length,
  },
};
fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
console.log(JSON.stringify({ ok: report.ok, summaryPath, runId: report.runId }, null, 2));
process.exit(report.ok ? 0 : 2);
