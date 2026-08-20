#!/usr/bin/env node
/**
 * ADP Official Baseline Period 001 — controlled production measurement release.
 *
 * Sequence (default --preflight-only stops before provider calls):
 *   1. Freeze measurement contract (if missing)
 *   2. Archive/classify pre-baseline periods
 *   3. Cost + scenario preflight
 *   4. Optional: re-run pre-baseline audit gate
 *   5. --apply: synchronized live full-property pull for all 4 properties
 *
 * Usage:
 *   node scripts/run-adp-official-baseline-period-001.mjs --preflight-only
 *   node scripts/run-adp-official-baseline-period-001.mjs --dry-run
 *   node scripts/run-adp-official-baseline-period-001.mjs --apply
 */

import "../load-env.js";
import { existsSync, readFileSync, writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { spawnSync } from "child_process";
import {
  buildOfficialBaselinePreflight,
  executeOfficialBaselinePeriod001,
  loadFrozenContractHash,
} from "../lib/ai-demand-positioning/execution/official-baseline-period-001-v1.js";

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const dryRun = !apply;
const preflightOnly = args.includes("--preflight-only");
const skipAuditGate = args.includes("--skip-audit-gate");

function runNode(script, scriptArgs = []) {
  const result = spawnSync(process.execPath, [script, ...scriptArgs], {
    cwd: process.cwd(),
    encoding: "utf8",
    env: process.env,
    maxBuffer: 20 * 1024 * 1024,
  });
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  return result;
}

async function main() {
  console.log("\n=== ADP OFFICIAL BASELINE PERIOD 001 ===\n");

  // 1. Freeze contract
  const contractPath = join(
    process.cwd(),
    "data/ai-demand-positioning/contracts/adp-measurement-contract-v1.json"
  );
  if (!existsSync(contractPath)) {
    console.log("Freezing measurement contract...");
    const freeze = runNode("scripts/freeze-adp-measurement-contract-v1.mjs");
    if (freeze.status !== 0) {
      console.error("BASELINE_ABORTED_PRE_RUN_GATE: contract freeze failed");
      process.exit(1);
    }
  } else {
    console.log("Measurement contract already frozen:", contractPath);
  }

  const contract = JSON.parse(readFileSync(contractPath, "utf8"));
  console.log("CONTRACT_HASH:", contract.measurementContractHash);

  // 2. Archive pre-baseline
  console.log("\nArchiving pre-baseline period registry...");
  const archive = runNode("scripts/archive-adp-pre-baseline-periods-v1.mjs");
  if (archive.status !== 0) {
    console.error("BASELINE_ABORTED_PRE_RUN_GATE: archive classification failed");
    process.exit(1);
  }

  // 3. Cost preflight
  const preflight = buildOfficialBaselinePreflight();
  console.log("\n=== PREFLIGHT ===");
  console.log(JSON.stringify(preflight, null, 2));

  if (preflight.PREFLIGHT !== "PASS") {
    console.error("\nBASELINE_ABORTED_PRE_RUN_GATE: cost exceeds $40 — founder approval required");
    process.exit(2);
  }

  // 4. Zero-cost audit gate
  if (!skipAuditGate) {
    console.log("\nRunning pre-baseline full system audit gate (zero provider calls)...");
    const audit = runNode("scripts/run-adp-pre-baseline-full-system-audit-v1.mjs");
    const auditReportPath = join(
      process.cwd(),
      "reports/ai-demand-positioning/adp-pre-baseline-full-system-audit-v1.json"
    );
    if (!existsSync(auditReportPath)) {
      console.error("BASELINE_ABORTED_PRE_RUN_GATE: audit report missing");
      process.exit(1);
    }
    const auditReport = JSON.parse(readFileSync(auditReportPath, "utf8"));
    const pass =
      auditReport.executiveVerdict === "READY_FOR_MEASUREMENT_CONTRACT_FREEZE" ||
      auditReport.final === "ADP_PRE_BASELINE_FULL_SYSTEM_AUDIT_V1_PASS" ||
      audit.status === 0;
    // Soft: audit may still pass with P2; hard fail only on P0/P1 or failed gates
    const p0 = auditReport.P0_BLOCKERS ?? auditReport.p0Open ?? 0;
    const p1 = auditReport.P1_BLOCKERS ?? auditReport.p1Open ?? 0;
    const gateFail = Object.values(auditReport.gates || {}).some((g) => g.STATUS === "FAIL");
    if (!pass || p0 > 0 || p1 > 0 || gateFail) {
      console.error("BASELINE_ABORTED_PRE_RUN_GATE");
      console.error(JSON.stringify({ p0, p1, gateFail, verdict: auditReport.executiveVerdict }, null, 2));
      process.exit(1);
    }
    console.log("PRE_BASELINE_FULL_SYSTEM_AUDIT_STATUS = PASS");
  }

  if (preflightOnly) {
    console.log("\n--preflight-only: stopping before provider calls.");
    const out = {
      MEASUREMENT_CONTRACT_V1_FROZEN: "YES",
      MEASUREMENT_CONTRACT_HASH_PRESENT: "YES",
      CONTRACT_HASH: loadFrozenContractHash(),
      PREFLIGHT: preflight.PREFLIGHT,
      TOTAL_PLANNED_PROVIDER_CALLS: preflight.TOTAL_PLANNED_PROVIDER_CALLS,
      ESTIMATED_COST: preflight.TOTAL_ESTIMATED_COST,
      NEXT: apply ? "RUN_APPLY" : "AWAITING_APPLY",
    };
    const path = join(
      process.cwd(),
      "reports/ai-demand-positioning/adp-official-baseline-period-001-preflight.json"
    );
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, JSON.stringify({ ...out, preflight }, null, 2) + "\n");
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  console.log(`\nExecuting official baseline (${dryRun ? "DRY RUN" : "LIVE APPLY"})...`);
  const result = await executeOfficialBaselinePeriod001({
    dryRun,
    onProgress: ({ propertyId, completed, total }) => {
      if (completed % 10 === 0 || completed === total) {
        console.log(`  [${propertyId}] ${completed}/${total}`);
      }
    },
  });
  console.log("\n");
  console.log(JSON.stringify({
    status: result.status,
    RUN_START: result.RUN_START,
    RUN_END: result.RUN_END,
    PROVIDER_CALLS_ATTEMPTED: result.PROVIDER_CALLS_ATTEMPTED,
    PROVIDER_CALLS_SUCCESSFUL: result.PROVIDER_CALLS_SUCCESSFUL,
    PROVIDER_CALLS_FAILED: result.PROVIDER_CALLS_FAILED,
    ACTUAL_SPEND: result.ACTUAL_SPEND,
    properties: result.propertyResults?.map((r) => ({
      PROPERTY: r.PROPERTY,
      PERIOD_ID: r.PERIOD_ID,
      STATUS: r.STATUS,
      SCENARIOS: r.SCENARIOS,
    })),
    reportFile: result.reportFile,
  }, null, 2));

  if (!result.ok && !dryRun) {
    process.exit(3);
  }
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
