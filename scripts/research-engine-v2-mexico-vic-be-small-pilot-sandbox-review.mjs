/**
 * Read-only founder review of VIC → BE small pilot sandbox patch.
 *
 *   npm run research-engine-v2:mexico-vic-be-small-pilot-sandbox-review
 *
 * No Airtable writes (production or sandbox).
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import "dotenv/config";
import {
  runMexicoVicBeSmallPilotSandboxReview,
  renderSandboxReviewMarkdown,
  REVIEW_STATUS,
} from "../lib/research-engine-v2/mexico-vic-be-small-pilot-sandbox-review.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");
const EXEC_JSON = join(REPORTS, "mexico-vic-be-small-pilot-sandbox-patch-execution.json");

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

function runCmd(command, args, timeoutMs = 600000) {
  return new Promise((resolve) => {
    const child = spawn(command, args, { cwd: ROOT, shell: true, env: process.env });
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => {
      child.kill("SIGTERM");
      resolve({ ok: false, timedOut: true, stdout: stdout.slice(-4000), stderr: stderr.slice(-4000) });
    }, timeoutMs);
    child.stdout.on("data", (d) => {
      stdout += d.toString();
    });
    child.stderr.on("data", (d) => {
      stderr += d.toString();
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      resolve({ ok: code === 0, code, timedOut: false, stdout: stdout.slice(-8000), stderr: stderr.slice(-8000) });
    });
  });
}

async function main() {
  console.log("[sandbox-review] read-only review of vic.pilot.* sandbox rows");

  const report = await runMexicoVicBeSmallPilotSandboxReview();

  // Cite prior execution + optional live prod checks
  if (existsSync(EXEC_JSON)) {
    try {
      const exec = JSON.parse(readFileSync(EXEC_JSON, "utf8"));
      report.prior_execution = {
        status: exec.status,
        ops_executed: exec.ops_executed,
        production_writes: exec.production_writes,
        brand_status_unchanged: exec.brand_status_unchanged,
        recent_momentum_unchanged: exec.recent_momentum_unchanged,
        production_protected_checks: exec.production_protected_checks || null,
      };
      if (exec.production_protected_checks) {
        report.production_safety = {
          ...report.production_safety,
          cited_from_execution: true,
          active_universe_sot_ok: exec.production_protected_checks.active_universe_sot_ok,
          semantic_audit_ok: exec.production_protected_checks.semantic_audit_ok,
          momentum_evidence_ok: exec.production_protected_checks.momentum_evidence_ok,
          mandatory_release_gates_ok: exec.production_protected_checks.mandatory_release_gates_ok,
        };
      }
    } catch (err) {
      report.prior_execution_error = err.message;
    }
  }

  if (process.env.BE_PILOT_REVIEW_RUN_PROD_CHECKS === "1") {
    console.log("[sandbox-review] optional production checks…");
    const sot = await runCmd("npm", ["run", "brand-explorer-active-universe-source-of-truth", "--", "--dry-run"]);
    const sem = await runCmd("npm", [
      "run",
      "brand-explorer-global-active-semantic-audit",
      "--",
      "--dry-run",
      "--fresh",
    ]);
    report.production_safety.live_recheck = {
      active_universe_sot_ok: sot.ok,
      semantic_audit_ok: sem.ok,
    };
  }

  const md = renderSandboxReviewMarkdown(report);
  writeJson(join(REPORTS, "mexico-vic-be-small-pilot-sandbox-review.json"), report);
  writeMd(join(REPORTS, "mexico-vic-be-small-pilot-sandbox-review.md"), md);
  writeMd(
    join(DOCS, "mexico-vic-be-small-pilot-sandbox-review.md"),
    `${md}\n\n## Scope\n\nRead-only founder review packet. No production recommendation unless requested separately.\n`
  );

  console.log(`[sandbox-review] status=${report.status}`);
  console.log(`[sandbox-review] recommendation=${report.recommendation}`);
  console.log(
    `[sandbox-review] rows found=${report.row_inventory?.found} expected=${report.row_inventory?.expected} ok=${report.row_inventory?.row_count_ok}`
  );
  console.log(`[sandbox-review] expanded_may_proceed=${report.expanded_sandbox_pilot_may_proceed}`);

  if (
    report.status === REVIEW_STATUS.HOLD ||
    report.status === REVIEW_STATUS.ROW_MISMATCH
  ) {
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error("[sandbox-review] FATAL", err);
  process.exit(1);
});
