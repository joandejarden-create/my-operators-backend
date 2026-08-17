#!/usr/bin/env node
/**
 * Global Active Critical semantic blocker cleanup.
 *
 * Usage:
 *   npm run brand-explorer-global-critical-blocker-cleanup -- --dry-run
 *   npm run brand-explorer-global-critical-blocker-cleanup -- --apply [flags...]
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GLOBAL_CRITICAL_BLOCKER_CLEANUP_VERSION,
  runGlobalCriticalBlockerCleanup,
} from "../lib/partner-intelligence/brand-explorer-global-critical-blocker-cleanup.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REFRESH_JSON = path.join(
  ROOT,
  "reports",
  "brand-explorer-global-active-semantic-audit-refresh.json"
);

async function main() {
  const argv = process.argv.slice(2);
  const apply = argv.includes("--apply");
  const dryRun = !apply;

  if (!dryRun && !apply) {
    console.error("Require --dry-run or --apply");
    process.exit(2);
  }
  if (apply && argv.includes("--dry-run")) {
    console.error("Pass either --dry-run or --apply, not both.");
    process.exit(2);
  }

  let auditReport = null;
  if (fs.existsSync(REFRESH_JSON)) {
    auditReport = JSON.parse(fs.readFileSync(REFRESH_JSON, "utf8"));
    console.log(`[critical-cleanup] using refreshed audit ${auditReport.generatedAt}`);
  } else {
    console.log("[critical-cleanup] no refresh audit found — will run fresh audit inside cleanup");
  }

  console.log(`[${GLOBAL_CRITICAL_BLOCKER_CLEANUP_VERSION}] ${apply ? "APPLY" : "dry-run"}`);
  const report = await runGlobalCriticalBlockerCleanup({
    dryRun,
    argv: apply ? argv : [...argv, "--dry-run"],
    auditReport,
  });

  console.log(`Critical findings: ${report.criticalFindingCount}`);
  console.log(`Patches: ${report.patchCount}`);
  console.log(`Applied: ${report.applyResult?.applied ?? 0}`);
  console.log(`Ready: ${report.readyStatement}`);

  if (report.readyStatement?.includes("blocked") || report.applyResult?.errors?.length) {
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
