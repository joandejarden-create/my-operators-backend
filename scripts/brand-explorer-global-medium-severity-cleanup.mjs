#!/usr/bin/env node
/**
 * Global Active Medium semantic review + cleanup (final pre-54 freeze check).
 *
 * Usage:
 *   npm run brand-explorer-global-medium-severity-cleanup -- --dry-run
 *   npm run brand-explorer-global-medium-severity-cleanup -- --apply [flags...]
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GLOBAL_MEDIUM_SEVERITY_CLEANUP_VERSION,
  runGlobalMediumSeverityCleanup,
} from "../lib/partner-intelligence/brand-explorer-global-medium-severity-cleanup.js";

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

  if (apply && argv.includes("--dry-run")) {
    console.error("Pass either --dry-run or --apply, not both.");
    process.exit(2);
  }
  if (!fs.existsSync(REFRESH_JSON)) {
    console.error(`Missing refreshed audit: ${REFRESH_JSON}`);
    process.exit(2);
  }

  const auditReport = JSON.parse(fs.readFileSync(REFRESH_JSON, "utf8"));
  console.log(
    `[medium-cleanup] using refreshed audit ${auditReport.generatedAt} · critical=${auditReport.severityTotals?.critical} high=${auditReport.severityTotals?.high} medium=${auditReport.severityTotals?.medium}`
  );
  console.log(`[${GLOBAL_MEDIUM_SEVERITY_CLEANUP_VERSION}] ${apply ? "APPLY" : "dry-run"}`);

  const report = await runGlobalMediumSeverityCleanup({
    dryRun,
    argv: apply ? argv : [...argv, "--dry-run"],
    auditReport,
  });

  console.log(`Medium findings before: ${report.mediumFindingsBefore}`);
  console.log(`Action counts: ${JSON.stringify(report.actionCounts)}`);
  console.log(`Patches: ${report.patchCount}`);
  console.log(`Applied: ${report.applyResult?.applied ?? 0}`);
  console.log(`Ready: ${report.readyStatement}`);

  if (
    report.readyStatement?.includes("blocked") ||
    report.readyStatement?.includes("do_not_freeze") ||
    report.readyStatement?.includes("incomplete") ||
    report.applyResult?.errors?.length
  ) {
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
