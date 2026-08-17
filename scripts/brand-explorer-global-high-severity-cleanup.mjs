#!/usr/bin/env node
/**
 * Global Active High-severity semantic cleanup — Batch 1.
 *
 * Usage:
 *   npm run brand-explorer-global-high-severity-cleanup -- --batch 1 --dry-run
 *   npm run brand-explorer-global-high-severity-cleanup -- --batch 1 --apply [flags...]
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GLOBAL_HIGH_SEVERITY_CLEANUP_VERSION,
  runGlobalHighSeverityCleanup,
} from "../lib/partner-intelligence/brand-explorer-global-high-severity-cleanup.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REFRESH_JSON = path.join(
  ROOT,
  "reports",
  "brand-explorer-global-active-semantic-audit-refresh.json"
);

function parseBatch(argv) {
  const idx = argv.indexOf("--batch");
  if (idx >= 0 && argv[idx + 1]) return Number(argv[idx + 1]);
  const flag = argv.find((a) => /^--batch=\d+$/.test(a));
  if (flag) return Number(flag.split("=")[1]);
  return 1;
}

async function main() {
  const argv = process.argv.slice(2);
  const apply = argv.includes("--apply");
  const dryRun = !apply;
  const batch = parseBatch(argv);

  if (batch !== 1) {
    console.error(`Only --batch 1 is implemented (got ${batch}).`);
    process.exit(2);
  }
  if (apply && argv.includes("--dry-run")) {
    console.error("Pass either --dry-run or --apply, not both.");
    process.exit(2);
  }
  if (!fs.existsSync(REFRESH_JSON)) {
    console.error(`Missing refreshed audit: ${REFRESH_JSON}`);
    console.error("Run: npm run brand-explorer-global-active-semantic-audit -- --dry-run --fresh");
    process.exit(2);
  }

  const auditReport = JSON.parse(fs.readFileSync(REFRESH_JSON, "utf8"));
  console.log(
    `[high-cleanup] using refreshed audit ${auditReport.generatedAt} · critical=${auditReport.severityTotals?.critical} high=${auditReport.severityTotals?.high}`
  );
  console.log(`[${GLOBAL_HIGH_SEVERITY_CLEANUP_VERSION}] batch=${batch} ${apply ? "APPLY" : "dry-run"}`);

  const report = await runGlobalHighSeverityCleanup({
    dryRun,
    batch,
    argv: apply ? argv : [...argv, "--dry-run"],
    auditReport,
  });

  console.log(`High findings before: ${report.highFindingsBefore}`);
  console.log(`Batch 1 findings: ${report.batch1FindingCount}`);
  console.log(`Deferred: ${report.deferredFindingCount}`);
  console.log(`Patches: ${report.patchCount}`);
  console.log(`Applied: ${report.applyResult?.applied ?? 0}`);
  console.log(`Brands patched: ${(report.brandsPatched || []).length}`);
  console.log(`Ready: ${report.readyStatement}`);

  if (report.readyStatement?.includes("blocked") || report.applyResult?.errors?.length) {
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
