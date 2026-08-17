#!/usr/bin/env node
/**
 * Match Score v2 — Active/Live Brand Setup score-critical gap audit (read-only).
 *
 * Usage:
 *   npm run audit-match-score-brand-setup-gaps
 *   node scripts/audit-match-score-brand-setup-gaps.mjs
 */
import "dotenv/config";
import {
  AUDIT_VERSION,
  runMatchScoreBrandSetupGapAudit,
  buildFounderWorksheet,
  writeMatchScoreBrandSetupGapReports,
} from "../lib/brand-match-score-setup-gap-audit.js";

async function main() {
  if (process.argv.includes("--apply")) {
    console.error("This audit is read-only. Do not pass --apply.");
    process.exit(2);
  }
  console.log(`[${AUDIT_VERSION}] readOnly=true Active/Live Brand Setup score-critical gaps`);

  const report = await runMatchScoreBrandSetupGapAudit({});
  const worksheet = buildFounderWorksheet(report);
  const paths = writeMatchScoreBrandSetupGapReports(report, worksheet);

  console.log(`Active/Live count: ${report.summary.activeLiveCount}`);
  console.log(`P1 complete: ${report.summary.p1CompleteCount}/${report.summary.activeLiveCount}`);
  console.log(`Avg score-critical required fill: ${report.summary.avgScoreCriticalRequiredPct}%`);
  console.log(`Founder worksheet blank required rows: ${worksheet.rowCount}`);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.listPath}`);
  console.log(`Wrote ${paths.worksheetJsonPath}`);
  console.log(`Wrote ${paths.worksheetCsvPath}`);
  console.log(`Wrote ${paths.p2Path}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
