#!/usr/bin/env node
/**
 * Brand Explorer Everhome Openings Description + Momentum Parity v32E.
 *
 *   npm run brand-explorer-everhome-openings-momentum-rebuild-writer -- --brand everhome-suites --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_EVERHOME_ONLY,
  APPLY_FLAG_NO_APPROVALS,
  APPLY_FLAG_NO_IMAGE_FIELDS,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_NO_VISIBILITY,
  APPLY_FLAG_PRESERVE_LABELS,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerEverhomeOpeningsMomentumRebuildWriterReport,
  v32eWriterExists,
} from "../lib/partner-intelligence/brand-explorer-everhome-openings-momentum-rebuild-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

function hasFlag(name) {
  return process.argv.includes(name);
}

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  const apply = hasFlag("--apply");
  const brand = argValue("--brand", "everhome-suites");

  const report = await buildBrandExplorerEverhomeOpeningsMomentumRebuildWriterReport({
    brandArg: brand,
    apply,
    approveBatch: hasFlag(APPLY_FLAG_APPROVE),
    noValidationClaim: hasFlag(APPLY_FLAG_NO_VALIDATION),
    noImageFields: hasFlag(APPLY_FLAG_NO_IMAGE_FIELDS),
    noApprovals: hasFlag(APPLY_FLAG_NO_APPROVALS),
    noVisibility: hasFlag(APPLY_FLAG_NO_VISIBILITY),
    preserveLabels: hasFlag(APPLY_FLAG_PRESERVE_LABELS),
    everhomeOnly: hasFlag(APPLY_FLAG_EVERHOME_ONLY),
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`v32E exists: ${v32eWriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Openings rows audited: ${report.openingsAudit.length}`);
  console.log(`Momentum rows audited: ${report.momentumAudit.length}`);
  console.log(`Rows proposed: ${report.rowsUpdated.length}`);
  console.log(`Next writer: ${report.recommendedNextWriter}`);
  if (report.applyBlockers.length) {
    console.log(`Apply blockers: ${report.applyBlockers.join("; ")}`);
  }
  if (report.exactApplyCommand) {
    console.log(`Apply command:\n${report.exactApplyCommand}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
