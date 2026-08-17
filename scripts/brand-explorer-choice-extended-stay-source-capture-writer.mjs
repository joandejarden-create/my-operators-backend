#!/usr/bin/env node
/**
 * Brand Explorer Choice Extended-Stay Source Capture Writer v32B.
 *
 *   npm run brand-explorer-choice-extended-stay-source-capture-writer -- --brands everhome-suites,woodspring-suites,suburban-studios --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_NO_PRESENTATION,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_SOURCE_ONLY,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerChoiceExtendedStaySourceCaptureWriterReport,
  v32bWriterExists,
} from "../lib/partner-intelligence/brand-explorer-choice-extended-stay-source-capture-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

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
  const brands = argValue(
    "--brands",
    "everhome-suites,woodspring-suites,suburban-studios"
  );

  const report = await buildBrandExplorerChoiceExtendedStaySourceCaptureWriterReport({
    brands,
    apply,
    approveBatch: hasFlag(APPLY_FLAG_APPROVE),
    noValidationClaim: hasFlag(APPLY_FLAG_NO_VALIDATION),
    sourceOnly: hasFlag(APPLY_FLAG_SOURCE_ONLY),
    noPresentation: hasFlag(APPLY_FLAG_NO_PRESENTATION),
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);
  writeFileSync(
    DOC_MD,
    `# Brand Explorer Choice Extended-Stay Source Capture Writer v32B\n\nSee report: \`reports/${REPORT_MD_NAME}\`\n`
  );

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v32B exists: ${v32bWriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Next writer: ${report.recommendedNextWriter}`);
  for (const b of report.brandResults) {
    console.log(
      `  ${b.displayName}: ${b.existingSourceAudit.length} existing, ${b.proposedCreates.length} creates, ${b.proposedUpdates.length} updates — ${b.sourceReadiness.band}`
    );
  }
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
