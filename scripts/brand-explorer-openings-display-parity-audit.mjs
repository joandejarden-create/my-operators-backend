#!/usr/bin/env node
/**
 * Brand Explorer Openings Display Parity Audit v31K.
 *
 *   npm run brand-explorer-openings-display-parity-audit -- --left radisson --right radisson-individuals-by-choice --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  DEFAULT_LEFT,
  DEFAULT_RIGHT,
  buildBrandExplorerOpeningsDisplayParityAuditReport,
} from "../lib/partner-intelligence/brand-explorer-openings-display-parity-audit.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  const left = argValue("--left", DEFAULT_LEFT);
  const right = argValue("--right", DEFAULT_RIGHT);

  const report = await buildBrandExplorerOpeningsDisplayParityAuditReport({
    leftArg: left,
    rightArg: right,
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);
  writeFileSync(
    DOC_MD,
    `# Brand Explorer Openings Display Parity Audit v31K\n\nSee report: \`reports/${REPORT_MD_NAME}\`\n`
  );

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v31K exists: ${report.v31kAuditExists ? "yes" : "no"}`);
  console.log(`Section: ${report.sectionTitle} (${report.slotKey})`);
  console.log(
    `Left: ${report.leftBrand.name} — API openings: ${report.apiComparison.leftInBlocks}, images: ${report.airtableRowComparison.left.counts.withImageInApi}`
  );
  console.log(
    `Right: ${report.rightBrand.name} — API openings: ${report.apiComparison.rightInBlocks}, images: ${report.airtableRowComparison.right.counts.withImageInApi}`
  );
  console.log(
    `Display parity gap: ${report.frontendRenderingComparison.parityGap ? "yes" : "no"}`
  );
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
