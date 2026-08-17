#!/usr/bin/env node
/**
 * Brand Explorer Scenario Display Parity Audit v31H.
 *
 *   npm run brand-explorer-scenario-display-parity-audit -- --left radisson --right radisson-individuals-by-choice --slot overview.scenario.1 --dry-run
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
  DEFAULT_SLOT,
  buildBrandExplorerScenarioDisplayParityAuditReport,
} from "../lib/partner-intelligence/brand-explorer-scenario-display-parity-audit.js";

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
  const slot = argValue("--slot", DEFAULT_SLOT);

  const report = await buildBrandExplorerScenarioDisplayParityAuditReport({
    leftArg: left,
    rightArg: right,
    slotKey: slot,
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);
  writeFileSync(
    DOC_MD,
    `# Brand Explorer Scenario Display Parity Audit v31H\n\nSee report: \`reports/${REPORT_MD_NAME}\`\n`
  );

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v31H exists: ${report.v31hAuditExists ? "yes" : "no"}`);
  console.log(`Slot: ${report.slotKey}`);
  console.log(`Left: ${report.leftBrand.name} — API image: ${report.apiComparison.leftExposesImageUrl}`);
  console.log(
    `Right: ${report.rightBrand.name} — API image: ${report.apiComparison.rightExposesImageUrl}`
  );
  console.log(
    `Display parity gap: ${report.displayParitySummary.unexplainedDisplayGap ? "yes" : "no"}`
  );
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
