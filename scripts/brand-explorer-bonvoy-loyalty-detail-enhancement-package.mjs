#!/usr/bin/env node
/**
 * Brand Explorer Bonvoy Loyalty Detail Enhancement Package v25C-2E (read-only).
 *
 *   npm run brand-explorer-bonvoy-loyalty-detail-enhancement-package -- --brand tribute-portfolio --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerBonvoyLoyaltyDetailEnhancementMarkdown,
  buildBrandExplorerBonvoyLoyaltyDetailEnhancementReport,
} from "../lib/partner-intelligence/brand-explorer-bonvoy-loyalty-detail-enhancement-package.js";

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
  if (process.argv.includes("--apply")) {
    console.error(
      "[brand-explorer-bonvoy-loyalty-detail-enhancement-package] v25C-2E is read-only. Use --dry-run (default)."
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerBonvoyLoyaltyDetailEnhancementReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
  });
  const markdown = buildBrandExplorerBonvoyLoyaltyDetailEnhancementMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v25C-2E exists: ${report.v25C2EEnhancementPackageExists ? "yes" : "no"}`);
  console.log(`Approved facts reused: ${report.existingApprovedFactsReused.length}`);
  console.log(`New facts proposed: ${report.newFactsProposed.length}`);
  console.log(`New sources needed: ${report.newSourcesNeeded.length}`);
  console.log(`KPI rows excluded: ${report.kpiRowsExcluded ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Next writer:\n${report.exactNextWriterCommand}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
