#!/usr/bin/env node
/**
 * Tribute Visual Asset Slot Review & Candidate Selection v3.
 *
 *   npm run tribute-visual-asset-slot-review -- --dry-run
 *
 * Apply gated: --apply --approve-tribute-visual-slot-selection
 *
 * Does NOT download images, approve Explorer use, or write Brand Setup media fields.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildTributeVisualSlotReviewMarkdown,
  buildTributeVisualSlotReviewReport,
} from "../lib/partner-intelligence/tribute-visual-asset-slot-review.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

async function main() {
  const apply = process.argv.includes("--apply");
  const selectionApproved = process.argv.includes("--approve-tribute-visual-slot-selection");

  if (apply && !selectionApproved) {
    console.error(
      "[tribute-visual-asset-slot-review] --apply requires --approve-tribute-visual-slot-selection"
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const mode = apply && selectionApproved ? "selection-apply" : "dry-run";
  console.log(`[tribute-visual-asset-slot-review] mode=${mode}`);

  const report = await buildTributeVisualSlotReviewReport({
    apply,
    selectionApproved,
  });

  if (report.registryReadError) {
    console.error(report.registryReadError);
    process.exit(1);
  }

  const sw = report.selectionWriter;
  console.log(
    `  records=${report.totalRecordsScanned} primary=${report.recommendedPrimary.length} alternates=${report.recommendedAlternates.length} superseded=${report.recommendedSuperseded.length} proposed=${sw.proposed.length} airtable_modified=${report.airtableModified}`
  );

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildTributeVisualSlotReviewMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
