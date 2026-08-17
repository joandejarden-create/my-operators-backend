#!/usr/bin/env node
/**
 * Brand Explorer Visual Slot Requirements v2.
 *
 *   npm run brand-explorer-visual-slot-requirements -- --brand tribute-portfolio --dry-run
 *
 * Schema apply:  --apply --approve-brand-visual-slot-schema
 * Status writer: --apply --approve-brand-visual-slot-status
 *
 * Does NOT download images, overwrite Brand Setup media fields, or approve Explorer use.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildVisualSlotRequirementsMarkdown,
  buildVisualSlotRequirementsReport,
  BRAND_ASSET_PILOT_CONFIG,
} from "../lib/partner-intelligence/brand-explorer-visual-slot-requirements.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

function parseBrandArg() {
  const idx = process.argv.indexOf("--brand");
  if (idx >= 0 && process.argv[idx + 1]) return process.argv[idx + 1];
  return "tribute-portfolio";
}

async function main() {
  const apply = process.argv.includes("--apply");
  const schemaApproved = process.argv.includes("--approve-brand-visual-slot-schema");
  const statusApproved = process.argv.includes("--approve-brand-visual-slot-status");

  if (apply && !schemaApproved && !statusApproved) {
    console.error(
      "[brand-explorer-visual-slot-requirements] --apply requires --approve-brand-visual-slot-schema or --approve-brand-visual-slot-status."
    );
    process.exit(1);
  }
  if (apply && schemaApproved && statusApproved) {
    console.error(
      "[brand-explorer-visual-slot-requirements] Use one apply gate at a time (schema OR status, not both)."
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandKey = parseBrandArg();
  let mode = "dry-run";
  if (apply && schemaApproved) mode = "schema-apply";
  if (apply && statusApproved) mode = "status-apply";
  console.log(`[brand-explorer-visual-slot-requirements] brand=${brandKey} mode=${mode}`);

  const report = await buildVisualSlotRequirementsReport({
    brandKey,
    applySchema: apply && schemaApproved,
    schemaApproved,
    applyStatus: apply && statusApproved,
    statusApproved,
  });

  if (report.error) {
    console.error(report.error);
    process.exit(1);
  }

  const missing = report.missingSlots.length;
  const invalid = report.invalidAssets.length;
  const sw = report.statusWriter;
  const statusLine = sw
    ? ` proposed=${sw.recordsProposed} updated=${sw.recordsUpdated} skipped=${sw.recordsSkipped}`
    : "";
  console.log(
    `  records=${report.registryRecordCount} invalid=${invalid} missing_slots=${missing} schema_sufficient=${report.registrySchema.sufficientForSlotGovernance} airtable_modified=${report.airtableModified}${statusLine}`
  );

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildVisualSlotRequirementsMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);

  if (!BRAND_ASSET_PILOT_CONFIG[brandKey]) {
    console.log(`Known pilots: ${Object.keys(BRAND_ASSET_PILOT_CONFIG).join(", ")}`);
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
