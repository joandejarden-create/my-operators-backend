#!/usr/bin/env node
/**
 * Brand Asset Registry / Approval Workflow v2.
 *
 *   npm run brand-asset-registry-workflow -- --brand tribute-portfolio --dry-run
 *
 * Schema apply:  --apply --approve-brand-asset-registry-schema
 * Records apply: --apply --approve-brand-asset-registry-records
 *
 * Does NOT download images or overwrite Brand Setup media fields.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandAssetRegistryWorkflowMarkdown,
  buildBrandAssetRegistryWorkflowReport,
  BRAND_ASSET_PILOT_CONFIG,
} from "../lib/partner-intelligence/brand-asset-registry-workflow.js";

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
  const schemaApproved = process.argv.includes("--approve-brand-asset-registry-schema");
  const recordsApproved = process.argv.includes("--approve-brand-asset-registry-records");

  if (apply && !schemaApproved && !recordsApproved) {
    console.error(
      "[brand-asset-registry-workflow] --apply requires --approve-brand-asset-registry-schema and/or --approve-brand-asset-registry-records"
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandKey = parseBrandArg();
  const probeUrls = process.argv.includes("--probe-urls");

  const mode =
    apply && recordsApproved
      ? "records-apply"
      : apply && schemaApproved
        ? "schema-apply"
        : "dry-run";

  console.log(`[brand-asset-registry-workflow] brand=${brandKey} mode=${mode}`);

  const report = await buildBrandAssetRegistryWorkflowReport({
    brandKey,
    probeUrls,
    applySchema: apply && schemaApproved,
    schemaApproved,
    applyRecords: apply && recordsApproved,
    recordsApproved,
  });

  if (report.error) {
    console.error(report.error);
    process.exit(1);
  }

  const rw = report.recordWriter || {};
  console.log(
    `  registry_exists=${report.existingInfrastructure.registryTable.exists} existing=${rw.existingRecordsFound ?? 0} proposed=${rw.recordsProposed ?? 0} created=${rw.recordsCreated ?? 0} skipped=${rw.recordsSkippedDuplicates?.length ?? 0} airtable_modified=${report.airtableModified}`
  );

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildBrandAssetRegistryWorkflowMarkdown(report), "utf8");
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
