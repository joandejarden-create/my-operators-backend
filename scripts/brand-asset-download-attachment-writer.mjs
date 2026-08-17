#!/usr/bin/env node
/**
 * Brand Asset Download & Attachment Writer v6.
 *
 * Dry-run:
 *   npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio --dry-run
 *
 * Apply (gated):
 *   npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio --apply --approve-brand-asset-download-attachments
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandAssetDownloadAttachmentWriterMarkdown,
  buildBrandAssetDownloadAttachmentWriterReport,
} from "../lib/partner-intelligence/brand-asset-download-attachment-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

function parseBrandArg() {
  const idx = process.argv.indexOf("--brand");
  if (idx >= 0 && process.argv[idx + 1]) return process.argv[idx + 1];
  return "tribute-portfolio";
}

function parseAssetRecordIdArg() {
  const idx = process.argv.indexOf("--asset-record-id");
  if (idx >= 0 && process.argv[idx + 1]) return process.argv[idx + 1];
  return "";
}

async function main() {
  const apply = process.argv.includes("--apply");
  const applyApproved = process.argv.includes("--approve-brand-asset-download-attachments");
  const repairMissingAttachments = process.argv.includes("--repair-missing-attachments");
  const repairApproved = process.argv.includes("--approve-brand-asset-attachment-materialization-repair");
  const singleAssetMaterializationApproved = process.argv.includes(
    "--approve-brand-asset-single-attachment-materialization"
  );
  const assetRecordId = parseAssetRecordIdArg();
  if (apply && !applyApproved) {
    console.error(
      "[brand-asset-download-attachment-writer] --apply requires --approve-brand-asset-download-attachments"
    );
    process.exit(1);
  }
  if (repairMissingAttachments && apply && !repairApproved) {
    console.error(
      "[brand-asset-download-attachment-writer] --repair-missing-attachments with --apply requires --approve-brand-asset-attachment-materialization-repair"
    );
    process.exit(1);
  }
  if (apply && repairMissingAttachments && assetRecordId && !singleAssetMaterializationApproved) {
    console.error(
      "[brand-asset-download-attachment-writer] single-asset materialization apply requires --approve-brand-asset-single-attachment-materialization"
    );
    process.exit(1);
  }
  const mode = apply && applyApproved ? "download-attachment-apply" : "dry-run";
  const brandKey = parseBrandArg();
  console.log(
    `[brand-asset-download-attachment-writer] brand=${brandKey} mode=${mode} asset_record_id=${assetRecordId || "all"}`
  );

  const report = await buildBrandAssetDownloadAttachmentWriterReport({
    brandKey,
    apply,
    applyApproved,
    repairMissingAttachments,
    repairApproved,
    assetRecordId,
    singleAssetMaterializationApproved,
  });
  if (report.registryReadError) {
    console.error(report.registryReadError);
    process.exit(1);
  }
  console.log(
    `  records=${report.totalRecordsScanned} approved=${report.formallyApprovedRecordsFound.length} eligible=${report.recordsEligibleForDownload.length} excluded=${report.recordsExcluded.length} valid=${report.downloadValidation.dryRunValidation.passed} downloaded=${report.applyResult.filesDownloaded.length} updated=${report.applyResult.recordsUpdated.length} airtable_modified=${report.airtableModified}`
  );

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildBrandAssetDownloadAttachmentWriterMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
