#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_GALLERY_ROW_CREATE,
  APPLY_FLAG_GALLERY_IMAGE_REPAIR,
  APPLY_FLAG_FOUNDER_PROVISIONAL,
  APPLY_FLAG_PRESENTATION_IMAGE_CONTENT_UPLOAD,
  APPLY_FLAG_PROVISIONAL,
  APPLY_FLAG_STRICT,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerVisualMinimumsBackfillWriterMarkdown,
  buildBrandExplorerVisualMinimumsBackfillWriterReport,
} from "../lib/partner-intelligence/brand-explorer-visual-minimums-backfill-writer.js";

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
  const plan = argValue("--plan", "strict");
  const apply = hasFlag("--apply");

  const report = await buildBrandExplorerVisualMinimumsBackfillWriterReport({
    brandIdOrName: argValue("--brand", "tribute-portfolio"),
    plan,
    apply,
    applyApprovedStrict: hasFlag(APPLY_FLAG_STRICT),
    applyApprovedProvisional: hasFlag(APPLY_FLAG_PROVISIONAL),
    founderApprovedProvisional: hasFlag(APPLY_FLAG_FOUNDER_PROVISIONAL),
    galleryRowCreateApproved: hasFlag(APPLY_FLAG_GALLERY_ROW_CREATE),
    galleryImageRepairApproved: hasFlag(APPLY_FLAG_GALLERY_IMAGE_REPAIR),
    presentationImageContentUploadApproved: hasFlag(APPLY_FLAG_PRESENTATION_IMAGE_CONTENT_UPLOAD),
    allowExternalImageUrlFallback: hasFlag("--allow-brand-explorer-v25B-external-image-url-fallback"),
  });
  const markdown = buildBrandExplorerVisualMinimumsBackfillWriterMarkdown(report);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Plan: ${report.plan}`);
  console.log(`Would update: ${report.wouldUpdateCount}`);
  console.log(`Would create: ${report.wouldCreateCount}`);
  console.log(`Missing rows: ${report.missingTargetRows.length}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
