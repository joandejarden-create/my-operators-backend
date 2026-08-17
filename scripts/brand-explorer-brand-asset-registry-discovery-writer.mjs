#!/usr/bin/env node
/**
 * Brand Explorer Brand Asset Registry Discovery + Approval Queue v31B.
 *
 *   npm run brand-explorer-brand-asset-registry-discovery-writer -- --brand radisson-individuals-by-choice --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_NO_MATERIALIZE,
  APPLY_FLAG_QUEUE,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerBrandAssetRegistryDiscoveryWriterReport,
  buildMarkdown,
} from "../lib/partner-intelligence/brand-explorer-brand-asset-registry-discovery-writer.js";

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
  const dryRun = hasFlag("--dry-run") || !apply;
  const approveBatch = hasFlag(APPLY_FLAG_APPROVE);
  const createQueue = hasFlag(APPLY_FLAG_QUEUE);
  const noMaterialize = hasFlag(APPLY_FLAG_NO_MATERIALIZE);
  const brand = argValue("--brand", "radisson-individuals-by-choice");

  if (apply && (!approveBatch || !createQueue || !noMaterialize)) {
    console.error(
      `[v31B] Apply requires ${APPLY_FLAG_APPROVE}, ${APPLY_FLAG_QUEUE}, and ${APPLY_FLAG_NO_MATERIALIZE}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const report = await buildBrandExplorerBrandAssetRegistryDiscoveryWriterReport({
    brandArg: brand,
    apply: apply && !dryRun,
    approveBatch,
    createQueue,
    noMaterialize,
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");
  writeFileSync(DOC_MD, report.markdown, "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v31B exists: ${report.v31BWriterExists ? "yes" : "no"}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  console.log(`Candidates: ${report.candidateAssetsToCreate.length}`);
  console.log(`Wrong-brand risks: ${report.wrongBrandImageRisks.length}`);
  console.log(`Pending queue: ${report.imageApprovalQueue.length}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  if (report.exactApplyCommand) console.log(`Apply: ${report.exactApplyCommand}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
