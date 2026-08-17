#!/usr/bin/env node
/**
 * Tribute Portfolio — targeted source-backed extraction (dry-run default).
 *
 *   npm run tribute-portfolio-targeted-extract -- --dry-run
 *   npm run tribute-portfolio-targeted-extract -- --apply
 *
 * Dry-run: diagnoses source text + existing facts, proposes clean facts, writes
 * nothing. Apply: creates NEW Pending facts only (never approves, never publishes,
 * never touches Brand Setup / hero / logo / Company Validated). Reruns are
 * de-duplicated by extractionRunId tag.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildTributeTargetedExtractReport,
  buildTributeTargetedExtractMarkdown,
} from "../lib/partner-intelligence/tribute-portfolio-targeted-extract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

const APPLY = process.argv.includes("--apply");

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const mode = APPLY ? "apply" : "dry-run";
  console.log(`[tribute-portfolio-targeted-extract] mode=${mode}`);

  const report = await buildTributeTargetedExtractReport({ mode });

  console.log(
    `  sources=${report.sourceInventory.length} existing_gaps=${report.existingFactAudit.dataGapCount} proposed=${report.proposedCount} approvable=${report.approvableCount} human_review=${report.humanReviewCount}`
  );
  console.log(
    `  v23_candidates=${report.v23CandidateCount ?? 0} v23B_package_ready=${report.v23EvidenceReadiness?.v23BReviewPackageCanBeBuilt ? "yes" : "no"}`
  );
  console.log(
    `  governance_after_approval=${report.governanceProjection.wouldEnableGovernanceAfterApproval} airtable_modified=${report.airtableModified}`
  );
  if (report.applyResult) {
    console.log(
      `  applied: created=${report.applyResult.created.length} errors=${report.applyResult.errors.length} run=${report.applyResult.runId}`
    );
  }

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildTributeTargetedExtractMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Next: ${report.nextCommand}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
