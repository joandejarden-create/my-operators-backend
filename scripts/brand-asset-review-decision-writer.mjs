#!/usr/bin/env node
/**
 * Brand Asset Review Decision Writer v5.1.
 *
 *   npm run brand-asset-review-decision-writer -- --brand tribute-portfolio --dry-run
 *
 * Decision apply gated:
 *   --apply --approve-brand-asset-review-decisions --approve-records recA,recB
 *
 * Approval-state correction apply gated:
 *   --apply --approve-brand-asset-approval-state-corrections
 *
 * Optional: --reject-records recC,recD  --keep-candidate-records recE  --allow-non-primary
 *
 * Approval is never automatic — only explicit record IDs are touched. Does not
 * download images, attach files, write Brand Setup media, or promote to Explorer.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildReviewDecisionMarkdown,
  buildReviewDecisionReport,
} from "../lib/partner-intelligence/brand-asset-review-decision-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

function parseListArg(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx >= 0 && process.argv[idx + 1] && !process.argv[idx + 1].startsWith("--")) {
    return process.argv[idx + 1]
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [];
}

function parseBrandArg() {
  const idx = process.argv.indexOf("--brand");
  if (idx >= 0 && process.argv[idx + 1]) return process.argv[idx + 1];
  return "tribute-portfolio";
}

async function main() {
  const apply = process.argv.includes("--apply");
  const decisionsApproved = process.argv.includes("--approve-brand-asset-review-decisions");
  const approvalStateCorrectionsApproved = process.argv.includes(
    "--approve-brand-asset-approval-state-corrections"
  );
  const allowNonPrimary = process.argv.includes("--allow-non-primary");
  const approveRecords = parseListArg("--approve-records");
  const rejectRecords = parseListArg("--reject-records");
  const keepCandidateRecords = parseListArg("--keep-candidate-records");

  if (apply && !decisionsApproved && !approvalStateCorrectionsApproved) {
    console.error(
      "[brand-asset-review-decision-writer] --apply requires --approve-brand-asset-review-decisions or --approve-brand-asset-approval-state-corrections"
    );
    process.exit(1);
  }

  if (apply && decisionsApproved && approvalStateCorrectionsApproved) {
    console.error(
      "[brand-asset-review-decision-writer] Pass only one apply gate per run: decisions OR approval-state corrections."
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandKey = parseBrandArg();

  const hasSelections = approveRecords.length || rejectRecords.length || keepCandidateRecords.length;
  const effectiveDecisionsApply = apply && decisionsApproved && hasSelections;
  const effectiveCorrectionsApply = apply && approvalStateCorrectionsApproved;

  if (apply && decisionsApproved && !hasSelections) {
    console.warn(
      "[brand-asset-review-decision-writer] --apply passed with no --approve-records/--reject-records/--keep-candidate-records; no decision Airtable changes will be made."
    );
  }

  const mode = effectiveCorrectionsApply
    ? "approval-state-corrections-apply"
    : effectiveDecisionsApply
      ? "decisions-apply"
      : "dry-run";
  console.log(`[brand-asset-review-decision-writer] brand=${brandKey} mode=${mode}`);

  const report = await buildReviewDecisionReport({
    brandKey,
    approveRecords,
    rejectRecords,
    keepCandidateRecords,
    allowNonPrimary,
    apply: effectiveDecisionsApply || effectiveCorrectionsApply,
    decisionsApproved: effectiveDecisionsApply,
    applyApprovalStateCorrections: effectiveCorrectionsApply,
    approvalStateCorrectionsApproved: effectiveCorrectionsApply,
  });

  if (report.registryReadError) {
    console.error(report.registryReadError);
    process.exit(1);
  }

  console.log(
    `  records=${report.totalRecordsScanned} formal_approved=${report.formalApprovedRecords?.length || 0} conflicts=${report.approvalStateConflicts?.length || 0} proposed_corrections=${report.recordsProposedForCorrection?.length || 0} approve=${report.selectedForApproval.length} reject=${report.selectedForRejection.length} keep=${report.selectedKeptAsCandidate.length} blocked=${report.blocked.length} updated=${report.updated.length} corrections_updated=${report.correctionsUpdated?.length || 0} airtable_modified=${report.airtableModified}`
  );

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildReviewDecisionMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
