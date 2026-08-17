#!/usr/bin/env node
/**
 * Gated overlay → Airtable apply preview (v1: no live Airtable writes).
 *
 *   npm run operator-explorer-overlay-airtable-apply -- --dry-run
 *   npm run operator-explorer-overlay-airtable-apply -- --operators arbor-lodging-cala --dry-run
 */
import {
  runOperatorExplorerOverlayAirtableApply,
  writeOperatorExplorerOverlayAirtableApplyReports,
  OPERATOR_OVERLAY_AIRTABLE_APPLY_VERSION,
} from "../lib/partner-intelligence/operator-explorer-overlay-airtable-apply.js";

function parseArgs(argv) {
  const out = {
    operators: null,
    apply: false,
    approveOverlayAirtableApply: false,
    confirmAirtableWrite: false,
    confirmNoCompanyValidationChanges: false,
    confirmNoSourceLibraryStatusChanges: false,
    confirmNoRegistryApprovalChanges: false,
    confirmFixtureOverlayReviewed: false,
    confirmBaselineRevision: false,
    confirmFixtureOverlayOnly: true,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--operators" && argv[i + 1]) {
      out.operators = argv[++i].split(",").map((s) => s.trim()).filter(Boolean);
    } else if (a.startsWith("--operators=")) {
      out.operators = a
        .slice("--operators=".length)
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    } else if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-operator-overlay-airtable-apply") {
      out.approveOverlayAirtableApply = true;
    } else if (a === "--confirm-airtable-write") out.confirmAirtableWrite = true;
    else if (a === "--confirm-no-company-validation-changes") {
      out.confirmNoCompanyValidationChanges = true;
    } else if (a === "--confirm-no-source-library-status-changes") {
      out.confirmNoSourceLibraryStatusChanges = true;
    } else if (a === "--confirm-no-registry-approval-changes") {
      out.confirmNoRegistryApprovalChanges = true;
    } else if (a === "--confirm-fixture-overlay-reviewed") {
      out.confirmFixtureOverlayReviewed = true;
    } else if (a === "--confirm-baseline-revision") out.confirmBaselineRevision = true;
    else if (a === "--confirm-fixture-overlay-only") out.confirmFixtureOverlayOnly = true;
  }
  return out;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  console.log(`[${OPERATOR_OVERLAY_AIRTABLE_APPLY_VERSION}] dryRun=${!args.apply}`);
  const report = runOperatorExplorerOverlayAirtableApply({
    operators: args.operators || undefined,
    apply: args.apply,
    approveOverlayAirtableApply: args.approveOverlayAirtableApply,
    confirmAirtableWrite: args.confirmAirtableWrite,
    confirmNoCompanyValidationChanges: args.confirmNoCompanyValidationChanges,
    confirmNoSourceLibraryStatusChanges: args.confirmNoSourceLibraryStatusChanges,
    confirmNoRegistryApprovalChanges: args.confirmNoRegistryApprovalChanges,
    confirmFixtureOverlayReviewed: args.confirmFixtureOverlayReviewed,
    confirmBaselineRevision: args.confirmBaselineRevision,
    confirmFixtureOverlayOnly: args.confirmFixtureOverlayOnly,
  });
  const paths = writeOperatorExplorerOverlayAirtableApplyReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `overlaysFound=${report.summary.overlaysFound} validationPass=${report.summary.validationPass} airtableWrites=${report.airtableWrites}`
  );
}

try {
  main();
} catch (err) {
  console.error(err?.message || err);
  process.exit(1);
}
