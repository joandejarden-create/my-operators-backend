#!/usr/bin/env node
/**
 * Brand Explorer — Factory Preview Mode (read-only audit + plan).
 *
 *   npm run brand-explorer-factory-preview-mode -- --dry-run
 *
 * No Airtable writes. No Brand Status / CV / Source / Registry changes.
 */
import "dotenv/config";
import {
  AUDIT_JSON,
  AUDIT_MD,
  STATUS_CORRECTION_MD,
  runFactoryPreviewModeAudit,
} from "../lib/partner-intelligence/brand-explorer-factory-preview-mode.js";
import { FACTORY_PREVIEW_VERSION } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = !argv.includes("--apply"); // apply is intentionally unsupported for writes
  if (argv.includes("--apply")) {
    console.error("[refuse] Factory Preview Mode never applies Airtable writes. Use --dry-run only.");
    process.exit(2);
  }

  console.log(`[${FACTORY_PREVIEW_VERSION}] factory preview mode audit (dry-run=${dryRun})`);
  const report = await runFactoryPreviewModeAudit({ dryRun: true, writeReports: true });

  console.log(`Active universe: ${report.activeUniverseCount} (expected ${report.expectedProtectedBaselineCount})`);
  console.log(`Drift: ${report.activeUniverseDrift ? "YES" : "no"} → ${(report.activeDriftSlugs || []).join(", ") || "—"}`);
  console.log(`Recommendation: ${report.recommendation}`);
  console.log(`Wrote reports/${AUDIT_JSON}`);
  console.log(`Wrote reports/${AUDIT_MD}`);
  console.log(`Wrote reports/${STATUS_CORRECTION_MD}`);

  for (const c of report.candidates || []) {
    console.log(
      `  ${c.slug}: status=${c.currentBrandStatus} publicFull=${c.shouldRenderFullProfilePublic} factoryPreview=${c.canRenderFactoryPreview} inActive=${c.accidentallyInActiveUniverse} baselineFail=${c.protectedBaselineFailsBecauseOfIt}`
    );
  }

  // Audit always exits 0 — status drift is reported, not treated as a script failure.
  // Use test:brand-explorer-factory-preview-mode for acceptance gates.
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
