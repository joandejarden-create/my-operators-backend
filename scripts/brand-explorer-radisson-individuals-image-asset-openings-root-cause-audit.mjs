#!/usr/bin/env node
/**
 * Brand Explorer Radisson Individuals Image / Asset / Openings Root-Cause Audit v31I.
 *
 *   npm run brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit -- --brand radisson-individuals-by-choice --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  AUDIT_VERSION,
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerRadissonIndividualsImageAssetOpeningsRootCauseAuditReport,
} from "../lib/partner-intelligence/brand-explorer-radisson-individuals-image-asset-openings-root-cause-audit.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  const dryRun = process.argv.includes("--dry-run") || !process.argv.includes("--apply");
  const brand = argValue("--brand", "radisson-individuals-by-choice");

  if (process.argv.includes("--apply")) {
    console.error("[v31I] Audit is read-only — do not pass --apply");
    process.exit(1);
  }

  const report =
    await buildBrandExplorerRadissonIndividualsImageAssetOpeningsRootCauseAuditReport({
      brandArg: brand,
      dryRun,
    });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);
  writeFileSync(
    DOC_MD,
    `# Brand Explorer Radisson Individuals Image / Asset / Openings Root-Cause Audit v${AUDIT_VERSION}\n\nRead-only audit. See \`reports/${REPORT_MD_NAME}\`.\n`
  );

  const s = report.imageRestorationSummary || {};
  const u = report.sourceUrlExpirationSummary || {};
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`v31I exists: ${report.v31iAuditExists ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Visual rows audited: ${s.totalVisualRowsAudited ?? 0}`);
  console.log(`Cleared by v31D: ${s.clearedByV31D ?? 0} · Restored v31D-R1: ${s.restoredByV31DR1 ?? 0}`);
  console.log(`Registry rows: ${report.liveCounts?.registryRows ?? 0}`);
  console.log(`Temp attachment URLs: ${u.temporary_attachment_url ?? 0}`);
  console.log(`Duplicate groups: ${report.duplicateRegistryAudit?.duplicateGroupCount ?? 0}`);
  console.log(`Openings rows: ${report.openingsRootCauseAudit?.length ?? 0}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
