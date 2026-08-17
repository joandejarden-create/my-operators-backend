#!/usr/bin/env node
/**
 * Choice legacy Brand Explorer profiles — source package planning (dry-run default).
 *
 *   npm run choice-legacy-brand-source-package -- --dry-run
 *   npm run choice-legacy-brand-source-package -- --dry-run --brand ascend-hotel-collection
 *
 * Apply (local PDFs only, explicit approval):
 *   npm run choice-legacy-brand-source-package -- --apply --approve-choice-legacy-source-register
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildChoiceLegacySourcePackageMarkdown,
  buildChoiceLegacySourcePackageReport,
  applyLocalSourceRegistrations,
} from "../lib/partner-intelligence/choice-legacy-brand-source-package.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const GOVERNANCE_JSON = join(ROOT, "reports", "active-brand-governance-upgrade.json");

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || process.argv.includes("--plan") || !APPLY;
const APPROVED = process.argv.includes("--approve-choice-legacy-source-register");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

function loadGovernanceReport() {
  try {
    return JSON.parse(readFileSync(GOVERNANCE_JSON, "utf8"));
  } catch {
    return null;
  }
}

async function main() {
  if (APPLY && !APPROVED) {
    console.error(
      "[choice-legacy-brand-source-package] Apply requires --approve-choice-legacy-source-register (local PDFs only)."
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandFilter = argValue("--brand") || null;
  const governanceReport = loadGovernanceReport();

  console.log(
    `[choice-legacy-brand-source-package] v1 mode=${DRY_RUN ? "dry-run" : "apply"} brand=${brandFilter || "all"}`
  );

  const report = await buildChoiceLegacySourcePackageReport({
    governanceReport,
    brandFilter,
  });

  let applyResult = null;
  if (APPLY) {
    applyResult = await applyLocalSourceRegistrations(report, { brandFilter });
    report.mode = "apply";
    report.airtableModified = applyResult.applied.length > 0;
    report.applyResult = applyResult;
    console.log(
      `[choice-legacy-brand-source-package] apply applied=${applyResult.applied.length} skipped=${applyResult.skipped.length} errors=${applyResult.errors.length}`
    );
  } else {
    for (const row of report.brands) {
      const ready = row.proposedP0.filter((s) => s.registrationStatus === "ready_to_register_local").length;
      const capture = row.proposedP0.filter((s) => s.registrationStatus === "capture_needed_url").length;
      console.log(
        `  ${row.brandName}: local PDFs=${row.localFilesFound.filter((f) => f.ext === ".pdf").length} ready_register=${ready} capture_urls=${capture}`
      );
    }
  }

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify({ ...report, applyResult }, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildChoiceLegacySourcePackageMarkdown(report), "utf8");

  console.log(
    `[choice-legacy-brand-source-package] summary brands=${report.summary.totalBrands} capture_needed=${report.summary.captureNeeded}`
  );
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);

  if (applyResult?.errors?.length) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
