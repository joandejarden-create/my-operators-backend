#!/usr/bin/env node
/**
 * Tribute Portfolio by Marriott — full Brand Intelligence Package pilot (dry-run default).
 *
 *   npm run tribute-portfolio-brand-package -- --dry-run
 *
 * Read-only planner: no Airtable writes, no source registration, no extraction,
 * no fact approval, no governance publish. Apply is intentionally not supported
 * here — use the stewardship/extraction/governance scripts once sources exist.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildTributePortfolioBrandPackageMarkdown,
  buildTributePortfolioBrandPackageReport,
} from "../lib/partner-intelligence/tribute-portfolio-brand-package.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

const SKIP_URL_PROBE = process.argv.includes("--skip-url-probe");
const APPLY_FLAGS = ["--apply", "--register", "--publish-apply"];

function main() {
  for (const flag of APPLY_FLAGS) {
    if (process.argv.includes(flag)) {
      console.error(
        `[tribute-portfolio-brand-package] ${flag} is not supported. This is a dry-run planner; use stewardship/extraction/governance scripts after sources are registered.`
      );
      process.exit(1);
    }
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  console.log(`[tribute-portfolio-brand-package] dry-run probe=${!SKIP_URL_PROBE}`);

  return buildTributePortfolioBrandPackageReport({ probeUrls: !SKIP_URL_PROBE }).then((report) => {
    console.log(
      `  completeness=${report.profileCompleteness.category} pi_sources=${report.partnerIntelligence.existingSourceCount} local_pdfs=${report.localFiles.pdfs.length}`
    );
    console.log(
      `  registerable_sources=${report.proposedSourcePackage.registerableCount} governance=${report.governanceRecommendation.recommendedPosture} ready=${report.readyForPipeline}`
    );

    mkdirSync(dirname(REPORT_JSON), { recursive: true });
    writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
    writeFileSync(REPORT_MD, buildTributePortfolioBrandPackageMarkdown(report), "utf8");
    console.log(`Wrote ${REPORT_MD}`);
    console.log(`Wrote ${REPORT_JSON}`);
  });
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
