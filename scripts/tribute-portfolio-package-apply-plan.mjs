#!/usr/bin/env node
/**
 * Tribute Portfolio — source-backed package apply plan (dry-run default).
 *
 *   npm run tribute-portfolio-package-apply-plan -- --dry-run
 *
 * Read-only planner. Reuses reports/tribute-portfolio-brand-package.json when
 * present (falls back to a fresh probe). No Airtable writes, no registration,
 * no extraction, no fact approval, no governance publish. Apply is not supported
 * here — the plan emits the exact apply commands to run through existing scripts.
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildTributePortfolioApplyPlanMarkdown,
  buildTributePortfolioApplyPlanReport,
} from "../lib/partner-intelligence/tribute-portfolio-package-apply-plan.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const PACKAGE_JSON = join(ROOT, "reports", "tribute-portfolio-brand-package.json");

const SKIP_URL_PROBE = process.argv.includes("--skip-url-probe");
const APPLY_FLAGS = ["--apply", "--register", "--publish-apply", "--approve"];

function loadPackageReport() {
  try {
    return JSON.parse(readFileSync(PACKAGE_JSON, "utf8"));
  } catch {
    return null;
  }
}

async function main() {
  for (const flag of APPLY_FLAGS) {
    if (process.argv.includes(flag)) {
      console.error(
        `[tribute-portfolio-package-apply-plan] ${flag} is not supported. This is a dry-run planner; run the emitted apply commands through the existing source/extraction/governance scripts.`
      );
      process.exit(1);
    }
  }
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const cached = loadPackageReport();
  console.log(
    `[tribute-portfolio-package-apply-plan] dry-run ${cached ? "using cached package report" : "building fresh (probe=" + !SKIP_URL_PROBE + ")"}`
  );

  const report = await buildTributePortfolioApplyPlanReport({
    probeUrls: !SKIP_URL_PROBE,
    packageReport: cached,
  });

  console.log(
    `  register=${report.sourceRegistrationPlan.readyToRegisterCount} (valid=${report.sourceRegistrationPlan.allValid}) facts=${report.extractionPlan.proposedFactCount} approvable=${report.extractionPlan.proposedApprovable} held=${report.extractionPlan.heldFacts.length}`
  );
  console.log(`  governance_target=${report.governanceReadinessPath.targetPosture} airtable_modified=${report.airtableModified}`);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildTributePortfolioApplyPlanMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
