#!/usr/bin/env node
/**
 * Tribute Portfolio Package Pipeline v1 (dry-run default).
 *
 *   npm run tribute-portfolio-package-pipeline -- --dry-run
 *   npm run tribute-portfolio-package-pipeline -- --apply --approve-tribute-portfolio-package-pipeline
 *
 * One command orchestrates: source registration → source stewardship → extraction
 * → fact stewardship → governance publish → verification, reusing the existing
 * Dealality Intelligence Factory primitives. Apply runs only safe pending stages,
 * skips duplicates, and halts on blockers. See tribute-portfolio-package-pipeline-v1.md.
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildTributePortfolioPipelineMarkdown,
  runTributePortfolioPackagePipeline,
} from "../lib/partner-intelligence/tribute-portfolio-package-pipeline.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const APPLY_PLAN_JSON = join(ROOT, "reports", "tribute-portfolio-package-apply-plan.json");

const APPLY = process.argv.includes("--apply");
const APPROVED = process.argv.includes(APPLY_FLAG);
const SKIP_URL_PROBE = process.argv.includes("--skip-url-probe");

function loadApplyPlan() {
  try {
    return JSON.parse(readFileSync(APPLY_PLAN_JSON, "utf8"));
  } catch {
    return null;
  }
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }
  if (APPLY && !APPROVED) {
    console.error(`Refusing to apply without ${APPLY_FLAG}. Re-run with the approval flag.`);
    process.exit(1);
  }

  const mode = APPLY ? "apply" : "dry-run";
  const cachedPlan = loadApplyPlan();
  console.log(
    `[tribute-portfolio-package-pipeline] mode=${mode} ${cachedPlan && !APPLY ? "(cached apply-plan)" : "(fresh apply-plan probe=" + !SKIP_URL_PROBE + ")"}`
  );

  const report = await runTributePortfolioPackagePipeline({
    mode,
    probeUrls: !SKIP_URL_PROBE,
    applyPlanReport: !APPLY ? cachedPlan : null,
  });

  const es = report.executiveSummary;
  console.log(
    `  stage=${es.currentStage} sources=${es.liveSources} facts=${es.liveFacts} approved=${es.approvedFacts} governed_ready=${es.governedPlatformReady} airtable_modified=${report.airtableModified}`
  );
  if (report.halted) console.log(`  HALTED: ${report.haltReason}`);

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildTributePortfolioPipelineMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
