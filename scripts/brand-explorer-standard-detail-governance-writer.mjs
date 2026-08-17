#!/usr/bin/env node
/**
 * Brand Explorer Standard Detail Governance Writer.
 *
 * Radisson brands (v27C):
 *   npm run brand-explorer-standard-detail-governance-writer -- --brands radisson-blu,radisson --dry-run
 *
 * WoodSpring (v33F):
 *   npm run brand-explorer-standard-detail-governance-writer -- --brand woodspring-suites --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE as RADISSON_APPLY,
  APPLY_FLAG_FOUNDER as RADISSON_FOUNDER,
  APPLY_FLAG_NO_LEGAL as RADISSON_NO_LEGAL,
  DOC_MD_NAME as RADISSON_DOC,
  REPORT_JSON_NAME as RADISSON_JSON,
  REPORT_MD_NAME as RADISSON_MD,
  buildBrandExplorerStandardDetailGovernanceWriterMarkdown,
  buildBrandExplorerStandardDetailGovernanceWriterReport,
} from "../lib/partner-intelligence/brand-explorer-standard-detail-governance-writer.js";
import {
  APPLY_FLAG_APPROVE as WOODSPRING_APPLY,
  APPLY_FLAG_FOUNDER as WOODSPRING_FOUNDER,
  APPLY_FLAG_NO_GALLERY as WOODSPRING_NO_GALLERY,
  APPLY_FLAG_NO_IMAGE_FIELDS as WOODSPRING_NO_IMAGE_FIELDS,
  APPLY_FLAG_NO_OPENINGS_MOMENTUM as WOODSPRING_NO_OPENINGS_MOMENTUM,
  APPLY_FLAG_NO_PROOF as WOODSPRING_NO_PROOF,
  APPLY_FLAG_NO_VALIDATION as WOODSPRING_NO_VALIDATION,
  APPLY_FLAG_WOODSPRING_ONLY as WOODSPRING_ONLY,
  DOC_MD_NAME as WOODSPRING_DOC,
  REPORT_JSON_NAME as WOODSPRING_JSON,
  REPORT_MD_NAME as WOODSPRING_MD,
  buildBrandExplorerWoodspringStandardDetailGovernanceWriterReport,
  v33fWriterExists,
} from "../lib/partner-intelligence/brand-explorer-woodspring-standard-detail-governance-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function hasFlag(name) {
  return process.argv.includes(name);
}

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

function isWoodspringRoute() {
  const brand = argValue("--brand", "").toLowerCase();
  const brands = argValue("--brands", "").toLowerCase();
  return brand === "woodspring-suites" || brand === "recsod51nzrpysmko" || brands.includes("woodspring-suites");
}

async function runWoodspring() {
  const apply = hasFlag("--apply");
  const report = await buildBrandExplorerWoodspringStandardDetailGovernanceWriterReport({
    brandArg: argValue("--brand", "woodspring-suites"),
    apply,
    approveBatch: hasFlag(WOODSPRING_APPLY),
    founderReviewed: hasFlag(WOODSPRING_FOUNDER),
    noValidationClaim: hasFlag(WOODSPRING_NO_VALIDATION),
    noImageFieldChanges: hasFlag(WOODSPRING_NO_IMAGE_FIELDS),
    noOpeningMomentumChanges: hasFlag(WOODSPRING_NO_OPENINGS_MOMENTUM),
    noGalleryChanges: hasFlag(WOODSPRING_NO_GALLERY),
    noProofCardChanges: hasFlag(WOODSPRING_NO_PROOF),
    woodspringOnly: hasFlag(WOODSPRING_ONLY),
  });

  const reportJson = join(ROOT, "reports", WOODSPRING_JSON);
  const reportMd = join(ROOT, "reports", WOODSPRING_MD);
  const docMd = join(ROOT, "docs", "data-intelligence", WOODSPRING_DOC);

  mkdirSync(dirname(reportJson), { recursive: true });
  mkdirSync(dirname(docMd), { recursive: true });
  writeFileSync(reportJson, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(reportMd, `${report.markdown}\n`);
  writeFileSync(docMd, `${report.markdown}\n`);

  console.log(`Wrote ${reportJson}`);
  console.log(`Wrote ${reportMd}`);
  console.log(`v33F exists: ${v33fWriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Presentation patches: ${report.presentationPatches.length}`);
  console.log(`Standards ready (current): ${report.currentStandardsApproval.ready ? "yes" : "no"}`);
  console.log(`Standards ready (projected): ${report.projectedStandardsApproval.ready ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Final QA: ${report.expectedFinalQaResult}`);
  console.log(`Complete Build: ${report.expectedCompleteBuildResult}`);
  if (report.applyBlockers.length) {
    console.log(`Apply blockers: ${report.applyBlockers.join("; ")}`);
  }
  if (report.exactApplyCommand) {
    console.log(`Apply command:\n${report.exactApplyCommand}`);
  }
  if (report.applyResults?.errors?.length) process.exit(1);
}

async function runRadisson() {
  const apply = hasFlag("--apply");
  const dryRun = hasFlag("--dry-run") || !apply;
  const approveBatch = hasFlag(RADISSON_APPLY);
  const founderReviewed = hasFlag(RADISSON_FOUNDER);
  const noLegalOrCompanyConfirmed = hasFlag(RADISSON_NO_LEGAL);

  if (apply && (!approveBatch || !founderReviewed || !noLegalOrCompanyConfirmed)) {
    console.error(
      `[brand-explorer-standard-detail-governance-writer] Apply requires ${RADISSON_APPLY}, ${RADISSON_FOUNDER}, and ${RADISSON_NO_LEGAL}`
    );
    process.exit(1);
  }

  const report = await buildBrandExplorerStandardDetailGovernanceWriterReport({
    brands: argValue("--brands", "radisson-blu,radisson"),
    apply: apply && !dryRun,
    approveBatch,
    founderReviewed,
    noLegalOrCompanyConfirmed,
  });
  const markdown = buildBrandExplorerStandardDetailGovernanceWriterMarkdown(report);

  const reportJson = join(ROOT, "reports", RADISSON_JSON);
  const reportMd = join(ROOT, "reports", RADISSON_MD);
  const docMd = join(ROOT, "docs", "data-intelligence", RADISSON_DOC);

  mkdirSync(dirname(reportJson), { recursive: true });
  mkdirSync(dirname(docMd), { recursive: true });
  writeFileSync(reportJson, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(reportMd, markdown, "utf8");
  writeFileSync(docMd, markdown, "utf8");

  console.log(`Wrote ${reportMd}`);
  console.log(`Wrote ${reportJson}`);
  console.log(`Mode: ${dryRun ? "dry-run" : report.mode}`);
  for (const plan of report.brandPlans) {
    console.log(
      `${plan.brand.name}: contract ${plan.diagnosis.contractScoreBefore} → ${plan.projectedContractScore} · create ${plan.rowsWouldCreate.length} · update ${plan.rowsWouldUpdate.length}`
    );
  }
  console.log(`Dry-run clean for apply: ${report.batchSummary.dryRunClean ? "yes" : "no"}`);
  if (!apply) console.log(`Exact apply command:\n${report.exactApplyCommand}`);
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }
  if (isWoodspringRoute()) {
    await runWoodspring();
    return;
  }
  await runRadisson();
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
