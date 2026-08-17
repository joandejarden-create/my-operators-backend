#!/usr/bin/env node
/**
 * Brand Explorer WoodSpring Visual Completion v33D.
 *
 *   npm run brand-explorer-woodspring-visual-completion-writer -- --brand woodspring-suites --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_FOUNDER,
  APPLY_FLAG_NO_MOMENTUM,
  APPLY_FLAG_NO_SOURCE_LIBRARY,
  APPLY_FLAG_NO_SUMMARY_URL,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_OFFICIAL_ONLY,
  APPLY_FLAG_WOODSPRING_ONLY,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerWoodspringVisualCompletionWriterReport,
  v33dWriterExists,
} from "../lib/partner-intelligence/brand-explorer-woodspring-visual-completion-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

function hasFlag(name) {
  return process.argv.includes(name);
}

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  const apply = hasFlag("--apply");
  const brand = argValue("--brand", "woodspring-suites");

  const report = await buildBrandExplorerWoodspringVisualCompletionWriterReport({
    brandArg: brand,
    apply,
    approveBatch: hasFlag(APPLY_FLAG_APPROVE),
    founderApproved: hasFlag(APPLY_FLAG_FOUNDER),
    officialImagesOnly: hasFlag(APPLY_FLAG_OFFICIAL_ONLY),
    noValidationClaim: hasFlag(APPLY_FLAG_NO_VALIDATION),
    noSourceLibrary: hasFlag(APPLY_FLAG_NO_SOURCE_LIBRARY),
    noSummaryUrl: hasFlag(APPLY_FLAG_NO_SUMMARY_URL),
    noMomentumChanges: hasFlag(APPLY_FLAG_NO_MOMENTUM),
    woodspringOnly: hasFlag(APPLY_FLAG_WOODSPRING_ONLY),
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(REPORT_MD, `${report.markdown}\n`);

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`v33D exists: ${v33dWriterExists() ? "yes" : "no"}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Dry-run clean: ${report.dryRunClean ? "yes" : "no"}`);
  console.log(`Airtable modified: ${report.airtableModified ? "yes" : "no"}`);
  console.log(`Image pool: ${report.imagePoolSize} (OG: ${report.officialOgImagesDiscovered})`);
  console.log(`Opening plans: ${report.openingImagePlan.filter((p) => !p.blocked).length}/${report.openingImagePlan.length}`);
  console.log(`Scenario plans: ${report.scenarioImagePlan.filter((p) => !p.blocked).length}/${report.scenarioImagePlan.length}`);
  console.log(`Gallery plans: ${report.galleryImagePlan.filter((p) => !p.blocked).length}/${report.galleryImagePlan.length}`);
  console.log(`Registry patches/creates: ${report.registryPatches.length}/${report.registryCreates.length}`);
  console.log(`Gallery visible/hidden: ${report.visibleGallerySlotsProposed}/${report.hiddenGallerySlots}`);
  console.log(`Distinct images: ${report.distinctImageCount}`);
  console.log(`Gallery premium enough: ${report.galleryPremiumEnoughForActiveProfile ? "yes" : "no"}`);
  console.log(`Repeated image uses: ${report.repeatedImageUses?.length ?? 0}`);
  console.log(`Image writes proposed: ${report.imageFieldWritesProposed.length}`);
  console.log(`Images skipped: ${report.imagesSkipped.length}`);
  console.log(`Summary URL protected: ${report.summaryUrlProtectionConfirmed ? "yes" : "no"}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  console.log(`Final QA: ${report.expectedFinalQaResult}`);
  console.log(`Complete Build: ${report.expectedCompleteBuildResult}`);
  console.log(`Next writer: ${report.recommendedNextWriter}`);
  if (report.applyBlockers.length) {
    console.log(`Apply blockers: ${report.applyBlockers.join("; ")}`);
  }
  if (report.exactApplyCommand) {
    console.log(`Apply command:\n${report.exactApplyCommand}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
