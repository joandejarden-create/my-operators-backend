#!/usr/bin/env node
/**
 * Brand Explorer Design Hotels + SLH Asset Pack / Draft Validation v35D.
 *
 *   npm run brand-explorer-design-slh-asset-pack-draft-v35D
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  V35D_VERSION,
  buildBrandMarkdown,
  buildDesignSlhAssetPackDraftV35DReport,
  buildV35DMarkdown,
} from "../lib/partner-intelligence/brand-explorer-design-slh-asset-pack-draft-v35D.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

async function main() {
  const report = await buildDesignSlhAssetPackDraftV35DReport();
  report.markdown = buildV35DMarkdown(report);

  const docPath = join(ROOT, "docs/data-intelligence/brand-explorer-design-slh-asset-pack-draft-v35D.md");
  mkdirSync(dirname(docPath), { recursive: true });
  writeFileSync(docPath, `${report.markdown}\n`);

  for (const brand of report.brands) {
    const jsonPath = join(
      ROOT,
      "reports",
      `brand-explorer-active-profile-factory-${brand.reportKey}-v35D.json`
    );
    const mdPath = join(
      ROOT,
      "reports",
      `brand-explorer-active-profile-factory-${brand.reportKey}-v35D.md`
    );
    const brandPayload = {
      version: V35D_VERSION,
      generatedAt: report.generatedAt,
      brand,
      recommendation: report.recommendation,
    };
    writeFileSync(jsonPath, `${JSON.stringify(brandPayload, null, 2)}\n`);
    writeFileSync(mdPath, `${buildBrandMarkdown(brand)}\n`);
    console.log(`Wrote ${jsonPath}`);
    console.log(`Wrote ${mdPath}`);
  }

  console.log(`Wrote ${docPath}`);
  console.log(`v35D validation complete`);
  console.log(`Draft apply first: ${report.recommendation.draftApplyFirst}`);
  for (const brand of report.brands) {
    console.log(
      `  ${brand.name}: readiness=${brand.readiness} sources=${brand.sourceIngestion.approvedForExplorer} assetPack=${brand.assetPackSummary.ready} preflight=${brand.founderReview.preflightPass}`
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
