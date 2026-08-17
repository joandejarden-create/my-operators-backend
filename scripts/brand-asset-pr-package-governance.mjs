#!/usr/bin/env node
/**
 * Brand Asset & PR Package Governance v1 (dry-run only).
 *
 *   npm run brand-asset-pr-package-governance -- --brand tribute-portfolio --dry-run
 *
 * Read-only: inspects logo/hero/image/PR status, local assets, and official
 * Marriott-controlled candidates. Does not download images or write Airtable.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandAssetPrPackageGovernanceMarkdown,
  buildBrandAssetPrPackageGovernanceReport,
  BRAND_ASSET_PILOT_CONFIG,
} from "../lib/partner-intelligence/brand-asset-pr-package-governance.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

function parseBrandArg() {
  const idx = process.argv.indexOf("--brand");
  if (idx >= 0 && process.argv[idx + 1]) return process.argv[idx + 1];
  return "tribute-portfolio";
}

async function main() {
  if (process.argv.includes("--apply")) {
    console.error(
      "[brand-asset-pr-package-governance] --apply is not supported in v1. This module is read-only."
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandKey = parseBrandArg();
  const probeUrls = !process.argv.includes("--skip-url-probe");

  console.log(`[brand-asset-pr-package-governance] brand=${brandKey} dry-run probe=${probeUrls}`);

  const report = await buildBrandAssetPrPackageGovernanceReport({ brandKey, probeUrls });

  if (report.error) {
    console.error(report.error);
    process.exit(1);
  }

  console.log(
    `  text_governance_ready=${report.governedProfileStatus.textGovernancePlatformReady} logo=${report.currentStatus.logo.status} hero=${report.currentStatus.hero.status}`
  );
  console.log(
    `  local_images=${report.localAssets.images.length} image_candidates=${report.officialCandidates.heroProperty.length + report.officialCandidates.imageLogo.length} airtable_modified=${report.airtableModified}`
  );

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildBrandAssetPrPackageGovernanceMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);

  const pilots = Object.keys(BRAND_ASSET_PILOT_CONFIG).join(", ");
  if (!BRAND_ASSET_PILOT_CONFIG[brandKey]) {
    console.log(`Known pilots: ${pilots}`);
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
