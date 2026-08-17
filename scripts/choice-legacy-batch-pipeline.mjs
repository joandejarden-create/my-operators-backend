#!/usr/bin/env node
/**
 * Choice legacy batch pipeline orchestrator v1.
 *
 *   npm run choice-legacy-batch-pipeline -- --batch mini-batch-2 --dry-run
 *   npm run choice-legacy-batch-pipeline -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-pipeline
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { PARTNER_INTELLIGENCE_LINKS } from "../api/lib/partner-intelligence-field-map.js";
import { cellToString } from "../lib/airtable-utils.js";
import {
  APPLY_FLAG,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildChoiceLegacyBatchPipelineMarkdown,
  parseBatchNameFromArgv,
  runChoiceLegacyBatchPipeline,
} from "../lib/partner-intelligence/choice-legacy-batch-pipeline.js";
import { BRAND_TABLE } from "../lib/partner-intelligence/profile-governance-publish.js";
import { getBatchBrandConfigs } from "../lib/partner-intelligence/choice-legacy-batch-config.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const GOVERNANCE_JSON = join(ROOT, "reports", "active-brand-governance-upgrade.json");
const READINESS_JSON = join(ROOT, "reports", "partner-intelligence-publish-readiness.json");

const batchName = parseBatchNameFromArgv();
const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || process.argv.includes("--plan") || !APPLY;
const APPROVED = process.argv.includes(APPLY_FLAG);
const SKIP_LIVE_PROBE = process.argv.includes("--skip-live-probe");
const SKIP_URL_PROBE = process.argv.includes("--skip-url-probe");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

function loadJson(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

async function loadBrandProfiles(base, brandConfigs) {
  const profiles = new Map();
  for (const brandConfig of brandConfigs) {
    try {
      const rec = await base(BRAND_TABLE).find(brandConfig.recordId);
      profiles.set(brandConfig.recordId, {
        id: rec.id,
        entityType: "brand",
        name: cellToString(rec.fields["Brand Name"]) || brandConfig.brandName,
        fields: rec.fields || {},
      });
    } catch {
      profiles.set(brandConfig.recordId, {
        id: brandConfig.recordId,
        entityType: "brand",
        name: brandConfig.brandName,
        fields: {},
      });
    }
    await new Promise((r) => setTimeout(r, 120));
  }
  return profiles;
}

async function applyPatch(base, { table, recordId, patch }) {
  if (!patch || !Object.keys(patch).length) {
    return { applied: false, fieldCount: 0 };
  }
  await base(table).update(recordId, patch, { typecast: true });
  return { applied: true, fieldCount: Object.keys(patch).length };
}

async function main() {
  if (APPLY && !APPROVED) {
    console.error(
      `[choice-legacy-batch-pipeline] Apply requires ${APPLY_FLAG}`
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandFilter = argValue("--brand") || null;
  const brandConfigs = getBatchBrandConfigs(batchName, brandFilter);
  const governanceReport = loadJson(GOVERNANCE_JSON);
  const readinessReport = loadJson(READINESS_JSON);

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
  const targetProfilesById = await loadBrandProfiles(base, brandConfigs);
  const catalogRows = new Map(
    [...targetProfilesById.values()].map((p) => [p.id, { id: p.id, name: p.name, fields: p.fields }])
  );

  console.log(
    `[choice-legacy-batch-pipeline] ${batchName} mode=${DRY_RUN ? "dry-run" : "apply"} brand=${brandFilter || "all"}`
  );

  const report = await runChoiceLegacyBatchPipeline({
    batchName,
    mode: DRY_RUN ? "dry-run" : "apply",
    brandFilter,
    governanceReport,
    readinessReport,
    probeLive: !SKIP_LIVE_PROBE,
    probeUrls: !SKIP_URL_PROBE,
    targetProfilesById,
    applyPatch: APPLY ? (args) => applyPatch(base, args) : null,
    catalogRows,
  });

  for (const brand of report.brands) {
    console.log(
      `  ${brand.brandName}: stage=${brand.currentStage} sources=${brand.approvedSourceCount}/${brand.sourceCount} facts=${brand.approvedFactCount}/${brand.pendingFactCount} ready=${brand.platformReady ? "yes" : "no"}`
    );
  }

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildChoiceLegacyBatchPipelineMarkdown(report), "utf8");

  console.log(
    `[choice-legacy-batch-pipeline] summary platform_ready=${report.executiveSummary.brandsPlatformReady} blocked=${report.executiveSummary.brandsBlocked} apply_recommended=${report.executiveSummary.applyRecommended}`
  );
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);

  if (report.errors?.length) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
