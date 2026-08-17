#!/usr/bin/env node
/**
 * Choice legacy mini-batch profile governance publish.
 *
 *   npm run choice-legacy-batch-governance-publish -- --batch mini-batch-2 --dry-run
 *   npm run choice-legacy-batch-governance-publish -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-governance-publish
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { cellToString } from "../lib/airtable-utils.js";
import {
  applyChoiceLegacyBatchGovernancePublish,
  buildChoiceLegacyBatchGovernancePublishMarkdown,
  buildChoiceLegacyBatchGovernancePublishReport,
  getBatchGovernanceBrandConfigs,
} from "../lib/partner-intelligence/choice-legacy-batch-governance-publish.js";
import { BRAND_TABLE } from "../lib/partner-intelligence/profile-governance-publish.js";
import {
  getBatchBrandKeys,
  getBatchReportFiles,
  parseBatchNameFromArgv,
} from "../lib/partner-intelligence/choice-legacy-batch-config.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

const batchName = parseBatchNameFromArgv();
const reportFiles = getBatchReportFiles(batchName, "governancePublish");
const REPORT_JSON = join(ROOT, "reports", reportFiles.json);
const REPORT_MD = join(ROOT, "reports", reportFiles.md);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVED = process.argv.includes("--approve-choice-legacy-batch-governance-publish");

const BRAND_NAME_FIELD = "Brand Name";

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

function readName(fields, candidates) {
  for (const key of candidates) {
    const v = cellToString(fields[key]);
    if (v) return v;
  }
  return null;
}

async function loadBrandProfiles(base, brandConfigs) {
  const profiles = new Map();
  for (const brandConfig of brandConfigs) {
    try {
      const rec = await base(BRAND_TABLE).find(brandConfig.recordId);
      profiles.set(brandConfig.recordId, {
        id: rec.id,
        entityType: "brand",
        name: readName(rec.fields, [BRAND_NAME_FIELD, "brand_name"]) || brandConfig.brandName,
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
      "[choice-legacy-batch-governance-publish] Apply requires --approve-choice-legacy-batch-governance-publish"
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const brandFilter = argValue("--brand") || null;
  const allowedKeys = new Set(getBatchBrandKeys(batchName));
  if (brandFilter && !allowedKeys.has(brandFilter) && !/^rec[a-zA-Z0-9]{10,}$/.test(brandFilter)) {
    console.error(
      `[choice-legacy-batch-governance-publish] Invalid --brand: ${brandFilter} (use one of: ${[...allowedKeys].join(", ")})`
    );
    process.exit(1);
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
  const brandConfigs = getBatchGovernanceBrandConfigs(batchName, brandFilter);
  const generatedAt = new Date().toISOString();
  const applyTimestamp = generatedAt.split("T")[0];

  console.log(
    `[choice-legacy-batch-governance-publish] ${batchName} mode=${DRY_RUN ? "dry-run" : "apply"} brand=${brandFilter || "all"}`
  );

  const targetProfilesById = await loadBrandProfiles(base, brandConfigs);

  const report = await buildChoiceLegacyBatchGovernancePublishReport({
    brandFilter,
    batchName,
    targetProfilesById,
    mode: DRY_RUN ? "dry-run" : "apply",
    applyTimestamp: APPLY ? applyTimestamp : null,
  });

  let applyResult = null;
  if (APPLY) {
    if (report.summary.eligibleForGovernancePublish === 0) {
      console.error(
        "[choice-legacy-batch-governance-publish] Apply rejected: no brands eligible for batch governance publish."
      );
      process.exit(1);
    }

    applyResult = await applyChoiceLegacyBatchGovernancePublish(report, {
      brandFilter,
      applyPatch: (args) => applyPatch(base, args),
    });
    report.mode = "apply";
    report.airtableModified = applyResult.applied.length > 0;
    report.applyResult = applyResult;
    report.summary.skipped = applyResult.skipped.length;
    console.log(
      `[choice-legacy-batch-governance-publish] apply applied=${applyResult.applied.length} skipped=${applyResult.skipped.length} errors=${applyResult.errors.length}`
    );
  } else {
    for (const brand of report.brands) {
      console.log(
        `  ${brand.brandName}: sources=${brand.approvedSourceCount} facts=${brand.approvedFactCount} pending=${brand.pendingFactCount} eligible=${brand.eligibleForBatchApply ? "yes" : "no"}`
      );
    }
  }

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify({ ...report, applyResult }, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildChoiceLegacyBatchGovernancePublishMarkdown(report), "utf8");

  console.log(
    `[choice-legacy-batch-governance-publish] summary eligible=${report.summary.eligibleForGovernancePublish} blocked=${report.summary.blocked}`
  );
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);

  if (applyResult?.errors?.length) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
