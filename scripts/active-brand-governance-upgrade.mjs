#!/usr/bin/env node
/**
 * Active Brand Explorer profiles — governance upgrade audit (dry-run default).
 *
 *   npm run active-brand-governance-upgrade -- --dry-run
 *
 * Read-only. No Airtable writes, no governance publish, no source registration.
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { PARTNER_INTELLIGENCE_LINKS } from "../api/lib/partner-intelligence-field-map.js";
import { listPartnerSources } from "../lib/partner-intelligence/airtable-source.js";
import { listPartnerFacts } from "../lib/partner-intelligence/airtable-facts.js";
import {
  ACTIVE_BRAND_BATCH,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  UPGRADE_VERSION,
  buildActiveBrandGovernanceUpgradeMarkdown,
  buildActiveBrandGovernanceUpgradeReport,
  inspectActiveBrand,
  resolveBrandCatalogEntry,
} from "../lib/partner-intelligence/active-brand-governance-upgrade.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const READINESS_JSON = join(ROOT, "reports", "partner-intelligence-publish-readiness.json");

const DRY_RUN = process.argv.includes("--dry-run") || process.argv.includes("--plan") || !process.argv.includes("--apply");

const APPLY_FLAGS = ["--apply", "--publish-apply", "--approve-stewardship"];

async function fetchAllSources(brandId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerSources({ brandId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchAllFacts(brandId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchBrandCatalog() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const table = PARTNER_INTELLIGENCE_LINKS.brandBasics;
  const base = new Airtable({ apiKey }).base(baseId);
  const records = [];
  await new Promise((resolve, reject) => {
    base(table)
      .select({ pageSize: 100 })
      .eachPage(
        (page, next) => {
          for (const rec of page) {
            const fields = rec.fields || {};
            const name = String(fields["Brand Name"] || fields.brand_name || "").trim();
            records.push({ id: rec.id, name, fields });
          }
          next();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });
  return records;
}

function loadReadinessReport() {
  try {
    return JSON.parse(readFileSync(READINESS_JSON, "utf8"));
  } catch {
    return null;
  }
}

function rejectApplyFlags() {
  for (const flag of APPLY_FLAGS) {
    if (process.argv.includes(flag)) {
      return {
        rejected: true,
        message:
          "[active-brand-governance-upgrade] Apply mode is disabled in v1. Use steward/extract/publish scripts per brand.",
      };
    }
  }
  return { rejected: false };
}

async function main() {
  const applyReject = rejectApplyFlags();
  if (applyReject.rejected) {
    console.error(applyReject.message);
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  console.log(
    `[active-brand-governance-upgrade] v${UPGRADE_VERSION} mode=${DRY_RUN ? "dry-run" : "apply"} brands=${ACTIVE_BRAND_BATCH.length}`
  );

  const [catalog, readinessReport] = await Promise.all([
    fetchBrandCatalog(),
    Promise.resolve(loadReadinessReport()),
  ]);

  const rows = [];
  for (const batchEntry of ACTIVE_BRAND_BATCH) {
    const resolution = resolveBrandCatalogEntry(batchEntry, catalog);
    if (!resolution.resolved) {
      console.log(`  ${batchEntry.displayName}: unresolved — ${resolution.unresolvedReason}`);
      rows.push(
        inspectActiveBrand({
          batchEntry,
          resolution,
          sources: [],
          facts: [],
          readinessReport,
        })
      );
      continue;
    }

    const [sources, facts] = await Promise.all([
      fetchAllSources(resolution.recordId),
      fetchAllFacts(resolution.recordId),
    ]);

    const row = inspectActiveBrand({
      batchEntry,
      resolution,
      sources,
      facts,
      readinessReport,
    });
    rows.push(row);
    console.log(
      `  ${row.brandName}: ${row.profileStatus} · sources ${row.approvedSourceCount}/${row.piSourceCount} · facts ${row.approvedFactCount}/${row.totalFactCount}`
    );
  }

  const report = buildActiveBrandGovernanceUpgradeReport(rows);
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildActiveBrandGovernanceUpgradeMarkdown(report), "utf8");

  console.log(
    `[active-brand-governance-upgrade] summary resolved=${report.summary.resolved} platform_ready=${report.summary.platformReady} governance_upgrade=${report.summary.governanceUpgradeNeeded}`
  );
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
