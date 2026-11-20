/**
 * Correct Brand Website on Active Brand Basics rows to brand-specific pages.
 *
 * Usage:
 *   node scripts/correct-active-brand-websites.mjs --dry-run
 *   node scripts/correct-active-brand-websites.mjs --apply
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";
import path from "path";
import {
  ACTIVE_BRAND_WEBSITE_CORRECTIONS,
  isParentCompanyHomepage,
  normalizeBrandWebsiteUrl,
} from "../lib/brand-explorer/active-brand-website-corrections.js";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const FIELD = "Brand Website";
const REPORT_PATH = path.join("reports", "active-brand-website-corrections.json");

const apply = process.argv.includes("--apply");
const dryRun = !apply || process.argv.includes("--dry-run");

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

// Permanent rule: Brand Website = brand-specific page for every Basics row we
 // can map — Active/Live plus Draft/Under Review factory brands. Never leave a
 // parent-company homepage when a brand page is known.
const rows = await base(BASICS_TABLE)
  .select({
    fields: ["Brand Name", "Parent Company", FIELD, "Brand Status"],
    sort: [{ field: "Brand Name", direction: "asc" }],
  })
  .all();

const planned = [];
const skipped = [];
const missingMapping = [];
const unmappedParentHomepage = [];

for (const row of rows) {
  const name = String(row.get("Brand Name") || "").trim();
  const parent = String(row.get("Parent Company") || "").trim();
  const status = String(row.get("Brand Status") || "").trim();
  const current = normalizeBrandWebsiteUrl(row.get(FIELD) || "");
  const targetRaw = ACTIVE_BRAND_WEBSITE_CORRECTIONS[name];
  const parentHomepage = isParentCompanyHomepage(current, parent, name);

  if (!targetRaw) {
    if (parentHomepage) {
      unmappedParentHomepage.push({ id: row.id, name, status, current });
      missingMapping.push({ id: row.id, name, status, current });
    }
    continue;
  }

  const target = normalizeBrandWebsiteUrl(targetRaw);
  const needsUpdate =
    current !== target ||
    parentHomepage ||
    /^https:\/\/www\./i.test(String(row.get(FIELD) || ""));

  if (!needsUpdate) {
    skipped.push({ id: row.id, name, status, current, target });
    continue;
  }

  planned.push({
    id: row.id,
    name,
    parent,
    status,
    current: current || null,
    target,
    parentHomepageBefore: parentHomepage,
  });
}

const report = {
  generatedAt: new Date().toISOString(),
  mode: dryRun ? "dry-run" : "apply",
  brandBasicsCount: rows.length,
  updateCount: planned.length,
  skippedCount: skipped.length,
  missingMappingCount: missingMapping.length,
  unmappedParentHomepageCount: unmappedParentHomepage.length,
  planned,
  skipped,
  missingMapping,
  unmappedParentHomepage,
};

fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2) + "\n", "utf8");

console.log(
  JSON.stringify(
    {
      mode: report.mode,
      brandBasicsCount: report.brandBasicsCount,
      updateCount: report.updateCount,
      skippedCount: report.skippedCount,
      unmappedParentHomepageCount: report.unmappedParentHomepageCount,
      report: REPORT_PATH,
    },
    null,
    2
  )
);

for (const item of planned) {
  console.log(`${item.name} [${item.status}]: ${item.current || "(blank)"} -> ${item.target}`);
}

if (unmappedParentHomepage.length) {
  console.error(
    "Unmapped parent-homepage Brand Website rows:",
    unmappedParentHomepage.map((m) => m.name).join(", ")
  );
  process.exitCode = 1;
}

if (!apply) {
  console.log("Dry run only. Re-run with --apply to write Brand Website values.");
  process.exit(0);
}

const results = { updated: [], failed: [] };
for (const item of planned) {
  try {
    await base(BASICS_TABLE).update(item.id, { [FIELD]: item.target });
    results.updated.push({ id: item.id, name: item.name, target: item.target });
  } catch (err) {
    results.failed.push({
      id: item.id,
      name: item.name,
      error: err?.message || String(err),
    });
  }
}

report.applyResults = results;
fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2) + "\n", "utf8");

console.log(
  JSON.stringify(
    {
      updated: results.updated.length,
      failed: results.failed.length,
      report: REPORT_PATH,
    },
    null,
    2
  )
);

if (results.failed.length) process.exit(1);
