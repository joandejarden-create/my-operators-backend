/**
 * Import LinkedIn-triaged contacts into Pilot Target List (GTM base).
 *
 *   node scripts/import-pilot-target-list-linkedin.mjs --dry-run
 *   node scripts/import-pilot-target-list-linkedin.mjs --execute
 *
 * Reports:
 *   reports/pilot-target-list-linkedin-import-report.json
 *   reports/pilot-target-list-linkedin-import-report.md
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  EXISTING_PILOT_TARGET_NAMES,
  LINKEDIN_PILOT_CONTACTS,
} from "../lib/gtm-owner-target/pilot-target-list-linkedin-contacts.js";
import {
  buildImportPlan,
  normalizeContactNameKey,
  summarizeImportPlan,
} from "../lib/gtm-owner-target/pilot-target-list-linkedin-import.js";
import {
  GTM_PILOT_TARGET_LIST_TABLE,
  MAP_PILOT_TARGET_LIST,
} from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import {
  assertGtmBaseConfigured,
  assertNotProductBase,
  getGtmAirtableBase,
} from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_JSON = path.join(ROOT, "reports", "pilot-target-list-linkedin-import-report.json");
const REPORT_MD = path.join(ROOT, "reports", "pilot-target-list-linkedin-import-report.md");

const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = process.argv.includes("--dry-run") || !EXECUTE;

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

function buildMarkdown(report) {
  const lines = [
    "# Pilot Target List — LinkedIn import report",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: ${report.mode}`,
    `Base: \`${report.baseId}\``,
    `Table: ${report.tableName}`,
    "",
    "## Summary",
    "",
    `- Seed contacts: ${report.seedContactCount}`,
    `- Already in Airtable (skipped): ${report.skippedExistingCount}`,
    `- Invalid rows: ${report.invalidCount}`,
    `- Created: ${report.createdCount}`,
    "",
    "### By tier (created)",
    "",
  ];

  for (const [tier, count] of Object.entries(report.byTier || {}).sort()) {
    lines.push(`- Tier ${tier}: ${count}`);
  }

  if (report.skippedExisting?.length) {
    lines.push("", "## Skipped (already in Airtable)", "");
    for (const row of report.skippedExisting) {
      lines.push(`- ${row.name}`);
    }
  }

  if (report.invalid?.length) {
    lines.push("", "## Invalid", "");
    for (const row of report.invalid) {
      lines.push(`- ${row.name}: ${row.errors.join(", ")}`);
    }
  }

  if (report.created?.length) {
    lines.push("", "## Created records", "");
    for (const row of report.created) {
      lines.push(`- ${row.name} (${row.recordId}) — tier ${row.tier}, ${row.segment}, ${row.priority}`);
    }
  }

  return `${lines.join("\n")}\n`;
}

async function fetchExistingNameKeys(base) {
  const keys = new Set(EXISTING_PILOT_TARGET_NAMES);
  const records = await base(GTM_PILOT_TARGET_LIST_TABLE)
    .select({ fields: [MAP_PILOT_TARGET_LIST.name] })
    .all();

  for (const rec of records) {
    const name = String(rec.get(MAP_PILOT_TARGET_LIST.name) || "").trim();
    if (name) keys.add(normalizeContactNameKey(name));
  }
  return keys;
}

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const existingNameKeys = await fetchExistingNameKeys(base);
  const plan = buildImportPlan(LINKEDIN_PILOT_CONTACTS, existingNameKeys);
  const summary = summarizeImportPlan(plan);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    baseId,
    tableName: GTM_PILOT_TARGET_LIST_TABLE,
    seedContactCount: LINKEDIN_PILOT_CONTACTS.length,
    skippedExistingCount: plan.skippedExisting.length,
    invalidCount: plan.invalid.length,
    createdCount: 0,
    byTier: summary.byTier,
    skippedExisting: plan.skippedExisting,
    invalid: plan.invalid,
    created: [],
  };

  console.log(`Pilot Target List LinkedIn import (${report.mode})`);
  console.log(`Seed contacts: ${report.seedContactCount}`);
  console.log(`To create: ${summary.createCount}`);
  console.log(`Skip existing: ${summary.skippedExistingCount}`);
  console.log(`Invalid: ${summary.invalidCount}`);

  if (plan.invalid.length) {
    console.error("Validation failures:");
    for (const row of plan.invalid) {
      console.error(`  - ${row.name}: ${row.errors.join(", ")}`);
    }
    process.exitCode = 1;
    return;
  }

  if (EXECUTE && plan.toCreate.length) {
    for (const batch of chunk(plan.toCreate, 10)) {
      const created = await base(GTM_PILOT_TARGET_LIST_TABLE).create(
        batch.map((row) => ({ fields: row.fields }))
      );
      for (let i = 0; i < created.length; i += 1) {
        const src = batch[i];
        report.created.push({
          recordId: created[i].id,
          name: src.name,
          tier: src.tier,
          segment: src.segment,
          priority: src.priority,
        });
      }
    }
    report.createdCount = report.created.length;
    console.log(`Created ${report.createdCount} records.`);
  } else if (DRY_RUN) {
    report.preview = plan.toCreate.map((row) => ({
      name: row.name,
      tier: row.tier,
      segment: row.segment,
      priority: row.priority,
      category: row.fields[MAP_PILOT_TARGET_LIST.category],
      pilotFit: row.fields[MAP_PILOT_TARGET_LIST.pilotFit],
    }));
    console.log("Dry run only — pass --execute to write to Airtable.");
  }

  fs.mkdirSync(path.dirname(REPORT_JSON), { recursive: true });
  fs.writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(REPORT_MD, buildMarkdown(report));
  console.log(`Report: ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error("[import-pilot-target-list-linkedin]", err.message || err);
  process.exitCode = 1;
});
