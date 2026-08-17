#!/usr/bin/env node
/**
 * Backfill Submarket (+ Tulum airport city) for Mexico Cancún Travel Infrastructure.
 *
 *   node scripts/backfill-mexico-cancun-ti-submarkets.mjs --dry-run
 *   node scripts/backfill-mexico-cancun-ti-submarkets.mjs --apply
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { fetchTravelInfrastructureRecords } from "../lib/travel-infrastructure/airtable-travel-infrastructure-io.js";
import {
  getTravelInfrastructureAirtableConfig,
  resolveTravelInfrastructureTableName,
} from "../lib/travel-infrastructure/travel-infrastructure-base.js";
import { TRAVEL_INFRASTRUCTURE_FIELDS as TI_F } from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";
import {
  planMexicoCancunTiSubmarketBackfill,
  MEXICO_CANCUN_TI_SUBMARKET_TARGETS,
} from "../lib/radar-buildout/mexico-cancun-ti-submarket-backfill.js";
import {
  fetchAirtableTableFieldNameSet,
  filterFieldsToAirtableSchema,
} from "../lib/third-party-operator-basics-airtable-column-aliases.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const VERBOSE = process.argv.includes("--verbose");
const output = (() => {
  const idx = process.argv.indexOf("--output");
  return idx >= 0 ? process.argv[idx + 1] : "data/mexico-cancun-ti-submarket-backfill-report.json";
})();

async function fetchSubmarketChoices(baseId, apiKey, tableName) {
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return new Set();
    const data = await res.json();
    const table = (data.tables || []).find((t) => t.name === tableName);
    const field = (table?.fields || []).find((f) => f.name === TI_F.submarket);
    const choices = (field?.options?.choices || []).map((c) => c.name).filter(Boolean);
    return new Set(choices);
  } catch {
    return new Set();
  }
}

const result = await fetchTravelInfrastructureRecords({ country: "Mexico", includeHidden: true });
if (result.error) {
  console.error("Failed to fetch Travel Infrastructure:", result.error);
  process.exit(1);
}

const records = result.allPoints || result.points || [];
const cfg = getTravelInfrastructureAirtableConfig();
if (!cfg) {
  console.error("Airtable config missing");
  process.exit(1);
}
const tableName = await resolveTravelInfrastructureTableName(cfg.baseId, cfg.apiKey);
const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);
const allowedSubmarkets = await fetchSubmarketChoices(cfg.baseId, cfg.apiKey, tableName);

const missingTargets = MEXICO_CANCUN_TI_SUBMARKET_TARGETS.filter(
  (t) => t !== "Other" && allowedSubmarkets.size && !allowedSubmarkets.has(t)
);

const plan = planMexicoCancunTiSubmarketBackfill(records, {
  country: "Mexico",
  allowedSubmarkets,
});

console.log(DRY_RUN ? "=== DRY RUN TI submarket backfill ===" : "=== APPLY TI submarket backfill ===");
console.log("Mexico TI scanned:", plan.scanned);
console.log("Market-matched:", plan.marketMatched);
console.log("Needing update:", plan.needingUpdate);
console.log("Missing Airtable submarket options:", missingTargets.length ? missingTargets.join(", ") : "(none — all targets present or schema unknown)");
console.log("No mapping rule:", plan.noMappingRule.length);

if (plan.samples.length) {
  console.log("\nSample updates:");
  for (const s of plan.samples) {
    console.log(`  ${s.name}: ${s.changes.join("; ")}`);
  }
}

if (plan.missingSubmarketOptions.length) {
  console.log("\nRecords blocked by missing submarket options:");
  for (const m of plan.missingSubmarketOptions) {
    console.log(`  ${m.name} needs "${m.missingOption}"`);
  }
}

const applyReport = { updated: [], skipped: [], errors: [] };
const AIRTABLE_BATCH_DELAY_MS = Number(process.env.AIRTABLE_WRITE_DELAY_MS) || 220;
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

if (APPLY) {
  for (const item of plan.updates) {
    const airtablePatch = {};
    if (item.patch.submarket) airtablePatch[TI_F.submarket] = item.patch.submarket;
    if (item.patch.city) airtablePatch[TI_F.city] = item.patch.city;
    if (item.patch.notes) airtablePatch[TI_F.notes] = item.patch.notes;
    const patch = filterFieldsToAirtableSchema(airtablePatch, schema);
    if (!Object.keys(patch).length) {
      applyReport.skipped.push({ id: item.recordId, name: item.name, reason: "empty_patch" });
      continue;
    }
    try {
      await cfg.base(tableName).update(item.recordId, patch, { typecast: true });
      applyReport.updated.push({ id: item.recordId, name: item.name, changes: item.changes });
      if (VERBOSE) console.log("UPDATED", item.name, item.changes.join("; "));
    } catch (err) {
      applyReport.errors.push({ id: item.recordId, name: item.name, message: err?.message || String(err) });
      console.error("FAIL", item.name, err?.message || err);
    }
    await sleep(AIRTABLE_BATCH_DELAY_MS);
  }
  console.log("\nApply:", `updated=${applyReport.updated.length}`, `skipped=${applyReport.skipped.length}`, `errors=${applyReport.errors.length}`);
}

const report = {
  generatedAt: new Date().toISOString(),
  mode: APPLY ? "apply" : "dry-run",
  allowedSubmarketOptions: [...allowedSubmarkets],
  missingTargetOptions: missingTargets,
  plan: {
    scanned: plan.scanned,
    marketMatched: plan.marketMatched,
    needingUpdate: plan.needingUpdate,
    samples: plan.samples,
    missingSubmarketOptions: plan.missingSubmarketOptions,
    noMappingRule: plan.noMappingRule,
  },
  apply: APPLY ? applyReport : null,
};

const outPath = join(root, output);
const outDir = dirname(outPath);
if (!existsSync(outDir)) mkdirSync(outDir, { recursive: true });
writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");
console.log("Report written:", output);

if (DRY_RUN) {
  console.log("\nNo writes performed. Re-run with --apply when mapping looks correct.");
}

if (applyReport.errors.length) process.exit(1);
