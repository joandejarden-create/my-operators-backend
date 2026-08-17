#!/usr/bin/env node
/**
 * Backfill Mexico Demand Anchors + Travel Infrastructure Region → North America.
 *
 *   node scripts/backfill-mexico-radar-region.mjs --dry-run
 *   node scripts/backfill-mexico-radar-region.mjs --apply
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { fetchDemandAnchorRecords } from "../lib/demand-anchors/airtable-demand-anchors-io.js";
import { fetchTravelInfrastructureRecords } from "../lib/travel-infrastructure/airtable-travel-infrastructure-io.js";
import { getDemandAnchorsAirtableConfig, resolveDemandAnchorsTableName } from "../lib/demand-anchors/demand-anchors-base.js";
import {
  getTravelInfrastructureAirtableConfig,
  resolveTravelInfrastructureTableName,
} from "../lib/travel-infrastructure/travel-infrastructure-base.js";
import { DEMAND_ANCHORS_FIELDS as DA_F } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import { TRAVEL_INFRASTRUCTURE_FIELDS as TI_F } from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";
import { MEXICO_RADAR_REGION } from "../lib/radar-buildout/mexico-radar-region.js";
import {
  fetchAirtableTableFieldNameSet,
  filterFieldsToAirtableSchema,
} from "../lib/third-party-operator-basics-airtable-column-aliases.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const DRY_RUN = !APPLY;
const WRONG = new Set(["Mexico", "Caribbean"]);
const DELAY_MS = Number(process.env.AIRTABLE_WRITE_DELAY_MS) || 220;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function needsRegionFix(record) {
  const country = String(record.country || "").trim();
  const region = String(record.region || "").trim();
  return country === "Mexico" && WRONG.has(region);
}

async function backfillTable({ label, fetchFn, getCfg, resolveTable, fieldMap, regionField }) {
  const result = await fetchFn({ country: "Mexico", includeHidden: true });
  if (result.error) throw new Error(`${label}: ${result.error}`);

  const records = result.allPoints || result.points || [];
  const targets = records.filter(needsRegionFix);
  const cfg = getCfg();
  if (!cfg) throw new Error(`${label}: airtable_config_missing`);

  const tableName = await resolveTable(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);

  console.log(`\n${label}: scanned=${records.length} needing=${targets.length}`);

  const report = { updated: [], skipped: [], errors: [] };

  for (const row of targets) {
    const patch = filterFieldsToAirtableSchema({ [regionField]: MEXICO_RADAR_REGION }, schema);
    if (!Object.keys(patch).length) {
      report.skipped.push({ id: row.id, name: row.name, reason: "schema_missing_region" });
      continue;
    }

    if (DRY_RUN) {
      console.log(`  would update ${row.name}: ${row.region} → ${MEXICO_RADAR_REGION}`);
      report.updated.push({ id: row.id, name: row.name, from: row.region, to: MEXICO_RADAR_REGION });
      continue;
    }

    try {
      await cfg.base(tableName).update(row.id, patch, { typecast: true });
      report.updated.push({ id: row.id, name: row.name, from: row.region, to: MEXICO_RADAR_REGION });
      console.log(`  updated ${row.name}`);
    } catch (err) {
      report.errors.push({ id: row.id, name: row.name, message: err?.message || String(err) });
      console.error(`  FAIL ${row.name}:`, err?.message || err);
    }
    await sleep(DELAY_MS);
  }

  return { label, tableName, scanned: records.length, needing: targets.length, report };
}

const sections = [];

sections.push(
  await backfillTable({
    label: "Demand Anchors",
    fetchFn: fetchDemandAnchorRecords,
    getCfg: getDemandAnchorsAirtableConfig,
    resolveTable: resolveDemandAnchorsTableName,
    fieldMap: DA_F,
    regionField: DA_F.region,
  })
);

sections.push(
  await backfillTable({
    label: "Travel Infrastructure",
    fetchFn: fetchTravelInfrastructureRecords,
    getCfg: getTravelInfrastructureAirtableConfig,
    resolveTable: resolveTravelInfrastructureTableName,
    fieldMap: TI_F,
    regionField: TI_F.region,
  })
);

const out = {
  generatedAt: new Date().toISOString(),
  mode: DRY_RUN ? "dry-run" : "apply",
  targetRegion: MEXICO_RADAR_REGION,
  sections,
};

const outPath = join(root, "data/mexico-radar-region-backfill-report.json");
if (!existsSync(dirname(outPath))) mkdirSync(dirname(outPath), { recursive: true });
writeFileSync(outPath, `${JSON.stringify(out, null, 2)}\n`);

console.log(`\nReport: ${outPath}`);
if (DRY_RUN) {
  console.log("No writes performed. Re-run with --apply to update Airtable.");
}

const errorCount = sections.reduce((n, s) => n + s.report.errors.length, 0);
if (errorCount) process.exit(1);
