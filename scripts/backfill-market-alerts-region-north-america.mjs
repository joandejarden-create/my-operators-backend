#!/usr/bin/env node
/**
 * Reclassify MarketAlerts rows about the US/Canada to Region Group "North America".
 *
 * Prerequisite: add "North America" to the Region Group single-select in Airtable
 * (or rely on typecast:true if your base allows new options).
 *
 * Usage:
 *   node scripts/backfill-market-alerts-region-north-america.mjs --dry-run
 *   node scripts/backfill-market-alerts-region-north-america.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  MAP_ALERT,
  inferRegionGroupFromFields,
} from "../api/lib/market-alerts-rss-airtable.js";

const BATCH = 10;
const FIELDS = [MAP_ALERT.title, MAP_ALERT.summary, MAP_ALERT.sourceUrl, MAP_ALERT.sourceName, MAP_ALERT.regionGroup];

function parseArgs(argv) {
  let dryRun = false;
  for (const arg of argv) {
    if (arg === "--dry-run") dryRun = true;
  }
  return { dryRun };
}

async function main() {
  const { dryRun } = parseArgs(process.argv.slice(2));
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";

  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const toUpdate = [];
  let scanned = 0;
  const fromRegion = {};

  await base(table)
    .select({ fields: FIELDS, pageSize: 100 })
    .eachPage((records, next) => {
      for (const rec of records) {
        scanned += 1;
        const before = rec.fields;
        const current = before[MAP_ALERT.regionGroup] || "Global";
        const inferred = inferRegionGroupFromFields(before);
        if (inferred !== "North America" || current === "North America") continue;
        fromRegion[current] = (fromRegion[current] || 0) + 1;
        toUpdate.push({
          id: rec.id,
          before,
          fields: { [MAP_ALERT.regionGroup]: "North America" },
        });
      }
      next();
    });

  console.log(
    JSON.stringify(
      {
        table,
        dryRun,
        scanned,
        toUpdate: toUpdate.length,
        fromRegion,
        samples: toUpdate.slice(0, 8).map((u) => ({
          id: u.id,
          from: u.before[MAP_ALERT.regionGroup] || "Global",
          title: (u.before[MAP_ALERT.title] || "").slice(0, 80),
        })),
      },
      null,
      2
    )
  );

  if (dryRun || toUpdate.length === 0) process.exit(0);

  let updated = 0;
  const errors = [];
  for (let i = 0; i < toUpdate.length; i += BATCH) {
    const batch = toUpdate.slice(i, i + BATCH).map(({ id, fields }) => ({ id, fields }));
    try {
      await base(table).update(batch, { typecast: true });
      updated += batch.length;
    } catch (err) {
      errors.push({ batchStart: i, message: err.message || String(err) });
      console.error("[backfill-market-alerts-region-north-america] batch failed:", err.message);
    }
  }

  console.log(JSON.stringify({ updated, errors }, null, 2));
  process.exit(errors.length ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
