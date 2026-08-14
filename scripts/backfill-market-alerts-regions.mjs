#!/usr/bin/env node
/**
 * Reclassify MarketAlerts Region Group + Category using current inference rules.
 *
 * Usage:
 *   node scripts/backfill-market-alerts-regions.mjs --dry-run
 *   node scripts/backfill-market-alerts-regions.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  MAP_ALERT,
  inferClassificationPatch,
} from "../api/lib/market-alerts-rss-airtable.js";

const BATCH = 10;
const FIELDS = [
  MAP_ALERT.title,
  MAP_ALERT.summary,
  MAP_ALERT.sourceUrl,
  MAP_ALERT.sourceName,
  MAP_ALERT.regionGroup,
  MAP_ALERT.category,
];

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
  const transitions = {};
  const categoryChanges = {};

  await base(table)
    .select({ fields: FIELDS, pageSize: 100 })
    .eachPage((records, next) => {
      for (const rec of records) {
        scanned += 1;
        const before = rec.fields;
        const { patch, region, category, currentRegion, currentCategory } =
          inferClassificationPatch(before);
        if (!Object.keys(patch).length) continue;

        if (patch[MAP_ALERT.regionGroup]) {
          const key = `${currentRegion} → ${region}`;
          transitions[key] = (transitions[key] || 0) + 1;
        }
        if (patch[MAP_ALERT.category]) {
          const ckey = `${currentCategory} → ${category}`;
          categoryChanges[ckey] = (categoryChanges[ckey] || 0) + 1;
        }

        toUpdate.push({ id: rec.id, before, fields: patch, region, category });
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
        regionTransitions: transitions,
        categoryTransitions: categoryChanges,
        samples: toUpdate.slice(0, 12).map((u) => ({
          title: (u.before[MAP_ALERT.title] || "").slice(0, 80),
          region: `${u.before[MAP_ALERT.regionGroup] || "Global"} → ${u.region}`,
          category: `${u.before[MAP_ALERT.category] || "?"} → ${u.category}`,
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
      console.error("[backfill-market-alerts-regions] batch failed:", err.message);
    }
  }

  console.log(JSON.stringify({ updated, errors }, null, 2));
  process.exit(errors.length ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
