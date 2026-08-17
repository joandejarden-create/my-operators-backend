#!/usr/bin/env node
/**
 * Backfill Market Alerts intelligence fields.
 *
 *   npm run market-alerts:intelligence:backfill -- --dry-run --limit 100
 *   npm run market-alerts:intelligence:backfill -- --apply --limit 100
 *   npm run market-alerts:intelligence:backfill -- --dry-run --id recXXX
 *   npm run market-alerts:intelligence:backfill -- --dry-run --since 2026-01-01
 */
import "../load-env.js";
import Airtable from "airtable";
import { MAP_ALERT } from "../api/lib/market-alerts-rss-airtable.js";
import { MAP_INTEL } from "../api/lib/market-alerts-intelligence-map.js";
import {
  applyIntelligencePatch,
  computeMarketAlertIntelligence,
} from "../lib/market-alerts-intelligence.js";

const APPLY = process.argv.includes("--apply");
const DRY_RUN = !APPLY || process.argv.includes("--dry-run");
const ONLY_UNENRICHED = process.argv.includes("--only-unenriched");

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  if (i === -1) return null;
  return process.argv[i + 1] || null;
}

async function loadKnownFieldNames(baseId, token, tableName) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  const table = (json.tables || []).find((t) => t.name === tableName);
  if (!table) throw new Error(`Table not found: ${tableName}`);
  return new Set((table.fields || []).map((f) => f.name));
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const tableName = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const limit = Math.min(Math.max(parseInt(argValue("--limit") || "100", 10) || 100, 1), 500);
  const since = argValue("--since");
  const id = argValue("--id");

  console.log(`Mode: ${DRY_RUN ? "dry-run" : "apply"} limit=${limit}${ONLY_UNENRICHED ? " only-unenriched" : ""}`);

  const knownFieldNames = await loadKnownFieldNames(baseId, token, tableName);
  const required = [
    MAP_INTEL.intelligenceStatus,
    MAP_INTEL.worthReviewingOwner,
    MAP_INTEL.eventType,
  ];
  const missing = required.filter((f) => !knownFieldNames.has(f));
  if (missing.length) {
    console.error(
      "Missing intelligence fields. Run: npm run market-alerts:intelligence:ensure -- --apply"
    );
    console.error("Missing:", missing.join(", "));
    process.exit(1);
  }

  const base = new Airtable({ apiKey: token }).base(baseId);
  const select = {
    fields: [
      MAP_ALERT.title,
      MAP_ALERT.summary,
      MAP_ALERT.publishedAt,
      MAP_INTEL.intelligenceStatus,
      MAP_INTEL.entityKey,
    ],
    sort: [{ field: MAP_ALERT.publishedAt, direction: "desc" }],
    maxRecords: id ? 1 : limit,
  };

  if (id) {
    select.filterByFormula = `RECORD_ID() = '${id.replace(/'/g, "\\'")}'`;
  } else if (since) {
    select.filterByFormula = `IS_AFTER({${MAP_ALERT.publishedAt}}, '${since}')`;
  } else if (ONLY_UNENRICHED) {
    select.filterByFormula = `OR({${MAP_INTEL.intelligenceStatus}} = BLANK(), {${MAP_INTEL.intelligenceStatus}} = "Pending", {${MAP_INTEL.intelligenceStatus}} = "Error")`;
  }

  const records = await base(tableName).select(select).all();
  console.log(`Loaded ${records.length} alerts`);

  const preview = [];
  let updated = 0;
  let errors = 0;

  for (const rec of records) {
    const title = rec.fields[MAP_ALERT.title] || "";
    const summary = rec.fields[MAP_ALERT.summary] || "";
    const computed = computeMarketAlertIntelligence({
      title,
      summary,
      alertId: rec.id,
    });

    preview.push({
      id: rec.id,
      title: String(title).slice(0, 100),
      eventType: computed.meta.event?.eventType || null,
      treatment: computed.meta.treatment,
      worth: {
        owner: !!computed.fields[MAP_INTEL.worthReviewingOwner],
        brand: !!computed.fields[MAP_INTEL.worthReviewingBrand],
        operator: !!computed.fields[MAP_INTEL.worthReviewingOperator],
      },
      hotelProject: computed.meta.entities?.hotelProject || null,
      rooms: computed.meta.entities?.rooms ?? null,
    });

    const result = await applyIntelligencePatch(rec.id, computed.fields, {
      tableName,
      knownFieldNames,
      dryRun: DRY_RUN,
    });
    if (result.ok) updated += 1;
    else {
      errors += 1;
      console.error(`Failed ${rec.id}:`, result.error || result.reason);
    }
  }

  console.log("\nSample preview (up to 15):");
  for (const row of preview.slice(0, 15)) {
    console.log(
      `- ${row.id} | ${row.eventType || "—"} | ${row.treatment} | O/B/Op=${row.worth.owner}/${row.worth.brand}/${row.worth.operator} | ${row.title}`
    );
  }

  console.log(
    `\nSummary: processed=${records.length} ${DRY_RUN ? "wouldUpdate" : "updated"}=${updated} errors=${errors}`
  );
  if (DRY_RUN) console.log("Re-run with --apply to write.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
