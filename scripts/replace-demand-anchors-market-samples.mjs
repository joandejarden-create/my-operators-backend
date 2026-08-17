#!/usr/bin/env node
/**
 * Remove demo/sample Demand Anchor records for a market before real-data import.
 *
 *   node scripts/replace-demand-anchors-market-samples.mjs --market "San Juan" --dry-run
 *   node scripts/replace-demand-anchors-market-samples.mjs --market "San Juan" --apply
 *
 * Matches records where City = market (or name/market context) AND any of:
 *   - name contains "sample" (case-insensitive)
 *   - notes contain seeded/sample fixture markers
 *   - visibility Demo with seeded notes
 *
 * Never deletes records that do not match sample markers.
 */
import "../load-env.js";
import { DEMAND_ANCHORS_FIELDS as F } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import {
  getDemandAnchorsAirtableConfig,
  resolveDemandAnchorsTableName,
} from "../lib/demand-anchors/demand-anchors-base.js";
import { fetchAirtableTableFieldNameSet } from "../lib/third-party-operator-basics-airtable-column-aliases.js";

const args = process.argv.slice(2);
const APPLY = args.includes("--apply");
const DRY = args.includes("--dry-run") || !APPLY;
const marketIdx = args.indexOf("--market");
const market = marketIdx >= 0 ? args[marketIdx + 1] : "San Juan";

const SAMPLE_NOTE_MARKERS = [
  "seeded demand anchor sample",
  "population fixture",
  "illustrative future growth",
  "verify before production use",
];

function isSampleRecord(fields) {
  const name = String(fields[F.name] || "").toLowerCase();
  const notes = String(fields[F.notes] || "").toLowerCase();
  const visibility = String(fields[F.visibility] || "");

  if (name.includes("sample")) return { match: true, reason: "name_contains_sample" };
  for (const marker of SAMPLE_NOTE_MARKERS) {
    if (notes.includes(marker)) return { match: true, reason: `notes:${marker}` };
  }
  if (visibility === "Demo" && notes.includes("seeded")) {
    return { match: true, reason: "demo_visibility_seeded_notes" };
  }
  return { match: false, reason: null };
}

function recordCity(fields) {
  return String(fields[F.city] || "").trim().toLowerCase();
}

async function main() {
  const cfg = getDemandAnchorsAirtableConfig();
  if (!cfg) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT required");

  const tableName = await resolveDemandAnchorsTableName(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);
  const fields = [F.name, F.city, F.country, F.notes, F.visibility, F.pointType].filter(
    (f) => !schema || schema.has(f)
  );

  const records = await cfg.base(tableName).select({ fields }).all();
  const marketKey = market.trim().toLowerCase();

  const targets = [];
  const retained = [];

  for (const rec of records) {
    const city = recordCity(rec.fields);
    const inMarket = city === marketKey || city.includes(marketKey);
    if (!inMarket) {
      retained.push(rec);
      continue;
    }
    const sample = isSampleRecord(rec.fields);
    if (sample.match) {
      targets.push({
        id: rec.id,
        name: rec.fields[F.name],
        pointType: rec.fields[F.pointType],
        reason: sample.reason,
        notes: rec.fields[F.notes],
      });
    } else {
      retained.push(rec);
    }
  }

  console.log("Market:", market);
  console.log("Total records scanned:", records.length);
  console.log("Sample records to remove:", targets.length);
  console.log("Non-sample records retained:", retained.length);

  for (const t of targets) {
    console.log(" ", t.id, "—", t.name, `(${t.reason})`);
  }

  if (!targets.length) {
    console.log("\nNo sample records matched. Nothing to delete.");
    return;
  }

  if (DRY) {
    console.log("\nDry run — pass --apply to delete listed records.");
    return;
  }

  let deleted = 0;
  const errors = [];
  for (const t of targets) {
    try {
      await cfg.base(tableName).destroy(t.id);
      deleted += 1;
      console.log("DELETED", t.id, t.name);
    } catch (err) {
      errors.push({ id: t.id, name: t.name, message: err?.message || String(err) });
      console.error("FAILED", t.id, err?.message);
    }
  }

  console.log("\nDone.", deleted, "deleted,", errors.length, "errors.");
  if (errors.length) process.exit(1);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
