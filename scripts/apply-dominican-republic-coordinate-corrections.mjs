#!/usr/bin/env node
/**
 * Audit + apply verified coordinate corrections for Dominican Republic radar points.
 *
 *   node scripts/apply-dominican-republic-coordinate-corrections.mjs
 *   node scripts/apply-dominican-republic-coordinate-corrections.mjs --apply
 *   node scripts/apply-dominican-republic-coordinate-corrections.mjs --apply --verbose
 */
import "../load-env.js";
import { DEMAND_ANCHORS_FIELDS as DA_F } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import { TRAVEL_INFRASTRUCTURE_FIELDS as TI_F } from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";
import { getDemandAnchorsAirtableConfig, resolveDemandAnchorsTableName } from "../lib/demand-anchors/demand-anchors-base.js";
import {
  getTravelInfrastructureAirtableConfig,
  resolveTravelInfrastructureTableName,
} from "../lib/travel-infrastructure/travel-infrastructure-base.js";
import {
  DR_VERIFIED_COORDINATES,
  coordDistanceKm,
} from "../lib/radar-buildout/dominican-republic-verified-coordinates.js";

const APPLY = process.argv.includes("--apply");
const VERBOSE = process.argv.includes("--verbose");
const MIN_DRIFT_KM = 0.15;
const COUNTRY = "Dominican Republic";
const DELAY = Number(process.env.AIRTABLE_WRITE_DELAY_MS) || 220;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function buildPatch(name, fields, latField, lngField, cityField) {
  const verified = DR_VERIFIED_COORDINATES[name];
  if (!verified) return null;
  const lat = num(fields[latField]);
  const lng = num(fields[lngField]);
  if (lat == null || lng == null) return null;
  const drift = coordDistanceKm(name, lat, lng, verified);
  if (drift == null || drift < MIN_DRIFT_KM) return null;
  const patch = {
    [latField]: verified.latitude,
    [lngField]: verified.longitude,
  };
  if (verified.city && cityField && fields[cityField] !== verified.city) {
    patch[cityField] = verified.city;
  }
  return { patch, driftKm: drift, from: { lat, lng }, to: verified };
}

async function patchTable({ label, cfg, tableName, nameField, latField, lngField, cityField, countryField }) {
  const formula = `{${countryField}} = '${COUNTRY}'`;
  const records = await cfg.base(tableName).select({ filterByFormula: formula }).all();
  const updates = [];

  for (const rec of records) {
    const name = String(rec.fields[nameField] || "").trim();
    const result = buildPatch(name, rec.fields, latField, lngField, cityField);
    if (!result) continue;
    updates.push({ id: rec.id, name, ...result });
  }

  console.log(`\n=== ${label} ===`);
  console.log("Scanned:", records.length);
  console.log("Need coordinate fix (≥" + MIN_DRIFT_KM + " km drift):", updates.length);

  updates.sort((a, b) => b.driftKm - a.driftKm);
  for (const u of updates) {
    console.log(
      `  ${u.name}: ${u.driftKm.toFixed(1)} km | (${u.from.lat}, ${u.from.lng}) → (${u.to.latitude}, ${u.to.longitude})`
    );
    if (VERBOSE) console.log("   ", JSON.stringify(u.patch));
  }

  if (!APPLY) return { updated: 0, failed: 0 };

  let updated = 0;
  let failed = 0;
  for (const u of updates) {
    try {
      await cfg.base(tableName).update(u.id, u.patch, { typecast: true });
      updated += 1;
    } catch (err) {
      failed += 1;
      console.error("FAIL", u.name, err?.message || err);
    }
    await sleep(DELAY);
  }
  console.log(`${label} apply: updated=${updated} failed=${failed}`);
  return { updated, failed };
}

async function main() {
  console.log(APPLY ? "=== APPLY ===" : "=== DRY RUN ===");
  const daCfg = getDemandAnchorsAirtableConfig();
  const tiCfg = getTravelInfrastructureAirtableConfig();
  const daTable = await resolveDemandAnchorsTableName(daCfg.baseId, daCfg.apiKey);
  const tiTable = await resolveTravelInfrastructureTableName(tiCfg.baseId, tiCfg.apiKey);

  const da = await patchTable({
    label: "Demand Anchors",
    cfg: daCfg,
    tableName: daTable,
    nameField: DA_F.name,
    latField: DA_F.lat,
    lngField: DA_F.lng,
    cityField: DA_F.city,
    countryField: DA_F.country,
  });
  const ti = await patchTable({
    label: "Travel Infrastructure",
    cfg: tiCfg,
    tableName: tiTable,
    nameField: TI_F.name,
    latField: TI_F.lat,
    lngField: TI_F.lng,
    cityField: TI_F.city,
    countryField: TI_F.country,
  });

  if (!APPLY) {
    console.log("\nNo writes. Re-run with --apply to update Airtable coordinates.");
  } else {
    console.log("\nTotals: DA updated", da.updated, "TI updated", ti.updated);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
