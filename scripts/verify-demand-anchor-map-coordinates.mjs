#!/usr/bin/env node
/**
 * Verify demand anchor coordinates via OSM geocoding; gate map display.
 *
 *   node scripts/verify-demand-anchor-map-coordinates.mjs --country "Dominican Republic"
 *   node scripts/verify-demand-anchor-map-coordinates.mjs --country "Dominican Republic" --apply
 */
import "../load-env.js";
import { DEMAND_ANCHORS_FIELDS as F } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import { getDemandAnchorsAirtableConfig, resolveDemandAnchorsTableName } from "../lib/demand-anchors/demand-anchors-base.js";
import { fetchDemandAnchorRecords } from "../lib/demand-anchors/airtable-demand-anchors-io.js";
import {
  verifyDemandAnchorCoordinates,
  todayIsoDate,
} from "../lib/demand-anchors/coordinate-verification.js";
import { DR_VERIFIED_COORDINATES } from "../lib/radar-buildout/dominican-republic-verified-coordinates.js";

const APPLY = process.argv.includes("--apply");
const countryArg = process.argv.find((a, i) => process.argv[i - 1] === "--country");
const COUNTRY = countryArg || "Dominican Republic";
const DELAY_MS = Number(process.env.NOMINATIM_DELAY_MS) || 1100;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const manualRef = COUNTRY === "Dominican Republic" ? DR_VERIFIED_COORDINATES : {};

const { allPoints, error } = await fetchDemandAnchorRecords({
  country: COUNTRY,
  includeHidden: true,
});
if (error) {
  console.error("Fetch failed:", error);
  process.exit(1);
}

const points = allPoints || [];
console.log(APPLY ? "=== APPLY ===" : "=== DRY RUN ===");
console.log("Country:", COUNTRY);
console.log("Points:", points.length);

const cfg = getDemandAnchorsAirtableConfig();
const tableName = await resolveDemandAnchorsTableName(cfg.baseId, cfg.apiKey);
const verifiedDate = todayIsoDate();

const summary = { verified: 0, corrected: 0, hidden: 0, updated: 0, failed: 0 };
const hiddenList = [];
const correctedList = [];

for (const p of points.sort((a, b) => a.name.localeCompare(b.name))) {
  const result = await verifyDemandAnchorCoordinates(p, { manualReference: manualRef });
  const patch = {
    [F.lat]: result.latitude,
    [F.lng]: result.longitude,
    [F.includeOnRadarMap]: result.includeOnRadarMap,
  };
  if (result.includeOnRadarMap) {
    patch[F.lastVerified] = verifiedDate;
  } else {
    patch[F.lastVerified] = null;
  }

  if (result.status === "verified") summary.verified += 1;
  else if (result.status === "corrected") {
    summary.corrected += 1;
    correctedList.push({
      name: p.name,
      driftKm: result.driftKm?.toFixed(1),
      from: `${p.latitude}, ${p.longitude}`,
      to: `${result.latitude}, ${result.longitude}`,
      reason: result.reason,
    });
  } else {
    summary.hidden += 1;
    hiddenList.push({
      name: p.name,
      reason: result.reason,
      driftKm: result.driftKm != null ? result.driftKm.toFixed(1) : null,
      osm: result.osmDisplay?.slice(0, 80),
    });
  }

  if (APPLY) {
    try {
      await cfg.base(tableName).update(p.id, patch, { typecast: true });
      summary.updated += 1;
    } catch (err) {
      summary.failed += 1;
      console.error("FAIL", p.name, err?.message || err);
    }
  }

  await sleep(DELAY_MS);
}

console.log("\nSummary:", summary);
console.log("Map-visible after pass:", summary.verified + summary.corrected);
console.log("Hidden from map:", summary.hidden);

if (correctedList.length) {
  console.log("\n--- Corrected (" + correctedList.length + ") ---");
  for (const r of correctedList.slice(0, 30)) {
    console.log(`  ${r.name} (${r.driftKm} km) → ${r.to}`);
  }
  if (correctedList.length > 30) console.log(`  ... +${correctedList.length - 30} more`);
}

if (hiddenList.length) {
  console.log("\n--- Hidden from map (" + hiddenList.length + ") ---");
  for (const r of hiddenList) {
    console.log(`  ${r.name} | ${r.reason}${r.driftKm ? " | drift " + r.driftKm + " km" : ""}`);
  }
}

if (!APPLY) {
  console.log("\nNo writes. Re-run with --apply to update Airtable.");
}
