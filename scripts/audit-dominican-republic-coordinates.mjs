#!/usr/bin/env node
/**
 * Audit DR radar point coordinates vs verified reference map.
 *   node scripts/audit-dominican-republic-coordinates.mjs
 */
import "../load-env.js";
import { fetchDemandAnchorRecords } from "../lib/demand-anchors/airtable-demand-anchors-io.js";
import { fetchTravelInfrastructureRecords } from "../lib/travel-infrastructure/airtable-travel-infrastructure-io.js";
import {
  DR_VERIFIED_COORDINATES,
  coordDistanceKm,
  listUnverifiedDrPointNames,
} from "../lib/radar-buildout/dominican-republic-verified-coordinates.js";

const THRESHOLD_KM = 0.15;

const [da, ti] = await Promise.all([
  fetchDemandAnchorRecords({ country: "Dominican Republic", includeHidden: true }),
  fetchTravelInfrastructureRecords({ country: "Dominican Republic", includeHidden: true }),
]);

const points = [
  ...(da.allPoints || []).map((p) => ({ ...p, table: "Demand Anchors" })),
  ...(ti.allPoints || []).map((p) => ({ ...p, table: "Travel Infrastructure" })),
];

const names = points.map((p) => p.name);
const unverified = listUnverifiedDrPointNames(names);
const drifted = [];
const ok = [];

for (const p of points) {
  const lat = p.latitude;
  const lng = p.longitude;
  const drift = coordDistanceKm(p.name, lat, lng);
  if (drift == null) continue;
  if (drift >= THRESHOLD_KM) {
    drifted.push({ name: p.name, table: p.table, drift: drift.toFixed(2), lat, lng, ...DR_VERIFIED_COORDINATES[p.name] });
  } else {
    ok.push(p.name);
  }
}

console.log("DR coordinate audit");
console.log("Total points:", points.length);
console.log("Verified reference entries:", Object.keys(DR_VERIFIED_COORDINATES).length);
console.log("Unverified (no reference):", unverified.length, unverified.length ? unverified : "");
console.log("Within threshold:", ok.length);
console.log("Drift >= " + THRESHOLD_KM + " km:", drifted.length);

if (drifted.length) {
  console.log("\nDrifted records:");
  for (const d of drifted.sort((a, b) => Number(b.drift) - Number(a.drift))) {
    console.log(`  ${d.drift} km | ${d.table} | ${d.name}`);
    console.log(`    current: (${d.lat}, ${d.lng})`);
    console.log(`    verified: (${d.latitude}, ${d.longitude})`);
  }
  process.exit(1);
}

console.log("\nAll DR coordinates within verified tolerance.");
