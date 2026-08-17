#!/usr/bin/env node
/**
 * Cross-check DR demand anchor coords against OpenStreetMap Nominatim.
 *   node scripts/geocode-audit-dominican-republic-demand-anchors.mjs
 */
import "../load-env.js";
import { fetchDemandAnchorRecords } from "../lib/demand-anchors/airtable-demand-anchors-io.js";

const DELAY_MS = 1100;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function haversineKm(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLng = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function buildQuery(p) {
  const name = String(p.name || "")
    .replace(/—/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const city = String(p.city || "").trim();
  return `${name}, ${city}, Dominican Republic`;
}

async function nominatimSearch(q) {
  const url =
    "https://nominatim.openstreetmap.org/search?" +
    new URLSearchParams({
      q,
      format: "json",
      limit: "3",
      countrycodes: "do",
    });
  const res = await fetch(url, {
    headers: { "User-Agent": "deal-capture-proxy/1.0 (radar coordinate audit)" },
  });
  if (!res.ok) throw new Error(`Nominatim ${res.status}`);
  return res.json();
}

const { allPoints } = await fetchDemandAnchorRecords({
  country: "Dominican Republic",
  includeHidden: true,
});
const points = (allPoints || []).sort((a, b) => a.name.localeCompare(b.name));

const results = [];
for (const p of points) {
  const q = buildQuery(p);
  let hits = [];
  try {
    hits = await nominatimSearch(q);
  } catch (err) {
    results.push({ ...p, error: err.message, query: q });
    await sleep(DELAY_MS);
    continue;
  }
  const best = hits[0];
  if (!best) {
    results.push({ ...p, query: q, noHit: true });
  } else {
    const osmLat = Number(best.lat);
    const osmLng = Number(best.lon);
    const drift = haversineKm(p.latitude, p.longitude, osmLat, osmLng);
    results.push({
      name: p.name,
      city: p.city,
      pointType: p.pointType,
      query: q,
      current: { lat: p.latitude, lng: p.longitude },
      osm: { lat: osmLat, lng: osmLng, display: best.display_name, type: best.type, class: best.class },
      driftKm: drift,
      osmHits: hits.length,
    });
  }
  await sleep(DELAY_MS);
}

const flagged = results
  .filter((r) => r.driftKm != null && r.driftKm >= 1)
  .sort((a, b) => b.driftKm - a.driftKm);

console.log("Geocoded", results.length, "demand anchors");
console.log("No OSM hit:", results.filter((r) => r.noHit).length);
console.log("Errors:", results.filter((r) => r.error).length);
console.log("Drift >= 1 km:", flagged.length, "\n");

for (const r of flagged) {
  console.log(
    `${r.driftKm.toFixed(1)} km | ${r.pointType} | ${r.name}`
  );
  console.log(`  current: ${r.current.lat}, ${r.current.lng}`);
  console.log(`  osm:     ${r.osm.lat}, ${r.osm.lng} (${r.osm.type})`);
  console.log(`  osm name: ${r.osm.display?.slice(0, 100)}`);
  console.log("");
}

// Also print 0.5-1km band
const minor = results
  .filter((r) => r.driftKm != null && r.driftKm >= 0.5 && r.driftKm < 1)
  .sort((a, b) => b.driftKm - a.driftKm);
if (minor.length) {
  console.log("--- 0.5–1 km drift ---");
  for (const r of minor) {
    console.log(`${r.driftKm.toFixed(2)} km | ${r.name}`);
  }
}
