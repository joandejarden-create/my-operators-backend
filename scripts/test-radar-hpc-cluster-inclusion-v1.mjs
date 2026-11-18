#!/usr/bin/env node
/**
 * Gate: authorized HPC hotels remain visible in Radar low-zoom city aggregates
 * and continue to participate in MarkerCluster rebuilds after merge.
 *
 * Free / deterministic — no Airtable, no network.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const MAPPING = path.join(ROOT, "public/brand-presence-mapping.js");
const HOST = path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html");

const mapping = fs.readFileSync(MAPPING, "utf8");
const host = fs.readFileSync(HOST, "utf8");

assert.match(
  mapping,
  /includesInternalPreview/,
  "city aggregation rows must track internal-preview membership"
);
assert.match(
  mapping,
  /internalPreviewOnly\)\s*row\.includesInternalPreview\s*=\s*true/,
  "internal-preview hotels must flag their city aggregate row"
);
assert.match(
  mapping,
  /if\s*\(\s*row\s*&&\s*row\.includesInternalPreview\s*\)\s*return\s*true/,
  "city aggregate filter must bypass min-size for internal-preview cities"
);
assert.match(
  mapping,
  /mergeAuthorizedInternalPreviewHotels/,
  "HPC merge must remain wired before displayHotels"
);
assert.match(
  mapping,
  /HOTEL_MARKER_MIN_ZOOM\s*=\s*7/,
  "low-zoom city-aggregate path remains the wide-zoom cluster surface"
);
assert.match(
  mapping,
  /ensureLowZoomCityAggregation/,
  "wide zoom must rebuild city aggregates (not leave census-only server paint)"
);
assert.match(
  mapping,
  /markersCluster\.addLayers/,
  "MarkerCluster rebuild path must still add layers for medium/close zoom"
);
assert.doesNotMatch(
  mapping,
  /includesInternalPreview\s*=\s*true\s*;\s*\/\/\s*always/,
  "do not force every city row to bypass the filter"
);

assert.match(
  host,
  /brand-presence-mapping\.js\?v=radar-hpc-auth-parity-1/,
  "Radar host must cache-bust mapping after auth-parity + density fixes"
);
assert.match(
  mapping,
  /lowZoomNeedsDensity/,
  "renderCityAggregationMarkers must auto-force below HOTEL_MARKER_MIN_ZOOM"
);
assert.match(
  mapping,
  /earlyCitiesPainted\s*&&\s*[\s\S]*?ensureLowZoomCityAggregation\(\)/,
  "post-load rAF must re-force low-zoom density after early city paint (not clear-then-bail)"
);
assert.doesNotMatch(
  mapping,
  /if\s*\(\s*isCityAggregationEnabled\s*\|\|\s*earlyCitiesPainted\s*\)\s*renderCityAggregationMarkers\(\)/,
  "unforced earlyCitiesPainted renderCityAggregationMarkers call must not remain"
);

// Pure filter regression (mirrors client rule).
function cityRowPassesAggFilter(row, token) {
  if (row && row.includesInternalPreview) return true;
  const raw = String(token || "hotels:3").trim();
  const parts = raw.split(":");
  const mode = parts[0] || "hotels";
  const min = Math.max(0, parseInt(parts[1], 10) || 0);
  if (mode === "rooms") return row.totalRooms >= min;
  if (mode === "open") return row.openHotels >= min;
  if (mode === "pipeline") return row.pipelineHotels >= min;
  return row.totalHotels >= Math.max(1, min || 1);
}

const bethAlone = {
  city: "Bethesda",
  totalHotels: 1,
  totalRooms: 407,
  openHotels: 1,
  pipelineHotels: 0,
  includesInternalPreview: true,
};
const rockvilleCensus = {
  city: "Rockville",
  totalHotels: 2,
  totalRooms: 300,
  openHotels: 2,
  pipelineHotels: 0,
  includesInternalPreview: false,
};
const miamiCensus = {
  city: "Miami",
  totalHotels: 12,
  totalRooms: 4000,
  openHotels: 10,
  pipelineHotels: 2,
  includesInternalPreview: false,
};

assert.equal(
  cityRowPassesAggFilter(bethAlone, "hotels:3"),
  true,
  "Bethesda HPC-only city must render under default hotels:3"
);
assert.equal(
  cityRowPassesAggFilter(rockvilleCensus, "hotels:3"),
  false,
  "census cities below threshold stay hidden"
);
assert.equal(
  cityRowPassesAggFilter(miamiCensus, "hotels:3"),
  true,
  "legacy census cities at/above threshold still render"
);

// Unauthorized / public pool: no internalPreviewOnly flag → Bethesda-sized city stays hidden
const bethPublicShaped = { ...bethAlone, includesInternalPreview: false };
assert.equal(
  cityRowPassesAggFilter(bethPublicShaped, "hotels:3"),
  false,
  "without internal-preview flag, singleton Bethesda city remains excluded"
);

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-hpc-cluster-inclusion-v1",
    wideZoomSurface: "city-aggregate",
    mediumZoomSurface: "leaflet-markercluster",
    bethesdaBypass: true,
  })
);
