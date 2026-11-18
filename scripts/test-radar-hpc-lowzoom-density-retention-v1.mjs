#!/usr/bin/env node
/**
 * HE-002 discrepancy fix: low-zoom city density must survive post-load rAF
 * when the city-aggregation toggle is off. Otherwise HPC Open/Pipeline hotels
 * remain in allHotels / __radarInternalPreviewAddedIds while the map is empty.
 *
 * Deterministic source gate — no Airtable / no network.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const mapping = fs.readFileSync(path.join(ROOT, "public/brand-presence-mapping.js"), "utf8");
const host = fs.readFileSync(
  path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html"),
  "utf8"
);

assert.match(mapping, /const HOTEL_MARKER_MIN_ZOOM\s*=\s*7/);
assert.match(mapping, /function ensureLowZoomCityAggregation\(\)\s*\{/);
assert.match(
  mapping,
  /renderCityAggregationMarkers\(\{\s*force:\s*true\s*\}\)/,
  "ensureLowZoomCityAggregation must force-render"
);

assert.match(
  mapping,
  /const lowZoomNeedsDensity\s*=\s*[\s\S]*?map\.getZoom\(\)\s*<\s*HOTEL_MARKER_MIN_ZOOM/,
  "low-zoom band must imply force inside renderCityAggregationMarkers"
);
assert.match(
  mapping,
  /const force\s*=\s*opts\.force\s*===\s*true\s*\|\|\s*lowZoomNeedsDensity/,
  "force flag must include lowZoomNeedsDensity"
);

assert.match(
  mapping,
  /if\s*\(\s*isCityAggregationEnabled\s*\)\s*\{\s*renderCityAggregationMarkers\(\);\s*\}\s*else if\s*\(\s*earlyCitiesPainted/,
  "post-load rAF must branch: toggle-on render vs earlyCities low-zoom ensure"
);
assert.match(
  mapping,
  /else if\s*\(\s*earlyCitiesPainted[\s\S]*?ensureLowZoomCityAggregation\(\)/,
  "earlyCitiesPainted path must call ensureLowZoomCityAggregation"
);

assert.match(
  mapping,
  /else if\s*\(\s*map\s*&&\s*typeof map\.getZoom\s*===\s*'function'\s*&&\s*map\.getZoom\(\)\s*<\s*HOTEL_MARKER_MIN_ZOOM\s*\)\s*\{\s*\/\/ Keep temporary regional density[\s\S]*?ensureLowZoomCityAggregation\(\)/,
  "turning city-agg toggle off at low zoom must retain density layer"
);

assert.match(host, /brand-presence-mapping\.js\?v=radar-hpc-workspace-principal-1/);

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-hpc-lowzoom-density-retention-v1",
    rootCause: "unforced-renderCityAggregationMarkers-clear-then-bail-after-earlyCities",
    retention: "auto-force-below-HOTEL_MARKER_MIN_ZOOM",
  })
);
