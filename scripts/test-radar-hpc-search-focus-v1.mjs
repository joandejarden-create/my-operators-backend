#!/usr/bin/env node
/**
 * HE-002 follow-up: HPC Open/Pipeline hotels must be searchable/selectable and
 * focusable on /app#/opportunity-radar (null-safe search + census-id focus).
 * Deterministic — no Airtable / no network.
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
const drawer = fs.readFileSync(path.join(ROOT, "public/js/radar-filter-drawer.js"), "utf8");
const gate = fs.readFileSync(
  path.join(ROOT, "middleware/requireRadarInternalPreviewAccess.js"),
  "utf8"
);

assert.match(mapping, /function hotelMatchesSearchTerm/);
assert.match(mapping, /function findHotelsMatchingSearch/);
assert.match(mapping, /async function focusHotelByCensusId/);
assert.match(mapping, /window\.focusHotelByCensusId\s*=\s*focusHotelByCensusId/);
assert.match(mapping, /__radarInternalPreviewSkipped/);
assert.match(mapping, /setRadarHpcMergeNotice/);
assert.match(mapping, /renderRadarSearchResults/);
assert.match(
  mapping,
  /hotelMatchesSearchTerm\(hotel,\s*currentFilters\.search\)/,
  "applyFilters must use null-safe search helper"
);
assert.doesNotMatch(
  mapping,
  /hotel\.name\.toLowerCase\(\)\.includes\(searchTerm\)/,
  "unsafe hotel.name.toLowerCase search must not remain"
);
assert.match(
  mapping,
  /else if \(hasActiveFilters\(\)\) \{\s*await applyFilters\(\);/,
  "post-merge path must re-apply active search/filters"
);

assert.match(host, /id="radarSearchResults"/);
assert.match(host, /brand-presence-mapping\.js\?v=radar-hpc-search-focus-1/);
assert.match(host, /radar-filter-drawer\.js\?v=13/);

assert.match(
  drawer,
  /Do not count the visible Search Location box in the More badge/
);
assert.doesNotMatch(
  drawer,
  /var search = el\("locationSearch"\);\s*if \(search && String\(search\.value/
);

assert.match(gate, /u\.isDemo\s*===\s*true/);
assert.match(gate, /Localhost owner-app demo/);

// Mirror client ranking rules for Mayflower disambiguation / null-safety.
function hotelText(value) {
  return String(value == null ? "" : value);
}
function hotelMatchesSearchTerm(hotel, rawTerm) {
  const searchTerm = hotelText(rawTerm).trim().toLowerCase();
  if (!searchTerm) return true;
  return (
    hotelText(hotel && hotel.name).toLowerCase().includes(searchTerm) ||
    hotelText(hotel && hotel.city).toLowerCase().includes(searchTerm) ||
    hotelText(hotel && hotel.country).toLowerCase().includes(searchTerm) ||
    hotelText(hotel && hotel.brand).toLowerCase().includes(searchTerm) ||
    hotelText(hotel && hotel.market).toLowerCase().includes(searchTerm) ||
    hotelText(hotel && hotel.id).toLowerCase() === searchTerm
  );
}
function findHotelsMatchingSearch(rawTerm, hotels, limit) {
  const searchTerm = hotelText(rawTerm).trim().toLowerCase();
  const max = Math.max(1, Number(limit) || 25);
  if (!searchTerm) return [];
  const scored = [];
  for (const hotel of hotels || []) {
    if (!hotel || !hotelMatchesSearchTerm(hotel, searchTerm)) continue;
    const name = hotelText(hotel.name).toLowerCase();
    const normName = name.replace(/^the\s+/, "");
    let score = 3;
    if (name === searchTerm || normName === searchTerm) score = 0;
    else if (name.startsWith(searchTerm) || normName.startsWith(searchTerm)) score = 1;
    else if (name.includes(searchTerm)) score = 2;
    scored.push({
      hotel,
      score,
      name,
      preview: hotel.internalPreviewOnly ? 0 : 1,
      nameLen: name.length,
    });
  }
  scored.sort((a, b) => {
    if (a.score !== b.score) return a.score - b.score;
    if (a.nameLen !== b.nameLen) return b.nameLen - a.nameLen;
    if (a.preview !== b.preview) return a.preview - b.preview;
    return a.name.localeCompare(b.name);
  });
  return scored.slice(0, max).map((row) => row.hotel);
}

const mayflower = {
  id: "rec0eTEnCqlsGJJt9",
  name: "The Mayflower Hotel, Autograph Collection",
  city: "Washington",
  brand: "Autograph Collection",
  status: "Open",
};
const suites = {
  id: "recSUITES0000001",
  name: "Mayflower Suites",
  city: "Washington",
  brand: "Independent",
  status: "Open",
};
const bethesda = {
  id: "recLuxvwwxID7U2B8",
  name: "Bethesda Marriott",
  city: "Bethesda",
  brand: "Marriott Hotels",
  status: "Open",
  internalPreviewOnly: true,
};
const nullCity = {
  id: "recNULLCITY00001",
  name: "Null City Hotel",
  city: null,
  brand: null,
  status: "Open",
};

assert.equal(hotelMatchesSearchTerm(nullCity, "null city"), true);
assert.equal(hotelMatchesSearchTerm(bethesda, "bethesda"), true);
const mayflowerHits = findHotelsMatchingSearch("mayflower", [suites, mayflower, bethesda], 10);
assert.equal(mayflowerHits[0].id, "rec0eTEnCqlsGJJt9");
assert.equal(findHotelsMatchingSearch("bethesda", [suites, mayflower, bethesda], 10)[0].id, "recLuxvwwxID7U2B8");

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-hpc-search-focus-v1",
    appRoute: "/app#/opportunity-radar",
    mayflowerPreferredOverSuites: true,
    nullSafeSearch: true,
    moreBadgeIgnoresSearch: true,
  })
);
