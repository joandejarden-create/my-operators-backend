#!/usr/bin/env node
/**
 * HE-002: Open/Pipeline reconciliation — missing coords, identity collision,
 * city|country aggregation accuracy, filtered recount.
 * Deterministic — no network / no Airtable.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";
import {
  clearRadarHpcStatusCache,
  hotelHasMapCoordinates,
  loadAuthorizedInternalPreviewHotels,
  mergeInternalPreviewHotelsIntoBrandPresenceList,
  buildHotelIdentityKeys,
} from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const mapping = fs.readFileSync(path.join(ROOT, "public/brand-presence-mapping.js"), "utf8");
const host = fs.readFileSync(
  path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html"),
  "utf8"
);

assert.match(mapping, /function buildRadarCityAggregationRows/);
assert.match(mapping, /missingCoordinates/);
assert.match(mapping, /searchable \/ Hotel Explorer/);
assert.match(mapping, /Fill Latitude\/Longitude on Hotel Property Census/);
assert.match(host, /brand-presence-mapping\.js\?v=radar-hpc-missing-coords-searchable-2/);
assert.match(mapping, /lowZoomNeedsDensity/);
assert.match(mapping, /mapAddedIds/);

clearRadarHpcStatusCache();

const RADISSON = "recH9h3McgMNFQEla";
const BETHESDA = "recLuxvwwxID7U2B8";
const CASTILLO = "rec82D9zpB8fede1I";
const SHERATON = "recjhQdAUiSyxfCqE";

function hpcRecord(id, name, status, extra = {}) {
  return {
    id,
    fields: {
      "Property Name": name,
      "Canonical Property Name": name,
      "Radar Status": status,
      City: extra.city || "Unknown",
      Country: extra.country || "Unknown",
      ...(extra.lat != null ? { Latitude: extra.lat } : {}),
      ...(extra.lng != null ? { Longitude: extra.lng } : {}),
      ...(extra.rooms != null ? { "Rooms / Keys": extra.rooms } : {}),
    },
  };
}

const records = [
  hpcRecord(RADISSON, "Radisson Blu Toronto Downtown", "Open", {
    city: "Toronto",
    country: "Canada",
  }),
  hpcRecord(BETHESDA, "Bethesda Marriott", "Open", {
    city: "Bethesda",
    country: "United States",
    lat: 39.016519,
    lng: -77.103463,
    rooms: 407,
  }),
  hpcRecord(CASTILLO, "Castillo Hotel Son Vida", "Open", {
    city: "Palma de Mallorca",
    country: "Spain",
    lat: 39.5928,
    lng: 2.5936,
    rooms: 164,
  }),
  hpcRecord(SHERATON, "Sheraton Mallorca Arabella Golf Hotel", "Open", {
    city: "Palma de Mallorca",
    country: "Spain",
    lat: 39.5912,
    lng: 2.5958,
    rooms: 266,
  }),
  hpcRecord("recCandidate00001", "Hidden Candidate", "Hidden", {
    city: "Toronto",
    country: "Canada",
    lat: 43.65,
    lng: -79.38,
  }),
];

const loaded = await loadAuthorizedInternalPreviewHotels({
  dealalityUser: {
    isAdmin: false,
    workspaceAccess: ["Owner", "Operator"],
    userSource: "AIRTABLE",
  },
  req: { hostname: "localhost", headers: { host: "localhost:8080" } },
  records,
  bypassCache: true,
});

assert.equal(loaded.diagnostics.openCount, 4, "four Open status rows (Hidden excluded)");
assert.equal(loaded.diagnostics.pipelineCount, 0);
assert.equal(loaded.hotels.length, 4, "searchEligible includes missing-coord Open rows");
assert.equal(loaded.hotelsMapEligible.length, 3, "mapEligible excludes missing coords");
assert.equal(loaded.diagnostics.missingCoordinates.length, 1);
assert.equal(loaded.diagnostics.missingCoordinates[0].id, RADISSON);
assert.ok(loaded.hotels.some((h) => h.id === RADISSON), "Radisson remains searchable");
assert.ok(loaded.hotels.find((h) => h.id === RADISSON)?.radarMapEligible === false);
assert.ok(loaded.hotelsMapEligible.every((h) => hotelHasMapCoordinates(h)));
assert.ok(!loaded.hotelsMapEligible.some((h) => h.id === RADISSON));

// Identity collision: existing census row with same geo fingerprint keeps census id
const existing = [
  {
    id: "recCensusBethesda01",
    name: "Bethesda Marriott",
    lat: 39.016519,
    lng: -77.103463,
    city: "Bethesda",
    country: "United States",
    status: "Open",
  },
];
const merge = mergeInternalPreviewHotelsIntoBrandPresenceList(existing, loaded.hotels);
assert.ok(merge.duplicatesReconciled.some((d) => d.hpcId === BETHESDA));
assert.ok(!merge.addedIds.includes(BETHESDA));
assert.ok(merge.addedIds.includes(CASTILLO));
assert.ok(merge.addedIds.includes(SHERATON));
assert.ok(merge.addedIds.includes(RADISSON), "missing-coord hotel merges for search");
assert.ok(merge.mapAddedIds.includes(CASTILLO));
assert.ok(!merge.mapAddedIds.includes(RADISSON), "missing-coord hotel not map-added");
assert.equal(
  merge.hotels.filter((h) => /bethesda/i.test(h.name)).length,
  1,
  "Bethesda counted once after collision"
);

// Search / focus contract: Radisson matchable and Explorer-openable without coords
function hotelText(v) {
  return v == null ? "" : String(v);
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
const radissonRow = merge.hotels.find((h) => h.id === RADISSON);
assert.ok(radissonRow);
assert.ok(hotelMatchesSearchTerm(radissonRow, "Radisson Blu Toronto"));
assert.ok(hotelMatchesSearchTerm(radissonRow, RADISSON));
assert.equal(radissonRow.radarMapEligible, false);

// City aggregation: country-keyed; Palma total=2; Open tallies separate from Candidate
function buildRadarCityAggregationRows(sourceHotels) {
  const cityGroups = {};
  for (const hotel of sourceHotels || []) {
    const city = String(hotel.city || "").trim();
    const country = String(hotel.country || "").trim();
    if (!city) continue;
    const cityKey = city + "|" + (country || "Unknown Country");
    if (!cityGroups[cityKey]) {
      cityGroups[cityKey] = {
        city,
        country: country || "Unknown Country",
        totalHotels: 0,
        openHotels: 0,
        pipelineHotels: 0,
        candidateHotels: 0,
        includesInternalPreview: false,
        lat: null,
        lng: null,
      };
    }
    const row = cityGroups[cityKey];
    row.totalHotels += 1;
    if (hotel.internalPreviewOnly) row.includesInternalPreview = true;
    const status = String(hotel.status || "").toLowerCase();
    if (status === "open") row.openHotels += 1;
    else if (status === "pipeline") row.pipelineHotels += 1;
    else if (status === "candidate") row.candidateHotels += 1;
    const lat = Number(hotel.lat);
    const lng = Number(hotel.lng);
    if (Number.isFinite(lat) && Number.isFinite(lng) && (lat !== 0 || lng !== 0)) {
      if (!Number.isFinite(row.lat)) {
        row.lat = lat;
        row.lng = lng;
      }
    }
  }
  return Object.values(cityGroups);
}

const pool = [
  ...merge.hotels,
  {
    id: "recParisOpen00001",
    name: "Paris Open Hotel",
    city: "Paris",
    country: "France",
    status: "Open",
    lat: 48.85,
    lng: 2.35,
  },
  {
    id: "recParisTX0000001",
    name: "Paris Texas Inn",
    city: "Paris",
    country: "United States",
    status: "Open",
    lat: 33.66,
    lng: -95.55,
  },
  {
    id: "recCandidatePalma1",
    name: "Palma Candidate",
    city: "Palma de Mallorca",
    country: "Spain",
    status: "Candidate",
    lat: 39.57,
    lng: 2.65,
  },
];

const rows = buildRadarCityAggregationRows(pool);
const palma = rows.find((r) => r.city === "Palma de Mallorca" && r.country === "Spain");
assert.ok(palma);
assert.equal(palma.totalHotels, 3, "Palma total includes Open HPC + Candidate");
assert.equal(palma.openHotels, 2);
assert.equal(palma.candidateHotels, 1);
assert.notEqual(palma.openHotels, palma.totalHotels, "do not silently mix Open-only with totals");

const parisFR = rows.find((r) => r.city === "Paris" && r.country === "France");
const parisUS = rows.find((r) => r.city === "Paris" && r.country === "United States");
assert.ok(parisFR && parisUS, "same city name in different countries stay separate");
assert.equal(parisFR.totalHotels, 1);
assert.equal(parisUS.totalHotels, 1);

// Filtered aggregate recalculation: status Open filter shrinks city totals
const openOnly = pool.filter((h) => String(h.status) === "Open");
const openRows = buildRadarCityAggregationRows(openOnly);
const palmaOpen = openRows.find((r) => r.city === "Palma de Mallorca");
assert.equal(palmaOpen.totalHotels, 2);
assert.equal(palmaOpen.openHotels, 2);
assert.equal(palmaOpen.candidateHotels, 0);

// Identity keys for Radisson without coords still yield id key only
const radissonKeys = [...buildHotelIdentityKeys({ id: RADISSON, name: "Radisson Blu Toronto Downtown", lat: 0, lng: 0 })];
assert.ok(radissonKeys.includes(`id:${RADISSON}`));
assert.ok(!radissonKeys.some((k) => k.startsWith("geo:")), "no geo key without coordinates");

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-hpc-open-pipeline-reconciliation-v1",
    radissonStage: "MISSING_COORDINATES_SEARCHABLE",
    openStatus: 4,
    searchEligible: 4,
    mapEligible: 3,
    missingCoordinates: 1,
    cityCountrySeparation: true,
    filteredRecount: true,
  })
);
