/**
 * Aggregate brand footprint metrics from Hotel Property Census (canonical).
 * Affiliation matchers resolve against HPC `Current Brand` (exact).
 * Read-only. Excludes Independent. Legacy Hotel Census is not queried.
 */

import {
  CENSUS_INDEPENDENT_AFFILIATION,
  STATUS_OPEN,
  STATUS_PIPELINE,
} from "./fields.js";
import { exactMatchKey } from "./brand-alias-resolve.js";
import { resolveDealalityRegion } from "./region.js";
import {
  HOTEL_PROPERTY_CENSUS_TABLE,
  getPlatformBase,
} from "./platform-base.js";

const HPC = Object.freeze({
  brand: "Current Brand",
  brandFamily: "Brand Family",
  parentCompany: "Parent Company",
  status: "Radar Status",
  rooms: "Rooms / Keys",
  country: "Country",
  city: "City",
  market: "Market",
  region: "Continent",
  chainScale: "Hotel Class / Segment",
  location: "Asset Context",
  projectPhase: "project_phase",
  dataConfidence: "Data Confidence Tier",
});

function parseRooms(value) {
  const n = parseInt(value, 10);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function normalizeChainScale(raw) {
  const s = exactMatchKey(raw);
  if (!s) return "Unknown";
  return s.replace(/\s+chain\s*$/i, "").trim() || s;
}

function normalizeStatus(raw) {
  const s = exactMatchKey(raw);
  if (!s) return "";
  if (s === "open" || s === "Open") return STATUS_OPEN;
  if (s === "pipeline" || s === "Pipeline") return STATUS_PIPELINE;
  return s;
}

function bumpMix(map, key, hotels, keys) {
  const k = key || "Unknown";
  if (!map[k]) map[k] = { label: k, hotels: 0, keys: 0 };
  map[k].hotels += hotels;
  map[k].keys += keys;
}

/** Open vs pipeline counts per breakdown dimension (country, region, chain, location). */
function bumpBreakdown(map, key, status, hotels, keys) {
  const k = key || "Unknown";
  if (!map[k]) {
    map[k] = { label: k, hotels: 0, keys: 0, pipelineHotels: 0, pipelineKeys: 0 };
  }
  if (status === STATUS_OPEN) {
    map[k].hotels += hotels;
    map[k].keys += keys;
  } else if (status === STATUS_PIPELINE) {
    map[k].pipelineHotels += hotels;
    map[k].pipelineKeys += keys;
  }
}

function mixToArray(map, totalKeys) {
  return Object.values(map)
    .map((row) => ({
      ...row,
      keysPct: totalKeys > 0 ? Math.round((row.keys / totalKeys) * 1000) / 10 : 0,
    }))
    .sort((a, b) => b.keys - a.keys || b.hotels - a.hotels);
}

function readBrand(fields) {
  return (
    exactMatchKey(fields[HPC.brand]) ||
    exactMatchKey(fields[HPC.brandFamily]) ||
    ""
  );
}

/**
 * Fetch HPC rows and aggregate metrics for exact brand matchers.
 * Read-only. Excludes Current Brand = Independent.
 *
 * @param {object} params
 * @param {string[]} params.affiliationMatchers Exact brand strings (from Brand Alias Mapping)
 * @param {string} [params.parentCompany] Optional exact Parent Company filter
 */
export async function aggregateCensusPresenceSummary(params) {
  const base = getPlatformBase();
  if (!base) {
    return { ok: false, error: "Platform base not configured" };
  }

  const matcherSet = new Set(
    (params.affiliationMatchers || []).map((a) => exactMatchKey(a)).filter(Boolean)
  );
  if (matcherSet.size === 0) {
    return { ok: false, error: "No affiliation matchers to query" };
  }

  const parentFilter = exactMatchKey(params.parentCompany);
  const matcherList = [...matcherSet];

  const selectFields = [
    HPC.brand,
    HPC.brandFamily,
    HPC.parentCompany,
    HPC.status,
    HPC.rooms,
    HPC.country,
    HPC.city,
    HPC.market,
    HPC.region,
    HPC.chainScale,
    HPC.location,
  ];

  let records = [];
  const CHUNK = 8;
  for (let i = 0; i < matcherList.length; i += CHUNK) {
    const chunk = matcherList.slice(i, i + CHUNK);
    const orParts = chunk.map((a) => {
      const esc = a.replace(/'/g, "\\'");
      return `OR({${HPC.brand}}='${esc}',{${HPC.brandFamily}}='${esc}')`;
    });
    let formula = `OR(${orParts.join(",")})`;
    if (parentFilter) {
      const escP = parentFilter.replace(/'/g, "\\'");
      formula = `AND(${formula}, OR({${HPC.parentCompany}}='${escP}',{${HPC.brandFamily}}='${escP}'))`;
    }
    // eslint-disable-next-line no-await-in-loop
    const batch = await base(HOTEL_PROPERTY_CENSUS_TABLE)
      .select({ filterByFormula: formula, fields: selectFields, pageSize: 100 })
      .all();
    records = records.concat(batch);
  }

  const seenIds = new Set();
  records = records.filter((r) => {
    if (seenIds.has(r.id)) return false;
    seenIds.add(r.id);
    return true;
  });

  let excludedIndependent = 0;
  let excludedAffiliationMismatch = 0;
  const excludedIncludeInBrandExplorer = 0;

  const openRows = [];
  const pipelineRows = [];

  for (const rec of records) {
    const f = rec.fields || {};
    const affiliation = readBrand(f);

    if (affiliation === CENSUS_INDEPENDENT_AFFILIATION) {
      excludedIndependent += 1;
      continue;
    }
    if (!matcherSet.has(affiliation)) {
      excludedAffiliationMismatch += 1;
      continue;
    }

    const status = normalizeStatus(f[HPC.status]);
    const row = {
      id: rec.id,
      affiliation,
      parentCompany: exactMatchKey(f[HPC.parentCompany] || f[HPC.brandFamily]),
      status,
      rooms: parseRooms(f[HPC.rooms]),
      country: exactMatchKey(f[HPC.country]),
      region: exactMatchKey(f[HPC.region]),
      city: exactMatchKey(f[HPC.city]),
      market: exactMatchKey(f[HPC.market]),
      chainScale: normalizeChainScale(f[HPC.chainScale]),
      locationType: exactMatchKey(f[HPC.location]) || "Unknown",
      projectPhase: exactMatchKey(f[HPC.projectPhase]),
      dataConfidence: null,
    };

    if (status === STATUS_OPEN) openRows.push(row);
    else if (status === STATUS_PIPELINE) pipelineRows.push(row);
  }

  const countryMap = {};
  const regionMap = {};
  const chainMap = {};
  const locationMap = {};
  const phaseMap = {};
  const confidenceMap = {};

  for (const row of openRows) {
    bumpBreakdown(countryMap, row.country || "Unknown", STATUS_OPEN, 1, row.rooms);
    bumpBreakdown(
      regionMap,
      resolveDealalityRegion(row.region, row.country),
      STATUS_OPEN,
      1,
      row.rooms
    );
    bumpBreakdown(chainMap, row.chainScale, STATUS_OPEN, 1, row.rooms);
    bumpBreakdown(locationMap, row.locationType, STATUS_OPEN, 1, row.rooms);
  }

  for (const row of pipelineRows) {
    bumpBreakdown(countryMap, row.country || "Unknown", STATUS_PIPELINE, 1, row.rooms);
    bumpBreakdown(
      regionMap,
      resolveDealalityRegion(row.region, row.country),
      STATUS_PIPELINE,
      1,
      row.rooms
    );
    bumpBreakdown(chainMap, row.chainScale, STATUS_PIPELINE, 1, row.rooms);
    bumpBreakdown(locationMap, row.locationType, STATUS_PIPELINE, 1, row.rooms);
    const phase = row.projectPhase || "Pipeline";
    bumpMix(phaseMap, phase, 1, row.rooms);
  }

  const totalOpenHotels = openRows.length;
  const totalOpenKeys = openRows.reduce((s, r) => s + r.rooms, 0);
  const totalPipelineHotels = pipelineRows.length;
  const totalPipelineKeys = pipelineRows.reduce((s, r) => s + r.rooms, 0);

  return {
    ok: true,
    censusRecordsMatched: records.length,
    excludedIndependent,
    excludedAffiliationMismatch,
    metrics: {
      totalOpenHotels,
      totalOpenKeys,
      totalPipelineHotels,
      totalPipelineKeys,
      countryCount: Object.keys(countryMap).filter((k) => k && k !== "Unknown").length,
      dealalityRegionCount: Object.keys(regionMap).filter((k) => k && k !== "Other").length,
    },
    countryBreakdown: mixToArray(countryMap, totalOpenKeys),
    dealalityRegionBreakdown: mixToArray(regionMap, totalOpenKeys),
    chainScaleMix: mixToArray(chainMap, totalOpenKeys),
    locationTypeMix: mixToArray(locationMap, totalOpenKeys),
    pipelinePhaseMix: mixToArray(phaseMap, totalPipelineKeys),
    dataConfidenceBreakdown: null,
    governance: {
      includeInBrandExplorerFieldPresent: false,
      dataConfidenceFieldPresent: false,
      note: "HPC path — Brand Explorer Include flag not applied (HPC Radar Status Open|Pipeline only)",
    },
    source: {
      base: "AIRTABLE_BASE_ID_ALT",
      table: HOTEL_PROPERTY_CENSUS_TABLE,
      tableId: "tbl9aY5ijiuIzzWam",
      affiliationField: HPC.brand,
      aggregatedAt: new Date().toISOString(),
      legacyHotelCensusRuntimeDependency: false,
    },
    dataConfidenceNotes:
      "Property-level HPC; exact Current Brand / Brand Family match via Brand Alias Mapping; open=Radar Status Open; excludes Independent.",
    excludedIncludeInBrandExplorer,
  };
}
