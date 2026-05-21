import {
  CENSUS_FIELDS,
  CENSUS_INDEPENDENT_AFFILIATION,
  STATUS_OPEN,
  STATUS_PIPELINE,
} from "./fields.js";
import { exactMatchKey } from "./brand-alias-resolve.js";
import { countryToDealalityRegion } from "./region.js";
import { HOTEL_CENSUS_TABLE, getPlatformBase } from "./platform-base.js";
import {
  getGovernanceFieldAvailability,
  shouldIncludeRowForBrandExplorer,
  governanceMeta,
} from "./census-governance.js";

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

/**
 * Fetch census rows and aggregate metrics for exact affiliation matchers.
 * Read-only. Excludes Affiliation = Independent.
 *
 * @param {object} params
 * @param {string[]} params.affiliationMatchers Exact Affiliation strings
 * @param {string} [params.parentCompany] Optional exact Parent Company filter on census rows
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

  const governance = await getGovernanceFieldAvailability(base);

  const selectFields = [
    CENSUS_FIELDS.affiliation,
    CENSUS_FIELDS.parentCompany,
    CENSUS_FIELDS.status,
    CENSUS_FIELDS.rooms,
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.city,
    CENSUS_FIELDS.market,
    CENSUS_FIELDS.chainScale,
    CENSUS_FIELDS.location,
    CENSUS_FIELDS.projectPhase,
  ];
  if (governance.includeInBrandExplorer) {
    selectFields.push(CENSUS_FIELDS.includeInBrandExplorer);
  }
  if (governance.dataConfidence) {
    selectFields.push(CENSUS_FIELDS.dataConfidence);
  }

  let records = [];
  const CHUNK = 8;
  for (let i = 0; i < matcherList.length; i += CHUNK) {
    const chunk = matcherList.slice(i, i + CHUNK);
    const orParts = chunk.map((a) => {
      const esc = a.replace(/'/g, "\\'");
      return `{${CENSUS_FIELDS.affiliation}}='${esc}'`;
    });
    let formula = `OR(${orParts.join(",")})`;
    if (parentFilter) {
      const escP = parentFilter.replace(/'/g, "\\'");
      formula = `AND(${formula}, {${CENSUS_FIELDS.parentCompany}}='${escP}')`;
    }
    const batch = await base(HOTEL_CENSUS_TABLE)
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
  let excludedIncludeInBrandExplorer = 0;

  const openRows = [];
  const pipelineRows = [];

  for (const rec of records) {
    const f = rec.fields || {};
    const affiliation = exactMatchKey(f[CENSUS_FIELDS.affiliation]);

    if (affiliation === CENSUS_INDEPENDENT_AFFILIATION) {
      excludedIndependent += 1;
      continue;
    }
    if (!matcherSet.has(affiliation)) {
      excludedAffiliationMismatch += 1;
      continue;
    }

    if (
      !shouldIncludeRowForBrandExplorer(
        f,
        CENSUS_FIELDS.includeInBrandExplorer,
        governance.includeInBrandExplorer
      )
    ) {
      excludedIncludeInBrandExplorer += 1;
      continue;
    }

    const status = normalizeStatus(f[CENSUS_FIELDS.status]);
    const row = {
      id: rec.id,
      affiliation,
      parentCompany: exactMatchKey(f[CENSUS_FIELDS.parentCompany]),
      status,
      rooms: parseRooms(f[CENSUS_FIELDS.rooms]),
      country: exactMatchKey(f[CENSUS_FIELDS.country]),
      city: exactMatchKey(f[CENSUS_FIELDS.city]),
      market: exactMatchKey(f[CENSUS_FIELDS.market]),
      chainScale: normalizeChainScale(f[CENSUS_FIELDS.chainScale]),
      locationType: exactMatchKey(f[CENSUS_FIELDS.location]) || "Unknown",
      projectPhase: exactMatchKey(f[CENSUS_FIELDS.projectPhase]),
      dataConfidence: governance.dataConfidence
        ? exactMatchKey(f[CENSUS_FIELDS.dataConfidence]) || "Unknown"
        : null,
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

  function bumpConfidence(rows) {
    if (!governance.dataConfidence) return;
    for (const row of rows) {
      bumpMix(confidenceMap, row.dataConfidence || "Unknown", 1, row.rooms);
    }
  }

  for (const row of openRows) {
    bumpBreakdown(countryMap, row.country || "Unknown", STATUS_OPEN, 1, row.rooms);
    bumpBreakdown(regionMap, countryToDealalityRegion(row.country), STATUS_OPEN, 1, row.rooms);
    bumpBreakdown(chainMap, row.chainScale, STATUS_OPEN, 1, row.rooms);
    bumpBreakdown(locationMap, row.locationType, STATUS_OPEN, 1, row.rooms);
  }

  for (const row of pipelineRows) {
    bumpBreakdown(countryMap, row.country || "Unknown", STATUS_PIPELINE, 1, row.rooms);
    bumpBreakdown(regionMap, countryToDealalityRegion(row.country), STATUS_PIPELINE, 1, row.rooms);
    bumpBreakdown(chainMap, row.chainScale, STATUS_PIPELINE, 1, row.rooms);
    bumpBreakdown(locationMap, row.locationType, STATUS_PIPELINE, 1, row.rooms);
    const phase = row.projectPhase || "Pipeline";
    bumpMix(phaseMap, phase, 1, row.rooms);
  }

  bumpConfidence(openRows);
  bumpConfidence(pipelineRows);

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
    dataConfidenceBreakdown: governance.dataConfidence
      ? mixToArray(confidenceMap, totalOpenKeys + totalPipelineKeys)
      : null,
    governance: governanceMeta(governance),
    source: {
      base: "AIRTABLE_BASE_ID_ALT",
      table: HOTEL_CENSUS_TABLE,
      affiliationField: CENSUS_FIELDS.affiliation,
      aggregatedAt: new Date().toISOString(),
    },
    dataConfidenceNotes:
      "Property-level census; exact Affiliation match via Brand Alias Mapping; open=status Open; excludes Affiliation Independent." +
      (governance.includeInBrandExplorer
        ? " Include in Brand Explorer: exclude only when explicitly false; blank counts."
        : "") +
      (governance.dataConfidence ? " Data Confidence reported, not used as a filter in Phase 1B." : ""),
    excludedIncludeInBrandExplorer,
  };
}
