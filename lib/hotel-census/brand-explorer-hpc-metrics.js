/**
 * P8.10 — Brand Explorer HPC metrics aggregator (shadow / opt-in).
 *
 * Replaces Legacy Hotel Census Affiliation rollups with Hotel Property Census.
 * Match grain: Current Brand / Brand Family against Brand Alias Mapping matchers
 * (same alias strings used for Legacy Affiliation — not a Legacy table read).
 *
 * No STR Number. No Chain Scale copy. No Legacy rooms fill.
 * Rooms: product-governed only; coverage reported.
 * Affiliation: BRANDED_CONFIRMED only in brand footprint; unknowns tracked separately.
 */

import {
  createBrandPresenceBase,
  deriveHpcLifecycleStatus,
  deriveHpcProductRooms,
  HPC_FIELDS,
  HPC_MAP_FIELDS,
  HPC_TABLE,
  isBrandPresenceHpcV2Enabled,
} from "./brand-presence-hpc-adapter.js";
import { exactMatchKey } from "./brand-alias-resolve.js";
import { resolveDealalityRegion } from "./region.js";
import { STATUS_OPEN, STATUS_PIPELINE } from "./fields.js";

export const BE_HPC_ADAPTER = "brand-explorer-hpc-v2";

/** @type {{ hpc: number, legacy: number, silentFallback: number }} */
export const brandExplorerCensusReadCounters = {
  hpc: 0,
  legacy: 0,
  silentFallback: 0,
};

export function resetBrandExplorerCensusReadCounters() {
  brandExplorerCensusReadCounters.hpc = 0;
  brandExplorerCensusReadCounters.legacy = 0;
  brandExplorerCensusReadCounters.silentFallback = 0;
}

export function snapshotBrandExplorerCensusReadCounters() {
  return { ...brandExplorerCensusReadCounters, at: new Date().toISOString() };
}

function escapeFormula(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

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

function bumpMix(map, key, hotels, keys) {
  const k = key || "Unknown";
  if (!map[k]) map[k] = { label: k, hotels: 0, keys: 0 };
  map[k].hotels += hotels;
  map[k].keys += keys;
}

function mixToArray(map, totalKeys) {
  return Object.values(map)
    .map((row) => ({
      ...row,
      keysPct: totalKeys > 0 ? Math.round((row.keys / totalKeys) * 1000) / 10 : 0,
    }))
    .sort((a, b) => b.keys - a.keys || b.hotels - a.hotels);
}

function readText(fields, key) {
  const raw = fields[key];
  if (raw == null || raw === "") return null;
  if (typeof raw === "string") return raw.trim() || null;
  if (Array.isArray(raw)) {
    return (
      raw
        .map((item) => (typeof item === "string" ? item.trim() : item?.name || ""))
        .filter(Boolean)
        .join("; ") || null
    );
  }
  if (typeof raw === "object" && raw.name) return String(raw.name).trim() || null;
  return String(raw).trim() || null;
}

/**
 * Map HPC Affiliation Status → BE footprint eligibility.
 * Brand footprint metrics use BRANDED_CONFIRMED only (never UNKNOWN=Independent).
 */
export function classifyHpcAffiliationForBe(affiliationStatus) {
  const s = exactMatchKey(affiliationStatus);
  if (s === "Independent") {
    return { class: "INDEPENDENT_CONFIRMED", includeInBrandFootprint: false };
  }
  if (s === "Brand-Unconfirmed") {
    return { class: "BRAND_UNCONFIRMED", includeInBrandFootprint: false };
  }
  if (!s || s === "Unknown") {
    return { class: "AFFILIATION_UNKNOWN", includeInBrandFootprint: false };
  }
  if (
    s === "Branded" ||
    s === "Soft-Branded / Collection" ||
    s === "Formerly Branded" ||
    s === "Future / Pipeline"
  ) {
    return { class: "BRANDED_CONFIRMED", includeInBrandFootprint: true };
  }
  return { class: "AFFILIATION_UNKNOWN", includeInBrandFootprint: false };
}

/**
 * Aggregate HPC properties for Brand Explorer censusSummary shape.
 * @param {{ affiliationMatchers: string[], parentCompany?: string|null }} params
 */
export async function aggregateHpcBrandPresenceSummary(params) {
  if (!isBrandPresenceHpcV2Enabled()) {
    return { ok: false, error: "BRAND_PRESENCE_HPC_V2 must be enabled for BE HPC metrics" };
  }
  const base = createBrandPresenceBase();
  if (!base) {
    return { ok: false, error: "Platform base not configured" };
  }

  const matcherSet = new Set(
    (params.affiliationMatchers || []).map((a) => exactMatchKey(a)).filter(Boolean)
  );
  if (matcherSet.size === 0) {
    return { ok: false, error: "No affiliation matchers to query" };
  }

  const matcherList = [...matcherSet];

  let records = [];
  const CHUNK = 6;
  for (let i = 0; i < matcherList.length; i += CHUNK) {
    const chunk = matcherList.slice(i, i + CHUNK);
    const brandOr = chunk.map((a) => {
      const esc = escapeFormula(a);
      return `OR({${HPC_FIELDS.brand}}='${esc}',{${HPC_FIELDS.brandFamily}}='${esc}')`;
    });
    const formula = brandOr.length === 1 ? brandOr[0] : `OR(${brandOr.join(",")})`;
    const batch = await base(HPC_TABLE)
      .select({
        filterByFormula: formula,
        fields: [...HPC_MAP_FIELDS],
        pageSize: 100,
      })
      .all();
    records = records.concat(batch);
  }

  brandExplorerCensusReadCounters.hpc += 1;

  const seen = new Set();
  records = records.filter((r) => {
    if (seen.has(r.id)) return false;
    seen.add(r.id);
    return true;
  });

  let excludedIndependent = 0;
  let excludedAffiliationMismatch = 0;
  let excludedNotBrandedConfirmed = 0;
  let roomsKnownOpen = 0;
  let roomsUnknownOpen = 0;
  let roomsKnownPipeline = 0;
  let roomsUnknownPipeline = 0;
  const affiliationClassCounts = {
    BRANDED_CONFIRMED: 0,
    INDEPENDENT_CONFIRMED: 0,
    BRAND_UNCONFIRMED: 0,
    AFFILIATION_UNKNOWN: 0,
  };

  const openRows = [];
  const pipelineRows = [];

  for (const rec of records) {
    const f = rec.fields || {};
    const currentBrand = exactMatchKey(readText(f, HPC_FIELDS.brand));
    const brandFamily = exactMatchKey(readText(f, HPC_FIELDS.brandFamily));
    const brandHit =
      (currentBrand && matcherSet.has(currentBrand)) ||
      (brandFamily && matcherSet.has(brandFamily));
    if (!brandHit) {
      excludedAffiliationMismatch += 1;
      continue;
    }

    const affStatus = readText(f, HPC_FIELDS.affiliationStatus);
    const affClass = classifyHpcAffiliationForBe(affStatus);
    affiliationClassCounts[affClass.class] =
      (affiliationClassCounts[affClass.class] || 0) + 1;

    if (affClass.class === "INDEPENDENT_CONFIRMED") {
      excludedIndependent += 1;
      continue;
    }
    if (!affClass.includeInBrandFootprint) {
      excludedNotBrandedConfirmed += 1;
      continue;
    }

    const status = deriveHpcLifecycleStatus(f);
    const rooms = deriveHpcProductRooms(f);
    const roomsKnown = rooms != null && Number.isFinite(Number(rooms)) && Number(rooms) > 0;
    const roomsVal = roomsKnown ? Number(rooms) : 0;
    const country = exactMatchKey(readText(f, HPC_FIELDS.country));
    const stateRegion = exactMatchKey(readText(f, HPC_FIELDS.state));
    const city = exactMatchKey(readText(f, HPC_FIELDS.city));
    const dealalityMarket = exactMatchKey(readText(f, HPC_FIELDS.market));

    const row = {
      id: readText(f, HPC_FIELDS.dhl) || `hpc_${rec.id}`,
      dhl_: readText(f, HPC_FIELDS.dhl),
      hpcRecordId: rec.id,
      affiliation: currentBrand || brandFamily || "",
      affiliationClass: affClass.class,
      parentCompany: brandFamily || "",
      status,
      rooms: roomsVal,
      roomsKnown,
      country,
      region: stateRegion,
      city,
      market: dealalityMarket || "",
      chainScale: "Unknown",
      locationType: "Unknown",
      projectPhase: status === STATUS_PIPELINE ? "Pipeline" : "",
      dataConfidence: null,
    };

    if (status === STATUS_OPEN) {
      openRows.push(row);
      if (roomsKnown) roomsKnownOpen += 1;
      else roomsUnknownOpen += 1;
    } else if (status === STATUS_PIPELINE) {
      pipelineRows.push(row);
      if (roomsKnown) roomsKnownPipeline += 1;
      else roomsUnknownPipeline += 1;
    }
  }

  const countryMap = {};
  const regionMap = {};
  const chainMap = {};
  const locationMap = {};
  const phaseMap = {};

  for (const row of openRows) {
    bumpBreakdown(countryMap, row.country || "Unknown", STATUS_OPEN, 1, row.rooms);
    bumpBreakdown(
      regionMap,
      resolveDealalityRegion(row.region, row.country),
      STATUS_OPEN,
      1,
      row.rooms
    );
    bumpBreakdown(chainMap, "Unknown", STATUS_OPEN, 1, row.rooms);
    bumpBreakdown(locationMap, "Unknown", STATUS_OPEN, 1, row.rooms);
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
    bumpBreakdown(chainMap, "Unknown", STATUS_PIPELINE, 1, row.rooms);
    bumpBreakdown(locationMap, "Unknown", STATUS_PIPELINE, 1, row.rooms);
    bumpMix(phaseMap, row.projectPhase || "Pipeline", 1, row.rooms);
  }

  const totalOpenHotels = openRows.length;
  const totalOpenKeys = openRows.reduce((s, r) => s + r.rooms, 0);
  const totalPipelineHotels = pipelineRows.length;
  const totalPipelineKeys = pipelineRows.reduce((s, r) => s + r.rooms, 0);
  const openDenom = Math.max(1, roomsKnownOpen + roomsUnknownOpen);
  const roomsCoveragePctOpen =
    totalOpenHotels > 0 ? Math.round((1000 * roomsKnownOpen) / openDenom) / 10 : null;

  return {
    ok: true,
    censusRecordsMatched: records.length,
    excludedIndependent,
    excludedAffiliationMismatch,
    excludedNotBrandedConfirmed,
    metrics: {
      totalOpenHotels,
      totalOpenKeys,
      totalPipelineHotels,
      totalPipelineKeys,
      countryCount: Object.keys(countryMap).filter((k) => k && k !== "Unknown").length,
      dealalityRegionCount: Object.keys(regionMap).filter((k) => k && k !== "Other").length,
      roomsKnownOpen,
      roomsUnknownOpen,
      roomsKnownPipeline,
      roomsUnknownPipeline,
      roomsCoveragePctOpen,
    },
    countryBreakdown: mixToArray(countryMap, totalOpenKeys),
    dealalityRegionBreakdown: mixToArray(regionMap, totalOpenKeys),
    chainScaleMix: mixToArray(chainMap, totalOpenKeys),
    locationTypeMix: mixToArray(locationMap, totalOpenKeys),
    pipelinePhaseMix: mixToArray(phaseMap, totalPipelineKeys),
    dataConfidenceBreakdown: null,
    affiliationClassCounts,
    governance: {
      includeInBrandExplorerFieldPresent: false,
      dataConfidenceFieldPresent: false,
      note: "HPC path: Production Use / Affiliation Status govern inclusion; no Legacy Include-in-BE field",
    },
    source: {
      base: "AIRTABLE_BASE_ID_ALT",
      table: HPC_TABLE,
      adapter: BE_HPC_ADAPTER,
      censusSource: "hpc",
      hotelSource: "Hotel Property Census",
      affiliationField: HPC_FIELDS.brand,
      brandFamilyField: HPC_FIELDS.brandFamily,
      noLegacyReads: true,
      noStrJoins: true,
      chainScale: "UNKNOWN_SAFE_DEGRADATION",
      rooms: "product_governed_partial",
      aggregatedAt: new Date().toISOString(),
    },
    dataConfidenceNotes:
      "HPC Brand Explorer metrics: match Current Brand / Brand Family to Brand Alias Mapping strings; " +
      "footprint = BRANDED_CONFIRMED only (Independent / Brand-Unconfirmed / Unknown excluded from brand stock); " +
      "Open/Pipeline from HPC lifecycle; rooms = High+official product rooms only (coverage reported); " +
      "Chain Scale absent (Unknown); Dealality region from State/Region + country; no Legacy Hotel Census reads.",
    excludedIncludeInBrandExplorer: 0,
  };
}
