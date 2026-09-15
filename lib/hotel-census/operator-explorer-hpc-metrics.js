/**
 * P8.12 — Operator Explorer HPC footprint adapter (shadow / opt-in).
 *
 * Path: Operator Master ID → canonical hotel↔operator relationships (JSON store)
 *       → HPC by dhl_ → footprint metrics.
 *
 * No Legacy Hotel Census reads on HPC path.
 * No Legacy Management Company promotion.
 * Rooms: product-governed partial or suppressed.
 * Chain Scale: SAFE_DEGRADE (Unknown).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createBrandPresenceBase,
  deriveHpcLifecycleStatus,
  deriveHpcProductRooms,
  HPC_FIELDS,
  HPC_MAP_FIELDS,
  HPC_TABLE,
  isBrandPresenceHpcV2Enabled,
} from "./brand-presence-hpc-adapter.js";
import { countryToDealalityRegion, resolveDealalityRegion } from "./region.js";
import { STATUS_OPEN, STATUS_PIPELINE } from "./fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
/** P8.12B shared runtime store (OE + Operator Intelligence). P8.12 path remains fallback. */
const DEFAULT_REL_PATH = path.resolve(
  __dirname,
  "../../data/operator-intelligence/operator-hotel-relationships.json"
);
const FALLBACK_REL_PATH = path.resolve(
  __dirname,
  "../../data/hotel-census-v2/p812-operator-explorer/hotel-operator-relationships-v1.json"
);

function normalizeRelationshipRow(r) {
  if (!r || typeof r !== "object") return null;
  const operatorId = r.operatorId || r.operator_id || null;
  const hotel_dhl = r.hotel_dhl || null;
  const relationshipType =
    r.relationshipType || r.relationship_type || "CURRENT_OPERATOR";
  const confidence = r.confidence || "HIGH";
  const statusRaw = r.status || r.relationship_status || "";
  const status =
    statusRaw === "CURRENT" ||
    statusRaw === "CURRENT_CONFIRMED" ||
    relationshipType === "CURRENT_OPERATOR" ||
    relationshipType === "PREOPENING_OPERATOR" ||
    relationshipType === "HOTEL_OPERATOR"
      ? "CURRENT_CONFIRMED"
      : statusRaw || "UNKNOWN";
  return {
    ...r,
    operatorId,
    hotel_dhl,
    relationshipType,
    confidence,
    status,
    operatorCanonicalName: r.operatorCanonicalName || r.canonicalName || null,
  };
}

function normalizeRelationshipStore(raw) {
  const relationships = (raw.relationships || [])
    .map(normalizeRelationshipRow)
    .filter(Boolean);
  const operators = (raw.operators || []).map((o) => ({
    ...o,
    operatorId: o.operatorId || o.operator_id,
    canonicalName: o.canonicalName || o.operatorCanonicalName || o.name,
  }));
  return { ...raw, relationships, operators };
}

export const OE_HPC_ADAPTER = "operator-explorer-hpc-v2";

/** @type {{ hpc: number, legacy: number, relationshipStore: number, silentFallback: number }} */
export const operatorExplorerCensusReadCounters = {
  hpc: 0,
  legacy: 0,
  relationshipStore: 0,
  silentFallback: 0,
};

export function resetOperatorExplorerCensusReadCounters() {
  operatorExplorerCensusReadCounters.hpc = 0;
  operatorExplorerCensusReadCounters.legacy = 0;
  operatorExplorerCensusReadCounters.relationshipStore = 0;
  operatorExplorerCensusReadCounters.silentFallback = 0;
}

export function snapshotOperatorExplorerCensusReadCounters() {
  return { ...operatorExplorerCensusReadCounters, at: new Date().toISOString() };
}

let _relStore = null;

export function loadHotelOperatorRelationshipStore(
  storePath = process.env.OE_HOTEL_OPERATOR_RELATIONSHIP_STORE || DEFAULT_REL_PATH
) {
  if (_relStore) return _relStore;
  try {
    let resolved = storePath;
    if (!fs.existsSync(resolved) && resolved === DEFAULT_REL_PATH && fs.existsSync(FALLBACK_REL_PATH)) {
      resolved = FALLBACK_REL_PATH;
    }
    if (!fs.existsSync(resolved)) {
      _relStore = Object.freeze({ operators: [], relationships: [], stats: {} });
      return _relStore;
    }
    const raw = JSON.parse(fs.readFileSync(resolved, "utf8"));
    const base = raw && typeof raw === "object" ? raw : { operators: [], relationships: [] };
    _relStore = Object.freeze(normalizeRelationshipStore(base));
    return _relStore;
  } catch (err) {
    console.error("[oe-hpc] failed to load relationship store:", err?.message || err);
    _relStore = Object.freeze({ operators: [], relationships: [], stats: {} });
    return _relStore;
  }
}

export function clearHotelOperatorRelationshipStoreCache() {
  _relStore = null;
}

function escapeFormula(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
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

const REGION_LABEL_TO_GEO_ID = {
  "north america": "na",
  "caribbean & latin america": "cala",
  europe: "eu",
  "middle east & africa": "mea",
  "asia pacific": "apac",
};

function regionLabelToGeoId(regionLabel) {
  const key = String(regionLabel || "").toLowerCase().replace(/\s+/g, " ").trim();
  return REGION_LABEL_TO_GEO_ID[key] || null;
}

function brandKeyFromName(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/g, "");
}

/**
 * Current HIGH product relationships for an Operator Master id.
 */
export function listCurrentOperatorHotelDhls(operatorMasterId, store = loadHotelOperatorRelationshipStore()) {
  const id = String(operatorMasterId || "").trim();
  if (!id) return [];
  operatorExplorerCensusReadCounters.relationshipStore += 1;
  return (store.relationships || []).filter((r) => {
    if (r.operatorId !== id) return false;
    if (r.status !== "CURRENT_CONFIRMED") return false;
    if (r.confidence !== "HIGH") return false;
    if (!r.hotel_dhl || !String(r.hotel_dhl).startsWith("dhl_")) return false;
    // OE current footprint = CURRENT_OPERATOR only (PREOPENING counted separately in pipeline metrics).
    const rt = r.relationshipType || "CURRENT_OPERATOR";
    return (
      rt === "CURRENT_OPERATOR" ||
      rt === "HOTEL_OPERATOR" ||
      rt === "PREOPENING_OPERATOR"
    );
  });
}

/**
 * Aggregate OE footprint from HPC via relationship store (no Legacy Census).
 * @param {{ masterId?: string, prefill?: object }} opts
 */
export async function buildOperatorHpcCensusFootprint(opts = {}) {
  if (!isBrandPresenceHpcV2Enabled()) {
    return { ok: false, reason: "BRAND_PRESENCE_HPC_V2_required", managementCompany: "" };
  }
  const masterId = String(opts.masterId || "").trim();
  if (!masterId) {
    return { ok: false, reason: "master_id_required", managementCompany: "" };
  }

  // Arbor-style: no census overlay
  if (masterId === "recF5Z87OAqFgndoq") {
    return {
      ok: false,
      reason: "census_footprint_disabled_for_master",
      managementCompany: "",
      hpcPath: true,
      adapter: OE_HPC_ADAPTER,
      footprintAvailabilityState: "CENSUS_DISABLED",
      footprintPresentation: {
        primaryLabel: null,
        footprintAvailabilityState: "CENSUS_DISABLED",
        portfolioCoverage: "Unknown",
        footprintClass: "CENSUS_DISABLED",
        doNotLabelAsTotalPortfolio: true,
        publicCopyNote: "Census footprint module disabled for this operator profile",
        suppressFootprintMetrics: true,
      },
    };
  }

  const rels = listCurrentOperatorHotelDhls(masterId);
  const store = loadHotelOperatorRelationshipStore();
  const opMeta = (store.operators || []).find((o) => o.operatorId === masterId);

  if (rels.length === 0) {
    return {
      ok: false,
      reason: "no_confirmed_operator_relationships",
      managementCompany: opMeta?.canonicalName || "",
      hpcPath: true,
      adapter: OE_HPC_ADAPTER,
      footprintAvailabilityState: "INSUFFICIENT_CONFIRMED_DATA",
      footprintPresentation: {
        primaryLabel: "Confirmed properties",
        operatingLabel: "Confirmed operating hotels",
        pipelineLabel: "Confirmed pipeline hotels",
        portfolioCoverage: "Unknown",
        footprintClass: "INSUFFICIENT",
        footprintAvailabilityState: "INSUFFICIENT_CONFIRMED_DATA",
        doNotLabelAsTotalPortfolio: true,
        suppressFootprintMetrics: true,
        publicCopyNote: "Verified property footprint not yet available",
      },
      relationshipCoverage: {
        confirmedHotels: 0,
        note: "No HIGH CURRENT hotel↔operator relationships in canonical store",
        portfolioCoverage: "Unknown",
      },
      source: {
        censusSource: "hpc",
        hotelSource: "Hotel Property Census",
        adapter: OE_HPC_ADAPTER,
        noLegacyReads: true,
        noLegacyManagementCompany: true,
      },
    };
  }

  const base = createBrandPresenceBase();
  if (!base) {
    return { ok: false, reason: "platform_base_missing", hpcPath: true };
  }

  const dhlList = [...new Set(rels.map((r) => r.hotel_dhl))];
  const records = [];
  const CHUNK = 8;
  for (let i = 0; i < dhlList.length; i += CHUNK) {
    const chunk = dhlList.slice(i, i + CHUNK);
    const or = chunk.map((d) => `{${HPC_FIELDS.dhl}}='${escapeFormula(d)}'`).join(",");
    const formula = chunk.length === 1 ? or : `OR(${or})`;
    const batch = await base(HPC_TABLE)
      .select({ filterByFormula: formula, fields: [...HPC_MAP_FIELDS], pageSize: 100 })
      .all();
    records.push(...batch);
  }
  operatorExplorerCensusReadCounters.hpc += 1;

  const byDhl = new Map();
  for (const rec of records) {
    const dhl = readText(rec.fields || {}, HPC_FIELDS.dhl);
    if (dhl) byDhl.set(dhl, rec);
  }

  const byBrand = new Map();
  const byGeo = new Map();
  let totalExistingHotels = 0;
  let totalExistingRooms = 0;
  let totalPipelineHotels = 0;
  let totalPipelineRooms = 0;
  let roomsKnownOpen = 0;
  let roomsUnknownOpen = 0;
  let matched = 0;
  let missingHpc = 0;

  for (const rel of rels) {
    const rec = byDhl.get(rel.hotel_dhl);
    if (!rec) {
      missingHpc += 1;
      continue;
    }
    matched += 1;
    const f = rec.fields || {};
    const status = deriveHpcLifecycleStatus(f);
    if (status !== STATUS_OPEN && status !== STATUS_PIPELINE) continue;

    const brand = readText(f, HPC_FIELDS.brand) || "Independent";
    const roomsRaw = deriveHpcProductRooms(f);
    const roomsKnown = roomsRaw != null && Number(roomsRaw) > 0;
    const rooms = roomsKnown ? Number(roomsRaw) : 0;
    const isOpen = status === STATUS_OPEN;

    if (!byBrand.has(brand)) {
      byBrand.set(brand, {
        brand_name: brand,
        brand_key: brandKeyFromName(brand),
        existing_properties: 0,
        existing_rooms: 0,
        pipeline_properties: 0,
        pipeline_rooms: 0,
      });
    }
    const brandRow = byBrand.get(brand);
    if (isOpen) {
      brandRow.existing_properties += 1;
      brandRow.existing_rooms += rooms;
      totalExistingHotels += 1;
      totalExistingRooms += rooms;
      if (roomsKnown) roomsKnownOpen += 1;
      else roomsUnknownOpen += 1;
    } else {
      brandRow.pipeline_properties += 1;
      brandRow.pipeline_rooms += rooms;
      totalPipelineHotels += 1;
      totalPipelineRooms += rooms;
    }

    const country = readText(f, HPC_FIELDS.country);
    const state = readText(f, HPC_FIELDS.state);
    const regionLabel =
      resolveDealalityRegion(state, country) || countryToDealalityRegion(country);
    const geoId = regionLabelToGeoId(regionLabel);
    if (geoId) {
      if (!byGeo.has(geoId)) {
        byGeo.set(geoId, {
          existing_hotels: 0,
          existing_rooms: 0,
          pipeline_hotels: 0,
          pipeline_rooms: 0,
        });
      }
      const g = byGeo.get(geoId);
      if (isOpen) {
        g.existing_hotels += 1;
        g.existing_rooms += rooms;
      } else {
        g.pipeline_hotels += 1;
        g.pipeline_rooms += rooms;
      }
    }
  }

  const brandsPortfolioDetail = [...byBrand.values()].sort(
    (a, b) =>
      b.existing_properties +
        b.pipeline_properties -
        (a.existing_properties + a.pipeline_properties) ||
      b.existing_rooms + b.pipeline_rooms - (a.existing_rooms + a.pipeline_rooms)
  );

  const geoFields = {};
  for (const [geoId, g] of byGeo.entries()) {
    geoFields[`geo_${geoId}_existing_hotels`] = String(g.existing_hotels);
    geoFields[`geo_${geoId}_existing_rooms`] = String(g.existing_rooms);
    geoFields[`geo_${geoId}_pipeline_hotels`] = String(g.pipeline_hotels);
    geoFields[`geo_${geoId}_pipeline_rooms`] = String(g.pipeline_rooms);
  }
  if (totalExistingHotels + totalPipelineHotels > 0) {
    geoFields.geo_total_existing_hotels = String(totalExistingHotels);
    geoFields.geo_total_existing_rooms = String(totalExistingRooms);
    geoFields.geo_total_pipeline_hotels = String(totalPipelineHotels);
    geoFields.geo_total_pipeline_rooms = String(totalPipelineRooms);
  }

  const openDenom = Math.max(1, roomsKnownOpen + roomsUnknownOpen);
  const roomsCoveragePctOpen =
    totalExistingHotels > 0
      ? Math.round((1000 * roomsKnownOpen) / openDenom) / 10
      : null;
  const suppressRooms = roomsCoveragePctOpen == null || roomsCoveragePctOpen < 95;

  const totalHotels = totalExistingHotels + totalPipelineHotels;

  // P8.12D — footprint availability contract
  const coverageMeta = loadOperatorCoverageMeta(masterId, store);
  const portfolioCoverage = coverageMeta.portfolioCoverage || "Partial";
  const footprintReadyClass = coverageMeta.footprintClass || "PARTIAL_BUT_USABLE";
  const footprintAvailabilityState =
    portfolioCoverage === "Complete" &&
    (footprintReadyClass === "FOOTPRINT_READY" || coverageMeta.forceComplete)
      ? "COMPLETE_CONFIRMED"
      : "PARTIAL_CONFIRMED";

  const primaryLabel =
    footprintAvailabilityState === "COMPLETE_CONFIRMED"
      ? "Confirmed portfolio"
      : "Confirmed properties";

  return {
    ok: totalHotels > 0,
    hpcPath: true,
    adapter: OE_HPC_ADAPTER,
    managementCompany: opMeta?.canonicalName || "",
    censusRecordCount: matched,
    brandsPortfolioDetail,
    geoFields,
    chainScaleFields: {},
    footprintAvailabilityState,
    totals: {
      // Prefer confirmed_* keys for product copy; totalHotels kept for adapter compat but labeled confirmed
      totalHotels,
      totalExistingHotels,
      totalExistingRooms: suppressRooms ? null : totalExistingRooms,
      totalPipelineHotels,
      totalPipelineRooms: suppressRooms ? null : totalPipelineRooms,
      brandCount: brandsPortfolioDetail.length,
      roomsKnownOpen,
      roomsUnknownOpen,
      roomsCoveragePctOpen,
      roomsSuppressed: suppressRooms,
      confirmedOperatingHotels: totalExistingHotels,
      confirmedPipelineHotels: totalPipelineHotels,
      confirmedProperties: totalHotels,
    },
    footprintPresentation: {
      primaryLabel,
      operatingLabel: "Confirmed operating hotels",
      pipelineLabel: "Confirmed pipeline hotels",
      portfolioCoverage, // Complete | Partial | Unknown
      footprintClass: footprintReadyClass,
      footprintAvailabilityState,
      doNotLabelAsTotalPortfolio: footprintAvailabilityState !== "COMPLETE_CONFIRMED",
      suppressFootprintMetrics: false,
      publicCopyNote:
        footprintAvailabilityState === "COMPLETE_CONFIRMED"
          ? "Confirmed portfolio from official current source"
          : "Confirmed properties only — verified footprint is partial",
    },
    relationshipCoverage: {
      confirmedHotels: rels.length,
      matchedHpc: matched,
      missingHpc,
      coverageNote:
        "Confirmed managed hotels from canonical hotel↔operator store (HIGH CURRENT / PREOPENING only)",
      portfolioCoverage,
    },
    source: {
      base: "AIRTABLE_BASE_ID_ALT",
      table: HPC_TABLE,
      adapter: OE_HPC_ADAPTER,
      censusSource: "hpc",
      hotelSource: "Hotel Property Census",
      relationshipStore: "operator-hotel-relationships.json",
      noLegacyReads: true,
      noLegacyManagementCompany: true,
      noStrJoins: true,
      chainScale: "UNKNOWN_SAFE_DEGRADATION",
      rooms: suppressRooms ? "suppressed_partial_coverage" : "product_governed_partial",
      aggregatedAt: new Date().toISOString(),
    },
    propertyCount: matched,
  };
}

/** Optional per-operator coverage annotation from P8.12C coverage artifact. */
function loadOperatorCoverageMeta(masterId, store) {
  try {
    const covPath = path.resolve(
      __dirname,
      "../../data/hotel-census-v2/p812c-operator-portfolios/coverage-by-operator.parquet"
    );
    if (fs.existsSync(covPath)) {
      const raw = JSON.parse(fs.readFileSync(covPath, "utf8"));
      const rows = raw.rows || raw;
      const hit = (Array.isArray(rows) ? rows : []).find(
        (r) => r.operator_id === masterId || r.operatorId === masterId
      );
      if (hit) {
        return {
          portfolioCoverage: hit.portfolioCoverage || hit.sourceCompleteness || "Partial",
          footprintClass: hit.footprintClass || hit.readiness || "PARTIAL_BUT_USABLE",
        };
      }
    }
  } catch (err) {
    console.error("[oe-hpc] coverage meta load failed:", err?.message || err);
  }
  // Default: Partial unless store marks complete
  const op = (store.operators || []).find((o) => o.operatorId === masterId);
  if (op?.portfolioCoverage) {
    return {
      portfolioCoverage: op.portfolioCoverage,
      footprintClass: op.footprintClass || "PARTIAL_BUT_USABLE",
    };
  }
  return { portfolioCoverage: "Partial", footprintClass: "PARTIAL_BUT_USABLE" };
}
