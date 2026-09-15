/**
 * P8.12B — Operator Intelligence HPC adapter (shadow / opt-in).
 *
 * canonical operator relationships → dhl_ → HPC → brand / geography aggregations.
 * No Legacy Hotel Census. No Legacy Management Company.
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
import {
  loadHotelOperatorRelationshipStore,
  listCurrentOperatorHotelDhls,
} from "./operator-explorer-hpc-metrics.js";
import { countryToDealalityRegion, resolveDealalityRegion } from "./region.js";
import { STATUS_OPEN, STATUS_PIPELINE } from "./fields.js";

export const OI_HPC_ADAPTER = "operator-intelligence-hpc-v2";

/** @type {{ hpc: number, legacy: number, silentFallback: number }} */
export const operatorIntelligenceReadCounters = {
  hpc: 0,
  legacy: 0,
  silentFallback: 0,
};

export function resetOperatorIntelligenceReadCounters() {
  operatorIntelligenceReadCounters.hpc = 0;
  operatorIntelligenceReadCounters.legacy = 0;
  operatorIntelligenceReadCounters.silentFallback = 0;
}

export function snapshotOperatorIntelligenceReadCounters() {
  return { ...operatorIntelligenceReadCounters, at: new Date().toISOString() };
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

function countryToRegionUI(country) {
  const r = countryToDealalityRegion(country);
  if (/caribbean|latin|cala/i.test(r || "")) return "Caribbean & Latin America";
  if (/north america/i.test(r || "")) return "North America";
  if (/europe/i.test(r || "")) return "Europe";
  if (/middle east|africa|mea/i.test(r || "")) return "Middle East & Africa";
  if (/asia|pacific|apac/i.test(r || "")) return "Asia Pacific";
  return r || "Other";
}

/**
 * Load CURRENT product relationships from shared runtime store.
 */
export function loadOiRelationshipRuntime() {
  return loadHotelOperatorRelationshipStore(
    process.env.OE_HOTEL_OPERATOR_RELATIONSHIP_STORE || undefined
  );
}

/**
 * Build Operator Intelligence style aggregation from relationship store + HPC.
 * @param {{ region?: string, brand?: string, country?: string, limit?: number }} query
 */
export async function buildOperatorIntelligenceHpcReport(query = {}) {
  if (!isBrandPresenceHpcV2Enabled()) {
    return { ok: false, error: "BRAND_PRESENCE_HPC_V2 required" };
  }
  const store = loadOiRelationshipRuntime();
  const rels = (store.relationships || []).filter(
    (r) =>
      r.confidence === "HIGH" &&
      r.status === "CURRENT_CONFIRMED" &&
      r.hotel_dhl &&
      String(r.hotel_dhl).startsWith("dhl_") &&
      (r.relationshipType === "CURRENT_OPERATOR" ||
        r.relationshipType === "PREOPENING_OPERATOR" ||
        r.relationshipType === "HOTEL_OPERATOR")
  );

  if (!rels.length) {
    return {
      ok: true,
      operators: [],
      brands: [],
      regions: [],
      warnings: ["NO_CONFIRMED_OPERATOR_RELATIONSHIPS"],
      source: {
        adapter: OI_HPC_ADAPTER,
        censusSource: "hpc",
        noLegacyReads: true,
        noLegacyManagementCompany: true,
      },
    };
  }

  const base = createBrandPresenceBase();
  if (!base) return { ok: false, error: "platform base missing" };

  const dhlList = [...new Set(rels.map((r) => r.hotel_dhl))];
  const records = [];
  const CHUNK = 10;
  for (let i = 0; i < dhlList.length; i += CHUNK) {
    const chunk = dhlList.slice(i, i + CHUNK);
    const or = chunk.map((d) => `{${HPC_FIELDS.dhl}}='${escapeFormula(d)}'`).join(",");
    const formula = chunk.length === 1 ? or : `OR(${or})`;
    const batch = await base(HPC_TABLE)
      .select({ filterByFormula: formula, fields: [...HPC_MAP_FIELDS], pageSize: 100 })
      .all();
    records.push(...batch);
  }
  operatorIntelligenceReadCounters.hpc += 1;

  const byDhl = new Map();
  for (const rec of records) {
    const dhl = readText(rec.fields || {}, HPC_FIELDS.dhl);
    if (dhl) byDhl.set(dhl, rec);
  }

  const regionFilter = String(query.region || "").trim().toLowerCase();
  const brandFilter = String(query.brand || "").trim().toLowerCase();
  const countryFilter = String(query.country || "").trim().toLowerCase();

  const byOperator = new Map();
  const byBrand = new Map();
  const byRegion = new Map();

  for (const rel of rels) {
    const rec = byDhl.get(rel.hotel_dhl);
    if (!rec) continue;
    const f = rec.fields || {};
    const status = deriveHpcLifecycleStatus(f);
    if (status !== STATUS_OPEN && status !== STATUS_PIPELINE) continue;

    const brand = readText(f, HPC_FIELDS.brand) || "(no brand)";
    const country = readText(f, HPC_FIELDS.country) || "";
    const city = readText(f, HPC_FIELDS.city) || "";
    const regionUI = countryToRegionUI(country);
    const roomsRaw = deriveHpcProductRooms(f);
    const rooms = roomsRaw != null && Number(roomsRaw) > 0 ? Number(roomsRaw) : 0;
    const name = readText(f, HPC_FIELDS.name) || readText(f, HPC_FIELDS.canonicalName) || "Unknown";

    if (regionFilter && !regionUI.toLowerCase().includes(regionFilter)) continue;
    if (brandFilter && !brand.toLowerCase().includes(brandFilter)) continue;
    if (countryFilter && !country.toLowerCase().includes(countryFilter)) continue;

    const opId = rel.operatorId || rel.operator_id;
    const opName = rel.operatorCanonicalName || opId;
    if (!byOperator.has(opId)) {
      byOperator.set(opId, {
        operator_id: opId,
        operator_name: opName,
        hotel_count: 0,
        total_keys: 0,
        brands: new Set(),
        countries: new Set(),
        properties: [],
      });
    }
    const op = byOperator.get(opId);
    op.hotel_count += 1;
    op.total_keys += rooms;
    op.brands.add(brand);
    if (country) op.countries.add(country);
    op.properties.push({
      dhl_: rel.hotel_dhl,
      name,
      brand,
      city,
      country,
      status,
      rooms,
    });

    if (!byBrand.has(brand)) byBrand.set(brand, { name: brand, hotel_count: 0, total_keys: 0 });
    const b = byBrand.get(brand);
    b.hotel_count += 1;
    b.total_keys += rooms;

    if (!byRegion.has(regionUI)) byRegion.set(regionUI, { name: regionUI, hotel_count: 0, total_keys: 0 });
    const r = byRegion.get(regionUI);
    r.hotel_count += 1;
    r.total_keys += rooms;
  }

  const operators = [...byOperator.values()]
    .map((op) => ({
      operator_id: op.operator_id,
      operator_name: op.operator_name,
      hotel_count: op.hotel_count,
      total_keys: op.total_keys,
      brands: [...op.brands].sort(),
      countries: [...op.countries].sort(),
      properties: op.properties,
    }))
    .sort((a, b) => b.hotel_count - a.hotel_count || b.total_keys - a.total_keys);

  const limit = Math.max(1, Math.min(500, Number(query.limit) || 100));

  return {
    ok: true,
    operators: operators.slice(0, limit),
    brands: [...byBrand.values()].sort((a, b) => b.hotel_count - a.hotel_count),
    regions: [...byRegion.values()].sort((a, b) => b.hotel_count - a.hotel_count),
    metrics: {
      operatorCount: operators.length,
      hotelCount: operators.reduce((s, o) => s + o.hotel_count, 0),
      relationshipInput: rels.length,
      hpcMatched: records.length,
    },
    warnings: [
      "OI_HPC: rooms are product-governed partial (often 0); Chain Scale unknown SAFE_DEGRADE; CURRENT relationships only.",
    ],
    source: {
      adapter: OI_HPC_ADAPTER,
      censusSource: "hpc",
      hotelSource: "Hotel Property Census",
      relationshipStore: "operator-hotel-relationships.json",
      noLegacyReads: true,
      noLegacyManagementCompany: true,
      noStrJoins: true,
      aggregatedAt: new Date().toISOString(),
    },
  };
}

export { listCurrentOperatorHotelDhls };
