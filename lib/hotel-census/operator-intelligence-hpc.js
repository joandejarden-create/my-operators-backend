/**
 * P8.14 — Operator Intelligence HPC adapter (shadow / opt-in).
 *
 * Architecture:
 *   Operator Intelligence
 *   → canonical relationship store
 *   → CURRENT_OPERATOR (HIGH)
 *   → dhl_
 *   → HPC
 *   → canonical brand + geography
 *   → aggregate by operator
 *
 * No Legacy Hotel Census. No Legacy Management Company. No STR joins.
 * Rankings = confirmed operator presence, not market share.
 */

import {
  createBrandPresenceBase,
  deriveHpcLifecycleStatus,
  deriveHpcBrandDisplay,
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
import { countryToDealalityRegion } from "./region.js";
import { STATUS_OPEN, STATUS_PIPELINE } from "./fields.js";

export const OI_HPC_ADAPTER = "operator-intelligence-hpc-v2";

/** UI region labels → country lists (same contract as Legacy OI / Radar). */
export const OI_REGION_UI_TO_COUNTRIES = Object.freeze({
  "Caribbean & Latin America": [
    "Mexico", "Jamaica", "Dominican Republic", "Puerto Rico", "Cuba", "Bahamas", "Aruba",
    "Curaçao", "Cayman Islands", "Trinidad and Tobago", "Barbados", "Haiti",
    "Saint Lucia", "Antigua and Barbuda", "Grenada", "Saint Vincent and the Grenadines",
    "Dominica", "Saint Kitts and Nevis", "Turks and Caicos", "British Virgin Islands",
    "U.S. Virgin Islands", "Martinique", "Guadeloupe", "Bonaire",
    "Colombia", "Brazil", "Argentina", "Chile", "Peru", "Ecuador", "Costa Rica",
    "Panama", "Guatemala", "Honduras", "El Salvador", "Nicaragua", "Venezuela", "Uruguay",
    "Paraguay", "Bolivia",
  ],
  "North America": ["United States", "USA", "Canada", "United States of America"],
  Europe: [
    "United Kingdom", "France", "Germany", "Spain", "Italy", "Portugal", "Netherlands",
    "Ireland", "Switzerland", "Austria", "Belgium", "Greece", "Poland", "Turkey",
    "Russia", "Czech Republic", "Hungary", "Romania", "Sweden", "Norway", "Denmark", "Finland",
    "Iceland", "Luxembourg", "Malta", "Cyprus", "Croatia", "Bulgaria", "Serbia", "Ukraine",
  ],
  "Middle East & Africa": [
    "United Arab Emirates", "Saudi Arabia", "Qatar", "Israel", "Egypt", "Jordan",
    "Lebanon", "Bahrain", "Kuwait", "Oman", "South Africa", "Morocco", "Kenya",
    "Nigeria", "Ethiopia", "Tanzania", "Ghana", "Tunisia", "Mauritius", "Rwanda",
  ],
  "Asia Pacific": [
    "China", "Japan", "India", "Singapore", "Thailand", "Indonesia", "Malaysia",
    "South Korea", "Vietnam", "Philippines", "Australia", "New Zealand", "Hong Kong",
    "Taiwan", "Sri Lanka", "Maldives", "Cambodia", "Myanmar", "Macau", "Pakistan", "Bangladesh",
  ],
});

/** @type {{ hpc: number, legacy: number, silentFallback: number, relationshipStore: number }} */
export const operatorIntelligenceReadCounters = {
  hpc: 0,
  legacy: 0,
  silentFallback: 0,
  relationshipStore: 0,
};

export function resetOperatorIntelligenceReadCounters() {
  operatorIntelligenceReadCounters.hpc = 0;
  operatorIntelligenceReadCounters.legacy = 0;
  operatorIntelligenceReadCounters.silentFallback = 0;
  operatorIntelligenceReadCounters.relationshipStore = 0;
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

function normalize(s) {
  return String(s || "")
    .toLowerCase()
    .trim();
}

function countryInRegionUI(country, regionKey) {
  if (!country || !regionKey) return false;
  const list = OI_REGION_UI_TO_COUNTRIES[regionKey];
  if (!list) return false;
  const c = normalize(country);
  return list.some((r) => normalize(r) === c);
}

export function countryToRegionUI(country) {
  if (!country || !normalize(country)) return "UNKNOWN_REGION";
  const c = normalize(country);
  for (const [regionKey, list] of Object.entries(OI_REGION_UI_TO_COUNTRIES)) {
    if (list.some((r) => normalize(r) === c)) return regionKey;
  }
  return "UNKNOWN_REGION";
}

/**
 * Brand confirmation class for OI brand-filtered ranking.
 * Brand-specific queries use CONFIRMED_BRAND only.
 */
export function classifyOiBrandAffiliation(fields) {
  const aff = readText(fields, HPC_FIELDS.affiliationStatus);
  const display = deriveHpcBrandDisplay(fields);
  if (aff === "Unknown" || !aff) {
    return { class: "AFFILIATION_UNKNOWN", brand: display };
  }
  if (aff === "Brand-Unconfirmed") {
    return { class: "BRAND_UNCONFIRMED", brand: display };
  }
  if (
    aff === "Branded" ||
    aff === "Soft-Branded / Collection" ||
    aff === "Independent" ||
    aff === "Formerly Branded" ||
    aff === "Future / Pipeline"
  ) {
    return { class: "CONFIRMED_BRAND", brand: display };
  }
  if (display) return { class: "CONFIRMED_BRAND", brand: display };
  return { class: "AFFILIATION_UNKNOWN", brand: null };
}

export function loadOiRelationshipRuntime() {
  operatorIntelligenceReadCounters.relationshipStore += 1;
  return loadHotelOperatorRelationshipStore(
    process.env.OE_HOTEL_OPERATOR_RELATIONSHIP_STORE || undefined
  );
}

/**
 * Product ranking relationships: CURRENT_OPERATOR only (HIGH / CURRENT_CONFIRMED).
 * Excludes FORMER / AMBIGUOUS / UNKNOWN / PREOPENING.
 */
export function listOiCurrentOperatorRelationships(store = loadOiRelationshipRuntime()) {
  return (store.relationships || []).filter((r) => {
    if (r.status !== "CURRENT_CONFIRMED") return false;
    if (r.confidence !== "HIGH") return false;
    if (!r.hotel_dhl || !String(r.hotel_dhl).startsWith("dhl_")) return false;
    const rt = r.relationshipType || "CURRENT_OPERATOR";
    return rt === "CURRENT_OPERATOR" || rt === "HOTEL_OPERATOR";
  });
}

export function listOiPreopeningRelationships(store = loadOiRelationshipRuntime()) {
  return (store.relationships || []).filter((r) => {
    if (r.status !== "CURRENT_CONFIRMED") return false;
    if (r.confidence !== "HIGH") return false;
    if (!r.hotel_dhl || !String(r.hotel_dhl).startsWith("dhl_")) return false;
    return (r.relationshipType || "") === "PREOPENING_OPERATOR";
  });
}

const CENSUS_DISABLED_OPERATOR_IDS = new Set(["recF5Z87OAqFgndoq"]); // Arbor

async function fetchHpcByDhls(base, dhlList) {
  const records = [];
  const CHUNK = 40;
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
  return records;
}

function emptyReport(extra = {}) {
  return {
    ok: true,
    operators: [],
    brands: [],
    parent_companies: [],
    regions: [],
    chain_scales: [],
    statuses: [],
    properties: [],
    total_properties: 0,
    total_in_census: null,
    pipeline_operators: [],
    metrics: {
      operatorCount: 0,
      confirmedHotelCount: 0,
      relationshipInput: 0,
      hpcMatched: 0,
    },
    emptyState: {
      kind: "NO_CONFIRMED_MATCHES",
      title: "No confirmed operator relationships",
      message:
        "Dealality does not currently have confirmed operator relationships matching this selection. This does not mean no operators exist in the market.",
      doNotImplyNoOperatorsExist: true,
    },
    metricSemantics: {
      hotel_count: "confirmed_current_operator_hotels",
      total_keys: "product_governed_rooms_partial_or_zero",
      rankingBasis: "confirmed_CURRENT_OPERATOR_relationships",
      notMarketShare: true,
      coverageLabel: "Based on confirmed operator relationships",
      countLabel: "Confirmed properties",
    },
    warnings: ["NO_CONFIRMED_OPERATOR_RELATIONSHIPS_FOR_SELECTION"],
    source: {
      adapter: OI_HPC_ADAPTER,
      censusSource: "hpc",
      hotelSource: "Hotel Property Census",
      relationshipStore: "operator-hotel-relationships.json",
      noLegacyReads: true,
      noLegacyManagementCompany: true,
      noStrJoins: true,
      noLegacyGeo: true,
      noLegacyRooms: true,
      noLegacyChainScale: true,
    },
    ...extra,
  };
}

/**
 * Build Operator Intelligence ranking from relationship store + HPC.
 * @param {{ region?: string, brand?: string, country?: string, search?: string, limit?: number, includePipeline?: boolean }} query
 */
export async function buildOperatorIntelligenceHpcReport(query = {}) {
  if (!isBrandPresenceHpcV2Enabled()) {
    return { ok: false, error: "BRAND_PRESENCE_HPC_V2 required" };
  }

  const store = loadOiRelationshipRuntime();
  const currentRels = listOiCurrentOperatorRelationships(store).filter(
    (r) => !CENSUS_DISABLED_OPERATOR_IDS.has(r.operatorId)
  );
  const preopeningRels = listOiPreopeningRelationships(store).filter(
    (r) => !CENSUS_DISABLED_OPERATOR_IDS.has(r.operatorId)
  );

  if (!currentRels.length && !preopeningRels.length) {
    return emptyReport({
      metrics: {
        operatorCount: 0,
        confirmedHotelCount: 0,
        relationshipInput: 0,
        hpcMatched: 0,
      },
    });
  }

  const base = createBrandPresenceBase();
  if (!base) return { ok: false, error: "platform base missing" };

  const dhlList = [
    ...new Set([...currentRels, ...preopeningRels].map((r) => r.hotel_dhl)),
  ];
  const records = await fetchHpcByDhls(base, dhlList);
  const byDhl = new Map();
  for (const rec of records) {
    const dhl = readText(rec.fields || {}, HPC_FIELDS.dhl);
    if (dhl) byDhl.set(dhl, rec);
  }

  const regionFilter = String(query.region || "").trim();
  const brandFilter = String(query.brand || "").trim();
  const countryFilter = String(query.country || "").trim();
  const searchFilter = String(query.search || "").trim();
  const brandFilterOn = !!normalize(brandFilter);

  const opMetaById = new Map(
    (store.operators || []).map((o) => [o.operatorId, o])
  );

  function matchFilters(f, brandInfo, name) {
    const country = readText(f, HPC_FIELDS.country) || "";
    const city = readText(f, HPC_FIELDS.city) || "";
    const regionUI = country ? countryToRegionUI(country) : "UNKNOWN_REGION";

    if (regionFilter) {
      // Prefer exact UI region key; also allow substring for flexibility
      if (OI_REGION_UI_TO_COUNTRIES[regionFilter]) {
        if (!countryInRegionUI(country, regionFilter) && regionUI !== regionFilter) {
          // Keep UNKNOWN_REGION rows only when no region filter country list match —
          // do not drop solely for null higher-level region; country drives region.
          if (!countryInRegionUI(country, regionFilter)) return false;
        }
      } else if (!normalize(regionUI).includes(normalize(regionFilter))) {
        return false;
      }
    }
    if (countryFilter && !normalize(country).includes(normalize(countryFilter))) {
      return false;
    }
    if (brandFilterOn) {
      if (brandInfo.class !== "CONFIRMED_BRAND") return false;
      if (!brandInfo.brand || !normalize(brandInfo.brand).includes(normalize(brandFilter))) {
        return false;
      }
    }
    if (searchFilter) {
      const q = normalize(searchFilter);
      if (
        !normalize(name).includes(q) &&
        !normalize(city).includes(q) &&
        !normalize(country).includes(q)
      ) {
        return false;
      }
    }
    return true;
  }

  function aggregate(rels, { currentOnlyLifecycle = true } = {}) {
    const byOperator = new Map();
    const byBrand = new Map();
    const byRegion = new Map();
    const properties = [];
    let skippedMissingHpc = 0;
    let skippedBrandClass = 0;
    let skippedLifecycle = 0;

    for (const rel of rels) {
      const rec = byDhl.get(rel.hotel_dhl);
      if (!rec) {
        skippedMissingHpc += 1;
        continue;
      }
      const f = rec.fields || {};
      const status = deriveHpcLifecycleStatus(f);
      if (currentOnlyLifecycle) {
        if (status !== STATUS_OPEN && status !== STATUS_PIPELINE) {
          skippedLifecycle += 1;
          continue;
        }
        // Current ranking: prefer Open; Pipeline hotel lifecycle still allowed when
        // relationship is CURRENT_OPERATOR (rare). PREOPENING relationships handled separately.
      }

      const brandInfo = classifyOiBrandAffiliation(f);
      const brand = brandInfo.brand || null;
      const name =
        readText(f, HPC_FIELDS.name) ||
        readText(f, HPC_FIELDS.canonicalName) ||
        "Unknown";
      if (!matchFilters(f, brandInfo, name)) {
        if (brandFilterOn && brandInfo.class !== "CONFIRMED_BRAND") skippedBrandClass += 1;
        continue;
      }

      const country = readText(f, HPC_FIELDS.country) || "";
      const city = readText(f, HPC_FIELDS.city) || "";
      const regionUI = country ? countryToRegionUI(country) : "UNKNOWN_REGION";
      const roomsRaw = deriveHpcProductRooms(f);
      const rooms = roomsRaw != null && Number(roomsRaw) > 0 ? Number(roomsRaw) : 0;
      const lat = Number(f[HPC_FIELDS.lat]);
      const lng = Number(f[HPC_FIELDS.lng]);

      const opId = rel.operatorId || rel.operator_id;
      const meta = opMetaById.get(opId) || {};
      const opName =
        rel.operatorCanonicalName || meta.canonicalName || opId;

      if (!byOperator.has(opId)) {
        byOperator.set(opId, {
          operator_id: opId,
          operator_name: opName,
          hotel_count: 0,
          confirmed_hotel_count: 0,
          total_keys: 0,
          brands: new Set(),
          countries: new Set(),
          cities: new Set(),
          hotel_types: new Set(),
          parent_companies: new Set(),
          recent_openings: [],
          properties: [],
          footprintAvailabilityState:
            meta.footprintAvailabilityState ||
            (meta.portfolioCoverage === "Complete"
              ? "COMPLETE_CONFIRMED"
              : meta.portfolioCoverage === "Partial"
                ? "PARTIAL_CONFIRMED"
                : "PARTIAL_CONFIRMED"),
          portfolioCoverage: meta.portfolioCoverage || "Partial",
        });
      }
      const op = byOperator.get(opId);
      op.hotel_count += 1;
      op.confirmed_hotel_count += 1;
      op.total_keys += rooms;
      if (brand) op.brands.add(brand);
      if (country) op.countries.add(country);
      if (city) op.cities.add(city);

      const prop = {
        id: rel.hotel_dhl,
        dhl_: rel.hotel_dhl,
        property_name: name,
        name,
        brand: brand || "",
        brand_class: brandInfo.class,
        city,
        country,
        region: regionUI,
        status,
        keys: rooms,
        rooms,
        chain_scale: null,
        lat: Number.isFinite(lat) ? lat : null,
        lng: Number.isFinite(lng) ? lng : null,
        operator_name: opName,
        operator_id: opId,
        relationship_type: rel.relationshipType || "CURRENT_OPERATOR",
        dealality_region: countryToDealalityRegion(country) || null,
      };
      op.properties.push(prop);
      properties.push(prop);

      const brandKey = brand || "(brand unknown)";
      if (!byBrand.has(brandKey)) {
        byBrand.set(brandKey, { name: brandKey, hotel_count: 0, total_keys: 0, properties: [] });
      }
      const b = byBrand.get(brandKey);
      b.hotel_count += 1;
      b.total_keys += rooms;
      b.properties.push(prop);

      if (!byRegion.has(regionUI)) {
        byRegion.set(regionUI, { name: regionUI, hotel_count: 0, total_keys: 0, properties: [] });
      }
      const r = byRegion.get(regionUI);
      r.hotel_count += 1;
      r.total_keys += rooms;
      r.properties.push(prop);
    }

    return {
      byOperator,
      byBrand,
      byRegion,
      properties,
      skippedMissingHpc,
      skippedBrandClass,
      skippedLifecycle,
    };
  }

  const currentAgg = aggregate(currentRels, { currentOnlyLifecycle: true });
  const pipelineAgg = aggregate(preopeningRels, { currentOnlyLifecycle: false });

  const operators = [...currentAgg.byOperator.values()]
    .map((op) => ({
      operator_id: op.operator_id,
      operator_name: op.operator_name,
      hotel_count: op.hotel_count,
      confirmed_hotel_count: op.confirmed_hotel_count,
      total_keys: op.total_keys,
      brands: [...op.brands].sort(),
      countries: [...op.countries].sort(),
      cities: [...op.cities].sort(),
      hotel_types: [],
      parent_company: null,
      parent_companies: [],
      recent_openings: [],
      recent_openings_count: 0,
      properties: op.properties,
      footprintAvailabilityState: op.footprintAvailabilityState,
      portfolioCoverage: op.portfolioCoverage,
      metricLabel: "Confirmed properties",
    }))
    .sort(
      (a, b) =>
        b.confirmed_hotel_count - a.confirmed_hotel_count ||
        b.hotel_count - a.hotel_count ||
        (a.operator_name || "").localeCompare(b.operator_name || "")
    );

  const pipeline_operators = [...pipelineAgg.byOperator.values()]
    .map((op) => ({
      operator_id: op.operator_id,
      operator_name: op.operator_name,
      confirmed_pipeline_hotel_count: op.hotel_count,
      hotel_count: op.hotel_count,
      properties: op.properties,
      metricLabel: "Confirmed pipeline properties",
    }))
    .sort((a, b) => b.hotel_count - a.hotel_count);

  const limit = Math.max(1, Math.min(500, Number(query.limit) || 200));
  const limitedOps = operators.slice(0, limit);

  if (!limitedOps.length) {
    return emptyReport({
      pipeline_operators: query.includePipeline ? pipeline_operators : [],
      metrics: {
        operatorCount: 0,
        confirmedHotelCount: 0,
        relationshipInput: currentRels.length,
        hpcMatched: records.length,
        skippedMissingHpc: currentAgg.skippedMissingHpc,
      },
      source: {
        adapter: OI_HPC_ADAPTER,
        censusSource: "hpc",
        hotelSource: "Hotel Property Census",
        relationshipStore: "operator-hotel-relationships.json",
        noLegacyReads: true,
        noLegacyManagementCompany: true,
        noStrJoins: true,
        noLegacyGeo: true,
        noLegacyRooms: true,
        noLegacyChainScale: true,
        aggregatedAt: new Date().toISOString(),
      },
    });
  }

  const brands = [...currentAgg.byBrand.values()].sort(
    (a, b) => b.hotel_count - a.hotel_count
  );
  const regions = [...currentAgg.byRegion.values()].sort(
    (a, b) => b.hotel_count - a.hotel_count
  );

  return {
    ok: true,
    operators: limitedOps,
    brands,
    parent_companies: [],
    regions,
    chain_scales: [],
    statuses: [
      { name: "Open", hotel_count: currentAgg.properties.filter((p) => p.status === STATUS_OPEN).length, total_keys: 0, properties: [] },
      { name: "Pipeline", hotel_count: currentAgg.properties.filter((p) => p.status === STATUS_PIPELINE).length, total_keys: 0, properties: [] },
    ].filter((s) => s.hotel_count > 0),
    properties: currentAgg.properties,
    total_properties: currentAgg.properties.length,
    total_in_census: null,
    pipeline_operators: query.includePipeline === false ? [] : pipeline_operators,
    metrics: {
      operatorCount: operators.length,
      confirmedHotelCount: currentAgg.properties.length,
      relationshipInput: currentRels.length,
      hpcMatched: records.length,
      skippedMissingHpc: currentAgg.skippedMissingHpc,
      skippedBrandClass: currentAgg.skippedBrandClass,
      preopeningRelationshipCount: preopeningRels.length,
    },
    emptyState: null,
    metricSemantics: {
      hotel_count: "confirmed_current_operator_hotels",
      total_keys: "product_governed_rooms_partial_or_zero",
      rankingBasis: "confirmed_CURRENT_OPERATOR_relationships",
      notMarketShare: true,
      coverageLabel: "Based on confirmed operator relationships",
      countLabel: "Confirmed properties",
      chainScale: "SAFE_DEGRADE_unavailable",
      rooms: "SAFE_DEGRADE_partial",
      parentCompany: "REMOVE_FROM_HPC_PATH",
      locationType: "REMOVE_FROM_HPC_PATH",
      operationType: "REMOVE_FROM_HPC_PATH",
    },
    filterSupport: {
      brand: "SUPPORTED_CANONICALLY",
      region: "SUPPORTED_CANONICALLY",
      country: "SUPPORTED_CANONICALLY",
      search: "SUPPORTED_CANONICALLY",
      chainScale: "SAFE_DEGRADE",
      rooms: "SAFE_DEGRADE",
      parentCompany: "REMOVE_FROM_HPC_PATH",
      locationType: "REMOVE_FROM_HPC_PATH",
      operationType: "REMOVE_FROM_HPC_PATH",
      status: "SAFE_DEGRADE",
    },
    warnings: [
      "OI_HPC: counts are confirmed CURRENT_OPERATOR relationships only — not market share.",
      "Chain Scale / Legacy rooms filters ignored on HPC path (SAFE_DEGRADE).",
    ],
    source: {
      adapter: OI_HPC_ADAPTER,
      censusSource: "hpc",
      hotelSource: "Hotel Property Census",
      relationshipStore: "operator-hotel-relationships.json",
      noLegacyReads: true,
      noLegacyManagementCompany: true,
      noStrJoins: true,
      noLegacyGeo: true,
      noLegacyRooms: true,
      noLegacyChainScale: true,
      aggregatedAt: new Date().toISOString(),
    },
  };
}

/**
 * Filter dropdown options for OI HPC path (no Legacy Census scan).
 */
export async function buildOperatorIntelligenceHpcFilters() {
  const report = await buildOperatorIntelligenceHpcReport({ limit: 500 });
  if (!report.ok) return { ok: false, error: report.error };
  const brands = [...new Set((report.brands || []).map((b) => b.name).filter(Boolean))].sort();
  return {
    ok: true,
    parentCompanies: [],
    brands,
    locationTypes: [],
    operationTypes: [],
    chainScales: [],
    regions: Object.keys(OI_REGION_UI_TO_COUNTRIES),
    source: report.source,
    filterSupport: report.filterSupport,
    note: "HPC filters: brand/region/search supported; Chain Scale / location / parent company degraded or removed",
  };
}

export { listCurrentOperatorHotelDhls };
