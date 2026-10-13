/**
 * Hotel Census → Operator Setup / Explorer footprint (Portfolio Distribution).
 * Primary source: Management Company on Hotel Census (ALT base).
 * Operator Setup prefill values are used only when census has no matching rows.
 */

import { countryToDealalityRegion } from "./region.js";
import { CENSUS_FIELDS, STATUS_OPEN, STATUS_PIPELINE } from "./fields.js";
import { HOTEL_CENSUS_TABLE, getPlatformBase } from "./platform-base.js";
import { HE_MGMT } from "./he-cala-census-apply.js";

export { HE_MGMT };

/** Master record ids that must use Operator Setup footprint only (no Hotel Census overlay). */
export const CENSUS_FOOTPRINT_DISABLED_MASTER_IDS = new Set([
  "recF5Z87OAqFgndoq", // Arbor Lodging (CALA) — no managed CALA hotels
]);

/** Master record id → exact Management Company label in Hotel Census. */
export const MAP_OPERATOR_MASTER_TO_CENSUS_MGMT = {
  recWPKu5laVZxsvpn: HE_MGMT,
};

const REGION_LABEL_TO_GEO_ID = {
  "north america": "na",
  "caribbean & latin america": "cala",
  europe: "eu",
  "middle east & africa": "mea",
  "asia pacific": "apac",
};

const CHAIN_SCALE_TIER_ORDER = [
  { id: "luxury", re: /luxury/i },
  { id: "upperUpscale", re: /upper\s*upscale/i },
  { id: "upperMidscale", re: /upper\s*midscale/i },
  { id: "upscale", re: /upscale/i },
  { id: "midscale", re: /midscale/i },
  { id: "economy", re: /economy/i },
];

function nz(v) {
  return v != null && String(v).trim() !== "" ? String(v).trim() : "";
}

function parseRooms(value) {
  const n = parseInt(value, 10);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function normalizeStatus(raw) {
  const parts = Array.isArray(raw) ? raw : [raw];
  for (const p of parts) {
    const s = nz(p).toLowerCase();
    if (s === "open") return STATUS_OPEN;
    if (s === "pipeline") return STATUS_PIPELINE;
  }
  const s = nz(raw).toLowerCase();
  if (s === "open") return STATUS_OPEN;
  if (s === "pipeline") return STATUS_PIPELINE;
  return "";
}

function brandKeyFromName(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/g, "");
}

function chainScaleToTierId(chainScaleRaw) {
  const s = String(chainScaleRaw || "")
    .toLowerCase()
    .replace(/\s+chain\s*$/i, "")
    .trim();
  if (!s) return null;
  for (const tier of CHAIN_SCALE_TIER_ORDER) {
    if (tier.re.test(s)) return tier.id;
  }
  if (/independent/.test(s)) return "upscale";
  return null;
}

function regionLabelToGeoId(regionLabel) {
  const key = String(regionLabel || "").toLowerCase().replace(/\s+/g, " ").trim();
  return REGION_LABEL_TO_GEO_ID[key] || null;
}

/**
 * Resolve Management Company for census query.
 * @param {string} [masterId]
 * @param {object} [prefill]
 */
export function resolveCensusManagementCompany(masterId, prefill) {
  const mid = nz(masterId);
  if (mid && CENSUS_FOOTPRINT_DISABLED_MASTER_IDS.has(mid)) {
    return "";
  }
  if (mid && MAP_OPERATOR_MASTER_TO_CENSUS_MGMT[mid]) {
    return MAP_OPERATOR_MASTER_TO_CENSUS_MGMT[mid];
  }
  const name = nz(prefill?.companyName);
  if (/hotel\s+equities/i.test(name) && /cala|caribbean|latin/i.test(name)) {
    return HE_MGMT;
  }
  if (name === HE_MGMT) return HE_MGMT;
  return "";
}

/**
 * @param {string} mgmtCompany Exact Airtable Management Company value
 */
export async function fetchCensusRecordsByManagementCompany(mgmtCompany) {
  const base = getPlatformBase();
  const label = nz(mgmtCompany);
  if (!base || !label) {
    return { ok: false, error: "Platform base or management company missing", records: [] };
  }
  const esc = label.replace(/'/g, "\\'");
  const formula = `{${CENSUS_FIELDS.managementCompany}}='${esc}'`;
  const fields = [
    CENSUS_FIELDS.name,
    CENSUS_FIELDS.affiliation,
    CENSUS_FIELDS.status,
    CENSUS_FIELDS.rooms,
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.region,
    CENSUS_FIELDS.chainScale,
    CENSUS_FIELDS.managementCompany,
  ];

  try {
    const records = await base(HOTEL_CENSUS_TABLE)
      .select({ filterByFormula: formula, fields, pageSize: 100 })
      .all();
    return { ok: true, records, managementCompany: label };
  } catch (e) {
    return {
      ok: false,
      error: e && e.message ? e.message : String(e),
      records: [],
      managementCompany: label,
    };
  }
}

/**
 * Aggregate census rows into Operator Setup / Explorer footprint shape.
 * @param {import('airtable').Records<any>} records
 */
export function aggregateOperatorCensusFootprint(records) {
  const byBrand = new Map();
  const byGeo = new Map();
  const byChain = new Map();
  let totalExistingHotels = 0;
  let totalExistingRooms = 0;
  let totalPipelineHotels = 0;
  let totalPipelineRooms = 0;

  for (const rec of records || []) {
    const f = rec.fields || {};
    const status = normalizeStatus(f[CENSUS_FIELDS.status]);
    if (status !== STATUS_OPEN && status !== STATUS_PIPELINE) continue;

    const affiliation = nz(f[CENSUS_FIELDS.affiliation]) || "Independent";
    const rooms = parseRooms(f[CENSUS_FIELDS.rooms]);
    const isOpen = status === STATUS_OPEN;

    if (!byBrand.has(affiliation)) {
      byBrand.set(affiliation, {
        brand_name: affiliation,
        brand_key: brandKeyFromName(affiliation),
        existing_properties: 0,
        existing_rooms: 0,
        pipeline_properties: 0,
        pipeline_rooms: 0,
      });
    }
    const brandRow = byBrand.get(affiliation);
    if (isOpen) {
      brandRow.existing_properties += 1;
      brandRow.existing_rooms += rooms;
      totalExistingHotels += 1;
      totalExistingRooms += rooms;
    } else {
      brandRow.pipeline_properties += 1;
      brandRow.pipeline_rooms += rooms;
      totalPipelineHotels += 1;
      totalPipelineRooms += rooms;
    }

    const regionLabel =
      nz(f[CENSUS_FIELDS.region]) || countryToDealalityRegion(f[CENSUS_FIELDS.country]);
    const geoId = regionLabelToGeoId(regionLabel);
    if (geoId) {
      if (!byGeo.has(geoId)) {
        byGeo.set(geoId, { existing_hotels: 0, existing_rooms: 0, pipeline_hotels: 0, pipeline_rooms: 0 });
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

    const tierId = chainScaleToTierId(f[CENSUS_FIELDS.chainScale]);
    if (tierId) {
      if (!byChain.has(tierId)) {
        byChain.set(tierId, {
          existing_properties: 0,
          existing_rooms: 0,
          pipeline_properties: 0,
          pipeline_rooms: 0,
        });
      }
      const c = byChain.get(tierId);
      if (isOpen) {
        c.existing_properties += 1;
        c.existing_rooms += rooms;
      } else {
        c.pipeline_properties += 1;
        c.pipeline_rooms += rooms;
      }
    }
  }

  const brandsPortfolioDetail = [...byBrand.values()]
    .filter(
      (r) =>
        r.existing_properties + r.existing_rooms + r.pipeline_properties + r.pipeline_rooms > 0
    )
    .sort(
      (a, b) =>
        b.existing_rooms +
        b.pipeline_rooms -
        (a.existing_rooms + a.pipeline_rooms) ||
        b.existing_properties +
          b.pipeline_properties -
          (a.existing_properties + a.pipeline_properties)
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

  const chainScaleFields = {};
  for (const [tierId, c] of byChain.entries()) {
    chainScaleFields[`${tierId}ExistingProperties`] = String(c.existing_properties);
    chainScaleFields[`${tierId}ExistingRooms`] = String(c.existing_rooms);
    chainScaleFields[`${tierId}PipelineProperties`] = String(c.pipeline_properties);
    chainScaleFields[`${tierId}PipelineRooms`] = String(c.pipeline_rooms);
  }

  const totalHotels = totalExistingHotels + totalPipelineHotels;
  return {
    ok: totalHotels > 0,
    brandsPortfolioDetail,
    geoFields,
    chainScaleFields,
    totals: {
      totalHotels,
      totalExistingHotels,
      totalExistingRooms,
      totalPipelineHotels,
      totalPipelineRooms,
      brandCount: brandsPortfolioDetail.length,
    },
    propertyCount: (records || []).length,
  };
}

/**
 * Load and aggregate footprint for an operator.
 * @param {{ masterId?: string, prefill?: object }} opts
 */
export async function buildOperatorCensusFootprint(opts = {}) {
  const mgmt = resolveCensusManagementCompany(opts.masterId, opts.prefill);
  if (!mgmt) {
    return { ok: false, reason: "no_management_company_mapping", managementCompany: "" };
  }
  const fetchRes = await fetchCensusRecordsByManagementCompany(mgmt);
  if (!fetchRes.ok) {
    return {
      ok: false,
      reason: "census_fetch_failed",
      managementCompany: mgmt,
      error: fetchRes.error,
    };
  }
  const aggregated = aggregateOperatorCensusFootprint(fetchRes.records);
  return {
    ...aggregated,
    managementCompany: mgmt,
    censusRecordCount: fetchRes.records.length,
  };
}

/**
 * Merge census footprint into operator prefill (census wins when it has hotels).
 * @param {object} prefill
 * @param {object} censusFootprint from buildOperatorCensusFootprint
 * @returns {{ source: 'hotel_census'|'operator_setup', applied: boolean }}
 */
/** Airtable Governance column titles for footprint keys (form prefill key → field titles). */
const FOOTPRINT_PREFILL_TO_AIRTABLE = {
  brandsPortfolioDetail: ["Brands Portfolio Detail", "Brand Units & Staffing Detail"],
  geo_na_existing_hotels: ["Geo NA Existing Hotels", "NA Existing Hotels"],
  geo_na_existing_rooms: ["Geo NA Existing Rooms", "NA Existing Rooms"],
  geo_na_pipeline_hotels: ["Geo NA Pipeline Hotels", "NA Pipeline Hotels"],
  geo_na_pipeline_rooms: ["Geo NA Pipeline Rooms", "NA Pipeline Rooms"],
  geo_cala_existing_hotels: ["Geo CALA Existing Hotels", "CALA Existing Hotels"],
  geo_cala_existing_rooms: ["Geo CALA Existing Rooms", "CALA Existing Rooms"],
  geo_cala_pipeline_hotels: ["Geo CALA Pipeline Hotels", "CALA Pipeline Hotels"],
  geo_cala_pipeline_rooms: ["Geo CALA Pipeline Rooms", "CALA Pipeline Rooms"],
  geo_eu_existing_hotels: ["Geo EU Existing Hotels", "EU Existing Hotels"],
  geo_eu_existing_rooms: ["Geo EU Existing Rooms", "EU Existing Rooms"],
  geo_eu_pipeline_hotels: ["Geo EU Pipeline Hotels", "EU Pipeline Hotels"],
  geo_eu_pipeline_rooms: ["Geo EU Pipeline Rooms", "EU Pipeline Rooms"],
  geo_mea_existing_hotels: ["Geo MEA Existing Hotels", "MEA Existing Hotels"],
  geo_mea_existing_rooms: ["Geo MEA Existing Rooms", "MEA Existing Rooms"],
  geo_mea_pipeline_hotels: ["Geo MEA Pipeline Hotels", "MEA Pipeline Hotels"],
  geo_mea_pipeline_rooms: ["Geo MEA Pipeline Rooms", "MEA Pipeline Rooms"],
  geo_apac_existing_hotels: ["Geo APAC Existing Hotels", "APAC Existing Hotels"],
  geo_apac_existing_rooms: ["Geo APAC Existing Rooms", "APAC Existing Rooms"],
  geo_apac_pipeline_hotels: ["Geo APAC Pipeline Hotels", "APAC Pipeline Hotels"],
  geo_apac_pipeline_rooms: ["Geo APAC Pipeline Rooms", "APAC Pipeline Rooms"],
  geo_total_existing_hotels: ["Geo Total Existing Hotels"],
  geo_total_existing_rooms: ["Geo Total Existing Rooms"],
  geo_total_pipeline_hotels: ["Geo Total Pipeline Hotels"],
  geo_total_pipeline_rooms: ["Geo Total Pipeline Rooms"],
  luxuryExistingProperties: ["Luxury Existing Properties"],
  luxuryExistingRooms: ["Luxury Existing Rooms"],
  luxuryPipelineProperties: ["Luxury Pipeline Properties"],
  luxuryPipelineRooms: ["Luxury Pipeline Rooms"],
  upperUpscaleExistingProperties: ["Upper Upscale Existing Properties"],
  upperUpscaleExistingRooms: ["Upper Upscale Existing Rooms"],
  upperUpscalePipelineProperties: ["Upper Upscale Pipeline Properties"],
  upperUpscalePipelineRooms: ["Upper Upscale Pipeline Rooms"],
  upscaleExistingProperties: ["Upscale Existing Properties"],
  upscaleExistingRooms: ["Upscale Existing Rooms"],
  upscalePipelineProperties: ["Upscale Pipeline Properties"],
  upscalePipelineRooms: ["Upscale Pipeline Rooms"],
  upperMidscaleExistingProperties: ["Upper Midscale Existing Properties"],
  upperMidscaleExistingRooms: ["Upper Midscale Existing Rooms"],
  upperMidscalePipelineProperties: ["Upper Midscale Pipeline Properties"],
  upperMidscalePipelineRooms: ["Upper Midscale Pipeline Rooms"],
  midscaleExistingProperties: ["Midscale Existing Properties"],
  midscaleExistingRooms: ["Midscale Existing Rooms"],
  midscalePipelineProperties: ["Midscale Pipeline Properties"],
  midscalePipelineRooms: ["Midscale Pipeline Rooms"],
  economyExistingProperties: ["Economy Existing Properties"],
  economyExistingRooms: ["Economy Existing Rooms"],
  economyPipelineProperties: ["Economy Pipeline Properties"],
  economyPipelineRooms: ["Economy Pipeline Rooms"],
};

function writeFootprintValueToFields(fields, prefillKey, value) {
  if (!fields || value == null || value === "") return;
  const titles = FOOTPRINT_PREFILL_TO_AIRTABLE[prefillKey];
  if (!titles) return;
  let stored = value;
  if (prefillKey === "brandsPortfolioDetail" && Array.isArray(value)) {
    stored = JSON.stringify(value);
  }
  const s = String(stored);
  titles.forEach((title) => {
    fields[title] = stored;
  });
  fields[prefillKey] = stored;
}

/**
 * Mirror census footprint prefill keys onto Basics-shaped `fields` for Explorer pickField().
 * @param {object} fields
 * @param {object} prefill
 */
export function syncOperatorFieldsFromCensusFootprint(fields, prefill) {
  if (!fields || !prefill || prefill.footprintPortfolioSource !== "hotel_census") return;
  Object.keys(FOOTPRINT_PREFILL_TO_AIRTABLE).forEach((key) => {
    if (prefill[key] == null || prefill[key] === "") return;
    writeFootprintValueToFields(fields, key, prefill[key]);
  });
}

export function applyCensusFootprintToOperatorPrefill(prefill, censusFootprint) {
  if (!prefill || !censusFootprint?.ok || !(censusFootprint.totals?.totalHotels > 0)) {
    return { source: "operator_setup", applied: false };
  }

  if (censusFootprint.brandsPortfolioDetail?.length) {
    prefill.brandsPortfolioDetail = censusFootprint.brandsPortfolioDetail;
  }
  Object.assign(prefill, censusFootprint.geoFields || {});
  Object.assign(prefill, censusFootprint.chainScaleFields || {});

  if (censusFootprint.totals) {
    prefill.geo_total_existing_hotels = String(censusFootprint.totals.totalExistingHotels ?? "");
    prefill.geo_total_existing_rooms = String(censusFootprint.totals.totalExistingRooms ?? "");
    prefill.geo_total_pipeline_hotels = String(censusFootprint.totals.totalPipelineHotels ?? "");
    prefill.geo_total_pipeline_rooms = String(censusFootprint.totals.totalPipelineRooms ?? "");
    prefill.totalProperties = String(censusFootprint.totals.totalHotels ?? "");
    prefill.totalRooms = String(
      (censusFootprint.totals.totalExistingRooms ?? 0) +
        (censusFootprint.totals.totalPipelineRooms ?? 0)
    );
  }

  prefill.footprintPortfolioSource = "hotel_census";
  prefill.footprintPortfolioManagementCompany = censusFootprint.managementCompany || "";

  return { source: "hotel_census", applied: true };
}

/**
 * Apply census footprint to prefill + fields (single entry for API and tests).
 * @param {{ prefill: object, fields?: object }} target
 * @param {object} censusFootprint
 */
export function applyCensusFootprintToOperatorDetail(target, censusFootprint) {
  const prefill = target?.prefill;
  const fields = target?.fields;
  const applied = applyCensusFootprintToOperatorPrefill(prefill, censusFootprint);
  if (applied.applied && fields) {
    syncOperatorFieldsFromCensusFootprint(fields, prefill);
  }
  return applied;
}
