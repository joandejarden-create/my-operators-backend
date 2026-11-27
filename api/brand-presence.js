import Airtable from "airtable";
import { buildHiltonAmenitiesDisplay } from "../lib/hilton-amenity-display.js";
import {
  getFixtureHotelById,
  isAirtableCredentialError,
  isMexicoRadarFixtureHotelId,
  searchFixtureHotels,
  shouldUseMexicoRadarFixtureFallback,
} from "../lib/hotel-intelligence/golden-demo/mexico-radar-fixture-fallback.js";
import {
  formatHotelPropertyCensusAsBrandPresenceHotel,
  HOTEL_PROPERTY_CENSUS_TABLE,
} from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";
import {
  resolvePropertyCensusOwnership,
  HOTEL_CENSUS_TABLE,
  PROPERTY_IDENTITY_CONTRACT,
} from "../lib/hotel-intelligence/property-census-identity-v1.js";
import {
  applyOverviewEnrichmentToHotel,
  buildHotelExplorerOverviewEnrichment,
} from "../lib/hotel-intelligence/hotel-explorer-overview-enrichment-v1.js";
import { attachOverviewFieldProvenance } from "../lib/hotel-intelligence/he-overview-field-provenance-v1.js";

const AIRTABLE_KEY = process.env.AIRTABLE_API_KEY;
const AIRTABLE_BASE = process.env.AIRTABLE_BASE_ID_ALT;
const base = (AIRTABLE_KEY && AIRTABLE_BASE)
  ? new Airtable({ apiKey: AIRTABLE_KEY }).base(AIRTABLE_BASE)
  : null;

function ensureBrandPresenceConfig(res) {
  if (base) return true;
  res.status(500).json({ error: "Missing Airtable API key or base id for brand presence" });
  return false;
}

function fixtureHotelResponse(recordId) {
  const hotel = getFixtureHotelById(recordId);
  if (!hotel) return null;
  hotel.amenitiesDisplay = buildHiltonAmenitiesDisplay(hotel);
  return { success: true, hotel, source: "golden_demo_radar_fixture" };
}

// In-memory cache for performance optimization
const cache = new Map();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes cache
/** Bump when API response shape or fetched fields change (invalidates stale cache). */
const CACHE_SCHEMA_VERSION = 10; // HPC-native list/search (legacy Hotel Census retired for runtime)

// Cache helper functions
function getCacheKey(query) {
  return `brand-presence-v${CACHE_SCHEMA_VERSION}-${JSON.stringify(query)}`;
}

function getFromCache(key) {
  const cached = cache.get(key);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return cached.data;
  }
  return null;
}

function setCache(key, data) {
  cache.set(key, { data, timestamp: Date.now() });
}

// Background data refresh to keep cache fresh
let isRefreshing = false;
async function refreshCacheInBackground() {
  if (isRefreshing) return;
  
  isRefreshing = true;
  console.log('🔄 Background cache refresh started...');
  
  try {
    // Clear existing cache to force fresh data on next request
    cache.clear();
    console.log('✅ Background cache refresh completed - cache cleared');
  } catch (error) {
    console.error('❌ Background cache refresh failed:', error);
  } finally {
    isRefreshing = false;
  }
}

// Refresh cache every 10 minutes
setInterval(refreshCacheInBackground, 10 * 60 * 1000);

// Field mappings — Hotel Property Census (canonical). Legacy Hotel Census retired for runtime.
const F = {
  hotels: {
    table: HOTEL_PROPERTY_CENSUS_TABLE,
    tableId: "tbl9aY5ijiuIzzWam",
    id: "id",
    name: "Property Name",
    brand: "Current Brand",
    // HPC schema: brand parent lives on Brand Family (not legacy "Parent Company").
    parentCompany: "Brand Family",
    status: "Radar Status",
    lat: "Latitude",
    lng: "Longitude",
    city: "City",
    country: "Country",
    region: "Continent",
    locationType: "Asset Context",
    rooms: "Rooms / Keys",
    strNumber: "STR Number",
    chainScale: "Hotel Class / Segment",
    // Not present on current HPC schema — keep keys for UI compatibility; omit from selects.
    projectPhase: null,
    propertyType: "Property Type",
    operationType: null,
    managementCompany: "Operator / Management Company",
    website: "Official Property URL",
    telephone: "Phone",
    address1: "Address",
    address2: "Address 2",
    market: "Market",
    submarket: "Submarket",
    openDate: "Opening Date",
    projectedOpenDate: null,
    starRating: null,
    amenities: "Amenities - Source Text",
    hotelDescription: "Short Property Summary",
    hotelHeadline: "Property Positioning",
    occupancyRate: null,
    adr: null,
    revpar: null,
    state: "State / Region",
    postalCode: "Postal Code",
    floors: null,
    totalMeetingSpace: null,
    dataConfidence: "Data Confidence Tier",
    censusPropertyType: "Property Type",
    hotelServiceModel: "Hotel Service Model",
    allSuites: null,
    boutique: null,
    casino: null,
    conference: null,
    convention: null,
    golf: null,
    resort: null,
    restaurant: null,
    ski: null,
    spa: null,
  },
};

/** Fields required for map markers and filters — real HPC field names only. */
const MAP_HOTEL_FIELDS = [
  F.hotels.name,
  "Canonical Property Name",
  F.hotels.brand,
  "Brand Family",
  F.hotels.parentCompany,
  F.hotels.status,
  F.hotels.lat,
  F.hotels.lng,
  F.hotels.city,
  F.hotels.country,
  F.hotels.region,
  "State / Region",
  F.hotels.locationType,
  F.hotels.rooms,
  F.hotels.chainScale,
  F.hotels.managementCompany,
  F.hotels.market,
  F.hotels.submarket,
  F.hotels.censusPropertyType,
].filter((name) => typeof name === "string" && name.trim());

function readTextField(fields, key) {
  const raw = fields[key];
  if (raw == null || raw === "") return null;
  if (typeof raw === "string") return raw.trim() || null;
  if (Array.isArray(raw)) {
    const joined = raw
      .map((item) => (typeof item === "string" ? item.trim() : item?.name || ""))
      .filter(Boolean)
      .join("; ");
    return joined || null;
  }
  if (typeof raw === "object" && raw.name) return String(raw.name).trim() || null;
  return String(raw).trim() || null;
}

/** Prefer legacy Radar fields, then production-census Title Case aliases. */
function readTextFieldAliases(fields, keys) {
  for (const key of keys) {
    const value = readTextField(fields, key);
    if (value) return value;
  }
  return null;
}

function readNumberField(fields, key) {
  const raw = fields[key];
  if (raw == null || raw === "") return null;
  const num = Number(raw);
  return Number.isFinite(num) ? num : null;
}

/** Prefer legacy census numeric fields, then HPC-style aliases (e.g. Rooms / Keys). */
function readNumberFieldAliases(fields, keys) {
  for (const key of keys) {
    const value = readNumberField(fields, key);
    if (value != null) return value;
  }
  return null;
}

function readYnFlag(fields, key) {
  const raw = readTextField(fields, key);
  if (!raw) return false;
  const normalized = raw.toLowerCase();
  return normalized === "y" || normalized === "yes" || normalized === "true" || normalized === "1";
}

function readDateField(fields, key) {
  const raw = fields[key];
  if (!raw) return null;
  return String(raw).slice(0, 10);
}

function formatHotelRecord(hotel) {
  const fields = hotel.fields || {};
  const lat = parseFloat(fields[F.hotels.lat]);
  const lng = parseFloat(fields[F.hotels.lng]);
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng) && (lat !== 0 || lng !== 0);

  const amenityFlags = {
    allSuites: readYnFlag(fields, F.hotels.allSuites),
    boutique: readYnFlag(fields, F.hotels.boutique),
    casino: readYnFlag(fields, F.hotels.casino),
    conference: readYnFlag(fields, F.hotels.conference),
    convention: readYnFlag(fields, F.hotels.convention),
    golf: readYnFlag(fields, F.hotels.golf),
    resort: readYnFlag(fields, F.hotels.resort),
    restaurant: readYnFlag(fields, F.hotels.restaurant),
    ski: readYnFlag(fields, F.hotels.ski),
    spa: readYnFlag(fields, F.hotels.spa)
  };

  return {
    id: hotel.id,
    name:
      readTextFieldAliases(fields, [
        F.hotels.name,
        "Property Name",
        "Canonical Property Name",
      ]) || "Unknown Hotel",
    brand:
      readTextFieldAliases(fields, [
        F.hotels.brand,
        "Current Brand",
        "Brand Family",
      ]) || "Unknown Brand",
    parentCompany:
      readTextFieldAliases(fields, [
        F.hotels.parentCompany,
        "Brand Family",
        "Family / Source Family",
      ]) || "Unknown",
    status:
      readTextFieldAliases(fields, [
        F.hotels.status,
        "Radar Status",
        "project_phase",
        "Project Phase",
      ]) || "unknown",
    lat: hasCoords ? lat : 0,
    lng: hasCoords ? lng : 0,
    city:
      readTextFieldAliases(fields, [F.hotels.city, "City"]) || "Unknown City",
    country:
      readTextFieldAliases(fields, [F.hotels.country, "Country"]) ||
      "Unknown Country",
    region:
      readTextFieldAliases(fields, [F.hotels.region, "Continent", "State / Region"]) ||
      "Unknown Region",
    locationType:
      readTextFieldAliases(fields, [F.hotels.locationType, "Asset Context"]) || null,
    rooms: readNumberFieldAliases(fields, [F.hotels.rooms, "Rooms / Keys", "Rooms"]) || 0,
    strNumber: fields[F.hotels.strNumber] ?? null,
    chainScale: readTextFieldAliases(fields, [
      F.hotels.chainScale,
      "Hotel Class / Segment",
    ]),
    projectPhase: readTextField(fields, F.hotels.projectPhase),
    propertyType: readTextField(fields, F.hotels.propertyType),
    operationType: readTextFieldAliases(fields, [
      F.hotels.operationType,
      "Management Model",
    ]),
    managementCompany: readTextFieldAliases(fields, [
      F.hotels.managementCompany,
      "Operator / Management Company",
    ]),
    website: readTextFieldAliases(fields, [
      F.hotels.website,
      "Official Property URL",
    ]),
    telephone: readTextFieldAliases(fields, [F.hotels.telephone, "Phone"]),
    address1: readTextFieldAliases(fields, [F.hotels.address1, "Address"]),
    address2: readTextField(fields, F.hotels.address2),
    market: readTextFieldAliases(fields, [F.hotels.market, "Market / Submarket"]),
    submarket: readTextField(fields, F.hotels.submarket),
    openDate:
      readDateField(fields, F.hotels.openDate) ||
      readDateField(fields, "Open Date") ||
      readDateField(fields, "Opening Date"),
    openingDate:
      readDateField(fields, F.hotels.openDate) ||
      readDateField(fields, "Open Date") ||
      readDateField(fields, "Opening Date"),
    yearBuilt: (() => {
      const dedicated =
        readNumberField(fields, "Year Built") ||
        readDateField(fields, "Year Built");
      if (dedicated != null) {
        const m = String(dedicated).match(/(18|19|20)\d{2}/);
        if (m) {
          const y = Number(m[0]);
          if (Number.isFinite(y) && y >= 1800 && y <= 2100) return y;
        }
      }
      const raw =
        readDateField(fields, F.hotels.openDate) ||
        readDateField(fields, "Open Date") ||
        readDateField(fields, "Opening Date");
      if (!raw) return null;
      const m = String(raw).match(/(18|19|20)\d{2}/);
      if (!m) return null;
      const y = Number(m[0]);
      return Number.isFinite(y) && y >= 1800 && y <= 2100 ? y : null;
    })(),
    lastRenovated: (() => {
      const raw =
        readDateField(fields, "Renovation / Conversion Date") ||
        readDateField(fields, "Last Renovated") ||
        readNumberField(fields, "Year Renovated");
      if (!raw) return null;
      const m = String(raw).match(/(18|19|20)\d{2}/);
      if (!m) return null;
      const y = Number(m[0]);
      return Number.isFinite(y) && y >= 1800 && y <= 2100 ? y : null;
    })(),
    yearRenovated: (() => {
      const raw =
        readDateField(fields, "Renovation / Conversion Date") ||
        readDateField(fields, "Last Renovated") ||
        readNumberField(fields, "Year Renovated");
      if (!raw) return null;
      const m = String(raw).match(/(18|19|20)\d{2}/);
      if (!m) return null;
      const y = Number(m[0]);
      return Number.isFinite(y) && y >= 1800 && y <= 2100 ? y : null;
    })(),
    projectedOpenDate: readDateField(fields, F.hotels.projectedOpenDate),
    starRating: readNumberField(fields, F.hotels.starRating),
    amenities: readTextFieldAliases(fields, [
      F.hotels.amenities,
      "Amenities - Source Text",
      "Amenities - Structured Tags",
    ]),
    hotelDescription: readTextFieldAliases(fields, [
      F.hotels.hotelDescription,
      "Short Property Summary",
      "Property Positioning",
      "Hotel Description - AI Summary",
      "Hotel Description - Source Text",
    ]),
    hotelHeadline: readTextFieldAliases(fields, [
      F.hotels.hotelHeadline,
      "Property Positioning",
      "Hotel Headline",
    ]),
    occupancyRate: readNumberField(fields, F.hotels.occupancyRate),
    adr: readNumberField(fields, F.hotels.adr),
    revpar: readNumberField(fields, F.hotels.revpar),
    state: readTextFieldAliases(fields, [F.hotels.state, "State / Region"]),
    postalCode: readTextField(fields, F.hotels.postalCode),
    floors: readNumberField(fields, F.hotels.floors),
    totalMeetingSpace: readNumberFieldAliases(fields, [
      F.hotels.totalMeetingSpace,
      "Total Meeting Space (sq ft)",
      "Total Meeting Space Sq Ft",
      "Meeting Space Sq Ft",
      "Total Meeting Space",
    ]),
    dataConfidence: readTextField(fields, F.hotels.dataConfidence),
    censusPropertyType: readTextField(fields, F.hotels.censusPropertyType),
    hotelServiceModel: readTextField(fields, F.hotels.hotelServiceModel),
    amenityFlags
  };
}

// Single hotel detail — fresh read for detail panel (amenities, website, phone, etc.)
export async function getBrandPresenceHotelById(req, res) {
  const recordId = String(req.params.recordId || "").trim();
  if (!recordId || !recordId.startsWith("rec")) {
    return res.status(400).json({ success: false, error: "Valid Airtable record id required" });
  }

  if (!base) {
    const fixture = fixtureHotelResponse(recordId);
    if (fixture) return res.json(fixture);
    return res.status(500).json({ error: "Missing Airtable API key or base id for brand presence" });
  }

  try {
    // Fail-closed ownership: RECORD_ID() filter — do not trust GET-by-id table path.
    const identity = await resolvePropertyCensusOwnership(base, recordId);
    let formatted = null;
    let sourceTable = null;
    let owningTableKind = identity.owningTableKind;

    if (identity.ok && identity.record && identity.owningTableKind === "HPC") {
      formatted = formatHotelPropertyCensusAsBrandPresenceHotel(identity.record);
      sourceTable = HOTEL_PROPERTY_CENSUS_TABLE;
      delete formatted.internalPreviewOnly;
    } else if (identity.ok && identity.record && identity.owningTableKind === "HOTEL_CENSUS") {
      formatted = formatHotelRecord(identity.record);
      sourceTable = HOTEL_CENSUS_TABLE;
    } else if (identity.ok && identity.record && identity.owningTableKind === "AMBIGUOUS") {
      formatted = formatHotelPropertyCensusAsBrandPresenceHotel(identity.record);
      sourceTable = HOTEL_PROPERTY_CENSUS_TABLE;
      delete formatted.internalPreviewOnly;
    }

    if (!formatted) {
      throw Object.assign(new Error("Hotel record not found"), { statusCode: 404 });
    }

    formatted.amenitiesDisplay = buildHiltonAmenitiesDisplay(formatted);
    formatted.censusSourceTable = sourceTable;
    formatted.propertyIdentity = {
      contract: PROPERTY_IDENTITY_CONTRACT,
      owningTable: sourceTable,
      owningTableKind,
      censusFound: identity.censusFound === true,
      hpcFound: identity.hpcFound === true,
      warning: identity.warning || null,
    };

    // Golden-demo Mexico presentation overlay is UX-only for Hotel Census–owned
    // Mexico demo hotels. Never overlay fixtures onto HPC-owned canonical records.
    if (
      owningTableKind === "HOTEL_CENSUS" &&
      isMexicoRadarFixtureHotelId(recordId)
    ) {
      const presentation = getFixtureHotelById(recordId);
      if (presentation) {
        const overlayFields = [];
        const applyOverlay = (key, value) => {
          if (value == null || value === "") return;
          formatted[key] = value;
          overlayFields.push(key);
        };
        applyOverlay("name", presentation.name);
        applyOverlay("brand", presentation.brand);
        applyOverlay("parentCompany", presentation.parentCompany);
        applyOverlay("managementCompany", presentation.managementCompany);
        applyOverlay("chainScale", presentation.chainScale);
        applyOverlay("market", presentation.market);
        applyOverlay("submarket", presentation.submarket);
        applyOverlay("website", presentation.website);
        applyOverlay("hotelDescription", presentation.hotelDescription);
        if (presentation.amenities) {
          formatted.amenities = presentation.amenities;
          overlayFields.push("amenities");
        }
        formatted.amenitiesDisplay = buildHiltonAmenitiesDisplay(formatted);
        formatted.presentationOverlay = {
          source: "golden_demo_mexico_fixture",
          applied: true,
          fields: overlayFields,
          note: "UX reference overlay on Hotel Census–owned Mexico demo hotel — not verified HPC canonical intelligence",
        };
      }
    }

    // Hotel-agnostic Overview enrichment (display-only overlays; no Airtable writes).
    // Fail-open: enrichment errors must not break legacy census hotel detail.
    try {
      const enrichment = await buildHotelExplorerOverviewEnrichment(recordId, {
        hotelDto: formatted,
        audience: "internal",
        skipLiveHpc: true,
        skipHiAirtable: true,
      });
      formatted = applyOverviewEnrichmentToHotel(formatted, enrichment);
      if (
        Array.isArray(formatted.verifiedAmenitiesDisplay) &&
        formatted.verifiedAmenitiesDisplay.length &&
        (!Array.isArray(formatted.amenitiesDisplay) ||
          formatted.amenitiesDisplay.length === 0)
      ) {
        formatted.amenitiesDisplay = formatted.verifiedAmenitiesDisplay.map((item) => ({
          id: item.id || null,
          label: item.label,
        }));
      } else if (
        Array.isArray(formatted.verifiedAmenitiesDisplay) &&
        formatted.verifiedAmenitiesDisplay.length &&
        Array.isArray(formatted.amenitiesDisplay)
      ) {
        const seen = new Set(
          formatted.amenitiesDisplay.map((a) =>
            String(a?.label || "")
              .toLowerCase()
              .trim()
          )
        );
        for (const item of formatted.verifiedAmenitiesDisplay) {
          const label = String(item?.label || "").trim();
          if (!label) continue;
          const key = label.toLowerCase();
          if (seen.has(key)) continue;
          seen.add(key);
          formatted.amenitiesDisplay.push({ id: item.id || null, label });
        }
      }
      if (!formatted.amenities && Array.isArray(formatted.amenitiesDisplay)) {
        // Keep amenities text parse path available for HDP resolveAmenitiesDisplay.
        const labels = formatted.amenitiesDisplay
          .map((a) => a?.label)
          .filter(Boolean);
        if (labels.length) formatted.amenities = labels.join("; ");
      }
    } catch (enrichErr) {
      console.warn(
        "[brand-presence] overview enrichment skipped",
        recordId,
        enrichErr?.message || enrichErr
      );
    }

    // HPC: re-assert Parent Company / Brand Family over ADP parent overlay, and
    // attach explicit provenance for the five Gate 1 Overview audit fields.
    if (owningTableKind === "HPC") {
      attachOverviewFieldProvenance(formatted, {
        hpcFields: identity.record?.fields || {},
        owningTableKind,
      });
      // Strip ADP-only parent provenance when HPC Brand Family / Parent Company wins.
      if (
        formatted.fieldProvenance?.parentCompany?.class === "VERIFIED_HPC" &&
        formatted.parentCompanyProvenance?.provenance ===
          "adp_property_profile_parent_company"
      ) {
        delete formatted.parentCompanyProvenance;
      }
    }

    res.json({ success: true, hotel: formatted });
  } catch (error) {
    const fixture = fixtureHotelResponse(recordId);
    if (fixture) {
      console.warn(
        "[brand-presence] hotel detail falling back to golden-demo fixture",
        recordId,
        error?.message || error
      );
      return res.json(fixture);
    }
    const status = error?.statusCode === 404 ? 404 : 500;
    console.error("Error getting brand presence hotel detail:", error);
    res.status(status).json({
      success: false,
      error: status === 404 ? "Hotel record not found" : "Internal Server Error",
      details: error.message
    });
  }
}

function escapeAirtableString(value) {
  return String(value || "").replace(/'/g, "\\'");
}

// Get brand presence data
export async function getBrandPresence(req, res) {
  const requestedLimit = parseInt(req.query.limit, 10);
  const limit = Number.isFinite(requestedLimit) && requestedLimit > 0 ? requestedLimit : null;
  const { brand, status, region, search, page = 0 } = req.query;
  const country = String(req.query.country || "").trim();

  function fixtureSearchResponse() {
    if (!shouldUseMexicoRadarFixtureFallback({ search, country: search })) return null;
    const hotels = searchFixtureHotels(search || "", { limit: limit || 400 });
    if (!hotels.length) return null;
    return {
      success: true,
      hotels,
      statistics: {},
      insights: [],
      totalCount: hotels.length,
      totalWithCoordinates: hotels.filter(
        (h) => Number.isFinite(h.lat) && Number.isFinite(h.lng) && (h.lat !== 0 || h.lng !== 0)
      ).length,
      skippedNoCoordinates: 0,
      hasMore: false,
      page: parseInt(page, 10) || 0,
      limit,
      cached: false,
      source: "golden_demo_radar_fixture",
    };
  }

  if (!base) {
    const fixture = fixtureSearchResponse();
    if (fixture) return res.json(fixture);
    return res.status(500).json({ error: "Missing Airtable API key or base id for brand presence" });
  }

  try {
    // Check cache first
    const cacheKey = getCacheKey({ brand, status, region, country, search, limit, page });
    const cachedData = getFromCache(cacheKey);
    
    if (cachedData) {
      console.log('📦 Serving from cache:', cacheKey);
      return res.json(cachedData);
    }
    
    console.log('🔄 Fetching from Airtable:', { brand, status, region, country, search, limit, page });
    
    // Build filter formula
    let filterFormula = '';
    const conditions = [];
    
    if (brand) {
      conditions.push(`{${F.hotels.brand}} = '${escapeAirtableString(brand)}'`);
    }
    
    if (status) {
      conditions.push(`{${F.hotels.status}} = '${escapeAirtableString(status)}'`);
    } else {
      // HPC Radar Status: exclude Hidden/blank from map inventory (Open|Pipeline only).
      conditions.push(
        `OR({${F.hotels.status}}='Open',{${F.hotels.status}}='Pipeline')`
      );
    }
    
    if (region) {
      conditions.push(`{${F.hotels.region}} = '${escapeAirtableString(region)}'`);
    }

    // Share / scoped reads: filter at source so Mexico packs do not download all markets.
    if (country) {
      conditions.push(`{${F.hotels.country}} = '${escapeAirtableString(country)}'`);
    }
    
    if (search) {
      const safeSearch = escapeAirtableString(search);
      const searchConditions = [
        `SEARCH('${safeSearch}', {${F.hotels.name}})`,
        `SEARCH('${safeSearch}', {${F.hotels.city}})`,
        `SEARCH('${safeSearch}', {${F.hotels.country}})`
      ];
      conditions.push(`OR(${searchConditions.join(', ')})`);
    }
    
    if (conditions.length > 0) {
      filterFormula = `AND(${conditions.join(', ')})`;
    }
    
    // Bulk map load — lean field set; detail panel fetches full record per hotel on click
    const selectOptions = {
      fields: MAP_HOTEL_FIELDS,
      pageSize: 100, // Airtable's optimal page size
      sort: [{ field: F.hotels.name, direction: 'asc' }]
    };
    if (limit) {
      selectOptions.maxRecords = limit;
    }
    
    // Only add filterByFormula if we have a valid filter
    if (filterFormula && filterFormula.trim() !== '') {
      selectOptions.filterByFormula = filterFormula;
    }

    let hotels;
    try {
      hotels = await base(F.hotels.table).select(selectOptions).all();
    } catch (selectErr) {
      const msg = String(selectErr?.message || selectErr || "");
      if (
        selectErr?.error === "UNKNOWN_FIELD_NAME" ||
        /Unknown field name/i.test(msg)
      ) {
        selectOptions.fields = MAP_HOTEL_FIELDS.filter(
          (field) => field !== F.hotels.market && field !== F.hotels.submarket
        );
        hotels = await base(F.hotels.table).select(selectOptions).all();
      } else {
        throw selectErr;
      }
    }
    
    // Format response; track valid coordinates for diagnostics
    let skippedNoCoordinates = 0;
    const formattedHotels = hotels.map((hotel) => {
      const formatted = formatHotelRecord(hotel);
      const hasCoords = Number.isFinite(formatted.lat) && Number.isFinite(formatted.lng)
        && (formatted.lat !== 0 || formatted.lng !== 0);
      if (!hasCoords) skippedNoCoordinates++;
      return formatted;
    });
    
    // Calculate statistics
    const stats = calculateStatistics(formattedHotels);
    
    // Generate insights
    const insights = generateInsights(formattedHotels);
    
    const response = {
      success: true,
      hotels: formattedHotels,
      statistics: stats,
      insights: insights,
      totalCount: formattedHotels.length,
      totalWithCoordinates: formattedHotels.length - skippedNoCoordinates,
      skippedNoCoordinates,
      hasMore: !!limit && hotels.length === limit,
      page: parseInt(page, 10),
      limit,
      cached: false,
      censusSourceTable: HOTEL_PROPERTY_CENSUS_TABLE,
      censusSourceTableId: "tbl9aY5ijiuIzzWam",
      legacyHotelCensusRuntimeDependency: false,
    };

    // Cache the response
    setCache(cacheKey, response);
    console.log('💾 Cached response for:', cacheKey);

    res.json(response);
    
  } catch (error) {
    const fixture = fixtureSearchResponse();
    if (fixture || isAirtableCredentialError(error)) {
      if (fixture) {
        console.warn(
          "[brand-presence] search falling back to golden-demo fixture",
          search,
          error?.message || error
        );
        return res.json(fixture);
      }
    }
    console.error("Error getting brand presence data:", error);
    res.status(500).json({ 
      error: "Internal Server Error",
      details: error.message
    });
  }
}

// Get brand statistics
export async function getBrandStatistics(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const { region } = req.query;
    
    // Build filter for region if specified
    let filterFormula = '';
    if (region) {
      filterFormula = `{${F.hotels.region}} = '${region}'`;
    }
    
    // Fetch all hotels for statistics
    const selectOptions = {
      fields: [F.hotels.brand, F.hotels.status, F.hotels.rooms, F.hotels.region]
    };
    
    // Only add filterByFormula if we have a valid filter
    if (filterFormula && filterFormula.trim() !== '') {
      selectOptions.filterByFormula = filterFormula;
    }
    
    const hotels = await base(F.hotels.table)
      .select(selectOptions)
      .all();
    
    // Calculate brand statistics
    const brandStats = {};
    const regionStats = {};
    const statusStats = { open: 0, pipeline: 0, candidate: 0 };
    let totalRooms = 0;
    
    hotels.forEach(hotel => {
      const brand = hotel.fields[F.hotels.brand];
      const status = hotel.fields[F.hotels.status];
      const rooms = hotel.fields[F.hotels.rooms] || 0;
      const region = hotel.fields[F.hotels.region];
      
      // Brand statistics
      if (!brandStats[brand]) {
        brandStats[brand] = { total: 0, open: 0, pipeline: 0, candidate: 0, rooms: 0 };
      }
      brandStats[brand].total++;
      brandStats[brand][status]++;
      brandStats[brand].rooms += rooms;
      
      // Region statistics
      if (!regionStats[region]) {
        regionStats[region] = { total: 0, brands: new Set() };
      }
      regionStats[region].total++;
      regionStats[region].brands.add(brand);
      
      // Status statistics
      statusStats[status]++;
      totalRooms += rooms;
    });
    
    // Convert region stats to include brand count
    Object.keys(regionStats).forEach(region => {
      regionStats[region].brandCount = regionStats[region].brands.size;
      delete regionStats[region].brands;
    });
    
    res.json({
      success: true,
      brandStatistics: brandStats,
      regionStatistics: regionStats,
      statusStatistics: statusStats,
      totalHotels: hotels.length,
      totalRooms: totalRooms,
      averageRooms: hotels.length > 0 ? Math.round(totalRooms / hotels.length) : 0
    });
    
  } catch (error) {
    console.error("Error getting brand statistics:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Get white space opportunities
export async function getWhiteSpaceOpportunities(req, res) {
  try {
    const { region, minPopulation = 100000 } = req.query;
    
    // This would typically integrate with a cities database
    // For now, we'll return mock data
    const opportunities = [
      {
        city: "Santa Marta",
        country: "Colombia",
        region: "CALA",
        population: 500000,
        currentBrands: ["Hilton"],
        opportunityScore: 85,
        recommendedBrands: ["Marriott", "Hyatt", "Choice Hotels"]
      },
      {
        city: "Managua",
        country: "Nicaragua",
        region: "CALA",
        population: 1.5e6,
        currentBrands: [],
        opportunityScore: 92,
        recommendedBrands: ["Hilton", "Marriott", "IHG"]
      },
      {
        city: "Tegucigalpa",
        country: "Honduras",
        region: "CALA",
        population: 1.2e6,
        currentBrands: ["Holiday Inn"],
        opportunityScore: 78,
        recommendedBrands: ["Marriott", "Hilton", "Choice Hotels"]
      }
    ];
    
    res.json({
      success: true,
      opportunities: opportunities,
      totalOpportunities: opportunities.length
    });
    
  } catch (error) {
    console.error("Error getting white space opportunities:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Calculate statistics
function calculateStatistics(hotels) {
  const totalHotels = hotels.length;
  const openHotels = hotels.filter(h => h.status === 'open').length;
  const pipelineHotels = hotels.filter(h => h.status === 'pipeline').length;
  const candidateHotels = hotels.filter(h => h.status === 'candidate').length;
  
  const totalRooms = hotels.reduce((sum, h) => sum + (h.rooms || 0), 0);
  const averageRooms = totalHotels > 0 ? Math.round(totalRooms / totalHotels) : 0;
  
  // Brand distribution
  const brandCounts = {};
  hotels.forEach(hotel => {
    brandCounts[hotel.brand] = (brandCounts[hotel.brand] || 0) + 1;
  });
  
  // Region distribution
  const regionCounts = {};
  hotels.forEach(hotel => {
    regionCounts[hotel.region] = (regionCounts[hotel.region] || 0) + 1;
  });
  
  return {
    totalHotels,
    openHotels,
    pipelineHotels,
    candidateHotels,
    totalRooms,
    averageRooms,
    brandDistribution: brandCounts,
    regionDistribution: regionCounts
  };
}

// Generate insights
function generateInsights(hotels) {
  const insights = [];
  
  if (hotels.length === 0) {
    return [{
      priority: 'low',
      title: 'No Data Available',
      description: 'No hotels found matching the current filters.'
    }];
  }
  
  // Calculate brand distribution
  const brandCounts = {};
  hotels.forEach(hotel => {
    brandCounts[hotel.brand] = (brandCounts[hotel.brand] || 0) + 1;
  });
  
  const topBrand = Object.entries(brandCounts)
    .sort(([,a], [,b]) => b - a)[0];
  
  if (topBrand) {
    insights.push({
      priority: 'high',
      title: 'Market Leader',
      description: `${topBrand[0]} leads with ${topBrand[1]} properties in the selected region.`
    });
  }
  
  // Calculate pipeline vs open ratio
  const openCount = hotels.filter(h => h.status === 'open').length;
  const pipelineCount = hotels.filter(h => h.status === 'pipeline').length;
  const ratio = pipelineCount / openCount;
  
  if (ratio > 0.5) {
    insights.push({
      priority: 'medium',
      title: 'High Growth Activity',
      description: `Strong pipeline activity with ${pipelineCount} projects in development.`
    });
  }
  
  // Find white space opportunities
  const cities = [...new Set(hotels.map(h => h.city))];
  const citiesWithMultipleBrands = cities.filter(city => {
    const cityHotels = hotels.filter(h => h.city === city);
    const uniqueBrands = new Set(cityHotels.map(h => h.brand));
    return uniqueBrands.size > 1;
  });
  
  if (citiesWithMultipleBrands.length < cities.length * 0.5) {
    insights.push({
      priority: 'low',
      title: 'White Space Opportunities',
      description: `${cities.length - citiesWithMultipleBrands.length} cities have limited brand competition.`
    });
  }
  
  return insights;
}

// Get unique location types from Airtable
export async function getLocationTypes(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const hotels = await base(F.hotels.table)
      .select({
        fields: [F.hotels.locationType]
      })
      .all();
    
    // Extract unique location types
    const locationTypes = [...new Set(
      hotels
        .map(hotel => hotel.fields[F.hotels.locationType])
        .filter(type => type && type.trim() !== '')
    )].sort();
    
    res.json({
      success: true,
      locationTypes: locationTypes
    });
    
  } catch (error) {
    console.error("Error getting location types:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Get unique parent companies from Airtable
export async function getParentCompanies(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const hotels = await base(F.hotels.table)
      .select({
        fields: [F.hotels.parentCompany]
      })
      .all();
    
    // Extract unique parent companies
    const parentCompanies = [...new Set(
      hotels
        .map(hotel => hotel.fields[F.hotels.parentCompany])
        .filter(company => company && company.trim() !== '')
    )].sort();
    
    res.json({
      success: true,
      parentCompanies: parentCompanies
    });
    
  } catch (error) {
    console.error("Error getting parent companies:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Get unique brands from Airtable
export async function getBrands(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const hotels = await base(F.hotels.table)
      .select({
        fields: [F.hotels.brand]
      })
      .all();
    
    // Extract unique brands
    const brands = [...new Set(
      hotels
        .map(hotel => hotel.fields[F.hotels.brand])
        .filter(brand => brand && brand.trim() !== '')
    )].sort();
    
    res.json({
      success: true,
      brands: brands
    });
    
  } catch (error) {
    console.error("Error getting brands:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Get chain scales (property types) from Airtable
export async function getChainScales(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const hotels = await base(F.hotels.table)
      .select({
        fields: [F.hotels.propertyType]
      })
      .all();
    
    // Extract unique chain scales
    const chainScales = [...new Set(
      hotels
        .map(hotel => hotel.fields[F.hotels.propertyType])
        .filter(scale => scale && scale.trim() !== '')
    )].sort();
    
    res.json({
      success: true,
      chainScales: chainScales
    });
    
  } catch (error) {
    console.error("Error getting chain scales:", error);
    res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message
    });
  }
}

// Export data
export async function exportBrandPresenceData(req, res) {
  try {
    const { format = 'csv', brand, status, region } = req.query;
    
    // Get filtered data
    const response = await getBrandPresence(req, res);
    
    if (format === 'csv') {
      const csvContent = convertToCSV(response.hotels);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename="brand-presence-data.csv"');
      res.send(csvContent);
    } else if (format === 'json') {
      res.json(response);
    } else {
      res.status(400).json({ error: 'Unsupported format. Use csv or json.' });
    }
    
  } catch (error) {
    console.error("Error exporting brand presence data:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Convert to CSV
function convertToCSV(hotels) {
  const headers = [
    'Name', 'Brand', 'Status', 'City', 'Country', 'Region', 
    'Rooms', 'STR Number', 'Chain Scale', 'Project Phase'
  ];
  
  const rows = hotels.map(hotel => [
    hotel.name,
    hotel.brand,
    hotel.status,
    hotel.city,
    hotel.country,
    hotel.region,
    hotel.rooms,
    hotel.strNumber || '',
    hotel.chainScale || '',
    hotel.projectPhase || ''
  ]);
  
  return [headers, ...rows].map(row => 
    row.map(field => `"${field}"`).join(',')
  ).join('\n');
}
