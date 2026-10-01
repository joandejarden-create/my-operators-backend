/**
 * Canonical sparse Hotel Census map DTO — single source of truth for Radar/Scout
 * `view=map` payloads and the census-map filesystem snapshot.
 *
 * Do not duplicate MAP_HOTEL_FIELDS / formatMapHotelRecord elsewhere.
 */

/** Field map (Airtable column names) used by map DTO only. */
export const MAP_HOTEL_FIELD_KEYS = Object.freeze({
  name: "name",
  brand: "Affiliation",
  parentCompany: "Parent Company",
  status: "status",
  lat: "Latitude",
  lng: "Longitude",
  city: "city",
  country: "country",
  region: "Region",
  locationType: "Location",
  rooms: "rooms",
  strNumber: "STR Number",
  chainScale: "Chain Scale",
  projectPhase: "project_phase",
  propertyType: "Chain Scale",
  operationType: "Operation Type",
  managementCompany: "Management Company",
  market: "Market",
  submarket: "Submarket",
  censusPropertyType: "Property Type",
  hotelServiceModel: "Hotel Service Model",
});

/** Airtable `fields:` list for bulk map loads — keep lean. */
export const MAP_HOTEL_FIELDS = Object.freeze([
  MAP_HOTEL_FIELD_KEYS.name,
  MAP_HOTEL_FIELD_KEYS.brand,
  MAP_HOTEL_FIELD_KEYS.parentCompany,
  MAP_HOTEL_FIELD_KEYS.status,
  MAP_HOTEL_FIELD_KEYS.lat,
  MAP_HOTEL_FIELD_KEYS.lng,
  MAP_HOTEL_FIELD_KEYS.city,
  MAP_HOTEL_FIELD_KEYS.country,
  MAP_HOTEL_FIELD_KEYS.region,
  MAP_HOTEL_FIELD_KEYS.locationType,
  MAP_HOTEL_FIELD_KEYS.rooms,
  MAP_HOTEL_FIELD_KEYS.strNumber,
  MAP_HOTEL_FIELD_KEYS.chainScale,
  MAP_HOTEL_FIELD_KEYS.projectPhase,
  MAP_HOTEL_FIELD_KEYS.propertyType,
  MAP_HOTEL_FIELD_KEYS.operationType,
  MAP_HOTEL_FIELD_KEYS.managementCompany,
  MAP_HOTEL_FIELD_KEYS.market,
  MAP_HOTEL_FIELD_KEYS.submarket,
  MAP_HOTEL_FIELD_KEYS.censusPropertyType,
  MAP_HOTEL_FIELD_KEYS.hotelServiceModel,
]);

/** Bump when map DTO shape changes (invalidates in-memory + snapshot schema). */
export const MAP_DTO_SCHEMA_VERSION = "map1";

export const CENSUS_MAP_TABLE = "Hotel Census";

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

function omitEmptyMapFields(obj) {
  const out = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value == null || value === "") continue;
    out[key] = value;
  }
  return out;
}

/**
 * Sparse hotel DTO for Radar/Scout map loads.
 * Detail panels must use GET /api/brand-presence/hotel/:recordId for full records.
 * @param {{ id: string, fields?: Record<string, unknown> }} hotel Airtable record-like
 */
export function formatMapHotelRecord(hotel) {
  const F = MAP_HOTEL_FIELD_KEYS;
  const fields = hotel.fields || {};
  const lat = parseFloat(fields[F.lat]);
  const lng = parseFloat(fields[F.lng]);
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng) && (lat !== 0 || lng !== 0);
  const roomsRaw = parseInt(fields[F.rooms], 10);

  return omitEmptyMapFields({
    id: hotel.id,
    name: readTextField(fields, F.name) || "Unknown Hotel",
    brand: readTextField(fields, F.brand) || "Unknown Brand",
    parentCompany: readTextField(fields, F.parentCompany) || "Unknown",
    status: readTextField(fields, F.status) || "unknown",
    lat: hasCoords ? lat : 0,
    lng: hasCoords ? lng : 0,
    city: readTextField(fields, F.city) || "Unknown City",
    country: readTextField(fields, F.country) || "Unknown Country",
    region: readTextField(fields, F.region) || "Unknown Region",
    locationType: readTextField(fields, F.locationType),
    rooms: Number.isFinite(roomsRaw) ? roomsRaw : 0,
    chainScale: readTextField(fields, F.chainScale),
    projectPhase: readTextField(fields, F.projectPhase),
    propertyType: readTextField(fields, F.propertyType),
    operationType: readTextField(fields, F.operationType),
    managementCompany: readTextField(fields, F.managementCompany),
    market: readTextField(fields, F.market),
    submarket: readTextField(fields, F.submarket),
    censusPropertyType: readTextField(fields, F.censusPropertyType),
    hotelServiceModel: readTextField(fields, F.hotelServiceModel),
  });
}

/**
 * Fetch + format full census map universe from Airtable (same select as brand-presence map path).
 * @param {import('airtable').Base} base
 * @param {{ maxRecords?: number, onPage?: (n: number, pageSize: number) => void }} [opts]
 */
export async function fetchCanonicalMapHotelsFromAirtable(base, opts = {}) {
  const selectOptions = {
    fields: [...MAP_HOTEL_FIELDS],
    pageSize: 100,
    sort: [{ field: MAP_HOTEL_FIELD_KEYS.name, direction: "asc" }],
  };
  if (opts.maxRecords) selectOptions.maxRecords = opts.maxRecords;

  let hotels;
  try {
    if (typeof opts.onPage === "function" && typeof base(CENSUS_MAP_TABLE).select(selectOptions).eachPage === "function") {
      hotels = [];
      let pageNum = 0;
      await base(CENSUS_MAP_TABLE)
        .select(selectOptions)
        .eachPage((records, fetchNextPage) => {
          pageNum += 1;
          hotels.push(...records);
          opts.onPage(pageNum, records.length);
          fetchNextPage();
        });
    } else {
      hotels = await base(CENSUS_MAP_TABLE).select(selectOptions).all();
    }
  } catch (selectErr) {
    const msg = String(selectErr?.message || selectErr || "");
    if (selectErr?.error === "UNKNOWN_FIELD_NAME" || /Unknown field name/i.test(msg)) {
      selectOptions.fields = MAP_HOTEL_FIELDS.filter(
        (field) => field !== MAP_HOTEL_FIELD_KEYS.market && field !== MAP_HOTEL_FIELD_KEYS.submarket
      );
      hotels = await base(CENSUS_MAP_TABLE).select(selectOptions).all();
    } else {
      throw selectErr;
    }
  }

  let skippedNoCoordinates = 0;
  const formattedHotels = hotels.map((hotel) => {
    const formatted = formatMapHotelRecord(hotel);
    const hasCoords =
      Number.isFinite(formatted.lat) &&
      Number.isFinite(formatted.lng) &&
      (formatted.lat !== 0 || formatted.lng !== 0);
    if (!hasCoords) skippedNoCoordinates += 1;
    return formatted;
  });

  return {
    hotels: formattedHotels,
    totalCount: formattedHotels.length,
    totalWithCoordinates: formattedHotels.length - skippedNoCoordinates,
    skippedNoCoordinates,
  };
}
