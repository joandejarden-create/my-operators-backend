/**
 * Golden-demo Radar fixture fallback for Mexico Hotel Explorer share packs.
 * Used when live Airtable / Demand Anchors / Travel Infrastructure tables are unavailable.
 * Read-only. No Census writes.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const RADAR_FIXTURE_DIR = path.join(ROOT, "fixtures/golden-demo/radar");
const PUBLIC_FIXTURES = path.join(ROOT, "public/fixtures");

export const SHERATON_GDL_ID = "recsYJb2R1jarPpK3";
export const VOCO_CANCUN_ID = "recTYaiA4S6fR6ixx";
export const MEXICO_RADAR_HOTEL_IDS = new Set([SHERATON_GDL_ID, VOCO_CANCUN_ID]);

const HOTEL_PRESENTATION = {
  [SHERATON_GDL_ID]: {
    name: "Sheraton Guadalajara Expo",
    brand: "Sheraton Hotel",
    parentCompany: "Marriott International",
    managementCompany: "Aimbridge LATAM",
    chainScale: "Upper Upscale Chain",
    market: "Guadalajara Expo",
    submarket: "Guadalajara",
    city: "Zapopan",
    country: "Mexico",
    website:
      "https://www.marriott.com/en-us/hotels/gdlse-sheraton-guadalajara-expo/overview",
  },
  [VOCO_CANCUN_ID]: {
    name: "voco Cancún Zona Hotelera",
    brand: "voco",
    parentCompany: "IHG Hotels & Resorts",
    managementCompany: "Aimbridge LATAM",
    chainScale: "Upper Midscale Chain",
    market: "Cancún Zona Hotelera",
    submarket: "Cancún Hotel Zone",
    city: "Cancún",
    country: "Mexico",
    website: "https://www.ihg.com/voco/hotels/us/en/cancun/cuncn/hoteldetail",
  },
};

const jsonCache = new Map();

function readJson(filePath) {
  const key = String(filePath);
  if (jsonCache.has(key)) return jsonCache.get(key);
  if (!fs.existsSync(filePath)) {
    jsonCache.set(key, null);
    return null;
  }
  try {
    let raw = fs.readFileSync(filePath, "utf8");
    if (raw.charCodeAt(0) === 0xfeff) raw = raw.slice(1);
    const data = JSON.parse(raw);
    jsonCache.set(key, data);
    return data;
  } catch (err) {
    console.warn("[mexico-radar-fixture] failed to read", filePath, err?.message || err);
    jsonCache.set(key, null);
    return null;
  }
}

function normSearch(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase();
}

function toNum(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function normalizeFixturePoint(raw, idx, prefix) {
  if (!raw || typeof raw !== "object") return null;
  const lat = toNum(raw.latitude ?? raw.lat);
  const lng = toNum(raw.longitude ?? raw.lng);
  if (lat == null || lng == null || (lat === 0 && lng === 0)) return null;
  const pointType = String(raw.pointType || raw.type || "Unknown").trim() || "Unknown";
  return {
    id: raw.id || `${prefix}_${idx}`,
    name: String(raw.name || "Unnamed").trim(),
    pointType,
    type: pointType,
    pointSubtype: raw.pointSubtype || "",
    city: raw.city || "",
    country: raw.country || "Mexico",
    region: raw.region || "North America",
    submarket: raw.submarket || "",
    latitude: lat,
    longitude: lng,
    lat,
    lng,
    demandRelevance: raw.demandRelevance || "",
    dataConfidence: raw.dataConfidence || "",
    source: Array.isArray(raw.source) ? raw.source : raw.source ? [raw.source] : ["Public Source"],
    sourceReference: raw.sourceReference || "",
    notes: raw.notes || "",
    includeOnRadarMap: raw.includeOnRadarMap !== false,
    mapIconType: raw.mapIconType || pointType,
    hotelDemandRationale: raw.projectRelevanceLogic || raw.hotelDemandRationale || "",
    lastVerified: raw.lastVerified || (raw.verification && raw.verification.verifiedAt) || "",
  };
}

function loadPointsFromPublicFixtures(fileNames, prefix) {
  const out = [];
  for (const name of fileNames) {
    const data = readJson(path.join(PUBLIC_FIXTURES, name));
    const points = (data && Array.isArray(data.points) ? data.points : []) || [];
    points.forEach((p, i) => {
      const n = normalizeFixturePoint(p, `${name}_${i}`, prefix);
      if (n) out.push(n);
    });
  }
  return out;
}

function dedupePoints(points) {
  const seen = new Set();
  const out = [];
  for (const p of points) {
    const key = `${normSearch(p.name)}|${p.latitude}|${p.longitude}|${p.pointType}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(p);
  }
  return out;
}

function filterByCountryMarket(points, { country, market } = {}) {
  let rows = points.slice();
  const c = normSearch(country);
  if (c) {
    rows = rows.filter((p) => normSearch(p.country).includes(c) || c.includes(normSearch(p.country)));
  }
  const m = normSearch(market);
  if (m) {
    const marketHits = rows.filter((p) => {
      const blob = `${p.city || ""} ${p.submarket || ""} ${p.name || ""}`;
      return normSearch(blob).includes(m) || m.includes(normSearch(p.city));
    });
    // Keep market filter soft — country pack still useful when market label differs.
    if (marketHits.length) rows = marketHits;
  }
  return rows;
}

export function isAirtableCredentialError(err) {
  const msg = String(err?.message || err || "");
  return /valid api key|AUTHENTICATION_REQUIRED|UNAUTHORIZED|401|403|Invalid authentication/i.test(
    msg
  );
}

export function isMexicoRadarFixtureHotelId(recordId) {
  return MEXICO_RADAR_HOTEL_IDS.has(String(recordId || "").trim());
}

export function getFixtureHotelById(recordId) {
  const id = String(recordId || "").trim();
  if (!isMexicoRadarFixtureHotelId(id)) return null;
  const data = readJson(path.join(RADAR_FIXTURE_DIR, `hotel-${id}.json`));
  const hotel = data && data.hotel ? { ...data.hotel } : null;
  if (!hotel) return null;
  hotel.id = hotel.id || id;
  hotel.recordId = hotel.recordId || id;
  const presentation = HOTEL_PRESENTATION[id];
  if (presentation) {
    // Identity / geography from presentation; keep richer fixture narrative when present.
    const keepDescription = String(hotel.hotelDescription || "").trim();
    const keepAmenities = String(hotel.amenities || "").trim();
    const keepAmenitiesDisplay = Array.isArray(hotel.amenitiesDisplay)
      ? hotel.amenitiesDisplay
      : null;
    Object.assign(hotel, presentation);
    if (keepDescription && keepDescription.length >= String(presentation.hotelDescription || "").length) {
      hotel.hotelDescription = keepDescription;
    }
    if (keepAmenities && keepAmenities.length >= String(presentation.amenities || "").length) {
      hotel.amenities = keepAmenities;
    }
    if (keepAmenitiesDisplay && keepAmenitiesDisplay.length) {
      hotel.amenitiesDisplay = keepAmenitiesDisplay;
    }
    hotel.latitude = toNum(hotel.latitude ?? hotel.lat);
    hotel.longitude = toNum(hotel.longitude ?? hotel.lng);
    hotel.lat = hotel.latitude;
    hotel.lng = hotel.longitude;
  }
  hotel._fixtureSource = "golden_demo_radar_fixture";
  return hotel;
}

function loadAreaHotelPack(fileName) {
  const data = readJson(path.join(RADAR_FIXTURE_DIR, fileName));
  const hotels = (data && Array.isArray(data.hotels) ? data.hotels : []).map((h) => {
    const lat = toNum(h.lat ?? h.latitude);
    const lng = toNum(h.lng ?? h.longitude);
    const row = {
      ...h,
      lat: lat ?? 0,
      lng: lng ?? 0,
      latitude: lat,
      longitude: lng,
    };
    if (row.id === VOCO_CANCUN_ID || row.id === SHERATON_GDL_ID) {
      const presented = getFixtureHotelById(row.id);
      if (presented) {
        return {
          ...row,
          name: presented.name,
          brand: presented.brand,
          parentCompany: presented.parentCompany,
          managementCompany: presented.managementCompany,
          chainScale: presented.chainScale,
          market: presented.market,
          submarket: presented.submarket,
          website: presented.website,
        };
      }
    }
    return row;
  });
  return hotels;
}

export function searchFixtureHotels(search, { limit = 400 } = {}) {
  const q = normSearch(search);
  if (!q) return [];

  let packs = [];
  if (q.includes("cancun") || q.includes("cancún") || q === "mexico") {
    packs = packs.concat(loadAreaHotelPack("area-hotels-cancun.json"));
  }
  if (
    q.includes("zapopan") ||
    q.includes("guadalajara") ||
    q.includes("gdl") ||
    q.includes("pacific central") ||
    q === "mexico"
  ) {
    packs = packs.concat(loadAreaHotelPack("area-hotels-zapopan.json"));
    packs = packs.concat(loadAreaHotelPack("area-hotels-guadalajara.json"));
  }

  if (!packs.length) {
    // Soft match on city/name across both packs for unexpected query shapes.
    packs = packs
      .concat(loadAreaHotelPack("area-hotels-cancun.json"))
      .concat(loadAreaHotelPack("area-hotels-zapopan.json"))
      .concat(loadAreaHotelPack("area-hotels-guadalajara.json"))
      .filter((h) => {
        const blob = normSearch(`${h.name} ${h.city} ${h.market} ${h.submarket} ${h.country}`);
        return blob.includes(q);
      });
  }

  const seen = new Set();
  const out = [];
  for (const h of packs) {
    if (!h || !h.id || seen.has(h.id)) continue;
    seen.add(h.id);
    out.push(h);
    if (out.length >= limit) break;
  }
  return out;
}

export function loadMexicoDemandAnchorFixturePoints(query = {}) {
  const files = [
    "demand-anchors-guadalajara-real.json",
    "demand-anchors-mexico-cancun-riviera-maya-real.json",
    "demand-anchors-cancun.json",
    "demand-anchors-mexico-cancun-riviera-maya-delta-real.json",
    "demand-anchors-mexico-secondary-markets-real.json",
  ];
  const points = dedupePoints(loadPointsFromPublicFixtures(files, "da"));
  return filterByCountryMarket(points, query);
}

export function loadMexicoTravelInfrastructureFixturePoints(query = {}) {
  const files = [
    "travel-infrastructure-guadalajara-real.json",
    "travel-infrastructure-mexico-cancun-riviera-maya-delta-real.json",
    "travel-infrastructure-mexico-secondary-markets-real.json",
  ];
  const points = dedupePoints(loadPointsFromPublicFixtures(files, "ti"));
  return filterByCountryMarket(points, query);
}

function isOpenStatus(status) {
  const s = String(status || "").trim().toLowerCase();
  return !s || s === "open" || s === "operating" || s === "active";
}

function isPipelineStatus(status, phase) {
  const blob = `${status || ""} ${phase || ""}`.toLowerCase();
  return /pipeline|proposed|under construction|planned|pre-opening|announced/.test(blob);
}

/**
 * Scout-shaped coverage report from area-hotel fixtures (no Airtable).
 */
export function buildFixtureMarketCoverage(query = {}) {
  const market = String(query.market || query.strMarket || "").trim();
  const submarket = String(query.submarket || query.strSubmarket || "").trim();
  const city = String(query.city || "").trim();
  const country = String(query.country || "Mexico").trim() || "Mexico";

  const searchKey = city || market || submarket || "Mexico";
  let hotels = searchFixtureHotels(searchKey, { limit: 500 });
  if (submarket) {
    const sm = normSearch(submarket);
    const filtered = hotels.filter((h) => normSearch(h.submarket || "").includes(sm));
    if (filtered.length) hotels = filtered;
  }

  let openHotels = 0;
  let openRooms = 0;
  let pipelineHotels = 0;
  let pipelineRooms = 0;
  const brands = new Set();
  const parents = new Set();
  const byBrand = {};
  const byChainScale = {};
  const byParentCompany = {};
  const byStatus = {};

  function bumpBucket(map, key, rooms, kind) {
    const label = key || "Unknown";
    if (!map[label]) {
      map[label] = {
        label,
        hotels: 0,
        rooms: 0,
        openHotels: 0,
        openRooms: 0,
        pipelineHotels: 0,
        pipelineRooms: 0,
      };
    }
    const row = map[label];
    row.hotels += 1;
    row.rooms += rooms;
    if (kind === "pipeline") {
      row.pipelineHotels += 1;
      row.pipelineRooms += rooms;
    } else if (kind === "open") {
      row.openHotels += 1;
      row.openRooms += rooms;
    }
  }

  function sortedBuckets(map) {
    return Object.values(map).sort(
      (a, b) => b.hotels - a.hotels || b.rooms - a.rooms || String(a.label).localeCompare(String(b.label))
    );
  }

  for (const h of hotels) {
    const rooms = Number(h.rooms) || 0;
    let kind = "other";
    if (isPipelineStatus(h.status, h.projectPhase)) {
      pipelineHotels += 1;
      pipelineRooms += rooms;
      kind = "pipeline";
    } else if (isOpenStatus(h.status)) {
      openHotels += 1;
      openRooms += rooms;
      kind = "open";
    }
    if (h.brand) {
      brands.add(h.brand);
      bumpBucket(byBrand, h.brand, rooms, kind);
    }
    if (h.parentCompany) {
      parents.add(h.parentCompany);
      bumpBucket(byParentCompany, h.parentCompany, rooms, kind);
    }
    if (h.chainScale) bumpBucket(byChainScale, h.chainScale, rooms, kind);
    bumpBucket(byStatus, kind === "other" ? String(h.status || "Unknown") : kind === "open" ? "Open" : "Pipeline", rooms, kind);
  }

  return {
    success: true,
    source: "golden_demo_radar_fixture",
    filters: { country, city, market, submarket },
    metrics: {
      openHotels,
      openRooms,
      pipelineHotels,
      pipelineRooms,
      totalHotels: openHotels + pipelineHotels,
      totalRooms: openRooms + pipelineRooms,
      candidateRooms: 0,
      brandCount: brands.size,
      parentCompanyCount: parents.size,
    },
    breakdowns: {
      byStatus: sortedBuckets(byStatus),
      byBrand: sortedBuckets(byBrand),
      byChainScale: sortedBuckets(byChainScale),
      byParentCompany: sortedBuckets(byParentCompany),
    },
  };
}

export function shouldUseMexicoRadarFixtureFallback(query = {}) {
  const country = normSearch(query.country);
  const market = normSearch(query.market);
  const city = normSearch(query.city);
  const search = normSearch(query.search);
  if (country.includes("mexico") || country === "mx") return true;
  if (/cancun|zapopan|guadalajara|gdl|pacific central/.test(`${market} ${city} ${search}`)) {
    return true;
  }
  return false;
}
