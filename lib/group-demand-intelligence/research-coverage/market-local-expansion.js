/**
 * Market-local demand-generator expansion — generic category framework +
 * hotel-location query plan. Discovers local entities from evidence; does NOT
 * hard-code city/country organizations into production logic.
 *
 * Geographic bands use hotel lat/lng + archetype catchment miles (not DMV/NYC).
 */

import {
  COUNTRY_LOCALE,
  LOCALE_PACKS,
  inferDemandArchetype,
} from "../discovery-recall-v4.js";
import { buildGenericHotelSeedInput } from "./onboard-hotel-research-graph.js";
import { TARGET_STATUS, TARGET_TYPE } from "./constants.js";
import crypto from "node:crypto";

/**
 * Map evidence entityKind → Research Target type (generic ontology).
 * Unknown kinds fall back to OTHER_MONITORED_SOURCE — never invent hotel/city types.
 */
export function mapEvidenceEntityKindToTargetType(entityKind) {
  const k = String(entityKind || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "_");
  const MAP = {
    DEMAND_GENERATOR: TARGET_TYPE.DEMAND_GENERATOR,
    PROGRAM: TARGET_TYPE.PROGRAM,
    EVENT_SERIES: TARGET_TYPE.EVENT_SERIES,
    ASSOCIATION: TARGET_TYPE.ASSOCIATION,
    GOVERNMENT_PROGRAM: TARGET_TYPE.GOVERNMENT_PROGRAM,
    CORPORATE_PROGRAM: TARGET_TYPE.CORPORATE_PROGRAM,
    SPORTS_SERIES: TARGET_TYPE.SPORTS_SERIES,
    TRAINING_PROGRAM: TARGET_TYPE.TRAINING_PROGRAM,
    OFFICIAL_CALENDAR: TARGET_TYPE.OFFICIAL_CALENDAR,
    EVENT_SOURCE: TARGET_TYPE.OFFICIAL_CALENDAR,
    HOUSING_PAGE: TARGET_TYPE.HOUSING_PAGE,
    VENUE_PAGE: TARGET_TYPE.VENUE_PAGE,
    VENUE: TARGET_TYPE.VENUE_PAGE,
    PRIVATE_EVENT_VENUE: TARGET_TYPE.PRIVATE_EVENT_VENUE,
    PROJECT_CONTRACT: TARGET_TYPE.PROJECT_CONTRACT,
    OTHER_MONITORED_SOURCE: TARGET_TYPE.OTHER_MONITORED_SOURCE,
    ORGANIZATION: TARGET_TYPE.DEMAND_GENERATOR,
  };
  return MAP[k] || TARGET_TYPE.OTHER_MONITORED_SOURCE;
}

export const MARKET_LOCAL_EXPANSION_VERSION = "gdi_market_local_expansion_v1";

/** Generic demand categories — framework only; entities come from evidence. */
export const MARKET_LOCAL_CATEGORIES = Object.freeze([
  "corporate",
  "luxury_fashion",
  "entertainment",
  "film_media",
  "financial_business",
  "professional_associations",
  "international_organizations",
  "government_institutional",
  "embassies_diplomatic",
  "universities",
  "medical",
  "technology",
  "sports",
  "trade_shows_exhibitions",
  "mice",
  "destination_events",
  "cultural_institutions",
  "private_events",
  "weddings",
  "venues",
  "transportation_linked",
  "recurring_programs",
  "large_projects",
  "training",
  "incentive_travel",
]);

export const CATCHMENT_BAND = Object.freeze({
  CORE: "CORE",
  NEARBY: "NEARBY",
  EXTENDED: "EXTENDED",
  DESTINATION_RELEVANT: "DESTINATION_RELEVANT",
  OUT_OF_MARKET: "OUT_OF_MARKET",
});

/** Practical km bands by demand archetype — not city-specific. */
export const CATCHMENT_KM_BY_ARCHETYPE = Object.freeze({
  LUXURY_DESTINATION_SMALL: { core: 4, nearby: 10, extended: 25 },
  URBAN_BUSINESS_MEETINGS: { core: 5, nearby: 12, extended: 30 },
  RESORT_DESTINATION: { core: 20, nearby: 50, extended: 120 },
  CORPORATE_SECONDARY: { core: 8, nearby: 20, extended: 45 },
  MIXED: { core: 6, nearby: 15, extended: 35 },
});

function clean(s) {
  return String(s || "").trim();
}

function slug(s) {
  return clean(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function isoCountry(country) {
  const c = clean(country).toUpperCase();
  if (c === "IT" || c.includes("ITALY") || c.includes("ITALIA")) return "IT";
  if (c === "US" || c.includes("UNITED STATES") || c === "USA") return "US";
  if (c === "DO" || c.includes("DOMINICAN")) return "DO";
  if (c === "MX" || c.includes("MEXICO")) return "MX";
  if (c === "CO" || c.includes("COLOMBIA")) return "CO";
  if (c === "BM" || c.includes("BERMUDA")) return "BM";
  if (c.length === 2) return c;
  return "DEFAULT";
}

/**
 * Classify a distance (km) into catchment band.
 */
export function classifyCatchmentBand(distanceKm, archetype = "MIXED") {
  if (distanceKm == null || !Number.isFinite(Number(distanceKm))) {
    return { band: CATCHMENT_BAND.DESTINATION_RELEVANT, detail: "distance_unknown" };
  }
  const d = Number(distanceKm);
  const bands =
    CATCHMENT_KM_BY_ARCHETYPE[archetype] || CATCHMENT_KM_BY_ARCHETYPE.MIXED;
  if (d <= bands.core) return { band: CATCHMENT_BAND.CORE, detail: `km<=${bands.core}` };
  if (d <= bands.nearby)
    return { band: CATCHMENT_BAND.NEARBY, detail: `km<=${bands.nearby}` };
  if (d <= bands.extended)
    return { band: CATCHMENT_BAND.EXTENDED, detail: `km<=${bands.extended}` };
  return { band: CATCHMENT_BAND.OUT_OF_MARKET, detail: `km>${bands.extended}` };
}

/**
 * Haversine distance in km.
 */
export function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (x) => (Number(x) * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/**
 * Build multilingual market-local discovery query tasks from hotel location.
 * Pure — no network. Entities are NOT invented here.
 */
export function buildMarketLocalDiscoveryPlan(hotelId, opts = {}) {
  const input = opts.input || buildGenericHotelSeedInput(hotelId, opts);
  const countryCode = isoCountry(input.country);
  const locale = COUNTRY_LOCALE[countryCode] || COUNTRY_LOCALE.DEFAULT;
  const languages = locale.languages || ["en"];
  const city = clean(input.city) || clean(input.market) || "hotel city";
  const market = clean(input.market) || city;
  const archetype = inferDemandArchetype(input.config || {}, {
    rooms: input.rooms,
  });
  const categories = opts.categories || MARKET_LOCAL_CATEGORIES;
  const yearHints = opts.yearHints || ["2026", "2027"];

  const tasks = [];
  for (const lang of languages) {
    const pack = LOCALE_PACKS[lang] || LOCALE_PACKS.en;
    for (const category of categories) {
      for (const year of yearHints) {
        const eventHint = pack.eventHints[0] || "conference";
        const openHint = pack.openHints[0] || "official hotel";
        tasks.push({
          taskId: `ml_${slug(countryCode)}_${slug(city)}_${slug(category)}_${lang}_${year}`,
          category,
          language: lang,
          hl: pack.hl,
          gl: locale.gl,
          year,
          query: [
            city,
            market,
            category.replace(/_/g, " "),
            eventHint,
            year,
            openHint,
          ]
            .filter(Boolean)
            .join(" "),
          sourceFamiliesPreferred: [
            "official_event_site",
            "official_venue_site",
            "association_program_site",
            "trade_show_organizer",
            "conference_center",
            "institutional_site",
            "corporate_event_page",
            "public_calendar",
            "tourism_convention",
            "government_institutional",
            "official_pdf",
            "registration_housing",
          ],
        });
      }
    }
  }

  return {
    ok: true,
    version: MARKET_LOCAL_EXPANSION_VERSION,
    hotelId: input.hotelId,
    hotelName: input.hotelName,
    city,
    market,
    country: input.country,
    countryCode,
    languages,
    archetype,
    catchmentBands: CATCHMENT_KM_BY_ARCHETYPE[archetype] || CATCHMENT_KM_BY_ARCHETYPE.MIXED,
    geo: {
      latitude: input.latitude,
      longitude: input.longitude,
    },
    categoryCount: categories.length,
    taskCount: tasks.length,
    tasks: tasks.slice(0, opts.maxTasks || 80),
    note:
      "Query plan only — market-local entities must be discovered from evidence; no city orgs hard-coded.",
  };
}

/**
 * Deduplicate proposed local entities by domain + normalized name.
 */
export function dedupeLocalEntities(entities = []) {
  const seen = new Set();
  const out = [];
  for (const e of entities) {
    const key = [
      slug(e.officialDomain || e.website || ""),
      slug(e.organizationName || e.programName || e.venueName || ""),
      slug(e.entityKind || "org"),
    ].join("|");
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(e);
  }
  return out;
}

/**
 * Convert evidence-backed local entities into research-target proposals.
 * Does not invent entities — caller must supply discovered rows.
 */
export function proposeMarketLocalTargets(hotelId, discoveredEntities = [], opts = {}) {
  const input = opts.input || buildGenericHotelSeedInput(hotelId, opts);
  const now = opts.now ? new Date(opts.now) : new Date();
  const iso = now.toISOString();
  const archetype = inferDemandArchetype(input.config || {}, { rooms: input.rooms });
  const deduped = dedupeLocalEntities(discoveredEntities);
  const targets = [];
  const rejected = [];

  for (const e of deduped) {
    if (!e.organizationName && !e.programName && !e.venueName) {
      rejected.push({ reason: "missing_identity", entity: e });
      continue;
    }
    if (!e.sourceUrl && !(e.sourceUrls && e.sourceUrls[0])) {
      rejected.push({ reason: "missing_source", entity: e });
      continue;
    }
    let band = e.catchmentBand || null;
    if (
      !band &&
      input.latitude != null &&
      input.longitude != null &&
      e.latitude != null &&
      e.longitude != null
    ) {
      const km = haversineKm(input.latitude, input.longitude, e.latitude, e.longitude);
      band = classifyCatchmentBand(km, archetype).band;
    }
    if (band === CATCHMENT_BAND.OUT_OF_MARKET && !e.destinationRelevant) {
      rejected.push({
        reason: "out_of_market",
        organizationName: e.organizationName || e.venueName,
      });
      continue;
    }

    const name = e.organizationName || e.programName || e.venueName;
    const targetId = `trg_ml_${slug(input.hotelId)}_${slug(name)}_${crypto
      .createHash("sha1")
      .update(String(e.sourceUrl || e.sourceUrls?.[0] || name))
      .digest("hex")
      .slice(0, 8)}`;

    const targetType = mapEvidenceEntityKindToTargetType(
      e.entityKind || e.targetType || "DEMAND_GENERATOR"
    );
    const sourceUrls = e.sourceUrls || (e.sourceUrl ? [e.sourceUrl] : []);
    targets.push({
      targetId,
      hotelId: input.hotelId,
      hotelName: input.hotelName,
      targetType,
      entityType: e.entityKind || "ORGANIZATION",
      displayName: name,
      status: TARGET_STATUS.ACTIVE,
      priority: e.priority || "MEDIUM",
      researchCadence: e.researchCadence || "WEEKLY",
      reasonMonitored: [
        "market_local_expansion",
        `category=${e.category || "unknown"}`,
        `band=${band || "DESTINATION_RELEVANT"}`,
        `lang=${e.sourceLanguage || "unknown"}`,
      ].join("; "),
      sourceUrls,
      primarySourceUrl: sourceUrls[0] || null,
      sourceLanguage: e.sourceLanguage || null,
      catchmentBand: band || CATCHMENT_BAND.DESTINATION_RELEVANT,
      seedProvenance: {
        version: MARKET_LOCAL_EXPANSION_VERSION,
        generatedAt: iso,
        baseline: true,
        createsWeeklyNew: false,
        createsOpportunity: false,
        fitBasis: "market_local_evidence",
      },
    });
  }

  return {
    ok: true,
    version: MARKET_LOCAL_EXPANSION_VERSION,
    hotelId: input.hotelId,
    discovered: deduped.length,
    targets,
    rejected,
    totals: {
      targets: targets.length,
      rejected: rejected.length,
      byBand: targets.reduce((acc, t) => {
        acc[t.catchmentBand] = (acc[t.catchmentBand] || 0) + 1;
        return acc;
      }, {}),
      byCategory: deduped.reduce((acc, e) => {
        const c = e.category || "unknown";
        acc[c] = (acc[c] || 0) + 1;
        return acc;
      }, {}),
    },
    quality: {
      baselineCreatesWeeklyNew: false,
      hotelSpecificOrgsHardcoded: false,
      entitiesFromEvidenceOnly: true,
    },
  };
}
