/**
 * Universal GDI hotel profile resolution V1
 *
 * Resolves a research-ready hotel profile for any canonical HPC hotelId:
 *   1) existing config/group-demand-intelligence/hotels/{id}.json
 *   2) live HPC identity / rooms
 *   3) ADP property-profile fixture (read-only) when linked
 *   4) config-declared verified inventory evidence (official sources)
 *
 * Does not invent inventory. Does not silently persist unless opts.persistSynthesized=true.
 * Hotel-generic — no Mexico/US special-case loaders.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelGdiConfig } from "./research/commercial-demand-discovery-v4.js";
import { resolveCanonicalHotelId } from "../hotel-census/adp-gdi-canonical-identity.js";
import { fetchHpcRecord } from "../hotel-census/hpc-shared-upsert-v1.js";
import { getCensusRecordIdForAdpProperty } from "../ai-demand-positioning/census-link-registry.js";

export const HOTEL_GDI_PROFILE_RESOLUTION_V1 =
  "gdi-hotel-gdi-profile-resolution-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const HOTEL_CONFIG_DIR = path.join(
  ROOT,
  "config/group-demand-intelligence/hotels",
);
const ADP_FIXTURE_DIR = path.join(ROOT, "fixtures/ai-demand-positioning");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function numberOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function readJsonSafe(p) {
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Discover ADP fixture by hpcHotelId / censusRecordId / subjectId.
 */
export function findAdpPropertyFixtureByHotelId(hotelId) {
  const id = text(hotelId);
  if (!id || !fs.existsSync(ADP_FIXTURE_DIR)) return null;
  const files = fs.readdirSync(ADP_FIXTURE_DIR).filter((f) => f.endsWith(".json"));
  for (const file of files) {
    const abs = path.join(ADP_FIXTURE_DIR, file);
    const doc = readJsonSafe(abs);
    if (!doc || typeof doc !== "object") continue;
    const keys = [
      doc.hpcHotelId,
      doc.censusRecordId,
      doc.subjectId,
      doc.hotelId,
    ]
      .map(text)
      .filter(Boolean);
    if (keys.includes(id)) {
      return {
        path: path.relative(ROOT, abs).split(path.sep).join("/"),
        fixture: doc,
      };
    }
  }
  return null;
}

function hpcIdentityFields(fields = {}) {
  const f = fields || {};
  return {
    displayName:
      text(f["Canonical Property Name"]) ||
      text(f["Property Name"]) ||
      text(f.Name) ||
      null,
    city: text(f.City) || null,
    country: text(f.Country) || null,
    region: text(f["State / Region"]) || null,
    market: text(f.Market) || null,
    submarket: text(f.Submarket) || null,
    address: text(f.Address) || null,
    brand: text(f["Current Brand"]) || null,
    brandFamily: text(f["Brand Family"]) || null,
    officialUrl:
      text(f["Official Property URL"]) ||
      text(f["Source URL"]) ||
      null,
    rooms: numberOrNull(f["Rooms / Keys"]),
    roomsConfidence: text(f["Rooms Confidence"]) || null,
    roomsSourceType: text(f["Rooms Source Type"]) || null,
    roomsNotes: text(f["Rooms Notes"]) || null,
    roomsReviewedDate: text(f["Rooms Reviewed Date"]) || null,
  };
}

function inventoryFromAdpFixture(fixture) {
  if (!fixture) return {};
  const rooms = numberOrNull(fixture.rooms);
  const meeting =
    fixture.meetingSpace && typeof fixture.meetingSpace === "object"
      ? fixture.meetingSpace
      : {};
  const steward = fixture.attributeStewardship?.verifiedFacts || {};
  let meetingSqFt = numberOrNull(meeting.indoorSqFt || meeting.totalSqFt);
  if (!meetingSqFt && steward.meeting_space) {
    const m = String(steward.meeting_space).match(
      /([\d,]+)\s*(?:\+|plus)?\s*sq\s*ft/i,
    );
    if (m) meetingSqFt = numberOrNull(m[1].replace(/,/g, ""));
  }
  return {
    totalGuestrooms: rooms,
    totalMeetingSpaceSqFt: meetingSqFt,
    meetingRoomCount: numberOrNull(
      meeting.meetingRoomCount || meeting.roomCount || fixture.meetingRoomCount,
    ),
    largestReceptionCapacity: numberOrNull(
      meeting.auditoriumCapacity || fixture.largestReceptionCapacity,
    ),
    brand: text(fixture.brand) || null,
    parentCompany: text(fixture.parentCompany) || null,
    loyalty: text(fixture.affiliation) || null,
  };
}

function inventoryFromVerifiedEvidence(evidence = {}) {
  const inv = evidence.inventory || evidence;
  return {
    totalGuestrooms: numberOrNull(inv.totalGuestrooms || inv.rooms),
    meetingRoomCount: numberOrNull(
      inv.meetingRoomCount || inv.meetingRooms || inv.breakoutRooms,
    ),
    meetingRoomsIndoor: numberOrNull(
      inv.meetingRoomsIndoor || inv.meetingRoomCount || inv.meetingRooms,
    ),
    totalMeetingSpaceSqFt: numberOrNull(
      inv.totalMeetingSpaceSqFt || inv.indoorMeetingSpaceSqFt,
    ),
    outdoorMeetingSpaceSqFt: numberOrNull(inv.outdoorMeetingSpaceSqFt),
    largestBallroomSqFt: numberOrNull(inv.largestBallroomSqFt),
    largestReceptionCapacity: numberOrNull(
      inv.largestReceptionCapacity || inv.auditoriumCapacity,
    ),
  };
}

function firstPositive(...vals) {
  for (const v of vals) {
    const n = numberOrNull(v);
    if (n != null) return n;
  }
  return null;
}

function defaultTerritoryFromHpc(hpc) {
  const city = hpc.city;
  const market = hpc.market || city;
  const sub = hpc.submarket;
  const includes = [];
  if (sub) includes.push(`PRIMARY: ${sub}`);
  if (city && (!sub || !sub.toLowerCase().includes(city.toLowerCase()))) {
    includes.push(city);
  }
  if (market && market !== city) includes.push(market);
  return {
    label: [sub, market || city].filter(Boolean).join(" / ") || city || "market",
    country: hpc.country || null,
    includes,
    evaluationQuestion: city
      ? `Could this hotel realistically compete for demand tied to ${city}?`
      : "Could this hotel realistically compete for this demand?",
  };
}

function localeForCountry(country) {
  const c = text(country).toLowerCase();
  if (c === "mexico" || c === "méxico" || c === "mx") {
    return { primary: "es", secondary: ["en"], selective: [] };
  }
  if (!c || c === "united states" || c === "usa" || c === "us") {
    return { primary: "en", secondary: [], selective: [] };
  }
  return { primary: "en", secondary: [], selective: [] };
}

/**
 * Merge capability inventory without inventing values.
 * Prefer existing config verified numbers, then verified evidence, then HPC, then ADP.
 */
export function mergeCapabilityInventory({
  existingCap = {},
  hpc = {},
  adpInv = {},
  verifiedInv = {},
  evidenceMeta = null,
} = {}) {
  const totalGuestrooms = firstPositive(
    existingCap.totalGuestrooms,
    verifiedInv.totalGuestrooms,
    hpc.rooms,
    adpInv.totalGuestrooms,
  );
  const meetingRoomCount = firstPositive(
    existingCap.meetingRoomCount,
    existingCap.meetingRoomsIndoor,
    verifiedInv.meetingRoomCount,
    verifiedInv.meetingRoomsIndoor,
    adpInv.meetingRoomCount,
  );
  const totalMeetingSpaceSqFt = firstPositive(
    existingCap.totalMeetingSpaceSqFt,
    verifiedInv.totalMeetingSpaceSqFt,
    adpInv.totalMeetingSpaceSqFt,
  );
  const provenance = [];
  if (totalGuestrooms != null) {
    if (numberOrNull(existingCap.totalGuestrooms) === totalGuestrooms) {
      provenance.push({ field: "totalGuestrooms", source: "existing_gdi_config" });
    } else if (numberOrNull(verifiedInv.totalGuestrooms) === totalGuestrooms) {
      provenance.push({
        field: "totalGuestrooms",
        source: "verified_official_inventory",
        urls: evidenceMeta?.sourceUrls || null,
        verifiedOn: evidenceMeta?.verifiedOn || null,
      });
    } else if (numberOrNull(hpc.rooms) === totalGuestrooms) {
      provenance.push({
        field: "totalGuestrooms",
        source: "hpc_rooms_keys",
        confidence: hpc.roomsConfidence || null,
        notes: hpc.roomsNotes || null,
      });
    } else if (numberOrNull(adpInv.totalGuestrooms) === totalGuestrooms) {
      provenance.push({ field: "totalGuestrooms", source: "adp_fixture" });
    }
  }
  if (meetingRoomCount != null) {
    provenance.push({
      field: "meetingRoomCount",
      source:
        numberOrNull(existingCap.meetingRoomCount) === meetingRoomCount ||
        numberOrNull(existingCap.meetingRoomsIndoor) === meetingRoomCount
          ? "existing_gdi_config"
          : numberOrNull(verifiedInv.meetingRoomCount) === meetingRoomCount
            ? "verified_official_inventory"
            : "adp_or_merged",
    });
  }
  if (totalMeetingSpaceSqFt != null) {
    provenance.push({
      field: "totalMeetingSpaceSqFt",
      source:
        numberOrNull(existingCap.totalMeetingSpaceSqFt) === totalMeetingSpaceSqFt
          ? "existing_gdi_config"
          : numberOrNull(verifiedInv.totalMeetingSpaceSqFt) ===
              totalMeetingSpaceSqFt
            ? "verified_official_inventory"
            : "adp_or_merged",
    });
  }

  return {
    capabilityProfile: {
      ...existingCap,
      totalGuestrooms,
      meetingRoomCount,
      meetingRoomsIndoor:
        firstPositive(
          existingCap.meetingRoomsIndoor,
          verifiedInv.meetingRoomsIndoor,
          meetingRoomCount,
        ) || null,
      totalMeetingSpaceSqFt,
      outdoorMeetingSpaceSqFt: firstPositive(
        existingCap.outdoorMeetingSpaceSqFt,
        verifiedInv.outdoorMeetingSpaceSqFt,
      ),
      largestBallroomSqFt: firstPositive(
        existingCap.largestBallroomSqFt,
        verifiedInv.largestBallroomSqFt,
      ),
      largestReceptionCapacity: firstPositive(
        existingCap.largestReceptionCapacity,
        existingCap.auditoriumCapacityClaim,
        verifiedInv.largestReceptionCapacity,
        adpInv.largestReceptionCapacity,
      ),
      brand: text(existingCap.brand) || text(adpInv.brand) || text(hpc.brand) || null,
      parentCompany:
        text(existingCap.parentCompany) ||
        text(adpInv.parentCompany) ||
        text(hpc.brandFamily) ||
        null,
      loyalty: text(existingCap.loyalty) || text(adpInv.loyalty) || null,
      country: text(existingCap.country) || text(hpc.country) || null,
    },
    inventoryProvenance: provenance,
  };
}

/**
 * Resolve a research-ready GDI hotel profile for scaling / segment runners.
 */
export async function resolveHotelGdiProfileForResearch(hotelId, opts = {}) {
  const requestedId = text(hotelId);
  const canonicalHotelId =
    resolveCanonicalHotelId(requestedId) || requestedId || null;
  const resolution = {
    version: HOTEL_GDI_PROFILE_RESOLUTION_V1,
    requestedId,
    canonicalHotelId,
    configLoaded: false,
    hpcLoaded: false,
    adpFixtureLoaded: false,
    synthesized: false,
    inventorySources: [],
    blockers: [],
  };

  if (!canonicalHotelId || !/^rec[a-zA-Z0-9]{14}$/.test(canonicalHotelId)) {
    resolution.blockers.push("HOTEL_ID_NOT_CANONICAL_HPC");
    return { ok: false, hotel: null, resolution };
  }

  let config = loadHotelGdiConfig(canonicalHotelId) || null;
  if (config) resolution.configLoaded = true;

  let hpc = null;
  let hpcFields = {};
  if (opts.skipHpc !== true) {
    try {
      const rec = await fetchHpcRecord(canonicalHotelId);
      if (rec?.id) {
        resolution.hpcLoaded = true;
        hpcFields = hpcIdentityFields(rec.fields || {});
        hpc = { id: rec.id, ...hpcFields, fields: rec.fields || {} };
      }
    } catch (err) {
      resolution.hpcError = err.message || String(err);
    }
  }

  if (!hpc && !config) {
    resolution.blockers.push("HOTEL_PROFILE_UNRESOLVED");
    resolution.blockers.push("HPC_RECORD_UNAVAILABLE");
    return { ok: false, hotel: null, resolution };
  }

  if (hpc && config) {
    const cfgName = text(config.displayName).toLowerCase();
    const hpcName = text(hpc.displayName).toLowerCase();
    if (
      cfgName &&
      hpcName &&
      cfgName.slice(0, 18) !== hpcName.slice(0, 18) &&
      !cfgName.includes(hpcName.slice(0, 12)) &&
      !hpcName.includes(cfgName.slice(0, 12))
    ) {
      resolution.blockers.push("HOTEL_IDENTITY_MISMATCH_CONFIG_VS_HPC");
      resolution.identityMismatch = {
        configName: config.displayName,
        hpcName: hpc.displayName,
      };
      return { ok: false, hotel: null, resolution };
    }
  }

  let adpHit = null;
  if (config?.adpFixturePath) {
    const abs = path.isAbsolute(config.adpFixturePath)
      ? config.adpFixturePath
      : path.join(ROOT, config.adpFixturePath);
    const fixture = readJsonSafe(abs);
    if (fixture) {
      adpHit = {
        path: config.adpFixturePath,
        fixture,
      };
      resolution.adpFixtureLoaded = true;
    }
  }
  if (!adpHit) {
    adpHit = findAdpPropertyFixtureByHotelId(canonicalHotelId);
    if (adpHit) resolution.adpFixtureLoaded = true;
  }

  const verifiedEvidence =
    config?.verifiedInventoryEvidence ||
    opts.verifiedInventoryEvidence ||
    null;
  const verifiedInv = verifiedEvidence
    ? inventoryFromVerifiedEvidence(verifiedEvidence)
    : {};
  const adpInv = inventoryFromAdpFixture(adpHit?.fixture);
  const existingCap = config?.capabilityProfile || {};

  const merged = mergeCapabilityInventory({
    existingCap,
    hpc: hpc || {},
    adpInv,
    verifiedInv,
    evidenceMeta: verifiedEvidence
      ? {
          sourceUrls: verifiedEvidence.sourceUrls || verifiedEvidence.sources,
          verifiedOn: verifiedEvidence.verifiedOn || null,
        }
      : null,
  });
  resolution.inventorySources = merged.inventoryProvenance;

  const displayName =
    text(config?.displayName) ||
    text(hpc?.displayName) ||
    text(adpHit?.fixture?.displayName) ||
    text(adpHit?.fixture?.name) ||
    null;

  if (!displayName) {
    resolution.blockers.push("HOTEL_DISPLAY_NAME_UNRESOLVED");
    return { ok: false, hotel: null, resolution };
  }

  const city =
    text(config?.city) ||
    text(hpc?.city) ||
    text(adpHit?.fixture?.city) ||
    null;
  const country =
    text(config?.country) ||
    text(hpc?.country) ||
    text(adpHit?.fixture?.country) ||
    null;

  let demandTerritory = config?.demandTerritory || null;
  if (!demandTerritory && hpc) {
    demandTerritory = defaultTerritoryFromHpc(hpc);
  }

  const hotel = {
    ...(config || {}),
    hotelId: canonicalHotelId,
    displayName,
    city,
    country,
    region:
      text(config?.region) ||
      text(hpc?.region) ||
      text(adpHit?.fixture?.region) ||
      null,
    address:
      text(config?.address) ||
      text(hpc?.address) ||
      text(adpHit?.fixture?.address) ||
      null,
    locale: config?.locale || localeForCountry(country),
    currency: config?.currency || (country === "Mexico" ? "MXN" : config?.currency),
    timeZone:
      config?.timeZone ||
      (country === "Mexico"
        ? city && /canc/i.test(city)
          ? "America/Cancun"
          : "America/Mexico_City"
        : config?.timeZone),
    commercialPriorities: config?.commercialPriorities || {
      targetSegments: [],
      notes:
        "Segment applicability not pre-seeded — discovery must not invent demand-type assumptions.",
    },
    demandTerritory,
    capabilityProfile: {
      ...merged.capabilityProfile,
      serviceLevel:
        text(config?.capabilityProfile?.serviceLevel) ||
        text(adpHit?.fixture?.propertyType) ||
        null,
      classification:
        text(config?.capabilityProfile?.classification) || null,
    },
    aliases: {
      ...(config?.aliases || {}),
      censusRecordId: canonicalHotelId,
      adpPropertyId:
        config?.aliases?.adpPropertyId ||
        adpHit?.fixture?.propertyId ||
        null,
      note:
        config?.aliases?.note ||
        "Canonical hotelId is HPC record. Profile resolved via universal GDI onboarding.",
    },
    adpFixturePath:
      config?.adpFixturePath || adpHit?.path || null,
    verifiedInventoryEvidence:
      verifiedEvidence || config?.verifiedInventoryEvidence || null,
    inventoryProvenance: merged.inventoryProvenance,
    gdiOnboarding: {
      ...(config?.gdiOnboarding || {}),
      status: config
        ? "CONFIG_ENRICHED_RUNTIME"
        : "SYNTHESIZED_FROM_HPC_AND_EVIDENCE",
      resolvedAt: new Date().toISOString(),
      resolutionVersion: HOTEL_GDI_PROFILE_RESOLUTION_V1,
      hpcVerified: Boolean(hpc),
      paidResearch: "NOT_AUTHORIZED",
    },
  };

  if (!config) resolution.synthesized = true;

  if (opts.persistSynthesized === true && resolution.synthesized) {
    const outPath = path.join(HOTEL_CONFIG_DIR, `${canonicalHotelId}.json`);
    if (!fs.existsSync(outPath)) {
      fs.mkdirSync(HOTEL_CONFIG_DIR, { recursive: true });
      const toWrite = { ...hotel };
      delete toWrite.inventoryProvenance;
      fs.writeFileSync(outPath, JSON.stringify(toWrite, null, 2) + "\n");
      resolution.persistedConfigPath = path
        .relative(ROOT, outPath)
        .split(path.sep)
        .join("/");
    }
  }

  return { ok: true, hotel, resolution, hpc, adpFixture: adpHit };
}

export function hotelConfigPathForId(hotelId) {
  return path.join(HOTEL_CONFIG_DIR, `${text(hotelId)}.json`);
}

export {
  getCensusRecordIdForAdpProperty,
};
