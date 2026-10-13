/**
 * Build Hotel Group Demand Profile from Dealality knowledge (Level 1).
 * ADP fixture is READ-ONLY seed — never written back.
 *
 * REUSABLE_PRODUCT_LOGIC: buildHotelGroupDemandProfile(hotelId) works for any
 * hotel with config/group-demand-intelligence/hotels/{hotelId}.json + optional ADP fixture.
 * Bethesda wrapper retained for backward compatibility.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { CLAIM_KIND } from "./claim-types.js";
import { resolveCanonicalHotelId } from "../hotel-census/adp-gdi-canonical-identity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../..");

const BETHESDA_ADP_FIXTURE = path.join(
  REPO_ROOT,
  "fixtures/ai-demand-positioning/bethesda-marriott-property-profile.json"
);

const BETHESDA_CONFIG = path.join(
  REPO_ROOT,
  "config/group-demand-intelligence/hotels/recLuxvwwxID7U2B8.json"
);

/** First pilot hotel — census product key. Other hotels onboard via config file. */
export const PILOT_HOTEL_ID = "recLuxvwwxID7U2B8";

function readJsonSafe(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Load hotel demand configuration (HOTEL_SPECIFIC_DATA).
 */
export function loadHotelDemandConfig(hotelId) {
  const id = String(hotelId || "").trim();
  if (!id) return null;
  const directPath = path.join(
    REPO_ROOT,
    "config/group-demand-intelligence/hotels",
    `${id}.json`
  );
  let cfg = readJsonSafe(directPath);
  if (cfg?.redirectTo) {
    cfg = readJsonSafe(
      path.join(
        REPO_ROOT,
        "config/group-demand-intelligence/hotels",
        `${String(cfg.redirectTo).trim()}.json`
      )
    );
  }
  if (cfg?.displayName || cfg?.commercialPriorities) return cfg;

  const canonical = resolveCanonicalHotelId(id);
  if (canonical && canonical !== id) {
    const canonCfg = readJsonSafe(
      path.join(
        REPO_ROOT,
        "config/group-demand-intelligence/hotels",
        `${canonical}.json`
      )
    );
    if (canonCfg?.redirectTo) return null;
    if (canonCfg) return canonCfg;
  }
  return cfg;
}

/**
 * True when a hotel has a GDI onboard config file.
 */
export function isHotelOnboardedForGdi(hotelId) {
  return Boolean(loadHotelDemandConfig(hotelId));
}

function resolveAdpFixturePath(config) {
  if (config?.adpFixturePath) {
    return path.isAbsolute(config.adpFixturePath)
      ? config.adpFixturePath
      : path.join(REPO_ROOT, config.adpFixturePath);
  }
  if (config?.hotelId === PILOT_HOTEL_ID || !config) {
    return BETHESDA_ADP_FIXTURE;
  }
  return null;
}

function peakRoomPlanningBand(config, rooms) {
  const min = config?.commercialPriorities?.coreTargetPeakRoomsMin;
  const max = config?.commercialPriorities?.coreTargetPeakRoomsMax;
  if (Number.isFinite(min) && Number.isFinite(max)) {
    return {
      value: Math.round((min + max) / 2),
      min,
      max,
      claimKind: CLAIM_KIND.ESTIMATED,
      note: "From hotel GDI commercialPriorities peak-room band (HOTEL_SPECIFIC_DATA).",
    };
  }
  if (Number.isFinite(rooms) && rooms > 0) {
    const estMax = Math.min(Math.round(rooms * 0.7), rooms - 10);
    const estMin = Math.max(20, Math.round(rooms * 0.15));
    return {
      value: Math.round((estMin + estMax) / 2),
      min: estMin,
      max: estMax,
      claimKind: CLAIM_KIND.ESTIMATED,
      note: "Derived from total guestrooms — refine via hotel GDI config when available.",
    };
  }
  return {
    value: CLAIM_KIND.UNKNOWN,
    claimKind: CLAIM_KIND.UNKNOWN,
    note: "Peak room band unknown — set commercialPriorities in hotel GDI config.",
  };
}

/**
 * Generic Hotel Group Demand Profile builder.
 * Classification: REUSABLE_PRODUCT_LOGIC (reads HOTEL_SPECIFIC_DATA from config + ADP RO).
 *
 * @param {string} hotelId
 * @param {{ config?: object, adp?: object } } [opts]
 */
export function buildHotelGroupDemandProfile(hotelId, opts = {}) {
  const id = String(hotelId || "").trim();
  if (!id) {
    const err = new Error("hotelId_required");
    err.code = "hotelId_required";
    throw err;
  }

  const config = opts.config || loadHotelDemandConfig(id);
  if (!config && id !== PILOT_HOTEL_ID) {
    const err = new Error("hotel_not_onboarded_for_gdi");
    err.code = "hotel_not_onboarded_for_gdi";
    err.message = `No GDI hotel config at config/group-demand-intelligence/hotels/${id}.json`;
    throw err;
  }

  const effectiveConfig =
    config || (id === PILOT_HOTEL_ID ? defaultBethesdaConfig() : null);

  const adpPath = resolveAdpFixturePath(effectiveConfig);
  const adp = opts.adp || (adpPath ? readJsonSafe(adpPath) : null);

  if (!adp) {
    const err = new Error("gdi_adp_fixture_missing");
    err.code = "gdi_adp_fixture_missing";
    err.message = `ADP fixture missing for ${id} (expected ${adpPath || "adpFixturePath in hotel config"})`;
    throw err;
  }

  const meeting = adp.meetingSpace || {};
  const capability = effectiveConfig?.capabilityProfile || {};
  const rooms =
    capability.totalGuestrooms ??
    adp.rooms ??
    CLAIM_KIND.UNKNOWN;
  const peakBand = peakRoomPlanningBand(effectiveConfig, Number(rooms));

  const reusedFields = [
    "name",
    "address/city/state",
    "brand/affiliation",
    "rooms",
    "meetingSpace",
    "declaredCompSet",
    "positioning",
    "geography",
    "attributes",
  ].filter((f) => {
    if (f === "name") return Boolean(adp.name);
    if (f === "address/city/state") return Boolean(adp.city || adp.address);
    if (f === "brand/affiliation") return Boolean(adp.brand || adp.affiliation);
    if (f === "rooms") return adp.rooms != null;
    if (f === "meetingSpace") return Boolean(adp.meetingSpace);
    if (f === "declaredCompSet") return Array.isArray(adp.declaredCompSet);
    if (f === "positioning") return Boolean(adp.positioning);
    if (f === "geography") return Boolean(adp.geography || adp.market);
    if (f === "attributes") return Array.isArray(adp.attributes);
    return false;
  });

  const missingGdiCritical = [];
  if (adp.rooms == null && capability.totalGuestrooms == null) {
    missingGdiCritical.push("totalGuestrooms");
  }
  if (!meeting.totalSqFt && !capability.totalMeetingSpaceSqFt) {
    missingGdiCritical.push("totalMeetingSpaceSqFt");
  }
  if (!(effectiveConfig?.demandTerritory?.classificationKeywords)) {
    missingGdiCritical.push("demandTerritory.classificationKeywords");
  }
  if (!effectiveConfig?.aliases?.censusRecordId && id !== PILOT_HOTEL_ID) {
    missingGdiCritical.push("censusRecordId (provisional GDI key in use)");
  }

  return {
    hotelId: id,
    canonicalIds: {
      censusRecordId: effectiveConfig?.aliases?.censusRecordId || (id === PILOT_HOTEL_ID ? id : null),
      censusIdentityKey: adp.sourceGovernance?.censusIdentityKey || null,
      marriottHotelCode: adp.sourceGovernance?.marriottHotelCode || null,
      hiltonPropertyPath: effectiveConfig?.aliases?.hiltonPropertyPath || null,
      adpPropertyId: adp.propertyId || effectiveConfig?.aliases?.adpPropertyId || null,
      adpPropertyIdUsage: "READ_ONLY_REFERENCE_NOT_PRODUCT_KEY",
      provisionalGdiHotelId: id.startsWith("gdi_hotel_") ? id : null,
    },
    identity: {
      hotelName: adp.name || effectiveConfig?.displayName || id,
      address: adp.address || null,
      city: adp.city || null,
      state: adp.state || null,
      country: adp.country || "US",
      market: adp.market || null,
      submarket: adp.submarket || null,
      latitude: adp.geography?.latitude ?? null,
      longitude: adp.geography?.longitude ?? null,
      brand: adp.brand || adp.affiliation || null,
      parentBrandCompany: adp.parentCompany || null,
      managementCompany: CLAIM_KIND.UNKNOWN,
      ownership: adp.owner || CLAIM_KIND.UNKNOWN,
      chainScale: adp.chainScale || null,
      website: adp.website || null,
      distinctFrom: adp.identityConfusableExclusions || effectiveConfig?.identityExclusions || [],
    },
    guestrooms: {
      totalGuestrooms: rooms,
      totalGuestroomsClaimKind: CLAIM_KIND.FACT,
      totalGuestroomsConfidence: adp.roomCountConfidence || "MEDIUM",
      suites: CLAIM_KIND.UNKNOWN,
      realisticPeakGroupRoomBlock: peakBand,
      roomBlockFlexibility: CLAIM_KIND.UNKNOWN,
    },
    meetingCapability: {
      totalMeetingSpaceSqFt:
        capability.totalMeetingSpaceSqFt ?? meeting.totalSqFt ?? CLAIM_KIND.UNKNOWN,
      meetingRooms: capability.meetingRoomsIndoor ?? meeting.meetingRooms ?? CLAIM_KIND.UNKNOWN,
      largestMeetingRoom: meeting.largestRoom?.name || CLAIM_KIND.UNKNOWN,
      ballroom: meeting.largestRoom?.name || CLAIM_KIND.UNKNOWN,
      ballroomSqFt:
        capability.largestBallroomSqFt ?? meeting.largestRoom?.sqFt ?? CLAIM_KIND.UNKNOWN,
      theaterCapacity:
        capability.largestTheaterCapacity ?? meeting.largestRoom?.capacity ?? CLAIM_KIND.UNKNOWN,
      banquetCapacity: CLAIM_KIND.UNKNOWN,
      classroomCapacity: CLAIM_KIND.UNKNOWN,
      receptionCapacity: CLAIM_KIND.UNKNOWN,
      breakoutRooms: CLAIM_KIND.UNKNOWN,
      boardrooms: CLAIM_KIND.UNKNOWN,
      outdoorMeetingSpace:
        capability.outdoorEventSpace != null
          ? Boolean(capability.outdoorEventSpace)
          : Array.isArray(meeting.outdoorSpaces) && meeting.outdoorSpaces.length > 0
            ? true
            : CLAIM_KIND.UNKNOWN,
      privateDining: CLAIM_KIND.UNKNOWN,
      meetingRoomsDetail: meeting.rooms || null,
      outdoorSpacesDetail: meeting.outdoorSpaces || null,
      sourceUrl: meeting.source || null,
      confidence: meeting.confidence || "HIGH",
      claimKind: CLAIM_KIND.FACT,
    },
    hotelCharacteristics: {
      serviceLevel:
        capability.serviceLevel ||
        (capability.classification?.includes("resort") ? "full_service_resort" : "full_service"),
      foodAndBeverage: Array.isArray(adp.restaurants) && adp.restaurants.length > 0,
      restaurant: (adp.restaurants || []).map((r) => r.name),
      bar: CLAIM_KIND.UNKNOWN,
      catering: CLAIM_KIND.ESTIMATED,
      parking: CLAIM_KIND.UNKNOWN,
      parkingType: CLAIM_KIND.UNKNOWN,
      busCoachAccess: CLAIM_KIND.UNKNOWN,
      airportAccess: CLAIM_KIND.ESTIMATED,
      publicTransit: CLAIM_KIND.UNKNOWN,
      walkability: CLAIM_KIND.UNKNOWN,
      classification:
        capability.classification ||
        adp.positioning?.primary ||
        "unknown",
      marinaAccess: Boolean(capability.marinaAccess),
      softBrand: capability.softBrand || adp.affiliation || null,
      attributes: adp.attributes || [],
      positioning: adp.positioning || null,
    },
    demandAnchors: (adp.nearby || adp.geography?.nearbyAttractions || []).map((name) => ({
      name,
      claimKind: CLAIM_KIND.FACT,
      source: "adp_property_profile_read_only",
    })),
    declaredCompetitiveSet: (adp.declaredCompSet || []).map((name) => ({
      name,
      claimKind: CLAIM_KIND.FACT,
      source: "adp_property_profile_read_only",
      note: "Legacy ADP-declared names — superseded for GDI by hotel-provided STR + group alternatives below when present.",
    })),
    strCompSet: (effectiveConfig?.competitiveContext?.strCompSet || []).map((c) => ({
      ...c,
      claimKind: CLAIM_KIND.FACT,
    })),
    relevantGroupDemandAlternatives: (
      effectiveConfig?.competitiveContext?.relevantGroupDemandAlternatives || []
    ).map((c) => ({
      ...c,
      claimKind: CLAIM_KIND.FACT,
    })),
    demandTerritory: effectiveConfig?.demandTerritory || null,
    commercialPriorityConfiguration: effectiveConfig?.commercialPriorities || null,
    capabilityProfile: capability,
    adpCanonicalReuse: {
      classification: "HOTEL_SPECIFIC_DATA",
      reusedFields,
      missingGdiCriticalFields: missingGdiCritical,
      newlyResearchedFields: [],
      adpFixturePath: adpPath ? path.relative(REPO_ROOT, adpPath).replace(/\\/g, "/") : null,
      note: "No duplicate hotel profile created — ADP fixture consumed read-only.",
    },
    researchNotes: {
      level1Sources: [
        adpPath ? path.relative(REPO_ROOT, adpPath).replace(/\\/g, "/") + " (READ_ONLY)" : null,
        `config/group-demand-intelligence/hotels/${id}.json`,
      ].filter(Boolean),
      adpMethodologyTouched: false,
      builtAt: new Date().toISOString(),
    },
    schemaVersion: "gdi-hotel-profile-v1.2",
  };
}

/**
 * Bethesda wrapper — preserves prior call sites.
 */
export function buildBethesdaMarriottProfileFromExistingKnowledge() {
  const profile = buildHotelGroupDemandProfile(PILOT_HOTEL_ID, {
    config: readJsonSafe(BETHESDA_CONFIG) || defaultBethesdaConfig(),
    adp: readJsonSafe(BETHESDA_ADP_FIXTURE),
  });
  // Preserve Bethesda-specific transit note from prior profile shape
  if (profile.hotelCharacteristics) {
    profile.hotelCharacteristics.publicTransit = "dc_metro_access";
    profile.hotelCharacteristics.classification = "suburban";
  }
  if (profile.guestrooms?.realisticPeakGroupRoomBlock) {
    profile.guestrooms.realisticPeakGroupRoomBlock = {
      value: 120,
      claimKind: CLAIM_KIND.ESTIMATED,
      note: "Pilot planning assumption — do not allocate all 407 rooms to one group; ~100–300 target segment mid.",
    };
  }
  return profile;
}

export function defaultBethesdaConfig() {
  return {
    hotelId: PILOT_HOTEL_ID,
    displayName: "Bethesda Marriott",
    commercialPriorities: {
      targetSegments: [
        "Associations",
        "Medical",
        "Healthcare",
        "Scientific",
        "Corporate",
        "Government",
        "Government contractor",
        "Weekend group",
        "Social",
        "Sports",
        "University / education",
      ],
      coreTargetPeakRoomsMin: 100,
      coreTargetPeakRoomsMax: 300,
      allowSmallerIfCommerciallyMeaningful: true,
      demandAnchorFocus: [
        "NIH",
        "Walter Reed National Military Medical Center",
        "federal health / research ecosystem",
        "Washington DC metro access",
        "Montgomery County employers",
      ],
      weekendFocus: true,
      notes: "Pilot configuration only — not hardcoded into GDI architecture.",
    },
  };
}
