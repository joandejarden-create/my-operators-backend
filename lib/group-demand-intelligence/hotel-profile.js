/**
 * Build Hotel Group Demand Profile from Dealality knowledge (Level 1).
 * ADP fixture is READ-ONLY seed — never written back.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { CLAIM_KIND } from "./claim-types.js";

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

export const PILOT_HOTEL_ID = "recLuxvwwxID7U2B8";

function readJsonSafe(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Load pilot demand configuration (Bethesda-specific priorities as DATA, not architecture).
 */
export function loadHotelDemandConfig(hotelId) {
  const p = path.join(
    REPO_ROOT,
    "config/group-demand-intelligence/hotels",
    `${hotelId}.json`
  );
  return readJsonSafe(p);
}

export function buildBethesdaMarriottProfileFromExistingKnowledge() {
  const adp = readJsonSafe(BETHESDA_ADP_FIXTURE);
  const config = readJsonSafe(BETHESDA_CONFIG) || defaultBethesdaConfig();

  if (!adp) {
    throw new Error("bethesda_adp_fixture_missing");
  }

  const meeting = adp.meetingSpace || {};
  return {
    hotelId: PILOT_HOTEL_ID,
    canonicalIds: {
      censusRecordId: PILOT_HOTEL_ID,
      censusIdentityKey: adp.sourceGovernance?.censusIdentityKey || "ind_marriott_us_wasbt",
      marriottHotelCode: adp.sourceGovernance?.marriottHotelCode || "WASBT",
      adpPropertyId: adp.propertyId || "adp_bethesda_marriott",
      adpPropertyIdUsage: "READ_ONLY_REFERENCE_NOT_PRODUCT_KEY",
    },
    identity: {
      hotelName: adp.name,
      address: adp.address,
      city: adp.city,
      state: adp.state,
      country: adp.country,
      latitude: null,
      longitude: null,
      brand: adp.brand,
      parentBrandCompany: adp.parentCompany,
      managementCompany: CLAIM_KIND.UNKNOWN,
      ownership: adp.owner || CLAIM_KIND.UNKNOWN,
      chainScale: adp.chainScale,
      website: adp.website,
      distinctFrom: adp.identityConfusableExclusions || [],
    },
    guestrooms: {
      totalGuestrooms: adp.rooms,
      totalGuestroomsClaimKind: CLAIM_KIND.FACT,
      totalGuestroomsConfidence: adp.roomCountConfidence || "MEDIUM",
      suites: CLAIM_KIND.UNKNOWN,
      realisticPeakGroupRoomBlock: {
        value: 120,
        claimKind: CLAIM_KIND.ESTIMATED,
        note: "Pilot planning assumption — do not allocate all 407 rooms to one group; ~100–300 target segment mid.",
      },
      roomBlockFlexibility: CLAIM_KIND.UNKNOWN,
    },
    meetingCapability: {
      totalMeetingSpaceSqFt: meeting.totalSqFt ?? CLAIM_KIND.UNKNOWN,
      meetingRooms: meeting.meetingRooms ?? CLAIM_KIND.UNKNOWN,
      largestMeetingRoom: meeting.largestRoom?.name || CLAIM_KIND.UNKNOWN,
      ballroom: meeting.largestRoom?.name || CLAIM_KIND.UNKNOWN,
      ballroomSqFt: meeting.largestRoom?.sqFt ?? CLAIM_KIND.UNKNOWN,
      theaterCapacity: meeting.largestRoom?.capacity ?? CLAIM_KIND.UNKNOWN,
      banquetCapacity: CLAIM_KIND.UNKNOWN,
      classroomCapacity: CLAIM_KIND.UNKNOWN,
      receptionCapacity: CLAIM_KIND.UNKNOWN,
      breakoutRooms: CLAIM_KIND.UNKNOWN,
      boardrooms: CLAIM_KIND.UNKNOWN,
      outdoorMeetingSpace: CLAIM_KIND.UNKNOWN,
      privateDining: CLAIM_KIND.UNKNOWN,
      sourceUrl: meeting.source || null,
      confidence: meeting.confidence || "HIGH",
      claimKind: CLAIM_KIND.FACT,
    },
    hotelCharacteristics: {
      serviceLevel: "full_service",
      foodAndBeverage: true,
      restaurant: (adp.restaurants || []).map((r) => r.name),
      bar: CLAIM_KIND.UNKNOWN,
      catering: CLAIM_KIND.ESTIMATED,
      parking: CLAIM_KIND.UNKNOWN,
      parkingType: CLAIM_KIND.UNKNOWN,
      busCoachAccess: CLAIM_KIND.UNKNOWN,
      airportAccess: CLAIM_KIND.ESTIMATED,
      publicTransit: "dc_metro_access",
      walkability: CLAIM_KIND.UNKNOWN,
      classification: "suburban",
    },
    demandAnchors: (adp.nearby || []).map((name) => ({
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
    /** GM-provided STR competitive set (hotel configuration) */
    strCompSet: (config.competitiveContext?.strCompSet || []).map((c) => ({
      ...c,
      claimKind: CLAIM_KIND.FACT,
    })),
    /** Nearby Marriott-area alternatives that may compete for groups outside formal STR */
    relevantGroupDemandAlternatives: (
      config.competitiveContext?.relevantGroupDemandAlternatives || []
    ).map((c) => ({
      ...c,
      claimKind: CLAIM_KIND.FACT,
    })),
    demandTerritory: config.demandTerritory || null,
    commercialPriorityConfiguration: config.commercialPriorities,
    researchNotes: {
      level1Sources: [
        "fixtures/ai-demand-positioning/bethesda-marriott-property-profile.json (READ_ONLY)",
        "config/group-demand-intelligence/hotels/recLuxvwwxID7U2B8.json",
      ],
      adpMethodologyTouched: false,
      builtAt: new Date().toISOString(),
    },
    schemaVersion: "gdi-hotel-profile-v1.1",
  };
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
      notes:
        "Pilot configuration only — not hardcoded into GDI architecture.",
    },
  };
}
