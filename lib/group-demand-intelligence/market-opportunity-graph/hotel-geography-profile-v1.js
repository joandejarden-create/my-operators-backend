/**
 * NYC GDI hotel geography profiles — derived from hotel demand configs + ADP fixtures.
 * No invented addresses; lat/lng from ADP/config when present.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelDemandConfig } from "../hotel-profile.js";
import { SANTO_DOMINGO_HOTELS } from "./santo-domingo-market-geography-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const NYC_CONTROL_HOTELS = Object.freeze({
  RENAISSANCE: "recG66DQJKP2c0UNh",
  HILTON: "rec35fExUxCClpOP6",
  NOW_NOW: "recGkME49yYuxQl0u",
});

function readAdpFixture(relPath) {
  if (!relPath) return null;
  const full = path.join(ROOT, relPath);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Build canonical geography profile for a GDI hotel.
 */
export function buildHotelGeographyProfile(hotelId) {
  const cfg = loadHotelDemandConfig(hotelId);
  if (!cfg) {
    return {
      hotelId,
      error: "hotel_config_missing",
    };
  }
  const adp = readAdpFixture(cfg.adpFixturePath);
  const lat =
    cfg.geo?.latitude ??
    adp?.identity?.latitude ??
    adp?.geography?.latitude ??
    adp?.geo?.latitude ??
    adp?.location?.latitude ??
    adp?.latitude ??
    null;
  const lng =
    cfg.geo?.longitude ??
    adp?.identity?.longitude ??
    adp?.geography?.longitude ??
    adp?.geo?.longitude ??
    adp?.location?.longitude ??
    adp?.longitude ??
    null;
  const territoryLabel = cfg.demandTerritory?.label || null;
  const classification = cfg.capabilityProfile?.classification || null;

  let borough = null;
  let sector = null;
  let submarket = territoryLabel;
  let microArea = null;
  let archetype = classification;
  let metro = null;

  if (/times.?square|midtown/i.test(String(territoryLabel))) {
    metro = "New York City";
    borough = "Manhattan";
    sector = "Midtown";
    submarket = "Times Square / Midtown West";
    microArea = "Times Square Theater Cluster";
    archetype = archetype || "times_square_midtown";
  } else if (/noho|soho|village|lower manhattan/i.test(String(territoryLabel))) {
    metro = "New York City";
    borough = "Manhattan";
    sector = "Downtown / Lower Manhattan";
    submarket = "NoHo / SoHo / Village";
    microArea = "NoHo Lafayette Corridor";
    archetype = archetype || "noho_lower_manhattan";
  } else if (/piantini/i.test(String(territoryLabel))) {
    metro = "Santo Domingo";
    borough = "Distrito Nacional";
    sector = "Piantini / Polanco-equivalent business";
    submarket = "Piantini / Blue Mall";
    microArea = "Winston Churchill / Blue Mall node";
    archetype = archetype || "santo_domingo_piantini_urban_luxury";
  } else if (/naco|tiradentes/i.test(String(territoryLabel))) {
    metro = "Santo Domingo";
    borough = "Distrito Nacional";
    sector = "Naco financial corridor";
    submarket = "Naco / Tiradentes";
    microArea = "Tiradentes / Presidente González node";
    archetype = archetype || "santo_domingo_naco_urban_upper_upscale";
  } else if (/santo domingo/i.test(String(territoryLabel))) {
    metro = "Santo Domingo";
    borough = "Distrito Nacional";
  } else {
    // Legacy NYC default only when territory clearly NYC
    if (/manhattan|nyc|new york/i.test(String(territoryLabel || ""))) {
      metro = "New York City";
      borough = "Manhattan";
    }
  }

  const address =
    adp?.identity?.address ||
    adp?.address ||
    adp?.identity?.streetAddress ||
    null;

  return {
    hotelId,
    displayName: cfg.displayName || hotelId,
    address,
    lat,
    lon: lng,
    metro,
    borough,
    districtSector: sector,
    submarket,
    microArea,
    primaryDemandNodes: cfg.demandTerritory?.includes || [],
    secondaryDemandNodes: Object.keys(
      cfg.demandTerritory?.classificationKeywords?.TERRITORY_NEARBY || {}
    ).length
      ? cfg.demandTerritory.classificationKeywords.TERRITORY_NEARBY
      : [],
    territoryLabel,
    archetype,
    rooms: cfg.capabilityProfile?.totalGuestrooms ?? null,
    meetingSqFt: cfg.capabilityProfile?.totalMeetingSpaceSqFt ?? null,
    peakRoomsMin: cfg.commercialPriorities?.coreTargetPeakRoomsMin ?? null,
    peakRoomsMax: cfg.commercialPriorities?.coreTargetPeakRoomsMax ?? null,
    serviceLevel: cfg.capabilityProfile?.serviceLevel || null,
    softBrand: cfg.capabilityProfile?.softBrand || null,
    config: cfg,
    adpSubmarket: adp?.submarket || adp?.identity?.submarket || null,
    marketHintsKey: metro === "Santo Domingo" ? "santo_domingo" : metro === "New York City" ? "nyc" : null,
  };
}

export function buildNycControlGeographyProfiles() {
  return {
    RENAISSANCE: buildHotelGeographyProfile(NYC_CONTROL_HOTELS.RENAISSANCE),
    HILTON: buildHotelGeographyProfile(NYC_CONTROL_HOTELS.HILTON),
    NOW_NOW: buildHotelGeographyProfile(NYC_CONTROL_HOTELS.NOW_NOW),
  };
}

export function buildSantoDomingoGeographyProfiles() {
  return {
    JW: buildHotelGeographyProfile(SANTO_DOMINGO_HOTELS.JW),
    RADISSON: buildHotelGeographyProfile(SANTO_DOMINGO_HOTELS.RADISSON),
  };
}
