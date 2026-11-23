/**
 * GDI Hotel Configuration Proposal V2
 *
 * Propose a GDI hotel config JSON from verified HPC (+ optional ADP / HE)
 * intelligence. Marks unsupported fields as unresolved — never guesses.
 * Uses existing config schema (no schema changes).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveHotelGdiProfileForResearch,
  hotelConfigPathForId,
} from "../hotel-gdi-profile-resolution-v1.js";
import { loadHotelGdiConfig } from "../research/commercial-demand-discovery-v4.js";

export const GDI_HOTEL_CONFIG_PROPOSAL_V2 = "gdi-hotel-config-proposal-v2";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const REQUIRED_FIELDS = Object.freeze([
  "hotelId",
  "displayName",
  "country",
  "city",
  "market",
  "coordinatesOrVerifiedLocation",
  "roomCount",
  "meetingSpace",
  "positioning",
  "demandMix",
  "demandTerritory",
  "commercialSegmentPriorities",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function numberOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

/**
 * Build a proposed config + field resolution ledger for one HPC hotel.
 */
export async function proposeHotelGdiConfig(hotelId, opts = {}) {
  const resolved = await resolveHotelGdiProfileForResearch(hotelId, {
    skipHpc: opts.skipHpc === true,
    persistSynthesized: false,
    verifiedInventoryEvidence: opts.verifiedInventoryEvidence || null,
  });

  const fieldStatus = {};
  const unresolved = [];
  const populated = [];

  function mark(field, value, source, required = true) {
    const ok =
      value != null &&
      value !== "" &&
      !(Array.isArray(value) && value.length === 0) &&
      !(typeof value === "object" && !Array.isArray(value) && !Object.keys(value).length);
    fieldStatus[field] = {
      field,
      status: ok ? "POPULATED" : "UNRESOLVED",
      source: ok ? source : null,
      value: ok ? value : null,
      required,
    };
    if (ok) populated.push(field);
    else if (required) unresolved.push(field);
  }

  if (!resolved.ok || !resolved.hotel) {
    return {
      ok: false,
      contract: GDI_HOTEL_CONFIG_PROPOSAL_V2,
      hotelId,
      error: "HOTEL_PROFILE_UNRESOLVED",
      blockers: resolved.resolution?.blockers || [],
      resolution: resolved.resolution || null,
      fieldStatus: Object.fromEntries(
        REQUIRED_FIELDS.map((f) => [
          f,
          { field: f, status: "UNRESOLVED", source: null, value: null, required: true },
        ]),
      ),
      unresolved: [...REQUIRED_FIELDS],
      proposedConfig: null,
    };
  }

  const hotel = resolved.hotel;
  const hpc = resolved.hpc || {};
  const cap = hotel.capabilityProfile || {};

  mark("hotelId", hotel.hotelId, "hpc_canonical_id");
  mark("displayName", hotel.displayName, hotel.gdiOnboarding?.hpcVerified ? "hpc" : "config_or_hpc");
  mark("country", hotel.country, "hpc_or_config");
  mark("city", hotel.city, "hpc_or_config");
  const market = text(hotel.demandTerritory?.label) || text(hpc.market) || text(hotel.city);
  mark("market", market || null, market ? "hpc_market_or_city" : null);

  const coords =
    (Number.isFinite(Number(hpc.fields?.Latitude)) &&
      Number.isFinite(Number(hpc.fields?.Longitude)) && {
        lat: Number(hpc.fields.Latitude),
        lng: Number(hpc.fields.Longitude),
      }) ||
    (text(hotel.address)
      ? { address: hotel.address, verifiedLocationLabel: hotel.address }
      : null);
  mark(
    "coordinatesOrVerifiedLocation",
    coords,
    coords?.lat != null ? "hpc_lat_lng" : coords ? "verified_address" : null,
  );

  const rooms =
    numberOrNull(cap.rooms) ||
    numberOrNull(cap.totalGuestrooms) ||
    numberOrNull(hotel.verifiedInventoryEvidence?.rooms);
  mark("roomCount", rooms, rooms != null ? "capability_or_verified_inventory" : null);

  const meeting =
    numberOrNull(cap.meetingRoomsIndoor) ||
    numberOrNull(cap.meetingRoomCount) ||
    numberOrNull(cap.totalMeetingSpaceSqFt) ||
    numberOrNull(hotel.verifiedInventoryEvidence?.meetingRoomsIndoor);
  // Meeting space may be legitimately absent for villa/leisure hotels
  mark(
    "meetingSpace",
    meeting != null
      ? {
          meetingRoomsIndoor: numberOrNull(cap.meetingRoomsIndoor || cap.meetingRoomCount),
          totalMeetingSpaceSqFt: numberOrNull(cap.totalMeetingSpaceSqFt),
        }
      : null,
    meeting != null ? "capability_or_verified_inventory" : null,
    false, // not hard-required — mark unresolved but do not block proposal
  );
  if (meeting == null && !unresolved.includes("meetingSpace")) {
    fieldStatus.meetingSpace.status = "UNRESOLVED";
    fieldStatus.meetingSpace.note =
      "No verified meeting inventory — acceptable for leisure/villa hotels; do not invent.";
  }

  const positioning =
    text(cap.classification) ||
    text(cap.serviceLevel) ||
    text(hotel.commercialPriorities?.notes) ||
    null;
  mark("positioning", positioning, positioning ? "capability_or_priorities" : null, false);

  const demandMix = Array.isArray(hotel.commercialPriorities?.targetSegments)
    ? hotel.commercialPriorities.targetSegments
    : null;
  mark(
    "demandMix",
    demandMix?.length ? demandMix : null,
    demandMix?.length ? "commercial_priorities" : null,
    false,
  );

  mark(
    "demandTerritory",
    hotel.demandTerritory || null,
    hotel.demandTerritory ? "config_or_hpc_default" : null,
  );

  mark(
    "commercialSegmentPriorities",
    demandMix?.length ? demandMix : null,
    demandMix?.length ? "commercial_priorities" : null,
    false,
  );

  const proposedConfig = {
    hotelId: hotel.hotelId,
    displayName: hotel.displayName,
    city: hotel.city || null,
    country: hotel.country || null,
    region: hotel.region || null,
    address: hotel.address || null,
    timeZone: hotel.timeZone || null,
    currency: hotel.currency || null,
    aliases: {
      ...(hotel.aliases || {}),
      censusRecordId: hotel.hotelId,
      note:
        hotel.aliases?.note ||
        "Proposed by gdi-hotel-config-proposal-v2 from HPC / verified evidence — review before paid research.",
    },
    commercialPriorities: hotel.commercialPriorities || {
      targetSegments: [],
      notes: "UNRESOLVED — do not invent segment priorities.",
    },
    demandTerritory: hotel.demandTerritory || {
      label: market || "UNRESOLVED",
      includes: [],
      notes: "UNRESOLVED — do not invent catchment.",
    },
    capabilityProfile: {
      rooms: rooms || null,
      meetingRoomsIndoor: numberOrNull(cap.meetingRoomsIndoor || cap.meetingRoomCount),
      totalMeetingSpaceSqFt: numberOrNull(cap.totalMeetingSpaceSqFt),
      brand: text(cap.brand) || null,
      parentCompany: text(cap.parentCompany) || null,
      serviceLevel: text(cap.serviceLevel) || null,
      classification: text(cap.classification) || null,
      country: hotel.country || null,
    },
    verifiedInventoryEvidence: hotel.verifiedInventoryEvidence || null,
    gdiOnboarding: {
      status: "PROPOSED_V2_NOT_PERSISTED",
      proposalVersion: GDI_HOTEL_CONFIG_PROPOSAL_V2,
      proposedAt: new Date().toISOString(),
      unresolvedFields: unresolved,
      hpcVerified: Boolean(resolved.resolution?.hpcLoaded),
    },
  };

  const hardBlockers = unresolved.filter((f) =>
    ["hotelId", "displayName", "country", "city", "roomCount", "demandTerritory"].includes(f),
  );

  return {
    ok: hardBlockers.length === 0,
    contract: GDI_HOTEL_CONFIG_PROPOSAL_V2,
    hotelId: hotel.hotelId,
    proposedConfig,
    fieldStatus,
    populated,
    unresolved,
    hardBlockers,
    resolution: resolved.resolution,
    existingConfigPath: fs.existsSync(hotelConfigPathForId(hotel.hotelId))
      ? path.relative(ROOT, hotelConfigPathForId(hotel.hotelId)).split(path.sep).join("/")
      : null,
  };
}

/**
 * Validate a proposed (or generated) config against a known pilot fixture config.
 * Does not require exact equality — checks identity + key commercial fields.
 */
export function validateProposedAgainstPilotConfig(proposed, pilotConfig, opts = {}) {
  const p = proposed?.proposedConfig || proposed || {};
  const pilot = pilotConfig || {};
  const checks = [];
  function check(name, ok, detail) {
    checks.push({ name, ok: Boolean(ok), detail: detail || null });
  }
  check("hotelId_match", text(p.hotelId) === text(pilot.hotelId), {
    proposed: p.hotelId,
    pilot: pilot.hotelId,
  });
  check(
    "displayName_compatible",
    text(p.displayName)
      .toLowerCase()
      .includes(text(pilot.displayName).toLowerCase().slice(0, 8)) ||
      text(pilot.displayName)
        .toLowerCase()
        .includes(text(p.displayName).toLowerCase().slice(0, 8)),
    { proposed: p.displayName, pilot: pilot.displayName },
  );
  check("country_match", text(p.country).toLowerCase() === text(pilot.country).toLowerCase(), {
    proposed: p.country,
    pilot: pilot.country,
  });
  check(
    "city_compatible",
    !pilot.city ||
      text(p.city).toLowerCase().includes(text(pilot.city).toLowerCase().slice(0, 4)) ||
      text(pilot.city).toLowerCase().includes(text(p.city).toLowerCase().slice(0, 4)),
    { proposed: p.city, pilot: pilot.city },
  );
  const pRooms = numberOrNull(p.capabilityProfile?.rooms);
  const pilotRooms = numberOrNull(pilot.capabilityProfile?.rooms);
  check(
    "rooms_match_when_both_known",
    pRooms == null || pilotRooms == null || pRooms === pilotRooms,
    { proposed: pRooms, pilot: pilotRooms },
  );
  check(
    "demandTerritory_present",
    Boolean(p.demandTerritory?.label || p.demandTerritory?.includes?.length),
    { proposedLabel: p.demandTerritory?.label || null },
  );
  const failed = checks.filter((c) => !c.ok);
  return {
    ok: failed.length === 0,
    hotelId: p.hotelId || pilot.hotelId,
    checks,
    failed: failed.map((f) => f.name),
  };
}

/**
 * Validate proposals for the three Design Hotels pilot hotels against frozen configs.
 */
export async function validateDesignHotelsPilotConfigProposals(opts = {}) {
  const pilotIds = opts.hotelIds || [
    "rece0NdV9FCnaFlVS",
    "recaokE8psIbuVeuH",
    "rec1t35e3sPerzxYu",
  ];
  const results = [];
  for (const hotelId of pilotIds) {
    const existing = loadHotelGdiConfig(hotelId);
    const proposal = await proposeHotelGdiConfig(hotelId, {
      skipHpc: opts.skipHpc === true,
    });
    const validation = existing
      ? validateProposedAgainstPilotConfig(proposal, existing)
      : { ok: false, failed: ["PILOT_CONFIG_MISSING"], checks: [] };
    results.push({
      hotelId,
      proposalOk: proposal.ok,
      unresolved: proposal.unresolved || [],
      hardBlockers: proposal.hardBlockers || [],
      validation,
      existingConfigLoaded: Boolean(existing),
    });
  }
  return {
    contract: GDI_HOTEL_CONFIG_PROPOSAL_V2,
    mode: "validate-design-hotels-pilot",
    ok: results.every((r) => r.proposalOk && r.validation.ok),
    results,
  };
}

export default {
  GDI_HOTEL_CONFIG_PROPOSAL_V2,
  proposeHotelGdiConfig,
  validateProposedAgainstPilotConfig,
  validateDesignHotelsPilotConfigProposals,
};
