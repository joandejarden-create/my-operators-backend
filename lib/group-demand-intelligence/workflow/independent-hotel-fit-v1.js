/**
 * Independent hotel-fit assessment for shared-market reuse.
 * Does not copy Bethesda sports fit into downtown Mayflower / other hotels.
 * Distinguishes total hotel inventory from available group rooms.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { normalizeOrgIdentityKey } from "./reuse-prior-intelligence-v1.js";

export const INDEPENDENT_HOTEL_FIT_VERSION = "gdi-independent-hotel-fit-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

const SPORTS_STAY_TO_PLAY_RE =
  /\b(soccer|lacrosse|sports?\s*tournament|soccerplex|boyds|montgomery\s*county|stay[- ]to[- ]play|premier\s*cup|state\s*cup|memorial\s*tournament)\b/i;

const RENAISSANCE_NOISE_RE =
  /\b(exhibitor\s*console|interested\s*in\s*exhibiting|scan\s*exhibitors|how\s*to\s*manage\s*the\s*paper|interiors\s*from\s*spain|onda,?\s*castellon|supporters\s*&\s*sponsors|powered\s*by\s*a2z)\b/i;

export const FIT_OUTCOME = Object.freeze({
  FITS_THIS_HOTEL: "FITS_THIS_HOTEL",
  POSSIBLE_FIT: "POSSIBLE_FIT",
  OUT_OF_TERRITORY: "OUT_OF_TERRITORY",
  UNSUPPORTED_ORG: "UNSUPPORTED_ORG",
  NEEDS_HUMAN_REVIEW: "NEEDS_HUMAN_REVIEW",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
});

export function loadHotelConfig(hotelId) {
  const p = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${String(hotelId || "").trim()}.json`
  );
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function blob(candidate = {}) {
  return [
    candidate.organizationName,
    candidate.company,
    candidate.title,
    candidate.opportunityName,
    candidate.summaryWhat,
    candidate.fitExplanation,
    candidate.hotelOpportunityThesis,
    ...(Array.isArray(candidate.labels) ? candidate.labels : []),
  ]
    .filter(Boolean)
    .join(" ");
}

/**
 * Assess whether a candidate (often from another hotel's corpus) fits THIS hotel.
 */
export function assessIndependentHotelFit(candidate = {}, hotelId, opts = {}) {
  const id = String(hotelId || candidate.hotelId || "").trim();
  const cfg = opts.hotelConfig || loadHotelConfig(id);
  const textBlob = blob(candidate);
  const org = candidate.organizationName || candidate.company || "";
  const orgKey = normalizeOrgIdentityKey(org);
  const totalRooms =
    cfg?.capabilityProfile?.totalGuestrooms ||
    cfg?.capabilityProfile?.rooms ||
    null;
  const peakMin = cfg?.commercialPriorities?.coreTargetPeakRoomsMin ?? null;
  const peakMax = cfg?.commercialPriorities?.coreTargetPeakRoomsMax ?? null;
  const territoryLabel = cfg?.demandTerritory?.label || cfg?.locale?.city || null;

  const caveats = [];
  caveats.push(
    "Hotel total rooms are property capacity, not evidence of available group inventory or a room block."
  );
  if (totalRooms != null) {
    caveats.push(`Property lists approximately ${totalRooms} total guestrooms.`);
  }
  if (peakMin != null && peakMax != null) {
    caveats.push(
      `Configured peak-room commercial band is ${peakMin}–${peakMax}; do not infer peak rooms from attendance alone.`
    );
  }

  // Renaissance noise / unsupported org shells
  if (id === "recG66DQJKP2c0UNh" && RENAISSANCE_NOISE_RE.test(textBlob + " " + org)) {
    return {
      version: INDEPENDENT_HOTEL_FIT_VERSION,
      hotelId: id,
      organizationIdentityKey: orgKey,
      outcome: FIT_OUTCOME.UNSUPPORTED_ORG,
      promote: false,
      rationale:
        "Organization/title matches unsupported or noisy corpus patterns — not a credible account for promotion.",
      caveats,
      territoryLabel,
      totalGuestrooms: totalRooms,
      peakRoomBand: peakMin != null ? { min: peakMin, max: peakMax } : null,
    };
  }

  // Mayflower: do not inherit Bethesda Montgomery / sports stay-to-play as core fit
  if (id === "rec0eTEnCqlsGJJt9" && SPORTS_STAY_TO_PLAY_RE.test(textBlob)) {
    return {
      version: INDEPENDENT_HOTEL_FIT_VERSION,
      hotelId: id,
      organizationIdentityKey: orgKey,
      outcome: FIT_OUTCOME.OUT_OF_TERRITORY,
      promote: false,
      rationale:
        "Sports / SoccerPlex / Montgomery stay-to-play demand is Bethesda-market core, not automatic downtown Mayflower fit.",
      caveats,
      territoryLabel,
      totalGuestrooms: totalRooms,
      peakRoomBand: peakMin != null ? { min: peakMin, max: peakMax } : null,
    };
  }

  // Source hotel sports stay-to-play when assessing for a different downtown hotel
  const sourceHotel = String(candidate.hotelId || opts.sourceHotelId || "").trim();
  if (
    sourceHotel &&
    sourceHotel !== id &&
    sourceHotel === "recLuxvwwxID7U2B8" &&
    SPORTS_STAY_TO_PLAY_RE.test(textBlob) &&
    id !== "recLuxvwwxID7U2B8"
  ) {
    return {
      version: INDEPENDENT_HOTEL_FIT_VERSION,
      hotelId: id,
      organizationIdentityKey: orgKey,
      outcome: FIT_OUTCOME.OUT_OF_TERRITORY,
      promote: false,
      rationale:
        "Do not copy Bethesda sports stay-to-play fit into a different hotel without independent venue/geography evidence.",
      caveats,
      territoryLabel,
      totalGuestrooms: totalRooms,
      peakRoomBand: peakMin != null ? { min: peakMin, max: peakMax } : null,
    };
  }

  if (!org || orgKey.length < 2 || /^tbd$/i.test(org)) {
    return {
      version: INDEPENDENT_HOTEL_FIT_VERSION,
      hotelId: id,
      organizationIdentityKey: orgKey,
      outcome: FIT_OUTCOME.UNSUPPORTED_ORG,
      promote: false,
      rationale: "Missing or placeholder organization identity.",
      caveats,
      territoryLabel,
      totalGuestrooms: totalRooms,
      peakRoomBand: peakMin != null ? { min: peakMin, max: peakMax } : null,
    };
  }

  const hasThesis = Boolean(
    text(candidate.hotelOpportunityThesis) ||
      text(candidate.fitExplanation) ||
      text(candidate.summaryWhyHotel)
  );
  const hasGeoHint = Boolean(
    candidate.eventStartDate ||
      candidate.destinationStatus ||
      candidate.city ||
      /washington|dc|bethesda|arlington|san diego|times square|manhattan/i.test(textBlob)
  );

  if (!hasThesis && !hasGeoHint) {
    return {
      version: INDEPENDENT_HOTEL_FIT_VERSION,
      hotelId: id,
      organizationIdentityKey: orgKey,
      outcome: FIT_OUTCOME.INSUFFICIENT_EVIDENCE,
      promote: false,
      rationale: "Insufficient hotel-specific thesis and geography evidence for fit promotion.",
      caveats,
      territoryLabel,
      totalGuestrooms: totalRooms,
      peakRoomBand: peakMin != null ? { min: peakMin, max: peakMax } : null,
    };
  }

  if (hasThesis && String(candidate.hotelId || "") === id) {
    return {
      version: INDEPENDENT_HOTEL_FIT_VERSION,
      hotelId: id,
      organizationIdentityKey: orgKey,
      outcome: FIT_OUTCOME.FITS_THIS_HOTEL,
      promote: false, // still not auto-publish
      rationale:
        "Existing thesis is already hotel-scoped; retain for human shortlist — not automatic publication.",
      caveats,
      territoryLabel,
      totalGuestrooms: totalRooms,
      peakRoomBand: peakMin != null ? { min: peakMin, max: peakMax } : null,
    };
  }

  return {
    version: INDEPENDENT_HOTEL_FIT_VERSION,
    hotelId: id,
    organizationIdentityKey: orgKey,
    outcome: FIT_OUTCOME.NEEDS_HUMAN_REVIEW,
    promote: false,
    rationale:
      "Shared-market or cross-hotel reuse candidate — requires independent human fit judgment for this property.",
    caveats,
    territoryLabel,
    totalGuestrooms: totalRooms,
    peakRoomBand: peakMin != null ? { min: peakMin, max: peakMax } : null,
  };
}

function text(v) {
  return String(v || "").trim();
}

/**
 * Assess a list of candidates for a target hotel.
 */
export function assessIndependentHotelFitBatch(candidates = [], hotelId, opts = {}) {
  const results = (candidates || []).map((c) => ({
    candidateId: c.id || c.opportunityId || c.sourceRecordId || null,
    organizationName: c.organizationName || c.company || null,
    fit: assessIndependentHotelFit(c, hotelId, opts),
  }));
  const counts = {};
  for (const o of Object.values(FIT_OUTCOME)) counts[o] = 0;
  for (const r of results) counts[r.fit.outcome] = (counts[r.fit.outcome] || 0) + 1;
  return {
    version: INDEPENDENT_HOTEL_FIT_VERSION,
    hotelId,
    counts,
    results,
  };
}
