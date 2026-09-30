/**
 * Generic structured venue page parsers (Cvent + narrative conflict capture).
 * No hotel-specific hardcoding.
 */

import {
  SOURCE_AUTHORITY,
  SOURCE_FAMILY,
  classifyUrlAuthority,
  classifySourceFamily,
} from "./evidence-depth-v2.js";

function unescapeHtmlPayload(text = "") {
  return String(text || "")
    .replace(/\\u0026/g, "&")
    .replace(/\\"/g, '"')
    .replace(/\\\\/g, "\\");
}

function extractBalancedObject(text, marker) {
  const start = text.indexOf(marker);
  if (start < 0) return null;
  let depth = 0;
  let end = -1;
  for (let i = start + marker.length - 1; i < text.length; i++) {
    const ch = text[i];
    if (ch === "{") depth++;
    else if (ch === "}") {
      depth--;
      if (depth === 0) {
        end = i + 1;
        break;
      }
    }
  }
  if (end < 0) return null;
  return text.slice(start + marker.length - 1, end);
}

function num(re, text) {
  const m = text.match(re);
  if (!m) return null;
  const n = Number(String(m[1]).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

function roundInt(n) {
  if (n == null || !Number.isFinite(Number(n))) return null;
  return Math.round(Number(n));
}

/**
 * Parse Cvent venue HTML into structured meeting inventory + conflicts.
 */
export function parseCventVenueHtml(html, { url = "", hotelName = "" } = {}) {
  const text = unescapeHtmlPayload(html);
  const venueBlock = extractBalancedObject(text, '"currentVenueData":{') || "";

  // Prefer currentVenueData object fields (venue-specific card)
  let card = null;
  if (venueBlock) {
    try {
      card = JSON.parse(venueBlock);
    } catch {
      card = null;
    }
  }

  const totalSqFt =
    roundInt(card?.totalMeetingSpace?.imperialValue) ??
    num(/"totalMeetingSpace"\s*:\s*\{\s*"imperialValue"\s*:\s*([\d.]+)/, venueBlock || text);
  const totalSqM =
    roundInt(card?.totalMeetingSpace?.metricValue) ??
    num(
      /"totalMeetingSpace"\s*:\s*\{[^}]*"metricValue"\s*:\s*([\d.]+)/,
      venueBlock || ""
    );
  const largestSqFt =
    roundInt(card?.largestRoom?.imperialValue) ??
    num(/"largestRoom"\s*:\s*\{\s*"imperialValue"\s*:\s*([\d.]+)/, venueBlock || "") ??
    num(/"largestMeetingRoom"\s*:\s*(\d+)/, text);
  const largestSqM = roundInt(card?.largestRoom?.metricValue);
  const meetingRoomCount =
    roundInt(card?.totalMeetingRoom) ??
    num(/"totalMeetingRoom"\s*:\s*(\d+)/, venueBlock || "") ??
    num(/"numberOfMeetingRooms"\s*:\s*(\d+)/, text);
  const guestRooms =
    roundInt(card?.totalSleepingRoom) ??
    num(/"totalSleepingRoom"\s*:\s*(\d+)/, venueBlock || "") ??
    num(/"numberOfSleepingRooms"\s*:\s*(\d+)/, text);
  const secondLargestSqFt = num(/"secondLargestMeetingRoom"\s*:\s*(\d+)/, text);
  const exhibitSqFt = num(/"exhibitSpace"\s*:\s*\{\s*"imperialValue"\s*:\s*([\d.]+)/, text);

  // Capacity: prefer narrative "hasta 500" on this page; also scan unique large maxCapacity near venue
  const narrativeCapacity = num(
    /(?:hasta|up to|capacidad(?:\s+desde[^.]{0,40})?)\s*(?:grandes\s+conferencias\s+de\s+hasta\s*)?([\d.,]+)\s*personas/i,
    text
  );
  const maxCapacity = narrativeCapacity ?? num(/"maxCapacity"\s*:\s*(500)\b/, text);

  const venueName = card?.venueName || hotelName || null;

  // Narrative conflict: e.g. "1000 metros cuadrados" vs structured imperial/metric
  const narrativeSqM = num(
    /(?:total\s+de|aproximadamente|about|approx\.?)\s*([\d.,]+)\s*(?:metros?\s*cuadrados|m(?:²|2))/i,
    text
  );
  const conflicts = [];
  if (totalSqFt != null && narrativeSqM != null) {
    const narrativeAsSqFt = Math.round(narrativeSqM * 10.764);
    const delta = Math.abs(narrativeAsSqFt - totalSqFt);
    if (delta > Math.max(500, totalSqFt * 0.15)) {
      conflicts.push({
        field: "totalMeetingSpaceSqFt",
        structuredValue: totalSqFt,
        structuredUnit: "sq_ft",
        structuredMetricValue: totalSqM,
        narrativeValue: narrativeSqM,
        narrativeUnit: "sq_m",
        narrativeAsSqFt,
        authorityPreferred: "structured_inventory",
        note: "Structured venue inventory preferred over descriptive narrative area; conflict retained",
      });
    }
  }
  // Also conflict if structured metric (800) vs narrative 1000
  if (totalSqM != null && narrativeSqM != null && Math.abs(totalSqM - narrativeSqM) > 50) {
    conflicts.push({
      field: "totalMeetingSpaceSqM",
      structuredValue: totalSqM,
      structuredUnit: "sq_m",
      narrativeValue: narrativeSqM,
      narrativeUnit: "sq_m",
      authorityPreferred: "structured_inventory",
      note: "Structured metricValue preferred over narrative square meters",
    });
  }

  const rooms = [];
  if (largestSqFt != null) {
    rooms.push({
      spaceName: "Largest meeting room",
      spaceType: "Ballroom",
      sqFt: largestSqFt,
      sqM: largestSqM,
      theaterCapacity: maxCapacity,
      capacityType: maxCapacity != null ? "UNKNOWN_MAX" : null,
      sourceUrl: url,
      confidence: "HIGH",
    });
  }
  if (secondLargestSqFt != null) {
    rooms.push({
      spaceName: "Second-largest meeting room",
      spaceType: "Meeting Room",
      sqFt: secondLargestSqFt,
      sourceUrl: url,
      confidence: "HIGH",
    });
  }
  // Aggregate inventory row when totals known
  if (totalSqFt != null || meetingRoomCount != null) {
    rooms.push({
      spaceName: "Aggregate meeting inventory (structured venue profile)",
      spaceType: "Meeting Room",
      sqFt: totalSqFt,
      sqM: totalSqM,
      theaterCapacity: maxCapacity,
      capacityType: maxCapacity != null ? "UNKNOWN_MAX" : null,
      meetingRoomCount,
      sourceUrl: url,
      confidence: "HIGH",
      notes: "Structured venue profile totals",
    });
  }

  const populated =
    totalSqFt != null ||
    meetingRoomCount != null ||
    largestSqFt != null ||
    rooms.some((r) => r.sqFt != null);

  return {
    ok: populated,
    sourceFamily: SOURCE_FAMILY.CVENT,
    authority: classifyUrlAuthority(url) || SOURCE_AUTHORITY.TIER_B_STRUCTURED_VENUE,
    url,
    venueName,
    hotelNameHint: hotelName || venueName || null,
    commercial: {
      roomsKeys: guestRooms,
      totalMeetingSpaceSqFt: totalSqFt,
      totalMeetingSpaceSqM: totalSqM,
      meetingRoomCount,
      largestMeetingSpaceSqFt: largestSqFt,
      secondLargestMeetingSpaceSqFt: secondLargestSqFt,
      largestEventCapacity: maxCapacity,
      largestEventCapacityType: maxCapacity != null ? "UNKNOWN_MAX" : null,
      exhibitSpaceSqFt: exhibitSqFt,
    },
    eventSpaces: rooms,
    conflicts,
    narrative: {
      totalMeetingSpaceSqM: narrativeSqM,
      maxCapacityPersons: narrativeCapacity,
    },
    rawSignals: {
      venueBlockFound: Boolean(venueBlock),
      cardParsed: Boolean(card),
    },
  };
}

/**
 * Lightweight first-party meetings page extractor.
 */
export function parseOfficialMeetingsHtml(html, { url = "" } = {}) {
  const text = String(html || "");
  const meetingRoomCount = num(/(\d{1,3})\s*(?:meeting|event)\s*rooms?/i, text);
  const totalSqFt = num(/([\d,]+)\s*(?:sq\.?\s*ft|square feet)/i, text);
  const largestCap = num(
    /(?:up to|hasta)\s*([\d,]+)\s*(?:guests?|people|personas|attendees)/i,
    text
  );
  return {
    ok: meetingRoomCount != null || totalSqFt != null,
    sourceFamily: classifySourceFamily(url),
    authority: classifyUrlAuthority(url),
    url,
    commercial: {
      totalMeetingSpaceSqFt: totalSqFt,
      meetingRoomCount,
      largestEventCapacity: largestCap,
      largestEventCapacityType: largestCap != null ? "UNKNOWN_MAX" : null,
    },
    eventSpaces: [],
    conflicts: [],
  };
}

export { SOURCE_AUTHORITY, SOURCE_FAMILY };
