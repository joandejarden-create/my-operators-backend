/**
 * Hotel Explorer commercial description rubric (reusable, hotel-agnostic).
 * Evidence-grounded owner/operator/asset-manager copy — never invents performance or demand.
 *
 * Version: hotel-commercial-description-v1
 */

export const HOTEL_COMMERCIAL_DESCRIPTION_VERSION = "hotel-commercial-description-v1";

const UNKNOWN_RE = /unknown/i;

function clean(v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s || UNKNOWN_RE.test(s)) return null;
  return s;
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function fmtRooms(n) {
  return Math.round(n).toLocaleString("en-US");
}

function articleFor(phrase) {
  return /^[aeiou]/i.test(String(phrase || "").trim()) ? "an" : "a";
}

function meetingPhrase(hotel) {
  if (hotel.meetingSpaceDisplay) return String(hotel.meetingSpaceDisplay).trim();
  const sq = num(hotel.totalMeetingSpace);
  if (!sq) return null;
  const rooms = num(hotel.meetingRooms || hotel.meetingRoomCount);
  const largest =
    hotel.largestMeetingRoomName ||
    (hotel.meetingSpaceProvenance &&
      hotel.meetingSpaceProvenance.largestRoom &&
      hotel.meetingSpaceProvenance.largestRoom.name) ||
    null;
  const largestSq =
    hotel.largestMeetingRoomSqFt ||
    (hotel.meetingSpaceProvenance &&
      hotel.meetingSpaceProvenance.largestRoom &&
      hotel.meetingSpaceProvenance.largestRoom.sqFt) ||
    null;
  let out = `${fmtRooms(sq)} sq ft of meeting and event space`;
  if (rooms) out += ` across ${fmtRooms(rooms)} rooms`;
  if (largest && largestSq) {
    out += `, with largest room ${largest} (${fmtRooms(largestSq)} sq ft)`;
  } else if (largest) {
    out += `, largest room ${largest}`;
  }
  return out;
}

function demandGenerators(hotel) {
  const list = [];
  const raw =
    hotel.demandGenerators ||
    hotel.nearbyDemand ||
    (hotel.overviewEnrichment && hotel.overviewEnrichment.demandGenerators) ||
    [];
  if (Array.isArray(raw)) {
    raw.forEach((d) => {
      const name = clean(typeof d === "string" ? d : d && (d.name || d.label));
      if (name) list.push(name);
    });
  }
  const singles = [
    hotel.primaryDemandDriver,
    hotel.airportName,
    hotel.conventionCenterName,
  ]
    .map(clean)
    .filter(Boolean);
  singles.forEach((s) => {
    if (!list.includes(s)) list.push(s);
  });
  return list.slice(0, 3);
}

/**
 * Detect thin / keyword-stuffed / technical descriptions that must not ship.
 */
export function isLowQualityCommercialDescription(text, hotel = {}) {
  const s = String(text || "").trim();
  if (!s) return true;
  if (/hotel\s+hotel/i.test(s)) return true;
  if (/signals\s*:/i.test(s)) return true;
  if (/_[a-z0-9]+_/i.test(s)) return true;
  if (/\brec[A-Za-z0-9]{10,}\b/.test(s)) return true;
  if (/\bdle_[A-Z0-9]+\b/i.test(s)) return true;
  if (/\b\d+\s+keys\b/i.test(s) && s.length < 120) return true;
  if (/^(OPEN|PIPELINE|CLOSED)\b/i.test(s) && s.length < 80) return true;
  // Fact-list / comma-stack pattern
  const commas = (s.match(/,/g) || []).length;
  if (commas >= 6 && s.split(".").length <= 2) return true;
  if (/serving\s+.+\s+—\s+signals/i.test(s)) return true;
  const censusType = clean(hotel.censusPropertyType);
  if (censusType && new RegExp(`^${censusType.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}\\s+hotel,`, "i").test(s)) {
    const hasBrand = clean(hotel.brand);
    const hasParent = clean(hotel.parentCompany);
    const hasRooms = num(hotel.rooms);
    if (!hasBrand && !hasParent && !hasRooms) return true;
  }
  return false;
}

/**
 * Build a commercial narrative from verified / present fields only.
 * @param {object} hotel
 * @param {object} [opts]
 * @param {object} [opts.ownership] — optional verified ownership context
 */
export function buildCommercialDescription(hotel = {}, opts = {}) {
  const name = clean(hotel.name) || "This hotel";
  const brand = clean(hotel.brand);
  const scaleRaw = clean(hotel.chainScale) || clean(hotel.propertyType);
  const scale = scaleRaw ? scaleRaw.replace(/\s+Chain$/i, "").toLowerCase() : null;
  const city = clean(hotel.city);
  const state = clean(hotel.state);
  const country = clean(hotel.country);
  const market = clean(hotel.market);
  const submarket = clean(hotel.submarket) || clean(hotel.effectiveSubmarket);
  const locationType = clean(hotel.locationType);
  const rooms = num(hotel.rooms);
  const meeting = meetingPhrase(hotel);
  const operator = clean(hotel.managementCompany) || clean(hotel.operator);
  const opType = clean(hotel.operationType);
  const ownership = opts.ownership || hotel.ownershipContext || null;
  const economicOwner =
    clean(ownership && (ownership.economicOwnerName || ownership.economic_owner)) ||
    clean(hotel.economicOwner);
  const legalOwner =
    clean(ownership && (ownership.legalOwnerName || ownership.legal_owner)) ||
    clean(hotel.legalOwner);
  const ownershipVerified =
    ownership &&
    (ownership.preview_state === "LIVE" ||
      ownership.verified === true ||
      ownership.confidence === "HIGH" ||
      ownership.confidence === "MEDIUM");
  const demand = demandGenerators(hotel);
  const sentences = [];

  // 1) Asset positioning
  let lead = `${name} is`;
  if (scale) lead += ` ${articleFor(scale)} ${scale} hotel`;
  else lead += " a hotel";
  if (brand) lead += ` branded ${brand}`;
  if (city) {
    lead += ` in ${city}`;
    const geo = [state, country]
      .filter((p) => p && String(p).toLowerCase() !== String(city).toLowerCase())
      .join(", ");
    if (geo) lead += `, ${geo}`;
  }
  lead += ".";
  sentences.push(lead);

  // 2) Physical product + group capability
  const productBits = [];
  if (rooms) productBits.push(`${fmtRooms(rooms)} guestrooms`);
  if (meeting) productBits.push(meeting);
  if (productBits.length) {
    sentences.push(
      productBits.length === 1
        ? `The physical product comprises ${productBits[0]}.`
        : `The physical product comprises ${productBits[0]} and ${productBits[1]}.`
    );
  }

  // 3) Market / location
  const place = [];
  if (submarket) place.push(submarket);
  else if (market) place.push(market);
  if (locationType) place.push(`${locationType.toLowerCase()} location`);
  if (place.length) {
    sentences.push(`It sits in ${place.join(" within a ")}.`);
  }

  // 4) Demand generators (only when named — never invent)
  if (demand.length) {
    if (demand.length === 1) {
      sentences.push(`Relevant demand context includes proximity to ${demand[0]}.`);
    } else {
      sentences.push(
        `Relevant demand context includes ${demand.slice(0, -1).join(", ")} and ${demand[demand.length - 1]}.`
      );
    }
  }

  // 5) Operating structure
  if (operator) {
    const via = opType ? ` under a ${opType.toLowerCase()}` : "";
    sentences.push(`Day-to-day operations are managed by ${operator}${via}.`);
  }

  // 6) Ownership context only when appropriately verified / explicitly provided
  if (ownershipVerified && (economicOwner || legalOwner)) {
    if (economicOwner && legalOwner && economicOwner !== legalOwner) {
      sentences.push(
        `Ownership intelligence identifies ${legalOwner} as legal owner and ${economicOwner} as economic sponsor.`
      );
    } else if (economicOwner) {
      sentences.push(`Economic ownership is associated with ${economicOwner}.`);
    } else if (legalOwner) {
      sentences.push(`Legal ownership is associated with ${legalOwner}.`);
    }
  }

  // 7) Explicit limitation — never claim ADR/RevPAR/occupancy
  // (no sentence added; rubric forbids manufacturing performance)

  const text = sentences.join(" ").replace(/\s+/g, " ").trim();
  return {
    version: HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
    text,
    lowQuality: isLowQualityCommercialDescription(text, hotel),
    usedFields: {
      brand: Boolean(brand),
      scale: Boolean(scale),
      rooms: Boolean(rooms),
      meeting: Boolean(meeting),
      operator: Boolean(operator),
      demand: demand.length,
      ownership: Boolean(ownershipVerified && (economicOwner || legalOwner)),
    },
  };
}

export function resolveCommercialDescription(hotel = {}, opts = {}) {
  const fromAirtable = clean(hotel.hotelDescription) || clean(hotel.description);
  if (fromAirtable && !isLowQualityCommercialDescription(fromAirtable, hotel)) {
    return {
      version: HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
      text: fromAirtable,
      source: "airtable",
      lowQuality: false,
    };
  }
  const positioning = clean(
    hotel.overviewEnrichment &&
      hotel.overviewEnrichment.positioning &&
      hotel.overviewEnrichment.positioning.text
  );
  if (positioning && !isLowQualityCommercialDescription(positioning, hotel)) {
    return {
      version: HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
      text: positioning,
      source: "overview_enrichment",
      lowQuality: false,
    };
  }
  const built = buildCommercialDescription(hotel, opts);
  if (!built.lowQuality && built.text) {
    return { ...built, source: "rubric_v1" };
  }
  return {
    version: HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
    text: "Property description is not yet available.",
    source: "unavailable",
    lowQuality: true,
  };
}
