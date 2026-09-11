/**
 * Hotel Intelligence — property fundamentals resolver (read path).
 * Census preferred; validated research fills gaps. Never writes Census.
 * Hotel-neutral — not Cambridge/Dovetail specific.
 */

export const ROOMS_SOURCE = Object.freeze({
  CENSUS: "dealality_census",
  HOTEL_INTELLIGENCE: "hotel_intelligence",
  RESEARCH: "validated_research",
  PROVIDER_RESEARCH: "provider_research",
  MISSING: "missing",
});

export const ROOMS_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  PROBABLE: "PROBABLE",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
  CONFLICTED: "CONFLICTED",
});

function isMissingRooms(v) {
  if (v == null || v === "") return true;
  if (typeof v === "string" && /^(unknown|n\/?a|not available|tbd|—|-)$/i.test(v.trim())) {
    return true;
  }
  const n = Number(v);
  if (!Number.isFinite(n)) return true;
  return n <= 0;
}

function toPositiveInt(v) {
  if (isMissingRooms(v)) return null;
  const n = Math.round(Number(v));
  return Number.isFinite(n) && n > 0 ? n : null;
}

function rankConfidence(c) {
  const order = {
    HIGH: 4,
    VERIFIED: 4,
    PROBABLE: 3,
    MEDIUM: 2,
    LOW: 1,
    CONFLICTED: 0,
  };
  return order[String(c || "").toUpperCase()] || 0;
}

function isCurrentTemporal(status) {
  const s = String(status || "CURRENT").toUpperCase();
  if (!s || s === "CURRENT" || s === "OPEN" || s === "LIVE") return true;
  if (/HISTORIC|FORMER|PRE_|ANNOUNCED|PLANNED|PIPELINE|FUTURE|UNCERTAIN/.test(s)) {
    return false;
  }
  return true;
}

function isAutoDisplayConfidence(confidence, sourceType = "") {
  const c = String(confidence || "").toUpperCase();
  if (c === "HIGH" || c === "VERIFIED") return true;
  if (c === "PROBABLE" || c === "MEDIUM") {
    return /official|first.?party|press|operator|owner|government|webhound|research_corpus|validated/i.test(
      String(sourceType || "")
    );
  }
  return false;
}

/**
 * Identity guard: observation hotel name must overlap the portfolio hotel name.
 */
export function roomsObservationMatchesHotel(hotel = {}, observation = {}) {
  const hotelName = String(hotel.name || hotel.hotel_name || "").toLowerCase().trim();
  const obsName = String(observation.hotel_name || observation.property_name || "").toLowerCase().trim();
  if (!hotelName || !obsName) return false;
  if (hotelName === obsName) return true;
  const tokens = (s) =>
    s
      .replace(/[^a-z0-9]+/g, " ")
      .split(/\s+/)
      .filter((t) => t.length > 2 && !/^(the|and|hotel|resort|spa|lodge)$/.test(t));
  const a = tokens(hotelName);
  const b = tokens(obsName);
  const shared = a.filter((t) => b.includes(t));
  return shared.length >= 2 || (shared.length >= 1 && (hotelName.includes(obsName) || obsName.includes(hotelName)));
}

function pickBestResearchObservation(hotel, observations) {
  return (observations || [])
    .filter((o) => o && o.field !== "accommodation_units")
    .filter((o) => roomsObservationMatchesHotel(hotel, o))
    .filter((o) => toPositiveInt(o.value ?? o.rooms) != null)
    .filter((o) => isCurrentTemporal(o.temporal_status || o.temporal))
    .map((o) => ({
      ...o,
      value: toPositiveInt(o.value ?? o.rooms),
      confidence: String(o.confidence || "PROBABLE").toUpperCase(),
    }))
    .filter((o) => isAutoDisplayConfidence(o.confidence, o.source_type || o.source || o.source_provider))
    .sort((a, b) => rankConfidence(b.confidence) - rankConfidence(a.confidence))[0] || null;
}

/**
 * Resolve room/key count for a portfolio / property row.
 *
 * Precedence:
 * 1. Canonical Census (positive)
 * 2. Verified Hotel Intelligence fundamental
 * 3. Validated research-derived observation / asset rooms
 * 4. High-confidence provider research (via observations)
 * 5. —
 *
 * Zero / null / unknown Census values do not block fallback.
 */
export function resolvePropertyRooms(hotel = {}, opts = {}) {
  const census = toPositiveInt(opts.censusRooms ?? hotel.census_rooms ?? hotel.censusRooms);
  const hi = toPositiveInt(
    opts.hotelIntelligenceRooms ?? hotel.hi_rooms ?? hotel.hotel_intelligence_rooms
  );
  const observations = Array.isArray(opts.researchObservations)
    ? opts.researchObservations
    : Array.isArray(hotel.research_rooms_observations)
      ? hotel.research_rooms_observations
      : [];

  const bestResearch = pickBestResearchObservation(hotel, observations);

  // Sparse asset.rooms on the hotel object (already normalized research)
  const assetRooms = toPositiveInt(hotel.rooms);
  const assetConfidence = String(hotel.rooms_confidence || hotel.confidence || "HIGH").toUpperCase();
  const assetEligible =
    assetRooms &&
    isAutoDisplayConfidence(assetConfidence, hotel.rooms_source_type || "validated_research")
      ? {
          value: assetRooms,
          confidence: assetConfidence === "MEDIUM" ? "PROBABLE" : assetConfidence,
          source_type: hotel.rooms_source_type || "validated_research",
          source_url: hotel.rooms_source_url || null,
          source_title: hotel.rooms_source_title || null,
          temporal_status: "CURRENT",
        }
      : null;

  let conflict = null;
  if (census && bestResearch && bestResearch.value !== census) {
    const delta = Math.abs(bestResearch.value - census);
    if (delta >= 3 || delta / census >= 0.05) {
      conflict = {
        census,
        research: bestResearch.value,
        note: "Material rooms conflict — Census retained for display",
      };
    }
  }

  if (census) {
    return {
      value: census,
      source: ROOMS_SOURCE.CENSUS,
      confidence: ROOMS_CONFIDENCE.HIGH,
      provenance: {
        field: "rooms",
        value: census,
        source_provider: ROOMS_SOURCE.CENSUS,
        source_type: "census",
        confidence: ROOMS_CONFIDENCE.HIGH,
        temporal_status: "CURRENT",
        resolution_method: "census_first",
      },
      conflict,
    };
  }

  if (hi) {
    return {
      value: hi,
      source: ROOMS_SOURCE.HOTEL_INTELLIGENCE,
      confidence: ROOMS_CONFIDENCE.HIGH,
      provenance: {
        field: "rooms",
        value: hi,
        source_provider: ROOMS_SOURCE.HOTEL_INTELLIGENCE,
        confidence: ROOMS_CONFIDENCE.HIGH,
        temporal_status: "CURRENT",
        resolution_method: "hotel_intelligence_fundamental",
      },
      conflict: null,
    };
  }

  const chosen =
    bestResearch && assetEligible
      ? rankConfidence(bestResearch.confidence) >= rankConfidence(assetEligible.confidence)
        ? bestResearch
        : assetEligible
      : bestResearch || assetEligible;

  if (chosen) {
    return {
      value: chosen.value,
      source: ROOMS_SOURCE.RESEARCH,
      confidence: chosen.confidence || ROOMS_CONFIDENCE.PROBABLE,
      provenance: {
        field: "rooms",
        value: chosen.value,
        source_provider: ROOMS_SOURCE.RESEARCH,
        source_type: chosen.source_type || "validated_research",
        source_url: chosen.source_url || null,
        source_title: chosen.source_title || null,
        confidence: chosen.confidence || ROOMS_CONFIDENCE.PROBABLE,
        temporal_status: chosen.temporal_status || "CURRENT",
        resolution_method: "research_fallback_no_census",
      },
      conflict: null,
    };
  }

  return {
    value: null,
    source: ROOMS_SOURCE.MISSING,
    confidence: null,
    provenance: {
      field: "rooms",
      value: null,
      source_provider: ROOMS_SOURCE.MISSING,
      resolution_method: "no_valid_rooms",
    },
    conflict: null,
  };
}

/**
 * Apply rooms resolution onto a portfolio hotel row (returns new object).
 */
export function applyRoomsResolutionToPortfolioHotel(hotel = {}, opts = {}) {
  const resolved = resolvePropertyRooms(hotel, opts);
  const next = { ...hotel };
  if (resolved.value != null) {
    next.rooms = resolved.value;
    next.rooms_display = resolved.value;
    next.rooms_status = "LIVE";
    next.rooms_source = resolved.source;
    next.rooms_confidence = resolved.confidence;
    next.rooms_provenance = resolved.provenance;
  } else {
    if (isMissingRooms(next.rooms)) {
      next.rooms = null;
      next.rooms_display = null;
      next.rooms_status = "UNKNOWN";
    } else {
      next.rooms_display = next.rooms;
      next.rooms_status = "LIVE";
    }
    next.rooms_provenance = resolved.provenance;
  }
  if (resolved.conflict) next.rooms_conflict = resolved.conflict;
  return next;
}

/**
 * Generic property fundamentals resolver (rooms + pass-through fields).
 */
export function resolvePropertyFundamentals(hotel = {}, opts = {}) {
  const rooms = resolvePropertyRooms(hotel, opts);
  return {
    hotel_id: hotel.hotel_id || hotel.id || hotel.airtable_record_id || null,
    hotel_name: hotel.name || hotel.hotel_name || null,
    market: hotel.market || hotel.market_display || hotel.city || null,
    brand: hotel.brand || hotel.brand_display || hotel.affiliation_display || null,
    status: hotel.status || null,
    rooms: {
      value: rooms.value,
      provenance: rooms.provenance,
      confidence: rooms.confidence,
      source: rooms.source,
      conflict: rooms.conflict,
    },
  };
}

export { isMissingRooms, toPositiveInt };
