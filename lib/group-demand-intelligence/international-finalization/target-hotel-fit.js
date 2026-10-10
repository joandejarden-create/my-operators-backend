/**
 * Deterministic Target Hotel Fit — evidence-backed, never city-only or availability-based.
 * Ready must not use UNKNOWN.
 */

import { TARGET_HOTEL_FIT } from "./constants.js";

/**
 * @param {object} input
 * @param {object} input.hotel — rooms, meetingSpace, market, city, brand, chainScale, lat/lng
 * @param {object} input.demand — destinationCity, venueName, venueCity, groupProfile, meetingRequired, officialHotels, knownCompetitorHost, historicalSecondaryPartner, accessNotes
 * @param {object[]} [input.fitEvidence] — { source, fact, value }
 */
export function evaluateTargetHotelFit({ hotel = {}, demand = {}, fitEvidence = [] } = {}) {
  const evidence = [...fitEvidence];
  const factors = [];
  const push = (id, status, note, source = null) => {
    factors.push({ id, status, note, source });
    if (source || note) {
      evidence.push({ source: source || "DERIVED_ASSESSMENT", fact: id, value: note, status });
    }
  };

  const hotelCity = norm(hotel.city || hotel.market);
  const destCity = norm(demand.destinationCity || demand.market || demand.venueCity);
  const venueCity = norm(demand.venueCity || demand.destinationCity);

  // Destination match — required; city alone is not enough for STRONG
  let destinationOk = false;
  if (destCity && hotelCity && (hotelCity === destCity || hotelCity.includes(destCity) || destCity.includes(hotelCity))) {
    destinationOk = true;
    push("destination_match", "PASS", `Hotel city/market aligns with demand destination (${hotel.city || hotel.market})`, "DEMAND_GEO");
  } else if (!destCity) {
    push("destination_match", "UNKNOWN", "Demand destination city not established", null);
  } else {
    push("destination_match", "FAIL", `Hotel ${hotel.city || "?"} vs demand ${demand.destinationCity || "?"}`, "DEMAND_GEO");
  }

  if (!destinationOk && destCity) {
    return finish(TARGET_HOTEL_FIT.NO_FIT, factors, evidence, {
      reason: "WRONG_DESTINATION",
      readyEligible: false,
    });
  }
  if (!destinationOk) {
    return finish(TARGET_HOTEL_FIT.UNKNOWN, factors, evidence, {
      reason: "DESTINATION_UNKNOWN",
      readyEligible: false,
    });
  }

  // Official / host lock — competitor exclusive host weakens subject fit unless secondary path evidenced
  // Historical secondary ≠ current placement; never invent overflow
  const officialHotels = (demand.knownOfficialHotels || demand.officialHotels || []).map(norm);
  const hotelName = norm(hotel.name || hotel.displayName);
  const isOfficial = officialHotels.some((o) => o && hotelName && (o.includes(hotelName) || hotelName.includes(o)));
  const competitorHost = Boolean(demand.knownCompetitorHost || demand.officialHostHotel);
  const secondaryOpen = demand.secondaryPartnerOpen === true || demand.overflowEvidenced === true;
  if (isOfficial) {
    push("official_hotel_structure", "PASS", "Subject named as official/partner hotel", "CAMPAIGN_HOUSING");
  } else if (competitorHost && secondaryOpen) {
    push(
      "official_hotel_structure",
      "PLAUSIBLE",
      `Host locked (${demand.officialHostHotel || demand.knownCompetitorHost}); secondary/overflow path currently evidenced`,
      "CAMPAIGN_HOUSING"
    );
  } else if (competitorHost && demand.historicalSecondaryPartner) {
    push(
      "official_hotel_structure",
      "WARN",
      "Host locked; historical secondary/partner pattern exists — not treated as current placement",
      "HISTORICAL_PROCESS"
    );
  } else if (competitorHost) {
    push(
      "official_hotel_structure",
      "WARN",
      `Competitor/host hotel locked (${demand.officialHostHotel || demand.knownCompetitorHost}); secondary path not evidenced`,
      "CAMPAIGN_HOUSING"
    );
  }

  // Room count — only when known
  const rooms = Number(hotel.rooms || hotel.roomCount || 0);
  if (rooms > 0) {
    const need = Number(demand.estimatedRooms || demand.minRooms || 0);
    if (need > 0 && rooms < need * 0.5) {
      push("room_count", "FAIL", `Hotel rooms ${rooms} well below stated need ${need}`, "HOTEL_INTEL");
    } else if (rooms >= 100) {
      push("room_count", "PASS", `${rooms} rooms supports group / congress lodging`, "HOTEL_INTEL");
    } else {
      push("room_count", "PLAUSIBLE", `${rooms} rooms — small-group plausible only`, "HOTEL_INTEL");
    }
  } else {
    push("room_count", "UNKNOWN", "Room count not in evidence", null);
  }

  // Meeting capacity — only when meeting required AND facts exist
  const meetingRequired = Boolean(demand.meetingRequired || demand.needsMeetingSpace);
  const meetingRooms = Number(hotel.meetingSpace?.meetingRooms || hotel.meetingRooms || 0);
  const largestSqM = Number(hotel.meetingSpace?.largestRoom?.sqM || hotel.largestMeetingSqM || 0);
  if (meetingRequired) {
    if (meetingRooms > 0 || largestSqM > 0) {
      push(
        "meeting_capacity",
        meetingRooms >= 5 || largestSqM >= 200 ? "PASS" : "PLAUSIBLE",
        `Meeting rooms=${meetingRooms || "?"} largestSqM=${largestSqM || "?"}`,
        "HOTEL_INTEL"
      );
    } else {
      push("meeting_capacity", "UNKNOWN", "Meeting required but hotel meeting facts missing", null);
    }
  } else {
    push("meeting_capacity", "N/A", "Demand does not require on-site meeting package", "DEMAND_PROFILE");
  }

  // Brand / segment
  const scale = norm(hotel.chainScale || hotel.segment);
  const demandScale = norm(demand.preferredScale || "");
  if (scale) {
    push("brand_segment", "PASS", `Hotel scale ${hotel.chainScale || hotel.segment}`, "HOTEL_INTEL");
  }
  if (demandScale && scale && !scale.includes(demandScale) && !demandScale.includes(scale)) {
    push("brand_segment_match", "WARN", `Demand preferred ${demand.preferredScale} vs hotel ${hotel.chainScale}`, "DEMAND_PROFILE");
  }

  // Access / venue adjacency — only with evidence notes (no invented distance)
  if (demand.venueAccessNote) {
    push("venue_access", "PLAUSIBLE", demand.venueAccessNote, demand.venueAccessSource || "PUBLIC_NOTE");
  } else if (demand.sameSubmarket === true) {
    push("venue_access", "PASS", "Same Dealality submarket / corridor as demand node", "GEO_CORRIDOR");
  } else {
    push("venue_access", "UNKNOWN", "No evidenced venue-access / corridor link", null);
  }

  // International traveler / group profile
  if (demand.groupProfile) {
    push("group_profile", "PLAUSIBLE", `Group profile: ${demand.groupProfile}`, "CAMPAIGN");
  }

  // Self-booking recommended list — fit is about being list-eligible, not block
  if (demand.selectionModel === "SELF_BOOKING_RECOMMENDED_LIST") {
    if (demand.onRecommendedList === true) {
      push("list_inclusion", "PASS", "On published recommended list", "OFFICIAL_LIST");
    } else if (demand.onRecommendedList === false) {
      push("list_inclusion", "WARN", "Absent from current recommended list — inclusion ask required", "OFFICIAL_LIST");
    }
  }

  // Score disposition — never STRONG from destination alone
  const fails = factors.filter((f) => f.status === "FAIL");
  const passes = factors.filter((f) => f.status === "PASS");
  const warns = factors.filter((f) => f.status === "WARN");
  const unknowns = factors.filter((f) => f.status === "UNKNOWN");

  if (fails.some((f) => f.id === "destination_match")) {
    return finish(TARGET_HOTEL_FIT.NO_FIT, factors, evidence, { reason: "WRONG_DESTINATION", readyEligible: false });
  }
  if (fails.length) {
    return finish(TARGET_HOTEL_FIT.WEAK_FIT, factors, evidence, { reason: "FACTOR_FAIL", readyEligible: false });
  }

  // Host locked + no *current* secondary evidence → not Ready-eligible (historical pattern ≠ current)
  if (competitorHost && !isOfficial && !secondaryOpen) {
    return finish(TARGET_HOTEL_FIT.WEAK_FIT, factors, evidence, {
      reason: demand.historicalSecondaryPartner
        ? "HOST_LOCKED_HISTORICAL_SECONDARY_ONLY"
        : "HOST_LOCKED_NO_SECONDARY_EVIDENCE",
      readyEligible: false,
      note: "Do not infer overflow; do not treat historical partner as current",
    });
  }

  // Absent from published recommended list → PLAUSIBLE at best until inclusion evidenced
  if (demand.selectionModel === "SELF_BOOKING_RECOMMENDED_LIST" && demand.onRecommendedList === false) {
    return finish(TARGET_HOTEL_FIT.PLAUSIBLE_FIT, factors, evidence, {
      reason: "NOT_ON_CURRENT_RECOMMENDED_LIST",
      readyEligible: false,
      note: "List inclusion ask required before Ready-eligible fit",
    });
  }

  const substantivePasses = passes.filter((f) => f.id !== "destination_match");
  if (substantivePasses.length >= 2 && warns.length === 0 && unknowns.filter((u) => u.id === "meeting_capacity" || u.id === "room_count").length === 0) {
    return finish(TARGET_HOTEL_FIT.STRONG_FIT, factors, evidence, { reason: "MULTI_FACTOR_EVIDENCED", readyEligible: true });
  }
  if (destinationOk && (substantivePasses.length >= 1 || factors.some((f) => f.status === "PLAUSIBLE"))) {
    return finish(TARGET_HOTEL_FIT.PLAUSIBLE_FIT, factors, evidence, {
      reason: "DESTINATION_PLUS_SUPPORTING_FACTOR",
      readyEligible: true,
      note: "City match alone is insufficient for STRONG_FIT",
    });
  }
  if (unknowns.length >= 3) {
    return finish(TARGET_HOTEL_FIT.UNKNOWN, factors, evidence, { reason: "INSUFFICIENT_FIT_FACTS", readyEligible: false });
  }
  return finish(TARGET_HOTEL_FIT.PLAUSIBLE_FIT, factors, evidence, {
    reason: "DESTINATION_ALIGNED_LIMITED_EVIDENCE",
    readyEligible: true,
  });
}

function finish(fitClass, factors, evidence, meta) {
  return {
    targetHotelFit: fitClass,
    fitClass,
    factors,
    fitEvidence: evidence,
    readyEligibleFit: meta.readyEligible === true && fitClass !== TARGET_HOTEL_FIT.UNKNOWN,
    reason: meta.reason,
    note: meta.note || null,
    cityOnlyInference: false,
  };
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function assertFitNotCityOnly(result) {
  const passes = (result.factors || []).filter((f) => f.status === "PASS");
  if (result.targetHotelFit === TARGET_HOTEL_FIT.STRONG_FIT && passes.every((f) => f.id === "destination_match")) {
    return { ok: false, reason: "STRONG_FIT_FROM_CITY_ONLY_FORBIDDEN" };
  }
  return { ok: true };
}
