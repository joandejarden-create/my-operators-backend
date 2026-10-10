/**
 * Base 10 — Hotel history patterns + lookalike prospecting.
 * Private history when available; else CompHistoricalDemandPattern fallback.
 * Do not fabricate future meetings.
 */

import { GDI_BASE_OF_DEMAND } from "./taxonomy.js";

/**
 * Build HotelHistoricalDemandPattern from private rows if present.
 */
export function buildHotelHistoricalDemandPattern(hotel = {}, historyRows = []) {
  if (!historyRows.length) {
    return {
      hotelKey: hotel.hotelKey,
      available: false,
      patterns: [],
      source: "NO_PRIVATE_HISTORY",
    };
  }

  const patterns = historyRows.map((r, i) => ({
    patternId: `hhp_${hotel.hotelKey}_${i}`,
    hotelKey: hotel.hotelKey,
    groupType: r.groupType || r.industry || "UNKNOWN",
    industry: r.industry || "",
    roomNights: r.roomNights ?? "UNKNOWN",
    peakRooms: r.peakRooms ?? "UNKNOWN",
    stayPattern: r.stayPattern || "",
    arrivalDay: r.arrivalDay || "",
    lengthOfStay: r.lengthOfStay || "",
    meetingSpaceUse: r.meetingSpaceUse || "",
    fbSpend: r.fbSpend || "",
    bookingWindow: r.bookingWindow || "",
    season: r.season || "",
    sourceMarket: r.sourceMarket || "",
    buyerType: r.buyerType || "",
    source: "PRIVATE_HOTEL_HISTORY",
  }));

  return {
    hotelKey: hotel.hotelKey,
    available: true,
    patterns,
    source: "PRIVATE_HOTEL_HISTORY",
  };
}

/**
 * Comp-set demonstrated demand as proxy when no private history.
 */
export function buildCompHistoricalDemandPattern(hotel = {}, compTraces = []) {
  const patterns = [];
  for (const t of compTraces) {
    if (!t.organization && !t.eventProgram) continue;
    const evidence = t.evidenceClass || "";
    if (evidence !== "DIRECT_CONFIRMED" && evidence !== "STRONG_ASSOCIATION") continue;
    patterns.push({
      patternId: `chp_${hotel.hotelKey}_${String(t.traceId || t.organization).slice(0, 40)}`,
      hotelKey: hotel.hotelKey,
      competitorHotel: t.competitorHotel,
      organization: t.organization,
      groupType: t.groupType || "COMP_DEMAND",
      industry: t.demandEngine || "",
      roomNights: "UNKNOWN",
      peakRooms: "UNKNOWN",
      stayPattern: "",
      eventYear: t.eventYear || "",
      lodgingContext: t.hotelRole || t.fact || "",
      source: "COMP_HISTORICAL_DEMAND",
      evidenceClass: evidence,
      officialSource: t.source,
    });
  }
  return {
    hotelKey: hotel.hotelKey,
    available: patterns.length > 0,
    patterns,
    source: "COMP_HISTORICAL_DEMAND",
  };
}

/**
 * Lookalike accounts from hotel or comp patterns.
 * Output is RESEARCH_LEAD candidates needing evidence — never fabricated meetings.
 */
export function findGdiLookalikeAccounts(hotel = {}, patterns = [], opts = {}) {
  const max = opts.maxLookalikes ?? 8;
  const accounts = [];

  for (const p of patterns.slice(0, 20)) {
    const org = p.organization;
    if (!org || org.length < 4) continue;

    // Seed lookalike hypotheses from pattern type (labeled inference)
    const similars = suggestSimilarAccountTypes(p, hotel);
    for (const s of similars) {
      accounts.push({
        lookalikeAccount: s.account,
        whySimilar: s.why,
        seedOrganization: org,
        seedCompetitorHotel: p.competitorHotel || "",
        trigger: s.trigger,
        buyer: s.buyer,
        hotelThesis: `${hotel.label || hotel.hotelName}: lookalike of ${org} pattern (${p.groupType || "group"}) — needs independent evidence.`,
        evidenceNeeded: "future_decision|buyer_path|lodging_or_comp_trace|named_motion",
        inferenceLabel: "LOOKALIKE_HYPOTHESIS_NOT_CONFIRMED_MEETING",
        isPredicted: true,
        timingState: "RECURRING_EXPECTED",
        baseOfDemand: GDI_BASE_OF_DEMAND.HOTEL_HISTORY_LOOKALIKE,
        hotelKey: hotel.hotelKey,
        patternId: p.patternId,
        demandEngine: p.industry || null,
        groupMotion: p.groupType || "LOOKALIKE_GROUP",
        organization: s.account,
        organizationName: s.account,
        title: `Lookalike: ${s.account} ↔ ${org}`,
        officialSource: p.officialSource || "",
        generatorOnly: !s.namedConcrete,
        namedEntity: s.namedConcrete ? s.account : "",
        participationEvidence: Boolean(p.evidenceClass),
        plausibleTravelingGroup: true,
        marketRelevant: true,
        hotelMotion: true,
        accommodationSignal: Boolean(p.lodgingContext),
        historicAttendance: true,
        knownTravelPattern: true,
      });
    }
  }

  // Dedupe by account name
  const seen = new Set();
  const deduped = [];
  for (const a of accounts) {
    const k = a.lookalikeAccount.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    deduped.push(a);
    if (deduped.length >= max) break;
  }
  return deduped;
}

function suggestSimilarAccountTypes(pattern, hotel) {
  const g = String(pattern.groupType || pattern.industry || "").toLowerCase();
  const org = pattern.organization;
  const out = [];

  // Concrete: same buyer may repeat — keep as lookalike of self for win-back / next cycle
  out.push({
    account: org,
    why: `Repeat / lookalike of demonstrated ${pattern.competitorHotel || "comp"} demand`,
    trigger: "REPEAT_BUYER_OR_NEXT_CYCLE",
    buyer: "prior_buyer_or_agency",
    namedConcrete: true,
  });

  if (/pharma|medical|investigator|advisory/i.test(g) || /pharma|medical|CRO/i.test(org)) {
    out.push({
      account: "Similar pharma / CRO training programs (unnamed cohort)",
      why: "Same industry + meeting+F&B training pattern",
      trigger: "INDUSTRY_LOOKALIKE",
      buyer: "medical_affairs_or_events",
      namedConcrete: false,
    });
  }
  if (/corporate|kickoff|sales|partner/i.test(g)) {
    out.push({
      account: "Similar regional corporate meeting programs (unnamed cohort)",
      why: "Recurring corporate meeting pattern",
      trigger: "CORPORATE_LOOKALIKE",
      buyer: "corporate_events",
      namedConcrete: false,
    });
  }
  if (/sports|tournament|team/i.test(g)) {
    out.push({
      account: "Similar federation / team travel programs (unnamed cohort)",
      why: "Sports lodging pattern",
      trigger: "SPORTS_LOOKALIKE",
      buyer: "team_travel_or_federation",
      namedConcrete: false,
    });
  }
  if (/association|congress|conference/i.test(g) || /association|society|congress/i.test(org)) {
    out.push({
      account: "Similar association overflow / housing programs (unnamed cohort)",
      why: "Association housing pattern in market",
      trigger: "ASSOCIATION_LOOKALIKE",
      buyer: "secretariat_or_housing_bureau",
      namedConcrete: false,
    });
  }

  // Market flavor
  if (hotel.hotelKey === "YOTEL" || hotel.hotelKey === "AC") {
    out.push({
      account: "European HQ / feeder-market corporate accounts (unnamed cohort)",
      why: `Feeder markets: ${(hotel.feederMarkets || []).slice(0, 3).join(", ")}`,
      trigger: "FEEDER_MARKET_LOOKALIKE",
      buyer: "regional_events",
      namedConcrete: false,
    });
  }

  return out.slice(0, 3);
}
