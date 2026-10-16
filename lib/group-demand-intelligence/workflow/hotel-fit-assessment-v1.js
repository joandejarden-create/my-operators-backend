/**
 * Canonical hotel-fit assessment V2 — shared across opportunity types.
 *
 * Dimensions (GM framework / scoring.js):
 *   physicalFit, geographyFit, timing, commercialValue,
 *   historicalFit, competitiveAccessibility, contactability
 *
 * Each dimension stores: score | status | rationale | evidenceRefs |
 * claimKind | assessmentDate | ruleVersion.
 *
 * Status: assessed | insufficient_evidence | not_applicable
 * Never invent scores to fill gaps. Unknown ≠ 0. N/A ≠ failure.
 * Overall score only when enough applicable dimensions are assessed;
 * weights are renormalized over assessed applicable dims (documented).
 *
 * Use-case profiles adjust evidence rules (not separate score systems):
 *   dated_group | standing_corporate | local_meetings_catering |
 *   rooms_plus_meetings | venue_referral
 */

import {
  computeEvidenceConfidence,
  buildEvidenceConfidenceExplanation,
  HOTEL_FIT_COMPONENT_LABELS,
  DEFAULT_HOTEL_FIT_WEIGHTS,
} from "../scoring.js";
import { loadHotelConfig } from "./independent-hotel-fit-v1.js";
import {
  stripEvidenceClaimPrefixes,
  detectClaimKindFromProse,
} from "./customer-facing-prose-v1.js";
import {
  DEMAND_USE_CASE,
  normalizeDemandUseCases,
} from "./shared-demand-model-v1.js";
import { isLocalMeetingsCateringOpportunity } from "./local-meetings-catering-research-v1.js";
import { isScalingCompanyOpportunity } from "./scaling-companies-research-v1.js";
import {
  OPPORTUNITY_QUALIFICATION,
  BOOKING_WINDOW,
} from "../claim-types.js";
import {
  detectNarrativeConsistencyIssues,
  repairNarrativeForConsistency,
} from "./hotel-fit-narrative-consistency-v1.js";

export const HOTEL_FIT_ASSESSMENT_VERSION = "gdi-hotel-fit-assessment-v2";
export const HOTEL_FIT_DIMENSION_RULE_VERSION = "gdi-hotel-fit-dim-rules-v2";

export const DIMENSION_STATUS = Object.freeze({
  ASSESSED: "assessed",
  INSUFFICIENT_EVIDENCE: "insufficient_evidence",
  NOT_APPLICABLE: "not_applicable",
});

export const USE_CASE_PROFILE = Object.freeze({
  DATED_GROUP: "dated_group",
  STANDING_CORPORATE: "standing_corporate",
  LOCAL_MEETINGS_CATERING: "local_meetings_catering",
  ROOMS_PLUS_MEETINGS: "rooms_plus_meetings",
  VENUE_REFERRAL: "venue_referral",
});

/** Min assessed applicable weight share before emitting overall numeric. */
export const MIN_OVERALL_WEIGHT_SHARE = 0.45;
/** Min count of assessed applicable dimensions before overall numeric. */
export const MIN_OVERALL_ASSESSED_COUNT = 3;

function text(v) {
  return stripEvidenceClaimPrefixes(String(v || "").replace(/\s+/g, " ").trim());
}

function blob(opp = {}) {
  return [
    opp.title,
    opp.organizationName,
    opp.summaryWhat,
    opp.destinationStatus,
    opp.eventLocationSummary,
    opp.eventCity,
    opp.venueCity,
    opp.city,
    opp.hotelOpportunityThesis,
    opp.fitExplanation,
    opp.whyThisHotel,
    opp.hotelFitRationale,
    opp.competitiveContextNote,
    opp.venueHistoryNote,
    opp.whyHotelDemand,
    opp.travelingCohort,
    opp.localConnection,
    opp.proposedHotelUse,
  ]
    .map(text)
    .filter(Boolean)
    .join(" ");
}

function hotelBits(cfg = {}) {
  return {
    name:
      text(cfg.displayName) ||
      text(cfg.name) ||
      text(cfg.aliases?.note)?.split(".")[0] ||
      "this hotel",
    rooms: cfg.capabilityProfile?.totalGuestrooms ?? cfg.capabilityProfile?.rooms ?? null,
    meetingRooms:
      cfg.capabilityProfile?.meetingRoomCount ??
      cfg.capabilityProfile?.meetingRoomsIndoor ??
      null,
    meetingSqFt: cfg.capabilityProfile?.totalMeetingSpaceSqFt ?? null,
    soft: text(cfg.capabilityProfile?.softBrand),
    territory: text(cfg.demandTerritory?.label),
    city: text(cfg.city) || text(cfg.locale?.city),
    includes: cfg.demandTerritory?.includes || [],
  };
}

export function detectUseCaseProfile(opp = {}) {
  const useCases = normalizeDemandUseCases(
    opp.demandUseCases || opp.useCases || []
  );
  if (
    opp.peVenueId ||
    opp.opportunityType === "VENUE_PARTNERSHIP" ||
    opp.opportunityType === "VENUE_REFERRAL_PARTNERSHIP" ||
    useCases.includes(DEMAND_USE_CASE.VENUE_REFERRAL_PARTNERSHIP)
  ) {
    return USE_CASE_PROFILE.VENUE_REFERRAL;
  }
  if (
    isLocalMeetingsCateringOpportunity(opp) ||
    opp.researchSegment === "local_meetings_catering" ||
    opp.cateringOnly === true ||
    (useCases.includes(DEMAND_USE_CASE.MEETINGS_AND_CATERING) &&
      !useCases.includes(DEMAND_USE_CASE.GROUP_LODGING) &&
      !useCases.includes(DEMAND_USE_CASE.ROOMS_PLUS_MEETINGS))
  ) {
    return USE_CASE_PROFILE.LOCAL_MEETINGS_CATERING;
  }
  if (
    isScalingCompanyOpportunity(opp) ||
    opp.researchSegment === "scaling_companies" ||
    opp.detailLayout === "CORPORATE_ACCOUNT_RELATIONSHIP" ||
    opp.opportunityType === "CORPORATE_ACCOUNT_RELATIONSHIP"
  ) {
    return USE_CASE_PROFILE.STANDING_CORPORATE;
  }
  if (
    useCases.includes(DEMAND_USE_CASE.ROOMS_PLUS_MEETINGS) ||
    (/rooms?\s*\+\s*meetings|meetings?\s*and\s*lodging/i.test(
      `${opp.title || ""} ${opp.proposedHotelUse || ""}`
    ))
  ) {
    return USE_CASE_PROFILE.ROOMS_PLUS_MEETINGS;
  }
  return USE_CASE_PROFILE.DATED_GROUP;
}

function occasionGeo(opp = {}) {
  return text(
    opp.eventLocationSummary ||
      opp.destinationStatus ||
      opp.localConnection ||
      [opp.eventCity, opp.venueCity, opp.city].filter(Boolean).join(", ") ||
      ""
  );
}

function classifyGeography(opp = {}, hotel = {}, cfg = {}, profile) {
  const geo = occasionGeo(opp).toLowerCase();
  const full = blob(opp).toLowerCase();
  const hotelCity = (hotel.city || "").toLowerCase();
  const kw = cfg.demandTerritory?.classificationKeywords || {};
  const includesBlob = (hotel.includes || []).join(" ").toLowerCase();

  for (const [klass, words] of Object.entries(kw)) {
    for (const w of words || []) {
      if (full.includes(String(w).toLowerCase()) || geo.includes(String(w).toLowerCase())) {
        return { class: klass, geo: occasionGeo(opp) || null };
      }
    }
  }

  if (
    /mayflower|connecticut|downtown\s*dc|farragut|dupont/i.test(hotel.name + " " + hotelCity) &&
    /arlington|crystal\s*city|crystal\s*gateway/i.test(full) &&
    profile === USE_CASE_PROFILE.DATED_GROUP
  ) {
    return { class: "TERRITORY_STRETCH", geo: occasionGeo(opp) || "Arlington / Crystal City pattern" };
  }

  // Standing corporate / LMC / PE: local office or downtown catchment
  if (
    profile === USE_CASE_PROFILE.STANDING_CORPORATE ||
    profile === USE_CASE_PROFILE.LOCAL_MEETINGS_CATERING ||
    profile === USE_CASE_PROFILE.VENUE_REFERRAL
  ) {
    const locBlob = `${geo} ${full}`;
    if (
      locBlob &&
      ((hotelCity && locBlob.includes(hotelCity)) ||
        /downtown|connecticut|pennsylvania avenue|gaslamp|bayside|harbor|seaport|dupont|little italy/i.test(
          locBlob
        ) ||
        includesBlob
          .split(/\s+/)
          .some((t) => t.length > 4 && locBlob.includes(t)))
    ) {
      return {
        class: "CORE_TERRITORY",
        geo: occasionGeo(opp) || geo || hotel.territory || hotel.city,
      };
    }
    if (
      /washington|district of columbia|\bdc\b|san diego|gaslamp/i.test(locBlob) &&
      /washington|dc|san diego/i.test(hotelCity + " " + hotel.territory)
    ) {
      return {
        class: "TERRITORY_COMPETITIVE",
        geo: occasionGeo(opp) || hotel.territory,
      };
    }
  }

  if (geo && hotelCity && geo.includes(hotelCity)) {
    return { class: "CORE_TERRITORY", geo: occasionGeo(opp) };
  }
  if (
    /washington|district of columbia|\bdc\b/.test(full) &&
    /washington|dc/i.test(hotelCity + hotel.territory)
  ) {
    return { class: "TERRITORY_COMPETITIVE", geo: occasionGeo(opp) || "Washington DC metro" };
  }
  if (
    /san diego|gaslamp|bayside|harbor island|little italy/i.test(full) &&
    /san diego/i.test(hotelCity + hotel.territory)
  ) {
    return { class: "TERRITORY_COMPETITIVE", geo: occasionGeo(opp) || "San Diego downtown / waterfront" };
  }
  return { class: null, geo: occasionGeo(opp) || null };
}

function hasUsableContact(opp = {}) {
  const c = opp.primaryContact || {};
  return Boolean(
    text(c.email) ||
      text(c.phone) ||
      text(opp.primaryContactEmail) ||
      text(opp.functionalContactEmail) ||
      text(opp.organizationContactEmail) ||
      text(opp.publicContactPath) ||
      text(opp.organizationContactUrl) ||
      text(opp.contactUrl)
  );
}

function dimBase(partial = {}) {
  return {
    status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
    score: null,
    level: partial.level || "Insufficient evidence",
    rationale: partial.rationale || "",
    evidenceRefs: partial.evidenceRefs || [],
    inputFacts: partial.inputFacts || [],
    inferences: partial.inferences || [],
    claimKind: partial.claimKind || "UNKNOWN",
    assessmentDate: new Date().toISOString().slice(0, 10),
    ruleVersion: HOTEL_FIT_DIMENSION_RULE_VERSION,
    ...partial,
  };
}

function assessPhysical(opp, hotel, profile) {
  const peak =
    opp.peakRooms ?? opp.estimatedPeakRooms ?? opp.publishedPeakRooms ?? null;
  const peakN = peak != null && Number.isFinite(Number(peak)) ? Number(peak) : null;

  if (profile === USE_CASE_PROFILE.LOCAL_MEETINGS_CATERING) {
    // Guestrooms not required — assess meeting/catering facilities
    if (hotel.meetingRooms != null || hotel.meetingSqFt != null) {
      const mr = hotel.meetingRooms;
      const sq = hotel.meetingSqFt;
      const strong = (mr != null && mr >= 8) || (sq != null && sq >= 8000);
      return dimBase({
        status: DIMENSION_STATUS.ASSESSED,
        scorable: true,
        score: strong ? 82 : 68,
        level: strong
          ? "Meeting/catering facilities align"
          : "Meeting product present; confirm function size",
        rationale: `${hotel.name} meeting inventory (${mr != null ? `${mr} rooms` : "rooms n/a"}${sq != null ? `, ~${sq} sq ft` : ""}) supports local meetings/catering without a guestroom requirement. Confirm banquet capacity vs occasion size before primary pursuit.`,
        inputFacts: [
          mr != null ? `meetingRooms=${mr}` : null,
          sq != null ? `meetingSqFt=${sq}` : null,
          "guestroomRequirement=not_required",
        ].filter(Boolean),
        claimKind: "MIXED",
      });
    }
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "Meeting inventory not configured",
      rationale: "Hotel meeting/catering capacity not on profile — physical fit for catering not scored.",
      claimKind: "UNKNOWN",
    });
  }

  if (profile === USE_CASE_PROFILE.VENUE_REFERRAL) {
    // Complementary lodging vs venue — rooms matter; peak may be unknown
    if (hotel.rooms != null) {
      return dimBase({
        status: DIMENSION_STATUS.ASSESSED,
        scorable: true,
        score: hotel.rooms >= 200 ? 75 : 62,
        level: "Complementary lodging capacity present",
        rationale: `${hotel.name} (~${hotel.rooms} guestrooms) can support overflow/referral lodging for a nearby venue partnership when traveling guests are evidenced. Venue marketing alone does not prove room demand.`,
        inputFacts: [`totalGuestrooms=${hotel.rooms}`],
        inferences: ["Referral lodging potential — not confirmed overflow allocation"],
        claimKind: "INFERENCE",
      });
    }
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "Property capacity unknown",
      rationale: "Guestroom inventory not configured for venue-referral physical assessment.",
    });
  }

  if (profile === USE_CASE_PROFILE.STANDING_CORPORATE) {
    // No peak required — assess rooms + meetings as relationship product
    if (hotel.rooms != null) {
      const meetingsOk =
        (hotel.meetingRooms != null && hotel.meetingRooms >= 5) ||
        (hotel.meetingSqFt != null && hotel.meetingSqFt >= 5000);
      return dimBase({
        status: DIMENSION_STATUS.ASSESSED,
        scorable: true,
        score: meetingsOk ? 78 : 65,
        level: meetingsOk
          ? "Rooms + meetings product supports corporate use"
          : "Lodging product present; meetings depth limited",
        rationale: `${hotel.name} (~${hotel.rooms} guestrooms${meetingsOk ? `, ${hotel.meetingRooms ?? "n/a"} meeting rooms` : ""}) can support standing corporate lodging/meetings uses. No invented room-night volume — confirm actual visiting-team or workshop needs.`,
        inputFacts: [
          `totalGuestrooms=${hotel.rooms}`,
          hotel.meetingRooms != null ? `meetingRooms=${hotel.meetingRooms}` : null,
        ].filter(Boolean),
        claimKind: "MIXED",
      });
    }
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "Property capacity unknown",
      rationale: "Hotel capacity not configured; corporate physical fit not scored.",
    });
  }

  // Dated group / rooms+meetings — peak vs keys when available
  if (peakN != null && hotel.rooms != null) {
    const ratio = peakN / hotel.rooms;
    let level = "Workable";
    let score = 70;
    if (ratio <= 0.35) {
      level = "Strong capacity alignment";
      score = 85;
    } else if (ratio <= 0.55) {
      level = "Workable peak vs property size";
      score = 72;
    } else if (ratio <= 0.85) {
      level = "Tight vs total keys — overflow/partial block more likely";
      score = 48;
    } else {
      level = "Peak exceeds realistic single-hotel capture";
      score = 28;
    }
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score,
      level,
      rationale: `${hotel.name} lists ~${hotel.rooms} guestrooms; evidenced peak ~${peakN}. ${level}. Total keys ≠ available group inventory.`,
      inputFacts: [`peakRooms=${peakN}`, `totalGuestrooms=${hotel.rooms}`, `ratio=${ratio.toFixed(2)}`],
      claimKind: "MIXED",
    });
  }

  if (hotel.rooms != null) {
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "Product plausible; peak rooms unknown",
      rationale: `${hotel.name} (~${hotel.rooms} guestrooms) can host group lodging in principle, but peak room demand is not published — do not invent a block size. Overall score may still compute from other assessed dimensions.`,
      inputFacts: [`totalGuestrooms=${hotel.rooms}`, "peakRooms=unknown"],
      claimKind: "INFERENCE",
    });
  }

  return dimBase({
    status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
    scorable: false,
    level: "Insufficient product evidence",
    rationale: "Property capacity not configured; physical fit not scored.",
  });
}

function assessGeography(geoInfo, hotel, profile) {
  if (geoInfo.class === "OUTSIDE_REALISTIC_TERRITORY") {
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: 15,
      level: "Outside realistic demand territory",
      rationale: `Geography (${geoInfo.geo || "unconfirmed"}) reads outside ${hotel.name}'s configured territory (${hotel.territory || hotel.city || "see hotel config"}).`,
      claimKind: "FACT",
    });
  }
  if (geoInfo.class === "TERRITORY_STRETCH") {
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: 38,
      level: "Stretch geography / travel practicality",
      rationale: `Footprint (${geoInfo.geo || "see evidence"}) is a stretch vs ${hotel.name} (${hotel.city || hotel.territory}). Not automatic primary host.`,
      claimKind: "MIXED",
    });
  }
  if (geoInfo.class === "CORE_TERRITORY" || geoInfo.class === "TERRITORY_COMPETITIVE") {
    const core = geoInfo.class === "CORE_TERRITORY";
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: core ? 86 : 68,
      level: core ? "In-territory" : "Competitive metro geography",
      rationale: `${profile === USE_CASE_PROFILE.STANDING_CORPORATE ? "Local presence / catchment" : "Occasion geography"} (${geoInfo.geo || "metro"}) is ${core ? "inside" : "competitive within"} ${hotel.name}'s demand territory.`,
      claimKind: "MIXED",
    });
  }
  return dimBase({
    status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
    scorable: false,
    level: "Geography not fully pinned",
    rationale: `Location vs ${hotel.name} is not fully evidenced — confirm footprint before primary-host pursuit.`,
  });
}

function assessTiming(opp, sourcing, sourcingUnconfirmed, profile) {
  const timingRaw = text(
    opp.whyNow || opp.summaryWhyNow || opp.eventDateDisplay || opp.eventStartDate
  );

  if (profile === USE_CASE_PROFILE.STANDING_CORPORATE) {
    // Standing account — no event date required; assess relationship timing from growth/local evidence
    const growth = text(opp.datedGrowthEvidence || opp.growthEvidenceDate || "");
    if (growth || /expand|relocat|hiring|new office|opened/i.test(blob(opp))) {
      return dimBase({
        status: DIMENSION_STATUS.ASSESSED,
        scorable: true,
        score: 58,
        level: "Relationship timing supported; no dated occasion required",
        rationale:
          "Standing corporate account with dated growth/local presence evidence. Timing/winnability here means relationship accessibility — not a probability of winning a specific RFP. No fabricated event date.",
        inputFacts: growth ? [`growthEvidence=${growth.slice(0, 120)}`] : [],
        inferences: ["Ongoing account outreach window — not Pursue-Now event urgency"],
        claimKind: "INFERENCE",
        limitations:
          "Does not imply a win probability. Separate from booking-window Pursuit labels.",
      });
    }
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "No dated growth or outreach trigger evidenced",
      rationale:
        "Standing account without dated expansion/outreach trigger — timing not scored. Do not invent urgency.",
      limitations: "Timing/winnability is not a win-probability estimate.",
    });
  }

  if (profile === USE_CASE_PROFILE.LOCAL_MEETINGS_CATERING) {
    if (timingRaw || /annual|awards|gala|recurring|wednesday|monthly/i.test(blob(opp))) {
      return dimBase({
        status: DIMENSION_STATUS.ASSESSED,
        scorable: true,
        score: sourcingUnconfirmed ? 64 : 55,
        level: "Function timing / recurrence evidenced",
        rationale:
          "Local function timing or recurrence is evidenced. Confirm whether this hotel's banquet product is still under consideration. Not a win probability.",
        claimKind: "MIXED",
        limitations: "Timing/winnability ≠ probability of winning.",
      });
    }
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "Function timing not established",
      rationale: "No verified function date or recurrence for timing assessment.",
    });
  }

  if (profile === USE_CASE_PROFILE.VENUE_REFERRAL) {
    return dimBase({
      status: DIMENSION_STATUS.NOT_APPLICABLE,
      scorable: false,
      level: "Not applicable — standing venue relationship",
      rationale:
        "Venue referral partnerships are ongoing relationship opportunities. Event-date winnability does not apply unless a specific overflow occasion is evidenced.",
      claimKind: "FACT",
    });
  }

  // Dated group
  if (sourcingUnconfirmed && timingRaw) {
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: 62,
      level: "Window to ask before host lock",
      rationale:
        "Dates/city public while hotel selection unconfirmed — practical outreach window. Not proof of open buying or win probability.",
      claimKind: "INFERENCE",
      limitations: "Timing/winnability ≠ probability of winning.",
    });
  }
  if (/RFP_ACTIVE|OPEN_UNRESOLVED/i.test(sourcing)) {
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: 78,
      level: "Active / open sourcing signal",
      rationale: `Venue sourcing status indicates active or unresolved sourcing (${sourcing}).`,
      claimKind: "FACT",
      limitations: "Does not alone justify Pursue Now without hotel-specific openness.",
    });
  }
  if (timingRaw) {
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "Timing known; openness unknown",
      rationale: `Timing evidence: ${timingRaw.slice(0, 180)}. Confirm whether lodging contracting is still open.`,
      claimKind: "MIXED",
    });
  }
  return dimBase({
    status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
    scorable: false,
    level: "Timing not established",
    rationale: "No verified occasion dates for timing assessment.",
  });
}

function assessCommercial(opp, peakN, profile, hotel) {
  if (profile === USE_CASE_PROFILE.LOCAL_MEETINGS_CATERING) {
    if (/gala|awards|fundraiser|banquet|chapter|annual dinner/i.test(blob(opp))) {
      return dimBase({
        status: DIMENSION_STATUS.ASSESSED,
        scorable: true,
        score: 66,
        level: "Credible catering/function commercial thesis",
        rationale:
          "Named local function/occasion activity supports a catering conversation. No invented F&B spend or attendance.",
        claimKind: "MIXED",
      });
    }
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "Commercial size not quantified",
      rationale: "Function size/spend not published — commercial potential remains unquantified.",
    });
  }

  if (profile === USE_CASE_PROFILE.STANDING_CORPORATE) {
    const local = text(opp.localConnection || "");
    const growth = text(opp.datedGrowthEvidence || "");
    const use = text(opp.proposedHotelUse || opp.hotelUseCase || "");
    if (local && (growth || use)) {
      return dimBase({
        status: DIMENSION_STATUS.ASSESSED,
        scorable: true,
        score: 64,
        level: "Plausible standing-account commercial use",
        rationale: `Verified local presence${growth ? " with dated growth evidence" : ""} supports a commercial lodging/meetings relationship thesis. No invented room nights or budgets.`,
        inputFacts: [
          local ? `localPresence=evidenced` : null,
          growth ? `growth=evidenced` : null,
        ].filter(Boolean),
        claimKind: "INFERENCE",
      });
    }
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "Commercial thesis incomplete",
      rationale: "Need verified local presence plus growth or supported hotel-use hypothesis.",
    });
  }

  if (profile === USE_CASE_PROFILE.VENUE_REFERRAL) {
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: hotel.rooms != null ? 60 : 50,
      level: "Referral relationship commercial potential",
      rationale:
        "Venue partnership commercial value depends on evidenced traveling guests or referral flow — not venue marketing alone. Score reflects relationship potential, not confirmed overflow.",
      claimKind: "INFERENCE",
    });
  }

  if (peakN != null && peakN >= 40) {
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: 70,
      level: "Material peak room potential",
      rationale: `Published/estimated peak ~${peakN} supports a commercially material conversation if the hotel can participate.`,
      claimKind: "MIXED",
    });
  }
  if (/VERIFIED_HOUSING_PROGRAM/i.test(String(opp.roomDemandStatus || ""))) {
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "Housing program exists; peak unknown",
      rationale:
        "Verified housing program does not quantify peak rooms or confirm open inclusion for this hotel.",
      claimKind: "FACT",
    });
  }
  return dimBase({
    status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
    scorable: false,
    level: "Commercial size not quantified",
    rationale: "Peak rooms / room nights not published — commercial potential not scored.",
  });
}

function assessHistorical(opp, profile, full) {
  if (profile === USE_CASE_PROFILE.STANDING_CORPORATE) {
    return dimBase({
      status: DIMENSION_STATUS.NOT_APPLICABLE,
      scorable: false,
      level: "Not applicable — no prior host pattern required",
      rationale:
        "Standing corporate accounts do not require historical hotel/brand host pattern. Unknown history is not a failure.",
      claimKind: "FACT",
    });
  }
  // LMC: prior banquet/gala at THIS hotel is historical evidence (not N/A).
  // Missing history → insufficient_evidence (applicable but unknown), never N/A solely for absence.
  if (profile === USE_CASE_PROFILE.LOCAL_MEETINGS_CATERING) {
    if (
      /held .{0,80}at the mayflower|selected mayflower|mayflower for .{0,40}gala|at the mayflower hotel|proven mayflower banquet|already selected mayflower/i.test(
        full
      ) ||
      (/held .{0,80}at .{0,40}(wyndham|hotel)/i.test(full) &&
        /gala|banquet|awards/i.test(full))
    ) {
      return dimBase({
        status: DIMENSION_STATUS.ASSESSED,
        scorable: true,
        score: 88,
        level: "Prior function hosted at this hotel",
        rationale:
          "Sourced prior banquet/gala or function activity at this hotel supports historical fit for ongoing local catering. Reconfirm current planner preference.",
        claimKind: "FACT",
        inputFacts: [
          "Public/local-connection evidence names this hotel as a prior function host.",
        ],
      });
    }
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "Prior hotel function history not yet established",
      rationale:
        "Local catering can benefit from prior banquet history at this hotel, but no sourced prior host evidence was found this pass. Applicable but unknown — not N/A, and not a failure score.",
      claimKind: "UNKNOWN",
    });
  }
  if (profile === USE_CASE_PROFILE.VENUE_REFERRAL) {
    // "Not named" on nearby lists ≠ partnership vacancy; history remains unknown until verified.
    if (/preferred|partner|overflow|referral|not named/i.test(full)) {
      return dimBase({
        status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
        scorable: false,
        level: "Partnership history not verified",
        rationale:
          "Do not invent preferred-partner vacancies from an unnamed nearby-hotel list. Confirm any prior referral relationship before scoring history.",
      });
    }
    return dimBase({
      status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
      scorable: false,
      level: "Prior partnership history not researched",
      rationale:
        "New venue referral discussions can start without prior history, but historicalFit stays applicable-unknown until researched — not scored as N/A solely for absence.",
      claimKind: "UNKNOWN",
    });
  }
  if (/crystal\s*gateway|arlington\s*host|historically\s*arlington/i.test(full)) {
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: 32,
      level: "Host pattern favors another corridor",
      rationale:
        "Historical host pattern points elsewhere. This hotel would need explicit preference or overflow need.",
      claimKind: "FACT",
    });
  }
  if (/official (hotel|housing|block)|named host|primary host/i.test(full)) {
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: 55,
      level: "Prior host / housing pattern evidenced",
      rationale: "Prior host or official housing pattern is evidenced; use as constraint, not automatic win.",
      claimKind: "MIXED",
    });
  }
  return dimBase({
    status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
    scorable: false,
    level: "Limited host-history evidence",
    rationale: "Prior venue/hotel pattern not exhaustively assessed. Unknown history ≠ failure score.",
  });
}

function assessCompetitive(opp, sourcingUnconfirmed, full, profile) {
  if (/crystal\s*gateway|grand\s*hyatt|omni\s*shoreham|hilton\s*national\s*mall/i.test(full)) {
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: 35,
      level: "Named competitor / primary likely",
      rationale:
        "A primary or patterned host may already be favored — pitch overflow/preferred only unless selection is confirmed open.",
      claimKind: "MIXED",
    });
  }
  if (profile === USE_CASE_PROFILE.VENUE_REFERRAL) {
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: 55,
      level: "Relationship accessibility to assess",
      rationale:
        "Assess complementary vs competing vs unsuitable venue relationship. Nearby venue marketing ≠ confirmed traveling guests.",
      claimKind: "INFERENCE",
    });
  }
  if (sourcingUnconfirmed || profile === USE_CASE_PROFILE.STANDING_CORPORATE) {
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: profile === USE_CASE_PROFILE.STANDING_CORPORATE ? 58 : 50,
      level:
        profile === USE_CASE_PROFILE.STANDING_CORPORATE
          ? "Account access open to establish"
          : "Competition unresolved",
      rationale:
        profile === USE_CASE_PROFILE.STANDING_CORPORATE
          ? "No exclusive hotel partner assumed for this standing account — relationship access is open to earn."
          : "Host hotel unpublished — competitive set not locked; do not assume exclusive access.",
      claimKind: "INFERENCE",
    });
  }
  return dimBase({
    status: DIMENSION_STATUS.INSUFFICIENT_EVIDENCE,
    scorable: false,
    level: "Competitive position not established",
    rationale: "Competing hotels and prior hosts not exhaustively assessed this pass.",
  });
}

function assessContactability(opp) {
  if (hasUsableContact(opp)) {
    const named = text(opp.primaryContact?.name || opp.primaryContactName);
    return dimBase({
      status: DIMENSION_STATUS.ASSESSED,
      scorable: true,
      score: named ? 82 : 72,
      level: named ? "Named contact + usable route" : "Usable organization/functional route",
      rationale: named
        ? `Suggested contact ${named} with a usable route. Contactability ≠ confirmed purchasing authority.`
        : "A published functional/organization route exists. Generic inbox ≠ confirmed buying authority.",
      claimKind: "FACT",
      limitations: "Reachable route is not booking decision-maker proof.",
    });
  }
  return dimBase({
    status: DIMENSION_STATUS.ASSESSED,
    scorable: true,
    score: 22,
    level: "Contact route incomplete",
    rationale:
      "No usable person/email/phone/URL route yet — enrich contact before treating as sales-ready. Distinct from product fit.",
    claimKind: "FACT",
  });
}

/**
 * Compute overall from assessed applicable dimensions only (renormalized weights).
 * Never fills missing dims with 50.
 */
export function computeOverallFromAssessedDimensions(
  dimensions = {},
  weights = DEFAULT_HOTEL_FIT_WEIGHTS
) {
  const assessed = {};
  let weightSum = 0;
  let count = 0;
  let insufficientWeight = 0;
  const insufficientKeys = [];
  const naKeys = [];
  for (const [key, weight] of Object.entries(weights)) {
    const d = dimensions[key];
    if (!d) continue;
    if (d.status === DIMENSION_STATUS.NOT_APPLICABLE) {
      naKeys.push(key);
      continue;
    }
    if (d.status === DIMENSION_STATUS.INSUFFICIENT_EVIDENCE) {
      insufficientWeight += weight;
      insufficientKeys.push(key);
      continue;
    }
    if (d.status !== DIMENSION_STATUS.ASSESSED || d.score == null) continue;
    assessed[key] = d.score;
    weightSum += weight;
    count += 1;
  }

  // Material gaps: physical / geography / commercial insufficient must not
  // silently disappear into a confident overall. N/A may legitimately exclude.
  const materialGap = ["physicalFit", "geographyFit", "commercialValue"].some(
    (k) => insufficientKeys.includes(k)
  );
  const provisional =
    insufficientWeight >= 0.15 ||
    materialGap ||
    insufficientKeys.length >= 2;

  if (count < MIN_OVERALL_ASSESSED_COUNT || weightSum < MIN_OVERALL_WEIGHT_SHARE) {
    return {
      hotelFitScore: null,
      components: null,
      numericScoreEmitted: false,
      provisional: true,
      calculationBasis: {
        assessedCount: count,
        assessedWeightShare: Number(weightSum.toFixed(3)),
        insufficientWeightShare: Number(insufficientWeight.toFixed(3)),
        insufficientKeys,
        notApplicableKeys: naKeys,
        minCount: MIN_OVERALL_ASSESSED_COUNT,
        minWeightShare: MIN_OVERALL_WEIGHT_SHARE,
        provisional: true,
        reason:
          "Overall numeric withheld — insufficient assessed applicable dimensions (no invented defaults).",
        note: "N/A dimensions excluded from weights. Insufficient-evidence dimensions keep the overall provisional/withheld — they do not silently inflate the score.",
        renormalized: false,
      },
      scoringVersion: null,
    };
  }

  // Renormalize weights over assessed applicable dims only.
  // Compute here (do not call computeHotelFitScore) — that helper zero-fills
  // missing dims via clampScore, which would treat N/A / gaps as failure.
  const renormalized = {};
  let weighted = 0;
  for (const key of Object.keys(assessed)) {
    renormalized[key] = weights[key] / weightSum;
    weighted += assessed[key] * renormalized[key];
  }
  const hotelFitScore = Math.max(0, Math.min(100, Math.round(weighted)));
  return {
    hotelFitScore,
    components: { ...assessed },
    numericScoreEmitted: true,
    provisional,
    calculationBasis: {
      assessedCount: count,
      assessedWeightShare: Number(weightSum.toFixed(3)),
      insufficientWeightShare: Number(insufficientWeight.toFixed(3)),
      insufficientKeys,
      notApplicableKeys: naKeys,
      assessedKeys: Object.keys(assessed),
      renormalizedWeights: renormalized,
      originalWeights: { ...weights },
      provisional,
      note: provisional
        ? "Weights renormalized over assessed applicable dimensions. Overall is PROVISIONAL because one or more applicable dimensions lack evidence — gaps were not zero-filled and must not be read as confirmed strength."
        : "Weights renormalized over assessed applicable dimensions only. N/A excluded; no insufficient-evidence dimensions in this assessment.",
      renormalized: true,
    },
    scoringVersion: provisional
      ? "gdi-hotel-fit-v2-renormalized-provisional"
      : "gdi-hotel-fit-v2-renormalized",
  };
}

function deriveEvidenceConfidence(opp = {}) {
  if (opp.evidenceConfidence != null && Number.isFinite(Number(opp.evidenceConfidence))) {
    return {
      evidenceConfidence: Number(opp.evidenceConfidence),
      evidenceConfidenceExplanation:
        opp.evidenceConfidenceExplanation ||
        buildEvidenceConfidenceExplanation({
          evidenceConfidence: opp.evidenceConfidence,
          evidence: opp.evidence,
          evidenceConfidenceExplanation: opp.evidenceConfidenceExplanation,
        }),
      basis: "existing_persisted",
    };
  }
  const evidence = Array.isArray(opp.evidence) ? opp.evidence : [];
  const sources = []
    .concat(opp.sourceUrls || [])
    .concat(opp.officialSources || [])
    .filter(Boolean);
  const verified = evidence.filter(
    (e) => e.claimKind === "FACT" || e.status === "VERIFIED"
  ).length;
  const inferred = evidence.filter((e) => e.claimKind === "INFERENCE").length;
  const sourceCount = Math.max(sources.length, evidence.length, 0);
  const computed = computeEvidenceConfidence({
    sourceAuthority: sourceCount >= 2 ? 70 : sourceCount === 1 ? 55 : 35,
    independentSourceCount: Math.min(5, sourceCount),
    directness: text(opp.officialSource || opp.officialDomain) ? 70 : 45,
    recency: opp.growthEvidenceDate || opp.eventStartDate || opp.verificationDate ? 65 : 45,
    verifiedFieldRatio: evidence.length
      ? verified / Math.max(1, evidence.length)
      : sourceCount >= 1
        ? 0.4
        : 0.15,
    firstPartyShare: text(opp.officialDomain) ? 60 : 35,
    completeness: text(opp.hotelOpportunityThesis || opp.fitExplanation) ? 55 : 35,
    conflictPenalty: 0,
  });
  return {
    evidenceConfidence: computed.evidenceConfidence,
    evidenceConfidenceExplanation: buildEvidenceConfidenceExplanation({
      evidenceConfidence: computed.evidenceConfidence,
      evidence,
      forceRebuild: true,
    }),
    factors: computed.factors,
    basis: "computed_from_available_evidence",
    version: computed.version,
  };
}

function deriveSalesPosture(assessment, opp) {
  // Preserve existing justified qualification when present
  if (opp.opportunityQualification) {
    return {
      opportunityQualification: opp.opportunityQualification,
      opportunityQualificationLabel: opp.opportunityQualificationLabel || null,
      provenance: "preserved_existing",
    };
  }
  const posture = assessment.posture;
  if (posture === "EXPLORATORY_STRETCH") {
    return {
      opportunityQualification: OPPORTUNITY_QUALIFICATION.WEAK,
      opportunityQualificationLabel: "Weak — exploratory / stretch",
      provenance: "derived_from_hotel_fit_posture",
    };
  }
  if (posture === "PRIORITY_CANDIDATE") {
    return {
      opportunityQualification: OPPORTUNITY_QUALIFICATION.STRONG,
      opportunityQualificationLabel: "Strong",
      provenance: "derived_from_hotel_fit_posture",
    };
  }
  return {
    opportunityQualification: OPPORTUNITY_QUALIFICATION.MODERATE,
    opportunityQualificationLabel: "Moderate — qualify further",
    provenance: "derived_from_hotel_fit_posture",
  };
}

function deriveSalesAction(assessment, opp) {
  if (opp.bookingWindowStatus) {
    return {
      bookingWindowStatus: opp.bookingWindowStatus,
      bookingWindowLabel: opp.bookingWindowLabel || null,
      recommendedAction: opp.recommendedAction || opp.nextSalesAction || null,
      provenance: "preserved_existing",
    };
  }
  const timing = assessment.dimensions?.timing;
  const profile = assessment.useCaseProfile;
  if (profile === USE_CASE_PROFILE.STANDING_CORPORATE) {
    return {
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      bookingWindowLabel: "QUALIFY NOW",
      recommendedAction:
        opp.recommendedAction ||
        "Introduce the hotel using the published contact route; ask who coordinates visiting colleagues and workshops. Do not invent event urgency.",
      provenance: "derived_standing_corporate",
    };
  }
  if (timing?.status === DIMENSION_STATUS.ASSESSED && timing.score >= 70) {
    return {
      bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
      bookingWindowLabel: "CONTACT NOW",
      recommendedAction:
        opp.recommendedAction ||
        "Confirm whether lodging/meeting selection remains open before intensifying pursuit.",
      provenance: "derived_from_timing_dimension",
    };
  }
  if (timing?.status === DIMENSION_STATUS.NOT_APPLICABLE) {
    return {
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      bookingWindowLabel: "QUALIFY NOW",
      recommendedAction:
        opp.recommendedAction ||
        "Open a venue-referral / partnership conversation; do not invent overflow demand.",
      provenance: "derived_venue_referral",
    };
  }
  return {
    bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
    bookingWindowLabel: "QUALIFY NOW",
    recommendedAction:
      opp.recommendedAction ||
      "Confirm timing openness and buying path before treating as Contact Now.",
    provenance: "derived_default_qualify",
  };
}

/**
 * Build dimension assessments + optional numeric overall + evidence/posture/action.
 */
export function assessHotelFitForOpportunity(opportunity = {}, opts = {}) {
  const hotelId = String(opts.hotelId || opportunity.hotelId || "").trim();
  const cfg = opts.hotelConfig || loadHotelConfig(hotelId) || {};
  const hotel = hotelBits(cfg);
  const profile = opts.useCaseProfile || detectUseCaseProfile(opportunity);
  const full = blob(opportunity);
  const geoInfo = classifyGeography(opportunity, hotel, cfg, profile);
  const peak =
    opportunity.peakRooms ??
    opportunity.estimatedPeakRooms ??
    opportunity.publishedPeakRooms ??
    null;
  const peakN = peak != null && Number.isFinite(Number(peak)) ? Number(peak) : null;
  const sourcing = String(
    opportunity.venueSourcingStatus || opportunity.venueSourcingStatusLabel || ""
  ).toUpperCase();
  const sourcingUnconfirmed =
    /UNCONFIRM|UNKNOWN|TBA|NOT\s*NAMED|NOT\s*PUBLICLY/i.test(sourcing) ||
    /hotel not (yet )?named|hotel\/host is not confirmed|sourcing unconfirmed/i.test(full);

  const dimensions = {
    physicalFit: assessPhysical(opportunity, hotel, profile),
    geographyFit: assessGeography(geoInfo, hotel, profile),
    timing: assessTiming(opportunity, sourcing, sourcingUnconfirmed, profile),
    commercialValue: assessCommercial(opportunity, peakN, profile, hotel),
    historicalFit: assessHistorical(opportunity, profile, full),
    competitiveAccessibility: assessCompetitive(
      opportunity,
      sourcingUnconfirmed,
      full,
      profile
    ),
    contactability: assessContactability(opportunity),
  };

  const overall = computeOverallFromAssessedDimensions(dimensions);

  const stretch =
    geoInfo.class === "TERRITORY_STRETCH" ||
    (dimensions.geographyFit?.score != null && dimensions.geographyFit.score < 45);
  const noAdvantage =
    stretch ||
    (dimensions.historicalFit?.status === DIMENSION_STATUS.ASSESSED &&
      dimensions.historicalFit.score != null &&
      dimensions.historicalFit.score < 40);

  const isProvisional = overall.provisional === true || noAdvantage;

  let summary;
  if (noAdvantage) {
    summary = `No supported primary-host advantage for ${hotel.name} versus the occasion geography/host pattern (${geoInfo.geo || "see evidence"}). Exploratory / stretch posture — pursue only if planners signal openness to this location or an overflow need appears.${
      overall.numericScoreEmitted
        ? ` Overall fit ${overall.hotelFitScore} is provisional while physical/commercial/geography evidence gaps remain.`
        : ""
    }`;
  } else if (overall.numericScoreEmitted && isProvisional) {
    summary = `Provisional hotel-fit score ${overall.hotelFitScore} from ${overall.calculationBasis.assessedCount} assessed dimensions (weights renormalized). Applicable dimensions still lacking evidence: ${(overall.calculationBasis.insufficientKeys || []).join(", ") || "see dimension tiles"}. Gaps were not scored as zero and must not be read as confirmed strength.`;
  } else if (overall.numericScoreEmitted) {
    summary = `Hotel-fit score ${overall.hotelFitScore} (${overall.scoringVersion}) from ${overall.calculationBasis.assessedCount} assessed applicable dimensions (weights renormalized). Re-check when evidence changes.`;
  } else {
    summary = `Dimension-level hotel-fit assessment for ${hotel.name} (${profile}). Overall numeric provisional/withheld: ${overall.calculationBasis.reason}`;
  }

  // Posture is a sales label derived from fit/geography — not independent analysis.
  const posture = noAdvantage
    ? "EXPLORATORY_STRETCH"
    : overall.numericScoreEmitted && !isProvisional && overall.hotelFitScore >= 72
      ? "PRIORITY_CANDIDATE"
      : "QUALIFY_FURTHER";

  const overallStatus = !overall.numericScoreEmitted
    ? DIMENSION_STATUS.INSUFFICIENT_EVIDENCE
    : isProvisional
      ? "provisional"
      : DIMENSION_STATUS.ASSESSED;

  const assessment = {
    version: HOTEL_FIT_ASSESSMENT_VERSION,
    scoringVersion: overall.scoringVersion || null,
    dimensionRuleVersion: HOTEL_FIT_DIMENSION_RULE_VERSION,
    hotelId,
    hotelName: hotel.name,
    useCaseProfile: profile,
    assessedAt: new Date().toISOString(),
    posture,
    postureNote:
      "Sales posture is derived from hotel-fit and territory evidence — it is not a separate win-probability analysis.",
    summary: text(summary),
    occasionGeography: geoInfo.geo,
    territoryClass: geoInfo.class,
    dimensions,
    componentLabels: { ...HOTEL_FIT_COMPONENT_LABELS },
    weights: { ...DEFAULT_HOTEL_FIT_WEIGHTS },
    hotelFitScore: overall.hotelFitScore,
    components: overall.components,
    numericScoreEmitted: overall.numericScoreEmitted,
    provisional: isProvisional,
    overallStatus,
    calculationBasis: overall.calculationBasis,
    inputs: {
      peakRooms: peakN,
      totalGuestrooms: hotel.rooms,
      meetingRooms: hotel.meetingRooms,
      meetingSqFt: hotel.meetingSqFt,
      venueSourcingStatus: sourcing || null,
      hasUsableContact: hasUsableContact(opportunity),
      occasionGeography: geoInfo.geo,
      territoryClass: geoInfo.class,
      useCaseProfile: profile,
    },
  };

  assessment.evidence = deriveEvidenceConfidence(opportunity);
  assessment.salesPosture = deriveSalesPosture(assessment, opportunity);
  assessment.salesAction = deriveSalesAction(assessment, opportunity);

  return assessment;
}

/**
 * Shared assessment contract: decide whether a structured hotel-fit assessment
 * authorizes a numeric overall score.
 *
 * Precedence:
 * 1. Explicit withheld / unscored / insufficient-evidence → null (stale numbers ignored)
 * 2. numericScoreEmitted === true with finite hotelFitScore → accept (incl. provisional)
 * 3. scored === true with finite numericScore|hotelFitScore → accept (legacy package shape)
 * 4. scoringVersion alone does NOT authorize a numeric score
 *
 * Legitimate zero is accepted when emitted/scored. Missing dimensions stay null.
 */
export function resolveStructuredHotelFitNumericScore(assessment = null) {
  if (!assessment || typeof assessment !== "object") {
    return {
      accept: false,
      score: null,
      scoringVersion: null,
      provisional: false,
      reason: "no_assessment",
    };
  }

  const basis = assessment.calculationBasis || {};
  const status = String(assessment.overallStatus || "").toLowerCase();
  const rationale = String(
    assessment.summary || basis.reason || assessment.hotelFitRationale || ""
  );
  const withheldExplicit =
    assessment.numericScoreEmitted === false ||
    assessment.scored === false ||
    status === "insufficient_evidence" ||
    /overall numeric withheld|withheld — insufficient|not scored — evidence/i.test(
      rationale
    ) ||
    /overall numeric withheld/i.test(String(basis.reason || ""));

  if (withheldExplicit) {
    return {
      accept: false,
      score: null,
      scoringVersion: null,
      provisional: true,
      reason: "withheld_or_insufficient",
    };
  }

  const provisional = assessment.provisional === true || basis.provisional === true;

  if (assessment.numericScoreEmitted === true && assessment.hotelFitScore != null) {
    const n = Number(assessment.hotelFitScore);
    if (Number.isFinite(n)) {
      return {
        accept: true,
        score: n,
        scoringVersion: assessment.scoringVersion || null,
        provisional,
        reason: provisional ? "numeric_emitted_provisional" : "numeric_emitted",
      };
    }
  }

  if (assessment.scored === true) {
    const raw =
      assessment.numericScore != null
        ? assessment.numericScore
        : assessment.hotelFitScore;
    if (raw != null) {
      const n = Number(raw);
      if (Number.isFinite(n)) {
        return {
          accept: true,
          score: n,
          scoringVersion: assessment.scoringVersion || null,
          provisional,
          reason: provisional ? "scored_true_provisional" : "scored_true",
        };
      }
    }
  }

  // Version identifies method only — never authorizes a stale flat score by itself.
  if (assessment.scoringVersion || assessment.version) {
    return {
      accept: false,
      score: null,
      scoringVersion: null,
      provisional: false,
      reason: "version_without_numeric_emit",
    };
  }

  return {
    accept: false,
    score: null,
    scoringVersion: null,
    provisional: false,
    reason: "unscored",
  };
}

/**
 * Apply assessment onto opportunity fields used by DTO / drawer / Airtable payload.
 * Writes flat *Score fields when assessed so reconstructScoreAudit + Bethesda UI work.
 */
export function applyHotelFitAssessmentToOpportunity(opportunity = {}, assessment = null) {
  const a = assessment || assessHotelFitForOpportunity(opportunity);
  const next = { ...opportunity };
  next.hotelFitAssessment = a;
  next.hotelFitAssessmentVersion = a.version;
  next.hotelFitPosture = a.posture;
  next.hotelFitPostureNote = a.postureNote || null;
  next.hotelFitUseCaseProfile = a.useCaseProfile;
  next.hotelFitCalculationBasis = a.calculationBasis;
  next.hotelFitOverallStatus = a.overallStatus;
  next.hotelFitProvisional = a.provisional === true;

  // Canonical per-dimension persistence (status + score + rationale)
  next.hotelFitDimensionAssessments = Object.fromEntries(
    Object.entries(a.dimensions || {}).map(([k, d]) => [
      k,
      {
        status: d.status,
        score: d.score,
        level: d.level,
        rationale: d.rationale,
        evidenceRefs: d.evidenceRefs || [],
        inputFacts: d.inputFacts || [],
        inferences: d.inferences || [],
        claimKind: d.claimKind,
        assessmentDate: d.assessmentDate,
        ruleVersion: d.ruleVersion,
        limitations: d.limitations || null,
      },
    ])
  );

  const dimLines = Object.entries(a.dimensions || {}).map(([key, d]) => {
    const label = a.componentLabels?.[key] || key;
    const statusNote =
      d.status === DIMENSION_STATUS.ASSESSED
        ? d.score != null
          ? `${d.score}`
          : d.level
        : d.status;
    return `${label}: ${statusNote} — ${d.level}. ${d.rationale}`;
  });
  next.hotelFitDimensionNotes = dimLines;
  next.hotelFitComponentLabels = a.componentLabels;

  // Flat scores for audit/UI — only when assessed; never invent
  const flatMap = {
    physicalFit: "physicalFitScore",
    geographyFit: "geographyFitScore",
    timing: "timingScore",
    commercialValue: "commercialValueScore",
    historicalFit: "historicalFitScore",
    competitiveAccessibility: "competitiveAccessibilityScore",
    contactability: "contactabilityScore",
  };
  const forceReassess = opportunity._reassessHotelFit === true;
  for (const [dim, field] of Object.entries(flatMap)) {
    const d = a.dimensions[dim];
    if (d?.status === DIMENSION_STATUS.ASSESSED && d.score != null) {
      next[field] = d.score;
    } else if (
      d?.status === DIMENSION_STATUS.NOT_APPLICABLE ||
      (forceReassess && d?.status === DIMENSION_STATUS.INSUFFICIENT_EVIDENCE)
    ) {
      // Clear stale legacy factory scores so N/A / gaps are not shown as fake numbers
      next[field] = null;
    }
  }

  const resolved = resolveStructuredHotelFitNumericScore(a);
  if (resolved.accept) {
    next.hotelFitScore = resolved.score;
    next.fitComponents = a.components;
    next.hotelFitScoreBasis = resolved.scoringVersion;
    next.hotelFitScoreNote = `${a.summary} Basis: assessed ${a.calculationBasis?.assessedCount} dims, weight share ${a.calculationBasis?.assessedWeightShare}, renormalized=${a.calculationBasis?.renormalized}.`;
    next.hotelFitRationale = a.summary;
  } else {
    next.hotelFitScore = null;
    next.hotelFitScoreBasis = null;
    next.hotelFitScoreNote = a.summary;
    next.hotelFitRationale = [a.summary, ...dimLines.slice(0, 4)].join(" ");
  }

  // Qualitative map always available for drawer (includes status)
  next.hotelFitQualitativeComponents = Object.fromEntries(
    Object.entries(a.dimensions).map(([k, d]) => [
      k,
      {
        status: d.status,
        level: d.level,
        rationale: d.rationale,
        claimKind: d.claimKind,
        score: d.score,
      },
    ])
  );

  // Evidence confidence
  if (a.evidence?.evidenceConfidence != null) {
    next.evidenceConfidence = a.evidence.evidenceConfidence;
    next.evidenceConfidenceExplanation = a.evidence.evidenceConfidenceExplanation;
    next.evidenceConfidenceBasis = a.evidence.basis;
  }

  // Sales posture / action — preserve existing when justified; else derive
  if (a.salesPosture) {
    if (
      !opportunity.opportunityQualification ||
      a.salesPosture.provenance === "preserved_existing"
    ) {
      next.opportunityQualification =
        a.salesPosture.opportunityQualification || next.opportunityQualification;
      next.opportunityQualificationLabel =
        a.salesPosture.opportunityQualificationLabel ||
        next.opportunityQualificationLabel;
    } else if (!opportunity.opportunityQualification) {
      next.opportunityQualification = a.salesPosture.opportunityQualification;
      next.opportunityQualificationLabel =
        a.salesPosture.opportunityQualificationLabel;
    }
  }
  if (a.salesAction) {
    if (!opportunity.bookingWindowStatus) {
      next.bookingWindowStatus = a.salesAction.bookingWindowStatus;
      next.bookingWindowLabel = a.salesAction.bookingWindowLabel;
    }
    if (!opportunity.recommendedAction && a.salesAction.recommendedAction) {
      next.recommendedAction = a.salesAction.recommendedAction;
    }
  }

  if (a.posture === "EXPLORATORY_STRETCH") {
    const existing = text(next.fitExplanation || next.hotelOpportunityThesis);
    if (!existing || existing.length < 40 || detectClaimKindFromProse(existing)) {
      next.fitExplanation = a.summary;
      next.fitRationale = a.summary;
      next.hotelOpportunityThesis = a.summary;
      next.summaryWhyHotel = a.summary;
    }
  } else if (!text(next.fitExplanation) && a.summary) {
    next.fitExplanation = a.summary;
    next.summaryWhyHotel = next.summaryWhyHotel || a.summary;
  }

  // Narrative ↔ assessment consistency (qualify unsupported claims; do not invent scores)
  const repaired = repairNarrativeForConsistency(next, {
    hotelName: a.hotelName,
    assessment: a,
  });
  Object.assign(next, repaired.opportunity);
  next.hotelFitNarrativeConsistency = {
    version: repaired.check?.version || null,
    ok: repaired.check?.ok ?? true,
    issueCount: repaired.check?.issueCount || 0,
    issues: repaired.check?.issues || [],
    repairs: repaired.changes || [],
    checkedAt: a.assessedAt,
  };
  // Re-check after repair for residual issues (informational)
  const residual = detectNarrativeConsistencyIssues(next, a);
  next.hotelFitNarrativeConsistency.residualIssues = residual.issues;

  // Soften PE partnershipPotential HIGH when history/geography lack evidence
  if (
    a.useCaseProfile === USE_CASE_PROFILE.VENUE_REFERRAL &&
    /HIGH/i.test(String(next.partnershipPotential || next.partnershipPotentialLabel || "")) &&
    (a.dimensions?.historicalFit?.status === DIMENSION_STATUS.INSUFFICIENT_EVIDENCE ||
      a.dimensions?.geographyFit?.status === DIMENSION_STATUS.INSUFFICIENT_EVIDENCE ||
      a.provisional)
  ) {
    next.partnershipPotential = "MODERATE";
    next.partnershipPotentialLabel =
      "To assess — not a confirmed partnership vacancy";
    next.hotelFitNarrativeConsistency.repairs.push({
      field: "partnershipPotential",
      before: "HIGH",
      after: "MODERATE",
    });
  }

  next.hotelFitAssessedAt = a.assessedAt;
  return next;
}
