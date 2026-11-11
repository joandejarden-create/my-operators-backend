/**
 * Commercial prequalification + ranking for Apify/Exa discovery candidates (V7.1).
 * Dates are one signal — not the dominant rank key.
 */

export const CANDIDATE_QUALITY_CLASS = Object.freeze({
  ACTUAL_DATED_DEMAND: "ACTUAL_DATED_DEMAND",
  PLAUSIBLE_DATED_DEMAND: "PLAUSIBLE_DATED_DEMAND",
  PLANNER_OR_RELATIONSHIP_LEAD: "PLANNER_OR_RELATIONSHIP_LEAD",
  VENUE_OR_DEMAND_GENERATOR: "VENUE_OR_DEMAND_GENERATOR",
  GENERIC_EVENT_LISTING: "GENERIC_EVENT_LISTING",
  UNRELATED_ACTIVITY: "UNRELATED_ACTIVITY",
  DUPLICATE_OR_IDENTITY_UNCERTAIN: "DUPLICATE_OR_IDENTITY_UNCERTAIN",
});

export const PREQUAL_DECISION = Object.freeze({
  ADVANCE_TO_DEEP_RESEARCH: "ADVANCE_TO_DEEP_RESEARCH",
  RELATIONSHIP_RESEARCH: "RELATIONSHIP_RESEARCH",
  HOLD_FOR_MORE_DISCOVERY: "HOLD_FOR_MORE_DISCOVERY",
  REJECT_EARLY: "REJECT_EARLY",
});

const NIGHTLIFE =
  /\b(party bus|nightclub|club euro|strip|drag queen|ladies night|dancehall|reggae|sip.?n.?paint|booze|scavenger hunt|open mic|paint and sip|yoga|sound bath|coffee club|social club)\b/i;
const WEDDING_PLANNER =
  /\b(wedding|bridal|planner|event planning|event planner|destination wedding|wedding producer)\b/i;
const BUSINESS_MEETING =
  /\b(leadership|workshop|networking|investor|conference|training|boot camp|seminar|goal setting|canva|professional|real estate investor|breakthrough session)\b/i;
const HOSPITALITY_ADJACENT =
  /\b(anniversary|gala|celebration|reunion|incentive|corporate|meeting|catering|reception)\b/i;

function blob(c) {
  return [
    c.title,
    c.organizationName,
    c.summary,
    c.category,
    c.venueName,
    c.sourceUrl,
    c.headline,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function isNeedMonth2027(startDate) {
  return /^2027-08|^2027-09|^2027-10/.test(String(startDate || ""));
}

function geoFit(c) {
  const b = `${c.city || ""} ${c.region || ""} ${c.venueName || ""} ${blob(c)}`;
  if (/boca raton/i.test(b)) return "STRONG_BOCA";
  if (/delray|deerfield|pompano|boynton|palm beach|fort lauderdale|west palm|coral springs|sunrise/i.test(b)) {
    return "SOUTH_FLORIDA";
  }
  if (/miami|aventura|hollywood|hollywood beach/i.test(b)) return "BROADER_SOUTH_FLORIDA";
  if (/bahamas/i.test(b)) return "DESTINATION_OUTSIDE_PRIMARY";
  return "WEAK_OR_UNKNOWN";
}

/**
 * Classify a normalized candidate for commercial quality audit.
 */
export function classifyDiscoveryCandidate(c = {}) {
  const b = blob(c);
  const dated = Boolean(c.startDate);
  const needMonth = isNeedMonth2027(c.startDate);
  const geo = geoFit(c);
  const reasons = [];

  if (!c.title || String(c.title).includes("[object Object]")) {
    return {
      qualityClass: CANDIDATE_QUALITY_CLASS.DUPLICATE_OR_IDENTITY_UNCERTAIN,
      reasons: ["identity_uncertain_or_object_string"],
      geo,
      needMonth,
    };
  }

  if (c.recordKind === "VENDOR_OR_VENUE_LISTING" || c.relationshipTrackOnly) {
    if (/venue|resort|ballroom|garden estate|country club/i.test(b) && !/planner|planning|events?\b/i.test(b)) {
      return {
        qualityClass: CANDIDATE_QUALITY_CLASS.VENUE_OR_DEMAND_GENERATOR,
        reasons: ["venue_or_facility_listing"],
        geo,
        needMonth: false,
      };
    }
    // Knot / vendor marketplace rows are relationship leads until proven otherwise —
    // do not require the literal word "wedding" in the title (Stage 1 failure mode).
    if (
      WEDDING_PLANNER.test(b) ||
      /event|planning|planner|collective|special day|design|produce/i.test(c.title || b)
    ) {
      return {
        qualityClass: CANDIDATE_QUALITY_CLASS.PLANNER_OR_RELATIONSHIP_LEAD,
        reasons: ["knot_or_vendor_planner_listing", "relationship_track"],
        geo,
        needMonth: false,
      };
    }
    return {
      qualityClass: CANDIDATE_QUALITY_CLASS.PLANNER_OR_RELATIONSHIP_LEAD,
      reasons: ["vendor_marketplace_relationship_default", "relationship_track"],
      geo,
      needMonth: false,
    };
  }

  if (NIGHTLIFE.test(b) || /hunk-o-mania|party bus|club euro|drag queen|ladies night/i.test(b)) {
    return {
      qualityClass: CANDIDATE_QUALITY_CLASS.UNRELATED_ACTIVITY,
      reasons: ["nightlife_leisure_transport_or_entertainment_not_hotel_group_demand"],
      geo,
      needMonth,
    };
  }

  // Exa discovery companies that already passed hospitality gate2 must not be
  // collapsed to GENERIC_EVENT_LISTING merely because the title lacks "workshop".
  if (
    c.recordKind === "EXA_DISCOVERY_CANDIDATE" ||
    c.exaGate2 === true ||
    /guestroom|room block|lodging|meeting|banquet|catering|training|conference|incentive|hotel/i.test(
      `${c.summary || ""} ${c.category || ""}`,
    )
  ) {
    if (needMonth || BUSINESS_MEETING.test(b) || HOSPITALITY_ADJACENT.test(b)) {
      return {
        qualityClass: CANDIDATE_QUALITY_CLASS.PLAUSIBLE_DATED_DEMAND,
        reasons: ["exa_or_hospitality_summary_signal", needMonth ? "need_month_overlap" : "timing_soft"],
        geo,
        needMonth,
      };
    }
    return {
      qualityClass: CANDIDATE_QUALITY_CLASS.PLANNER_OR_RELATIONSHIP_LEAD,
      reasons: ["exa_hospitality_account_without_confirmed_need_month"],
      geo,
      needMonth: false,
    };
  }

  if (
    /scavenger hunt|coffee club|social club|yoga|paint|drawing|clay|watercolor|open mic|folk dance|kindness spree|wellness walk|spiritual/i.test(
      b,
    )
  ) {
    return {
      qualityClass: CANDIDATE_QUALITY_CLASS.GENERIC_EVENT_LISTING,
      reasons: ["local_leisure_or_hobby_listing_low_room_block_thesis"],
      geo,
      needMonth,
    };
  }

  if (BUSINESS_MEETING.test(b) && needMonth) {
    return {
      qualityClass: CANDIDATE_QUALITY_CLASS.PLAUSIBLE_DATED_DEMAND,
      reasons: ["business_workshop_or_networking_in_need_months", "may_need_meeting_space_or_day_use"],
      geo,
      needMonth,
    };
  }

  if (HOSPITALITY_ADJACENT.test(b) && needMonth && geo !== "WEAK_OR_UNKNOWN") {
    return {
      qualityClass: CANDIDATE_QUALITY_CLASS.PLAUSIBLE_DATED_DEMAND,
      reasons: ["hospitality_adjacent_dated_activity"],
      geo,
      needMonth,
    };
  }

  if (dated && needMonth) {
    return {
      qualityClass: CANDIDATE_QUALITY_CLASS.GENERIC_EVENT_LISTING,
      reasons: ["dated_but_weak_hotel_revenue_thesis"],
      geo,
      needMonth,
    };
  }

  if (dated) {
    return {
      qualityClass: CANDIDATE_QUALITY_CLASS.GENERIC_EVENT_LISTING,
      reasons: ["dated_outside_or_weak"],
      geo,
      needMonth,
    };
  }

  return {
    qualityClass: CANDIDATE_QUALITY_CLASS.DUPLICATE_OR_IDENTITY_UNCERTAIN,
    reasons: ["unclassified"],
    geo,
    needMonth,
  };
}

/**
 * Six-question commercial prequalification before paid Deep Research.
 */
export function prequalifyForDeepResearch(c = {}, classification = null) {
  const cls = classification || classifyDiscoveryCandidate(c);
  const b = blob(c);
  const answers = {
    credibleDemandActivity: null,
    hotelRevenueMechanism: null,
    waterstoneGeoFit: null,
    buyerOrReferralPathway: null,
    timingOrRelationship: null,
    clearlyCommittedElsewhere: null,
    deepResearchQuestion: null,
  };

  const rejectEarly =
    cls.qualityClass === CANDIDATE_QUALITY_CLASS.UNRELATED_ACTIVITY ||
    cls.qualityClass === CANDIDATE_QUALITY_CLASS.GENERIC_EVENT_LISTING;

  if (cls.qualityClass === CANDIDATE_QUALITY_CLASS.UNRELATED_ACTIVITY) {
    answers.credibleDemandActivity = false;
    answers.hotelRevenueMechanism = "NONE_EVIDENT — nightlife/leisure transport/entertainment";
    answers.deepResearchQuestion = null;
    return {
      decision: PREQUAL_DECISION.REJECT_EARLY,
      rejectionReason: "NO_CREDIBLE_HOTEL_REVENUE_THESIS",
      qualityClass: cls.qualityClass,
      answers,
      classification: cls,
    };
  }

  if (cls.qualityClass === CANDIDATE_QUALITY_CLASS.GENERIC_EVENT_LISTING) {
    answers.credibleDemandActivity = "LOW";
    answers.hotelRevenueMechanism = "UNLIKELY_ROOM_BLOCK — local leisure/hobby";
    return {
      decision: PREQUAL_DECISION.REJECT_EARLY,
      rejectionReason: "GENERIC_LOCAL_LISTING_LOW_YIELD",
      qualityClass: cls.qualityClass,
      answers,
      classification: cls,
    };
  }

  if (cls.qualityClass === CANDIDATE_QUALITY_CLASS.PLANNER_OR_RELATIONSHIP_LEAD) {
    answers.credibleDemandActivity = true;
    answers.hotelRevenueMechanism =
      "POTENTIAL — planner referral for guestroom blocks and/or rehearsal / welcome / reception / banquet F&B (guestrooms not mandatory)";
    answers.waterstoneGeoFit = cls.geo;
    answers.buyerOrReferralPathway = "Planner business contact (email/phone on listing if present)";
    answers.timingOrRelationship = "ACCOUNT_RELATIONSHIP — not a need-month match without dated demand";
    answers.clearlyCommittedElsewhere = "UNKNOWN";
    answers.deepResearchQuestion =
      "Does this planner produce Boca/Palm Beach lodging and/or banquet-F&B demand, prefer specific hotels, and have a realistic referral pathway for Waterstone (rooms and/or events — never assume every wedding needs a room block)?";
    const strongGeo = cls.geo === "STRONG_BOCA" || cls.geo === "SOUTH_FLORIDA";
    return {
      decision: strongGeo
        ? PREQUAL_DECISION.RELATIONSHIP_RESEARCH
        : PREQUAL_DECISION.HOLD_FOR_MORE_DISCOVERY,
      rejectionReason: null,
      qualityClass: cls.qualityClass,
      answers,
      classification: cls,
    };
  }

  if (cls.qualityClass === CANDIDATE_QUALITY_CLASS.PLAUSIBLE_DATED_DEMAND) {
    answers.credibleDemandActivity = true;
    answers.hotelRevenueMechanism = BUSINESS_MEETING.test(b)
      ? "PLAUSIBLE — meeting/training day-use or small group (EVENT_FNB_ONLY unless nonlocal lodging evidenced); never assume guestrooms"
      : "PLAUSIBLE — hospitality-adjacent dated activity (classify guestrooms vs F&B independently)";
    answers.waterstoneGeoFit = cls.geo;
    answers.buyerOrReferralPathway = c.organizationName
      ? `Organizer: ${c.organizationName}`
      : "Organizer present but needs verification";
    answers.timingOrRelationship = cls.needMonth
      ? "DATE_SPECIFIC — overlaps Aug–Oct 2027 (unverified occurrence quality)"
      : "DATE_SPECIFIC — timing weak";
    answers.clearlyCommittedElsewhere = "UNKNOWN — check venue commitment";
    answers.deepResearchQuestion =
      "Who owns sourcing, are attendees nonlocal, is there a hotel room/meeting need, and is Waterstone contestable vs the listed venue?";
    return {
      decision:
        cls.geo === "STRONG_BOCA" || cls.geo === "SOUTH_FLORIDA"
          ? PREQUAL_DECISION.ADVANCE_TO_DEEP_RESEARCH
          : PREQUAL_DECISION.HOLD_FOR_MORE_DISCOVERY,
      rejectionReason: null,
      qualityClass: cls.qualityClass,
      answers,
      classification: cls,
    };
  }

  return {
    decision: PREQUAL_DECISION.HOLD_FOR_MORE_DISCOVERY,
    rejectionReason: null,
    qualityClass: cls.qualityClass,
    answers,
    classification: cls,
  };
}

/**
 * Rank candidates for Stage 2 — commercial thesis first, dates secondary.
 * Returns ordered list with explicit reason strings (no opaque scores as promotion).
 */
export function rankCandidatesForDeepResearch(candidates = []) {
  const rows = candidates.map((c) => {
    const classification = classifyDiscoveryCandidate(c);
    const prequal = prequalifyForDeepResearch(c, classification);
    const priority =
      prequal.decision === PREQUAL_DECISION.ADVANCE_TO_DEEP_RESEARCH
        ? 1
        : prequal.decision === PREQUAL_DECISION.RELATIONSHIP_RESEARCH
          ? 2
          : prequal.decision === PREQUAL_DECISION.HOLD_FOR_MORE_DISCOVERY
            ? 3
            : 9;
    return {
      candidate: c,
      classification,
      prequal,
      priority,
      rankReasons: [
        `quality=${classification.qualityClass}`,
        `decision=${prequal.decision}`,
        `geo=${classification.geo}`,
        classification.needMonth ? "need_month_overlap" : "no_need_month_claim",
        prequal.rejectionReason || "no_early_reject",
      ],
    };
  });

  rows.sort((a, b) => {
    if (a.priority !== b.priority) return a.priority - b.priority;
    // within relationship leads, prefer stronger geo + review signal
    const geoRank = (g) =>
      ({ STRONG_BOCA: 0, SOUTH_FLORIDA: 1, BROADER_SOUTH_FLORIDA: 2, DESTINATION_OUTSIDE_PRIMARY: 3 }[g] ?? 4);
    const dg = geoRank(a.classification.geo) - geoRank(b.classification.geo);
    if (dg !== 0) return dg;
    return String(a.candidate.title || "").localeCompare(String(b.candidate.title || ""));
  });

  return rows;
}

/**
 * Build adaptive Deep Research plan for a high-potential candidate.
 */
export function buildAdaptiveDeepResearchPlan(rankedRow) {
  const c = rankedRow.candidate;
  const p = rankedRow.prequal;
  return {
    candidateTitle: c.title,
    organizationName: c.organizationName,
    sourceUrl: c.sourceUrl,
    qualityClass: rankedRow.classification.qualityClass,
    primaryDemandHypothesis: p.answers.hotelRevenueMechanism,
    buyerResearchQuestions: [
      "Who is the commercial decision-maker or referral owner?",
      "What is their role relative to lodging / venue / catering sourcing?",
      "Is there a public contact pathway (not Surfe)?",
    ],
    timingQuestions: [
      "Is there verified Aug–Oct 2027 demand, or only a relationship?",
      "Are listed Eventbrite dates occurrence-accurate or series placeholders?",
    ],
    hotelSourcingQuestions: [
      "Is a hotel already committed?",
      "Is Waterstone contestable or only speculative overflow?",
    ],
    waterstoneFit: [
      "Guestroom and/or marina F&B / meeting space relevance (multi-revenue; guestrooms not mandatory)",
      "Geographic suitability for Boca Raton guests or day-event attendees",
    ],
    commercialAccessibility: [
      "DIRECT_ADDRESSABLE vs CREDIBLY_CONTESTABLE vs PARTNERSHIP_REFERRAL vs REJECT",
    ],
    revenueStreamClassification: [
      "GUESTROOMS_ONLY | EVENT_FNB_ONLY | COMBINED_GUESTROOMS_EVENT_FNB | REVENUE_STREAM_UNCONFIRMED",
      "Investigate each stream independently; do not demote catering-only for absent rooms",
    ],
    sourceVerification: ["Confirm organization legal/trading identity", "Confirm listing authenticity"],
    blockerResolution: ["Unresolved buyer", "Unresolved timing", "Unresolved venue commitment", "Unresolved revenue stream"],
    stopConditions: [
      "If thesis collapses to nightlife/leisure-only — stop; do not force hotel demand",
      "If already hotel-committed with no contestable path — classify COMPETITOR_COMMITTED and stop",
      "Do not invent room nights from attendance",
      "Do not assume meetings/weddings/sports require guestrooms",
    ],
    deepResearchQuestion: p.answers.deepResearchQuestion,
  };
}
