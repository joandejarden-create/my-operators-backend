/**
 * Demand-generator decomposers for Bases 1–9.
 * Generator ≠ opportunity. Child accounts must independently pass gates.
 */

import { GDI_BASE_OF_DEMAND } from "./taxonomy.js";
import { scoreChildAccount, selectHighValueChildLeads } from "./child-account-gate.js";

const B = GDI_BASE_OF_DEMAND;

function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function orgFromTitle(title) {
  return String(title || "")
    .split(/[|\-—:]/)[0]
    .trim()
    .slice(0, 120);
}

/**
 * Base 1 — Published event → account-level children (not one opportunity).
 */
export function decomposePublishedEventDemand(generator = {}, hotel = {}, opts = {}) {
  const eventName = generator.title || generator.eventProgram || generator.organization || "EVENT";
  const org = generator.organizationName || generator.organization || orgFromTitle(eventName);
  const year = generator.eventYear || (String(generator.title || "").match(/\b(202[6-9])\b/) || [])[1];

  const roleTemplates = [
    { role: "ORGANIZING_TEAM", participantType: "ORGANIZER", travel: true },
    { role: "SPONSOR_TEAM", participantType: "SPONSOR", travel: true },
    { role: "EXHIBITOR", participantType: "EXHIBITOR", travel: true },
    { role: "GOVERNMENT_DELEGATION", participantType: "DELEGATION", travel: true },
    { role: "UNIVERSITY_DELEGATION", participantType: "DELEGATION", travel: true },
    { role: "NGO_DELEGATION", participantType: "DELEGATION", travel: true },
    { role: "PRODUCTION_VENDOR", participantType: "VENDOR", travel: true },
    { role: "SPEAKER_GROUP", participantType: "SPEAKER", travel: true },
    { role: "PRE_POST_SATELLITE", participantType: "ANCILLARY", travel: true },
    { role: "OVERFLOW_ACCOMMODATION", participantType: "HOUSING", travel: true },
  ];

  // Only materialize children that have evidence hints in the generator blob
  const blob = `${generator.title} ${generator.snippet || ""} ${generator.summaryWhat || ""} ${generator.fact || ""}`;
  const children = [];

  // Always keep organizer path as a candidate child if named org exists
  if (org && org.length >= 6) {
    children.push({
      organization: org,
      namedEntity: org,
      role: "ORGANIZING_TEAM",
      participantType: "ORGANIZER",
      participationEvidence: true,
      evidenceSource: generator.officialSource || generator.source,
      plausibleTravelingGroup: true,
      marketRelevant: true,
      accommodationSignal: /hotel|housing|accommodation|hébergement|alojamiento/i.test(blob),
      hotelMotionHypothesis: "Organizer / host team lodging near event venue",
      futureTiming: year || generator.eventStartDate || generator.nextKnownCycle,
      buyerClarity: Boolean(generator.buyerEntity || generator.organizer),
      groupSize: "UNKNOWN",
    });
  }

  for (const rt of roleTemplates) {
    const roleHit =
      (rt.role === "SPONSOR_TEAM" && /sponsor/i.test(blob)) ||
      (rt.role === "EXHIBITOR" && /exhibitor|booth|pavilion/i.test(blob)) ||
      (rt.role === "GOVERNMENT_DELEGATION" && /government|ministry|delegation/i.test(blob)) ||
      (rt.role === "UNIVERSITY_DELEGATION" && /university|academic/i.test(blob)) ||
      (rt.role === "NGO_DELEGATION" && /NGO|foundation|association/i.test(blob)) ||
      (rt.role === "PRODUCTION_VENDOR" && /production|AV |broadcast/i.test(blob)) ||
      (rt.role === "SPEAKER_GROUP" && /speaker|keynote|faculty/i.test(blob)) ||
      (rt.role === "PRE_POST_SATELLITE" && /satellite|pre-?event|post-?event|side meeting/i.test(blob)) ||
      (rt.role === "OVERFLOW_ACCOMMODATION" && /overflow|housing|hotel block|official hotel/i.test(blob));

    if (!roleHit || rt.role === "ORGANIZING_TEAM") continue;

    // Named child only when snippet names an entity — otherwise keep as role intelligence
    const namedMatch = blob.match(
      new RegExp(
        `([A-Z][A-Za-z0-9&.'\\- ]{2,40})\\s+(?:${rt.participantType === "SPONSOR" ? "sponsor" : rt.participantType === "EXHIBITOR" ? "exhibitor" : "delegation|partner"})`,
        "i"
      )
    );
    const named = namedMatch ? namedMatch[1].trim() : null;
    if (!named || named.length < 4) {
      children.push({
        organization: `${org} — ${rt.role} (unnamed)`,
        namedEntity: "",
        role: rt.role,
        participantType: rt.participantType,
        participationEvidence: true,
        evidenceSource: generator.officialSource || generator.source,
        plausibleTravelingGroup: rt.travel,
        marketRelevant: true,
        generatorOnly: true,
        groupSize: "UNKNOWN",
        hotelMotionHypothesis: `${rt.role} may travel for ${eventName}`,
        futureTiming: year,
      });
      continue;
    }

    children.push({
      organization: named,
      namedEntity: named,
      role: rt.role,
      participantType: rt.participantType,
      participationEvidence: true,
      evidenceSource: generator.officialSource || generator.source,
      plausibleTravelingGroup: true,
      marketRelevant: true,
      accommodationSignal: /hotel|housing|block/i.test(blob),
      hotelMotionHypothesis: `${named} ${rt.role} lodging for ${eventName}`,
      futureTiming: year,
      groupSize: "UNKNOWN",
    });
  }

  return finalizeChildren(children, generator, hotel, B.PUBLISHED_EVENT_DECOMPOSITION, opts);
}

/**
 * Base 2 — International org meetings → delegation units.
 */
export function decomposeIntlOrgMeeting(generator = {}, hotel = {}, opts = {}) {
  const meeting = generator.title || generator.eventProgram || "INTL_ORG_MEETING";
  const org = generator.organizationName || generator.organization || orgFromTitle(meeting);
  const blob = `${meeting} ${generator.snippet || ""} ${generator.summaryWhat || ""}`;

  const units = [
    { role: "SECRETARIAT", type: "SECRETARIAT" },
    { role: "WORKING_GROUP", type: "WORKING_GROUP" },
    { role: "NATIONAL_DELEGATION", type: "DELEGATION" },
    { role: "REGIONAL_DELEGATION", type: "DELEGATION" },
    { role: "NGO_PARTNER", type: "NGO" },
    { role: "TECHNICAL_CONSULTANT", type: "CONSULTANT" },
    { role: "TRAINING_TEAM", type: "TRAINING" },
  ];

  const children = [];
  // Named org as secretariat / host
  if (org) {
    children.push({
      organization: org,
      namedEntity: org,
      role: "SECRETARIAT",
      participantType: "SECRETARIAT",
      participationEvidence: true,
      evidenceSource: generator.officialSource || generator.source,
      plausibleTravelingGroup: true,
      marketRelevant: true,
      buyerClarity: true,
      publicContactPath: generator.publicContactPath || generator.organizationContactUrl || null,
      futureTiming: generator.eventYear || generator.nextKnownCycle || generator.eventStartDate,
      meetingCycle: generator.cadence || "RECURRING",
      groupSize: "UNKNOWN",
      hotelMotionHypothesis: `${org} meeting support / secretariat lodging`,
      historicAttendance: /annual|recurring|working group/i.test(blob),
    });
  }

  // Country delegation cues
  const countryHits = blob.match(
    /\b(Canadian|Brazilian|French|German|Swiss|Spanish|UK|British|US|American|Mexican|Chilean|Argentine|Portuguese|Italian|Dutch|Swedish|Norwegian)\b/gi
  );
  for (const c of [...new Set(countryHits || [])].slice(0, 4)) {
    children.push({
      organization: `${c} delegation — ${org}`,
      namedEntity: `${c} delegation`,
      role: "NATIONAL_DELEGATION",
      participantType: "DELEGATION",
      participationEvidence: true,
      evidenceSource: generator.officialSource || generator.source,
      plausibleTravelingGroup: true,
      marketRelevant: true,
      delegationEvidence: true,
      futureTiming: generator.eventYear || generator.nextKnownCycle,
      groupSize: "UNKNOWN",
      hotelMotionHypothesis: `${c} national delegation lodging for ${meeting}`,
    });
  }

  for (const u of units) {
    if (u.role === "SECRETARIAT" || u.role === "NATIONAL_DELEGATION") continue;
    if (
      (u.role === "WORKING_GROUP" && /working group|comité|committee/i.test(blob)) ||
      (u.role === "NGO_PARTNER" && /NGO|civil society/i.test(blob)) ||
      (u.role === "TECHNICAL_CONSULTANT" && /consultant|expert/i.test(blob)) ||
      (u.role === "TRAINING_TEAM" && /training|capacity/i.test(blob))
    ) {
      children.push({
        organization: `${org} — ${u.role}`,
        namedEntity: org,
        role: u.role,
        participantType: u.type,
        participationEvidence: true,
        evidenceSource: generator.officialSource || generator.source,
        plausibleTravelingGroup: true,
        marketRelevant: true,
        futureTiming: generator.eventYear,
        groupSize: "UNKNOWN",
        hotelMotionHypothesis: `${u.role} lodging for ${meeting}`,
      });
    }
  }

  return finalizeChildren(children, generator, hotel, B.INTERNATIONAL_ORG_RECURRING_GROUPS, opts);
}

/**
 * Base 3 — Participant / exhibitor / sponsor mining from public lists cues.
 */
export function mineEventParticipants(generator = {}, hotel = {}, opts = {}) {
  const blob = `${generator.title} ${generator.snippet || ""} ${generator.summaryWhat || ""} ${generator.fact || ""}`;
  const event = generator.title || "EVENT";
  const children = [];

  // Extract capitalized multi-word orgs near role words
  const patterns = [
    { re: /([A-Z][\w&.'-]+(?:\s+[A-Z][\w&.'-]+){0,4})\s+(?:sponsor|sponsorship)/gi, role: "SPONSOR" },
    { re: /([A-Z][\w&.'-]+(?:\s+[A-Z][\w&.'-]+){0,4})\s+(?:exhibitor|exhibiting)/gi, role: "EXHIBITOR" },
    { re: /(?:speaker|keynote|chaired by)\s+([A-Z][\w&.'-]+(?:\s+[A-Z][\w&.'-]+){0,3})/gi, role: "SPEAKER" },
    { re: /([A-Z][\w&.'-]+(?:\s+[A-Z][\w&.'-]+){0,3})\s+(?:pavilion|partner)/gi, role: "PARTNER" },
  ];

  const seen = new Set();
  for (const p of patterns) {
    let m;
    const re = new RegExp(p.re.source, p.re.flags);
    while ((m = re.exec(blob)) !== null) {
      const name = m[1].trim();
      if (name.length < 4 || seen.has(name.toLowerCase())) continue;
      if (/^(the|and|for|with|this|our|new)$/i.test(name)) continue;
      seen.add(name.toLowerCase());
      children.push({
        organization: name,
        namedEntity: name,
        role: p.role,
        participantType: p.role,
        participationEvidence: true,
        evidenceSource: generator.officialSource || generator.source,
        plausibleTravelingGroup: true,
        marketRelevant: true,
        travelLikelihood: "PLAUSIBLE",
        hotelMotionHypothesis: `${name} ${p.role.toLowerCase()} travel for ${event}`,
        futureTiming: generator.eventYear,
        groupSize: "UNKNOWN",
        knownPeople: "",
      });
    }
  }

  // If no named extractions but list language present, keep generator intelligence
  if (!children.length && /sponsor|exhibitor|speaker|pavilion/i.test(blob)) {
    children.push({
      organization: orgFromTitle(event),
      namedEntity: orgFromTitle(event),
      role: "PARTICIPANT_LIST_PRESENT",
      participantType: "LIST_SIGNAL",
      participationEvidence: true,
      evidenceSource: generator.officialSource || generator.source,
      plausibleTravelingGroup: false,
      marketRelevant: true,
      generatorOnly: true,
      hotelMotionHypothesis: "Participant list exists — mine named accounts next",
      groupSize: "UNKNOWN",
    });
  }

  return finalizeChildren(children, generator, hotel, B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING, opts);
}

/**
 * Base 4 — Rotation opportunity intelligence (not auto-candidate).
 */
export function buildRotationOpportunityIntelligence(generator = {}, hotel = {}) {
  const blob = `${generator.title} ${generator.snippet || ""} ${generator.summaryWhat || ""}`;
  const years = [...new Set((blob.match(/\b(20(?:2[3-9]|3[0-2]))\b/g) || []))].sort();
  const cities = [];
  for (const c of [
    "Amsterdam",
    "Copenhagen",
    "Barcelona",
    "Munich",
    "Geneva",
    "Paris",
    "London",
    "Madrid",
    "New York",
    "Miami",
    "Toronto",
  ]) {
    if (new RegExp(c, "i").test(blob)) cities.push(c);
  }

  const rotation =
    /rotat|site selection|host city|host proposal|bidding|future host/i.test(blob);
  const recurring = /annual|yearly|édition|biennial|recurring/i.test(blob);
  const targetPlausible = (hotel.geoTokens || []).some((t) =>
    new RegExp(t, "i").test(`${blob} ${hotel.destinationMarket}`)
  );

  let nextPredictedCycle = null;
  let nextConfirmedCycle = null;
  if (generator.eventStartDate && String(generator.eventStartDate).slice(0, 4) >= "2026") {
    nextConfirmedCycle = generator.eventStartDate;
  } else if (years.some((y) => Number(y) >= 2027)) {
    nextPredictedCycle = years.filter((y) => Number(y) >= 2027).join("|");
  } else if (rotation || recurring) {
    nextPredictedCycle = "NEXT_CYCLE_TBD";
  }

  return {
    eventSeriesId: `rot_${slug(generator.title || generator.id)}`,
    hotelKey: hotel.hotelKey,
    title: generator.title,
    organization: generator.organizationName || generator.organization,
    historicCycles: years,
    historicCities: cities,
    historicCountries: [],
    cadence: recurring ? "ANNUAL_OR_SERIES" : rotation ? "ROTATION" : "UNKNOWN",
    typicalMonth: "",
    attendanceHistory: "UNKNOWN",
    housingHistory: generator.lodgingEvidence ? "HINT" : "UNKNOWN",
    organizer: generator.organizer || generator.organizationName || "",
    agency: generator.agency || "",
    buyer: generator.buyerEntity || "",
    nextConfirmedCycle,
    nextPredictedCycle,
    decisionWindow:
      nextConfirmedCycle ||
      (rotation ? "SITE_SELECTION_OR_HOST_BID_WINDOW" : recurring ? "NEXT_CYCLE_ANNOUNCEMENT_WINDOW" : ""),
    rotationPattern: cities.length >= 2 ? cities.join(" → ") : rotation ? "ROTATION_LANGUAGE" : "",
    targetMarketPlausible: targetPlausible || Boolean(hotel.destinationMarket),
    timingState: nextConfirmedCycle
      ? "CONFIRMED_FUTURE"
      : rotation
        ? "ROTATION_PREDICTED"
        : recurring
          ? "RECURRING_EXPECTED"
          : "FUTURE_UNCONFIRMED",
    isPredicted: !nextConfirmedCycle && Boolean(nextPredictedCycle),
    officialSource: generator.officialSource || generator.source,
    baseOfDemand: B.HISTORIC_ROTATION_PREDICTION,
  };
}

/**
 * Base 5 — Recurring corporate meeting pattern.
 */
export function buildRecurringCorporateMeetingPattern(generator = {}, hotel = {}) {
  const blob = `${generator.title} ${generator.snippet || ""}`;
  const meetingType =
    (/sales kickoff|SKO/i.test(blob) && "SALES_KICKOFF") ||
    (/leadership|town hall/i.test(blob) && "LEADERSHIP_SUMMIT") ||
    (/board meeting/i.test(blob) && "BOARD_MEETING") ||
    (/partner|distributor/i.test(blob) && "PARTNER_CONFERENCE") ||
    (/training|academy/i.test(blob) && "TRAINING") ||
    (/incentive/i.test(blob) && "INCENTIVE") ||
    (/investor/i.test(blob) && "INVESTOR_DAY") ||
    "CORPORATE_MEETING";

  return {
    company: generator.organizationName || generator.organization || orgFromTitle(generator.title),
    meetingType,
    historicDates: (blob.match(/\b(20(?:2[3-9]|3[0-2]))\b/g) || []).join("|"),
    historicDestinations: hotel.destinationMarket || "",
    historicHotels: generator.competitorHotel || "",
    cadence: /annual|yearly/i.test(blob) ? "ANNUAL" : "UNKNOWN",
    typicalMonth: "",
    size: "UNKNOWN",
    agency: generator.agency || "",
    buyerFunction: "CORPORATE_MEETINGS_OR_EVENTS_TEAM",
    nextLikelyCycle: /202[6-9]/.test(blob) ? "CYCLE_HINT_IN_SOURCE" : "ESTIMATED_FROM_CADENCE",
    isPredicted: !generator.eventStartDate,
    timingState: generator.eventStartDate ? "CONFIRMED_FUTURE" : "RECURRING_EXPECTED",
    decisionWindow: "NEXT_CYCLE_BOOKING_WINDOW",
    baseOfDemand: B.RECURRING_CORPORATE_MEETINGS,
    hotelKey: hotel.hotelKey,
    officialSource: generator.officialSource || generator.source,
    title: generator.title,
  };
}

/**
 * Base 6 — Corporate trigger → meeting thesis (inference labeled).
 */
export function buildCorporateTriggerMeetingThesis(generator = {}, hotel = {}) {
  const blob = `${generator.title} ${generator.snippet || ""} ${generator.summaryWhat || ""}`;
  const trigger =
    (/acquisit|merger|integrat/i.test(blob) && "M_AND_A_INTEGRATION") ||
    (/CEO|chief executive|new leadership/i.test(blob) && "LEADERSHIP_CHANGE") ||
    (/office opening|facility opening|HQ/i.test(blob) && "OFFICE_OR_FACILITY_OPENING") ||
    (/expansion|workforce|hiring/i.test(blob) && "WORKFORCE_EXPANSION") ||
    (/product launch|launch/i.test(blob) && "PRODUCT_LAUNCH") ||
    (/fundrais|series [a-c]|IPO/i.test(blob) && "FUNDRAISING") ||
    (/partnership|alliance/i.test(blob) && "MAJOR_PARTNERSHIP") ||
    (/contract award|won .+ contract/i.test(blob) && "LARGE_CONTRACT") ||
    "CORPORATE_TRIGGER";

  const motion =
    trigger === "M_AND_A_INTEGRATION"
      ? "leadership integration / training"
      : trigger === "PRODUCT_LAUNCH"
        ? "launch meeting / field training"
        : trigger === "OFFICE_OR_FACILITY_OPENING"
          ? "kickoff / client event"
          : "probable group meeting / travel motion";

  return {
    company: generator.organizationName || generator.organization || orgFromTitle(generator.title),
    triggerFact: generator.title,
    triggerType: trigger,
    whyGroupMotionMayFollow: motion,
    whoLikelyControls: "events / regional leadership / HR / medical affairs (UNRESOLVED)",
    expectedTimingRange: "INFERRED_3_TO_12_MONTHS_POST_TRIGGER",
    targetGeography: hotel.destinationMarket || hotel.market,
    hotelFit: hotel.fitLine || "market_default",
    evidenceMissing: "buyer_path|future_decision|lodging_evidence",
    inferenceLabel: "INFERENCE_NOT_CONFIRMED_EVENT",
    isPredicted: true,
    timingState: "RECURRING_EXPECTED",
    baseOfDemand: B.CORPORATE_TRIGGER_DEMAND,
    hotelKey: hotel.hotelKey,
    officialSource: generator.officialSource || generator.source,
    title: generator.title,
    supportingEvidencePresent: /meeting|kickoff|training|integration|offsite/i.test(blob),
  };
}

/**
 * Base 7 — Pharma / medical ecosystem expansion.
 */
export function expandMedicalDemandEcosystem(generator = {}, hotel = {}, opts = {}) {
  const blob = `${generator.title} ${generator.snippet || ""}`;
  const roles = [
    { role: "PHARMA_SPONSOR", re: /pharma|pharmaceutical|biotech|medtech/i },
    { role: "CRO", re: /\bCRO\b|contract research/i },
    { role: "MED_COMMS", re: /medical communications|medcomms/i },
    { role: "INVESTIGATOR_GROUP", re: /investigator|site initiation/i },
    { role: "ADVISORY_BOARD", re: /advisory board/i },
    { role: "RESEARCH_INSTITUTION", re: /university|institute|hospital/i },
    { role: "NGO", re: /foundation|patient|NGO/i },
  ];
  const org = generator.organizationName || generator.organization || orgFromTitle(generator.title);
  const children = [
    {
      organization: org,
      namedEntity: org,
      role: "CONGRESS_OR_PROGRAM",
      participantType: "MEDICAL_PROGRAM",
      participationEvidence: true,
      evidenceSource: generator.officialSource || generator.source,
      plausibleTravelingGroup: true,
      marketRelevant: true,
      futureTiming: generator.eventYear,
      accommodationSignal: /hotel|housing|accommodation/i.test(blob),
      hotelMotionHypothesis: "Medical program lodging / overflow",
      groupSize: "UNKNOWN",
      ancillaryMeetingEvidence: /investigator|advisory|satellite/i.test(blob),
    },
  ];

  for (const r of roles) {
    if (!r.re.test(blob)) continue;
    children.push({
      organization: `${org} — ${r.role}`,
      namedEntity: org,
      role: r.role,
      participantType: r.role,
      participationEvidence: true,
      evidenceSource: generator.officialSource || generator.source,
      plausibleTravelingGroup: true,
      marketRelevant: true,
      futureTiming: generator.eventYear,
      hotelMotionHypothesis: `${r.role} ancillary lodging for medical program`,
      groupSize: "UNKNOWN",
      ancillaryMeetingEvidence: true,
    });
  }

  return finalizeChildren(children, generator, hotel, B.PHARMA_MEDICAL_ECOSYSTEM, opts);
}

/**
 * Base 8 — Project workforce decomposition.
 */
export function decomposeProjectWorkforceDemand(generator = {}, hotel = {}, opts = {}) {
  const blob = `${generator.title} ${generator.snippet || ""}`;
  const org = generator.organizationName || generator.organization || orgFromTitle(generator.title);
  const roles = [
    "OWNER",
    "DEVELOPER",
    "GENERAL_CONTRACTOR",
    "ENGINEERS",
    "CONSULTANTS",
    "SUBCONTRACTORS",
    "COMMISSIONING",
    "VENDORS",
    "TRAINERS",
    "CLIENT_TEAM",
  ];
  const workforceMotion =
    /temporary accommodation|crew hotel|project lodging|workforce|Monday.?Thursday|commissioning|engineer accommodation/i.test(
      blob
    );

  const children = roles
    .filter((role) => {
      if (role === "GENERAL_CONTRACTOR") return /contractor|construction|EPC/i.test(blob);
      if (role === "ENGINEERS") return /engineer|AECOM|Arup|Jacobs/i.test(blob);
      if (role === "COMMISSIONING") return /commission/i.test(blob);
      if (role === "OWNER" || role === "DEVELOPER") return true;
      return workforceMotion;
    })
    .map((role) => ({
      organization: role === "OWNER" || role === "DEVELOPER" ? org : `${org} — ${role}`,
      namedEntity: org,
      role,
      participantType: "PROJECT_WORKFORCE",
      participationEvidence: true,
      evidenceSource: generator.officialSource || generator.source,
      plausibleTravelingGroup: workforceMotion || role === "GENERAL_CONTRACTOR" || role === "ENGINEERS",
      marketRelevant: true,
      accommodationSignal: workforceMotion,
      hotelMotionHypothesis: `${role} temporary / weekly lodging`,
      knownTravelPattern: /Monday|weekly|rotational/i.test(blob),
      futureTiming: generator.eventYear || "PROJECT_WINDOW",
      groupSize: "UNKNOWN",
      hotelMotion: workforceMotion,
    }));

  return finalizeChildren(children, generator, hotel, B.PROJECT_WORKFORCE_DEMAND, opts);
}

/**
 * Base 9 — Sports / entertainment / production (not spectators).
 */
export function decomposeSportsEntertainmentDemand(generator = {}, hotel = {}, opts = {}) {
  const blob = `${generator.title} ${generator.snippet || ""}`;
  const org = generator.organizationName || generator.organization || orgFromTitle(generator.title);
  if (/spectator|fan zone|ticket/i.test(blob) && !/team|crew|federation|broadcast|production/i.test(blob)) {
    return {
      generatorId: generator.id,
      baseOfDemand: B.SPORTS_ENTERTAINMENT_PRODUCTION,
      children: [],
      leads: [],
      intelligence: [
        {
          note: "Spectator attendance is not group hotel demand",
          organization: org,
          generatorOnly: true,
        },
      ],
    };
  }

  const roles = [
    { role: "TEAM", re: /team|squad|club/i },
    { role: "OFFICIALS", re: /official|referee|umpire/i },
    { role: "BROADCAST_CREW", re: /broadcast|TV crew|media/i },
    { role: "PRODUCTION_CREW", re: /production|stage|AV /i },
    { role: "FEDERATION", re: /federation|association|FIFA|UEFA|IOC/i },
    { role: "PROMOTER", re: /promoter|tour|concert/i },
    { role: "SPONSOR_ACTIVATION", re: /sponsor/i },
    { role: "SECURITY", re: /security/i },
  ];

  const children = [
    {
      organization: org,
      namedEntity: org,
      role: "EVENT_ORGANIZER",
      participantType: "ORGANIZER",
      participationEvidence: true,
      evidenceSource: generator.officialSource || generator.source,
      plausibleTravelingGroup: true,
      marketRelevant: true,
      buyerClarity: true,
      futureTiming: generator.eventYear || generator.eventStartDate,
      hotelMotionHypothesis: "Organizer / federation lodging (not spectator)",
      groupSize: "UNKNOWN",
    },
  ];

  for (const r of roles) {
    if (!r.re.test(blob)) continue;
    children.push({
      organization: `${org} — ${r.role}`,
      namedEntity: org,
      role: r.role,
      participantType: r.role,
      participationEvidence: true,
      evidenceSource: generator.officialSource || generator.source,
      plausibleTravelingGroup: true,
      marketRelevant: true,
      accommodationSignal: /hotel|housing|team hotel/i.test(blob),
      hotelMotionHypothesis: `${r.role} lodging`,
      futureTiming: generator.eventYear,
      groupSize: "UNKNOWN",
      buyerRoleHint:
        r.role === "TEAM"
          ? "team_travel_coordinator"
          : r.role === "PRODUCTION_CREW"
            ? "production_company"
            : r.role === "FEDERATION"
              ? "federation"
              : "event_organizer",
    });
  }

  return finalizeChildren(children, generator, hotel, B.SPORTS_ENTERTAINMENT_PRODUCTION, opts);
}

function finalizeChildren(children, generator, hotel, base, opts = {}) {
  const scored = children.map((c) => {
    const gate = scoreChildAccount(c, generator, hotel);
    return {
      ...c,
      ...gate,
      id: `child_${slug(base)}_${slug(c.organization)}_${slug(generator.id || generator.title)}`.slice(0, 96),
      generatorId: generator.id || generator.title,
      generatorTitle: generator.title,
      baseOfDemand: base,
      hotelKey: hotel.hotelKey,
      demandEngine: generator.demandEngine || generator.engine || null,
      officialSource: c.evidenceSource || generator.officialSource || generator.source,
      eventYear: generator.eventYear,
      eventStartDate: generator.eventStartDate,
      competitorHotel: generator.competitorHotel,
      evidenceClass: generator.evidenceClass,
      lodgingEvidence: generator.lodgingEvidence,
      buyerEntity: c.buyerEntity || generator.buyerEntity || null,
      organizer: c.organizer || generator.organizer || null,
      publicContactPath: c.publicContactPath || generator.publicContactPath || null,
    };
  });

  const leads = selectHighValueChildLeads(scored, {
    maxPerGenerator: opts.maxPerGenerator ?? 5,
  });
  const intelligence = scored.filter((c) => !c.admitAsLead);

  return {
    generatorId: generator.id || generator.title,
    baseOfDemand: base,
    hotelKey: hotel.hotelKey,
    children: scored,
    leads,
    intelligence,
    childCount: scored.length,
    leadCount: leads.length,
  };
}
