/**
 * Autonomous Demand Discovery V5
 *
 * Upgrade to existing GDI discovery — three complementary pathways into the SAME
 * canonical opportunity / ORS path:
 *   ORGANIZATION_LED · TRIGGER_LED · EVENT_LED
 *
 * Reuses V4 strategies, capturable-fit screen, normalize/dedupe, V4.1 spine,
 * weekly finding classes, and runDeepResearchOnExistingGdiOpportunity.
 * Does NOT replace ORS, qualification, or contact architecture.
 *
 * No paid spend · no Surfe/PDL/Parallel · no Airtable/publish · weekly production OFF.
 */

import { createHash } from "node:crypto";
import {
  SUPERVISOR_EXPECTED_SEGMENTS,
  canonicalizeSegmentId,
} from "./research-constants-v1.js";
import { RESEARCH_SEGMENT_LABEL } from "../workflow/hotel-segment-research-plan-v1.js";
import {
  COMMERCIAL_DEMAND_DISCOVERY_V4,
  DISCOVERY_TRACK,
  DEMAND_SIGNAL_KIND,
  BETHESDA_SEGMENT_DISCOVERY_WEIGHT,
  SEGMENT_DISCOVERY_SOURCE_TYPES,
  generateHotelDemandDiscoveryStrategies,
  assessCapturableHotelDemandFit,
  screenDiscoveryHitCommercialRelevance,
  normalizeDiscoveryHit,
  dedupeDiscoveryAgainstMemory,
  enqueueDiscoveryIntoOrsQueue,
  loadHotelGdiConfig,
} from "./commercial-demand-discovery-v4.js";
import { questionFingerprint } from "./dynamic-research-planner-v1.js";
import {
  WEEKLY_FINDING_CLASS,
  classifyFindingAgainstBethesdaBaseline,
} from "./bethesda-weekly-net-new-discovery-v1.js";
import { assertPaidContactProvidersDisabled } from "./commercial-spine-restoration-v41.js";

export const AUTONOMOUS_DEMAND_DISCOVERY_V5 = "gdi-autonomous-demand-discovery-v5";

export const DISCOVERY_PATHWAY = Object.freeze({
  ORGANIZATION_LED: "ORGANIZATION_LED",
  TRIGGER_LED: "TRIGGER_LED",
  EVENT_LED: "EVENT_LED",
});

/** Evidence-backed demand mechanism strength — maps into existing GDI evidence models. */
export const DEMAND_MECHANISM_CLASS = Object.freeze({
  VERIFIED_DEMAND: "VERIFIED_DEMAND",
  INFERRED_DEMAND: "INFERRED_DEMAND",
  UNSUPPORTED_DEMAND: "UNSUPPORTED_DEMAND",
});

export const V5_CANDIDATE_KIND = Object.freeze({
  OPPORTUNITY: "OPPORTUNITY",
  ACCOUNT_INTELLIGENCE: "ACCOUNT_INTELLIGENCE",
  RESEARCH_LEAD: "RESEARCH_LEAD",
  EXCLUDE: "EXCLUDE",
});

/** Incremental discovery priority (not equal quotas). */
export const V5_INCREMENTAL_PRIORITY_SEGMENTS = Object.freeze([
  "corporate_meetings_training",
  "professional_planners_dmc",
  "scaling_companies",
  "local_meetings_catering",
  "exhibitor_sponsor_teams",
]);

export const PROPOSED_V5_STAGE_A_CEILING_USD = 2.0;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function normName(v) {
  return text(v).toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function hashKey(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 12);
}

function geoHints(hotel = {}) {
  const name = text(hotel.displayName || hotel.name || "target hotel");
  const city = text(hotel.city || "");
  const territory = text(hotel.demandTerritory?.label || "");
  const isBethesda = /bethesda/i.test(name) || hotel.hotelId === "recLuxvwwxID7U2B8";
  const geo = [city, territory].filter(Boolean).join(" ") || (isBethesda ? "Bethesda DMV" : city);
  const anchors = [
    ...(hotel.commercialPriorities?.demandAnchorFocus || []),
    ...(hotel.demandTerritory?.includes || []).slice(0, 4),
  ].map(text).filter(Boolean);
  return {
    name,
    city: city || (isBethesda ? "Bethesda" : ""),
    territory,
    geo,
    isBethesda,
    anchors,
    rooms: hotel.capabilityProfile?.totalGuestrooms || null,
    meetingRooms: hotel.capabilityProfile?.meetingRoomCount || null,
    ballroomSqFt: hotel.capabilityProfile?.largestBallroomSqFt || null,
  };
}

/**
 * 11-segment strategy matrix — demand generators, signals, false positives, eligibility.
 */
export function buildElevenSegmentStrategyMatrix(hotel = {}) {
  const g = geoHints(hotel);
  const matrix = {
    associations_government: {
      pathways: [DISCOVERY_PATHWAY.EVENT_LED, DISCOVERY_PATHWAY.ORGANIZATION_LED, DISCOVERY_PATHWAY.TRIGGER_LED],
      demandGenerators: ["chapters", "advocacy days", "host-city RFPs", "galas"],
      hypotheses: ["DMV chapter venue sourcing", "future host-city bids"],
      sourceFamilies: ["association_rfp", "chapter_program_pages", "advocacy_calendars"],
      commercialSignals: ["venue RFP", "host city call", "open registration + lodging"],
      buyerFunctions: ["meeting planner", "chapter events chair", "AMC"],
      bethesdaFit: "DMV chapter events; overflow near NIH/WR",
      falsePositives: ["expired EM RFPs", "committed downtown HQ hotels", "housing tabs"],
      deepResearchWhen: "open venue/host path OR verified recurring chapter program with local fit",
    },
    medical_academic: {
      pathways: [DISCOVERY_PATHWAY.EVENT_LED, DISCOVERY_PATHWAY.ORGANIZATION_LED, DISCOVERY_PATHWAY.TRIGGER_LED],
      demandGenerators: ["NIH symposia", "society meetings", "federal health training"],
      hypotheses: ["campus overflow lodging", "society site selection"],
      sourceFamilies: ["nih_event_pages", "society_site_selection", "federal_training"],
      commercialSignals: ["site selection", "early RFP", "attendee lodging need"],
      buyerFunctions: ["society meetings staff", "housing bureau", "federal training coordinator"],
      bethesdaFit: "NIH/WR corridor; overflow when campus venue named",
      falsePositives: ["Natcher-only with no lodging signal", "housing directory pages"],
      deepResearchWhen: "forward cycle + lodging/overflow or open site selection",
    },
    sports: {
      pathways: [DISCOVERY_PATHWAY.EVENT_LED, DISCOVERY_PATHWAY.TRIGGER_LED],
      demandGenerators: ["tournaments", "team travel", "stay-to-play"],
      hypotheses: ["future tournament cycles near Soccerplex/DMV"],
      sourceFamilies: ["tournament_sites", "club_travel", "housing_agencies"],
      commercialSignals: ["hotel partner RFP", "team block", "future cycle dates"],
      buyerFunctions: ["tournament director", "housing agency", "club travel coordinator"],
      bethesdaFit: "Montgomery / DMV field events with lodging need",
      falsePositives: ["baseline Premier Cup rediscovery", "distant venue without Bethesda pathway"],
      deepResearchWhen: "forward tournament + lodging pathway not baseline duplicate",
    },
    corporate_meetings_training: {
      pathways: [DISCOVERY_PATHWAY.ORGANIZATION_LED, DISCOVERY_PATHWAY.TRIGGER_LED, DISCOVERY_PATHWAY.EVENT_LED],
      demandGenerators: ["corridor employers", "training providers", "sales kickoffs", "retreats"],
      hypotheses: ["recurring offsites", "residential certification", "contractor training"],
      sourceFamilies: ["corp_news", "training_calendars", "ops_disclosures", "procurement"],
      commercialSignals: ["multi-day training", "offsite venue", "kickoff hotel"],
      buyerFunctions: ["travel/meetings", "L&D", "procurement", "EA/office manager"],
      bethesdaFit: `${g.rooms || 407} rooms / ${g.meetingRooms || 27} meeting rooms for mid-size corporate`,
      falsePositives: ["office address only", "CVB hotel directory", "hiring without program"],
      deepResearchWhen: "specific program/offsite OR recurring training with venue need evidenced",
    },
    entertainment_production: {
      pathways: [DISCOVERY_PATHWAY.TRIGGER_LED, DISCOVERY_PATHWAY.ORGANIZATION_LED],
      demandGenerators: ["film/TV productions", "tour crews", "performing arts"],
      hypotheses: ["crew basecamp lodging near MD/DC shoots"],
      sourceFamilies: ["film_office", "production_notices", "tour_schedules"],
      commercialSignals: ["crew hotel", "basecamp", "tech rider lodging"],
      buyerFunctions: ["production manager", "location manager", "tour coordinator"],
      bethesdaFit: "MD/DC corridor productions needing crew lodging",
      falsePositives: ["AV vendor landings", "supplier marketing without named production"],
      deepResearchWhen: "named production + lodging/basecamp signal",
    },
    tours_educational_specialist: {
      pathways: [DISCOVERY_PATHWAY.ORGANIZATION_LED, DISCOVERY_PATHWAY.EVENT_LED],
      demandGenerators: ["university delegations", "study tours", "institutional visits"],
      hypotheses: ["NIH/Smithsonian-linked educational travel"],
      sourceFamilies: ["university_travel", "tour_operators", "institutional_calendars"],
      commercialSignals: ["group lodging", "delegation itinerary", "open hotel partner"],
      buyerFunctions: ["tour operator", "study-abroad office", "institutional events"],
      bethesdaFit: "educational groups with Montgomery/NIH itinerary",
      falsePositives: ["committed competitor venues", "generic tour landing pages"],
      deepResearchWhen: "forward program + open lodging or itinerary near Bethesda",
    },
    weddings_social_private_events: {
      pathways: [DISCOVERY_PATHWAY.ORGANIZATION_LED, DISCOVERY_PATHWAY.EVENT_LED],
      demandGenerators: ["planners", "multi-day celebrations", "venue referrals"],
      hypotheses: ["preferred-hotel partnerships", "guestroom blocks with banquet"],
      sourceFamilies: ["planner_portfolios", "venue_referrals", "social_calendars"],
      commercialSignals: ["preferred hotel", "room block request", "multi-day celebration"],
      buyerFunctions: ["wedding planner", "catering sales", "family coordinator"],
      bethesdaFit: `ballroom ~${g.ballroomSqFt || 4592} sq ft + guestrooms`,
      falsePositives: ["generic venue marketing", "inquiry-only pages with no organizer"],
      deepResearchWhen: "named planner/program with guestroom or banquet mechanism",
    },
    professional_planners_dmc: {
      pathways: [DISCOVERY_PATHWAY.ORGANIZATION_LED, DISCOVERY_PATHWAY.TRIGGER_LED],
      demandGenerators: ["DMCs", "incentive houses", "third-party planners", "AMCs"],
      hypotheses: ["hotel sourcing intermediaries controlling DMV programs"],
      sourceFamilies: ["dmc_portfolios", "incentive_case_studies", "planner_directories"],
      commercialSignals: ["hotel shortlist", "preferred hotels", "named client program"],
      buyerFunctions: ["DMC hotel sourcing", "incentive ops", "AMC housing"],
      bethesdaFit: "DMV DMC/planner with evidenced hotel sourcing capability",
      falsePositives: ["DMC homepage without program or sourcing capability", "directory listings"],
      deepResearchWhen: "sourcing capability + named client/program OR preferred-hotel process",
    },
    scaling_companies: {
      pathways: [DISCOVERY_PATHWAY.TRIGGER_LED, DISCOVERY_PATHWAY.ORGANIZATION_LED],
      demandGenerators: ["expansions", "hiring waves", "project mobilizations", "new offices"],
      hypotheses: ["training waves + temporary lodging from corridor scale-ups"],
      sourceFamilies: ["corp_announcements", "contract_awards", "facility_openings"],
      commercialSignals: ["training for new hires", "project team lodging", "offsite integration"],
      buyerFunctions: ["HR/L&D", "project PMO", "corporate travel"],
      bethesdaFit: "Bethesda/Rockville biotech/tech corridor employers",
      falsePositives: ["hiring news alone", "office address", "unsupported room-night estimates"],
      deepResearchWhen: "verified trigger + evidenced meeting/travel/training mechanism",
    },
    local_meetings_catering: {
      pathways: [DISCOVERY_PATHWAY.ORGANIZATION_LED, DISCOVERY_PATHWAY.EVENT_LED, DISCOVERY_PATHWAY.TRIGGER_LED],
      demandGenerators: ["board meetings", "seminars", "receptions", "chamber events"],
      hypotheses: ["day meetings with catering as primary revenue"],
      sourceFamilies: ["chamber_calendars", "professional_seminars", "institutional_receptions"],
      commercialSignals: ["day meeting venue", "catering RFP", "board offsite"],
      buyerFunctions: ["EA", "office manager", "events coordinator", "catering buyer"],
      bethesdaFit: "local executive/professional meetings near Metro/NIH",
      falsePositives: ["Exa Places stubs", "generic conference-room ads", "no org identity"],
      deepResearchWhen: "named org + specific meeting/catering need",
    },
    exhibitor_sponsor_teams: {
      pathways: [DISCOVERY_PATHWAY.EVENT_LED, DISCOVERY_PATHWAY.ORGANIZATION_LED, DISCOVERY_PATHWAY.TRIGGER_LED],
      demandGenerators: ["sponsors", "exhibitors", "vendor training accompanying conferences"],
      hypotheses: ["hospitality suites / team lodging distinct from host block"],
      sourceFamilies: ["sponsor_prospectus", "exhibitor_kits", "vendor_training"],
      commercialSignals: ["sponsor hospitality", "exhibitor hotel", "vendor training offsite"],
      buyerFunctions: ["sponsor ops", "exhibitor services", "agency"],
      bethesdaFit: "DMV conferences with overflow/sponsor team lodging",
      falsePositives: ["generic sponsor PDF for distant committed shows", "past events"],
      deepResearchWhen: "forward local/DMV show + distinct exhibitor/sponsor lodging path",
    },
  };

  return {
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    hotelId: hotel.hotelId || null,
    segments: Object.fromEntries(
      SUPERVISOR_EXPECTED_SEGMENTS.map((seg) => [
        seg,
        {
          label: RESEARCH_SEGMENT_LABEL[seg] || seg,
          incrementalPriority: V5_INCREMENTAL_PRIORITY_SEGMENTS.includes(seg),
          discoveryWeight: BETHESDA_SEGMENT_DISCOVERY_WEIGHT[seg] || 1,
          v4SourceTypes: SEGMENT_DISCOVERY_SOURCE_TYPES[seg] || [],
          ...(matrix[seg] || {}),
        },
      ]),
    ),
  };
}

/** Source families evaluated by opportunity quality, not URL volume. */
export function buildSourceFamilyStrategy() {
  return {
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    families: [
      {
        id: "corporate_news_announcements",
        pathways: [DISCOVERY_PATHWAY.TRIGGER_LED, DISCOVERY_PATHWAY.ORGANIZATION_LED],
        qualityMetric: "triggers_with_demand_mechanism",
        risk: "hiring_news_without_travel",
      },
      {
        id: "ops_office_disclosures",
        pathways: [DISCOVERY_PATHWAY.ORGANIZATION_LED],
        qualityMetric: "orgs_with_recurring_program_evidence",
        risk: "office_address_alone",
      },
      {
        id: "training_program_calendars",
        pathways: [DISCOVERY_PATHWAY.ORGANIZATION_LED, DISCOVERY_PATHWAY.EVENT_LED],
        qualityMetric: "residential_or_offsite_training",
        risk: "online_only_courses",
      },
      {
        id: "procurement_solicitations",
        pathways: [DISCOVERY_PATHWAY.EVENT_LED, DISCOVERY_PATHWAY.TRIGGER_LED],
        qualityMetric: "open_venue_or_meeting_rfps",
        risk: "expired_or_wrong_respondent_class",
      },
      {
        id: "government_contract_awards",
        pathways: [DISCOVERY_PATHWAY.TRIGGER_LED],
        qualityMetric: "awards_with_travel_or_training_mechanism",
        risk: "award_alone_no_travel",
      },
      {
        id: "dmc_planner_portfolios",
        pathways: [DISCOVERY_PATHWAY.ORGANIZATION_LED],
        qualityMetric: "sourcing_capability_plus_named_program",
        risk: "homepage_without_program",
      },
      {
        id: "sponsor_exhibitor_materials",
        pathways: [DISCOVERY_PATHWAY.EVENT_LED, DISCOVERY_PATHWAY.ORGANIZATION_LED],
        qualityMetric: "local_forward_shows_with_team_lodging",
        risk: "distant_committed_prospectus",
      },
      {
        id: "chapter_association_program_pages",
        pathways: [DISCOVERY_PATHWAY.EVENT_LED, DISCOVERY_PATHWAY.ORGANIZATION_LED],
        qualityMetric: "open_host_or_recurring_local_programs",
        risk: "generic_about_pages",
      },
      {
        id: "venue_host_rfps",
        pathways: [DISCOVERY_PATHWAY.EVENT_LED],
        qualityMetric: "geo_eligible_open_deadlines",
        risk: "housing_tabs_as_false_opportunities",
      },
    ],
    evaluationRule: "Rank families by credible demand mechanisms and Bethesda-fit yield — not raw hit count.",
    adapters: ["direct_public", "exa", "firecrawl"],
    forbiddenWithoutGo: ["surfe", "pdl", "parallel"],
  };
}

function pushStrategy(list, {
  pathway, segment, researchQuestion, sourceTypes = [], signalKinds = [], notes = null,
}) {
  const question = text(researchQuestion);
  if (!question) return;
  list.push({
    pathway,
    segment,
    researchQuestion: question,
    questionFingerprint: questionFingerprint(`${pathway}::${segment}::${question}`),
    sourceTypes,
    expectedSignalKinds: signalKinds,
    demandSeeking: true,
    housingPageExtractionOnly: false,
    discoveryWeight: BETHESDA_SEGMENT_DISCOVERY_WEIGHT[segment] || 1,
    underrepresentedPriority: V5_INCREMENTAL_PRIORITY_SEGMENTS.includes(segment),
    track: DISCOVERY_TRACK.NEW_DISCOVERY,
    notes,
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
  });
}

/**
 * ORGANIZATION_LED strategies — start from demand-capable organizations, not event titles.
 */
export function generateOrganizationLedStrategies(hotel = {}, { maxPerSegment = 2 } = {}) {
  const g = geoHints(hotel);
  const out = [];
  const bySeg = {
    corporate_meetings_training: [
      `${g.city} OR Rockville OR "Montgomery County" employer OR federal contractor training OR offsite meeting venue requirements (not office directory)`,
      `${g.geo} professional services OR biotech company recurring leadership offsite OR residential training near NIH`,
    ],
    professional_planners_dmc: [
      `${g.geo} DMC OR incentive house hotel sourcing capability preferred hotels OR client case study Bethesda OR DMV`,
      `${g.geo} third-party meeting planner corporate hotel shortlist Montgomery OR Washington (named program preferred)`,
    ],
    scaling_companies: [
      `${g.city} scale-up OR expanding company operations training program OR project team lodging needs`,
      `${g.geo} company with multi-site project mobilization lodging OR integration offsite OR corporate retreat`,
    ],
    local_meetings_catering: [
      `${g.city} company OR institution board meeting OR executive day meeting hotel catering requirements`,
      `${g.city} professional association OR chamber recurring luncheon OR seminar venue hotel`,
    ],
    exhibitor_sponsor_teams: [
      `${g.geo} sponsor OR exhibitor company hospitality OR team lodging policy accompanying DMV conference`,
    ],
    associations_government: [
      `${g.geo} association chapter recurring education OR board retreat venue hotel (organization-first)`,
    ],
    medical_academic: [
      `${g.anchors[0] || "NIH"} adjacent research institute OR society recurring training offsite hotel needs`,
    ],
    sports: [
      `${g.geo} sports club OR tournament organizer recurring travel lodging partners (not housing tab alone)`,
    ],
    entertainment_production: [
      `${g.geo} production company OR tour management crew lodging partners Maryland OR DC`,
    ],
    tours_educational_specialist: [
      `${g.geo} educational tour operator OR university travel office group lodging partners near NIH`,
    ],
    weddings_social_private_events: [
      `${g.city} wedding OR social planner preferred hotel partners guestroom blocks`,
    ],
  };

  for (const segment of SUPERVISOR_EXPECTED_SEGMENTS) {
    const templates = bySeg[segment] || [`${g.geo} ${segment.replace(/_/g, " ")} organization hotel demand mechanism`];
    for (const q of templates.slice(0, maxPerSegment)) {
      pushStrategy(out, {
        pathway: DISCOVERY_PATHWAY.ORGANIZATION_LED,
        segment,
        researchQuestion: q,
        sourceTypes: ["ops_office_disclosures", "training_program_calendars", "dmc_planner_portfolios", "chapter_association_program_pages"],
        signalKinds: [DEMAND_SIGNAL_KIND.ORGANIZATION_CAPABILITY, DEMAND_SIGNAL_KIND.RECURRING_CYCLE],
        notes: "Require specific demand mechanism before opportunity promotion",
      });
    }
  }
  return out;
}

/**
 * TRIGGER_LED strategies — changes that may create incremental hotel demand.
 */
export function generateTriggerLedStrategies(hotel = {}, { maxPerSegment = 2 } = {}) {
  const g = geoHints(hotel);
  const out = [];
  const bySeg = {
    scaling_companies: [
      `${g.city} "opens office" OR "expanding workforce" OR "new headquarters" OR relocation training OR offsite 2026 OR 2027`,
      `${g.geo} merger OR integration OR relocation project team lodging OR kickoff meeting OR corporate retreat`,
    ],
    corporate_meetings_training: [
      `${g.geo} "awarded contract" OR "training contract" federal contractor employee travel OR residential training Bethesda`,
      `${g.city} "sales kickoff" OR "leadership offsite" announced 2026 OR 2027 hotel OR venue`,
    ],
    associations_government: [
      `${g.geo} association "new annual meeting" OR "relocating conference" OR "seeking host city" 2027 OR 2028`,
    ],
    medical_academic: [
      `${g.anchors[0] || "NIH"} "new training program" OR "collaborative research kickoff" offsite meeting hotel`,
    ],
    professional_planners_dmc: [
      `${g.geo} "appointed DMC" OR "meeting agency of record" OR "preferred hotel program" launch`,
    ],
    local_meetings_catering: [
      `${g.city} "board retreat" OR "executive offsite" announced venue OR hotel catering`,
    ],
    exhibitor_sponsor_teams: [
      `${g.geo} sponsor hospitality suite OR exhibitor team hotel RFP 2026 OR 2027 DMV`,
    ],
    entertainment_production: [
      `${g.geo} Maryland film OR production "crew hotel" OR basecamp lodging announcement`,
    ],
    tours_educational_specialist: [
      `${g.geo} university "delegation visit" OR study tour lodging RFP Bethesda OR Rockville`,
    ],
    sports: [
      `${g.geo} tournament "new host" OR "hotel partner sought" 2027 OR 2028`,
    ],
    weddings_social_private_events: [
      `${g.city} multi-day celebration OR wedding weekend hotel block inquiry OR preferred hotel announcement`,
    ],
  };

  for (const segment of SUPERVISOR_EXPECTED_SEGMENTS) {
    const templates = bySeg[segment] || [`${g.geo} ${segment.replace(/_/g, " ")} expansion OR award OR new program hotel demand`];
    for (const q of templates.slice(0, maxPerSegment)) {
      pushStrategy(out, {
        pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
        segment,
        researchQuestion: q,
        sourceTypes: ["corporate_news_announcements", "government_contract_awards", "procurement_solicitations"],
        signalKinds: [DEMAND_SIGNAL_KIND.SOURCING_ACTIVITY, DEMAND_SIGNAL_KIND.PROGRAM_OR_EVENT],
        notes: "Trigger alone ≠ opportunity; require causal demand mechanism",
      });
    }
  }
  return out;
}

/**
 * EVENT_LED — preserve V4 strategies, tag pathway, add early RFP/deadline hygiene hints.
 */
export function generateEventLedStrategies(hotel = {}, { maxPerSegment = 3 } = {}) {
  const pack = generateHotelDemandDiscoveryStrategies(hotel, { maxPerSegment });
  return (pack.strategies || []).map((s) => ({
    ...s,
    pathway: DISCOVERY_PATHWAY.EVENT_LED,
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    earlyChecks: [
      "event_dates_forward_looking",
      "rfp_deadline_not_expired",
      "respondent_class_matches_hotel_or_venue",
      "destination_eligibility",
      "existing_hotel_commitments",
      "reject_housing_tab_as_opportunity",
    ],
    parentPackVersion: COMMERCIAL_DEMAND_DISCOVERY_V4,
  }));
}

/**
 * Combined tri-path strategy pack for a hotel.
 */
export function generateTriPathDiscoveryStrategies(hotel = {}, opts = {}) {
  const orgMax = opts.orgMaxPerSegment ?? 2;
  const triggerMax = opts.triggerMaxPerSegment ?? 2;
  const eventMax = opts.eventMaxPerSegment ?? 3;
  const organizationLed = generateOrganizationLedStrategies(hotel, { maxPerSegment: orgMax });
  const triggerLed = generateTriggerLedStrategies(hotel, { maxPerSegment: triggerMax });
  const eventLed = generateEventLedStrategies(hotel, { maxPerSegment: eventMax });
  const strategies = [...organizationLed, ...triggerLed, ...eventLed];
  const byPathway = {
    [DISCOVERY_PATHWAY.ORGANIZATION_LED]: organizationLed.length,
    [DISCOVERY_PATHWAY.TRIGGER_LED]: triggerLed.length,
    [DISCOVERY_PATHWAY.EVENT_LED]: eventLed.length,
  };
  return {
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    hotelId: hotel.hotelId || null,
    hotelName: hotel.displayName || hotel.name || null,
    strategyCount: strategies.length,
    byPathway,
    segmentsCovered: [...new Set(strategies.map((s) => s.segment))],
    allElevenSegmentsCovered: SUPERVISOR_EXPECTED_SEGMENTS.every((s) =>
      strategies.some((x) => x.segment === s)),
    incrementalPrioritySegments: [...V5_INCREMENTAL_PRIORITY_SEGMENTS],
    strategies,
    note: "Three pathways feed the same ORS/canonical opportunity path — not a separate V5 database.",
  };
}

const VERIFIED_DEMAND_RE = /\b(RFP|venue selection|site selection|host hotel|room block|hotel partner|preferred hotel|meeting space|in-kind|speaker room|lodging required|hotel sourcing|hotel lodging|hotel catering|residential (certification )?training|training programme|training program|multi-day (meeting|training|certification)|conference venue|symposium programme|venue host)\b/i;
const INFERRED_ACTIVITY_RE = /\b(expansion|new office|hiring|contract award|mobilization|integration|relocation|opens?\s+facility|opening\s+new|workforce|facility opening|new\s+\w+\s+facility)\b/i;
const TRAVEL_OR_MEETING_INFER_RE = /\b(training|offsite|travel|lodging|meeting|kickoff|retreat|onboarding|residential|catering)\b/i;
const UNSUPPORTED_ALONE_RE = /\b(companies hiring|careers|jobs|office directory|about us|contact us|linkedin)\b/i;

/**
 * Demand-mechanism intelligence — why hotel demand may exist.
 */
export function assessDemandMechanism(candidate = {}) {
  const org = text(candidate.organizationName);
  const activity = text(candidate.demandMechanism || candidate.programName || candidate.eventName || candidate.title);
  const blob = `${org} ${activity} ${candidate.demandSignalSummary || ""} ${candidate.triggerSummary || ""} ${candidate.sourceUrl || ""}`;
  const verified = VERIFIED_DEMAND_RE.test(blob)
    && Boolean(org)
    && activity.length >= 8
    && !UNSUPPORTED_ALONE_RE.test(activity);
  const inferred = !verified
    && INFERRED_ACTIVITY_RE.test(blob)
    && TRAVEL_OR_MEETING_INFER_RE.test(blob)
    && Boolean(org);
  const unsupported = !verified && !inferred;

  let mechanismClass = DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND;
  if (verified) mechanismClass = DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND;
  else if (inferred) mechanismClass = DEMAND_MECHANISM_CLASS.INFERRED_DEMAND;

  return {
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    mechanismClass,
    organizationName: org || null,
    activityOrProgram: activity || null,
    demandMechanismSummary: text(candidate.demandMechanism || activity || candidate.demandSignalSummary).slice(0, 400) || null,
    locationHint: text(candidate.geographyHint || candidate.eventLocationSummary) || null,
    timingHint: text(candidate.eventYear || candidate.timingHint) || null,
    potentialRevenueStreams: hypothesizeStreams(blob),
    sourcingStatusHint: /RFP|seeking|host|open|deadline/i.test(blob)
      ? "POSSIBLE_OPEN_SOURCING"
      : (/already contracted|held at|headquarters hotel/i.test(blob) ? "POSSIBLE_COMMITMENT" : "UNKNOWN"),
    supportingEvidenceUrls: [candidate.sourceUrl].filter(Boolean),
    uncertainties: unsupported
      ? ["No defensible commercial demand mechanism established"]
      : (inferred ? ["Demand inferred from organizational activity — lodging volumes not verified"] : []),
    note: "INFERRED_DEMAND is not confirmed guestrooms; may still support account-development qualification.",
  };
}

function hypothesizeStreams(blob) {
  const streams = [];
  if (/room|lodging|hotel|block|overnight|speaker room/i.test(blob)) streams.push("GUESTROOMS");
  if (/meeting|offsite|retreat|symposium|conference|training|kickoff/i.test(blob)) streams.push("MEETING_SPACE");
  if (/catering|banquet|reception|F&B|food/i.test(blob)) streams.push("CATERING");
  return streams.length ? streams : ["UNSPECIFIED"];
}

/**
 * Organization-led acceptance — real org + specific demand mechanism + Bethesda path.
 */
export function acceptOrganizationLedCandidate(candidate = {}, hotel = {}) {
  const org = text(candidate.organizationName);
  const mechanism = assessDemandMechanism(candidate);
  const screen = screenDiscoveryHitCommercialRelevance(candidate, hotel);
  const fit = screen.capturableFit || assessCapturableHotelDemandFit(candidate, hotel);
  const reject = [];

  if (!org || org.length < 3) reject.push("NO_REAL_ORGANIZATION");
  if (/^(hotel|hotels|companies hiring|linkedin|exa|cvent|visitmontgomery)$/i.test(org)) {
    reject.push("GENERIC_OR_PLATFORM_ORG");
  }
  if (mechanism.mechanismClass === DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND) {
    reject.push("NO_DEMAND_MECHANISM");
  }
  if (/office directory|about us|contact us|careers|hiring/i.test(`${candidate.programName || ""} ${candidate.demandSignalSummary || ""}`)
    && mechanism.mechanismClass !== DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND) {
    reject.push("DIRECTORY_OR_HIRING_ONLY");
  }
  if (/dmc|destination management|incentive/i.test(`${org} ${candidate.programName || ""}`)
    && !/hotel sourcing|preferred hotel|client|case study|program|RFP/i.test(`${candidate.programName || ""} ${candidate.demandSignalSummary || ""}`)) {
    reject.push("DMC_LANDING_WITHOUT_SOURCING_OR_PROGRAM");
  }
  if (/\b(canopy|marriott|hilton|hyatt|westin|sheraton|courtyard)\b/i.test(org)
    && /hotel|rfp|events in/i.test(`${candidate.programName || ""} ${candidate.sourceUrl || ""}`)) {
    reject.push("COMPETITOR_OR_VENUE_MARKETING");
  }
  if (!screen.pass && fit.fitClass === "EVENT_ELSEWHERE_COMMITTED_NOT_CAPTURABLE") {
    reject.push("COMMITTED_DISTANT_EVENT");
  }

  const plausibleBethesda = fit.deepResearchEligible || fit.accountDevelopmentEligible
    || /CAPTURABLE/i.test(fit.fitClass);
  if (!plausibleBethesda && reject.length === 0) reject.push("NO_PLAUSIBLE_BETHESDA_FIT");

  const ok = reject.length === 0;
  let candidateKind = V5_CANDIDATE_KIND.EXCLUDE;
  if (ok) {
    candidateKind = mechanism.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND
      ? V5_CANDIDATE_KIND.OPPORTUNITY
      : V5_CANDIDATE_KIND.ACCOUNT_INTELLIGENCE;
  } else if (org && reject.every((r) => ["NO_DEMAND_MECHANISM", "DIRECTORY_OR_HIRING_ONLY", "DMC_LANDING_WITHOUT_SOURCING_OR_PROGRAM"].includes(r))) {
    candidateKind = V5_CANDIDATE_KIND.RESEARCH_LEAD;
  }

  return {
    accepted: ok,
    candidateKind,
    rejectReasons: reject,
    mechanism,
    fit,
    screen,
    requires: ok ? {
      buyerFunctionToInvestigate: true,
      unansweredQuestionOrSalesAction: true,
    } : null,
    note: "Account intelligence without demand mechanism must not inflate NET_NEW opportunity counts.",
  };
}

/**
 * Trigger-led acceptance — verified trigger + causal hotel demand + commercial value.
 */
export function acceptTriggerLedCandidate(candidate = {}, hotel = {}) {
  const trigger = text(candidate.triggerSummary || candidate.programName || candidate.demandSignalSummary);
  const mechanism = assessDemandMechanism(candidate);
  const screen = screenDiscoveryHitCommercialRelevance(candidate, hotel);
  const fit = screen.capturableFit || assessCapturableHotelDemandFit(candidate, hotel);
  const reject = [];

  if (!trigger || trigger.length < 12) reject.push("TRIGGER_NOT_VERIFIED");
  if (!INFERRED_ACTIVITY_RE.test(trigger) && !VERIFIED_DEMAND_RE.test(trigger)) {
    reject.push("TRIGGER_LACKS_CHANGE_SIGNAL");
  }
  if (mechanism.mechanismClass === DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND) {
    reject.push("NO_CAUSAL_DEMAND_MECHANISM");
  }
  if (/hiring|careers|jobs/i.test(trigger) && !TRAVEL_OR_MEETING_INFER_RE.test(trigger)) {
    reject.push("HIRING_WITHOUT_TRAVEL_OR_TRAINING");
  }
  if (/contract award|awarded/i.test(trigger) && !TRAVEL_OR_MEETING_INFER_RE.test(`${trigger} ${candidate.demandSignalSummary || ""}`)) {
    reject.push("CONTRACT_AWARD_WITHOUT_TRAVEL_MECHANISM");
  }
  const year = Number(candidate.eventYear || (trigger.match(/\b(202[6-9]|2030)\b/) || [])[1] || 0);
  if (year && year < 2026) reject.push("TRIGGER_NOT_FORWARD_LOOKING");
  if (!fit.deepResearchEligible && !fit.accountDevelopmentEligible) {
    reject.push("BETHESDA_CANNOT_REASONABLY_COMPETE");
  }

  const ok = reject.length === 0;
  return {
    accepted: ok,
    candidateKind: ok
      ? (mechanism.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND
        ? V5_CANDIDATE_KIND.OPPORTUNITY
        : V5_CANDIDATE_KIND.ACCOUNT_INTELLIGENCE)
      : V5_CANDIDATE_KIND.EXCLUDE,
    rejectReasons: reject,
    mechanism,
    fit,
    screen,
    deepResearchEligible: ok && (fit.deepResearchEligible || mechanism.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND),
    note: "Do not interpret all business activity as hotel demand.",
  };
}

/**
 * Event-led acceptance — V4 screen + early deadline/respondent hygiene.
 */
export function acceptEventLedCandidate(candidate = {}, hotel = {}) {
  const screen = screenDiscoveryHitCommercialRelevance(candidate, hotel);
  const mechanism = assessDemandMechanism(candidate);
  const reject = [...(screen.rejectReasons || [])];
  const blob = `${candidate.programName || ""} ${candidate.demandSignalSummary || ""}`;

  if (/RFP Expired|expired\s+202[0-5]|deadline.{0,20}202[0-5]/i.test(blob)) {
    reject.push("RFP_EXPIRED");
  }
  if (/event management services|events manager to lead/i.test(blob)
    && !/hotel|venue host|meeting space|in-kind/i.test(blob)) {
    reject.push("WRONG_RESPONDENT_CLASS_NOT_HOTEL");
  }
  if (/\b(canopy|marriott|hilton|hyatt)\b/i.test(text(candidate.organizationName))
    && /hilton\.com|marriott\.com|hyatt\.com|canopy/i.test(String(candidate.sourceUrl || ""))) {
    reject.push("COMPETITOR_OR_VENUE_MARKETING");
  }
  if (/premier\s*cup|bethesdapremiercup/i.test(`${candidate.organizationName || ""} ${candidate.sourceUrl || ""}`)) {
    reject.push("BASELINE_RELATED_SPORTS_NOISE");
  }
  if (mechanism.mechanismClass === DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND) {
    reject.push("NO_DEMAND_MECHANISM");
  }

  const hardReject = [
    "RFP_EXPIRED",
    "WRONG_RESPONDENT_CLASS_NOT_HOTEL",
    "COMPETITOR_OR_VENUE_MARKETING",
    "BASELINE_RELATED_SPORTS_NOISE",
    "NO_DEMAND_MECHANISM",
    "EVENT_ELSEWHERE_COMMITTED_NOT_CAPTURABLE",
    "HOUSING_PAGE_FOR_NON_DMV_COMMITTED_EVENT",
    "NAMED_DISTANT_DESTINATION_WITHOUT_OVERFLOW_SIGNAL",
  ];
  const ok = screen.pass
    && !hardReject.some((r) => reject.includes(r))
    && mechanism.mechanismClass !== DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND;

  return {
    accepted: ok,
    candidateKind: ok ? V5_CANDIDATE_KIND.OPPORTUNITY : V5_CANDIDATE_KIND.EXCLUDE,
    rejectReasons: reject,
    mechanism,
    fit: screen.capturableFit,
    screen,
    deepResearchEligible: ok && screen.deepResearchEligible === true,
    enqueueAsResearchLeadOnly: screen.enqueueAsResearchLeadOnly === true,
  };
}

/**
 * Full V5 qualification against pathway + baseline + demand mechanism.
 */
export function qualifyV5DiscoveryCandidate(candidate = {}, hotel = {}, baseline = null) {
  const pathway = candidate.pathway || DISCOVERY_PATHWAY.EVENT_LED;
  let acceptance;
  if (pathway === DISCOVERY_PATHWAY.ORGANIZATION_LED) {
    acceptance = acceptOrganizationLedCandidate(candidate, hotel);
  } else if (pathway === DISCOVERY_PATHWAY.TRIGGER_LED) {
    acceptance = acceptTriggerLedCandidate(candidate, hotel);
  } else {
    acceptance = acceptEventLedCandidate(candidate, hotel);
  }

  let weeklyClass = WEEKLY_FINDING_CLASS.EXCLUDED;
  let countsAsNetNew = false;
  if (baseline && acceptance.accepted && acceptance.candidateKind === V5_CANDIDATE_KIND.OPPORTUNITY) {
    const clf = classifyFindingAgainstBethesdaBaseline({
      organizationName: candidate.organizationName,
      programName: candidate.programName || candidate.demandMechanism,
      sourceUrl: candidate.sourceUrl,
      segment: candidate.segment,
    }, baseline);
    weeklyClass = clf.class;
    countsAsNetNew = clf.countsAsNetNew === true;
  } else if (acceptance.accepted && acceptance.candidateKind === V5_CANDIDATE_KIND.OPPORTUNITY) {
    weeklyClass = WEEKLY_FINDING_CLASS.NET_NEW_OPPORTUNITY;
    countsAsNetNew = true;
  } else if (acceptance.candidateKind === V5_CANDIDATE_KIND.ACCOUNT_INTELLIGENCE) {
    weeklyClass = WEEKLY_FINDING_CLASS.EXCLUDED;
    countsAsNetNew = false;
  }

  const normalized = normalizeDiscoveryHit({
    ...candidate,
    signalKind: acceptance.mechanism?.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND
      ? DEMAND_SIGNAL_KIND.PROGRAM_OR_EVENT
      : DEMAND_SIGNAL_KIND.ORGANIZATION_CAPABILITY,
  }, { hotel, strategy: { segment: candidate.segment, researchQuestion: candidate.researchQuestion } });

  return {
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    pathway,
    ...acceptance,
    weeklyClass,
    countsAsNetNew,
    countsAsNetNewOpportunity: countsAsNetNew && acceptance.candidateKind === V5_CANDIDATE_KIND.OPPORTUNITY,
    normalized,
    orsEnqueueEligible: acceptance.accepted
      && (acceptance.candidateKind === V5_CANDIDATE_KIND.OPPORTUNITY
        || acceptance.candidateKind === V5_CANDIDATE_KIND.ACCOUNT_INTELLIGENCE),
    deepResearchEligible: acceptance.deepResearchEligible === true
      || (acceptance.accepted
        && acceptance.candidateKind === V5_CANDIDATE_KIND.OPPORTUNITY
        && acceptance.fit?.deepResearchEligible === true),
    hotelFacingPublicationEligible: false,
    buyerChainRequired: acceptance.accepted,
  };
}

/**
 * Funnel metrics by pathway and segment — no cost-per-actionable when yield is 0.
 */
export function buildV5DiscoveryFunnelReport({
  hits = [],
  qualifications = [],
  pathwaySpendUsd = {},
  deepResearchedIds = [],
  buyerIdentifiedIds = [],
  contactableIds = [],
  hotelFacingIds = [],
  actionableNetNewIds = [],
} = {}) {
  const byPathway = {};
  for (const p of Object.values(DISCOVERY_PATHWAY)) {
    const qRows = qualifications.filter((q) => q.pathway === p);
    const hitRows = hits.filter((h) => h.pathway === p);
    const netNew = qRows.filter((q) => q.countsAsNetNewOpportunity);
    const credible = qRows.filter((q) =>
      q.mechanism?.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND
      || q.mechanism?.mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND);
    const fit = qRows.filter((q) => /CAPTURABLE/i.test(String(q.fit?.fitClass || "")));
    const deepElig = qRows.filter((q) => q.deepResearchEligible);
    const spend = Number(pathwaySpendUsd[p] || 0);
    const actionable = actionableNetNewIds.filter((id) =>
      qRows.some((q) => (q.normalized?.discoveryId || q.candidateId) === id));
    byPathway[p] = {
      rawHits: hitRows.length,
      validOrgsPrograms: qRows.filter((q) => q.normalized?.organizationName).length,
      genuineNetNew: netNew.length,
      credibleDemandMechanisms: credible.length,
      bethesdaFit: fit.length,
      deepResearchEligible: deepElig.length,
      deeplyResearched: deepResearchedIds.filter((id) =>
        qRows.some((q) => (q.normalized?.discoveryId || q.candidateId) === id)).length,
      buyerResponsibilityIdentified: buyerIdentifiedIds.filter((id) =>
        qRows.some((q) => (q.normalized?.discoveryId || q.candidateId) === id)).length,
      usableContactRoute: contactableIds.filter((id) =>
        qRows.some((q) => (q.normalized?.discoveryId || q.candidateId) === id)).length,
      hotelFacingEligible: hotelFacingIds.filter((id) =>
        qRows.some((q) => (q.normalized?.discoveryId || q.candidateId) === id)).length,
      commerciallyActionableNetNew: actionable.length,
      spendUsd: spend,
      costPerCredibleOpportunity: credible.length
        ? Number((spend / credible.length).toFixed(4))
        : null,
      costPerContactableOpportunity: contactableIds.length
        ? Number((spend / Math.max(1, contactableIds.filter((id) =>
          qRows.some((q) => (q.normalized?.discoveryId || q.candidateId) === id)).length)).toFixed(4))
        : null,
      costPerActionableNetNew: actionable.length
        ? Number((spend / actionable.length).toFixed(4))
        : "UNDEFINED_ZERO_ACTIONABLE_YIELD",
    };
  }

  const bySegment = {};
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) {
    const qRows = qualifications.filter((q) => q.normalized?.segment === seg || q.segment === seg);
    bySegment[seg] = {
      label: RESEARCH_SEGMENT_LABEL[seg] || seg,
      qualified: qRows.length,
      netNewOpportunities: qRows.filter((q) => q.countsAsNetNewOpportunity).length,
      accountIntelligence: qRows.filter((q) => q.candidateKind === V5_CANDIDATE_KIND.ACCOUNT_INTELLIGENCE).length,
      excluded: qRows.filter((q) => q.candidateKind === V5_CANDIDATE_KIND.EXCLUDE).length,
    };
  }

  return {
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    byPathway,
    bySegment,
    note: "Do not claim cost per actionable when actionable yield is zero.",
  };
}

/**
 * Weekly plan-only orchestration — production scheduling remains disabled.
 */
export function planWeeklyTriPathDiscoveryCycle(hotel = {}, {
  feedback = null,
  priorMemory = null,
} = {}) {
  const cfg = hotel.hotelId ? (loadHotelGdiConfig(hotel.hotelId) || hotel) : hotel;
  const strategies = generateTriPathDiscoveryStrategies(cfg);
  const matrix = buildElevenSegmentStrategyMatrix(cfg);
  const sources = buildSourceFamilyStrategy();
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });

  return {
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    mode: "PLAN_ONLY",
    weeklyProductionEnabled: false,
    hotelId: cfg.hotelId || null,
    steps: [
      "load_hotel_profile",
      "load_existing_opportunities_and_research_memory",
      "apply_prior_sales_feedback",
      "run_organization_led_discovery",
      "run_trigger_led_discovery",
      "run_event_led_discovery",
      "resolve_candidate_identities",
      "assess_demand_mechanisms_and_hotel_fit",
      "deep_research_high_value_via_ORS",
      "buyer_responsibility_and_contact_routes",
      "qualify_opportunities",
      "publish_only_eligible_authorized",
      "track_net_new_reactivated_enriched",
      "schedule_follow_up_research",
    ],
    strategyCount: strategies.strategyCount,
    byPathway: strategies.byPathway,
    avoidUnchangedAccountRepeat: true,
    feedbackApplied: Boolean(feedback),
    memoryLoaded: Boolean(priorMemory),
    orsEntrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
    identityFirst: "existing create/identity-resolve before deep research",
    paidContactProviders: paidGuard,
    matrixVersion: matrix.version,
    sourceFamilies: sources.families.map((f) => f.id),
    note: "Plan only — do not activate weekly production without separate founder GO.",
  };
}

/**
 * Seed ORS queue from accepted V5 qualifications (reuses V4 enqueue helper).
 */
export function enqueueV5IntoOrsQueue(qualifications = [], hotel = {}, { existingQueue = [] } = {}) {
  const hits = qualifications
    .filter((q) => q.orsEnqueueEligible)
    .map((q) => ({
      ...q.normalized,
      pathway: q.pathway,
      demandMechanismClass: q.mechanism?.mechanismClass,
      v5CandidateKind: q.candidateKind,
    }));
  return enqueueDiscoveryIntoOrsQueue(hits, hotel, { existingQueue });
}

/**
 * Deterministic false-positive / acceptance fixtures for offline certification.
 */
export function buildV5OfflineFixtureSuite(hotel = {
  hotelId: "recLuxvwwxID7U2B8",
  displayName: "Bethesda Marriott",
  city: "Bethesda",
}) {
  const cases = [
    {
      id: "org_real_program",
      pathway: DISCOVERY_PATHWAY.ORGANIZATION_LED,
      expectAccepted: true,
      candidate: {
        organizationName: "Example Biotech Training Institute",
        programName: "Multi-day residential certification training near NIH",
        demandSignalSummary: "Offsite residential training programme with meeting space requirements Bethesda",
        segment: "corporate_meetings_training",
        sourceUrl: "https://example.org/training-residential",
      },
    },
    {
      id: "org_generic_no_mechanism",
      pathway: DISCOVERY_PATHWAY.ORGANIZATION_LED,
      expectAccepted: false,
      candidate: {
        organizationName: "Acme Corp",
        programName: "About us",
        demandSignalSummary: "Office directory Rockville",
        segment: "scaling_companies",
        sourceUrl: "https://example.org/about",
      },
    },
    {
      id: "trigger_contract_no_travel",
      pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
      expectAccepted: false,
      candidate: {
        organizationName: "GovContractor LLC",
        triggerSummary: "Awarded $50M federal contract",
        programName: "Contract award announcement",
        demandSignalSummary: "Company awarded contract",
        segment: "scaling_companies",
        sourceUrl: "https://example.org/award",
      },
    },
    {
      id: "trigger_expansion_with_training",
      pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
      expectAccepted: true,
      candidate: {
        organizationName: "ScaleUp Bio",
        triggerSummary: "Opening new Rockville facility with onboarding training offsite 2027",
        programName: "Facility expansion onboarding training offsite",
        demandSignalSummary: "New office expansion workforce training offsite lodging near Bethesda 2027",
        eventYear: "2027",
        segment: "scaling_companies",
        sourceUrl: "https://example.org/expansion-training",
      },
    },
    {
      id: "dmc_landing_only",
      pathway: DISCOVERY_PATHWAY.ORGANIZATION_LED,
      expectAccepted: false,
      candidate: {
        organizationName: "Incentive Travel House",
        programName: "Full Service Travel Company",
        demandSignalSummary: "Destination management services",
        segment: "professional_planners_dmc",
        sourceUrl: "https://example.org/dmc",
      },
    },
    {
      id: "dmc_with_sourcing",
      pathway: DISCOVERY_PATHWAY.ORGANIZATION_LED,
      expectAccepted: true,
      candidate: {
        organizationName: "DMV Meetings Partners",
        programName: "Preferred hotel sourcing programme for corporate incentives Bethesda",
        demandSignalSummary: "DMC hotel sourcing preferred hotels case study corporate meetings",
        segment: "professional_planners_dmc",
        sourceUrl: "https://example.org/dmc-hotels",
      },
    },
    {
      id: "event_venue_rfp_ok",
      pathway: DISCOVERY_PATHWAY.EVENT_LED,
      expectAccepted: true,
      candidate: {
        organizationName: "MPI Potomac Chapter",
        programName: "Educational Experiences e2 venue host RFP 2027 Bethesda Metro",
        demandSignalSummary: "Hotel or conference venue RFP meeting space catering in-kind speaker room Bethesda",
        segment: "associations_government",
        sourceUrl: "https://example.org/e2-rfp",
      },
    },
    {
      id: "event_expired_rfp",
      pathway: DISCOVERY_PATHWAY.EVENT_LED,
      expectAccepted: false,
      candidate: {
        organizationName: "ICF Metro DC",
        programName: "Event Management Services RFP",
        demandSignalSummary: "RFP Expired deadline March 2025 event management services",
        segment: "associations_government",
        sourceUrl: "https://example.org/expired",
      },
    },
    {
      id: "housing_page_fp",
      pathway: DISCOVERY_PATHWAY.EVENT_LED,
      expectAccepted: false,
      candidate: {
        organizationName: "Some Assoc",
        programName: "Hotel Information",
        demandSignalSummary: "Travel and lodging",
        sourceUrl: "https://example.org/annual-meeting/hotel-travel",
        segment: "associations_government",
      },
    },
    {
      id: "committed_distant",
      pathway: DISCOVERY_PATHWAY.EVENT_LED,
      expectAccepted: false,
      candidate: {
        organizationName: "National Society",
        programName: "Annual Meeting already contracted headquarters hotel Philadelphia Marriott Downtown",
        demandSignalSummary: "headquarters hotel Philadelphia Marriott Downtown already contracted",
        sourceUrl: "https://example.org/hotel",
        segment: "associations_government",
      },
    },
    {
      id: "local_meeting_catering",
      pathway: DISCOVERY_PATHWAY.ORGANIZATION_LED,
      expectAccepted: true,
      candidate: {
        organizationName: "Montgomery Professional Forum",
        programName: "Quarterly executive day meeting hotel catering Bethesda",
        demandSignalSummary: "Board and executive day meeting with catering at hotel conference room",
        segment: "local_meetings_catering",
        sourceUrl: "https://example.org/day-meeting",
      },
    },
    {
      id: "exhibitor_team",
      pathway: DISCOVERY_PATHWAY.EVENT_LED,
      expectAccepted: true,
      candidate: {
        organizationName: "VendorCo",
        programName: "Exhibitor team hotel lodging overflow near Bethesda conference 2027",
        demandSignalSummary: "Sponsor hospitality suite and exhibitor team hotel near Bethesda 2027",
        eventYear: "2027",
        segment: "exhibitor_sponsor_teams",
        sourceUrl: "https://example.org/exhibitor-hotel",
      },
    },
  ];

  const results = cases.map((c) => {
    const q = qualifyV5DiscoveryCandidate({ ...c.candidate, pathway: c.pathway }, hotel);
    return {
      id: c.id,
      expectAccepted: c.expectAccepted,
      accepted: q.accepted,
      pass: q.accepted === c.expectAccepted,
      candidateKind: q.candidateKind,
      rejectReasons: q.rejectReasons,
      mechanismClass: q.mechanism?.mechanismClass,
    };
  });

  return {
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    caseCount: cases.length,
    passed: results.filter((r) => r.pass).length,
    failed: results.filter((r) => !r.pass),
    results,
    allPassed: results.every((r) => r.pass),
  };
}

export function replayBethesdaStageAFalsePositives(hotel = {
  hotelId: "recLuxvwwxID7U2B8",
  displayName: "Bethesda Marriott",
  city: "Bethesda",
}) {
  const priors = [
    {
      id: "biobuzz_hiring",
      pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
      candidate: {
        organizationName: "BioBuzz",
        programName: "Companies hiring in MoCo biotech",
        triggerSummary: "Laid-off federal workers hiring opportunity",
        demandSignalSummary: "Companies hiring workforce careers",
        segment: "scaling_companies",
      },
    },
    {
      id: "canopy_competitor_rfp",
      pathway: DISCOVERY_PATHWAY.EVENT_LED,
      candidate: {
        organizationName: "Canopy by Hilton",
        programName: "Events in Bethesda RFP",
        demandSignalSummary: "Hotel RFP page",
        sourceUrl: "https://www.hilton.com/canopybethesda/rfp",
        segment: "corporate_meetings_training",
      },
    },
    {
      id: "premier_cup_baseline",
      pathway: DISCOVERY_PATHWAY.EVENT_LED,
      candidate: {
        organizationName: "Bethesda Premier Cup Hotels",
        programName: "Tournament housing",
        sourceUrl: "https://bethesdapremiercuphotels.com",
        segment: "sports",
      },
    },
    {
      id: "mpi_positive",
      pathway: DISCOVERY_PATHWAY.EVENT_LED,
      candidate: {
        organizationName: "MPI Potomac Chapter",
        programName: "e2 Educational Experiences venue host RFP meeting space catering Bethesda Metro 2027",
        demandSignalSummary: "Hotel venue RFP in-kind F&B meeting space speaker room Bethesda or Silver Spring",
        segment: "associations_government",
        sourceUrl: "https://www.mpi.org/docs/example-e2-rfp.pdf",
      },
    },
  ];

  return priors.map((p) => {
    const q = qualifyV5DiscoveryCandidate({ ...p.candidate, pathway: p.pathway }, hotel);
    return {
      id: p.id,
      accepted: q.accepted,
      candidateKind: q.candidateKind,
      rejectReasons: q.rejectReasons,
      expectedBehavior: p.id === "mpi_positive" ? "ACCEPT_PRE_SOURCING_PATTERN" : "REJECT_OR_LEAD_ONLY",
      ok: p.id === "mpi_positive" ? q.accepted === true : q.accepted === false,
    };
  });
}

export {
  assessCapturableHotelDemandFit,
  screenDiscoveryHitCommercialRelevance,
  normalizeDiscoveryHit,
  dedupeDiscoveryAgainstMemory,
  loadHotelGdiConfig,
  BETHESDA_SEGMENT_DISCOVERY_WEIGHT,
  SEGMENT_DISCOVERY_SOURCE_TYPES,
};
