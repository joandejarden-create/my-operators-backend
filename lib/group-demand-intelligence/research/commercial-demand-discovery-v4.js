/**
 * GDI V4 — Commercial Demand Discovery
 *
 * Primary objective: discover previously unknown, hotel-specific revenue
 * opportunities (demand signal → org/program → revenue → hotel fit → contact → sales action).
 *
 * Preserves V3 qualification, provenance, budget, source-independence, and publication controls.
 * ORS Research Supervisor remains the sole maturation orchestrator — this module feeds the queue.
 *
 * No paid spend unless authorizeLivePaid + budget + provider flags allow.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import {
  SUPERVISOR_EXPECTED_SEGMENTS,
  STOP_REASONS,
  canonicalizeSegmentId,
} from "./research-constants-v1.js";
import { qualifyCommercialProspectV3 } from "./commercial-qualification-v3.js";
import { questionFingerprint } from "./dynamic-research-planner-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const COMMERCIAL_DEMAND_DISCOVERY_V4 = "gdi-commercial-demand-discovery-v4";

export const DISCOVERY_TRACK = Object.freeze({
  NEW_DISCOVERY: "NEW_DISCOVERY",
  EXISTING_REFINEMENT: "EXISTING_REFINEMENT",
});

export const DEMAND_SIGNAL_KIND = Object.freeze({
  PROGRAM_OR_EVENT: "PROGRAM_OR_EVENT",
  ORGANIZATION_CAPABILITY: "ORGANIZATION_CAPABILITY",
  RECURRING_CYCLE: "RECURRING_CYCLE",
  SOURCING_ACTIVITY: "SOURCING_ACTIVITY",
});

const THREE_HOTEL_BENCHMARK = Object.freeze([
  { hotelId: "recC2kbCaS48jFjFF", displayName: "Nobu Hotel London Portman Square" },
  { hotelId: "recLuxvwwxID7U2B8", displayName: "Bethesda Marriott" },
  { hotelId: "recnPB3Q02NO8Ma1o", displayName: "Wyndham San Diego Bayside" },
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function normName(v) {
  return text(v).toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function hashKey(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 16);
}

export function loadHotelGdiConfig(hotelId, { root = ROOT } = {}) {
  const p = path.join(root, "config/group-demand-intelligence/hotels", `${hotelId}.json`);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Bethesda / DMV-weighted discovery budget — not equal across all 11 segments.
 * Underrepresented commercial channels prioritized (26-audit: 22/26 assoc+medical).
 * All 11 still receive ≥1 fresh strategy.
 */
export const BETHESDA_SEGMENT_DISCOVERY_WEIGHT = Object.freeze({
  corporate_meetings_training: 3,
  professional_planners_dmc: 3,
  scaling_companies: 3,
  local_meetings_catering: 3,
  exhibitor_sponsor_teams: 2,
  associations_government: 2,
  medical_academic: 2,
  sports: 2,
  weddings_social_private_events: 2,
  tours_educational_specialist: 2,
  entertainment_production: 2,
});

/** Source-type hints per segment — distinct channels, not eleven conference clones. */
export const SEGMENT_DISCOVERY_SOURCE_TYPES = Object.freeze({
  associations_government: ["association_rfp", "advocacy_day", "chapter_gala", "future_host_city"],
  medical_academic: ["nih_symposium", "society_site_selection", "federal_health_training", "early_cycle_rfp"],
  sports: ["tournament_housing", "team_travel", "future_cycle_stay_to_play"],
  corporate_meetings_training: ["corporate_offsite", "training_provider", "leadership_retreat", "sales_kickoff"],
  entertainment_production: ["film_permit", "tour_crew", "performing_arts_production"],
  tours_educational_specialist: ["university_delegation", "study_tour", "institutional_visit"],
  weddings_social_private_events: ["wedding_planner", "multi_day_celebration", "venue_referral_partner"],
  professional_planners_dmc: ["dmc_agency", "incentive_house", "third_party_planner", "hotel_sourcing_intermediary"],
  scaling_companies: ["workforce_expansion", "project_mobilization", "new_office_training_wave"],
  local_meetings_catering: ["executive_day_meeting", "professional_seminar", "reception_catering"],
  exhibitor_sponsor_teams: ["sponsor_hospitality", "vendor_training", "exhibitor_delegation"],
});

/**
 * Generate hotel-specific discovery strategies from profile (not frozen org names).
 * Covers all 11 canonical segments; searches for demand Bethesda (or peer) can capture —
 * not generic "hotel housing page" recall.
 */
export function generateHotelDemandDiscoveryStrategies(hotel = {}, { maxPerSegment = 3 } = {}) {
  const name = text(hotel.displayName || hotel.name || "target hotel");
  const city = text(hotel.city || hotel.locale?.note || "");
  const territory = text(hotel.demandTerritory?.label || "");
  const country = text(hotel.country || hotel.capabilityProfile?.country || "");
  const rooms = hotel.capabilityProfile?.totalGuestrooms || hotel.commercialPriorities?.coreTargetPeakRoomsMax || null;
  const meetingRooms = hotel.capabilityProfile?.meetingRoomCount || hotel.capabilityProfile?.meetingRoomsIndoor || null;
  const ballroom = hotel.capabilityProfile?.largestReceptionCapacity || hotel.capabilityProfile?.largestBallroomSqFt || null;
  const anchors = [
    ...(hotel.commercialPriorities?.demandAnchorFocus || []),
    ...(hotel.demandTerritory?.includes || []).slice(0, 4),
  ].map(text).filter(Boolean);
  const peakMin = hotel.commercialPriorities?.coreTargetPeakRoomsMin || null;
  const peakMax = hotel.commercialPriorities?.coreTargetPeakRoomsMax || rooms;
  const geo = [city, territory].filter(Boolean).join(" ") || territory || city || text(hotel.demandTerritory?.label);
  const isBethesda = /bethesda/i.test(name) || hotel.hotelId === "recLuxvwwxID7U2B8";
  const nih = anchors.find((a) => /NIH/i.test(a)) || (isBethesda ? "NIH" : "");
  const walterReed = anchors.find((a) => /Walter Reed/i.test(a)) || (isBethesda ? "Walter Reed" : "");
  const meetingHint = meetingRooms
    ? `${meetingRooms} meeting rooms`
    : rooms
      ? `${rooms} guestrooms meetings hotel`
      : "meetings hotel";

  // Distinct demand signals per segment — not eleven clones of "conference housing".
  const segmentActivityTemplates = {
    associations_government: [
      `${geo} association 2026 OR 2027 host city RFP OR venue selection (not housing page)`,
      `${geo} advocacy day OR hill day chapter lodging near ${city || "Bethesda"} Montgomery`,
      `${geo} association awards banquet OR gala venue RFP ${ballroom ? "ballroom" : "catering"}`,
    ],
    sports: [
      `${geo} tournament OR invitational visiting team hotel ${city || geo} 2026 OR 2027`,
      `${city || geo} club travel OR stay-to-play future cycle housing agency`,
      `${geo} sports staff lodging OR team travel hotel near ${anchors[0] || geo}`,
    ],
    corporate_meetings_training: [
      `${city || "Bethesda"} OR Rockville OR "Montgomery County" corporate offsite OR leadership retreat hotel ${peakMin || 50}-${peakMax || 200} rooms`,
      `${geo} sales kickoff OR regional management meeting hotel ${meetingHint}`,
      `${geo} professional certification OR train-the-trainer residential programme hotel`,
      `${city || geo} corporate training provider multi-day meeting hotel near NIH OR metro`,
    ],
    entertainment_production: [
      `${geo} film OR television production crew hotel OR basecamp lodging ${city || "Maryland"}`,
      `${city || geo} touring production OR performing arts technical crew hotel block`,
      `${geo} Maryland film office OR production temporary lodging near Bethesda`,
    ],
    medical_academic: [
      `${nih || "NIH"} symposium OR scientific meeting 2026 OR 2027 venue OR site selection Bethesda OR Rockville`,
      `${walterReed || "Walter Reed"} OR federal health training offsite hotel Montgomery`,
      `${geo} medical society regional meeting early RFP OR site selection (not official housing tab)`,
    ],
    tours_educational_specialist: [
      `${geo} university delegation OR academic visit hotel ${city || "Bethesda"} 2026 OR 2027`,
      `${city || geo} study tour OR educational travel group lodging near NIH OR Smithsonian`,
      `${geo} institutional visit OR specialist education programme residential hotel`,
    ],
    weddings_social_private_events: [
      `${city || "Bethesda"} wedding planner preferred hotel OR multi-day celebration guest rooms`,
      `${city || geo} social celebration OR private event venue referral partner lodging`,
      `${city || geo} wedding catering OR reception hotel ${ballroom ? "ballroom" : "banquet"} guestroom block optional`,
    ],
    professional_planners_dmc: [
      `${geo} destination management company Washington OR Bethesda corporate meetings incentives`,
      `${geo} third-party meeting planner hotel sourcing DMV OR Montgomery County`,
      `${geo} incentive travel house OR corporate event agency hotel partners Bethesda`,
      `${city || "Washington"} DMC supplier hotel shortlist OR preferred hotel programme`,
    ],
    scaling_companies: [
      `${city || "Bethesda"} OR Rockville biotech OR tech hiring surge OR new office training wave hotel`,
      `${geo} project mobilization OR long-stay project team hotel near ${city || "Bethesda"}`,
      `${geo} corporate relocation OR facility expansion employee lodging OR offsite`,
      `${city || geo} scale-up company recurring team gathering hotel meetings`,
    ],
    local_meetings_catering: [
      `${city || "Bethesda"} executive day meeting OR board meeting hotel conference room catering`,
      `${city || geo} professional seminar OR training day banquet hotel (meeting space primary)`,
      `${city || geo} institutional reception OR private corporate function hotel catering`,
      `${city || "Montgomery County"} chamber OR association luncheon venue hotel`,
    ],
    exhibitor_sponsor_teams: [
      `${geo} conference sponsor hospitality suite OR exhibitor team hotel near ${city || "Bethesda"}`,
      `${geo} vendor training programme accompanying medical OR association conference DMV`,
      `${nih || geo} exhibitor OR sponsor lodging overflow (not downtown-only committed block)`,
    ],
  };

  const weightMap = isBethesda ? BETHESDA_SEGMENT_DISCOVERY_WEIGHT : null;
  const strategies = [];
  for (const segment of SUPERVISOR_EXPECTED_SEGMENTS) {
    const templates = segmentActivityTemplates[segment] || [
      `${geo} ${segment.replace(/_/g, " ")} group meeting OR hotel demand ${city || ""}`,
    ];
    const weight = weightMap?.[segment] || 1;
    const take = Math.min(maxPerSegment, Math.max(1, weight));
    for (const q of templates.slice(0, take)) {
      const question = text(q);
      if (!question) continue;
      // Reject legacy housing-page-only query shapes
      const housingOnlyQuery = /\bhotel housing OR room block\b/i.test(question)
        && !/venue selection|RFP|site selection|offsite|banquet|DMC|overflow/i.test(question);
      strategies.push({
        segment,
        researchQuestion: question,
        questionFingerprint: questionFingerprint(question),
        demandSeeking: true,
        housingPageExtractionOnly: housingOnlyQuery,
        discoveryWeight: weight,
        underrepresentedPriority: isBethesda && weight >= 3,
        sourceTypes: SEGMENT_DISCOVERY_SOURCE_TYPES[segment] || [],
        hotelName: name,
        geo,
        inventoryHints: {
          guestrooms: rooms,
          meetingRooms,
          ballroomReception: ballroom,
          peakRoomsMin: peakMin,
          peakRoomsMax: peakMax,
        },
        anchors: anchors.slice(0, 6),
        expectedSignalKinds: [
          DEMAND_SIGNAL_KIND.PROGRAM_OR_EVENT,
          DEMAND_SIGNAL_KIND.ORGANIZATION_CAPABILITY,
          DEMAND_SIGNAL_KIND.RECURRING_CYCLE,
          DEMAND_SIGNAL_KIND.SOURCING_ACTIVITY,
        ],
        track: DISCOVERY_TRACK.NEW_DISCOVERY,
      });
    }
  }

  return {
    version: COMMERCIAL_DEMAND_DISCOVERY_V4,
    hotelId: hotel.hotelId || null,
    hotelName: name,
    strategyCount: strategies.length,
    segmentsCovered: [...new Set(strategies.map((s) => s.segment))],
    expectedSegmentCount: SUPERVISOR_EXPECTED_SEGMENTS.length,
    allElevenSegmentsCovered: SUPERVISOR_EXPECTED_SEGMENTS.every((s) =>
      strategies.some((x) => x.segment === s)),
    weightedForBethesda: isBethesda,
    underrepresentedSegmentsPrioritized: isBethesda
      ? Object.entries(BETHESDA_SEGMENT_DISCOVERY_WEIGHT)
        .filter(([, w]) => w >= 3)
        .map(([s]) => s)
      : [],
    sourceTypesBySegment: SEGMENT_DISCOVERY_SOURCE_TYPES,
    strategies,
    note: "Strategies seek demand-generating activities and open sourcing signals from hotel profile — housing pages are evidence artifacts, not automatic opportunities. Underrepresented Bethesda segments get higher discovery weight.",
  };
}

/**
 * Early hotel-fit / capturable-demand screen before expensive deep research.
 * Distinguishes potentially capturable Bethesda demand from "event elsewhere uses hotels".
 * Does NOT reject account-development solely for missing RFP/room block.
 */
export function assessCapturableHotelDemandFit(hit = {}, hotel = {}) {
  const city = text(hotel.city || "");
  const isBethesda = /bethesda/i.test(hotel.displayName || hotel.name || "")
    || hotel.hotelId === "recLuxvwwxID7U2B8";
  const blob = [
    hit.organizationName,
    hit.programName,
    hit.title,
    hit.demandSignalSummary,
    hit.eventLocationSummary,
    hit.namedHostHotel,
    hit.sourceUrl,
  ].map(text).join(" ");

  const coreGeo = isBethesda
    && /\b(Bethesda|Rockville|North Bethesda|Pooks Hill|NIH|Walter Reed|Montgomery County|Chevy Chase|Silver Spring)\b/i.test(blob);
  const dmvGeo = isBethesda
    && /\b(Washington|DC|District of Columbia|Arlington|Alexandria|Fairfax|College Park|DMV)\b/i.test(blob);
  const distantCommitted = /\b(Philadelphia|Las Vegas|Orlando|Chicago|San Francisco|Boston|Atlanta|Dallas|Seattle|Denver|National Harbor|Gaylord)\b/i.test(blob)
    && /(?:headquarters?\s+hotel|already\s+contracted|host\s+hotel|at\s+the\s+\w+\s+(?:marriott|hilton|hyatt|ritz))/i.test(blob);
  const housingTabOnly = /\/hotel|housing|hotel-travel|hoteltravel/i.test(String(hit.sourceUrl || ""))
    && !/\b(RFP|venue selection|site selection|offsite|seeking hotels|sourcing)\b/i.test(blob);
  const accountDevelopmentOk = /\b(DMC|meeting planner|incentive|offsite|training|retreat|recurring|chapter|preferred hotel|supplier)\b/i.test(blob)
    && !distantCommitted;

  let classLabel = "INSUFFICIENT_GEO_EVIDENCE";
  if (distantCommitted && !/overflow|satellite|Beltway|Montgomery/i.test(blob)) {
    classLabel = "EVENT_ELSEWHERE_COMMITTED_NOT_CAPTURABLE";
  } else if (coreGeo) {
    classLabel = "POTENTIALLY_CAPTURABLE_CORE";
  } else if (dmvGeo || accountDevelopmentOk) {
    classLabel = "POTENTIALLY_CAPTURABLE_DMV_OR_ACCOUNT";
  } else if (housingTabOnly) {
    classLabel = "HOUSING_TAB_EVIDENCE_ONLY_NOT_BETHESDA_DEMAND";
  }

  const deepResearchEligible = [
    "POTENTIALLY_CAPTURABLE_CORE",
    "POTENTIALLY_CAPTURABLE_DMV_OR_ACCOUNT",
  ].includes(classLabel);

  return {
    version: COMMERCIAL_DEMAND_DISCOVERY_V4,
    fitClass: classLabel,
    deepResearchEligible,
    accountDevelopmentEligible: accountDevelopmentOk && classLabel !== "EVENT_ELSEWHERE_COMMITTED_NOT_CAPTURABLE",
    housingTabOnly,
    coreGeo,
    dmvGeo,
    note: "Missing RFP/room block alone does not exclude account-development; committed distant host without overflow does.",
  };
}

/**
 * Screen a discovery hit before ORS enqueue — housing pages alone ≠ Bethesda-fit demand.
 */
export function screenDiscoveryHitCommercialRelevance(hit = {}, hotel = {}) {
  const url = text(hit.sourceUrl || hit.url);
  const title = text(hit.title || hit.programName || "");
  const org = text(hit.organizationName || hit.organization);
  const blob = `${org} ${title} ${hit.demandSignalSummary || ""} ${url}`;
  const city = text(hotel.city || "");
  const isBethesda = /bethesda/i.test(hotel.displayName || hotel.name || "")
    || hotel.hotelId === "recLuxvwwxID7U2B8";
  const housingPage = /\/hotel|housing|hotel-travel|hoteltravel|exhibitor[_-]?housing/i.test(url);
  const namedDistantHost = /\b(Philadelphia|Las Vegas|Orlando|Chicago|San Francisco|Boston|Atlanta|Dallas|Seattle|Denver)\b/i.test(blob)
    && city
    && !new RegExp(city, "i").test(blob);
  const parkingOrDirectionsOnly = /parking|directions|miscellaneous information/i.test(title)
    && !/hotel|housing|room\s*block|meeting|symposium programme/i.test(`${title} ${hit.demandSignalSummary || ""}`);
  const sampleContractPdf = /sample\.pdf|DocA\d+/i.test(url) && /BLANKET PURCHASE|BPA/i.test(title);
  const fit = assessCapturableHotelDemandFit(hit, hotel);

  const reasons = [];
  if (parkingOrDirectionsOnly) reasons.push("PARKING_OR_LOGISTICS_PAGE_NOT_DEMAND");
  if (sampleContractPdf) reasons.push("GENERIC_SAMPLE_CONTRACT_NOT_PROGRAM");
  if (namedDistantHost && !/overflow|satellite|DMV|Washington|Bethesda|Rockville|Montgomery/i.test(blob)) {
    reasons.push("NAMED_DISTANT_DESTINATION_WITHOUT_OVERFLOW_SIGNAL");
  }
  if (housingPage && !org) reasons.push("HOUSING_PAGE_WITHOUT_ORGANIZATION");
  if (isBethesda && housingPage && namedDistantHost) {
    reasons.push("HOUSING_PAGE_FOR_NON_DMV_COMMITTED_EVENT");
  }
  if (fit.fitClass === "EVENT_ELSEWHERE_COMMITTED_NOT_CAPTURABLE") {
    reasons.push("EVENT_ELSEWHERE_COMMITTED_NOT_CAPTURABLE");
  }

  const pass = reasons.length === 0
    || (reasons.length === 1 && reasons[0] === "HOUSING_PAGE_WITHOUT_ORGANIZATION" && Boolean(url));
  const enqueueAsResearchLeadOnly = (housingPage || reasons.includes("HOUSING_PAGE_WITHOUT_ORGANIZATION"))
    && fit.fitClass !== "EVENT_ELSEWHERE_COMMITTED_NOT_CAPTURABLE";

  return {
    version: COMMERCIAL_DEMAND_DISCOVERY_V4,
    pass: pass && !parkingOrDirectionsOnly && !sampleContractPdf
      && fit.fitClass !== "EVENT_ELSEWHERE_COMMITTED_NOT_CAPTURABLE",
    rejectReasons: reasons,
    housingPageEvidence: housingPage,
    enqueueAsResearchLeadOnly: enqueueAsResearchLeadOnly && pass,
    capturableFit: fit,
    deepResearchEligible: fit.deepResearchEligible,
    note: "Housing pages may enter research queue as evidence leads; they are not hotel-facing opportunities until identity, geo fit, and buyer route are established. Screen committed distant events before deep research.",
  };
}

/**
 * Adjacent-demand leads from an existing opportunity (exhibitor, planner, future cycle).
 * Does NOT auto-create NET_NEW — requires separate identity resolution + baseline reconcile.
 */
export function planAdjacentDemandLeadsFromOpportunity(opp = {}, { maxLeads = 5 } = {}) {
  const org = text(opp.organizationName);
  const title = text(opp.title || opp.canonicalEventName || opp.eventName);
  const segment = canonicalizeSegmentId(opp.segment || opp.researchSegment || "");
  const leads = [];
  const push = (kind, question, targetSegment) => {
    if (leads.length >= maxLeads) return;
    leads.push({
      adjacentKind: kind,
      researchQuestion: question,
      suggestedSegment: targetSegment,
      parentOpportunityId: opp.id || opp.opportunityId || null,
      parentOrganization: org || null,
      countsAsNetNewAutomatically: false,
      note: "Investigate only if commercially justified; reconcile against baseline before counting NET_NEW.",
    });
  };

  if (/medical_academic|associations_government/.test(segment) || /conference|meeting|symposium|annual/i.test(title)) {
    push(
      "EXHIBITOR_SPONSOR_TEAMS",
      `${org || title} exhibitor OR sponsor hospitality OR vendor team lodging (distinct from host conference)`,
      "exhibitor_sponsor_teams",
    );
    push(
      "PROFESSIONAL_PLANNER",
      `${org || title} official housing bureau OR third-party meeting planner OR AMC hotel sourcing`,
      "professional_planners_dmc",
    );
  }
  if (/university|college|academic|trusteeship/i.test(`${org} ${title}`)) {
    push(
      "EDUCATIONAL_GROUP",
      `${org} educational travel OR institutional visit OR delegation hotel (distinct programme)`,
      "tours_educational_specialist",
    );
  }
  if (/sport|soccer|tournament|showcase/i.test(`${org} ${title}`) || segment === "sports") {
    push(
      "FUTURE_TOURNAMENT_CYCLE",
      `${org} 2027 OR 2028 tournament housing OR stay-to-play (next cycle, not same event)`,
      "sports",
    );
  }
  if (/nih|nhlbi|scientific|medical/i.test(`${org} ${title}`)) {
    push(
      "RELATED_FEDERAL_TRAINING",
      `${org} training offsite OR project team residential meeting near Bethesda (not same symposium)`,
      "corporate_meetings_training",
    );
  }

  return {
    version: COMMERCIAL_DEMAND_DISCOVERY_V4,
    parentOpportunityId: opp.id || opp.opportunityId || null,
    leadCount: leads.length,
    leads,
    warning: "Parent conference/organizer/sponsors are not automatically new opportunities.",
  };
}

/**
 * Normalize a discovery hit into a demand-signal candidate (not yet matured).
 */
export function normalizeDiscoveryHit(hit = {}, { hotel = {}, strategy = null } = {}) {
  const organizationName = text(hit.organizationName || hit.organization || hit.org);
  const programName = text(hit.programName || hit.eventName || hit.meetingName || hit.programmeName);
  const signalKind = hit.signalKind
    || (programName ? DEMAND_SIGNAL_KIND.PROGRAM_OR_EVENT : DEMAND_SIGNAL_KIND.ORGANIZATION_CAPABILITY);
  const key = hashKey(`${normName(organizationName)}::${normName(programName)}::${strategy?.segment || hit.segment || ""}`);
  return {
    discoveryId: `disc_${key}`,
    track: DISCOVERY_TRACK.NEW_DISCOVERY,
    discoverySource: "commercial_demand_discovery_v4",
    organizationName: organizationName || null,
    programName: programName || null,
    signalKind,
    segment: canonicalizeSegmentId(hit.segment || strategy?.segment) || null,
    researchQuestion: strategy?.researchQuestion || hit.researchQuestion || null,
    demandSignalSummary: text(hit.demandSignalSummary || hit.summary || hit.snippet),
    sourceUrl: hit.sourceUrl || hit.url || null,
    publisher: hit.publisher || hit.sourcePublisher || null,
    independentPublisher: hit.independentPublisher === true,
    contactHint: hit.contactEmail || hit.contact || hit.contactHint || null,
    namedHostHotel: hit.namedHostHotel || null,
    hotelId: hotel.hotelId || null,
    hotelName: hotel.displayName || hotel.name || null,
    raw: hit,
  };
}

/**
 * Deduplicate discovery hits against research memory (known orgs + prior discovery ids).
 */
export function dedupeDiscoveryAgainstMemory(hits = [], memory = {}) {
  const knownOrgs = new Set(
    [...(memory.knownOrganizations || []), ...(memory.investigatedOrganizations || [])]
      .map(normName)
      .filter(Boolean),
  );
  const knownPrograms = new Set(
    (memory.knownPrograms || []).map(normName).filter(Boolean),
  );
  const knownIds = new Set((memory.discoveryIds || []).map(String));
  const seen = new Set();
  const unique = [];
  const duplicates = [];

  for (const hit of hits) {
    const orgKey = normName(hit.organizationName);
    const progKey = normName(hit.programName);
    const id = hit.discoveryId || hashKey(`${orgKey}::${progKey}`);
    if (knownIds.has(id) || seen.has(id)) {
      duplicates.push({ hit, reason: "duplicate_discovery_id" });
      continue;
    }
    if (orgKey && knownOrgs.has(orgKey) && !progKey) {
      duplicates.push({ hit, reason: "organization_already_in_memory" });
      continue;
    }
    if (orgKey && progKey && knownOrgs.has(orgKey) && knownPrograms.has(progKey)) {
      duplicates.push({ hit, reason: "org_program_already_in_memory" });
      continue;
    }
    seen.add(id);
    unique.push({ ...hit, discoveryId: id, deduped: true });
  }

  return { unique, duplicates, knownOrgCount: knownOrgs.size };
}

/**
 * Score commercial value of information for prioritization.
 */
export function scoreDiscoveryCommercialValue(hit = {}, hotel = {}) {
  let score = 0;
  const blob = `${hit.demandSignalSummary || ""} ${hit.programName || ""} ${hit.organizationName || ""}`;
  if (hit.programName) score += 25;
  if (hit.organizationName) score += 15;
  if (hit.contactHint && /@/.test(String(hit.contactHint))) score += 20;
  if (hit.independentPublisher) score += 12;
  if (/hotel[\s-]*sourc|room\s*block|housing|incentive|banquet|annual\s+meeting|conference|DMC/i.test(blob)) score += 18;
  if (hit.namedHostHotel) score -= 8; // committed elsewhere — still valuable as future-cycle watch
  const city = text(hotel.city || "");
  if (city && new RegExp(city.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(blob)) score += 10;
  const rooms = hotel.capabilityProfile?.totalGuestrooms;
  if (rooms && /guestroom|bedroom|room\s*block|housing|overnight/i.test(blob)) score += 8;
  if (hotel.capabilityProfile?.meetingRoomCount || hotel.capabilityProfile?.largestReceptionCapacity) {
    if (/banquet|ballroom|meeting|catering|gala/i.test(blob)) score += 8;
  }
  return Math.max(0, Math.min(100, score));
}

/**
 * Prioritize unique hits; connect demand → org/program → revenue hypothesis → fit → contact.
 */
export function prioritizeDiscoveryHits(hits = [], hotel = {}, { maxInvestigate = 12 } = {}) {
  const ranked = hits
    .map((h) => ({
      ...h,
      commercialValueScore: scoreDiscoveryCommercialValue(h, hotel),
      revenueHypothesis: hypothesizeRevenueStreams(h, hotel),
      hotelFitHypothesis: hypothesizeHotelFit(h, hotel),
    }))
    .sort((a, b) => (b.commercialValueScore - a.commercialValueScore)
      || text(a.organizationName).localeCompare(text(b.organizationName)));

  return {
    ranked,
    toInvestigate: ranked.slice(0, maxInvestigate),
    deferred: ranked.slice(maxInvestigate),
  };
}

export function hypothesizeRevenueStreams(hit = {}, hotel = {}) {
  const blob = `${hit.demandSignalSummary || ""} ${hit.programName || ""} ${hit.signalKind || ""}`;
  return {
    GUESTROOMS: /room\s*block|housing|overnight|incentive|residential|multi[- ]?day|hotel[\s-]*sourc|accommodation/i.test(blob),
    MEETING_SPACE: /meeting|boardroom|breakout|conference|training|congress|summit/i.test(blob),
    CATERING_AND_EVENTS: /banquet|gala|awards?\s+dinner|catering|reception|luncheon/i.test(blob),
    note: "Hypothesis only — V3 qualification after evidence confirms or rejects.",
  };
}

export function hypothesizeHotelFit(hit = {}, hotel = {}) {
  const name = text(hotel.displayName || hotel.name);
  const city = text(hotel.city || "");
  const blob = `${hit.demandSignalSummary || ""} ${hit.programName || ""}`;
  const geoMatch = !city || new RegExp(city.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(blob + " " + (hit.raw?.city || ""));
  return {
    targetHotel: name,
    geographyPlausible: geoMatch,
    note: geoMatch
      ? `Demand geography appears compatible with ${name} catchment.`
      : `Geography not yet evidenced for ${name} — require source confirmation.`,
  };
}

/**
 * Convert prioritized discovery hits into ORS queue opportunities (canonical shape).
 */
export function enqueueDiscoveryIntoOrsQueue(hits = [], hotel = {}, { existingQueue = [] } = {}) {
  const existingKeys = new Set(
    existingQueue.map((o) => normName(o.organizationName || o.name)).filter(Boolean),
  );
  const entered = [];
  const skipped = [];

  for (const hit of hits) {
    const orgKey = normName(hit.organizationName);
    if (!orgKey) {
      skipped.push({ hit, reason: "missing_organization" });
      continue;
    }
    if (existingKeys.has(orgKey) && !hit.programName) {
      skipped.push({ hit, reason: "already_in_ors_queue" });
      continue;
    }
    existingKeys.add(orgKey);
    const opportunity = {
      id: hit.discoveryId,
      opportunityId: hit.discoveryId,
      organizationName: hit.organizationName,
      name: hit.organizationName,
      eventName: hit.programName || null,
      programmeName: hit.programName || null,
      primarySegment: hit.segment,
      segment: hit.segment,
      discoverySource: "commercial_demand_discovery_v4",
      discoveryTrack: DISCOVERY_TRACK.NEW_DISCOVERY,
      demandSignalSummary: hit.demandSignalSummary,
      researchQuestion: hit.researchQuestion,
      officialSource: hit.sourceUrl || null,
      contactEmail: hit.contactHint || null,
      contact: hit.contactHint ? { email: hit.contactHint } : {},
      namedHostHotel: hit.namedHostHotel || null,
      hotelId: hotel.hotelId,
      hotelName: hotel.displayName || hotel.name,
      seedUrls: hit.sourceUrl ? [hit.sourceUrl] : [],
      commercialValueScore: hit.commercialValueScore,
      revenueHypothesis: hit.revenueHypothesis,
      hotelFitHypothesis: hit.hotelFitHypothesis,
      independentPublisher: hit.independentPublisher === true,
      publisher: hit.publisher || null,
      signalKind: hit.signalKind || null,
      notes: ["entered_candidate_queue:commercial_demand_discovery_v4"],
      researchAttempts: [],
      unresolvedLeads: [],
      revisitSchedule: null,
    };
    entered.push(opportunity);
  }

  return {
    entered,
    skipped,
    queueDelta: entered.length,
    track: DISCOVERY_TRACK.NEW_DISCOVERY,
  };
}

/**
 * When a public org-path is exhausted, plan independent / program-specific searches.
 * PUBLIC_PATH_EXHAUSTED ≠ all commercially relevant investigation ended.
 */
export function planIndependentSourceExpansion({
  opportunity = {},
  hotel = {},
  unresolvedQuestions = [],
  paidBlocked = true,
  parallelAvailable = false,
  remainingBudgetUsd = 0,
} = {}) {
  const org = text(opportunity.organizationName || opportunity.name);
  const program = text(opportunity.eventName || opportunity.programmeName || opportunity.meetingName);
  const city = text(hotel.city || hotel.demandTerritory?.label || "");
  const strategies = [];

  if (org) {
    strategies.push({
      kind: "independent_publisher_search",
      researchQuestion: `Independent press or CVB coverage of ${org}${program ? ` ${program}` : ""} hotel OR housing OR venue (not ${org} website)`,
      provider: "exa_contents",
      paid: true,
    });
    strategies.push({
      kind: "program_specific_search",
      researchQuestion: program
        ? `${program} ${city} host hotel OR room block OR venue selection`
        : `${org} upcoming ${city} meeting OR banquet OR incentive programme hotel`,
      provider: "exa_contents",
      paid: true,
    });
    strategies.push({
      kind: "related_entity_search",
      researchQuestion: `${org} partner DMC OR housing bureau OR venue partner ${city}`,
      provider: "direct_public",
      paid: false,
    });
  }

  for (const q of unresolvedQuestions.slice(0, 3)) {
    strategies.push({
      kind: "unresolved_question",
      researchQuestion: text(q),
      provider: "exa_contents",
      paid: true,
    });
  }

  const canExecutePaid = paidBlocked !== true
    && Number(remainingBudgetUsd) > 0.01
    && (parallelAvailable === true || process.env.GDI_EXA_ENABLED === "1");

  const executableNow = strategies.filter((s) => !s.paid || canExecutePaid);
  const blocked = strategies.filter((s) => s.paid && !canExecutePaid);
  const highValueBlocked = blocked.filter((s) =>
    s.kind === "independent_publisher_search"
    || s.kind === "program_specific_search"
    || s.kind === "unresolved_question");

  let stopReasonIfCannotContinue = null;
  if (!strategies.length) {
    stopReasonIfCannotContinue = STOP_REASONS.RESEARCH_EXHAUSTED;
  } else if (highValueBlocked.length && !canExecutePaid) {
    // Commercially material independent/program searches cannot run — report
    // provider/auth limitation even if a weak unpaid related-entity path remains.
    stopReasonIfCannotContinue = STOP_REASONS.PROVIDER_RESTRICTED_NOT_EXHAUSTION;
  } else if (!executableNow.length && blocked.length) {
    stopReasonIfCannotContinue = STOP_REASONS.PROVIDER_RESTRICTED_NOT_EXHAUSTION;
  }

  return {
    version: COMMERCIAL_DEMAND_DISCOVERY_V4,
    publicPathExhaustedMeans: "current_organization_owned_source_path_only",
    notEquivalentToFullInvestigationEnd: true,
    strategies,
    executableNow,
    blockedByAuthorizationOrProvider: blocked.map((s) => ({
      ...s,
      limitation: paidBlocked
        ? "paid_blocked_by_authorization"
        : "provider_or_budget_unavailable",
    })),
    highValueSearchesBlocked: highValueBlocked.length > 0,
    stopReasonIfCannotContinue,
    note: blocked.length
      ? "Report provider/authorization limitation — do not claim research exhaustion."
      : "Authorized follow-up searches should execute, not merely queue.",
  };
}

/**
 * Attach expansion plan onto an ORS opportunity after PUBLIC_PATH_EXHAUSTED.
 */
export function applyDiscoveryExpansionAfterPublicPathExhausted(opportunity = {}, hotel = {}, opts = {}) {
  const plan = planIndependentSourceExpansion({
    opportunity,
    hotel,
    unresolvedQuestions: opportunity.unresolvedQuestions || opportunity.researchGaps || [],
    paidBlocked: opts.paidBlocked !== false,
    parallelAvailable: opts.parallelAvailable === true,
    remainingBudgetUsd: opts.remainingBudgetUsd ?? 0,
  });

  const next = {
    ...opportunity,
    discoveryExpansionV4: plan,
    stopDetail: {
      ...(opportunity.stopDetail || {}),
      publicPathExhaustedClarification: plan.publicPathExhaustedMeans,
      independentSourceExpansionPending: plan.executableNow.length > 0,
      providerLimitation: plan.blockedByAuthorizationOrProvider.length
        ? plan.blockedByAuthorizationOrProvider[0].limitation
        : null,
    },
  };

  if (plan.stopReasonIfCannotContinue === STOP_REASONS.PROVIDER_RESTRICTED_NOT_EXHAUSTION) {
    next.pendingDiscoverySearches = plan.executableNow;
    next.stopReasonSuggested = STOP_REASONS.PROVIDER_RESTRICTED_NOT_EXHAUSTION;
  } else if (plan.executableNow.length) {
    next.pendingDiscoverySearches = plan.executableNow;
    next.stopReasonSuggested = STOP_REASONS.RELATED_ENTITY_FOLLOWUPS_PENDING;
  } else if (plan.stopReasonIfCannotContinue) {
    next.stopReasonSuggested = plan.stopReasonIfCannotContinue;
  }

  return { opportunity: next, plan };
}

/**
 * Resolve unresolved leads + revisit schedule for deferred discovery.
 */
export function preserveUnresolvedDiscoveryLeads(deferred = [], { revisitDays = 14 } = {}) {
  const revisitAt = new Date(Date.now() + revisitDays * 86400000).toISOString();
  return deferred.map((hit) => ({
    ...hit,
    unresolved: true,
    revisitSchedule: { revisitAt, reason: "commercial_voi_deferred_below_investigate_cap" },
  }));
}

/**
 * Run one discovery cycle (adapters injectable for offline mocks).
 * Does not call paid providers unless authorizeLivePaid.
 */
export async function runCommercialDemandDiscoveryCycleV4({
  hotelId,
  hotel = null,
  memory = {},
  existingQueue = [],
  searchAdapter = null,
  authorizeLivePaid = false,
  maxInvestigate = 12,
  maxStrategies = 24,
} = {}) {
  const cfg = hotel || loadHotelGdiConfig(hotelId) || { hotelId, displayName: hotelId };
  if (!cfg.hotelId) cfg.hotelId = hotelId;

  const strategyPack = generateHotelDemandDiscoveryStrategies(cfg);
  const strategies = strategyPack.strategies.slice(0, maxStrategies);

  const rawHits = [];
  const searchLog = [];
  for (const strategy of strategies) {
    if (typeof searchAdapter !== "function") {
      searchLog.push({
        strategy: strategy.researchQuestion,
        status: "adapter_missing",
        hits: 0,
      });
      continue;
    }
    if (strategy.paid && !authorizeLivePaid) {
      searchLog.push({
        strategy: strategy.researchQuestion,
        status: "paid_not_authorized",
        hits: 0,
      });
      continue;
    }
    const result = await searchAdapter({ strategy, hotel: cfg, authorizeLivePaid });
    const hits = (result?.hits || []).map((h) => normalizeDiscoveryHit(h, { hotel: cfg, strategy }));
    rawHits.push(...hits);
    searchLog.push({
      strategy: strategy.researchQuestion,
      segment: strategy.segment,
      status: result?.status || "ok",
      hits: hits.length,
      costUsd: Number(result?.costUsd || 0),
    });
  }

  const { unique, duplicates } = dedupeDiscoveryAgainstMemory(rawHits, memory);
  const prioritized = prioritizeDiscoveryHits(unique, cfg, { maxInvestigate });
  const enqueued = enqueueDiscoveryIntoOrsQueue(prioritized.toInvestigate, cfg, { existingQueue });
  const unresolved = preserveUnresolvedDiscoveryLeads(prioritized.deferred);

  const spentUsd = searchLog.reduce((s, r) => s + Number(r.costUsd || 0), 0);

  return {
    version: COMMERCIAL_DEMAND_DISCOVERY_V4,
    hotelId: cfg.hotelId,
    hotelName: cfg.displayName || cfg.name,
    track: DISCOVERY_TRACK.NEW_DISCOVERY,
    strategyPack: {
      strategyCount: strategyPack.strategyCount,
      segmentsCovered: strategyPack.segmentsCovered,
      allElevenSegmentsCovered: strategyPack.allElevenSegmentsCovered,
    },
    searchLog,
    rawHitCount: rawHits.length,
    uniqueNewCount: unique.length,
    duplicateCount: duplicates.length,
    investigatedCap: maxInvestigate,
    enqueued: enqueued.entered,
    enqueueSkipped: enqueued.skipped,
    unresolvedLeads: unresolved,
    spentUsd,
    authorizeLivePaid: authorizeLivePaid === true,
    coverageClaim: {
      comprehensive: false,
      denominator: "hotel_profile_generated_strategies_executed",
      strategiesExecuted: searchLog.filter((s) => s.status === "ok").length,
      strategiesTotal: strategies.length,
      note: "Do not claim comprehensive segment coverage without a defensible universe denominator.",
    },
  };
}

/**
 * Qualify a discovered opportunity with V3 after evidence assembly.
 */
export function qualifyDiscoveredOpportunityV4({
  opportunity = {},
  acceptedClaims = [],
  pageTexts = [],
  targetHotel = "",
  usableContactRoute = null,
} = {}) {
  const v3 = qualifyCommercialProspectV3({
    opportunity,
    acceptedClaims,
    pageTexts,
    targetHotel: targetHotel || opportunity.hotelName,
    usableContactRoute: usableContactRoute || opportunity.contactEmail,
  });
  return {
    ...v3,
    discoveryTrack: opportunity.discoveryTrack || DISCOVERY_TRACK.NEW_DISCOVERY,
    discoverySource: opportunity.discoverySource || "commercial_demand_discovery_v4",
    programSpecific: Boolean(opportunity.eventName || opportunity.programmeName),
    accountDevelopmentLead: v3.internalDisposition === "ACCOUNT_DEVELOPMENT",
  };
}

export function threeHotelDiscoveryBenchmarkScope() {
  return THREE_HOTEL_BENCHMARK.map((h) => ({
    ...h,
    config: loadHotelGdiConfig(h.hotelId),
  }));
}

export { THREE_HOTEL_BENCHMARK };
