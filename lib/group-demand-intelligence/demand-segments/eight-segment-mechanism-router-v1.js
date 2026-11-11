/**
 * Twelve-segment demand-mechanism router V1
 * (extends eight-segment surface; EIGHT_* exports retained for compatibility)
 *
 * Does NOT migrate historical records. Dual opportunities require distinct mechanisms.
 * Compatibility: still returns primaryResearchSegment for legacy runners.
 */

import {
  DEMAND_MECHANISM_CLASS,
  EVENT_SUBTYPE,
  routeDemandSegmentByMechanism as routeBinary,
} from "../research/demand-segment-mechanism-router-v1.js";
import { RESEARCH_SEGMENT } from "../workflow/hotel-segment-research-plan-v1.js";
import {
  DEMAND_SEGMENT,
  EIGHT_DEMAND_SEGMENT_REGISTRY_V1,
  TWELVE_DEMAND_SEGMENT_REGISTRY_V1,
  resolveDemandSegment,
} from "./eight-demand-segment-registry-v1.js";

export const EIGHT_SEGMENT_MECHANISM_ROUTER_V1 =
  "gdi-eight-segment-mechanism-router-v1";
export const TWELVE_SEGMENT_MECHANISM_ROUTER_V1 =
  "gdi-twelve-segment-mechanism-router-v1";

export { DEMAND_MECHANISM_CLASS, EVENT_SUBTYPE };

const EXTRA_MECHANISM = Object.freeze({
  GOVERNMENT_PROGRAMME: "government_programme",
  SPORTS_TOURNAMENT_OR_TEAM: "sports_tournament_or_team",
  ENTERTAINMENT_TOUR_OR_PRODUCTION: "entertainment_tour_or_production",
  EDUCATION_VISITING_PROGRAMME: "education_visiting_programme",
  PROJECT_SITE_DEPLOYMENT: "project_site_deployment",
  SOCIAL_GROUP_OR_DESTINATION_EVENT: "social_group_or_destination_event",
  LOCAL_CATERING_DAY_MEETING: "local_catering_day_meeting",
  CORPORATE_CELEBRATION_PRIVATE_EVENT: "corporate_celebration_private_event",
  TRAVEL_TRADE_INTERMEDIARY: "travel_trade_intermediary",
  RELOCATION_EXTENDED_STAY: "relocation_extended_stay",
});

function text(v) {
  return String(v || "").trim();
}

function blobOf(input = {}) {
  return [
    input.organizationName,
    input.companyName,
    input.title,
    input.eventName,
    input.summaryWhat,
    input.whatIsHappening,
    input.demandMechanism,
    input.projectName,
    input.programmeName,
  ]
    .map(text)
    .filter(Boolean)
    .join(" \n ")
    .toLowerCase();
}

function classifyExtended(blob, input = {}) {
  const explicit = text(input.demandMechanismClass || input.mechanismClass);
  if (explicit) return explicit;

  // Segment 12 — relocation / extended stay (before generic project)
  if (
    /\b(corporate relocation|employee relocation|expatriate|expat assignment|corporate mobility|temporary (executive )?assignment|interim management|extended[- ]stay|temporary accommodation|staff onboarding travel)\b/i.test(
      blob,
    ) &&
    !/\b(construction|commissioning|infrastructure project)\b/i.test(blob)
  ) {
    return EXTRA_MECHANISM.RELOCATION_EXTENDED_STAY;
  }

  // Segment 11 — DMC / tour operator / incentive house account
  if (
    /\b(destination management (company|co)|dmc\b|tour operator|incentive house|receptive operator|group wholesaler|meetings (and|&) events agency|group travel (organizer|company|intermediary))\b/i.test(
      blob,
    )
  ) {
    return EXTRA_MECHANISM.TRAVEL_TRADE_INTERMEDIARY;
  }

  // Segment 10 — corporate celebrations (not family wedding, not incentive trip)
  if (
    /\b(holiday party|christmas party|company anniversary|employee recognition (gala|event|dinner)|client appreciation|product launch (reception|party)|corporate gala|award ceremon|staff (party|celebration)|brand activation reception)\b/i.test(
      blob,
    ) &&
    !/\b(destination wedding|family reunion|president'?s club|incentive trip|gold trip)\b/i.test(
      blob,
    )
  ) {
    return EXTRA_MECHANISM.CORPORATE_CELEBRATION_PRIVATE_EVENT;
  }

  // Segment 9 — local catering / day meetings (no overnight thesis)
  if (
    (/\b(desayuno(s)? empresarial(es)?|capacitaciones?|reuniones? ejecutivas?|talleres?|seminarios?|networking (breakfast|reception)|day[- ]use meeting|local (training|workshop|board) meeting|coffee break|meeting room (plus|and) (lunch|catering)|private dining|board (meeting|session))\b/i.test(
      blob,
    ) ||
      input.localAttendeesOnly === true ||
      input.guestroomRequired === false) &&
    !/\b(overnight|room block|resort stay|incentive|offsite|retreat|congress|conference)\b/i.test(
      blob,
    )
  ) {
    return EXTRA_MECHANISM.LOCAL_CATERING_DAY_MEETING;
  }

  if (
    /\b(rfp|procurement notice|government (agency|delegation|training)|public[- ]sector|multilateral|embassy)\b/i.test(
      blob,
    )
  ) {
    return EXTRA_MECHANISM.GOVERNMENT_PROGRAMME;
  }
  if (
    /\b(tournament|fixture|visiting team|match officials|championship)\b/i.test(blob) &&
    !/\b(congress|conference)\b/i.test(blob)
  ) {
    return EXTRA_MECHANISM.SPORTS_TOURNAMENT_OR_TEAM;
  }
  if (
    /\b(concert tour|touring (crew|production)|film (shoot|production)|television production|festival crew)\b/i.test(
      blob,
    )
  ) {
    return EXTRA_MECHANISM.ENTERTAINMENT_TOUR_OR_PRODUCTION;
  }
  if (
    /\b(visiting (faculty|researchers|scholars)|research exchange|academic exchange|student (group|programme)|university programme)\b/i.test(
      blob,
    ) &&
    !/\b(congress|conference|symposium|annual meeting)\b/i.test(blob)
  ) {
    return EXTRA_MECHANISM.EDUCATION_VISITING_PROGRAMME;
  }
  if (
    /\b(construction(\s+crew|\s+commissioning)?|commissioning(\s+crew)?|project (mobilization|deployment|site)|engineering (team|assignment|commissioning)|infrastructure project|long[- ]stay project|visiting (contractors|engineers|specialists))\b/i.test(
      blob,
    )
  ) {
    return EXTRA_MECHANISM.PROJECT_SITE_DEPLOYMENT;
  }
  if (
    /\b(destination wedding|family reunion|religious (pilgrimage|gathering)|social celebration|reunion travel)\b/i.test(
      blob,
    )
  ) {
    return EXTRA_MECHANISM.SOCIAL_GROUP_OR_DESTINATION_EVENT;
  }
  return null;
}

function segmentForMechanism(mechanism, binaryRoute) {
  switch (mechanism) {
    case EXTRA_MECHANISM.LOCAL_CATERING_DAY_MEETING:
      return DEMAND_SEGMENT.LOCAL_CATERING_DAY_MEETINGS;
    case EXTRA_MECHANISM.CORPORATE_CELEBRATION_PRIVATE_EVENT:
      return DEMAND_SEGMENT.CORPORATE_CELEBRATIONS_PRIVATE_EVENTS;
    case EXTRA_MECHANISM.TRAVEL_TRADE_INTERMEDIARY:
      return DEMAND_SEGMENT.TRAVEL_TRADE_GROUP_INTERMEDIARIES;
    case EXTRA_MECHANISM.RELOCATION_EXTENDED_STAY:
      return DEMAND_SEGMENT.RELOCATION_EXTENDED_STAY;
    case EXTRA_MECHANISM.GOVERNMENT_PROGRAMME:
      return DEMAND_SEGMENT.GOVERNMENT_PUBLIC_SECTOR;
    case EXTRA_MECHANISM.SPORTS_TOURNAMENT_OR_TEAM:
    case EXTRA_MECHANISM.ENTERTAINMENT_TOUR_OR_PRODUCTION:
      return DEMAND_SEGMENT.SPORTS_ENTERTAINMENT;
    case EXTRA_MECHANISM.EDUCATION_VISITING_PROGRAMME:
      return DEMAND_SEGMENT.EDUCATION_SCIENTIFIC;
    case EXTRA_MECHANISM.PROJECT_SITE_DEPLOYMENT:
      return DEMAND_SEGMENT.PROJECTS_INFRASTRUCTURE;
    case EXTRA_MECHANISM.SOCIAL_GROUP_OR_DESTINATION_EVENT:
      return DEMAND_SEGMENT.SOCIAL_SPECIAL_EVENTS;
    case DEMAND_MECHANISM_CLASS.INCENTIVE_RECOGNITION_TRAVEL:
    case DEMAND_MECHANISM_CLASS.CORPORATE_MEETING_RETREAT:
      // Product Segment 3 — overnight / destination programmes; historical Events not auto-migrated
      return DEMAND_SEGMENT.CORPORATE_MEETINGS_INCENTIVES;
    case DEMAND_MECHANISM_CLASS.ASSOCIATION_CONGRESS:
    case DEMAND_MECHANISM_CLASS.TRADE_SHOW_EXHIBITOR:
    case DEMAND_MECHANISM_CLASS.ORGANIZED_EVENT_GATHERING:
    case DEMAND_MECHANISM_CLASS.HISTORICAL_EVENT_RELATIONSHIP:
      return DEMAND_SEGMENT.ASSOCIATIONS_CONFERENCES_EVENTS;
    case DEMAND_MECHANISM_CLASS.ONGOING_CORPORATE_ACCOUNT:
      return DEMAND_SEGMENT.CORPORATE_ACCOUNTS_SCALING;
    default:
      if (binaryRoute?.primaryResearchSegment === RESEARCH_SEGMENT.SCALING_COMPANIES) {
        return DEMAND_SEGMENT.CORPORATE_ACCOUNTS_SCALING;
      }
      if (
        binaryRoute?.primaryResearchSegment ===
        RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT
      ) {
        return DEMAND_SEGMENT.ASSOCIATIONS_CONFERENCES_EVENTS;
      }
      return null;
  }
}

/**
 * Route by demand mechanism → twelve-segment customer id + legacy research axis.
 */
export function routeTwelveDemandSegmentByMechanism(input = {}) {
  const binary = routeBinary(input);
  const blob = blobOf(input);
  const extended = classifyExtended(blob, input);
  const mechanism = extended || binary.demandMechanismClass;
  const segment = segmentForMechanism(mechanism, binary);

  // Scientific congress stays Segment 1 even if "academic" words appear
  let finalSegment = segment;
  if (
    segment?.id === "education_scientific" &&
    /\b(congress|conference|symposium|annual meeting)\b/i.test(blob)
  ) {
    finalSegment = DEMAND_SEGMENT.ASSOCIATIONS_CONFERENCES_EVENTS;
  }
  // Day-local workshops must not stay on Segment 3 overnight path
  if (
    segment?.id === "corporate_meetings_incentives" &&
    (input.localAttendeesOnly === true ||
      /\b(day[- ]use|local attendees?|no overnight|desayuno empresarial|capacitaci[oó]n)\b/i.test(
        blob,
      )) &&
    !/\b(overnight|resort|incentive|offsite|retreat|president'?s club)\b/i.test(blob)
  ) {
    finalSegment = DEMAND_SEGMENT.LOCAL_CATERING_DAY_MEETINGS;
  }

  return {
    version: TWELVE_SEGMENT_MECHANISM_ROUTER_V1,
    registryVersion: TWELVE_DEMAND_SEGMENT_REGISTRY_V1,
    opportunityId: input.id || input.opportunityId || null,
    demandMechanismClass: mechanism,
    customerDemandSegmentId: finalSegment?.id || null,
    customerDemandSegmentNumber: finalSegment?.number || null,
    customerSegmentLabel: finalSegment?.label || null,
    primaryResearchSegment:
      finalSegment?.researchAxis || binary.primaryResearchSegment,
    legacyBinaryRoute: binary,
    historicalMigration: "PROHIBITED_WITHOUT_EXPLICIT_FOUNDER_TASK",
    crossSegmentDuplicateRule:
      "One demand mechanism → one customer opportunity. Distinct mechanisms may share an organization with distinct IDs. DMC account ≠ the specific group programme it produces.",
    confidence: finalSegment ? binary.confidence || "MEDIUM" : "MANUAL_REVIEW",
    commercialNote: binary.commercialNote || "",
    applyToLiveRecords: false,
  };
}

/** @deprecated Prefer routeTwelveDemandSegmentByMechanism — same implementation. */
export function routeEightDemandSegmentByMechanism(input = {}) {
  const r = routeTwelveDemandSegmentByMechanism(input);
  return {
    ...r,
    version: EIGHT_SEGMENT_MECHANISM_ROUTER_V1,
    registryVersion: EIGHT_DEMAND_SEGMENT_REGISTRY_V1,
  };
}

/**
 * Dedup key helper — mechanism + org + edition/project identity.
 */
export function buildCrossSegmentDedupKey(input = {}, routing = null) {
  const route = routing || routeTwelveDemandSegmentByMechanism(input);
  const org = text(
    input.organizationKey || input.companyName || input.organizationName || "",
  )
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_");
  const identity = text(
    input.editionKey ||
      input.projectKey ||
      input.eventSeriesKey ||
      input.canonicalEventName ||
      input.projectName ||
      input.title ||
      "",
  )
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .slice(0, 80);
  return [
    route.customerDemandSegmentId || "unrouted",
    org || "org",
    identity || "identity",
  ].join("::");
}

export function segmentsWouldDuplicate(a = {}, b = {}) {
  const ka = buildCrossSegmentDedupKey(a);
  const kb = buildCrossSegmentDedupKey(b);
  return ka === kb;
}

export function resolveSegmentFromAnyKey(key) {
  return resolveDemandSegment(key);
}
