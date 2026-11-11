/**
 * Map Segments 4 / 9 / 12 Exa Agent deep-research → commercialIntelligenceBrief
 * + local shadow opportunity DTO. Does not reuse Scaling corporate-growth boilerplate.
 */
import { createHash } from "node:crypto";
import { resolveDemandSegment } from "../../demand-segments/eight-demand-segment-registry-v1.js";
import { evaluateRoomNightOptionalEligibility } from "../../demand-segments/room-night-optional-eligibility-v1.js";
import {
  writeSegmentSemanticCib,
  SEGMENT_SEMANTIC_CIB_WRITER_V1,
} from "../../demand-segments/segment-semantic-cib-writer-v1.js";
import { applyUniversalTimingOverlay } from "../../universal-timing-intelligence-v1.js";

export const EXA_AGENT_SEGMENTS_4_9_12_TO_CIB_MAPPING_V1 =
  "gdi-exa-segments-4-9-12-to-cib-mapping-v1";

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

export function stableSegment4912OpportunityId(hotelId, segmentId, candidate = {}) {
  const raw = [
    hotelId,
    segmentId,
    text(candidate.organizationName),
    text(candidate.programmeName || candidate.programmeIdentity),
  ].join("|");
  const h = createHash("sha256").update(raw).digest("hex").slice(0, 12);
  return `gdi_s4912_${h}`;
}

function mapContacts(candidate = {}, deep = {}) {
  const fromDeep = Array.isArray(deep.namedContacts) ? deep.namedContacts : [];
  const fromDisc = Array.isArray(candidate.namedContacts)
    ? candidate.namedContacts
    : [];
  const merged = [...fromDeep, ...fromDisc].filter((c) => text(c?.name));
  const seen = new Set();
  const out = [];
  for (const c of merged) {
    const k = text(c.name).toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(c);
  }
  return out;
}

function supportingSources(candidate = {}, deep = {}) {
  const fromDeep = Array.isArray(deep.supportingSources)
    ? deep.supportingSources
    : [];
  const fromDisc = (Array.isArray(candidate.evidenceUrls)
    ? candidate.evidenceUrls
    : []
  ).map((url) => ({
    claim: text(candidate.demandMechanismSummary) || "Discovery evidence URL",
    url,
  }));
  return [...fromDeep, ...fromDisc].filter((s) => text(s?.url));
}

const TBD = "Not yet verified";

/**
 * Build Section 1 structured overview values from existing Exa evidence only.
 */
export function buildSegment4912OverviewValues(
  segmentId,
  candidate = {},
  deep = {},
  hotel = {},
) {
  const guestroom =
    text(deep.guestroomDemandStatus || candidate.guestroomDemandStatus) || TBD;
  const guestroomLabel =
    guestroom === "NONE_CONFIRMED"
      ? "No guestroom demand confirmed — catering/meeting revenue optional"
      : guestroom === "EVIDENCE_SUPPORTED"
        ? "Guestroom demand evidenced (qualify volumes)"
        : "Guestroom demand not yet verified";
  const revenue = Array.isArray(deep.revenueTypes)
    ? deep.revenueTypes
    : candidate.revenueTypes || [];
  const revenueLine = revenue.length ? revenue.join(", ") : TBD;
  const timing =
    text(deep.assignmentTiming || deep.programmeTiming || deep.timing) || TBD;
  const hotelMarket =
    /chapultepec|mexico city/i.test(String(hotel.name || hotel.hotelId || ""))
      ? "Mexico City"
      : /condesa|cancún|cancun/i.test(String(hotel.name || hotel.hotelId || ""))
        ? "Cancún"
        : text(hotel.market || hotel.city);
  const location =
    text(deep.destination || deep.location || candidate.location) ||
    hotelMarket ||
    TBD;
  const procurement =
    text(deep.procurementOrAccessibilityNotes) ||
    "Procurement / accessibility not yet verified";

  if (segmentId === "government_public_sector") {
    return {
      timing,
      location,
      procurementStatus: procurement,
      travelLodgingDemand: guestroomLabel,
      programmeCycle: text(deep.programmeCycle) || TBD,
      meetingRequirements: revenueLine,
      contractingPathway: procurement,
    };
  }
  if (segmentId === "local_catering_day_meetings") {
    return {
      timing,
      location,
      venueSourcing: text(deep.venueSourcing) || "Venue sourcing unconfirmed",
      meetingCateringDemand: revenueLine,
      demandCycle: text(deep.demandCycle) || TBD,
      attendees: text(deep.attendees || deep.attendance) || TBD,
      guestroomDemand: guestroomLabel,
    };
  }
  if (segmentId === "relocation_extended_stay") {
    return {
      assignmentTiming: timing,
      destination: location,
      accommodationSourcing:
        text(deep.accommodationSourcing) || "Accommodation sourcing unconfirmed",
      stayDemand: guestroomLabel,
      assignmentCycle: text(deep.assignmentCycle) || TBD,
      expectedStayPattern:
        text(deep.expectedStayPattern) ||
        "Stay pattern not verified — do not treat employment expansion as lodging duration",
      mobilityTravelPathway: procurement,
    };
  }
  return {};
}

export function buildSegment4912CommercialIntelligenceBrief({
  hotel = {},
  segmentId,
  candidate = {},
  deepResearch = {},
} = {}) {
  const segment = resolveDemandSegment(segmentId);
  const contacts = mapContacts(candidate, deepResearch);
  const primary = contacts[0] || null;
  const secondary = contacts[1] || null;
  const sources = supportingSources(candidate, deepResearch);
  const org =
    text(candidate.organizationName) ||
    text(deepResearch.programmeIdentity)?.split("—")[0] ||
    "Organization";
  const programme =
    text(deepResearch.programmeIdentity) ||
    text(candidate.programmeName) ||
    org;
  const thesis = text(deepResearch.hotelDemandThesis);
  const discoverySummary = text(candidate.demandMechanismSummary);
  const what = discoverySummary || thesis || programme;
  const nextStep =
    text(deepResearch.recommendedNextStep) ||
    "Verify named contact, procurement accessibility, and revenue fit before outreach.";
  const potentialFromDeep = text(deepResearch.potentialBusinessForHotel);
  const revenueTypes =
    (Array.isArray(deepResearch.revenueTypes) && deepResearch.revenueTypes.length
      ? deepResearch.revenueTypes
      : candidate.revenueTypes) || [];
  const potential =
    potentialFromDeep ||
    (thesis && thesis !== what
      ? thesis
      : thesis
        ? `${thesis} Potential hotel revenue remains a hypothesis until a travel, meetings, catering, or mobility owner confirms patterns.`
        : revenueTypes.length
          ? `Potential revenue mechanisms include ${revenueTypes.join(", ")}. Volumes, buyer ownership, and accessibility remain unconfirmed until qualified with the account.`
          : "Hospitality demand is evidenced qualitatively only. Published room-night volumes are not required for this segment; qualify catering, meeting, or lodging fit before pursuit.");

  const hotelName = hotel.name || hotel.displayName || "this hotel";
  const whyHotel =
    text(deepResearch.whyThisHotel) ||
    text(deepResearch.hotelDemandThesis) ||
    `Evaluate fit of ${hotelName} against evidenced programme type and destination — only when commercial accessibility supports outreach.`;
  const advantages = [
    text(deepResearch.hotelFitAdvantage),
    hotelName
      ? `${hotelName} sits in the same commercial market as the documented programme activity.`
      : null,
  ].filter(Boolean);
  const limitations = [
    text(deepResearch.hotelFitLimitation),
    "No verified hotel booking, RFP, or preferred-supplier agreement is established from public research alone.",
    "Procurement accessibility and buyer ownership remain unverified.",
  ].filter(Boolean);

  const facts = [
    ...(Array.isArray(deepResearch.establishedFacts)
      ? deepResearch.establishedFacts
      : []),
    ...sources.slice(0, 4).map((s) => s.claim),
  ].filter(Boolean);
  const uncertainties = [
    ...(Array.isArray(deepResearch.uncertainties)
      ? deepResearch.uncertainties
      : []),
    ...(Array.isArray(candidate.uncertainties) ? candidate.uncertainties : []),
  ].filter(Boolean);

  const whyActNow =
    text(deepResearch.whyActNow) ||
    text(deepResearch.procurementOrAccessibilityNotes) ||
    "Confirm sourcing openness and buyer ownership before pursuit. Do not invent urgency from historical announcements alone.";

  const overviewValues = buildSegment4912OverviewValues(
    segment?.id || segmentId,
    candidate,
    deepResearch,
    hotel,
  );
  const evidenceSources = sources.map((s) => ({
    claim: s.claim,
    url: s.url,
    title: text(s.title || s.claim) || null,
    establishes: s.claim || null,
  }));

  return {
    version: "gdi-segments-4-9-12-commercial-intelligence-brief-v1",
    sourceMapping: EXA_AGENT_SEGMENTS_4_9_12_TO_CIB_MAPPING_V1,
    demandSegmentId: segment?.id || segmentId,
    segmentDisplayLabel: segment?.label || segmentId,
    opportunityOverview: `${org} — ${programme}`.replace(/^ — | — $/g, ""),
    whatIsHappening: what,
    companyAndLocalActivity: what,
    potentialBusinessForHotel: potential,
    potentialBusinessForThisHotel: potential,
    potentialBusiness: potential,
    whyThisHotel: whyHotel,
    whyThisHotelAdvantages: advantages,
    whyThisHotelLimitations: limitations,
    whyActNow,
    establishedFactsAndUncertainties: { facts, uncertainties },
    evidenceAndUncertainty: {
      establishedFacts: facts,
      uncertainties,
      sources: evidenceSources,
    },
    whoToContact: {
      primary: primary
        ? {
            name: primary.name,
            title: primary.title || null,
            organization: primary.organization || org,
            whyRelevant: primary.whyRelevant || null,
            sourceUrl: primary.sourceUrl || null,
          }
        : null,
      secondary: secondary
        ? {
            name: secondary.name,
            title: secondary.title || null,
            organization: secondary.organization || org,
            whyRelevant: secondary.whyRelevant || null,
            sourceUrl: secondary.sourceUrl || null,
          }
        : null,
      narrative: primary
        ? null
        : Array.isArray(candidate.relevantBuyerRoles) &&
            candidate.relevantBuyerRoles.length
          ? `Identify: ${candidate.relevantBuyerRoles.join(", ")}`
          : "Identify the programme / procurement owner before outreach.",
    },
    recommendedNextStep: nextStep,
    recommendedSalesApproach: {
      nextAction: nextStep,
      qualificationQuestions: Array.isArray(deepResearch.qualificationQuestions)
        ? deepResearch.qualificationQuestions
        : [],
    },
    qualificationQuestions: Array.isArray(deepResearch.qualificationQuestions)
      ? deepResearch.qualificationQuestions
      : [],
    supportingEvidence: evidenceSources,
    segmentOverview: overviewValues,
    hotelValidationActionOutcome: {
      validation: "Pending hotel team review",
      action: nextStep,
      outcome: null,
    },
    scalingBoilerplateExcluded: true,
  };
}

export function mapSegment4912AgentResultToOpportunity({
  hotel = {},
  segmentId,
  candidate = {},
  deepResearch = null,
} = {}) {
  const hotelId = hotel.hotelId || hotel.id;
  const segment = resolveDemandSegment(segmentId);
  const deep = deepResearch || {};
  // Preserve live-wave IDs: fingerprint used deep.programmeIdentity during staging.
  const id = stableSegment4912OpportunityId(hotelId, segmentId, {
    ...candidate,
    programmeName: deep.programmeIdentity || candidate.programmeName,
  });
  const written = writeSegmentSemanticCib({
    hotel,
    segmentId,
    candidate,
    deepResearch: deep,
  });
  const cib = written.commercialIntelligenceBrief;
  const primary = written.primaryContact;
  const contacts = [written.primaryContact, written.secondaryContact].filter(
    Boolean,
  );
  const revenueTypes =
    (Array.isArray(candidate.revenueTypes) && candidate.revenueTypes.length
      ? candidate.revenueTypes
      : deep.revenueTypes) || [];
  const guestroomDemandStatus =
    deep.guestroomDemandStatus || candidate.guestroomDemandStatus || "UNKNOWN";
  const eligibility = evaluateRoomNightOptionalEligibility({
    demandSegmentId: segment?.id || segmentId,
    guestroomDemandStatus,
    revenueTypes: Array.isArray(revenueTypes)
      ? revenueTypes.filter((t) => text(t).length < 60)
      : [],
    activityVerified: true,
    evidenceSupported: true,
    verifiedActivitySummary:
      written.whatIsHappening || text(candidate.demandMechanismSummary),
    sources: written.sources,
    primaryContact: primary,
    contacts,
    recommendedNextStep: written.recommendedNextStep,
    assumeFacilityFit: true,
  });

  const base = {
    id,
    opportunityId: id,
    hotelId,
    demandSegmentId: segment?.id || segmentId,
    researchSegment: segment?.id || segmentId,
    researchAxis: segment?.researchAxis || segmentId,
    segment: segment?.label || segmentId,
    segmentDisplayLabel: segment?.label || segmentId,
    organizationName: written.organizationName,
    organizationNameRaw: written.organizationNameRaw || written.organizationName,
    relatedOrganizations: written.relatedOrganizations || [],
    programmeName: written.programmeName,
    title: written.title,
    displayTitle: written.displayTitle || written.title,
    drawerHeaderTitle: written.drawerHeaderTitle || written.title,
    opportunityIdentity: written.opportunityIdentity || null,
    opportunityLevel: written.opportunityIdentity?.opportunityLevel || null,
    namingBasis: written.opportunityIdentity?.namingBasis || null,
    researchTitlePreserved:
      written.opportunityIdentity?.researchTitlePreserved || null,
    eventDisplayName: written.programmeName,
    summaryWhat: written.whatIsHappening,
    whatIsHappening: written.whatIsHappening,
    tileCustomerSummary: written.whatIsHappening,
    hotelOpportunityThesis: written.whyThisHotel,
    revenueTypes: Array.isArray(candidate.revenueTypes)
      ? candidate.revenueTypes
      : [],
    guestroomDemandStatus,
    roomNightOptionalEligibility: eligibility,
    opportunityQualification: eligibility?.customerReady
      ? "ASSIGN_FOR_DISCOVERY"
      : "WATCHLIST",
    priority: "WATCHLIST",
    commercialDisposition: eligibility?.disposition || "Watch",
    contactName: primary?.name || null,
    contactRole: primary?.title || null,
    primaryContact: written.primaryContact,
    secondaryContact: written.secondaryContact,
    commercialIntelligenceBrief: cib,
    segmentOverview: written.segmentOverview || {},
    whyThisHotel: written.whyThisHotel,
    whyThisHotelAdvantages: written.whyThisHotelAdvantages,
    whyThisHotelLimitations: written.whyThisHotelLimitations,
    whyNow: written.whyNow,
    establishedFacts: written.establishedFacts,
    uncertainties: written.uncertainties,
    sources: written.sources,
    evidenceUrls: written.evidenceUrls,
    recommendedNextStep: written.recommendedNextStep,
    recommendedAction: written.recommendedAction,
    localShadowOnly: true,
    localShadowPortfolio: true,
    segments4912LiveWaveV1: true,
    customerVisible: true,
    doesNotAffectProduction: true,
    liveExaAgentRun: true,
    scalingBoilerplateExcluded: true,
    semanticContentVersion: SEGMENT_SEMANTIC_CIB_WRITER_V1,
  };

  return applyUniversalTimingOverlay(base);
}
