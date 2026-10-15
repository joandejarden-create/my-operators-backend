/**
 * Local Meetings & Catering research segment V1.
 *
 * Qualifies local business / organizers that may produce meeting-room rental,
 * F&B, or social functions — including opportunities that require NO guestrooms.
 *
 * Guestrooms are NOT a qualification requirement.
 * Generic company directories do not qualify.
 * A nearby event venue is not automatically a catering buyer.
 * Dated growth evidence is NOT required (that belongs to scaling-companies).
 */

import { createHash } from "node:crypto";
import { OPPORTUNITY_TYPE } from "../claim-types.js";
import { DEMAND_FAMILY, DEMAND_SIGNAL_TYPE } from "../demand-signal-types.js";
import {
  ORGANIZATION_TYPE,
  GENERATOR_STATUS,
  PROGRAM_TYPE,
  RECURRENCE_STATUS,
} from "../demand-generators/constants.js";
import {
  computeDemandGeneratorId,
  computeProgramId,
  buildDemandGeneratorEntity,
  buildProgramEntity,
  domainFromUrl,
} from "../demand-generators/entities.js";
import {
  computeHotelGeneratorFitId,
  buildHotelDemandGeneratorFit,
} from "../demand-generators/fit-relevance.js";
import {
  DEMAND_USE_CASE,
  GUESTROOM_STATUS,
  GUESTROOM_STATUS_LABEL,
  RELATIONSHIP_GRAIN,
  guestroomStatusForUseCases,
  normalizeDemandUseCases,
} from "./shared-demand-model-v1.js";

export const LOCAL_MEETINGS_CATERING_RESEARCH_VERSION =
  "gdi-local-meetings-catering-research-v1";

export const LOCAL_CATERING_FAIL = Object.freeze({
  MISSING_BUYER: "MISSING_BUYER",
  MISSING_EVENT_ACTIVITY: "MISSING_EVENT_ACTIVITY",
  MISSING_LOCAL_CONNECTION: "MISSING_LOCAL_CONNECTION",
  MISSING_HOTEL_FIT: "MISSING_HOTEL_FIT",
  MISSING_CONTACT_ROUTE: "MISSING_CONTACT_ROUTE",
  MISSING_WHY_MATTERS: "MISSING_WHY_MATTERS",
  MISSING_NEXT_ACTION: "MISSING_NEXT_ACTION",
  DIRECTORY_ONLY: "DIRECTORY_ONLY",
  VENUE_AS_BUYER: "VENUE_AS_BUYER",
  HOTEL_PROFILE_INCOMPLETE: "HOTEL_PROFILE_INCOMPLETE",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

/**
 * Applicability from actual hotel meeting/catering inventory — no invented facilities.
 */
export function assessLocalMeetingsCateringApplicability(hotelConfig = {}) {
  const rooms =
    Number(
      hotelConfig.capabilityProfile?.meetingRoomCount ??
        hotelConfig.capabilityProfile?.meetingRoomsIndoor ??
        0
    ) || 0;
  const sqft = Number(hotelConfig.capabilityProfile?.totalMeetingSpaceSqFt || 0) || 0;
  const reception =
    Number(hotelConfig.capabilityProfile?.largestReceptionCapacity || 0) || 0;
  const notes = text(hotelConfig.capabilityProfile?.notes);
  const missingInventory = rooms < 1 && sqft < 1 && reception < 1;

  if (missingInventory) {
    return {
      applicable: false,
      needsProfileEnrichment: true,
      rationale:
        "Meeting/catering inventory not established in hotel profile — enrich function-space and catering fields before local meetings research.",
    };
  }

  if (rooms < 3 && sqft < 3000 && reception < 50) {
    return {
      applicable: false,
      needsProfileEnrichment: false,
      rationale:
        "Limited published meeting inventory — local meetings & catering not prioritized for this product profile.",
    };
  }

  return {
    applicable: true,
    needsProfileEnrichment: !notes,
    rationale: `Hotel publishes meeting inventory (${rooms || "n/a"} rooms, ${sqft || "n/a"} sq ft) supporting local meetings, F&B, and social functions — guestrooms not required for qualification.`,
    hotelMeetingSnapshot: {
      meetingRoomCount: rooms || null,
      totalMeetingSpaceSqFt: sqft || null,
      largestReceptionCapacity: reception || null,
      largestBallroomSqFt:
        Number(hotelConfig.capabilityProfile?.largestBallroomSqFt || 0) || null,
      outdoorEventSpace: hotelConfig.capabilityProfile?.outdoorEventSpace ?? null,
      serviceLevel: hotelConfig.capabilityProfile?.serviceLevel || null,
      classification: hotelConfig.capabilityProfile?.classification || null,
    },
  };
}

function isDirectoryOnly(candidate = {}) {
  const src = lower(
    [candidate.sourceUrl, ...(candidate.sourceUrls || []), candidate.sourceType].join(
      " "
    )
  );
  return /yellowpages|yelp\.com|chamber.?directory|member.?directory|crunchbase|linkedin\.com\/company\/?$/.test(
    src
  );
}

function isVenueAsBuyerMisuse(candidate = {}) {
  return (
    candidate.nearbyVenueAsBuyer === true ||
    candidate.treatVenueAsCateringBuyer === true ||
    (candidate.entityKind === "VENUE" &&
      candidate.relationshipGrain === RELATIONSHIP_GRAIN.VENUE_REFERRAL)
  );
}

/**
 * Qualify local meetings/catering candidate. Guestrooms optional.
 */
export function qualifyLocalMeetingsCateringCandidate(
  candidate = {},
  hotelConfig = {}
) {
  const fail = [];
  const warnings = [];

  const app = assessLocalMeetingsCateringApplicability(hotelConfig);
  if (!app.applicable && app.needsProfileEnrichment) {
    fail.push(LOCAL_CATERING_FAIL.HOTEL_PROFILE_INCOMPLETE);
  }

  const buyer = text(
    candidate.organizationName || candidate.buyerName || candidate.company
  );
  if (!buyer) fail.push(LOCAL_CATERING_FAIL.MISSING_BUYER);

  const activity = text(
    candidate.eventActivityEvidence ||
      candidate.recurringMeetingEvidence ||
      candidate.activityEvidence ||
      candidate.datedOccasionNote
  );
  if (!activity) fail.push(LOCAL_CATERING_FAIL.MISSING_EVENT_ACTIVITY);

  const local = text(
    candidate.localConnection || candidate.localPresence || candidate.marketConnection
  );
  if (!local) fail.push(LOCAL_CATERING_FAIL.MISSING_LOCAL_CONNECTION);

  const fit = text(
    candidate.hotelFitRationale || candidate.fitExplanation || candidate.whyThisHotel
  );
  if (!fit) fail.push(LOCAL_CATERING_FAIL.MISSING_HOTEL_FIT);

  const contactEmail = text(candidate.contactEmail);
  const contactPhone = text(candidate.contactPhone);
  const contactUrl = text(candidate.contactUrl || candidate.organizationPath);
  if (!contactEmail && !contactPhone && !contactUrl) {
    fail.push(LOCAL_CATERING_FAIL.MISSING_CONTACT_ROUTE);
  } else if (
    contactUrl &&
    !contactEmail &&
    !contactPhone &&
    /\/$/.test(contactUrl) &&
    !/\/contact|\/events|\/about|\/staff/i.test(contactUrl)
  ) {
    warnings.push("contact_url_may_be_homepage_only");
  }

  const whyMatters = text(
    candidate.whyThisMatters || candidate.summaryWhyMatters || candidate.commercialThesis
  );
  if (!whyMatters) fail.push(LOCAL_CATERING_FAIL.MISSING_WHY_MATTERS);

  const nextAction = text(
    candidate.recommendedAction || candidate.recommendedNextStep
  );
  if (!nextAction) fail.push(LOCAL_CATERING_FAIL.MISSING_NEXT_ACTION);

  if (isDirectoryOnly(candidate)) fail.push(LOCAL_CATERING_FAIL.DIRECTORY_ONLY);
  if (isVenueAsBuyerMisuse(candidate)) fail.push(LOCAL_CATERING_FAIL.VENUE_AS_BUYER);

  // Homepage-only (no path) without email/phone → fail contact
  if (
    contactUrl &&
    !contactEmail &&
    !contactPhone &&
    /^https?:\/\/[^/]+\/?$/i.test(contactUrl)
  ) {
    fail.push(LOCAL_CATERING_FAIL.MISSING_CONTACT_ROUTE);
  }

  const useCases = normalizeDemandUseCases(
    candidate.demandUseCases ||
      candidate.useCases || [DEMAND_USE_CASE.MEETINGS_AND_CATERING]
  );
  if (!useCases.length) useCases.push(DEMAND_USE_CASE.MEETINGS_AND_CATERING);

  const guestroomStatus = guestroomStatusForUseCases(
    useCases,
    candidate.guestroomStatus
  );

  return {
    ok: fail.length === 0,
    failReasons: fail,
    warnings,
    buyer,
    activity,
    localConnection: local,
    hotelFit: fit,
    whyMatters,
    recommendedAction: nextAction,
    demandUseCases: useCases,
    guestroomStatus,
    guestroomStatusLabel: GUESTROOM_STATUS_LABEL[guestroomStatus],
    hotelMeetingSnapshot: app.hotelMeetingSnapshot || null,
    contactEmail: contactEmail || null,
    contactPhone: contactPhone || null,
    contactUrl: contactUrl || null,
    contactName: text(candidate.contactName) || null,
    contactRole: text(candidate.contactRole) || "events / meetings organizer",
  };
}

export function computeLocalMeetingsCateringOpportunityId(
  hotelId,
  organizationName,
  domain
) {
  const d = text(domain) || "";
  const n = lower(organizationName).replace(/[^a-z0-9]+/g, " ").trim();
  const seed = `${text(hotelId)}|lmc|${d || n}`;
  return `gdi_lmc_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

export function buildLocalMeetingsCateringOpportunityCard(
  candidate = {},
  opts = {}
) {
  const hotelConfig = opts.hotelConfig || {};
  const hotelId =
    text(opts.hotelId) ||
    text(hotelConfig.hotelId) ||
    text(candidate.hotelId);
  const q = qualifyLocalMeetingsCateringCandidate(candidate, hotelConfig);
  if (!q.ok) {
    return { ok: false, qualification: q, opportunity: null };
  }

  const domain =
    text(candidate.officialDomain) ||
    domainFromUrl(candidate.website || candidate.sourceUrl || q.contactUrl);
  const opportunityId =
    text(candidate.existingOpportunityId) ||
    computeLocalMeetingsCateringOpportunityId(hotelId, q.buyer, domain);

  const publish = candidate.publish !== false;
  const nowIso = new Date().toISOString();
  const sources = []
    .concat(candidate.sources || [])
    .concat(
      (candidate.sourceUrls || []).map((url) => ({
        url,
        type: "event_activity",
        title: "Event activity evidence",
      }))
    );
  if (candidate.sourceUrl && !sources.some((s) => s.url === candidate.sourceUrl)) {
    sources.push({
      url: candidate.sourceUrl,
      type: "event_activity",
      title: "Event activity evidence",
    });
  }

  const dgId = computeDemandGeneratorId({
    organizationName: q.buyer,
    officialDomain: domain,
    website: candidate.website || q.contactUrl,
  });
  const fitId = computeHotelGeneratorFitId({
    hotelId,
    demandGeneratorId: dgId,
  });

  const functionType = text(candidate.functionType || candidate.eventType) ||
    "Local meeting / banquet / social function";
  const attendance = text(candidate.attendanceNote || candidate.estimatedAttendance);
  const recurrence = text(candidate.recurrenceNote) || "Recurring or repeatable local program";

  return {
    ok: true,
    qualification: q,
    opportunity: {
      id: opportunityId,
      opportunityId,
      hotelId,
      organizationName: q.buyer,
      company: q.buyer,
      title: `${q.buyer} — Local Meetings & Catering`,
      opportunityName: `${q.buyer} — Local Meetings & Catering`,
      opportunityType: OPPORTUNITY_TYPE.CORPORATE_ACCOUNT_RELATIONSHIP,
      opportunityTypeLabel: "Corporate Account Relationship",
      detailLayout: "CORPORATE_ACCOUNT_RELATIONSHIP",
      demandFamily: DEMAND_FAMILY.GROUP_MEETINGS,
      demandSignalType: DEMAND_SIGNAL_TYPE.CORPORATE_MEETING,
      segment: "Local Meetings & Catering",
      researchSegment: "local_meetings_catering",
      relationshipGrain: RELATIONSHIP_GRAIN.STANDING_ACCOUNT,
      demandUseCases: q.demandUseCases,
      standingOpportunity: true,
      cateringOnly: q.guestroomStatus === GUESTROOM_STATUS.NOT_REQUIRED,
      guestroomStatus: q.guestroomStatus,
      guestroomStatusLabel: q.guestroomStatusLabel,
      roomDemandStatus:
        q.guestroomStatus === GUESTROOM_STATUS.NOT_REQUIRED
          ? "NOT_REQUIRED"
          : "UNKNOWN",
      eventStartDate: candidate.eventStartDate || null,
      eventEndDate: null,
      eventDateDisplay:
        candidate.eventDateDisplay ||
        "Standing local meetings / catering relationship",
      timing: recurrence,
      functionType,
      attendanceNote: attendance || null,
      spaceNeedsNote: text(candidate.spaceNeedsNote) || null,
      localPresence: q.localConnection,
      localConnection: q.localConnection,
      eventActivityEvidence: q.activity,
      proposedHotelUse: q.demandUseCases
        .map((u) => u.replace(/_/g, " ").toLowerCase())
        .join("; "),
      whyThisHotel: q.hotelFit,
      hotelOpportunityThesis: q.hotelFit,
      fitExplanation: q.hotelFit,
      summaryWhyHotel: q.hotelFit,
      summaryWhat: `${q.buyer}: ${q.activity}`,
      summaryWhyMatters: q.whyMatters,
      whyNow:
        text(candidate.whyNow) ||
        "Standing local meeting/catering demand — no invented urgency; introduce hotel meeting product and ask how venues are currently selected.",
      whyEstablishContactNow:
        text(candidate.whyNow) ||
        "Local organizer with evidenced meeting/banquet activity in catchment.",
      constraints: [
        "Do not invent attendance, budgets, or room nights.",
        q.guestroomStatus === GUESTROOM_STATUS.NOT_REQUIRED
          ? "Guestrooms are not required for this opportunity."
          : "Guestroom demand is not established unless separately evidenced.",
        ...(candidate.constraints || []),
      ],
      uncertainties: candidate.uncertainties || [
        "Open sourcing / exclusive venue relationships not fully known from public pages.",
      ],
      establishedFacts: [
        q.activity,
        q.localConnection,
        `Contact route: ${q.contactEmail || q.contactPhone || q.contactUrl}`,
      ],
      inferredHotelDemand: [
        "Hypothesis: hotel meeting rooms and catering can serve this organizer's local functions when capacity and positioning fit.",
      ],
      contactName: q.contactName,
      contactEmail: q.contactEmail,
      contactPhone: q.contactPhone,
      contactUrl: q.contactUrl,
      contactRole: q.contactRole,
      contactResponsibility: q.contactRole,
      commercialContactPath: q.contactEmail
        ? "NAMED_DIRECT"
        : q.contactPhone
          ? "ORGANIZATION_PATH"
          : "ORGANIZATION_PATH",
      recommendedAction: q.recommendedAction,
      recommendedNextStep: q.recommendedAction,
      priority: candidate.priority || "MEDIUM_PRIORITY",
      venueSourcingStatus: "OPEN_UNRESOLVED",
      doNotEstimateAnnualRevenue: true,
      customerFacingState: publish ? "ACTIONABLE_NOW" : "INTERNAL_ONLY",
      customerVisible: publish,
      actionable: publish,
      prospectPublicationBasis: publish ? "REVIEWED_PACKAGE_V1" : null,
      prospectHotelFacing: publish,
      prospectReviewSection: "EXPLORATORY",
      prospectCommercialLabel: "Local meetings & catering",
      dgGeneratorId: dgId,
      hotelGeneratorFitId: fitId,
      schemaVersion: LOCAL_MEETINGS_CATERING_RESEARCH_VERSION,
      researchVersion:
        candidate.researchVersion || LOCAL_MEETINGS_CATERING_RESEARCH_VERSION,
      evidenceFreshnessAt: text(candidate.evidenceDate) || nowIso.slice(0, 10),
      lastVerifiedAt: nowIso,
      firstSeenAt: candidate.firstSeenAt || nowIso,
      updatedAt: nowIso,
      sources,
      officialSource: sources[0]?.url || null,
      hotelMeetingSnapshot: q.hotelMeetingSnapshot,
    },
  };
}

export function buildLocalMeetingsCateringDemandGeneratorPackage(
  candidate = {},
  opts = {}
) {
  const card = buildLocalMeetingsCateringOpportunityCard(candidate, opts);
  if (!card.ok) return card;
  const opp = card.opportunity;
  const domain =
    text(candidate.officialDomain) ||
    domainFromUrl(candidate.website || candidate.sourceUrl);

  const orgType =
    candidate.organizationType ||
    (/assoc|council|chamber|chapter|boma|asid|acec|mba/i.test(opp.organizationName)
      ? ORGANIZATION_TYPE.PROFESSIONAL_BODY
      : /nonprofit|foundation|partnership/i.test(opp.organizationName)
        ? ORGANIZATION_TYPE.NONPROFIT
        : ORGANIZATION_TYPE.OTHER);

  const generator = buildDemandGeneratorEntity({
    organizationName: opp.organizationName,
    officialDomain: domain,
    website: candidate.website || opp.contactUrl,
    organizationType: orgType,
    generatorStatus: GENERATOR_STATUS.VERIFIED,
    notes: `Local meetings & catering account. ${opp.eventActivityEvidence}`,
  });

  const program = buildProgramEntity({
    demandGeneratorId: generator.demandGeneratorId,
    programName: text(candidate.programName) || "Local meetings / banquet series",
    programType: PROGRAM_TYPE.REGIONAL_MEETING,
    recurrenceStatus: RECURRENCE_STATUS.POSSIBLE_RECURRING,
    notes: "Local F&B / meeting demand — guestrooms not required unless evidenced.",
  });

  const fit = buildHotelDemandGeneratorFit({
    hotelId: opp.hotelId,
    demandGeneratorId: generator.demandGeneratorId,
    marketRelevance: candidate.marketRelevance || "MEDIUM",
    productFit: candidate.productFit || "MEDIUM",
    generatorRelevance: "MEDIUM",
    likelyTravelCreation: "LOW",
    programObservability: "MEDIUM",
    buyerAccessibility: opp.contactEmail || opp.contactPhone ? "MEDIUM" : "LOW",
    evidenceQuality: "MEDIUM",
    fitRationale: opp.whyThisHotel,
  });

  return {
    ok: true,
    opportunity: opp,
    demandGenerator: generator,
    program,
    hotelGeneratorFit: fit,
    programId: program.programId || computeProgramId(program),
    dedupeKey: `${opp.hotelId}|${lower(opp.organizationName)}`,
  };
}

export function isLocalMeetingsCateringOpportunity(opp = {}) {
  if (opp.researchSegment === "local_meetings_catering") return true;
  if (opp.cateringOnly === true) return true;
  const uc = normalizeDemandUseCases(opp.demandUseCases || opp.useCases || []);
  if (
    uc.includes(DEMAND_USE_CASE.MEETINGS_AND_CATERING) &&
    opp.guestroomStatus === GUESTROOM_STATUS.NOT_REQUIRED
  ) {
    return true;
  }
  const blob = lower(
    [opp.segment, opp.title, opp.prospectCommercialLabel, opp.summaryWhat].join(" ")
  );
  return /local meetings.?catering|catering.?only|meetings & catering/.test(blob);
}

export function buildLocalMeetingsCateringResearchQueries(hotelConfig = {}) {
  const city = text(hotelConfig.city) || "market";
  const hotel = text(hotelConfig.displayName) || "hotel";
  return [
    {
      intent: "chapter_awards_dinners",
      query: `"${city}" (awards gala OR holiday luncheon OR chapter dinner OR "engineering excellence") (hotel OR banquet) -crunchbase`,
    },
    {
      intent: "nonprofit_fundraisers",
      query: `"${city}" (nonprofit OR foundation) (gala OR fundraiser OR awards) (downtown OR banquet) contact`,
    },
    {
      intent: "professional_networking",
      query: `"${city}" (bar association OR rotary OR "board of directors" luncheon) hotel catering`,
    },
    {
      intent: "exclude_venue_as_buyer",
      note: `Do not treat a nearby venue as a catering buyer for ${hotel}.`,
    },
  ];
}
