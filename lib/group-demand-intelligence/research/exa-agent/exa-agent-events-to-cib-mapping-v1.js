/**
 * Map validated Events Exa Agent deep-research → original commercialIntelligenceBrief
 * + opportunity DTO fields. Does not reuse Scaling corporate-growth boilerplate.
 */
import { createHash } from "node:crypto";
import { attachEventsOpportunityOverview } from "../../events-opportunity-overview-v1.js";
import { applyAssociationEventsGoldBrief } from "../association-events-gold-brief-v1.js";
import { RESEARCH_SEGMENT } from "../../workflow/hotel-segment-research-plan-v1.js";
import {
  applyUnifiedWhoToContactStandard,
  classifyEventsContactRole,
} from "../../unified-who-to-contact-v1.js";
import { applyUniversalTimingOverlay } from "../../universal-timing-intelligence-v1.js";

export const EXA_AGENT_EVENTS_TO_CIB_MAPPING_V1 =
  "gdi-exa-events-agent-to-cib-mapping-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function seriesKey(candidate = {}) {
  return (
    text(candidate.eventSeriesId) ||
    text(candidate.eventSeriesIdentity) ||
    text(candidate.eventIdentity) ||
    text(candidate.organizingEntity)
  );
}

export function stableEventsOpportunityId(hotelId, candidate = {}) {
  const raw = `${hotelId}|${seriesKey(candidate)}|${text(candidate.eventEdition)}`;
  const h = createHash("sha256").update(raw).digest("hex").slice(0, 12);
  return `gdi_ev_${h}`;
}

function mapSourcingStatus(candidate = {}) {
  const v = text(candidate.venueCommitmentStatus);
  if (v === "sourcing_potentially_open" || candidate.sourcingStatus === "open_or_unannounced") {
    return "Hotel Sourcing Unconfirmed";
  }
  if (v === "confirmed_competing_hotel" || v === "confirmed_this_hotel") {
    return "Venue Confirmed";
  }
  if (
    candidate.eventStatus === "historical_completed" ||
    candidate.commercialOpportunityTrack === "TRACK_D_HISTORICAL_RELATIONSHIP" ||
    candidate.commercialOpportunityTrack === "TRACK_B_RECURRING_COMPETITIVE_CAPTURE"
  ) {
    if (v === "confirmed_competing_hotel" || candidate.historicalHostHotels?.length) {
      return candidate.commercialOpportunityTrack === "TRACK_B_RECURRING_COMPETITIVE_CAPTURE" &&
        !candidate.verifiedDates
        ? "Historical Venue Only"
        : candidate.verifiedVenue
          ? "Venue Confirmed"
          : "Historical Venue Only";
    }
    return "Historical Venue Only";
  }
  return "Hotel Sourcing Unconfirmed";
}

function mapContacts(candidate = {}, deep = {}) {
  const fromDeep = Array.isArray(deep.organizerContactCandidates)
    ? deep.organizerContactCandidates
    : [];
  const fromDisc = Array.isArray(candidate.namedContacts) ? candidate.namedContacts : [];
  const merged = [...fromDeep, ...fromDisc].filter((c) => text(c?.name));
  const seen = new Set();
  const unique = [];
  for (const c of merged) {
    const k = text(c.name).toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    unique.push(c);
  }
  return unique;
}

/**
 * Build ten-section commercialIntelligenceBrief from Events Agent evidence.
 */
export function buildEventsCommercialIntelligenceBrief({
  hotel = {},
  candidate = {},
  deepResearch = {},
} = {}) {
  const hints = deepResearch.cibMappingHints || {};
  const overviewFields =
    deepResearch.eventsOpportunityOverviewFields ||
    candidate.eventsOpportunityOverviewFields ||
    {};
  const contacts = mapContacts(candidate, deepResearch);
  const primary = contacts[0] || null;
  const secondary = contacts[1] || null;
  const roleCtx = {
    researchSegment: "associations_government",
    eventName: candidate.eventIdentity,
    organizationName: candidate.organizingEntity,
  };
  const primaryRole = primary ? classifyEventsContactRole(primary, roleCtx) : null;
  const secondaryRole = secondary
    ? classifyEventsContactRole(secondary, roleCtx)
    : null;
  const sources = [
    ...(deepResearch.supportingSources || []),
    ...(candidate.supportingSources || []),
  ].filter((s) => s?.url && s?.claim);

  const what =
    text(hints.whatIsHappening) ||
    text(candidate.evidenceOfHospitalityDemand) ||
    text(candidate.eventIdentity);
  const potential =
    text(hints.potentialBusiness) ||
    text(candidate.potentialGuestroomDemand) ||
    "Hospitality demand evidenced qualitatively; published room-night volumes not available.";
  const whyHotel =
    text(hints.whyThisHotel) ||
    `Evaluate fit of ${hotel.displayName || "this hotel"} against evidenced destination and programme type — only when sourcing accessibility supports outreach.`;
  const whyNow =
    text(hints.accessibilityStatement) ||
    text(candidate.recommendedSalesAction) ||
    "Act according to track: upcoming open venue, future capture, constrained edition, or historical relationship.";
  const nextStep =
    text(hints.recommendedNextStep) ||
    text(deepResearch.findings?.recommendedSalesAction) ||
    text(candidate.recommendedSalesAction) ||
    "Verify named contact and sourcing openness before commercial outreach.";

  const uncertainties = [
    ...(deepResearch.remainingUncertainties || []),
    ...(candidate.commercialUncertainties || []),
  ].filter(Boolean);

  return {
    version: "gdi-events-commercial-intelligence-brief-v1",
    sourceMapping: EXA_AGENT_EVENTS_TO_CIB_MAPPING_V1,
    opportunityOverview:
      overviewFields.eventName && overviewFields.organizerName
        ? `${overviewFields.organizerName} — ${overviewFields.eventName}`
        : `${candidate.organizingEntity || ""} — ${candidate.eventIdentity || ""}`.replace(
            /^ — | — $/g,
            "",
          ),
    whatIsHappening: what,
    companyAndLocalActivity: what,
    potentialBusinessForHotel: potential,
    potentialBusiness: potential,
    whyThisHotel: whyHotel,
    whyActNow: whyNow,
    establishedFactsAndUncertainties: {
      facts: (sources || []).slice(0, 6).map((s) => s.claim),
      uncertainties,
    },
    whoToContact: {
      primary: primary
        ? {
            name: primary.name,
            title: primary.title || null,
            organization: primary.organization || candidate.organizingEntity,
            roleClass: primary.roleClass || primaryRole?.roleLabel || null,
            sourceUrl: primary.sourceUrl || null,
            profileUrl: primary.profileUrl || primary.sourceUrl || null,
            isNamedCommerciallyRelevant: primaryRole?.buyerEligible === true,
            contactClassificationLabel: primaryRole?.roleLabel || null,
            buyerEligibilityReason: primaryRole?.reason || null,
          }
        : null,
      secondary: secondary
        ? {
            name: secondary.name,
            title: secondary.title || null,
            organization: secondary.organization || candidate.organizingEntity,
            roleClass: secondary.roleClass || secondaryRole?.roleLabel || null,
            sourceUrl: secondary.sourceUrl || null,
            profileUrl: secondary.profileUrl || secondary.sourceUrl || null,
            isNamedCommerciallyRelevant: secondaryRole?.buyerEligible === true,
            contactClassificationLabel: secondaryRole?.roleLabel || null,
            buyerEligibilityReason: secondaryRole?.reason || null,
          }
        : null,
      narrative: text(hints.whoToContact) || null,
    },
    recommendedNextStep: nextStep,
    supportingEvidence: sources.map((s) => ({
      claim: s.claim,
      url: s.url,
      factVsInference: s.factVsInference || null,
    })),
    hotelValidationActionOutcome: {
      validation: "Pending hotel team review",
      action: nextStep,
      outcome: null,
    },
    eventsOpportunityOverviewFields: overviewFields,
    segment: "associations_government",
    customerUmbrella: "Association / Conference / Events",
    scalingBoilerplateExcluded: true,
  };
}

/**
 * Candidate + deep research → canonical Events opportunity DTO.
 */
export function mapEventsAgentResultToOpportunity({
  hotel = {},
  candidate = {},
  deepResearch = null,
  feedbackPreserved = null,
} = {}) {
  const hotelId = hotel.hotelId || hotel.id;
  const id = stableEventsOpportunityId(hotelId, candidate);
  const contacts = mapContacts(candidate, deepResearch || {});
  const primary = contacts[0] || null;
  const cib = buildEventsCommercialIntelligenceBrief({
    hotel,
    candidate,
    deepResearch: deepResearch || {},
  });

  let opp = {
    id,
    opportunityId: id,
    hotelId,
    researchSegment: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
    customerSegmentUmbrella: "Association / Conference / Events",
    organizationName: text(candidate.organizingEntity),
    eventName: text(candidate.eventIdentity),
    eventDisplayName: text(candidate.eventIdentity),
    eventSeriesId: candidate.eventSeriesId || null,
    eventSeriesIdentity: candidate.eventSeriesIdentity || null,
    eventEdition: candidate.eventEdition || null,
    title: text(candidate.eventIdentity),
    commercialOpportunityTrack: candidate.commercialOpportunityTrack,
    discoveryDisposition: candidate.discoveryDisposition,
    eventStatus: candidate.eventStatus,
    recurrenceStatus: candidate.recurrenceStatus,
    futureEditionStatus: candidate.futureEditionStatus,
    venueCommitmentStatus: candidate.venueCommitmentStatus,
    venueSourcingStatusLabel: mapSourcingStatus(candidate),
    hotelSourcingStatusLabel: mapSourcingStatus(candidate),
    verifiedDates: candidate.verifiedDates || null,
    eventStartDate: candidate.verifiedStartDate || candidate.verifiedDates || null,
    destination: candidate.destination || null,
    verifiedVenue: candidate.verifiedVenue || null,
    historicalHostHotels: candidate.historicalHostHotels || [],
    incumbentVenue: candidate.incumbentVenue || null,
    attendancePublished: candidate.attendancePublished ?? null,
    peakRoomsPublished: candidate.peakRoomsPublished ?? null,
    summaryWhat: cib.whatIsHappening,
    whatIsHappening: cib.whatIsHappening,
    contactName: primary?.name || null,
    contactRole: primary?.title || null,
    contactEmail: null,
    primaryContact: primary
      ? {
          name: primary.name,
          title: primary.title || null,
          role: primary.title || null,
          organization: primary.organization || candidate.organizingEntity || null,
          profileUrl: primary.profileUrl || primary.sourceUrl || null,
          sourceUrl: primary.sourceUrl || null,
          whyThisContact: null,
          roleClass: primary.roleClass || null,
        }
      : null,
    secondaryContact: contacts[1]
      ? {
          name: contacts[1].name,
          title: contacts[1].title || null,
          role: contacts[1].title || null,
          organization:
            contacts[1].organization || candidate.organizingEntity || null,
          profileUrl: contacts[1].profileUrl || contacts[1].sourceUrl || null,
          sourceUrl: contacts[1].sourceUrl || null,
        }
      : null,
    namedContacts: contacts,
    sources: cib.supportingEvidence,
    evidence: cib.supportingEvidence,
    commercialIntelligenceBrief: cib,
    customerVisible: false,
    hotelFacingEligible: false,
    fixtureLabel:
      /SYNTHETIC|example\.fixture/i.test(JSON.stringify(candidate.supportingSources || []))
        ? "FIXTURE_OR_SYNTHETIC"
        : "STORED_CASE_SHAPE",
    feedback: feedbackPreserved || null,
    identityFingerprint: seriesKey(candidate),
  };

  // Gold brief for Events qualification / drawer shared path
  opp = applyAssociationEventsGoldBrief(opp, { hotel });
  // Prefer original CIB commercial sections over AEGB Scaling-adjacent filler where mapped
  opp.commercialIntelligenceBrief = {
    ...opp.associationEventsGoldBrief,
    ...cib,
    opportunityOverview: cib.opportunityOverview,
    whatIsHappening: cib.whatIsHappening,
    potentialBusinessForHotel: cib.potentialBusinessForHotel,
    whyThisHotel: cib.whyThisHotel,
    whoToContact: cib.whoToContact,
    recommendedNextStep: cib.recommendedNextStep,
    supportingEvidence: cib.supportingEvidence,
  };
  opp = attachEventsOpportunityOverview(opp);
  // Shared Scaling gold-standard Who to Contact presentation (DTO only; no paid calls).
  opp = applyUnifiedWhoToContactStandard(opp);
  // Universal Timing Intelligence V1 — recover verified dates into structured pill fields.
  opp = applyUniversalTimingOverlay(opp);
  return opp;
}

/**
 * Weekly idempotent merge by eventSeriesId — preserve feedback, update edition fields.
 */
export function mergeWeeklyEventsOpportunities(prior = [], next = []) {
  const byKey = new Map();
  for (const o of prior) {
    byKey.set(o.eventSeriesId || o.identityFingerprint || o.id, { ...o });
  }
  const merged = [];
  const seen = new Set();
  for (const n of next) {
    const key = n.eventSeriesId || n.identityFingerprint || n.id;
    seen.add(key);
    const prev = byKey.get(key);
    if (!prev) {
      merged.push(applyUniversalTimingOverlay(n));
      continue;
    }
    merged.push(
      applyUniversalTimingOverlay({
        ...prev,
        ...n,
        id: prev.id,
        opportunityId: prev.opportunityId || prev.id,
        feedback: prev.feedback || n.feedback || null,
        historicalEditionsRetained: [
          ...(prev.historicalEditions || []),
          ...(n.historicalEditions || []),
        ],
        weeklyRefresh: true,
        priorEditionSnapshot: {
          verifiedDates: prev.verifiedDates,
          venueCommitmentStatus: prev.venueCommitmentStatus,
          eventStatus: prev.eventStatus,
        },
      }),
    );
  }
  // Retain prior series not in next (canceled/discontinued still identity-stable)
  for (const [key, prev] of byKey) {
    if (!seen.has(key)) {
      merged.push(
        applyUniversalTimingOverlay({
          ...prev,
          weeklyRefreshRetainedWithoutUpdate: true,
        }),
      );
    }
  }
  return merged;
}
