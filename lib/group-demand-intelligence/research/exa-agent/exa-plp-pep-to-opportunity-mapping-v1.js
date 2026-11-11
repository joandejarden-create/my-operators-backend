/**
 * Map live PLP / PEP Exa deep-research results → local-shadow opportunity DTOs.
 * Presentation layers applied separately; no invented volumes or drive times.
 */
import { createHash } from "node:crypto";
import {
  PREFERRED_LODGING_SUBTYPE,
  PREFERRED_LODGING_SEGMENT_ID,
  normalizeVenueOnsiteLodgingStatus,
  PARTNERSHIP_RELATIONSHIP_STATUS,
} from "../../preferred-lodging-partnership/preferred-lodging-partnership-contract-v1.js";
import { applyPreferredLodgingPresentation } from "../../preferred-lodging-partnership/preferred-lodging-presentation-v1.js";
import {
  PROFESSIONAL_EVENT_PLANNER_SUBTYPE,
  PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID,
  PLANNER_PARTNERSHIP_STATUS,
} from "../../professional-event-planners/professional-event-planner-contract-v1.js";
import { applyProfessionalEventPlannerPresentation } from "../../professional-event-planners/professional-event-planner-presentation-v1.js";

export const EXA_PLP_PEP_TO_OPPORTUNITY_MAPPING_V1 =
  "gdi-exa-plp-pep-to-opportunity-mapping-v1";

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function stableId(prefix, hotelId, key) {
  const h = createHash("sha256")
    .update(`${hotelId}|${key}`)
    .digest("hex")
    .slice(0, 12);
  return `${prefix}_${h}`;
}

function mapNamedContacts(arr = []) {
  return (Array.isArray(arr) ? arr : [])
    .map((c) => {
      if (!c || typeof c !== "object") return null;
      const name = text(c.name);
      if (!name || !/\s/.test(name)) return null;
      return {
        name,
        title: text(c.title) || null,
        relevance: text(c.whyRelevant || c.relevance) || null,
        publicProfileUrl: text(c.sourceUrl || c.publicProfileUrl || c.url) || null,
        organization: text(c.organization) || null,
      };
    })
    .filter(Boolean);
}

function mapSources(deep = {}, candidate = {}) {
  const fromDeep = (Array.isArray(deep.supportingSources)
    ? deep.supportingSources
    : []
  ).map((s) => ({
    label: text(s.claim) || "Supporting source",
    url: text(s.url),
  }));
  const fromDisc = (
    Array.isArray(candidate.evidenceUrls) ? candidate.evidenceUrls : []
  ).map((url) => ({
    label: "Discovery evidence URL",
    url: text(url),
  }));
  const seen = new Set();
  const out = [];
  for (const s of [...fromDeep, fromDisc]) {
    if (!s.url || seen.has(s.url)) continue;
    seen.add(s.url);
    out.push(s);
  }
  return out;
}

/** Prefer short discovery name; truncate verbose Agent identity prose. */
function shortEntityName(preferred, identityFallback, maxLen = 80) {
  const pref = text(preferred);
  if (pref && pref.length <= maxLen && !/[.;:]/.test(pref.slice(0, 40))) {
    return pref;
  }
  if (pref) return pref.length > maxLen ? `${pref.slice(0, maxLen - 1).trim()}…` : pref;
  const id = text(identityFallback);
  if (!id) return "";
  // Take text before em dash / first sentence boundary when Agent returns a paragraph.
  const cut = id.split(/\s+[—–-]\s+|[.;]\s+/)[0] || id;
  const cleaned = text(cut);
  return cleaned.length > maxLen
    ? `${cleaned.slice(0, maxLen - 1).trim()}…`
    : cleaned;
}

export function mapPlpDeepToOpportunity({
  hotel = {},
  candidate = {},
  deep = {},
} = {}) {
  const hotelId = hotel.hotelId || hotel.id;
  const venueName = shortEntityName(
    candidate.venueName || candidate.name,
    deep.venueIdentity,
  );
  if (!venueName) return null;

  const contacts = mapNamedContacts(deep.namedContacts);
  const primary = contacts[0] || null;
  const secondary = contacts[1] || null;
  const lodging = normalizeVenueOnsiteLodgingStatus(deep.onSiteLodgingStatus);
  const sources = mapSources(deep, candidate);
  const facts = Array.isArray(deep.establishedFacts)
    ? deep.establishedFacts.map(text).filter(Boolean)
    : [];
  const uncertainties = Array.isArray(deep.uncertainties)
    ? deep.uncertainties.map(text).filter(Boolean)
    : [];
  const advantages = Array.isArray(deep.advantages)
    ? deep.advantages.map(text).filter(Boolean)
    : [];
  const limitations = Array.isArray(deep.limitations)
    ? deep.limitations.map(text).filter(Boolean)
    : [];

  const seed = {
    id: stableId("gdi_plp_live", hotelId, venueName),
    hotelId,
    preferredLodgingPartnershipV1: true,
    preferredLodgingMexicoLiveWaveV1: true,
    demandOpportunitySubtype: PREFERRED_LODGING_SUBTYPE,
    demandSegmentId: PREFERRED_LODGING_SEGMENT_ID,
    opportunityType: PREFERRED_LODGING_SUBTYPE,
    demandSignalType: PREFERRED_LODGING_SUBTYPE,
    researchSegment: "weddings_social_private_events",
    organizationName: venueName,
    venueName,
    venueAddress: text(deep.venueAddress || candidate.venueAddress) || null,
    venueType: text(candidate.venueType) || null,
    contactUrl: text(deep.website || candidate.website) || null,
    onSiteLodgingStatus: lodging,
    eventActivityEvidenceStatus: text(deep.eventActivitySummary)
      ? "EVIDENCE_SUPPORTED"
      : "UNKNOWN",
    eventTypes: text(candidate.eventTypes) || null,
    maxCapacity: text(deep.capacityIfPublished) || null,
    summaryWhat:
      text(deep.eventActivitySummary) ||
      `Independent event venue researched as Preferred Lodging Partnership prospect for ${text(hotel.name)}.`,
    summaryWhyHotel:
      text(deep.hotelFitThesis) ||
      "Hotel fit thesis pending further qualification with venue contacts.",
    hotelOpportunityThesis:
      text(deep.recommendedNextStep) ||
      "Continue partnership development research; do not invent referral volumes.",
    standingOpportunity: true,
    eventDateDisplay: "Ongoing Partnership",
    primaryContact: primary,
    secondaryContact: secondary,
    namedContacts: contacts,
    sources,
    commercialPriority: "WATCHLIST",
    commercialStage: "Research Continuation",
    partnerStatusLabel: PARTNERSHIP_RELATIONSHIP_STATUS.PARTNERSHIP_DEVELOPMENT,
    timing: "Ongoing partnership development",
    segmentDisplayLabel: "Social Groups & Special Events",
    opportunityTypeLabel: "Preferred Lodging Partnership",
    detailLayout: "PREFERRED_LODGING_PARTNERSHIP_SHARED_DRAWER",
    title: venueName,
    displayTitle: venueName,
    drawerHeaderTitle: venueName,
    customerVisible: true,
    doesNotAffectProduction: true,
    localShadowOnly: true,
    localShadowPortfolio: true,
    liveExa: true,
    liveCampaignV1Shadow: true,
    stageEShadowPreview: true,
    commercialIntelligenceBrief: {
      opportunityOverview: text(deep.eventActivitySummary) || venueName,
      whatIsHappening:
        text(deep.eventActivitySummary) ||
        "Venue event activity summarized from live Exa deep research.",
      potentialBusinessForThisHotel:
        text(deep.hotelFitThesis) ||
        "Preferred lodging referral / block opportunity pending qualification.",
      whyThisHotel: { advantages, limitations },
      whyActNow: text(deep.recommendedNextStep) || "Research continuation",
      establishedFacts: facts,
      uncertainties,
      qualificationQuestions: Array.isArray(deep.qualificationQuestions)
        ? deep.qualificationQuestions.map(text).filter(Boolean)
        : [],
      recommendedNextStep: text(deep.recommendedNextStep) || null,
    },
  };

  return applyPreferredLodgingPresentation(seed, {
    hotelId,
    name: hotel.name,
  });
}

export function mapPepDeepToOpportunity({
  hotel = {},
  candidate = {},
  deep = {},
} = {}) {
  const hotelId = hotel.hotelId || hotel.id;
  const plannerName = shortEntityName(
    candidate.plannerCompanyName || candidate.name || candidate.companyName,
    deep.plannerIdentity,
  );
  if (!plannerName) return null;

  const contacts = mapNamedContacts(deep.namedContacts);
  const primary = contacts[0] || null;
  const secondary = contacts[1] || null;
  const sources = mapSources(deep, candidate);
  const facts = Array.isArray(deep.establishedFacts)
    ? deep.establishedFacts.map(text).filter(Boolean)
    : [];
  const uncertainties = Array.isArray(deep.uncertainties)
    ? deep.uncertainties.map(text).filter(Boolean)
    : [];
  const advantages = Array.isArray(deep.advantages)
    ? deep.advantages.map(text).filter(Boolean)
    : [];
  const limitations = Array.isArray(deep.limitations)
    ? deep.limitations.map(text).filter(Boolean)
    : [];
  const revenue = Array.isArray(deep.revenueMechanisms)
    ? deep.revenueMechanisms.map(text).filter(Boolean)
    : [];
  const markets = Array.isArray(deep.operatingMarkets)
    ? deep.operatingMarkets.map(text).filter(Boolean)
    : Array.isArray(candidate.operatingMarkets)
      ? candidate.operatingMarkets.map(text).filter(Boolean)
      : [];

  const seed = {
    id: stableId("gdi_pep_live", hotelId, plannerName),
    hotelId,
    professionalEventPlannerPartnershipV1: true,
    professionalEventPlannerMexicoLiveWaveV1: true,
    demandOpportunitySubtype: PROFESSIONAL_EVENT_PLANNER_SUBTYPE,
    demandSegmentId: PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID,
    opportunityType: PROFESSIONAL_EVENT_PLANNER_SUBTYPE,
    demandSignalType: PROFESSIONAL_EVENT_PLANNER_SUBTYPE,
    researchSegment: "weddings_social_private_events",
    organizationName: plannerName,
    plannerName,
    plannerSpecialization:
      text(deep.specialization || candidate.specialization) || null,
    officeLocation:
      text(deep.officeLocation || candidate.officeLocation) || null,
    operatingMarkets: markets,
    verifiedServiceArea: markets.join(" / ") || null,
    contactUrl: text(deep.website || candidate.officialWebsite) || null,
    eventTypes: text(candidate.eventTypes) || null,
    destinationEvidence: text(
      deep.destinationEvidence || candidate.destinationEvidence,
    )
      ? [
          {
            label: text(
              deep.destinationEvidence || candidate.destinationEvidence,
            ),
            verified: true,
          },
        ]
      : [],
    hotelInfluenceEvidence: Boolean(
      text(deep.commercialInfluenceSummary || candidate.hotelInfluenceSignals),
    ),
    multipleEventEvidence: Boolean(text(deep.eventPortfolioSummary)),
    plannerActivityVerified: Boolean(text(deep.eventPortfolioSummary)),
    revenueMechanisms: revenue.length ? revenue : ["OVERNIGHT_REFERRALS"],
    summaryWhat:
      text(deep.eventPortfolioSummary) ||
      `Professional event planner researched as partnership prospect for ${text(hotel.name)}.`,
    summaryWhyHotel:
      text(deep.hotelFitThesis) ||
      "Hotel fit thesis pending further qualification with planner contacts.",
    hotelOpportunityThesis:
      text(deep.recommendedNextStep) ||
      "Continue planner partnership research; do not invent volumes.",
    standingOpportunity: true,
    eventDateDisplay: "Ongoing Partnership",
    primaryContact: primary,
    secondaryContact: secondary,
    namedContacts: contacts,
    sources,
    commercialPriority: "WATCHLIST",
    commercialStage: "Research Continuation",
    plannerPartnershipStatus: PLANNER_PARTNERSHIP_STATUS.RESEARCH_CONTINUATION,
    timing: "Ongoing account development",
    segmentDisplayLabel: "Social Groups & Special Events",
    opportunityTypeLabel: "Professional Event Planner Partnership",
    detailLayout: "PROFESSIONAL_EVENT_PLANNER_SHARED_DRAWER",
    title: plannerName,
    displayTitle: plannerName,
    drawerHeaderTitle: plannerName,
    customerVisible: true,
    doesNotAffectProduction: true,
    localShadowOnly: true,
    localShadowPortfolio: true,
    liveExa: true,
    liveCampaignV1Shadow: true,
    stageEShadowPreview: true,
    commercialIntelligenceBrief: {
      opportunityOverview: text(deep.eventPortfolioSummary) || plannerName,
      whatIsHappening:
        text(deep.eventPortfolioSummary) ||
        "Planner portfolio summarized from live Exa deep research.",
      potentialBusinessForThisHotel:
        text(deep.hotelFitThesis) ||
        "Planner-influenced guestroom / banquet opportunity pending qualification.",
      whyThisHotel: { advantages, limitations },
      whyActNow: text(deep.recommendedNextStep) || "Research continuation",
      establishedFacts: facts,
      uncertainties,
      qualificationQuestions: Array.isArray(deep.qualificationQuestions)
        ? deep.qualificationQuestions.map(text).filter(Boolean)
        : [],
      recommendedNextStep: text(deep.recommendedNextStep) || null,
      commercialInfluenceSummary: text(deep.commercialInfluenceSummary) || null,
    },
  };

  return applyProfessionalEventPlannerPresentation(seed, {
    hotelId,
    name: hotel.name,
  });
}
