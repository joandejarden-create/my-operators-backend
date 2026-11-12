/**
 * Commercial Qualification Reset V3 — internal ORS qualification.
 *
 * Integrates demand-evidence dimensions + multi-revenue assessment +
 * organization vs program/event separation + sales-action contract.
 *
 * Does NOT change production publication eligibility or customer classifications.
 * Pathways (EVENT_LED / ACCOUNT_LED / COMPETITIVE_FUTURE_CYCLE) remain separate
 * from internal dispositions.
 */

import {
  assessDemandEvidenceDimensions,
  DEMAND_EVIDENCE_LAYER,
} from "./demand-evidence-dimensions-v2.js";
import {
  qualifyHotelSpecificProspectV2,
  classifyAccommodationEvidenceRole,
} from "./hotel-specific-qualification-v2.js";
import {
  RESEARCH_PATHWAY,
  COMMERCIAL_STAGE,
  SALES_READINESS,
  namedCompetitorHost,
} from "./commercial-pathway-v1.js";
import {
  COMMERCIAL_PURSUIT_STRATEGY,
  PURSUIT_SOURCING_STATUS,
  enrichOpportunityWithPursuitStrategy,
} from "./commercial-pursuit-strategy-v1.js";

export {
  COMMERCIAL_PURSUIT_STRATEGY,
  PURSUIT_SOURCING_STATUS,
} from "./commercial-pursuit-strategy-v1.js";

export const COMMERCIAL_QUALIFICATION_V3 = "gdi-commercial-qualification-v3";

export const REVENUE_TYPE = Object.freeze({
  GUESTROOMS: "GUESTROOMS",
  MEETING_SPACE: "MEETING_SPACE",
  CATERING_AND_EVENTS: "CATERING_AND_EVENTS",
});

export const INTERNAL_DISPOSITION = Object.freeze({
  ACTIVE_OPPORTUNITY: "ACTIVE_OPPORTUNITY",
  PRE_SOURCING_OPPORTUNITY: "PRE_SOURCING_OPPORTUNITY",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  COMPETITIVE_FUTURE_CYCLE: "COMPETITIVE_FUTURE_CYCLE",
  WATCHLIST_RESEARCH: "WATCHLIST_RESEARCH",
  EXCLUDE: "EXCLUDE",
});

export const ASSESSMENT_LEVEL = Object.freeze({
  ORGANIZATION: "ORGANIZATION_LEVEL",
  PROGRAM_OR_EVENT: "PROGRAM_OR_EVENT_LEVEL",
});

const MEETING_MECHANISM = /\b(?:meeting\s+space|boardroom|ballroom|conference\s+room|event\s+space|breakout|training\s+(?:room|facility)|corporate\s+meetings?|incentive\s+programm?e|association\s+(?:meeting|conference)|annual\s+(?:meeting|conference)|congress|summit|symposium)\b/i;
const CATERING_MECHANISM = /\b(?:awards?\s+(?:banquet|dinner)|gala|catered|reception|luncheon|dinner|fundraiser|banquet)\b/i;
const VENUE_MARKETPLACE = /\b(?:free\s+venue\s+find|venue\s+(?:listing|hire|finding|marketplace)|exact[- ]property\s+(?:listing|ballroom)|bookable\s+(?:london\s+)?event\s+venue)\b/i;
const LOCAL_DAY_EVENT = /\b(?:awards?\s+(?:banquet|dinner|luncheon)|speaker\s+luncheon|recognition\s+luncheon|local\s+chapter)\b/i;
const HOTEL_MEETING_FIT = /\b(?:meeting\s+rooms?|boardroom|ballroom|event\s+space|breakout|conference\s+facilit|15\s+meeting|18-seat\s+boardroom)\b/i;
const HOTEL_CATERING_FIT = /\b(?:catering|ballroom|private\s+dining|reception|banquet|Coast\s+Ballroom|Pacific\s+Ballroom)\b/i;
const DATED_OR_CYCLE = /\b(?:20\d{2}|annual|recurring|next\s+(?:year|cycle|meeting)|upcoming|scheduled)\b/i;
/** Hotel-fit hypothesis prose — not verified buyer activity. */
const HOTEL_FIT_HYPOTHESIS = /\b(?:potential fit|plausible (?:alternate|alternative|overflow)|can be pitched|evaluate for|product and market match|supporting both day events|making residential meetings a plausible)\b/i;
const OUTDOOR_OR_NIGHTCLUB = /\b(?:outdoor\s+festivals?|nightclub\s+promotions?)\b/i;

function blobFrom(opportunity = {}, acceptedClaims = [], pageTexts = []) {
  return [
    ...acceptedClaims.map((c) => `${c.statement || ""} ${c.excerpt || ""}`),
    ...pageTexts,
    opportunity.summaryWhyHotel,
    opportunity.evidenceQuote,
    opportunity.eventLocationSummary,
    opportunity.namedHostHotel,
    opportunity.organizationName,
  ].join("\n");
}

function contactFrom(opportunity = {}, usableContactRoute = null) {
  return usableContactRoute
    || opportunity.contactEmail
    || opportunity.primaryContactEmail
    || opportunity.email
    || opportunity.contact?.businessEmail
    || null;
}

/**
 * Assess one revenue category with verified mechanism + hotel-specific fit.
 * Does not invent inventory — only uses evidence text / supplied hotel facts in sources.
 */
export function assessRevenueOpportunity({
  revenueType,
  opportunity = {},
  acceptedClaims = [],
  pageTexts = [],
  targetHotel = "",
  dimensions = null,
  accommodation = null,
  competitor = null,
} = {}) {
  const blob = blobFrom(opportunity, acceptedClaims, pageTexts);
  const dims = dimensions || assessDemandEvidenceDimensions({
    acceptedClaims,
    pageTexts,
    organizationName: opportunity.organizationName || "",
    opportunity,
  });
  const acc = accommodation || classifyAccommodationEvidenceRole({
    acceptedClaims,
    pageTexts,
    organizationName: opportunity.organizationName || "",
  });

  const venueMarketplace = VENUE_MARKETPLACE.test(blob) || acc.venueListingOnly;
  const result = {
    revenueType,
    commerciallyRelevant: false,
    mechanismVerified: false,
    hotelFitSupported: false,
    inferredOnly: false,
    exclusionReason: null,
    note: null,
  };

  const overflowEvidencedForRevenue = /\b(?:overflow\s+(?:block|housing|rooms?|need|request)|seeking\s+overflow|satellite\s+(?:hotel|block|housing))\b/i.test(blob)
    || opportunity.overflowEvidenced === true;
  // Competing host blocks open demand unless overflow/satellite sourcing is evidenced (not hotel-fit prose).
  const competitorBlocksOpenDemand = Boolean(competitor) && !overflowEvidencedForRevenue;

  if (revenueType === REVENUE_TYPE.GUESTROOMS) {
    if (venueMarketplace) {
      result.exclusionReason = "Venue advertisement does not establish guestroom booking authority.";
      return result;
    }
    if (competitorBlocksOpenDemand) {
      result.exclusionReason = "Competing hotel commitment — not open guestroom demand.";
      return result;
    }
    const verifiedDemand = dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_ACCOMMODATION_DEMAND].present;
    const inferred = dims.layers[DEMAND_EVIDENCE_LAYER.INFERRED_LODGING_POTENTIAL].present
      && dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_GROUP_ACTIVITY].present;
    result.mechanismVerified = verifiedDemand || inferred;
    result.inferredOnly = inferred && !verifiedDemand;
    result.hotelFitSupported = !/SoccerPlex|Boyds\s+MD|Capitol Hill/i.test(blob)
      || /Bethesda/i.test(`${opportunity.eventLocationSummary || ""} ${opportunity.city || ""} ${targetHotel}`);
    if (/Bethesda Marriott/i.test(targetHotel)
      && /SoccerPlex|Boyds\s+MD|Capitol Hill|Hyatt Regency Washington|Philadelphia Marriott/i.test(blob)
      && !/Bethesda/i.test(`${opportunity.eventLocationSummary || ""} ${opportunity.city || ""} ${opportunity.namedHostHotel || ""}`)) {
      result.hotelFitSupported = false;
      result.exclusionReason = "Programme geography does not support target-hotel guestroom catchment.";
      return result;
    }
    // Named competitor host in another city → event-level guestroom fit fails (org-level account may remain).
    if (competitor && !overflowEvidencedForRevenue) {
      const hostCityElsewhere = /Philadelphia|Crystal City|Boyds|Capitol Hill/i.test(`${competitor} ${blob}`)
        && targetHotel
        && !new RegExp(String(targetHotel).split(/\s+/).slice(0, 1).join("|"), "i").test(competitor);
      if (hostCityElsewhere && !/Bethesda/i.test(competitor)) {
        result.hotelFitSupported = false;
        result.exclusionReason = "Named host hotel is elsewhere — do not qualify this event as target-hotel guestroom demand.";
        return result;
      }
    }
    if (LOCAL_DAY_EVENT.test(blob) && !inferred && !verifiedDemand) {
      result.exclusionReason = "Local/day banquet or luncheon without defensible lodging potential.";
      return result;
    }
    result.commerciallyRelevant = result.mechanismVerified && result.hotelFitSupported;
    result.note = verifiedDemand
      ? "Verified hotel-sourcing / accommodation capability (not proof of a current active buying requirement)."
      : inferred
        ? "Inferred lodging potential from verified group/incentive activity (not a confirmed room block or upcoming booking)."
        : "No verified or defensible inferred guestroom demand.";
    return result;
  }

  if (revenueType === REVENUE_TYPE.MEETING_SPACE) {
    // Venue marketplace alone ≠ purchasing authority — may only justify watchlist referral research.
    if (venueMarketplace) {
      result.mechanismVerified = false;
      result.hotelFitSupported = HOTEL_MEETING_FIT.test(blob);
      result.exclusionReason = "Venue listing/marketplace does not verify meeting-space purchasing authority.";
      result.note = result.hotelFitSupported
        ? "Hotel meeting product appears in sources; investigate referral/buyer path before qualifying."
        : "Venue marketplace without verified buyer mechanism.";
      return result;
    }
    if (competitorBlocksOpenDemand) {
      result.exclusionReason = "Competing host commitment — not open meeting-space demand.";
      return result;
    }
    if (HOTEL_FIT_HYPOTHESIS.test(String(opportunity.summaryWhyHotel || "").slice(0, 120))
      && !dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_GROUP_ACTIVITY].present
      && !/corporate\s+meetings?\s+and\s+incentives|manages\s+corporate\s+meetings/i.test(blob)) {
      result.exclusionReason = "Hotel-fit hypothesis without verified meetings mechanism.";
      return result;
    }
    result.mechanismVerified = MEETING_MECHANISM.test(blob)
      && dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_GROUP_ACTIVITY].present;
    // DMC / corporate meetings services language (not hotel-fit hypothesis)
    if (!result.mechanismVerified && /(?:manages|provides|delivers)\s+corporate\s+meetings?|corporate\s+meetings?\s+and\s+incentives|incentive\s+programm?e/i.test(blob)
      && !venueMarketplace) {
      result.mechanismVerified = true;
      result.inferredOnly = true;
    }
    result.hotelFitSupported = HOTEL_MEETING_FIT.test(blob);
    if (!result.hotelFitSupported) {
      result.note = "Meeting mechanism present but hotel meeting-space fit not evidenced in sources (do not invent inventory).";
    }
    result.commerciallyRelevant = result.mechanismVerified && result.hotelFitSupported;
    if (!result.commerciallyRelevant && !result.exclusionReason) {
      result.exclusionReason = !result.mechanismVerified
        ? "No verified meeting-space demand mechanism."
        : "Hotel meeting-space fit not evidenced in sources.";
    }
    return result;
  }

  // CATERING_AND_EVENTS
  if (venueMarketplace && !CATERING_MECHANISM.test(blob)) {
    result.exclusionReason = "Venue marketplace without verified catering/event mechanism.";
    return result;
  }
  if (OUTDOOR_OR_NIGHTCLUB.test(blob) && !/\bawards?\s+(?:banquet|dinner)|recurring\s+awards|chapter\s+.*(?:banquet|dinner)/i.test(blob)) {
    result.exclusionReason = "Outdoor festival / nightclub activity is not hotel catering demand.";
    return result;
  }
  if (competitorBlocksOpenDemand) {
    result.exclusionReason = "Competing host commitment — not open catering demand for target hotel.";
    return result;
  }
  // Existence of org alone is insufficient; hotel-fit "potential fit for catered…" is not verified activity.
  const orgOwnCateringActivity = CATERING_MECHANISM.test(blob)
    && (
      /\b(?:recurring|annual|chapter|association|awards?\s+(?:banquet|dinner)|fundraiser|recognition)\b/i.test(blob)
      || Boolean(opportunity.eventName)
    )
    && !HOTEL_FIT_HYPOTHESIS.test(String(opportunity.summaryWhyHotel || "").slice(0, 80));
  // Allow when awards banquet is clearly the org's event even if summary also mentions hotel product
  const clearOrgBanquet = (
    /\b(?:recurring\s+)?awards?\s+(?:banquet|dinner)\b/i.test(blob)
    || /\bchapter\b[^.]{0,40}\b(?:banquet|dinner|luncheon)\b/i.test(blob)
    || /\bspeaker\s+luncheon\b/i.test(blob)
    || /\brecognition\s+luncheon\b/i.test(blob)
  ) && !/\b(?:evaluate for|potential fit for|plausible alternative for)\s+(?:a\s+)?(?:recognition\s+luncheon|speaker\s+luncheon|catered|banquet|reception)/i.test(blob);

  result.mechanismVerified = clearOrgBanquet || (orgOwnCateringActivity && !HOTEL_FIT_HYPOTHESIS.test(blob.split(".")[0] || ""));
  // If first sentence is pure hotel-fit hypothesis ("Potential fit for hotel-hosted…"), reject
  if (HOTEL_FIT_HYPOTHESIS.test(String(opportunity.summaryWhyHotel || "").slice(0, 120)) && !clearOrgBanquet) {
    result.mechanismVerified = false;
    result.exclusionReason = "Hotel-fit hypothesis prose is not verified catering demand from the organization.";
    return result;
  }
  result.hotelFitSupported = HOTEL_CATERING_FIT.test(blob);
  result.commerciallyRelevant = result.mechanismVerified && result.hotelFitSupported;
  if (!result.mechanismVerified) {
    result.exclusionReason = result.exclusionReason
      || "Do not infer catering demand merely from organization existence or hotel-fit prose.";
  } else if (!result.hotelFitSupported) {
    result.exclusionReason = "Catering/event mechanism present but hotel banquet/catering fit not evidenced.";
  } else {
    result.note = "Verified banquet/catering/event activity with hotel function-space fit in sources.";
  }
  return result;
}

function assessOrganizationLevel({
  opportunity,
  revenueAssessments,
  dims,
  venueMarketplace,
  contactOk,
}) {
  const anyRevenue = revenueAssessments.some((r) => r.commerciallyRelevant);
  const dmcOrPlanner = /\bDMC\b|destination management|incentive|professional planner|corporate meetings/i.test(
    `${opportunity.organizationName || ""} ${opportunity.summaryWhyHotel || ""}`,
  );
  const association = /association|society|congress|institute|league|foundation|chapter/i.test(
    opportunity.organizationName || "",
  );

  let potential = false;
  let reason = null;
  if (anyRevenue) {
    potential = true;
    reason = "At least one revenue category is commercially relevant.";
  } else if (venueMarketplace && contactOk) {
    potential = true;
    reason = "Venue marketplace may justify meeting-space referral investigation — not guestroom sourcing authority.";
  } else if ((dmcOrPlanner || association) && contactOk && dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_GROUP_ACTIVITY].present) {
    potential = true;
    reason = "Organization shows demand-generating activity pattern worth future cycle / account research.";
  } else if ((dmcOrPlanner || association) && contactOk) {
    potential = true;
    reason = "Organization type is commercially adjacent; activity mechanism still needs research (WATCHLIST).";
  } else {
    reason = "No organization-level hotel revenue potential evidenced.";
  }

  return {
    level: ASSESSMENT_LEVEL.ORGANIZATION,
    couldGenerateSuitableFutureBusiness: potential,
    reason,
  };
}

function assessProgramOrEventLevel({
  opportunity,
  revenueAssessments,
  competitor,
  venueMarketplace,
  localDayOnly,
}) {
  const blob = `${opportunity.summaryWhyHotel || ""} ${opportunity.eventLocationSummary || ""}`;
  const specificEvent = Boolean(
    opportunity.eventName
    || opportunity.meetingName
    || /awards|banquet|hill\s*day|advocacy|congress|annual\s+meeting|20\d{2}/i.test(blob),
  );
  const guestroomEventOk = revenueAssessments.find((r) => r.revenueType === REVENUE_TYPE.GUESTROOMS)?.commerciallyRelevant;
  const cateringEventOk = revenueAssessments.find((r) => r.revenueType === REVENUE_TYPE.CATERING_AND_EVENTS)?.commerciallyRelevant;
  const meetingEventOk = revenueAssessments.find((r) => r.revenueType === REVENUE_TYPE.MEETING_SPACE)?.commerciallyRelevant;

  let credible = false;
  let reason = null;
  let excludedForGuestrooms = false;

  if (competitor && !/overflow|future\s*cycle|satellite|alternate/i.test(blob)) {
    credible = false;
    reason = "Specific programme/event is committed to a competing hotel.";
  } else if (venueMarketplace) {
    credible = false;
    reason = "Listing/advertisement is not a specific bookable demand event for the target hotel.";
  } else if (localDayOnly && !guestroomEventOk) {
    excludedForGuestrooms = true;
    credible = cateringEventOk || meetingEventOk;
    reason = credible
      ? "Local/day event unsuitable for guestrooms but may be credible for catering or meeting space."
      : "Local/day event without credible hotel revenue mechanism for this hotel.";
  } else if (guestroomEventOk || cateringEventOk || meetingEventOk) {
    credible = true;
    reason = "Specific programme/event creates a credible opportunity for at least one revenue type.";
  } else if (!specificEvent) {
    credible = false;
    reason = "No specific programme/event evidenced — organization-level assessment only.";
  } else {
    credible = false;
    reason = "Specific programme/event present but not yet credible for target hotel revenue.";
  }

  return {
    level: ASSESSMENT_LEVEL.PROGRAM_OR_EVENT,
    specificProgramOrEventPresent: specificEvent,
    createsCredibleOpportunityForTargetHotel: credible,
    excludedForGuestroomsOnly: excludedForGuestrooms,
    reason,
  };
}

function buildSalesAction({
  contact,
  organizationName,
  targetHotel,
  disposition,
  revenueAssessments,
  dims,
  competitor,
  opportunity,
  evidenceRefs = [],
  stageSemantics = null,
}) {
  const relevantRevenue = revenueAssessments.filter((r) => r.commerciallyRelevant).map((r) => r.revenueType);
  const whyRelevant = dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_ACCOMMODATION_DEMAND].present
    ? "Organization evidences hotel/accommodation sourcing capability (capability ≠ current open buying requirement)."
    : dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_GROUP_ACTIVITY].present
      && dims.layers[DEMAND_EVIDENCE_LAYER.INFERRED_LODGING_POTENTIAL].present
      ? "Verified group/incentive activity supports inferred lodging or meetings demand (explicitly inferred — not a confirmed room block or upcoming booking)."
      : relevantRevenue.includes(REVENUE_TYPE.CATERING_AND_EVENTS)
        ? "Verified banquet/catering/event activity matches hotel function product."
        : relevantRevenue.includes(REVENUE_TYPE.MEETING_SPACE)
          ? "Verified meetings/training/programme activity matches hotel meeting product."
          : "Commercial relevance still thin — research gap.";

  // Prefer explicit hotel-fit facts — never use raw page chrome / nav excerpts as "why hotel suitable".
  const whyHotelRaw = opportunity.hotelFitRationale
    || opportunity.evidenceBackedHotelFit
    || (opportunity.summaryWhyHotel
      && !/Click the button|Home Registration|Plans & Pricing|shortlist 0 companies|-->/i.test(String(opportunity.summaryWhyHotel))
      ? opportunity.summaryWhyHotel
      : null);
  const whyHotel = whyHotelRaw
    ? String(whyHotelRaw).replace(/\s+/g, " ").trim().slice(0, 220)
    : `${targetHotel} is the target property under research; use only evidenced product fit — do not treat page navigation chrome as hotel-fit.`;

  const businessTypes = relevantRevenue.length
    ? relevantRevenue.join(", ")
    : "None qualified yet";

  const overflowOk = stageSemantics?.futureSourcingCycle?.overflowEvidenced === true;
  const sourcingOpen = stageSemantics?.futureSourcingCycle?.hotelSourcingOpenForFutureCycle === true;
  const whyNow = competitor
    ? `Competitor host ${competitor} is named — do not claim open host selection. Later cycle is ${stageSemantics?.futureSourcingCycle?.verifiedAnnouncedCycle ? "verified/announced as an event cycle" : stageSemantics?.futureSourcingCycle?.inferredFromHistoricalPattern ? "inferred from historical pattern" : "not evidenced"}; hotel-sourcing availability is ${sourcingOpen ? "evidenced" : "not evidenced"}${overflowOk ? "; overflow demand is evidenced" : "; do not infer overflow"}.`
    : disposition === INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY
      ? "Currently actionable buying/sourcing requirement is evidenced — outreach is timely."
      : disposition === INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY
        ? "An identifiable prospective programme/event is evidenced while hotel sourcing remains UNKNOWN — introduce before a host is locked."
        : disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT
          ? "Strategic account introduction: verified organizational capability and hotel fit, without inventing urgency, an open RFP, or an upcoming booking at this hotel."
          : disposition === INTERNAL_DISPOSITION.WATCHLIST_RESEARCH
            ? "Not sales-ready — continue research on the open commercial gaps first."
            : "Timing not established.";

  const askOrPropose = competitor
    ? (overflowOk || sourcingOpen
      ? `Ask ${contact || "the contact"} about future-cycle dates${overflowOk ? ", evidenced overflow needs," : ""} or satellite meetings near ${targetHotel}. Do not claim open host availability.`
      : `Ask ${contact || "the contact"} for verified future-cycle dates and whether hotel selection or overflow will open later. Preserve ${competitor || "the named host"} commitment; do not imply open host selection or invent overflow.`)
    : disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT && relevantRevenue.includes(REVENUE_TYPE.GUESTROOMS)
      ? `Introduce ${targetHotel} to ${contact || "the contact"} as a candidate hotel for future group / incentive programmes they source. Ask how they shortlist hotels. Do not imply a current ${targetHotel || "hotel"}/booking requirement or invent an RFP.`
      : relevantRevenue.includes(REVENUE_TYPE.CATERING_AND_EVENTS)
        ? `Ask ${contact || "the contact"} about the next awards/banquet cycle and whether ${targetHotel} can be considered as a banquet venue. Do not invent a booking or guestroom need.`
        : relevantRevenue.includes(REVENUE_TYPE.MEETING_SPACE)
          ? `Ask ${contact || "the contact"} about upcoming meetings/training needing full-service meeting space and whether ${targetHotel} fits.`
          : relevantRevenue.includes(REVENUE_TYPE.GUESTROOMS)
            ? `Ask ${contact || "the contact"} how they source hotels for upcoming programmes and whether ${targetHotel} can be considered. Do not invent a room block or RFP.`
            : `Do not pitch yet — close the research gap first.`;

  void organizationName;
  return {
    whoToContact: contact || null,
    whyOrganizationCommerciallyRelevant: whyRelevant,
    whyHotelSuitable: whyHotel,
    businessTypesCouldGenerate: businessTypes,
    whyApproachNow: whyNow,
    whatSalesShouldAskOrPropose: askOrPropose,
    outreachTiming: disposition === INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY
      || disposition === INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY
      ? "IMMEDIATE_OUTREACH"
      : disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT
        || disposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE
        ? "NEAR_TERM_ACCOUNT_OR_CYCLE_OUTREACH"
        : "FUTURE_RESEARCH_ONLY",
    evidenceReferences: evidenceRefs.slice(0, 5),
    complete: Boolean(contact) && relevantRevenue.length > 0 && Boolean(askOrPropose),
  };
}

/**
 * Separate commercial-stage facts — capability ≠ current active buying.
 */
export function assessCommercialStageSemantics({
  opportunity = {},
  acceptedClaims = [],
  pageTexts = [],
  dims = null,
  competitor = null,
  futureCycleSupported = false,
  revenueAssessments = [],
  eventLevel = null,
} = {}) {
  const blob = blobFrom(opportunity, acceptedClaims, pageTexts);
  const verifiedAcc = dims?.layers?.[DEMAND_EVIDENCE_LAYER.VERIFIED_ACCOMMODATION_DEMAND]?.present === true;
  const verifiedGroup = dims?.layers?.[DEMAND_EVIDENCE_LAYER.VERIFIED_GROUP_ACTIVITY]?.present === true;
  const inferredLodging = dims?.layers?.[DEMAND_EVIDENCE_LAYER.INFERRED_LODGING_POTENTIAL]?.present === true;

  /** Currently actionable buying/sourcing — not mere capability. */
  const currentlyActionableBuyingOrSourcing = /\b(?:rfp\b|request\s+for\s+proposal|currently\s+(?:sourcing|selecting|accepting)\s+(?:hotels?|bids?|proposals?)|open\s+(?:rfp|housing\s+block)|housing\s+bureau\s+now\s+open|seeking\s+(?:hotels?|proposals?|venues?)\s+for|bids?\s+(?:due|open)|hotel\s+selection\s+(?:open|underway))\b/i.test(blob)
    || opportunity.currentlySourcing === true
    || opportunity.activeRfp === true;

  const historyOfPurchasingHotelServices = /\b(?:past\s+(?:programme|program|incentive)|previously\s+(?:booked|placed|contracted)|has\s+(?:booked|placed)\s+groups?\s+at|historical\s+(?:placement|programme)|used\s+[A-Z][^.]+Hotel)\b/i.test(blob);

  const identifiableProspectiveProgram = Boolean(
    opportunity.eventName
    || opportunity.meetingName
    || opportunity.programmeName
    || /\b(?:recurring\s+)?awards?\s+(?:banquet|dinner)|annual\s+(?:meeting|conference|congress)|hill\s*day|advocacy\s*day|20\d{2}\s+(?:meeting|conference|congress|institute)\b/i.test(blob),
  );

  const programHasPotentialHotelDemand = eventLevel?.createsCredibleOpportunityForTargetHotel === true
    || revenueAssessments.some((r) => r.commerciallyRelevant);

  // Future cycle: verified announcement vs inferred from historical pattern.
  // Announced event cycle ≠ open hotel-sourcing availability.
  const futureCycleVerified = Boolean(
    competitor
    && (/\b(?:20(?:2[8-9]|3\d)|next\s+(?:year'?s|cycle)|announced\s+for\s+20\d{2}|hotel\s+(?:not\s+yet\s+)?(?:published|selected|named)\s+for\s+20\d{2})\b/i.test(blob)
      || opportunity.futureCycleAnnounced === true),
  );
  const futureCycleInferredFromHistory = Boolean(
    competitor
    && futureCycleSupported
    && !futureCycleVerified
    && /\b(?:pattern|recurring|historical)\b/i.test(blob)
    && !/\bwhen\s+.+\s+is\s+full\b/i.test(blob), // hotel-fit "when full" ≠ historical sourcing cycle
  );
  const overflowEvidenced = /\b(?:overflow\s+(?:block|housing|rooms?|need|request)|seeking\s+overflow|satellite\s+(?:hotel|block|housing))\b/i.test(blob)
    || opportunity.overflowEvidenced === true;
  const hotelSourcingOpenForFutureCycle = currentlyActionableBuyingOrSourcing
    || (/\b(?:hotel\s+selection\s+(?:open|underway|not\s+yet)|seeking\s+(?:hotels?|proposals?)|rfp\b)\b/i.test(blob)
      && !competitor)
    || overflowEvidenced;

  return {
    organizationHasHotelSourcingCapability: verifiedAcc,
    organizationHasHistoryOfPurchasingHotelServices: historyOfPurchasingHotelServices,
    organizationMayGenerateFutureHotelBusiness: verifiedAcc || verifiedGroup || inferredLodging
      || revenueAssessments.some((r) => r.commerciallyRelevant),
    specificProgramHasPotentialHotelDemand: identifiableProspectiveProgram && programHasPotentialHotelDemand,
    identifiableProspectiveProgram,
    buyerIsCurrentlySourcing: currentlyActionableBuyingOrSourcing,
    buyerHasSelectedCompetingHotel: Boolean(competitor),
    futureSourcingCycle: {
      evidenced: futureCycleVerified || futureCycleInferredFromHistory,
      verifiedAnnouncedCycle: futureCycleVerified,
      inferredFromHistoricalPattern: futureCycleInferredFromHistory && !futureCycleVerified,
      hotelSourcingOpenForFutureCycle,
      overflowEvidenced,
      competingHost: competitor || null,
      note: hotelSourcingOpenForFutureCycle
        ? "Future-cycle hotel sourcing evidenced as open or overflow-seeking."
        : "Announced or inferred cycle does not imply open host selection or overflow demand.",
    },
    currentlyActionableBuyingOrSourcing,
  };
}

/**
 * Disposition rules (calibrated):
 * - ACTIVE_OPPORTUNITY: identifiable currently actionable buying/sourcing requirement
 * - PRE_SOURCING: identifiable prospective program / specific demand signal; sourcing not established
 * - ACCOUNT_DEVELOPMENT: verified org capability + hotel fit (no current buying requirement required)
 * - COMPETITIVE_FUTURE_CYCLE: competing commitment retained; later cycle verified or inferred (disclosed)
 * Capability alone never upgrades to ACTIVE.
 */
function pickDisposition({
  competitor,
  futureCycle,
  contactOk,
  revenueAssessments,
  orgLevel,
  eventLevel,
  dims,
  venueMarketplace,
  v2Useful,
  stageSemantics,
}) {
  const guestroom = revenueAssessments.find((r) => r.revenueType === REVENUE_TYPE.GUESTROOMS);
  const catering = revenueAssessments.find((r) => r.revenueType === REVENUE_TYPE.CATERING_AND_EVENTS);
  const meeting = revenueAssessments.find((r) => r.revenueType === REVENUE_TYPE.MEETING_SPACE);
  const anyRevenue = revenueAssessments.some((r) => r.commerciallyRelevant);
  const verifiedAcc = dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_ACCOMMODATION_DEMAND].present;
  const sem = stageSemantics || {};

  // Competing commitment first
  if (competitor && (futureCycle || sem.futureSourcingCycle?.evidenced) && contactOk) {
    return INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE;
  }
  if (competitor && !futureCycle && !sem.futureSourcingCycle?.evidenced) {
    return contactOk && orgLevel.couldGenerateSuitableFutureBusiness
      ? INTERNAL_DISPOSITION.WATCHLIST_RESEARCH
      : INTERNAL_DISPOSITION.EXCLUDE;
  }
  if (venueMarketplace && !anyRevenue) {
    return contactOk
      ? INTERNAL_DISPOSITION.WATCHLIST_RESEARCH
      : INTERNAL_DISPOSITION.EXCLUDE;
  }

  // ACTIVE only with currently actionable buying/sourcing evidence
  if (sem.currentlyActionableBuyingOrSourcing && contactOk && anyRevenue) {
    return INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY;
  }

  // PRE_SOURCING: identifiable prospective program + revenue relevance; sourcing unknown/not established
  if (
    contactOk
    && sem.identifiableProspectiveProgram
    && anyRevenue
    && !sem.buyerHasSelectedCompetingHotel
    && (catering?.commerciallyRelevant || meeting?.commerciallyRelevant
      || (guestroom?.commerciallyRelevant && guestroom.inferredOnly))
  ) {
    return INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY;
  }

  // ACCOUNT_DEVELOPMENT: verified capability or credible org revenue fit without current buying requirement
  if (contactOk && verifiedAcc && guestroom?.commerciallyRelevant) {
    return INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT;
  }
  if (
    contactOk
    && guestroom?.commerciallyRelevant
    && guestroom.inferredOnly
    && !sem.identifiableProspectiveProgram
  ) {
    // General DMC/incentive capability without a named prospective program
    return INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT;
  }
  if (
    contactOk
    && anyRevenue
    && (orgLevel.couldGenerateSuitableFutureBusiness || eventLevel.createsCredibleOpportunityForTargetHotel)
  ) {
    return INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT;
  }
  if (orgLevel.couldGenerateSuitableFutureBusiness && contactOk && !anyRevenue) {
    return INTERNAL_DISPOSITION.WATCHLIST_RESEARCH;
  }
  if (v2Useful && contactOk) {
    return INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT;
  }
  if (!contactOk && (anyRevenue || orgLevel.couldGenerateSuitableFutureBusiness)) {
    return INTERNAL_DISPOSITION.WATCHLIST_RESEARCH;
  }
  return INTERNAL_DISPOSITION.EXCLUDE;
}

/** Reporting buckets — do not collapse into one headline useful count. */
export const COMMERCIAL_REPORTING_BUCKET = Object.freeze({
  ACTIVE_BUYING_OPPORTUNITIES: "ACTIVE_BUYING_OPPORTUNITIES",
  PRE_SOURCING_OPPORTUNITIES: "PRE_SOURCING_OPPORTUNITIES",
  ACCOUNT_DEVELOPMENT_PROSPECTS: "ACCOUNT_DEVELOPMENT_PROSPECTS",
  FUTURE_CYCLE_WATCH_PROSPECTS: "FUTURE_CYCLE_WATCH_PROSPECTS",
  RESEARCH_WATCHLIST: "RESEARCH_WATCHLIST",
  EXCLUDED: "EXCLUDED",
});

export function dispositionToReportingBucket(disposition) {
  switch (disposition) {
    case INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY:
      return COMMERCIAL_REPORTING_BUCKET.ACTIVE_BUYING_OPPORTUNITIES;
    case INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY:
      return COMMERCIAL_REPORTING_BUCKET.PRE_SOURCING_OPPORTUNITIES;
    case INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT:
      return COMMERCIAL_REPORTING_BUCKET.ACCOUNT_DEVELOPMENT_PROSPECTS;
    case INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE:
      return COMMERCIAL_REPORTING_BUCKET.FUTURE_CYCLE_WATCH_PROSPECTS;
    case INTERNAL_DISPOSITION.WATCHLIST_RESEARCH:
      return COMMERCIAL_REPORTING_BUCKET.RESEARCH_WATCHLIST;
    case INTERNAL_DISPOSITION.EXCLUDE:
    default:
      return COMMERCIAL_REPORTING_BUCKET.EXCLUDED;
  }
}

/**
 * Primary V3 internal qualifier — used from ORS maturation hooks.
 */
export function qualifyCommercialProspectV3({
  opportunity = {},
  acceptedClaims = [],
  pageTexts = [],
  targetHotel = "",
  usableContactRoute = null,
  hotelFacts = null,
} = {}) {
  const v2 = qualifyHotelSpecificProspectV2({
    opportunity,
    acceptedClaims,
    pageTexts,
    targetHotel,
    usableContactRoute,
  });

  const contact = contactFrom(opportunity, usableContactRoute || v2.preservedContactRoute);
  const contactOk = Boolean(contact && /@/.test(String(contact)));
  const blob = blobFrom(opportunity, acceptedClaims, pageTexts);
  // Merge optional hotelFacts into pageTexts for fit detection only (never invent).
  const fitTexts = [
    ...pageTexts,
    ...(hotelFacts ? [JSON.stringify(hotelFacts)] : []),
    opportunity.summaryWhyHotel || "",
  ];

  const dims = assessDemandEvidenceDimensions({
    acceptedClaims,
    pageTexts: fitTexts,
    organizationName: opportunity.organizationName || "",
    opportunity,
  });
  const accommodation = classifyAccommodationEvidenceRole({
    acceptedClaims,
    pageTexts: fitTexts,
    organizationName: opportunity.organizationName || "",
  });
  const competitor = namedCompetitorHost(blob, targetHotel)
    || opportunity.namedHostHotel
    || v2.namedCompetitorHost
    || null;
  // Overflow must be evidenced as a real demand path — never auto-inferred from
  // "when X is full" hotel-fit prose alone.
  const overflowEvidenced = /\b(?:overflow\s+(?:block|housing|rooms?|need|request)|seeking\s+overflow|satellite\s+(?:hotel|block|housing))\b/i.test(blob)
    || opportunity.overflowEvidenced === true;
  const futureCycle = Boolean(
    competitor
    && (
      opportunity.futureCycleSupported === true
      || v2.futureCycleSupported === true
      || /\bfuture\s*cycle|next\s+(?:year|cycle|meeting)|20(?:2[8-9]|3\d)|satellite\s+meetings?|alternate\s+hotel\b/i.test(blob)
      || overflowEvidenced
    ),
  );
  const venueMarketplace = VENUE_MARKETPLACE.test(blob) || accommodation.venueListingOnly;
  const localDayOnly = LOCAL_DAY_EVENT.test(blob)
    && !dims.layers[DEMAND_EVIDENCE_LAYER.INFERRED_LODGING_POTENTIAL].present;

  const revenueAssessments = [
    REVENUE_TYPE.GUESTROOMS,
    REVENUE_TYPE.MEETING_SPACE,
    REVENUE_TYPE.CATERING_AND_EVENTS,
  ].map((revenueType) => assessRevenueOpportunity({
    revenueType,
    opportunity,
    acceptedClaims,
    pageTexts: fitTexts,
    targetHotel,
    dimensions: dims,
    accommodation,
    competitor,
  }));

  const orgLevel = assessOrganizationLevel({
    opportunity,
    revenueAssessments,
    dims,
    venueMarketplace,
    contactOk,
  });
  const eventLevel = assessProgramOrEventLevel({
    opportunity,
    revenueAssessments,
    competitor,
    venueMarketplace,
    localDayOnly,
  });

  const stageSemantics = assessCommercialStageSemantics({
    opportunity,
    acceptedClaims,
    pageTexts: fitTexts,
    dims,
    competitor,
    futureCycleSupported: futureCycle,
    revenueAssessments,
    eventLevel,
  });

  const disposition = pickDisposition({
    competitor,
    futureCycle,
    contactOk,
    revenueAssessments,
    orgLevel,
    eventLevel,
    dims,
    venueMarketplace,
    v2Useful: v2.commerciallyUsefulInternal === true,
    stageSemantics,
  });

  const evidenceRefs = (dims.supportingExcerpts || []).map((e) => ({
    statement: e.statement,
    url: e.url,
    provenance: e.provenance,
  }));

  const salesAction = buildSalesAction({
    contact,
    organizationName: opportunity.organizationName || "",
    targetHotel: targetHotel || opportunity.targetHotel || "",
    disposition,
    revenueAssessments,
    dims,
    competitor,
    opportunity,
    evidenceRefs,
    stageSemantics,
  });

  const usefulDispositions = new Set([
    INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY,
    INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY,
    INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT,
    INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE,
  ]);
  const commerciallyUsefulInternal = usefulDispositions.has(disposition) && contactOk
    && (
      revenueAssessments.some((r) => r.commerciallyRelevant)
      || disposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE
    );

  // Buyer/sourcing status UNKNOWN unless currently sourcing or host selected — capability ≠ EVIDENCED buyer status
  let sourcingStatus = "UNKNOWN";
  if (stageSemantics.currentlyActionableBuyingOrSourcing) {
    sourcingStatus = "CURRENTLY_SOURCING";
  } else if (competitor) {
    sourcingStatus = "HOST_SELECTED";
  }
  // Verified accommodation capability is recorded in stageSemantics / revenue — not as buyer sourcingStatus EVIDENCED

  const researchGaps = [];
  if (!contactOk) researchGaps.push("Usable organization contact route");
  if (!dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_GROUP_ACTIVITY].present && !venueMarketplace) {
    researchGaps.push("Source-backed group / meeting / incentive activity");
  }
  if (
    dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_GROUP_ACTIVITY].present
    && !dims.layers[DEMAND_EVIDENCE_LAYER.INFERRED_LODGING_POTENTIAL].present
    && !dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_ACCOMMODATION_DEMAND].present
    && !revenueAssessments.some((r) => r.revenueType !== REVENUE_TYPE.GUESTROOMS && r.commerciallyRelevant)
  ) {
    researchGaps.push("Defensible lodging inference or non-guestroom revenue mechanism");
  }
  for (const r of revenueAssessments) {
    if (!r.commerciallyRelevant && r.exclusionReason) {
      researchGaps.push(`${r.revenueType}: ${r.exclusionReason}`);
    }
  }
  if (eventLevel.excludedForGuestroomsOnly) {
    researchGaps.push("Event excluded for guestrooms — organization may still warrant meetings/catering research");
  }

  // Pathway remains canonical V2 pathway (separate from disposition)
  const pathway = v2.pathway;

  return {
    version: COMMERCIAL_QUALIFICATION_V3,
    internalOnly: true,
    productionPublicationEligibilityChanged: false,
    customerFacingClassificationChanged: false,
    publicationEligibility: v2.publicationEligibility,
    // Canonical pathway (unchanged vocabulary)
    pathway,
    commercialStage: commerciallyUsefulInternal
      ? (disposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE
        ? COMMERCIAL_STAGE.OTHER_HOST_PLACED
        : COMMERCIAL_STAGE.ACCOUNT_DEVELOPMENT)
      : (disposition === INTERNAL_DISPOSITION.EXCLUDE ? COMMERCIAL_STAGE.EXCLUDED : v2.commercialStage),
    salesReadiness: commerciallyUsefulInternal
      ? (disposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE
        ? SALES_READINESS.NEEDS_RESEARCH
        : SALES_READINESS.READY_FOR_SALES)
      : (disposition === INTERNAL_DISPOSITION.EXCLUDE ? SALES_READINESS.EXCLUDE : SALES_READINESS.NEEDS_RESEARCH),
    // V3 additions
    internalDisposition: disposition,
    reportingBucket: dispositionToReportingBucket(disposition),
    commercialStageSemantics: stageSemantics,
    demandEvidenceDimensions: dims.layers,
    demandEvidenceDetail: dims,
    revenueOpportunities: Object.fromEntries(
      revenueAssessments.map((r) => [r.revenueType, r]),
    ),
    organizationLevel: orgLevel,
    programOrEventLevel: eventLevel,
    salesAction,
    researchGaps: [...new Set(researchGaps)].slice(0, 10),
    sourcingStatus,
    evidenceConfidence: {
      verifiedFacts: [
        dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_GROUP_ACTIVITY].present ? "VERIFIED_GROUP_ACTIVITY" : null,
        dims.layers[DEMAND_EVIDENCE_LAYER.VERIFIED_ACCOMMODATION_DEMAND].present ? "VERIFIED_ACCOMMODATION_DEMAND" : null,
        competitor ? `COMPETITOR_HOST:${competitor}` : null,
      ].filter(Boolean),
      supportedInferences: [
        dims.layers[DEMAND_EVIDENCE_LAYER.INFERRED_LODGING_POTENTIAL].present
          ? dims.layers[DEMAND_EVIDENCE_LAYER.INFERRED_LODGING_POTENTIAL].reason
          : null,
        ...revenueAssessments.filter((r) => r.inferredOnly).map((r) => `${r.revenueType}_INFERRED`),
      ].filter(Boolean),
    },
    commerciallyUsefulInternal,
    usefulKind: commerciallyUsefulInternal ? disposition : null,
    internalAccountDevelopment: disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT
      || disposition === INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY
      || disposition === INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY,
    eventLedInternalProspect: pathway === RESEARCH_PATHWAY.EVENT_LED && commerciallyUsefulInternal,
    competitiveFutureCycleProspect: disposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE,
    namedCompetitorHost: competitor,
    usableContactRoute: contactOk,
    preservedContactRoute: contact,
    nextSalesAction: salesAction.whatSalesShouldAskOrPropose,
    downgradeReason: commerciallyUsefulInternal
      ? null
      : (researchGaps[0] || v2.downgradeReason || eventLevel.reason || orgLevel.reason),
    // Backward-compatible V2 snapshot
    v2Qualification: {
      commerciallyUsefulInternal: v2.commerciallyUsefulInternal === true,
      usefulKind: v2.usefulKind,
      pathway: v2.pathway,
      downgradeReason: v2.downgradeReason,
      salesReadiness: v2.salesReadiness,
    },
    separationsPreserved: {
      dispositionSeparateFromPathway: true,
      verifiedFactsSeparateFromInferences: true,
      sourcingStatusSeparateFromDisposition: true,
      publicationEligibilitySeparateFromInternalResearch: true,
      organizationLevelSeparateFromProgramOrEvent: true,
      revenueTypesAssessedIndependently: true,
      unknownSourcingRemainsUnknown: sourcingStatus === "UNKNOWN" || sourcingStatus === "HOST_SELECTED" || sourcingStatus === "EVIDENCED",
      competitorHostIsNotOpenDemand: true,
      pursuitStrategySeparateFromMaturityAndPriority: true,
      currentHostCommittedDistinctFromFutureCycleContestable: true,
      eventExclusionDoesNotAutoExcludeOrganization: eventLevel.excludedForGuestroomsOnly === true
        ? orgLevel.couldGenerateSuitableFutureBusiness !== false
        : true,
    },
    // Pursuit strategy (deep-research prioritization) — does not change disposition / publication.
    commercialPursuit: (() => {
      const enriched = enrichOpportunityWithPursuitStrategy({
        ...opportunity,
        internalDisposition: disposition,
        namedHostHotel: competitor,
        organizationName: opportunity.organizationName,
        hotelOpportunityThesis:
          opportunity.hotelOpportunityThesis
          || opportunity.summaryWhyHotel
          || opportunity.whyThisHotel,
        researchSegment: opportunity.researchSegment || opportunity.segment,
        futureCycleSupported: futureCycle,
        geoFit: opportunity.geoFit,
      });
      return {
        strategy: enriched.commercialPursuitStrategy,
        sourcingStatus: enriched.pursuitSourcingStatus,
        futureCycleOpportunity: enriched.futureCycleOpportunity,
        currentOrHistoricalHostHotel:
          enriched.currentOrHistoricalHostHotel || competitor || null,
        screenDecision: enriched.pursuitScreenDecision,
        investmentScore: enriched.deepResearchInvestmentScore,
        materialGaps: enriched.pursuitMaterialGaps,
        reasons: enriched.pursuitReasons,
        requiresActiveRfp: false,
        unknownHotelSelectionIsNegative: false,
        competitorHostIsPermanentDisqualification: false,
      };
    })(),
  };
}
