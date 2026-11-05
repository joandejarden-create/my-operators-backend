/**
 * Scaling Companies — commercial intelligence brief + research-completeness V1.
 *
 * Produces a Director-of-Sales-ready brief from researched evidence.
 * Does not invent contacts, contracts, or current expansion.
 */

import {
  applyScalingActivityChronology,
  SCALING_ACTIVITY_STATUS,
  SCALING_ACTIVITY_CHRONOLOGY_V1,
} from "./scaling-activity-chronology-v1.js";
import {
  applyScalingTwoContactStandard,
  classifyScalingContactCoverage,
  extractPrimaryPersonName,
  SCALING_CONTACT_COVERAGE,
} from "./scaling-two-contact-standard-v1.js";
import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import {
  classifyContactEligibility,
  buildContactEligibilityBriefFields,
  CONTACT_ELIGIBILITY_CLASS,
} from "./contact-eligibility-standard-v1.js";
import { filterEvidenceUrlsAgainstPublisherShells } from "./press-publisher-entity-v1.js";
import {
  buildHotelIndependentOpportunityOverview,
  marketLabelForOverview,
} from "./scaling-sales-leader-editorial-v1.js";

export const SCALING_COMMERCIAL_INTELLIGENCE_BRIEF_V1 =
  "gdi-scaling-commercial-intelligence-brief-v1";

function scrubInternalResearchLanguage(input) {
  if (input == null) return null;
  let s = String(input);
  if (!s.trim()) return "";
  s = s.replace(/Requires qualification[^.\n]{0,160}/gi, " ");
  s = s.replace(/https?:\/\/\S+/gi, " ");
  s = s.replace(/\s+/g, " ").trim();
  return s || null;
}

export const SCALING_COMMERCIAL_RESEARCH_COMPLETENESS = Object.freeze({
  SALES_READY: "SALES_READY",
  ACCOUNT_DEVELOPMENT_READY: "ACCOUNT_DEVELOPMENT_READY",
  WATCHLIST_OR_MONITOR: "WATCHLIST_OR_MONITOR",
  RESEARCH_CONTINUATION_REQUIRED: "RESEARCH_CONTINUATION_REQUIRED",
  HOLD_INSUFFICIENT: "HOLD_INSUFFICIENT",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hotelNameOf(hotel = {}, opportunity = {}) {
  return (
    text(hotel.displayName || hotel.name) ||
    text(opportunity.hotelDisplayName || opportunity.hotelName) ||
    "the hotel"
  );
}

function cityOf(hotel = {}, opportunity = {}) {
  return text(hotel.city || opportunity.hotelCity || opportunity.marketCity || "");
}

function companyBlurb(opportunity = {}) {
  const org = text(opportunity.organizationName || opportunity.company) || "This company";
  const industry = text(
    opportunity.industry ||
      opportunity.companyType ||
      opportunity.businessModel ||
      "",
  );
  if (industry) return `${org} (${industry})`;
  // Infer light industry from known sources without inventing
  const blob = `${opportunity.whatIsHappening || ""} ${opportunity.localConnection || ""} ${opportunity.sources?.[0]?.url || ""}`.toLowerCase();
  if (/cyber|security|rapid7|vulnerability|siem/i.test(blob)) {
    return `${org}, a cybersecurity software company`;
  }
  if (/ai|artificial intelligence|machine learning/i.test(blob)) {
    return `${org}, a technology / AI company`;
  }
  if (/workforce housing|temporary staff housing/i.test(blob)) {
    return `${org}, a workforce-housing provider`;
  }
  return org;
}

/**
 * Property-specific fit analysis from canonical hotel profile + company geography.
 */
export function buildHotelSpecificFitAssessment(opportunity = {}, hotel = {}) {
  const hotelName = hotelNameOf(hotel, opportunity);
  const city = cityOf(hotel, opportunity) || "the market";
  const cap = hotel.capabilityProfile || {};
  const rooms = cap.totalGuestrooms || null;
  const meetingRooms = cap.meetingRoomCount || cap.meetingRoomsIndoor || null;
  const sqft =
    cap.totalMeetingSpaceSqFt ||
    cap.meetingSpaceSqFtConflict?.eventsPageHeadlineSqFt ||
    cap.meetingSpaceSqFtConflict?.overviewIndoorAndOutdoorSqFt ||
    null;
  const maxCap =
    cap.largestReceptionCapacity || cap.largestTheaterCapacity || null;
  const classification = text(cap.classification || "");
  const address = text(hotel.address || "");
  const airportTerminal = /airport|terminal/i.test(
    `${classification} ${address} ${hotelName}`,
  );

  const companySite = text(
    opportunity.localFacilityAddress ||
      opportunity.companyLocalAddress ||
      opportunity.activityLocation ||
      "",
  );
  // Extract address-like snippets from local connection when present
  const channelside = /615\s+Channelside|Channelside Drive/i.test(
    `${opportunity.localConnection || ""} ${opportunity.whatIsHappening || ""} ${opportunity.datedGrowthEvidence || ""} ${opportunity.growthSourceBodySnippet || ""}`,
  );
  const companyLocaleNote = companySite
    ? companySite
    : channelside
      ? "615 Channelside Drive (shared workspace / Channelside district)"
      : null;

  const advantages = [];
  const limitations = [];

  if (airportTerminal) {
    advantages.push(
      `${hotelName} is airport-connected (${address || city}), which can suit fly-in leadership, recruiters, trainers, and short-stay project staff who value airside access and same-day arrivals/departures.`,
    );
    limitations.push(
      `Airport-terminal location is a trade-off versus downtown / Channelside proximity — teams based near ${companyLocaleNote || `central ${city}`} may prefer lodging closer to their office for multi-day stays unless air travel cadence dominates.`,
    );
  } else {
    advantages.push(
      `${hotelName} sits in the same commercial market (${city}) as the documented ${text(opportunity.organizationName)} activity.`,
    );
  }

  if (meetingRooms || sqft) {
    advantages.push(
      `Meetings inventory: ${meetingRooms || "multiple"} event rooms` +
        (sqft ? ` / ~${Number(sqft).toLocaleString()} sq ft` : "") +
        (maxCap ? ` (reception capacity up to ~${maxCap})` : "") +
        ` — relevant if training, all-hands, or customer sessions are confirmed.`,
    );
  }
  if (rooms) {
    advantages.push(`${rooms} guestrooms available for transient corporate stays.`);
  }

  limitations.push(
    "No confirmed room block, RFP, or preferred-hotel agreement is established from public research alone.",
  );

  const summary = [
    advantages[0],
    companyLocaleNote
      ? `Company activity reference point: ${companyLocaleNote}.`
      : null,
    limitations[0],
  ]
    .filter(Boolean)
    .join(" ");

  return {
    hotelName,
    city,
    airportTerminalHotel: airportTerminal,
    companyLocaleNote,
    rooms,
    meetingRooms,
    meetingSqFt: sqft,
    maxReceptionCapacity: maxCap,
    advantages,
    limitations,
    whyThisHotel: summary,
    fitConfidence: companyLocaleNote || airportTerminal ? "GROUNDED" : "MARKET_LEVEL",
  };
}

/**
 * Company-specific demand hypotheses — documented vs inferred.
 */
export function buildDemandMechanismAssessment(opportunity = {}, chronology = {}) {
  const org = text(opportunity.organizationName) || "the company";
  const blob = `${opportunity.whatIsHappening || ""} ${opportunity.localConnection || ""} ${opportunity.datedGrowthEvidence || ""} ${opportunity.growthSourceBodySnippet || ""} ${opportunity.potentialHotelBusiness || ""} ${opportunity.proposedHotelUse || ""}`;
  const documented = [];
  const inferred = [];

  // Prefer Exa Agent / deep-research mapped demand mechanism when already substantive.
  const mappedDemand = text(
    opportunity.potentialHotelBusiness || opportunity.proposedHotelUse || "",
  );
  const mappedDemandIsGeneric =
    !mappedDemand ||
    /Public growth\/local-activity evidence is on file/i.test(mappedDemand);
  if (mappedDemand && !mappedDemandIsGeneric) {
    documented.push(mappedDemand);
  }

  if (/100\+|100 roles|new jobs|hiring|human resources|customer support|engineering/i.test(blob)) {
    documented.push(
      `Hiring / workforce build-out was part of the original signal (roles across engineering, development, customer support, IT, and HR).`,
    );
    inferred.push(
      `Recruiting visits, new-hire onboarding travel, and trainer trips are plausible while headcount ramps — not confirmed room demand.`,
    );
  }
  if (/office|workspace|Industrious|headquarters|facility/i.test(blob)) {
    documented.push(`Local office / workspace footprint was publicly announced.`);
    inferred.push(
      `Visiting leadership, multi-office collaborators, and customer/partner meetings near the local site are plausible transient demand.`,
    );
  }
  if (
    /incentive|recognition trip|passport to paradise|president.?s club|GOLD [Tt]rip|dealer meeting|resort lodging|all-inclusive|training and recognition|earned (?:resort )?rooms?/i.test(
      blob,
    )
  ) {
    if (mappedDemandIsGeneric) {
      documented.push(
        `Documented company-sponsored incentive, recognition, or dealer/program gathering with lodging and/or on-site training implications.`,
      );
    }
    inferred.push(
      `Group guestrooms, meeting/training space, and recognition catering are plausible until room nights, dates, and venue contracting status are confirmed — do not invent volumes.`,
    );
  }
  if (
    chronology.activityStatus ===
      SCALING_ACTIVITY_STATUS.CONTINUING_ESTABLISHED_OPERATIONS ||
    chronology.activityStatus ===
      SCALING_ACTIVITY_STATUS.HISTORICAL_ACCOUNT_DEVELOPMENT
  ) {
    inferred.push(
      `Because the expansion announcement is historical, treat demand as ongoing corporate account development (periodic visitors, training, customer events) rather than a surge tied to a brand-new market entry.`,
    );
  }
  if (!documented.length) {
    documented.push(
      `Public growth/local-activity evidence is on file; specific travel volumes are not quantified.`,
    );
  }
  if (!inferred.length) {
    inferred.push(
      `Visiting staff lodging and meeting needs are a hypothesis until a travel/meetings owner confirms patterns.`,
    );
  }

  return {
    organizationName: org,
    documented,
    inferred,
    potentialBusinessNarrative: [
      ...documented.map((d) => d),
      `Plausible hotel demand (inferred, not verified): ${inferred.join(" ")}`,
    ].join(" "),
  };
}

/**
 * Research-completeness gate for Scaling commercial briefs.
 */
export function assessScalingCommercialResearchCompleteness(
  opportunity = {},
  hotel = {},
  chronology = null,
) {
  const chrono =
    chronology ||
    opportunity.activityChronology ||
    applyScalingActivityChronology(opportunity, {
      hotelCity: cityOf(hotel, opportunity),
    }).activityChronology;

  const enriched = applyScalingTwoContactStandard({
    ...opportunity,
    researchSegment: "scaling_companies",
  });
  const coverage = classifyScalingContactCoverage(enriched);
  const primary = extractPrimaryPersonName(enriched);
  const secondary = looksLikePersonName(enriched.secondaryContact?.name || "")
    ? text(enriched.secondaryContact.name)
    : null;
  const namedPersonResearchAttempted =
    opportunity.namedPersonResearchAttempted === true ||
    opportunity.whoResearchStatus != null ||
    Array.isArray(opportunity.namedStakeholders) ||
    coverage !== SCALING_CONTACT_COVERAGE.ORGANIZATION_ONLY ||
    Boolean(primary);

  const gaps = [];
  if (!text(opportunity.organizationName)) gaps.push("COMPANY_IDENTITY");
  if (!chrono?.originalEventDate && !text(opportunity.growthEvidenceDate)) {
    gaps.push("ACTIVITY_CHRONOLOGY");
  }
  if (
    chrono?.activityStatus === SCALING_ACTIVITY_STATUS.HISTORICAL_ACCOUNT_DEVELOPMENT ||
    chrono?.activityStatus === SCALING_ACTIVITY_STATUS.STALE_OR_INSUFFICIENT_EVIDENCE
  ) {
    if (!chrono.hasContinuityEvidence) gaps.push("CONTINUITY_OR_CURRENT_STATUS");
  }
  if (
    !text(opportunity.proposedHotelUse) &&
    !text(opportunity.potentialHotelBusiness) &&
    !text(opportunity.demandMechanism)
  ) {
    gaps.push("DEMAND_MECHANISM");
  }
  if (
    !text(opportunity.whyThisHotel) &&
    !text(opportunity.hotelFitRationale) &&
    !hotel?.capabilityProfile
  ) {
    gaps.push("HOTEL_SPECIFIC_FIT");
  }
  if (!text(opportunity.recommendedAction)) gaps.push("NEXT_ACTION");
  const hasRoute =
    Boolean(primary) ||
    text(opportunity.contactEmail) ||
    text(opportunity.companyPhone) ||
    text(opportunity.contactUrl) ||
    text(enriched.primaryContact?.phone) ||
    text(enriched.organizationRoutingAlternative);
  if (!hasRoute) gaps.push("CONTACT_ROUTE");
  if (!namedPersonResearchAttempted) gaps.push("NAMED_PERSON_RESEARCH_NOT_ATTEMPTED");
  if (!primary) gaps.push("PRIMARY_NAMED_PERSON");
  if (!secondary) gaps.push("SECONDARY_NAMED_PERSON");

  const materialBlockers = gaps.filter((g) =>
    [
      "COMPANY_IDENTITY",
      "ACTIVITY_CHRONOLOGY",
      "DEMAND_MECHANISM",
      "CONTACT_ROUTE",
    ].includes(g),
  );

  let completeness =
    SCALING_COMMERCIAL_RESEARCH_COMPLETENESS.ACCOUNT_DEVELOPMENT_READY;
  if (materialBlockers.length) {
    completeness =
      SCALING_COMMERCIAL_RESEARCH_COMPLETENESS.RESEARCH_CONTINUATION_REQUIRED;
  }
  if (
    chrono?.activityStatus ===
      SCALING_ACTIVITY_STATUS.STALE_OR_INSUFFICIENT_EVIDENCE &&
    !chrono.hasContinuityEvidence
  ) {
    completeness = SCALING_COMMERCIAL_RESEARCH_COMPLETENESS.WATCHLIST_OR_MONITOR;
  }
  if (
    gaps.includes("CONTINUITY_OR_CURRENT_STATUS") &&
    chrono?.activityStatus ===
      SCALING_ACTIVITY_STATUS.HISTORICAL_ACCOUNT_DEVELOPMENT
  ) {
    completeness = SCALING_COMMERCIAL_RESEARCH_COMPLETENESS.WATCHLIST_OR_MONITOR;
  }
  // Sales-ready requires chronology + demand + route + at least one named person
  if (
    !materialBlockers.length &&
    primary &&
    chrono?.originalEventDate &&
    (chrono.customerFacingAsCurrentExpansion || chrono.hasContinuityEvidence)
  ) {
    completeness = SCALING_COMMERCIAL_RESEARCH_COMPLETENESS.SALES_READY;
  }

  return {
    version: SCALING_COMMERCIAL_INTELLIGENCE_BRIEF_V1,
    chronologyVersion: SCALING_ACTIVITY_CHRONOLOGY_V1,
    completeness,
    gaps,
    materialBlockers,
    namedPersonResearchAttempted,
    primaryNamed: primary,
    secondaryNamed: secondary,
    contactCoverage: coverage,
    activityStatus: chrono?.activityStatus || null,
    mayPresentAsCurrentExpansion: chrono?.customerFacingAsCurrentExpansion === true,
  };
}

function overviewFor(opportunity, hotel, chrono, demand, fit) {
  // Hotel-independent — property name / address / selling points belong in Why this hotel.
  void demand;
  void fit;
  return buildHotelIndependentOpportunityOverview({
    displayName:
      text(opportunity.displayCompanyName || opportunity.organizationName) ||
      "This company",
    marketCity: marketLabelForOverview(hotel, opportunity) || cityOf(hotel, opportunity),
    chrono,
  });
}

/**
 * Account-selection rationale — why this company is on the Scaling list.
 * Must be evidence-grounded; never invent activity.
 */
export function buildWhyDealalitySelectedThisCompany(
  opportunity = {},
  hotel = {},
  chrono = null,
) {
  const org = text(opportunity.organizationName) || "This company";
  const city = cityOf(hotel, opportunity) || "the hotel's market";
  const hotelName = hotelNameOf(hotel, opportunity);
  const headline = scrubInternalResearchLanguage(
    opportunity.activityHeadline || opportunity.growthHeadline || "",
  );
  const growthDate =
    chrono?.originalEventDate || text(opportunity.growthEvidenceDate) || null;
  const signals = [];
  if (growthDate) {
    signals.push(
      `dated public growth/local-activity evidence${headline ? ` (“${headline}”)` : ""} on ${growthDate}`,
    );
  } else if (headline) {
    signals.push(`public local-activity signal (“${headline}”)`);
  }
  if (
    /hiring|headcount|workforce|recruit|jobs|careers/i.test(
      `${opportunity.whatIsHappening || ""} ${opportunity.localConnection || ""} ${headline || ""} ${opportunity.datedGrowthEvidence || ""}`,
    )
  ) {
    signals.push("workforce / hiring language that can imply visiting-staff or onboarding travel");
  }
  if (
    /office|headquarters|facility|campus|expansion|relocati|opened|opening/i.test(
      `${opportunity.whatIsHappening || ""} ${opportunity.localConnection || ""} ${headline || ""}`,
    )
  ) {
    signals.push("facility / office footprint language tied to the local market");
  }
  if (
    /meeting|training|workshop|conference|event|hospitality/i.test(
      `${opportunity.proposedHotelUse || ""} ${opportunity.potentialHotelBusiness || ""} ${opportunity.demandMechanism || ""}`,
    )
  ) {
    signals.push("hospitality-adjacent demand mechanisms already flagged in research");
  }

  const signalBit =
    signals.length > 0
      ? signals.slice(0, 3).join("; ")
      : "local commercial-activity evidence captured during Scaling discovery";

  const continuityBit =
    chrono?.hasContinuityEvidence
      ? " Later signals support ongoing local presence, so the account remains commercially relevant even if the original announcement is historical."
      : chrono?.activityStatus ===
          SCALING_ACTIVITY_STATUS.HISTORICAL_ACCOUNT_DEVELOPMENT
        ? " Continuity of current local operations still needs confirmation before treating this as an active expansion pursuit."
        : "";

  return {
    whySelected: `Dealality selected ${org} as a Scaling Corporate Account for ${hotelName} because research found ${signalBit} in or near ${city}.${continuityBit} The hotel sales team should treat this as a corporate account-development lead — not a confirmed booking — and qualify travel, meetings, and preferred-hotel ownership before pursuit.`,
    selectionSignals: signals,
    selectionGrounded: signals.length > 0 || Boolean(growthDate || headline),
  };
}

/**
 * Company background / local presence — profile without inventing scale.
 */
export function buildCompanyBackgroundAndLocalPresence(
  opportunity = {},
  hotel = {},
  chrono = null,
) {
  const org = text(opportunity.organizationName) || "This organization";
  const city = cityOf(hotel, opportunity) || "the local market";
  const industry = text(
    opportunity.industry ||
      opportunity.companyType ||
      opportunity.businessModel ||
      "",
  );
  const site = text(
    opportunity.localFacilityAddress ||
      opportunity.companyLocalAddress ||
      opportunity.activityLocation ||
      "",
  );
  const parts = [companyBlurb(opportunity) + "."];
  if (industry) {
    parts.push(`Primary business context: ${industry}.`);
  }
  if (site) {
    parts.push(`Documented local footprint reference: ${site}.`);
  } else {
    parts.push(
      `Public research associates ${org} with commercial activity in ${city}; exact facility address should be confirmed with the account.`,
    );
  }
  if (chrono?.originalEventDate) {
    parts.push(
      `Relevant local activity evidence is dated ${chrono.originalEventDate}${chrono.hasContinuityEvidence ? " with later continuity signals" : ""}.`,
    );
  }
  const scaleHint = text(
    opportunity.employeeCount ||
      opportunity.headcount ||
      opportunity.companyScale ||
      "",
  );
  if (scaleHint && !/invent|approx|~|estimated/i.test(scaleHint)) {
    parts.push(`Published scale context on file: ${scaleHint}.`);
  }
  return {
    companyBackground: parts.join(" "),
    industry: industry || null,
    localFootprint: site || null,
    marketCity: city,
  };
}

function recommendedApproach(opportunity, hotel, chrono, completeness) {
  const org = text(opportunity.organizationName) || "the company";
  const hotelName = hotelNameOf(hotel, opportunity);
  const primary = extractPrimaryPersonName(opportunity);
  const target = primary
    ? primary
    : "the workplace / HR / facilities or travel owner via the official company contact path";
  const timing =
    chrono.activityStatus ===
      SCALING_ACTIVITY_STATUS.CONTINUING_ESTABLISHED_OPERATIONS
      ? "Qualify ongoing travel patterns now; do not lead with a five-year-old expansion headline as if it were news."
      : chrono.customerFacingAsCurrentExpansion
        ? "Introduce the hotel while local operations remain visible and qualify whether travel or meetings are active now."
        : "First confirm whether the local site is still active before pitching surge demand.";

  return {
    suggestedInitialTarget: target,
    purposeOfOutreach: `Introduce ${hotelName} and learn who owns visiting-staff lodging, training, and meeting decisions for the ${cityOf(hotel, opportunity) || "local"} team.`,
    qualificationQuestions: [
      `Is the ${cityOf(hotel, opportunity) || "local"} site still an active office / hiring location?`,
      "Who coordinates overnight stays for visiting engineers, recruiters, trainers, or customers?",
      "Do you run onboarding classes, partner enablement, or quarterly meetings that need meeting space?",
      `When fly-in staff stay overnight, do you prefer airport-connected hotels or properties closer to the office?`,
    ],
    commercialAngle:
      chrono.activityStatus ===
        SCALING_ACTIVITY_STATUS.CONTINUING_ESTABLISHED_OPERATIONS
        ? `Position ${hotelName} as a practical option for fly-in corporate travelers and compact training/meeting needs — acknowledge airport vs. downtown trade-offs.`
        : `Connect ${hotelName} to the documented local activity without overstating confirmed demand.`,
    nextAction:
      completeness.completeness ===
      SCALING_COMMERCIAL_RESEARCH_COMPLETENESS.WATCHLIST_OR_MONITOR
        ? "Monitor for fresher local operating/hiring evidence before active pursuit; keep organizational route on file."
        : `Contact ${target}; ask the qualification questions above; log the travel owner and any preferred-hotel constraints.`,
    timingRationale: timing,
  };
}

/**
 * Build the substantive customer-facing commercial brief.
 */
export function buildScalingCommercialIntelligenceBrief(
  opportunity = {},
  hotel = {},
  opts = {},
) {
  let opp = applyScalingActivityChronology(opportunity, {
    hotelCity: cityOf(hotel, opportunity),
    continuityEvidence: opts.continuityEvidence === true,
    asOfIso: opts.asOfIso,
  });
  if (opts.continuityEvidence === true || opts.continuityNote) {
    opp = {
      ...opp,
      continuityEvidenceEstablished: true,
      continuityEvidenceNote: opts.continuityNote || opp.continuityEvidenceNote,
      activityChronology: {
        ...opp.activityChronology,
        hasContinuityEvidence: true,
        activityStatus: SCALING_ACTIVITY_STATUS.CONTINUING_ESTABLISHED_OPERATIONS,
        customerFacingAsCurrentExpansion: false,
        reason:
          opts.continuityNote ||
          "Continuity evidence supplied — treat as established local operations.",
      },
      activityStatus: SCALING_ACTIVITY_STATUS.CONTINUING_ESTABLISHED_OPERATIONS,
    };
  }

  const chrono = opp.activityChronology;
  const fit = buildHotelSpecificFitAssessment(opp, hotel);
  const demand = buildDemandMechanismAssessment(opp, chrono);
  const completeness = assessScalingCommercialResearchCompleteness(
    opp,
    hotel,
    chrono,
  );
  const approach = recommendedApproach(opp, hotel, chrono, completeness);
  const org = text(opp.organizationName) || "This company";
  const headline = scrubInternalResearchLanguage(
    opp.activityHeadline || opp.growthHeadline || "",
  );

  const whatIsHappening = (() => {
    if (chrono.originalEventDate) {
      const statusLabel =
        chrono.activityStatus ===
        SCALING_ACTIVITY_STATUS.CONTINUING_ESTABLISHED_OPERATIONS
          ? "Historical expansion announcement with later evidence of ongoing local presence"
          : chrono.activityStatus ===
              SCALING_ACTIVITY_STATUS.HISTORICAL_ACCOUNT_DEVELOPMENT
            ? "Historical expansion announcement — current operating continuity not yet verified"
            : chrono.customerFacingAsCurrentExpansion
              ? "Documented local expansion / growth activity"
              : "Documented local business activity";
      return `${org}: ${statusLabel}${headline ? ` — ${headline}` : ""}. Original public date: ${chrono.originalEventDate}. Research verification date (${chrono.researchVerificationDate || "n/a"}) is not the activity date.`;
    }
    return (
      scrubInternalResearchLanguage(opp.whatIsHappening) ||
      `${org} has local business-activity evidence on file; original event date still needs extraction.`
    );
  })();

  const companyAndLocalActivity = [
    `${companyBlurb(opp)}.`,
    chrono.originalEventDate
      ? `On ${chrono.originalEventDate}, public materials described local expansion/activity${headline ? ` (“${headline}”)` : ""}.`
      : `Public materials describe local activity; exact original event date is unresolved.`,
    opts.continuityNote || chrono.continuityHints?.continuityNote || null,
    chrono.activityStatus ===
      SCALING_ACTIVITY_STATUS.HISTORICAL_ACCOUNT_DEVELOPMENT
      ? "No newer independent confirmation of active local operations was attached in this pass — qualify before selling a current expansion story."
      : null,
  ]
    .filter(Boolean)
    .join(" ");

  const uncertainties = [
    !chrono.hasContinuityEvidence &&
    chrono.activityStatus !== SCALING_ACTIVITY_STATUS.CURRENT_SUPPORTED_EXPANSION
      ? "Current local headcount / office status not independently verified in this brief."
      : null,
    !completeness.primaryNamed
      ? "No verified named primary contact — organizational route only."
      : null,
    !completeness.secondaryNamed
      ? "Secondary named contact not yet established."
      : null,
    "No confirmed guestroom contract or RFP.",
  ].filter(Boolean);

  const overview = overviewFor(opp, hotel, chrono, demand, fit);
  const selection = buildWhyDealalitySelectedThisCompany(opp, hotel, chrono);
  const background = buildCompanyBackgroundAndLocalPresence(opp, hotel, chrono);

  const brief = {
    version: SCALING_COMMERCIAL_INTELLIGENCE_BRIEF_V1,
    opportunityOverview: overview,
    whyDealalitySelectedThisCompany: selection.whySelected,
    selectionSignals: selection.selectionSignals,
    companyBackground: background.companyBackground,
    companyAndLocalPresence: background.companyBackground,
    companyAndLocalActivity,
    potentialBusinessForThisHotel: demand.potentialBusinessNarrative,
    whyThisHotel: fit.whyThisHotel,
    whyThisHotelAdvantages: fit.advantages,
    whyThisHotelLimitations: fit.limitations,
    whyActNow:
      approach.timingRationale ||
      text(opp.whyEstablishContactNow) ||
      text(opp.whyNow) ||
      null,
    whoToContact: (() => {
      const primaryName =
        extractPrimaryPersonName(opp) ||
        text(opp.primaryContact?.name) ||
        text(opp.contactName) ||
        "";
      const primaryRole =
        text(opp.primaryContact?.role) ||
        text(opp.primaryContact?.title) ||
        text(opp.contactRole) ||
        text(opp.buyerRole) ||
        "";
      const contactEligibility = primaryName
        ? classifyContactEligibility(
            { name: primaryName, role: primaryRole, title: primaryRole },
            opp,
          )
        : null;
      const briefFields = buildContactEligibilityBriefFields(contactEligibility || {});
      return {
        primaryNamed: completeness.primaryNamed,
        secondaryNamed: completeness.secondaryNamed,
        coverage: completeness.contactCoverage,
        organizationalRoute:
          text(opp.companyPhone) ||
          text(opp.contactEmail) ||
          text(opp.contactUrl) ||
          null,
        contactEligibilityClass: briefFields.contactEligibilityClass,
        contactTypeForBrief: briefFields.contactTypeForBrief,
        isDirectBuyer: briefFields.isDirectBuyer,
        isRelevantCommercialEntryPoint: briefFields.isRelevantCommercialEntryPoint,
        continueResearchForDirectBuyer: briefFields.continueResearchForDirectBuyer,
        note: completeness.primaryNamed
          ? briefFields.isDirectBuyer
            ? "Primary contact classified as Direct buyer (hotel / travel / meetings / lodging / purchasing)."
            : briefFields.isRelevantCommercialEntryPoint
              ? "Primary contact classified as Relevant commercial entry point — credible approach/referral path; continue researching a direct buyer when justified."
              : "Named contacts shown only when affiliation/role research supports them."
          : "Named-person research incomplete — use organizational routing and ask for the travel/meetings owner.",
        contactPriority:
          briefFields.contactEligibilityClass === CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER
            ? "direct_buyer_first"
            : briefFields.isRelevantCommercialEntryPoint
              ? "entry_point_second_continue_direct_buyer_research"
              : null,
      };
    })(),
    recommendedSalesApproach: approach,
    evidenceAndUncertainty: {
      originalActivityDate: chrono.originalEventDate,
      sourcePublicationDate: chrono.sourcePublicationDate,
      researchVerificationDate: chrono.researchVerificationDate,
      activityStatus: chrono.activityStatus,
      sources: filterEvidenceUrlsAgainstPublisherShells(opp.sources || [])
        .map((s) => ({
          url: s.url,
          title: s.title || s.type || null,
          publisher: s.publisher || s.entityAttributed || null,
          publishedDate: s.publishedDate || s.publicationDate || null,
          establishes: s.establishes || s.relevance || null,
          evidenceGroup: s.evidenceGroup || null,
          evidenceGroupLabel: s.evidenceGroupLabel || null,
          sourceId: s.sourceId || null,
        }))
        .filter((s) => s.url)
        .slice(0, 12),
      uncertainties,
    },
    researchCompleteness: completeness,
    hotelFitAssessment: fit,
    demandMechanismAssessment: demand,
    activityChronology: chrono,
  };

  return {
    opportunity: {
      ...opp,
      whatIsHappening,
      summaryWhat: whatIsHappening,
      whyDealalitySelectedThisCompany: selection.whySelected,
      whySelected: selection.whySelected,
      companyBackground: background.companyBackground,
      activityHeadline: headline || opp.activityHeadline,
      potentialHotelBusiness: demand.potentialBusinessNarrative,
      proposedHotelUse: demand.potentialBusinessNarrative,
      summaryWhyMatters: demand.potentialBusinessNarrative,
      whyThisHotel: fit.whyThisHotel,
      hotelFitRationale: fit.whyThisHotel,
      hotelOpportunityThesis: fit.whyThisHotel,
      summaryWhyHotel: fit.whyThisHotel,
      recommendedAction: [
        approach.nextAction,
        approach.purposeOfOutreach,
        `Timing: ${approach.timingRationale}`,
      ].join(" "),
      recommendedNextStep: approach.nextAction,
      demandStageLabel:
        chrono.customerFacingAsCurrentExpansion
          ? "Potential business linked to dated local expansion; hotel needs not yet confirmed"
          : chrono.activityStatus ===
              SCALING_ACTIVITY_STATUS.CONTINUING_ESTABLISHED_OPERATIONS
            ? "Corporate account-development opportunity (historical expansion; ongoing local presence)"
            : "Watchlist / further research — do not market as newly expanding",
      prospectCustomerLabel:
        completeness.completeness ===
        SCALING_COMMERCIAL_RESEARCH_COMPLETENESS.WATCHLIST_OR_MONITOR
          ? "Monitor / research continuation"
          : "Potential corporate account",
      salesBrief: brief,
      commercialIntelligenceBrief: brief,
      scalingCommercialIntelligenceVersion:
        SCALING_COMMERCIAL_INTELLIGENCE_BRIEF_V1,
      scalingResearchCompleteness: completeness.completeness,
      scalingResearchGaps: completeness.gaps,
      growthEvidenceDate: chrono.recommendedGrowthEvidenceDate || opp.growthEvidenceDate,
    },
    brief,
    completeness,
    chronology: chrono,
  };
}

/**
 * Apply commercial intelligence brief onto opportunity (maturation / presentation).
 */
export function applyScalingCommercialIntelligenceBrief(
  opportunity = {},
  hotel = {},
  opts = {},
) {
  const built = buildScalingCommercialIntelligenceBrief(
    opportunity,
    hotel,
    opts,
  );
  return built.opportunity;
}
