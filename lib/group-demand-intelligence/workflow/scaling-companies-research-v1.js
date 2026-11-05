/**
 * Scaling Companies / Emerging Corporate Accounts — GDI research segment V1.
 *
 * Identifies growing companies whose local operations, expansion, distributed
 * teams, or customer activity could generate recurring hotel rooms and/or
 * meeting demand.
 *
 * Funding, hiring pages, or startup-directory listings alone do NOT qualify.
 * Standing corporate relationship prospects — do not invent event dates.
 * No NYC/SF hardcodes. No invented travel volume, budgets, or buying authority.
 */

import { createHash } from "node:crypto";
import { OPPORTUNITY_TYPE } from "../claim-types.js";
import { DEMAND_FAMILY, DEMAND_SIGNAL_TYPE } from "../demand-signal-types.js";
import {
  ORGANIZATION_TYPE,
  TRIGGER_TYPE,
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
import { buildDemandGeneratorSignal } from "../demand-generators/qualify-promote.js";
import {
  DEMAND_USE_CASE,
  RELATIONSHIP_GRAIN,
  normalizeDemandUseCases,
  guestroomStatusForUseCases,
} from "./shared-demand-model-v1.js";
import {
  applyScalingSalesPresentation,
  excerptIsInternalOpsNote,
  resolveCommercialContactPath,
  scrubInternalResearchLanguage,
} from "../scaling-companies-sales-presentation-v1.js";

export const SCALING_COMPANIES_RESEARCH_VERSION =
  "gdi-scaling-companies-research-v1";

export const SCALING_GROWTH_SIGNAL = Object.freeze({
  NEW_OFFICE_OR_LOCAL_EXPANSION: "NEW_OFFICE_OR_LOCAL_EXPANSION",
  HIRING_DISTRIBUTED_TEAMS: "HIRING_DISTRIBUTED_TEAMS",
  ONBOARDING_TRAINING: "ONBOARDING_TRAINING",
  EXECUTIVE_OR_INVESTOR_VISITS: "EXECUTIVE_OR_INVESTOR_VISITS",
  CUSTOMER_IMPLEMENTATION_TRAINING: "CUSTOMER_IMPLEMENTATION_TRAINING",
  COMPANY_GATHERING_OR_OFFSITE: "COMPANY_GATHERING_OR_OFFSITE",
  FUNDING_WITH_OPERATING_EXPANSION: "FUNDING_WITH_OPERATING_EXPANSION",
});

export const SCALING_GROWTH_SIGNAL_LABEL = Object.freeze({
  [SCALING_GROWTH_SIGNAL.NEW_OFFICE_OR_LOCAL_EXPANSION]:
    "New office or local expansion",
  [SCALING_GROWTH_SIGNAL.HIRING_DISTRIBUTED_TEAMS]:
    "Hiring growth / geographically distributed teams",
  [SCALING_GROWTH_SIGNAL.ONBOARDING_TRAINING]: "Onboarding / training activity",
  [SCALING_GROWTH_SIGNAL.EXECUTIVE_OR_INVESTOR_VISITS]:
    "Executive or investor visits",
  [SCALING_GROWTH_SIGNAL.CUSTOMER_IMPLEMENTATION_TRAINING]:
    "Customer implementation and training",
  [SCALING_GROWTH_SIGNAL.COMPANY_GATHERING_OR_OFFSITE]:
    "Company gathering or offsite",
  [SCALING_GROWTH_SIGNAL.FUNDING_WITH_OPERATING_EXPANSION]:
    "Funding with concrete operating expansion",
});

/** Contact role preference by proposed hotel use case */
export const SCALING_CONTACT_ROLE_BY_USE = Object.freeze({
  OFFICE_EXPANSION: ["workplace_operations", "office_manager", "facilities"],
  TRAINING_ONBOARDING: ["people_operations", "hr_ops", "learning_development"],
  EXECUTIVE_VISITS: ["executive_support", "ea", "chief_of_staff"],
  CUSTOMER_IMPLEMENTATION: ["customer_success", "professional_services", "events"],
  OFFSITE_GATHERING: ["events", "people_operations", "executive_support"],
  GENERAL_TRAVEL: ["travel_procurement", "workplace_operations", "people_operations"],
});

export const SCALING_QUALIFY_FAIL = Object.freeze({
  MISSING_COMPANY: "MISSING_COMPANY",
  MISSING_LOCAL_CONNECTION: "MISSING_LOCAL_CONNECTION",
  MISSING_DATED_GROWTH_EVIDENCE: "MISSING_DATED_GROWTH_EVIDENCE",
  FUNDING_ONLY: "FUNDING_ONLY",
  HIRING_PAGE_ONLY: "HIRING_PAGE_ONLY",
  DIRECTORY_ONLY: "DIRECTORY_ONLY",
  MISSING_HOTEL_USE_THESIS: "MISSING_HOTEL_USE_THESIS",
  MISSING_HOTEL_FIT: "MISSING_HOTEL_FIT",
  INVENTED_TRAVEL_VOLUME: "INVENTED_TRAVEL_VOLUME",
  INVENTED_EVENT_DATE: "INVENTED_EVENT_DATE",
  NOT_SCALING_COMPANY_ENTITY: "NOT_SCALING_COMPANY_ENTITY",
});

/**
 * Entity classes that must NOT be published as Scaling Companies merely because
 * discovery returned them. Prefer the named alternate GDI segment when known.
 */
export const SCALING_ENTITY_CLASS = Object.freeze({
  SCALING_COMPANY: "SCALING_COMPANY",
  PROFESSIONAL_PLANNER_DMC: "PROFESSIONAL_PLANNER_DMC",
  ATTRACTION_OR_VENUE_OPERATOR: "ATTRACTION_OR_VENUE_OPERATOR",
  INDIVIDUAL_PLANNER_OR_SOLE_OPERATOR: "INDIVIDUAL_PLANNER_OR_SOLE_OPERATOR",
  TOUR_OPERATOR: "TOUR_OPERATOR",
  EVENT_ORGANIZER_INTERMEDIARY: "EVENT_ORGANIZER_INTERMEDIARY",
  LODGING_OR_HOSPITALITY_OPERATOR: "LODGING_OR_HOSPITALITY_OPERATOR",
  UNKNOWN: "UNKNOWN",
});

export const SCALING_ENTITY_ALTERNATE_SEGMENT = Object.freeze({
  [SCALING_ENTITY_CLASS.PROFESSIONAL_PLANNER_DMC]: "professional_planners_dmc",
  [SCALING_ENTITY_CLASS.ATTRACTION_OR_VENUE_OPERATOR]:
    "tours_educational_groups",
  [SCALING_ENTITY_CLASS.INDIVIDUAL_PLANNER_OR_SOLE_OPERATOR]:
    "professional_planners_dmc",
  [SCALING_ENTITY_CLASS.TOUR_OPERATOR]: "tours_educational_groups",
  [SCALING_ENTITY_CLASS.EVENT_ORGANIZER_INTERMEDIARY]:
    "professional_planners_dmc",
  [SCALING_ENTITY_CLASS.LODGING_OR_HOSPITALITY_OPERATOR]: null,
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

function companyKey(name, domain) {
  const d = text(domain) || domainFromUrl(domain) || "";
  const n = lower(name).replace(/[^a-z0-9]+/g, " ").trim();
  return d ? `d:${d}` : `n:${n}`;
}

export function computeScalingCompanyOpportunityId(hotelId, organizationName, domain) {
  const seed = `${text(hotelId)}|${companyKey(organizationName, domain)}`;
  return `gdi_sc_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

/**
 * Resolve a research city/market label from hotel profile fields.
 * Prefer explicit city, then territory includes, then territory label.
 */
export function resolveScalingCompaniesResearchCity(hotelConfig = {}) {
  const city = text(hotelConfig.city);
  if (city) return city;
  for (const raw of hotelConfig.demandTerritory?.includes || []) {
    const first = text(raw).split(/\s+where\b|\s*\/\s*|,/)[0];
    if (first && first.length >= 3 && first.length <= 48) return first;
  }
  const label = text(hotelConfig.demandTerritory?.label);
  if (label) return label.split("/")[0].trim() || label;
  return "";
}

/**
 * Hotel/market fit for the scaling-companies segment.
 *
 * Research eligibility ≠ opportunity publication.
 * A hotel may have zero existing scaling opportunities and still qualify for discovery.
 * Resort / marina / coastal classifications are NOT automatic rejects.
 * Do not require urban/downtown keywords or arbitrary room/meeting floors.
 *
 * Hard requirements: resolvable identity, geography, and verified inventory
 * sufficient to run hotel-specific research. Explicit prohibitions still apply.
 * Commercial relevance is decided per opportunity (qualifyScalingCompanyCandidate).
 */
export function assessScalingCompaniesApplicability(hotelConfig = {}) {
  const hotelId = text(hotelConfig.hotelId || hotelConfig.id);
  const displayName = text(hotelConfig.displayName || hotelConfig.name);
  if (!hotelId && !displayName) {
    return {
      applicable: false,
      profileStatus: "UNKNOWN_UNTIL_PROFILE",
      rationale:
        "Canonical hotel identity is missing — scaling-company research cannot be scoped.",
    };
  }

  const exclusions = [
    ...(hotelConfig.identityExclusions || []),
    ...(hotelConfig.gdiSegmentExclusions || []),
    ...(hotelConfig.commercialPriorities?.excludedSegments || []),
  ]
    .map(lower)
    .filter(Boolean);
  if (
    exclusions.some((x) =>
      /scaling.?compan|emerging.?corporate|corporate.?account/.test(x),
    )
  ) {
    return {
      applicable: false,
      rationale:
        "Hotel profile explicitly excludes scaling-companies / emerging corporate accounts research.",
    };
  }
  if (hotelConfig.commercialPriorities?.scalingCompaniesResearch === false) {
    return {
      applicable: false,
      rationale:
        "Hotel profile sets scalingCompaniesResearch=false — research prohibited for this property.",
    };
  }

  const researchCity = resolveScalingCompaniesResearchCity(hotelConfig);
  const territoryLabel = text(hotelConfig.demandTerritory?.label);
  const territoryIncludes = (hotelConfig.demandTerritory?.includes || [])
    .map(text)
    .filter(Boolean);
  const hasGeography = Boolean(
    researchCity || territoryLabel || territoryIncludes.length,
  );
  if (!hasGeography) {
    return {
      applicable: false,
      profileStatus: "UNKNOWN_UNTIL_PROFILE",
      rationale:
        "No verified city or demand-territory geography — hotel-specific scaling research cannot be conducted yet.",
    };
  }

  const rooms = Number(hotelConfig.capabilityProfile?.totalGuestrooms || 0) || 0;
  const meetings =
    Number(
      hotelConfig.capabilityProfile?.meetingRoomCount ??
        hotelConfig.capabilityProfile?.meetingRoomsIndoor ??
        0,
    ) || 0;
  const meetingSqFt =
    Number(hotelConfig.capabilityProfile?.totalMeetingSpaceSqFt || 0) || 0;
  const inventoryKnown = rooms > 0 || meetings > 0 || meetingSqFt > 0;
  if (!inventoryKnown) {
    return {
      applicable: false,
      profileStatus: "UNKNOWN_UNTIL_PROFILE",
      rationale:
        "Guestroom and meeting inventory are unverified. Scaling-company research is not applicable and not exhausted until the property profile is complete.",
    };
  }

  // Soft signal only — never used as a hard reject for resorts / smaller meetings hotels.
  const blob = lower(
    [
      displayName,
      researchCity,
      territoryLabel,
      ...territoryIncludes,
      hotelConfig.capabilityProfile?.classification,
      hotelConfig.capabilityProfile?.serviceLevel,
      ...(hotelConfig.commercialPriorities?.targetSegments || []),
      ...(hotelConfig.commercialPriorities?.demandAnchorFocus || []),
    ].join(" "),
  );
  const businessCatchmentHints =
    /downtown|urban|business|meetings|convention|bayfront|harbor|capitol|marina|resort|coastal|corporate|offsite|retreat|training|ballroom/.test(
      blob,
    ) ||
    rooms >= 80 ||
    meetings >= 2 ||
    meetingSqFt >= 5000;

  return {
    applicable: true,
    researchCity: researchCity || null,
    inventory: { rooms, meetings, meetingSqFt },
    rationale: businessCatchmentHints
      ? "Verified identity, geography, and inventory support hotel-specific scaling-company discovery. Opportunity fit is evaluated per candidate — empty segment inventory does not block research."
      : "Verified identity, geography, and inventory present. Scaling-company research may proceed; keep opportunity-level qualification strict for hotel-use thesis and local connection.",
  };
}

function isFundingOnly(candidate = {}) {
  const signals = normalizeGrowthSignals(candidate);
  if (!signals.length) return false;
  const onlyFunding = signals.every(
    (s) => s === SCALING_GROWTH_SIGNAL.FUNDING_WITH_OPERATING_EXPANSION
  );
  const hasOperatingExpansion =
    candidate.operatingExpansionEvidence === true ||
    Boolean(text(candidate.operatingExpansionNote));
  return onlyFunding && !hasOperatingExpansion && !text(candidate.localConnection);
}

function isHiringPageOnly(candidate = {}) {
  const signals = normalizeGrowthSignals(candidate);
  const hiringOnly =
    signals.length === 1 &&
    signals[0] === SCALING_GROWTH_SIGNAL.HIRING_DISTRIBUTED_TEAMS;
  const src = lower(
    [
      candidate.sourceUrl,
      ...(candidate.sourceUrls || []),
      candidate.evidenceNote,
    ].join(" ")
  );
  return (
    hiringOnly &&
    /\/careers|\/jobs|greenhouse|lever\.co|workday|ashbyhq/.test(src) &&
    !text(candidate.localConnection) &&
    !candidate.distributedTeamsEvidence
  );
}

function isDirectoryOnly(candidate = {}) {
  const src = lower(
    [candidate.sourceUrl, ...(candidate.sourceUrls || []), candidate.sourceType].join(
      " "
    )
  );
  return (
    /crunchbase|pitchbook|angel\.co|wellfound|startup.?directory|ycombinator\.com\/companies/.test(
      src
    ) &&
    !text(candidate.datedGrowthEvidence) &&
    !candidate.growthEvidenceDate
  );
}

function normalizeGrowthSignals(candidate = {}) {
  const raw = []
    .concat(candidate.growthSignals || [])
    .concat(candidate.growthSignal ? [candidate.growthSignal] : [])
    .map((s) => text(s).toUpperCase())
    .filter(Boolean);
  return [...new Set(raw)].filter((s) =>
    Object.values(SCALING_GROWTH_SIGNAL).includes(s)
  );
}

function mapGrowthSignalToTrigger(signal) {
  switch (signal) {
    case SCALING_GROWTH_SIGNAL.NEW_OFFICE_OR_LOCAL_EXPANSION:
      return TRIGGER_TYPE.OFFICE_OPENING;
    case SCALING_GROWTH_SIGNAL.HIRING_DISTRIBUTED_TEAMS:
      return TRIGGER_TYPE.HIRING_GROWTH;
    case SCALING_GROWTH_SIGNAL.ONBOARDING_TRAINING:
      return TRIGGER_TYPE.ONBOARDING_TRAINING;
    case SCALING_GROWTH_SIGNAL.EXECUTIVE_OR_INVESTOR_VISITS:
      return TRIGGER_TYPE.EXECUTIVE_INVESTOR_VISIT;
    case SCALING_GROWTH_SIGNAL.CUSTOMER_IMPLEMENTATION_TRAINING:
      return TRIGGER_TYPE.CUSTOMER_IMPLEMENTATION;
    case SCALING_GROWTH_SIGNAL.COMPANY_GATHERING_OR_OFFSITE:
      return TRIGGER_TYPE.COMPANY_GATHERING_OFFSITE;
    case SCALING_GROWTH_SIGNAL.FUNDING_WITH_OPERATING_EXPANSION:
      return TRIGGER_TYPE.FUNDING_WITH_EXPANSION;
    default:
      return TRIGGER_TYPE.OTHER;
  }
}

function suggestedContactRoles(candidate = {}) {
  const use = lower(candidate.proposedHotelUse || candidate.hotelUseCase || "");
  if (/office|expansion|relocation|facility/.test(use)) {
    return SCALING_CONTACT_ROLE_BY_USE.OFFICE_EXPANSION;
  }
  if (/train|onboard|academy/.test(use)) {
    return SCALING_CONTACT_ROLE_BY_USE.TRAINING_ONBOARDING;
  }
  if (/executive|investor|board/.test(use)) {
    return SCALING_CONTACT_ROLE_BY_USE.EXECUTIVE_VISITS;
  }
  if (/customer|implementation|professional.?services/.test(use)) {
    return SCALING_CONTACT_ROLE_BY_USE.CUSTOMER_IMPLEMENTATION;
  }
  if (/offsite|gathering|all.?hands|kickoff/.test(use)) {
    return SCALING_CONTACT_ROLE_BY_USE.OFFSITE_GATHERING;
  }
  return SCALING_CONTACT_ROLE_BY_USE.GENERAL_TRAVEL;
}

/**
 * Qualify a scaling-company candidate. Does not invent demand.
 */
export function qualifyScalingCompanyCandidate(candidate = {}, hotelConfig = {}) {
  const fail = [];
  const warnings = [];
  const company = text(candidate.organizationName || candidate.companyName);
  if (!company) fail.push(SCALING_QUALIFY_FAIL.MISSING_COMPANY);

  const localConnection = text(
    candidate.localConnection ||
      candidate.localPresence ||
      candidate.marketConnection
  );
  if (!localConnection) fail.push(SCALING_QUALIFY_FAIL.MISSING_LOCAL_CONNECTION);

  const datedEvidence = text(
    candidate.datedGrowthEvidence || candidate.growthEvidenceNote
  );
  const evidenceDate = text(candidate.growthEvidenceDate || candidate.evidenceDate);
  if (!datedEvidence && !evidenceDate) {
    fail.push(SCALING_QUALIFY_FAIL.MISSING_DATED_GROWTH_EVIDENCE);
  }

  if (isFundingOnly(candidate)) fail.push(SCALING_QUALIFY_FAIL.FUNDING_ONLY);
  if (isHiringPageOnly(candidate)) fail.push(SCALING_QUALIFY_FAIL.HIRING_PAGE_ONLY);
  if (isDirectoryOnly(candidate)) fail.push(SCALING_QUALIFY_FAIL.DIRECTORY_ONLY);

  const entityFitness = classifyScalingCompanyEntityFitness(candidate);
  if (!entityFitness.ok) {
    fail.push(SCALING_QUALIFY_FAIL.NOT_SCALING_COMPANY_ENTITY);
    warnings.push(
      `${entityFitness.entityClass}: ${entityFitness.note}` +
        (entityFitness.alternateSegment
          ? ` Prefer segment ${entityFitness.alternateSegment}.`
          : ""),
    );
  }

  const hotelUse = text(
    candidate.proposedHotelUse ||
      candidate.hotelUseCase ||
      candidate.plausibleTravelOrMeetingUse
  );
  if (!hotelUse) fail.push(SCALING_QUALIFY_FAIL.MISSING_HOTEL_USE_THESIS);

  const hotelFit = text(
    candidate.hotelFitRationale ||
      candidate.fitExplanation ||
      candidate.whyThisHotel
  );
  if (!hotelFit) fail.push(SCALING_QUALIFY_FAIL.MISSING_HOTEL_FIT);

  if (
    candidate.inventedTravelVolume === true ||
    candidate.inventedRoomNights === true ||
    candidate.inventedBudget === true
  ) {
    fail.push(SCALING_QUALIFY_FAIL.INVENTED_TRAVEL_VOLUME);
  }
  if (
    candidate.inventedEventDate === true ||
    (candidate.forceEventDate === true && candidate.eventStartDate)
  ) {
    fail.push(SCALING_QUALIFY_FAIL.INVENTED_EVENT_DATE);
  }

  const applicability = assessScalingCompaniesApplicability(hotelConfig);
  if (!applicability.applicable) {
    warnings.push(applicability.rationale);
  }

  const establishedFacts = []
    .concat(candidate.establishedFacts || [])
    .map(text)
    .filter(Boolean);
  const inferredDemand = []
    .concat(candidate.inferredHotelDemand || [])
    .map(text)
    .filter(Boolean);

  return {
    ok: fail.length === 0,
    failReasons: fail,
    warnings,
    company,
    localConnection: localConnection || null,
    growthSignals: normalizeGrowthSignals(candidate),
    evidenceDate: evidenceDate || null,
    datedGrowthEvidence: datedEvidence || null,
    hotelUse: hotelUse || null,
    hotelFit: hotelFit || null,
    suggestedContactRoles: suggestedContactRoles(candidate),
    establishedFacts,
    inferredHotelDemand: inferredDemand,
    applicability,
  };
}

/**
 * Build standing corporate-account opportunity card (no invented event date).
 */
export function buildScalingCompanyOpportunityCard(candidate = {}, opts = {}) {
  const hotelId = text(opts.hotelId || candidate.hotelId);
  const hotelConfig = opts.hotelConfig || {};
  const qualification = qualifyScalingCompanyCandidate(candidate, hotelConfig);
  if (!qualification.ok) {
    return {
      ok: false,
      code: "NOT_QUALIFIED",
      failReasons: qualification.failReasons,
      warnings: qualification.warnings,
    };
  }

  const company = qualification.company;
  const domain =
    text(candidate.officialDomain) ||
    domainFromUrl(candidate.website || candidate.sourceUrl);
  const opportunityId =
    text(candidate.opportunityId) ||
    computeScalingCompanyOpportunityId(hotelId, company, domain);

  const growthSignals = qualification.growthSignals;
  const growthLabel = growthSignals
    .map((s) => SCALING_GROWTH_SIGNAL_LABEL[s] || s)
    .join("; ");
  const whyNow =
    text(candidate.whyEstablishContactNow) ||
    text(candidate.whyNow) ||
    (qualification.evidenceDate
      ? `Recent growth evidence dated ${qualification.evidenceDate} — establish the relationship before lodging patterns harden with competitors.`
      : "Establish an introductory corporate relationship while expansion activity is observable.");

  const constraints = []
    .concat(candidate.constraints || [])
    .concat(candidate.uncertainties || [])
    .map(text)
    .filter(Boolean);
  if (!constraints.length) {
    constraints.push(
      "Travel volume, budgets, and buying authority are not established from public research alone."
    );
  }

  const contactRole =
    text(candidate.contactRole) ||
    qualification.suggestedContactRoles[0] ||
    "workplace_operations";
  const contactName = text(candidate.contactName || candidate.eventContactName) || null;
  const contactEmail = text(candidate.contactEmail || candidate.eventContactEmail) || null;
  const contactPhone = text(candidate.contactPhone || candidate.eventContactPhone) || null;
  const contactUrl =
    text(candidate.contactUrl) ||
    text(candidate.organizationPath) ||
    text(candidate.website) ||
    null;

  const recommendedAction =
    text(candidate.recommendedAction) ||
    text(candidate.nextAction) ||
    `Introduce ${hotelConfig.displayName || "the hotel"} to ${contactRole.replace(/_/g, " ")} for ${company} — reference ${growthLabel || "recent local growth"} and ask how overnight and meeting needs are currently handled.`;

  const nowIso = new Date().toISOString();
  const sources = []
    .concat(candidate.sources || [])
    .concat(
      (candidate.sourceUrls || []).map((url) => ({
        url,
        type: "growth_evidence",
        title: "Growth evidence",
      }))
    );
  if (candidate.sourceUrl && !sources.some((s) => s.url === candidate.sourceUrl)) {
    sources.push({
      url: candidate.sourceUrl,
      type: "growth_evidence",
      title: "Growth evidence",
    });
  }

  const dgId = computeDemandGeneratorId({
    organizationName: company,
    officialDomain: domain,
    website: candidate.website || contactUrl,
  });
  const fitId = computeHotelGeneratorFitId({
    hotelId,
    demandGeneratorId: dgId,
  });

  const localClean =
    scrubInternalResearchLanguage(qualification.localConnection) ||
    scrubInternalResearchLanguage(candidate.activityHeadline) ||
    scrubInternalResearchLanguage(candidate.title) ||
    null;
  const growthEvidenceClean =
    scrubInternalResearchLanguage(qualification.datedGrowthEvidence) ||
    (excerptIsInternalOpsNote(qualification.datedGrowthEvidence)
      ? localClean
      : scrubInternalResearchLanguage(qualification.datedGrowthEvidence));
  const hotelFitClean =
    scrubInternalResearchLanguage(qualification.hotelFit) ||
    `${hotelConfig.displayName || "This hotel"} can compete for overnight/meeting needs linked to ${company} local activity if lodging or meetings materialize.`;

  const draft = {
    id: opportunityId,
    opportunityId,
    hotelId,
    organizationName: company,
    company,
    title: `${company} — Scaling Corporate Account`,
    opportunityName: `${company} — Scaling Corporate Account`,
    opportunityType: OPPORTUNITY_TYPE.CORPORATE_ACCOUNT_RELATIONSHIP,
    opportunityTypeLabel: "Corporate Account Relationship",
    detailLayout: "CORPORATE_ACCOUNT_RELATIONSHIP",
    demandFamily: DEMAND_FAMILY.GROUP_MEETINGS,
    demandSignalType: DEMAND_SIGNAL_TYPE.CORPORATE_MEETING,
    segment: "Scaling Companies / Emerging Corporate Accounts",
    researchSegment: "scaling_companies",
    relationshipGrain: RELATIONSHIP_GRAIN.STANDING_ACCOUNT,
    demandUseCases: normalizeDemandUseCases(
      candidate.demandUseCases ||
        candidate.useCases || [
          DEMAND_USE_CASE.ROOMS_PLUS_MEETINGS,
          DEMAND_USE_CASE.GROUP_LODGING,
          DEMAND_USE_CASE.MEETINGS_AND_CATERING,
        ]
    ),
    guestroomStatus: guestroomStatusForUseCases(
      candidate.demandUseCases ||
        candidate.useCases || [DEMAND_USE_CASE.ROOMS_PLUS_MEETINGS],
      candidate.guestroomStatus
    ),
    standingOpportunity: true,
    // Explicitly no invented occasion date
    eventStartDate: null,
    eventEndDate: null,
    eventDateDisplay: "Ongoing corporate relationship",
    timing: "Ongoing — no single dated occasion required",
    localPresence: localClean,
    localConnection: localClean,
    growthSignals,
    growthSignalLabels: growthSignals.map(
      (s) => SCALING_GROWTH_SIGNAL_LABEL[s] || s
    ),
    growthEvidenceDate: qualification.evidenceDate,
    datedGrowthEvidence: growthEvidenceClean,
    proposedHotelUse: scrubInternalResearchLanguage(qualification.hotelUse),
    potentialHotelUseCases: scrubInternalResearchLanguage(qualification.hotelUse),
    hotelFitRationale: hotelFitClean,
    whyThisHotel: hotelFitClean,
    hotelOpportunityThesis: hotelFitClean,
    fitExplanation: hotelFitClean,
    summaryWhyHotel: hotelFitClean,
    summaryWhat: localClean
      ? `${company}: ${localClean}.`
      : `${company}: documented local growth activity.`,
    summaryWhyMatters: scrubInternalResearchLanguage(qualification.hotelUse),
    whyNow: scrubInternalResearchLanguage(whyNow),
    whyEstablishContactNow: scrubInternalResearchLanguage(whyNow),
    constraints,
    uncertainties: constraints,
    establishedFacts: (qualification.establishedFacts || [])
      .map(scrubInternalResearchLanguage)
      .filter(Boolean),
    inferredHotelDemand: qualification.inferredHotelDemand,
    contactRole,
    contactResponsibility: contactRole.replace(/_/g, " "),
    suggestedContactRoles: qualification.suggestedContactRoles,
    contactName,
    contactEmail,
    contactPhone,
    contactUrl,
    ...resolveCommercialContactPath({
      contactName,
      contactEmail,
      contactPhone,
      contactUrl,
    }),
    recommendedAction: scrubInternalResearchLanguage(recommendedAction),
    recommendedNextStep: scrubInternalResearchLanguage(recommendedAction),
    // Priority / action / sourcing kept separate — introductory contact may be warranted
    priority: candidate.priority || "MEDIUM_PRIORITY",
    // Internal venue taxonomy only — customer sanitize clears venueSourcingStatusLabel.
    venueSourcingStatus: "OPEN_UNRESOLVED",
    venueSourcingStatusLabel: null,
    evidenceStatusLabel: null,
    roomDemandStatus: "UNKNOWN",
    doNotEstimateAnnualRevenue: true,
    customerFacingState: candidate.publish === false ? "INTERNAL_ONLY" : "ACTIONABLE_NOW",
    customerVisible: candidate.publish !== false,
    actionable: candidate.publish !== false,
    // Internal publication basis — never echoed into customer narrative
    prospectPublicationBasis:
      candidate.publish === false ? null : "REVIEWED_PACKAGE_V1",
    prospectHotelFacing: candidate.publish !== false,
    prospectReviewSection: "EXPLORATORY",
    prospectCustomerLabel: "Potential opportunity",
    prospectCommercialLabel: "Scaling corporate account",
    dgGeneratorId: dgId,
    hotelGeneratorFitId: fitId,
    schemaVersion: SCALING_COMPANIES_RESEARCH_VERSION,
    researchVersion:
      candidate.researchVersion || SCALING_COMPANIES_RESEARCH_VERSION,
    evidenceFreshnessAt: qualification.evidenceDate || nowIso.slice(0, 10),
    lastVerifiedAt: nowIso,
    firstSeenAt: candidate.firstSeenAt || nowIso,
    updatedAt: nowIso,
    sources,
    officialSource: sources[0]?.url || null,
    hotelDisplayName: hotelConfig.displayName || null,
  };

  return {
    ok: true,
    qualification,
    opportunity: applyScalingSalesPresentation(draft, {
      hotelDisplayName: hotelConfig.displayName,
    }),
  };
}

/**
 * Build Demand Generator + program + fit + signal package for persistence.
 * Signal uses CURRENT timing (ongoing relationship) — not an invented event date.
 */
export function buildScalingCompanyDemandGeneratorPackage(
  candidate = {},
  opts = {}
) {
  const card = buildScalingCompanyOpportunityCard(candidate, opts);
  if (!card.ok) return card;

  const opp = card.opportunity;
  const hotelId = opp.hotelId;
  const domain =
    text(candidate.officialDomain) ||
    domainFromUrl(candidate.website || candidate.sourceUrl);

  // Airtable Organization Type select does not yet include SCALING_COMPANY —
  // persist as LARGE_EMPLOYER and keep scaling semantics in notes + researchSegment.
  const generator = buildDemandGeneratorEntity({
    organizationName: opp.organizationName,
    officialDomain: domain,
    website: candidate.website || opp.contactUrl,
    organizationType: ORGANIZATION_TYPE.LARGE_EMPLOYER,
    generatorStatus: GENERATOR_STATUS.VERIFIED,
    headquartersLocation:
      text(candidate.headquartersLocation) ||
      [text(candidate.hqCity), text(candidate.hqCountry)].filter(Boolean).join(", ") ||
      null,
    notes: `Scaling company / emerging corporate account (researchSegment=scaling_companies). Local connection: ${opp.localConnection}`,
  });

  const program = buildProgramEntity({
    demandGeneratorId: generator.demandGeneratorId,
    programName: "Corporate travel & meetings relationship",
    programType: PROGRAM_TYPE.OTHER,
    recurrenceStatus: RECURRENCE_STATUS.POSSIBLE_RECURRING,
    notes:
      "Standing corporate relationship — not a single dated occasion unless separately evidenced.",
  });

  const fit = buildHotelDemandGeneratorFit({
    hotelId,
    demandGeneratorId: generator.demandGeneratorId,
    marketRelevance: candidate.marketRelevance || "MEDIUM",
    productFit: candidate.productFit || "MEDIUM",
    generatorRelevance: "MEDIUM",
    likelyTravelCreation: "MEDIUM",
    programObservability: "MEDIUM",
    buyerAccessibility: opp.contactEmail || opp.contactUrl ? "MEDIUM" : "LOW",
    evidenceQuality: "MEDIUM",
    fitRationale: opp.whyThisHotel,
  });

  const primarySignal =
    (opp.growthSignals && opp.growthSignals[0]) ||
    SCALING_GROWTH_SIGNAL.NEW_OFFICE_OR_LOCAL_EXPANSION;
  const signal = buildDemandGeneratorSignal({
    demandGeneratorId: generator.demandGeneratorId,
    programId: program.programId,
    hotelId,
    hotelGeneratorFitId: fit.fitId,
    triggerType: mapGrowthSignalToTrigger(primarySignal),
    title: `${opp.organizationName} — ${SCALING_GROWTH_SIGNAL_LABEL[primarySignal] || "growth signal"}`,
    // Ongoing — CURRENT timing, no invented start date
    timingStatus: "CURRENT",
    eventStartDate: null,
    hotelDemandThesis: opp.proposedHotelUse,
    lodgingDemandThesis: true,
    hasCredibleLodgingDemand: true,
    hotelFitOk: true,
    marketRelevance: candidate.marketRelevance || "MEDIUM",
    recommendedAction: opp.recommendedAction,
    sourceUrl: opp.sources?.[0]?.url || null,
    sourceUrls: (opp.sources || []).map((s) => s.url).filter(Boolean),
    gdiOpportunityId: opp.id,
    // Bypass dated-occasion requirement for standing corporate relationships
    generatorOnly: false,
  });

  // Standing relationships may fail qualifyGeneratorSignal on NO_FUTURE_TIMING —
  // mark explicitly for coordinators: opportunity card is authoritative for this segment.
  return {
    ok: true,
    opportunity: opp,
    demandGenerator: generator,
    program,
    hotelGeneratorFit: fit,
    signal: {
      ...signal,
      signalStatus:
        signal.signalStatus === "REJECTED" &&
        (signal.failReasons || []).includes("NO_FUTURE_TIMING")
          ? "QUALIFIED"
          : signal.signalStatus,
      standingCorporateRelationship: true,
      note: "Standing corporate account — opportunity card does not require an invented event date.",
    },
    dedupeKey: companyKey(opp.organizationName, domain),
  };
}

/**
 * Match existing opportunities that already represent this account (dedupe).
 */
export function findExistingScalingCompanyMatches(
  opportunities = [],
  organizationName,
  domain
) {
  const key = companyKey(organizationName, domain);
  const nameRe = new RegExp(
    text(organizationName)
      .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
      .replace(/\s+/g, "\\s+"),
    "i"
  );
  return (opportunities || []).filter((o) => {
    if (
      o.opportunityType === OPPORTUNITY_TYPE.CORPORATE_ACCOUNT_RELATIONSHIP ||
      o.researchSegment === "scaling_companies" ||
      o.detailLayout === "CORPORATE_ACCOUNT_RELATIONSHIP"
    ) {
      const ok = companyKey(o.organizationName || o.company, domainFromUrl(o.website));
      if (ok === key || nameRe.test(text(o.organizationName || o.company))) return true;
    }
    // Same undifferentiated corporate account under other types
    if (
      /corporate|scaling|emerging/i.test(text(o.segment)) &&
      nameRe.test(text(o.organizationName || o.company || o.title))
    ) {
      return true;
    }
    return false;
  });
}

/**
 * Classify whether an existing opportunity counts toward scaling-companies coverage.
 */
export function isScalingCompanyOpportunity(opp = {}) {
  if (
    opp.opportunityType === OPPORTUNITY_TYPE.CORPORATE_ACCOUNT_RELATIONSHIP ||
    opp.detailLayout === "CORPORATE_ACCOUNT_RELATIONSHIP" ||
    opp.researchSegment === "scaling_companies"
  ) {
    return true;
  }
  const blob = lower(
    [
      opp.segment,
      opp.title,
      opp.organizationName,
      opp.summaryWhat,
      opp.hotelOpportunityThesis,
    ].join(" ")
  );
  return (
    /scaling compan|emerging corporate|corporate account relationship/.test(blob) ||
    (/office opening|local expansion|distributed team|customer implementation|company offsite/.test(
      blob
    ) &&
      /corporate|company|employer/.test(blob))
  );
}

/**
 * Commercial entity fitness for the Scaling Companies segment.
 *
 * A DMC, attraction, individual retreat planner, tour operator, or pure event
 * intermediary is NOT a scaling company — even with a usable contact route.
 * Retain evidence and route to the alternate segment; do not force publication.
 */
const DESTINATION_PROGRAM_HINT_RE =
  /\b(incentive(?:\s+travel|\s+trip|\s+program)?|viajes? de incentivo|convenci[oó]n(?:es)? de ventas|kickoff(?:\s+comercial|\s+de ventas|\s+sales)?|sales kickoff|leadership retreat|retiro(?:s)? de liderazgo|corporate retreat|distributor(?:s)? meeting|employee recognition|programa(?:s)? de reconocimiento|residential training|capacitaci[oó]n residencial|evento(?:s)? corporativ)\b/i;

export function classifyScalingCompanyEntityFitness(candidate = {}) {
  const name = text(candidate.organizationName || candidate.company || candidate.companyName);
  const blob = lower(
    [
      name,
      candidate.title,
      candidate.opportunityName,
      candidate.summaryWhat,
      candidate.whatIsHappening,
      candidate.localPresence,
      candidate.localConnection,
      candidate.datedGrowthEvidence,
      candidate.activityHeadline,
      candidate.excerpt,
      candidate.note,
      candidate.organizationType,
      candidate.entityType,
      candidate.companyType,
      ...(Array.isArray(candidate.sources)
        ? candidate.sources.map((s) => `${s?.title || ""} ${s?.url || ""}`)
        : []),
    ].join(" ")
  );

  const reasons = [];
  let entityClass = SCALING_ENTITY_CLASS.UNKNOWN;

  const looksLikePersonName =
    Boolean(name) &&
    /^[A-ZÁÉÍÓÚÑ][a-záéíóúñ'’.-]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ'’.-]+){1,3}$/.test(
      name,
    ) &&
    !/\b(inc|llc|ltd|sa|s\.?a\.?|corp|company|group|hotels?|resort|olive|concepts|analytics|technologies|solutions|systems|partners|capital|ventures|industrias|empresa)\b/i.test(
      name,
    );

  // Lodging competitors / flex-living operators are not hotel demand accounts.
  const lodgingName =
    /\b(hotels?|hoteles|resorts?|resort)\b/i.test(name) &&
    !/\b(software|tech|technology|systems|saas|fintech|platform)\b/i.test(blob);
  if (
    lodgingName ||
    /\b(aparthotel|coliving|co-living|flex living|serviced apartment|extended stay operator|hospitality across|beach house)\b/.test(
      blob,
    ) ||
    /\b(kempinski|marriott|hilton|hyatt|ihg|accor|radisson|wyndham|fiesta americana|four seasons|rosewood|mandarin oriental|oasis hotels?|iberostar|city express|hoteles city|barcel[oó]|secrets resorts?|dreams resorts?|riuing|hard rock hotel)\b/.test(
      blob,
    )
  ) {
    entityClass = SCALING_ENTITY_CLASS.LODGING_OR_HOSPITALITY_OPERATOR;
    reasons.push("entity_reads_as_lodging_or_flex_living_operator");
  } else if (
    /\b(dmc|destination management|incentive travel|incentives?,?\s*meetings|meetings\s*&\s*groups|corporate event planner)\b/.test(
      blob,
    ) ||
    /\bdestination\s+management\b/.test(blob)
  ) {
    entityClass = SCALING_ENTITY_CLASS.PROFESSIONAL_PLANNER_DMC;
    reasons.push("entity_reads_as_dmc_or_incentive_planner");
  } else if (
    looksLikePersonName &&
    /\b(retreats?|incentive trips?|corporate retreats?|villa|boutique property|plans? retreats?)\b/.test(
      blob,
    )
  ) {
    entityClass = SCALING_ENTITY_CLASS.INDIVIDUAL_PLANNER_OR_SOLE_OPERATOR;
    reasons.push("entity_reads_as_individual_retreat_planner");
  } else if (
    /\b(jungle camp|adventure park|theme park|zipline|paintball|team[- ]?building (?:venue|camp|park|activity)|attraction)\b/.test(
      blob,
    )
  ) {
    entityClass = SCALING_ENTITY_CLASS.ATTRACTION_OR_VENUE_OPERATOR;
    reasons.push("entity_reads_as_attraction_or_team_building_venue");
  } else if (
    /\b(tour operator|tour company|sightseeing tours?|day trips?)\b/.test(blob)
  ) {
    entityClass = SCALING_ENTITY_CLASS.TOUR_OPERATOR;
    reasons.push("entity_reads_as_tour_operator");
  } else if (
    (/\b(event (?:organizer|organiser|agency|production)|meeting planner|wedding planner|brand experiences?|corporate events?|executive events?)\b/.test(
      blob,
    ) ||
      /\b(we create hospitality-driven|produced across|start a project)\b/.test(
        blob,
      )) &&
    !/\b(office opening|new offices?|global footprint|saas|software|fintech|biotech|manufactur|clinic|hospital)\b/.test(
      blob,
    )
  ) {
    entityClass = SCALING_ENTITY_CLASS.EVENT_ORGANIZER_INTERMEDIARY;
    reasons.push("entity_reads_as_event_intermediary_not_employer");
  } else if (
    /\b(office opening|new offices?|local expansion|hiring|distributed team|customer implementation|series [abc]|workforce|global footprint)\b/.test(
      blob,
    ) ||
    /\b(opened|opens|opening)\b.{0,60}\boffice\b/.test(blob) ||
    /\b(inc|llc|ltd|technologies|software|saas|analytics|sa de cv|s\.?a\.? de c\.?v\.?|industrias|empresa|corporativo|company|corp\.?|gmbh|plc)\b/.test(
      blob,
    ) ||
    // Destination program activity + named employer-like org (not lodging/DMC already ruled out)
    (DESTINATION_PROGRAM_HINT_RE.test(blob) &&
      Boolean(name) &&
      name.length >= 3 &&
      !looksLikePersonName)
  ) {
    entityClass = SCALING_ENTITY_CLASS.SCALING_COMPANY;
    reasons.push("entity_reads_as_employer_or_operating_company");
  }

  const ok = entityClass === SCALING_ENTITY_CLASS.SCALING_COMPANY;
  return {
    ok,
    entityClass,
    alternateSegment: ok
      ? null
      : SCALING_ENTITY_ALTERNATE_SEGMENT[entityClass] || null,
    reasons,
    organizationName: name || null,
    note: ok
      ? "Entity fits Scaling Companies / emerging corporate account definition."
      : "Not a scaling company — retain evidence; do not force scaling-segment publication.",
  };
}

/**
 * Build research queries for unpaid/native discovery (plan only — no paid execution).
 */
export function buildScalingCompanyResearchQueries(hotelConfig = {}) {
  const city = resolveScalingCompaniesResearchCity(hotelConfig) || "market";
  const territory = text(hotelConfig.demandTerritory?.label) || city;
  const nearby = (hotelConfig.demandTerritory?.includes || [])
    .map((s) => text(s).split(/\s+where\b/)[0].trim())
    .filter((s) => s && s.toLowerCase() !== city.toLowerCase())
    .slice(0, 3);
  const nearbyClause = nearby.length
    ? ` OR "${nearby.join('" OR "')}"`
    : "";
  const localePrimary = lower(
    hotelConfig.locale?.primary || hotelConfig.capabilityProfile?.localePrimary || "",
  );
  const country = lower(hotelConfig.country || hotelConfig.demandTerritory?.country || "");
  const spanishMarket =
    localePrimary === "es" ||
    country === "mexico" ||
    country === "méxico" ||
    country === "mx";
  const hasMarina = hotelConfig.capabilityProfile?.marinaAccess === true;
  const lodgingTerms = hasMarina
    ? 'hotel OR "meeting space" OR marina OR resort OR rooms'
    : 'hotel OR "meeting space" OR resort OR rooms OR "espacios de reunión"';

  const queries = [
    {
      intent: "local_hq_expansion",
      query: `"${city}"${nearbyClause} (opens OR opened OR expanding OR "new office" OR headquarters OR relocation) (company OR corporate OR firm) (hotel OR lodging OR meetings OR offsite) -crunchbase`,
    },
    {
      intent: "inbound_expansion",
      query: `("${city}" OR "${territory}") ("opening an office" OR "expands to" OR "new location" OR relocating) (hiring OR training OR offsite OR "project team" OR "customer success")`,
    },
    {
      intent: "training_retreat_meetings",
      query: `"${city}" (onboarding OR "sales kickoff" OR "customer training" OR offsite OR retreat OR "leadership meeting" OR "corporate retreat") (${lodgingTerms})`,
    },
    {
      intent: "visiting_project_teams",
      query: `"${city}" ("project team" OR mobilization OR "implementation team" OR "corporate travel") (lodging OR hotel OR "extended stay" OR meetings)`,
    },
  ];

  if (spanishMarket) {
    queries.push(
      {
        intent: "local_hq_expansion_es",
        language: "es",
        query: `"${city}" (abre OR abrió OR expansión OR "nueva oficina" OR sede OR relocalización OR "traslado de oficinas") (empresa OR corporativo OR compañía) (hotel OR hospedaje OR reuniones OR capacitación)`,
      },
      {
        intent: "training_retreat_meetings_es",
        language: "es",
        query: `"${city}" (capacitación OR "kickoff de ventas" OR entrenamiento OR retiro OR "reunión de liderazgo" OR "programa corporativo" OR incentivos) (hotel OR "salón de eventos" OR "espacios de reunión" OR hospedaje)`,
      },
      {
        intent: "visiting_project_teams_es",
        language: "es",
        query: `"${city}" ("equipo de proyecto" OR movilización OR implementación OR "viaje de negocios" OR "comisión de servicio") (hospedaje OR hotel OR reuniones OR "estancia prolongada")`,
      },
    );
  }

  queries.push({
    intent: "exclude_funding_only",
    note: "Funding announcements without operating expansion or local connection are insufficient. Broader regional expansion without a hotel-local connection is insufficient.",
  });
  return queries;
}
