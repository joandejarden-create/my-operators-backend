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
 * Hotel/market fit for the scaling-companies segment.
 * Not hardcoded to NYC or San Francisco.
 */
export function assessScalingCompaniesApplicability(hotelConfig = {}) {
  const blob = lower(
    [
      hotelConfig.displayName,
      hotelConfig.city,
      hotelConfig.demandTerritory?.label,
      ...(hotelConfig.demandTerritory?.includes || []),
      hotelConfig.capabilityProfile?.classification,
      hotelConfig.capabilityProfile?.serviceLevel,
      ...(hotelConfig.commercialPriorities?.targetSegments || []),
      ...(hotelConfig.commercialPriorities?.demandAnchorFocus || []),
    ].join(" ")
  );
  const rooms = Number(hotelConfig.capabilityProfile?.totalGuestrooms || 0) || 0;
  const meetings =
    Number(
      hotelConfig.capabilityProfile?.meetingRoomCount ??
        hotelConfig.capabilityProfile?.meetingRoomsIndoor ??
        0
    ) || 0;
  const urbanBusiness =
    meetings >= 5 ||
    rooms >= 150 ||
    /downtown|urban|business|meetings|convention|bayfront|harbor|capitol|connecticut/.test(
      blob
    );
  if (!urbanBusiness) {
    return {
      applicable: false,
      rationale:
        "Limited meetings/business catchment — scaling corporate accounts not prioritized for this inventory profile.",
    };
  }
  return {
    applicable: true,
    rationale:
      "Urban/business meetings hotel with practical commercial catchment — locally headquartered and inbound-expanding companies are in scope when growth-to-demand evidence exists.",
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

  return {
    ok: true,
    qualification,
    opportunity: {
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
      localPresence: qualification.localConnection,
      localConnection: qualification.localConnection,
      growthSignals,
      growthSignalLabels: growthSignals.map(
        (s) => SCALING_GROWTH_SIGNAL_LABEL[s] || s
      ),
      growthEvidenceDate: qualification.evidenceDate,
      datedGrowthEvidence: qualification.datedGrowthEvidence,
      proposedHotelUse: qualification.hotelUse,
      potentialHotelUseCases: qualification.hotelUse,
      whyThisHotel: qualification.hotelFit,
      hotelOpportunityThesis: qualification.hotelFit,
      fitExplanation: qualification.hotelFit,
      summaryWhyHotel: qualification.hotelFit,
      summaryWhat: `${company}: ${qualification.localConnection}. Growth: ${growthLabel || "see evidence"}.`,
      summaryWhyMatters: qualification.hotelUse,
      whyNow,
      whyEstablishContactNow: whyNow,
      constraints,
      uncertainties: constraints,
      establishedFacts: qualification.establishedFacts,
      inferredHotelDemand: qualification.inferredHotelDemand,
      contactRole,
      contactResponsibility: contactRole.replace(/_/g, " "),
      suggestedContactRoles: qualification.suggestedContactRoles,
      contactName,
      contactEmail,
      contactPhone,
      contactUrl,
      commercialContactPath: contactEmail
        ? "NAMED_DIRECT"
        : contactUrl
          ? "ORGANIZATION_PATH"
          : "NO_CONTACT",
      recommendedAction,
      recommendedNextStep: recommendedAction,
      // Priority / action / sourcing kept separate — introductory contact may be warranted
      priority: candidate.priority || "MEDIUM_PRIORITY",
      venueSourcingStatus: "OPEN_UNRESOLVED",
      roomDemandStatus: "UNKNOWN",
      doNotEstimateAnnualRevenue: true,
      customerFacingState: candidate.publish === false ? "INTERNAL_ONLY" : "ACTIONABLE_NOW",
      customerVisible: candidate.publish !== false,
      actionable: candidate.publish !== false,
      prospectPublicationBasis:
        candidate.publish === false ? null : "REVIEWED_PACKAGE_V1",
      prospectHotelFacing: candidate.publish !== false,
      prospectReviewSection: "EXPLORATORY",
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
    },
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
 * Build research queries for unpaid/native discovery (plan only — no paid execution).
 */
export function buildScalingCompanyResearchQueries(hotelConfig = {}) {
  const city =
    text(hotelConfig.city) ||
    text(hotelConfig.demandTerritory?.label).split("/")[0] ||
    "market";
  const territory = text(hotelConfig.demandTerritory?.label) || city;
  return [
    {
      intent: "local_hq_expansion",
      query: `"${city}" (opens OR opened OR expanding OR "new office" OR headquarters) (company OR startup OR "tech firm") hotel OR lodging OR meetings -crunchbase`,
    },
    {
      intent: "inbound_expansion",
      query: `"${territory}" ("opening an office" OR "expands to" OR "new location") (hiring OR training OR offsite OR "customer success")`,
    },
    {
      intent: "training_onsite",
      query: `"${city}" (onboarding OR "sales kickoff" OR "customer training" OR offsite) (hotel OR "meeting space" OR rooms)`,
    },
    {
      intent: "exclude_funding_only",
      note: "Funding announcements without operating expansion or local connection are insufficient.",
    },
  ];
}
