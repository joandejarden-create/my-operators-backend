/**
 * Portable national demand-generator seed templates for hotel onboarding.
 * Market/geo filled from hotel profile — no city-name switches, no Bethesda/DMV seeds.
 */

export const PORTABLE_GENERATOR_SEED_VERSION = "gdi_portable_seed_templates_v1";

/**
 * National / US-portable organizations relevant to urban full-service group demand.
 * Identity is domain-stable; hotel Fit rows carry market relevance.
 */
export const PORTABLE_NATIONAL_GENERATOR_TEMPLATES = Object.freeze([
  {
    organizationName: "Professional Convention Management Association",
    organizationAliases: ["PCMA"],
    organizationType: "ASSOCIATION",
    officialDomain: "pcma.org",
    website: "https://www.pcma.org/",
    headquartersLocation: "United States",
    primaryMarkets: ["United States", "major urban markets"],
    industry: "Meetings / association management",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.pcma.org/"],
    programs: [
      {
        programName: "PCMA Convening Leaders",
        programType: "ANNUAL_CONFERENCE",
        recurrenceStatus: "RECURRING_CONFIRMED",
        recurrenceFrequency: "annual",
        sourceUrls: ["https://www.pcma.org/"],
      },
    ],
    fitFactors: {
      generatorRelevance: "HIGH",
      recurrence: "RECURRING_CONFIRMED",
      marketRelevance: "COMPETITIVE",
      likelyTravelCreation: "HIGH",
      programObservability: "HIGH",
      buyerAccessibility: "MEDIUM",
      evidenceQuality: "HIGH",
    },
    archetypes: ["urban_full_service", "full_service_urban", "lifestyle_urban"],
  },
  {
    organizationName: "American Society of Association Executives",
    organizationAliases: ["ASAE"],
    organizationType: "ASSOCIATION",
    officialDomain: "asaecenter.org",
    website: "https://www.asaecenter.org/",
    headquartersLocation: "United States",
    primaryMarkets: ["United States", "major urban markets"],
    industry: "Association management",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.asaecenter.org/"],
    programs: [
      {
        programName: "ASAE Annual Meeting & Exposition",
        programType: "ANNUAL_CONFERENCE",
        recurrenceStatus: "RECURRING_CONFIRMED",
        recurrenceFrequency: "annual",
        sourceUrls: ["https://www.asaecenter.org/"],
      },
    ],
    fitFactors: {
      generatorRelevance: "HIGH",
      recurrence: "RECURRING_CONFIRMED",
      marketRelevance: "COMPETITIVE",
      likelyTravelCreation: "HIGH",
      programObservability: "HIGH",
      buyerAccessibility: "MEDIUM",
      evidenceQuality: "HIGH",
    },
    archetypes: ["urban_full_service", "full_service_urban", "lifestyle_urban"],
  },
  {
    organizationName: "Meeting Professionals International",
    organizationAliases: ["MPI"],
    organizationType: "ASSOCIATION",
    officialDomain: "mpi.org",
    website: "https://www.mpi.org/",
    headquartersLocation: "United States",
    primaryMarkets: ["United States", "major urban markets"],
    industry: "Meetings industry",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.mpi.org/"],
    programs: [
      {
        programName: "MPI World Education Congress",
        programType: "ANNUAL_CONFERENCE",
        recurrenceStatus: "RECURRING_CONFIRMED",
        recurrenceFrequency: "annual",
        sourceUrls: ["https://www.mpi.org/"],
      },
    ],
    fitFactors: {
      generatorRelevance: "MEDIUM",
      recurrence: "RECURRING_CONFIRMED",
      marketRelevance: "COMPETITIVE",
      likelyTravelCreation: "MEDIUM",
      programObservability: "HIGH",
      buyerAccessibility: "MEDIUM",
      evidenceQuality: "HIGH",
    },
    archetypes: ["urban_full_service", "full_service_urban", "lifestyle_urban", "resort"],
  },
  {
    organizationName: "Society for Incentive Travel Excellence",
    organizationAliases: ["SITE"],
    organizationType: "ASSOCIATION",
    officialDomain: "siteglobal.com",
    website: "https://www.siteglobal.com/",
    headquartersLocation: "United States",
    primaryMarkets: ["United States", "major urban markets"],
    industry: "Incentive travel",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.siteglobal.com/"],
    programs: [
      {
        programName: "SITE Global Conference",
        programType: "ANNUAL_CONFERENCE",
        recurrenceStatus: "RECURRING_CONFIRMED",
        recurrenceFrequency: "annual",
        sourceUrls: ["https://www.siteglobal.com/"],
      },
    ],
    fitFactors: {
      generatorRelevance: "MEDIUM",
      recurrence: "RECURRING_CONFIRMED",
      marketRelevance: "MEDIUM",
      likelyTravelCreation: "HIGH",
      programObservability: "MEDIUM",
      buyerAccessibility: "MEDIUM",
      evidenceQuality: "MEDIUM",
    },
    archetypes: ["urban_full_service", "full_service_urban", "lifestyle_urban", "resort"],
  },
  {
    organizationName: "International Association of Exhibitions and Events",
    organizationAliases: ["IAEE"],
    organizationType: "ASSOCIATION",
    officialDomain: "iaee.com",
    website: "https://www.iaee.com/",
    headquartersLocation: "United States",
    primaryMarkets: ["United States", "major urban markets"],
    industry: "Exhibitions / trade shows",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.iaee.com/"],
    programs: [
      {
        programName: "Expo! Expo! IAEE Annual Meeting",
        programType: "ANNUAL_CONFERENCE",
        recurrenceStatus: "RECURRING_CONFIRMED",
        recurrenceFrequency: "annual",
        sourceUrls: ["https://www.iaee.com/"],
      },
    ],
    fitFactors: {
      generatorRelevance: "MEDIUM",
      recurrence: "RECURRING_CONFIRMED",
      marketRelevance: "MEDIUM",
      likelyTravelCreation: "MEDIUM",
      programObservability: "HIGH",
      buyerAccessibility: "MEDIUM",
      evidenceQuality: "HIGH",
    },
    archetypes: ["urban_full_service", "full_service_urban"],
  },
  {
    organizationName: "Association of Corporate Travel Executives",
    organizationAliases: ["ACTE"],
    organizationType: "ASSOCIATION",
    officialDomain: "acte.org",
    website: "https://www.acte.org/",
    headquartersLocation: "United States",
    primaryMarkets: ["United States", "major urban markets"],
    industry: "Corporate travel",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.acte.org/"],
    programs: [
      {
        programName: "ACTE Global Conference",
        programType: "ANNUAL_CONFERENCE",
        recurrenceStatus: "RECURRING_CONFIRMED",
        recurrenceFrequency: "annual",
        sourceUrls: ["https://www.acte.org/"],
      },
    ],
    fitFactors: {
      generatorRelevance: "MEDIUM",
      recurrence: "RECURRING_CONFIRMED",
      marketRelevance: "COMPETITIVE",
      likelyTravelCreation: "MEDIUM",
      programObservability: "MEDIUM",
      buyerAccessibility: "MEDIUM",
      evidenceQuality: "MEDIUM",
    },
    archetypes: ["urban_full_service", "full_service_urban", "lifestyle_urban"],
  },
  {
    organizationName: "Healthcare Convention & Exhibitors Association",
    organizationAliases: ["HCEA"],
    organizationType: "ASSOCIATION",
    officialDomain: "hcea.org",
    website: "https://www.hcea.org/",
    headquartersLocation: "United States",
    primaryMarkets: ["United States", "major urban markets"],
    industry: "Healthcare meetings",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.hcea.org/"],
    programs: [
      {
        programName: "HCEA Annual Meeting",
        programType: "ANNUAL_CONFERENCE",
        recurrenceStatus: "RECURRING_CONFIRMED",
        recurrenceFrequency: "annual",
        sourceUrls: ["https://www.hcea.org/"],
      },
    ],
    fitFactors: {
      generatorRelevance: "MEDIUM",
      recurrence: "RECURRING_CONFIRMED",
      marketRelevance: "MEDIUM",
      likelyTravelCreation: "HIGH",
      programObservability: "MEDIUM",
      buyerAccessibility: "MEDIUM",
      evidenceQuality: "MEDIUM",
    },
    archetypes: ["urban_full_service", "full_service_urban", "medical_adjacent"],
  },
  {
    organizationName: "Religious Conference Management Association",
    organizationAliases: ["RCMA"],
    organizationType: "ASSOCIATION",
    officialDomain: "rcmaweb.org",
    website: "https://www.rcmaweb.org/",
    headquartersLocation: "United States",
    primaryMarkets: ["United States", "major urban markets"],
    industry: "Faith-based meetings",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.rcmaweb.org/"],
    programs: [
      {
        programName: "RCMA Annual Conference",
        programType: "ANNUAL_CONFERENCE",
        recurrenceStatus: "RECURRING_CONFIRMED",
        recurrenceFrequency: "annual",
        sourceUrls: ["https://www.rcmaweb.org/"],
      },
    ],
    fitFactors: {
      generatorRelevance: "MEDIUM",
      recurrence: "RECURRING_CONFIRMED",
      marketRelevance: "MEDIUM",
      likelyTravelCreation: "HIGH",
      programObservability: "MEDIUM",
      buyerAccessibility: "MEDIUM",
      evidenceQuality: "MEDIUM",
    },
    archetypes: ["urban_full_service", "full_service_urban", "resort"],
  },
]);

/** Tokens that mark Bethesda / DMV pilot bleed — reject for non-pilot hotels. */
export const PILOT_GEO_BLEED_TOKENS = Object.freeze([
  "bethesda",
  "pooks hill",
  "montgomery county",
  "dmv",
  "nih.gov",
  "walter reed",
  "woman's club",
  "womans club",
  "accp",
  "acvnu",
  "premier cup",
  "nrc",
  "gaithersburg",
  "rockville",
  "silver spring",
  "arlington va",
  "northern virginia",
]);

export function textHasPilotGeoBleed(text) {
  const blob = String(text || "").toLowerCase();
  return PILOT_GEO_BLEED_TOKENS.some((t) => blob.includes(t));
}

export function resolveHotelArchetypeKey(capabilityProfile = {}, config = {}) {
  const service =
    String(capabilityProfile.serviceLevel || config.capabilityProfile?.serviceLevel || "")
      .trim()
      .toLowerCase()
      .replace(/[\s-]+/g, "_") || "urban_full_service";
  if (service.includes("resort")) return "resort";
  if (service.includes("lifestyle") || service.includes("soft")) return "lifestyle_urban";
  if (service.includes("medical")) return "medical_adjacent";
  if (service.includes("full_service") || service.includes("urban")) return "full_service_urban";
  return service || "urban_full_service";
}

/**
 * Select portable templates for a hotel archetype (no hotel-name switches).
 */
export function selectPortableTemplatesForHotel({ capabilityProfile, config } = {}) {
  const archetype = resolveHotelArchetypeKey(capabilityProfile, config);
  const selected = PORTABLE_NATIONAL_GENERATOR_TEMPLATES.filter((t) => {
    const list = t.archetypes || [];
    return (
      list.includes(archetype) ||
      list.includes("urban_full_service") ||
      list.includes("full_service_urban")
    );
  });
  return { archetype, templates: selected };
}
