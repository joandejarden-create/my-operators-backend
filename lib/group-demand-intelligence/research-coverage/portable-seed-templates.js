/**
 * Portable national demand-generator seed templates for hotel onboarding.
 * Market/geo filled from hotel profile — no city-name switches, no Bethesda/DMV seeds.
 * Templates carry geoScopes so non-US hotels do not inherit US-only association seeds.
 */

export const PORTABLE_GENERATOR_SEED_VERSION = "gdi_portable_seed_templates_v2";

/**
 * Nation / region scopes for template selection (ISO-ish + GLOBAL/EU).
 * Hotel country → which template scopes are eligible.
 */
export const GEO_SCOPE = Object.freeze({
  US: "US",
  GLOBAL: "GLOBAL",
  EU: "EU",
});

/** Map hotel country codes / names → eligible portable scopes (order = preference). */
export function resolvePortableGeoScopes(countryHint = "") {
  const c = String(countryHint || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z]/g, " ");
  if (!c || c === "US" || c.includes("UNITED STATES") || c === "USA") {
    return [GEO_SCOPE.US, GEO_SCOPE.GLOBAL];
  }
  if (
    c === "IT" ||
    c.includes("ITALY") ||
    c.includes("ITALIA") ||
    c === "DE" ||
    c.includes("GERMANY") ||
    c === "FR" ||
    c.includes("FRANCE") ||
    c === "ES" ||
    c.includes("SPAIN") ||
    c === "PT" ||
    c.includes("PORTUGAL") ||
    c === "NL" ||
    c.includes("NETHERLANDS") ||
    c === "BE" ||
    c.includes("BELGIUM") ||
    c === "AT" ||
    c.includes("AUSTRIA") ||
    c === "CH" ||
    c.includes("SWITZERLAND") ||
    c === "IE" ||
    c.includes("IRELAND") ||
    c === "EU" ||
    c.includes("EUROPE")
  ) {
    return [GEO_SCOPE.EU, GEO_SCOPE.GLOBAL];
  }
  // Non-US default: global portable only (no US-association injection)
  return [GEO_SCOPE.GLOBAL];
}

/**
 * National / portable organizations relevant to urban full-service group demand.
 * Identity is domain-stable; hotel Fit rows carry market relevance.
 * geoScopes: US = US-centric; GLOBAL = international MICE; EU = Europe-relevant international.
 */
export const PORTABLE_NATIONAL_GENERATOR_TEMPLATES = Object.freeze([
  {
    organizationName: "Professional Convention Management Association",
    organizationAliases: ["PCMA"],
    organizationType: "ASSOCIATION",
    officialDomain: "pcma.org",
    website: "https://www.pcma.org/",
    headquartersLocation: "United States",
    primaryMarkets: ["United States", "major urban markets", "global association meetings"],
    industry: "Meetings / association management",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.pcma.org/"],
    geoScopes: [GEO_SCOPE.US, GEO_SCOPE.GLOBAL],
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
    geoScopes: [GEO_SCOPE.US],
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
    primaryMarkets: ["United States", "major urban markets", "international chapters"],
    industry: "Meetings industry",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.mpi.org/"],
    geoScopes: [GEO_SCOPE.US, GEO_SCOPE.GLOBAL, GEO_SCOPE.EU],
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
    primaryMarkets: ["United States", "major urban markets", "incentive destinations"],
    industry: "Incentive travel",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.siteglobal.com/"],
    geoScopes: [GEO_SCOPE.US, GEO_SCOPE.GLOBAL, GEO_SCOPE.EU],
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
    geoScopes: [GEO_SCOPE.US],
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
    geoScopes: [GEO_SCOPE.US, GEO_SCOPE.GLOBAL],
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
    geoScopes: [GEO_SCOPE.US],
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
    geoScopes: [GEO_SCOPE.US],
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
  {
    organizationName: "International Congress and Convention Association",
    organizationAliases: ["ICCA"],
    organizationType: "ASSOCIATION",
    officialDomain: "iccaworld.org",
    website: "https://www.iccaworld.org/",
    headquartersLocation: "Netherlands",
    primaryMarkets: ["Europe", "global association congress destinations"],
    industry: "International association congresses",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.iccaworld.org/"],
    geoScopes: [GEO_SCOPE.GLOBAL, GEO_SCOPE.EU],
    programs: [
      {
        programName: "ICCA Congress",
        programType: "ANNUAL_CONFERENCE",
        recurrenceStatus: "RECURRING_CONFIRMED",
        recurrenceFrequency: "annual",
        sourceUrls: ["https://www.iccaworld.org/"],
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
    organizationName: "UFI — The Global Association of the Exhibition Industry",
    organizationAliases: ["UFI"],
    organizationType: "ASSOCIATION",
    officialDomain: "ufi.org",
    website: "https://www.ufi.org/",
    headquartersLocation: "France",
    primaryMarkets: ["Europe", "global exhibition destinations"],
    industry: "Exhibitions / trade shows",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.ufi.org/"],
    geoScopes: [GEO_SCOPE.GLOBAL, GEO_SCOPE.EU],
    programs: [
      {
        programName: "UFI Global Congress",
        programType: "ANNUAL_CONFERENCE",
        recurrenceStatus: "RECURRING_CONFIRMED",
        recurrenceFrequency: "annual",
        sourceUrls: ["https://www.ufi.org/"],
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
    organizationName: "International Association of Professional Congress Organisers",
    organizationAliases: ["IAPCO"],
    organizationType: "ASSOCIATION",
    officialDomain: "iapco.org",
    website: "https://www.iapco.org/",
    headquartersLocation: "Switzerland",
    primaryMarkets: ["Europe", "global congress destinations"],
    industry: "Professional congress organization",
    generatorStatus: "ACTIVE_GENERATOR",
    sourceAuthority: "INDUSTRY",
    sourceUrls: ["https://www.iapco.org/"],
    geoScopes: [GEO_SCOPE.GLOBAL, GEO_SCOPE.EU],
    programs: [
      {
        programName: "IAPCO Annual Meeting",
        programType: "ANNUAL_CONFERENCE",
        recurrenceStatus: "RECURRING_CONFIRMED",
        recurrenceFrequency: "annual",
        sourceUrls: ["https://www.iapco.org/"],
      },
    ],
    fitFactors: {
      generatorRelevance: "MEDIUM",
      recurrence: "RECURRING_CONFIRMED",
      marketRelevance: "COMPETITIVE",
      likelyTravelCreation: "MEDIUM",
      programObservability: "MEDIUM",
      buyerAccessibility: "MEDIUM",
      evidenceQuality: "HIGH",
    },
    archetypes: ["urban_full_service", "full_service_urban", "lifestyle_urban"],
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

/** NYC / Times Square reference bleed — reject when hotel is not that market. */
export const NYC_REFERENCE_BLEED_TOKENS = Object.freeze([
  "times square",
  "midtown manhattan",
  "javits",
  "broadway theater district",
]);

export function textHasPilotGeoBleed(text) {
  const blob = String(text || "").toLowerCase();
  return PILOT_GEO_BLEED_TOKENS.some((t) => blob.includes(t));
}

export function textHasNycReferenceBleed(text) {
  const blob = String(text || "").toLowerCase();
  return NYC_REFERENCE_BLEED_TOKENS.some((t) => blob.includes(t));
}

export function resolveHotelArchetypeKey(capabilityProfile = {}, config = {}) {
  const service =
    String(capabilityProfile.serviceLevel || config.capabilityProfile?.serviceLevel || "")
      .trim()
      .toLowerCase()
      .replace(/[\s-]+/g, "_") || "urban_full_service";
  if (service.includes("resort")) return "resort";
  if (service.includes("lifestyle") || service.includes("luxury") || service.includes("soft")) {
    return "lifestyle_urban";
  }
  if (service.includes("medical")) return "medical_adjacent";
  if (service.includes("full_service") || service.includes("urban")) return "full_service_urban";
  return service || "urban_full_service";
}

function hotelCountryHint({ capabilityProfile, config, country } = {}) {
  return (
    country ||
    config?.profileCountry ||
    config?.country ||
    config?.demandTerritory?.country ||
    capabilityProfile?.country ||
    ""
  );
}

/**
 * Select portable templates for hotel archetype + country geo-scope.
 * No hotel-name / city-name switches.
 */
export function selectPortableTemplatesForHotel({
  capabilityProfile,
  config,
  country,
} = {}) {
  const archetype = resolveHotelArchetypeKey(capabilityProfile, config);
  const scopes = resolvePortableGeoScopes(hotelCountryHint({ capabilityProfile, config, country }));
  const scopeSet = new Set(scopes);
  const selected = PORTABLE_NATIONAL_GENERATOR_TEMPLATES.filter((t) => {
    const list = t.archetypes || [];
    const archOk =
      list.includes(archetype) ||
      list.includes("urban_full_service") ||
      list.includes("full_service_urban") ||
      list.includes("lifestyle_urban");
    if (!archOk) return false;
    const tplScopes = t.geoScopes || [GEO_SCOPE.US, GEO_SCOPE.GLOBAL];
    return tplScopes.some((s) => scopeSet.has(s));
  });
  return { archetype, geoScopes: scopes, templates: selected };
}
