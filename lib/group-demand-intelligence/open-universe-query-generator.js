/**
 * Open-universe discovery query families from hotel profile — no org hardcodes.
 * Output is candidate research themes / query classes for Research Target promotion.
 */

import { DISCOVERY_MODE } from "./discovery-modes.js";

export const OPEN_UNIVERSE_QUERY_GENERATOR_VERSION = "gdi-open-universe-query-v1";

/** Generic sector themes — selected from hotel archetype / market signals, not entity names. */
export const SECTOR_THEME_CATALOG = Object.freeze({
  MEDICAL_SCIENTIFIC: {
    id: "medical_scientific",
    labels: ["medical", "scientific", "biomedical", "clinical research"],
  },
  INSTITUTIONAL_CAMPUS_ADJACENT: {
    id: "institutional_campus_adjacent",
    labels: ["research campus", "federal research", "scientific institute"],
  },
  HEALTHCARE_ASSOCIATION: {
    id: "healthcare_association",
    labels: ["healthcare association", "medical association", "physician association"],
  },
  GOVERNMENT_CONTRACTOR: {
    id: "government_contractor",
    labels: ["government contractor", "federal contractor", "defense contractor"],
  },
  FEDERAL_TECHNOLOGY: {
    id: "federal_technology",
    labels: ["federal technology", "quantum", "cyber", "emerging tech policy"],
  },
  ASSOCIATION_ADVOCACY: {
    id: "association_advocacy",
    labels: ["advocacy summit", "fly-in", "hill day", "leadership conference"],
  },
  UNIVERSITY: {
    id: "university",
    labels: ["university", "alumni", "academic conference"],
  },
  SPORTS_TOURNAMENT: {
    id: "sports_tournament",
    labels: ["youth tournament", "sports tournament", "stay to play"],
  },
  CORPORATE_HEALTHCARE: {
    id: "corporate_healthcare",
    labels: ["healthcare company", "medical device", "life sciences symposium"],
  },
  /** Luxury / island / all-inclusive resort destination themes (generic, not hotel-hardcoded). */
  INCENTIVE_TRAVEL: {
    id: "incentive_travel",
    labels: ["incentive travel", "sales incentive", "corporate reward travel"],
  },
  EXECUTIVE_RETREAT: {
    id: "executive_retreat",
    labels: ["executive retreat", "leadership offsite", "board retreat"],
  },
  DESTINATION_WEDDING: {
    id: "destination_wedding",
    labels: ["destination wedding", "wedding planner group", "honeymoon group"],
  },
  TRAVEL_ADVISOR_FAM: {
    id: "travel_advisor_fam",
    labels: ["travel advisor FAM", "luxury advisor consortium", "host agency retreat"],
  },
  REGIONAL_ASSOCIATION: {
    id: "regional_association",
    labels: ["caribbean association", "regional association meeting", "regional conference"],
  },
  MARINE_YACHTING: {
    id: "marine_yachting",
    labels: ["yacht charter group", "marine industry meeting", "regatta crew lodging"],
  },
  PRODUCTION_MEDIA: {
    id: "production_media",
    labels: ["photo shoot lodging", "film production crew", "brand campaign travel"],
  },
  SPECIAL_INTEREST_GROUP: {
    id: "special_interest_group",
    labels: ["culinary group travel", "wellness retreat group", "affinity group travel"],
  },
});

/**
 * Infer themes from a hotel profile without encoding property-specific entities.
 * @param {{ market?: string, metro?: string, archetype?: string, classification?: string, serviceLevel?: string, demandSectors?: string[], meetingCapability?: string, institutionalAnchors?: string[] }} hotelProfile
 */
export function inferOpenUniverseThemes(hotelProfile = {}) {
  const themes = new Set();
  const blob = [
    hotelProfile.archetype,
    hotelProfile.classification,
    hotelProfile.serviceLevel,
    hotelProfile.meetingCapability,
    hotelProfile.capabilityProfile?.classification,
    hotelProfile.capabilityProfile?.serviceLevel,
    ...(hotelProfile.demandSectors || []),
    ...(hotelProfile.institutionalAnchors || []),
  ]
    .join(" ")
    .toLowerCase();

  const isResortDestination =
    /resort|beach|island|caribbean|all.?inclusive|destination.?leisure|honeymoon|wedding/.test(
      blob
    ) ||
    /RESORT_DESTINATION|LUXURY_DESTINATION|caribbean|all_inclusive|beach_resort/i.test(blob);

  if (isResortDestination) {
    themes.add("INCENTIVE_TRAVEL");
    themes.add("EXECUTIVE_RETREAT");
    themes.add("DESTINATION_WEDDING");
    themes.add("TRAVEL_ADVISOR_FAM");
    themes.add("REGIONAL_ASSOCIATION");
    themes.add("SPECIAL_INTEREST_GROUP");
    if (/yacht|marina|marine|harbor|harbour|boat/.test(blob) || /caribbean|island|grenada|st lucia|antigua|bermuda/.test(blob)) {
      themes.add("MARINE_YACHTING");
    }
    if (/production|media|film|editorial/.test(blob) || isResortDestination) {
      themes.add("PRODUCTION_MEDIA");
    }
    return [...themes];
  }

  if (/medical|health|hospital|clinical|bio|pharma|nih|research/.test(blob)) {
    themes.add("MEDICAL_SCIENTIFIC");
    themes.add("HEALTHCARE_ASSOCIATION");
    themes.add("CORPORATE_HEALTHCARE");
  }
  if (/campus|institute|federal research|government|agency/.test(blob)) {
    themes.add("INSTITUTIONAL_CAMPUS_ADJACENT");
    themes.add("GOVERNMENT_CONTRACTOR");
  }
  if (/tech|quantum|cyber|contractor|defense/.test(blob)) {
    themes.add("FEDERAL_TECHNOLOGY");
    themes.add("GOVERNMENT_CONTRACTOR");
  }
  if (/assoc|advocacy|policy|lobby/.test(blob)) {
    themes.add("ASSOCIATION_ADVOCACY");
  }
  if (/university|college|alumni|academic/.test(blob)) {
    themes.add("UNIVERSITY");
  }
  if (/sport|tournament|youth|arena/.test(blob)) {
    themes.add("SPORTS_TOURNAMENT");
  }

  // Safe defaults for full-service suburban meeting hotels near metros
  if (themes.size === 0) {
    themes.add("HEALTHCARE_ASSOCIATION");
    themes.add("ASSOCIATION_ADVOCACY");
    themes.add("CORPORATE_HEALTHCARE");
    themes.add("SPORTS_TOURNAMENT");
  }
  return [...themes];
}

/**
 * Build query families for open-universe discovery.
 * @returns {{ mode, version, themes, queries: Array<{theme, sector, queryClass, queryTemplate, reason}> }}
 */
export function generateOpenUniverseQueries(hotelProfile = {}, { yearsAhead = [0, 1, 2] } = {}) {
  const market = hotelProfile.market || hotelProfile.city || "{market}";
  const metro = hotelProfile.metro || hotelProfile.region || market;
  const year = new Date().getUTCFullYear();
  const themeKeys = inferOpenUniverseThemes(hotelProfile);
  const queries = [];

  for (const key of themeKeys) {
    const cat = SECTOR_THEME_CATALOG[key];
    if (!cat) continue;
    const sector = cat.labels[0];
    for (const offset of yearsAhead) {
      const y = year + offset;
      queries.push({
        theme: key,
        sector,
        queryClass: "SECTOR_YEAR_MEETING",
        queryTemplate: `${sector} ${y} conference OR summit OR "annual meeting" ${market} OR ${metro}`,
        reason: `Open-universe ${key} future-cycle scan`,
        discoveryMode: DISCOVERY_MODE.OPEN_UNIVERSE_DISCOVERY,
      });
    }
    queries.push({
      theme: key,
      sector,
      queryClass: "FUTURE_MEETINGS_CALENDAR",
      queryTemplate: `${sector} association "future meetings" OR "upcoming meetings" ${metro}`,
      reason: "Future meetings calendar pattern",
      discoveryMode: DISCOVERY_MODE.OPEN_UNIVERSE_DISCOVERY,
    });
    queries.push({
      theme: key,
      sector,
      queryClass: "DESTINATION_TBD",
      queryTemplate: `${sector} symposium OR "annual meeting" "destination TBD" OR "location TBD" OR "hotel TBD"`,
      reason: "Destination/venue TBD research signal",
      discoveryMode: DISCOVERY_MODE.OPEN_UNIVERSE_DISCOVERY,
    });
  }

  if (themeKeys.includes("SPORTS_TOURNAMENT") || themeKeys.includes("ASSOCIATION_ADVOCACY")) {
    queries.push({
      theme: "SPORTS_TOURNAMENT",
      sector: "tournament",
      queryClass: "STAY_TO_PLAY",
      queryTemplate: `${market} OR ${metro} tournament hotel selection OR "stay to play" OR "room block" OR "host hotel"`,
      reason: "Sports stay-to-play / hotel contracting window",
      discoveryMode: DISCOVERY_MODE.OPEN_UNIVERSE_DISCOVERY,
    });
  }

  if (themeKeys.includes("INSTITUTIONAL_CAMPUS_ADJACENT") || themeKeys.includes("MEDICAL_SCIENTIFIC")) {
    queries.push({
      theme: "INSTITUTIONAL_CAMPUS_ADJACENT",
      sector: "scientific symposium",
      queryClass: "FIXED_VENUE_OPEN_HOUSING",
      queryTemplate: `${metro} scientific OR clinical "annual meeting" OR symposium accommodations OR lodging OR "where to stay"`,
      reason: "Fixed-venue campus meetings with open housing",
      discoveryMode: DISCOVERY_MODE.EVENT_SERIES_EXPANSION,
    });
  }

  return {
    mode: DISCOVERY_MODE.OPEN_UNIVERSE_DISCOVERY,
    version: OPEN_UNIVERSE_QUERY_GENERATOR_VERSION,
    themes: themeKeys,
    market,
    metro,
    queries,
    promotionFlow: "CREATE_RESEARCH_TARGET_BEFORE_OPPORTUNITY",
  };
}

/**
 * Event-series expansion queries for a known (already validated) organization name.
 * Caller supplies org from Research Target — generator does not invent orgs.
 */
export function generateEventSeriesExpansionQueries({
  organization,
  eventSeries = null,
  years = null,
} = {}) {
  const org = String(organization || "").trim();
  if (!org) return { ok: false, error: "ORGANIZATION_REQUIRED", queries: [] };
  const y0 = new Date().getUTCFullYear();
  const yrs = years && years.length ? years : [y0, y0 + 1, y0 + 2];
  const series = eventSeries || "annual meeting";
  const queries = [
    {
      queryClass: "FUTURE_MEETINGS_PAGE",
      queryTemplate: `${org} "future meetings" OR "upcoming meetings" OR "save the date"`,
    },
    {
      queryClass: "SERIES_YEAR",
      queryTemplate: `${org} ${series} ${yrs.join(" OR ")}`,
    },
    {
      queryClass: "DESTINATION_VENUE_TBD",
      queryTemplate: `${org} ${series} destination OR venue OR hotel TBD OR "coming soon"`,
    },
    {
      queryClass: "HOUSING",
      queryTemplate: `${org} ${series} accommodations OR housing OR "official hotel" OR lodging`,
    },
    {
      queryClass: "HISTORY",
      queryTemplate: `${org} ${series} past locations OR "meeting history" OR previous destinations`,
    },
  ];
  return {
    ok: true,
    mode: DISCOVERY_MODE.EVENT_SERIES_EXPANSION,
    organization: org,
    eventSeries: series,
    years: yrs,
    queries,
  };
}
