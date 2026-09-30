/**
 * HI Evidence Depth V2 — research depth states + authority tiers.
 * RESEARCHED_EMPTY requires a completed bounded source ladder, not one URL.
 */

export const RESEARCH_DEPTH = Object.freeze({
  NOT_RESEARCHED: "NOT_RESEARCHED",
  FIRST_PARTY_RESEARCHED: "FIRST_PARTY_RESEARCHED",
  SECONDARY_STRUCTURED_RESEARCHED: "SECONDARY_STRUCTURED_RESEARCHED",
  BROADER_PUBLIC_RESEARCHED: "BROADER_PUBLIC_RESEARCHED",
  POPULATED: "POPULATED",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
  RESEARCHED_EMPTY: "RESEARCHED_EMPTY",
  CONFLICT_UNRESOLVED: "CONFLICT_UNRESOLVED",
  FAILED_PROVIDER: "FAILED_PROVIDER",
  FAILED_PARSER: "FAILED_PARSER",
});

export const SOURCE_AUTHORITY = Object.freeze({
  TIER_A_OFFICIAL: "TIER_A_OFFICIAL",
  TIER_B_STRUCTURED_VENUE: "TIER_B_STRUCTURED_VENUE",
  TIER_C_INSTITUTIONAL: "TIER_C_INSTITUTIONAL",
  TIER_D_AGGREGATOR: "TIER_D_AGGREGATOR",
  TIER_E_WEAK_MARKETING: "TIER_E_WEAK_MARKETING",
});

export const SOURCE_FAMILY = Object.freeze({
  FIRST_PARTY: "FIRST_PARTY",
  OFFICIAL_PDF: "OFFICIAL_PDF",
  CVENT: "CVENT",
  CVB: "CVB",
  TOURISM_AUTHORITY: "TOURISM_AUTHORITY",
  OPERATOR: "OPERATOR",
  OWNER: "OWNER",
  VENUE_DIRECTORY: "VENUE_DIRECTORY",
  INSTITUTIONAL: "INSTITUTIONAL",
  TRADE_SOURCE: "TRADE_SOURCE",
  OTHER: "OTHER",
});

export const HI_JEV_ACTIONS = Object.freeze({
  FIND_OFFICIAL_FACT_SHEET: "FIND_OFFICIAL_FACT_SHEET",
  FIND_MEETING_EVENTS_PAGE: "FIND_MEETING_EVENTS_PAGE",
  FIND_VENUE_PROFILE: "FIND_VENUE_PROFILE",
  FIND_CVENT_PROFILE: "FIND_CVENT_PROFILE",
  FIND_CVB_VENUE_PROFILE: "FIND_CVB_VENUE_PROFILE",
  FIND_MEETING_FLOORPLAN: "FIND_MEETING_FLOORPLAN",
  FIND_BANQUET_KIT: "FIND_BANQUET_KIT",
  FIND_HOTEL_SALES_KIT: "FIND_HOTEL_SALES_KIT",
  FIND_DESTINATION_MEETING_GUIDE: "FIND_DESTINATION_MEETING_GUIDE",
  FIND_DEMAND_GENERATOR_SOURCE: "FIND_DEMAND_GENERATOR_SOURCE",
  FIND_DESTINATION_SEASONALITY_SOURCE: "FIND_DESTINATION_SEASONALITY_SOURCE",
  SEARCH_STRUCTURED_VENUE_PROFILE: "SEARCH_STRUCTURED_VENUE_PROFILE",
  SEARCH_SECONDARY_STRUCTURED_SOURCE: "SEARCH_SECONDARY_STRUCTURED_SOURCE",
  VERIFY_MEETING_SPACE: "VERIFY_MEETING_SPACE",
  VERIFY_SOURCE_CONFLICT: "VERIFY_SOURCE_CONFLICT",
  STOP_PUBLIC_DATA_CEILING: "STOP_PUBLIC_DATA_CEILING",
});

/** Event-space source ladder (generic — no hotel hardcoding). */
export const EVENT_SPACE_SOURCE_LADDER = Object.freeze([
  { step: 1, family: SOURCE_FAMILY.FIRST_PARTY, action: "FETCH_OFFICIAL_PROPERTY" },
  { step: 2, family: SOURCE_FAMILY.FIRST_PARTY, action: "FETCH_OFFICIAL_MEETINGS" },
  { step: 3, family: SOURCE_FAMILY.OFFICIAL_PDF, action: "FIND_OFFICIAL_FACT_SHEET" },
  { step: 4, family: SOURCE_FAMILY.OFFICIAL_PDF, action: "FIND_BANQUET_KIT" },
  { step: 5, family: SOURCE_FAMILY.CVENT, action: "SEARCH_STRUCTURED_VENUE_PROFILE" },
  { step: 6, family: SOURCE_FAMILY.CVB, action: "FIND_CVB_VENUE_PROFILE" },
  { step: 7, family: SOURCE_FAMILY.VENUE_DIRECTORY, action: "SEARCH_SECONDARY_STRUCTURED_SOURCE" },
  { step: 8, family: SOURCE_FAMILY.OPERATOR, action: "FIND_OPERATOR_PROPERTY_PAGE" },
  { step: 9, family: SOURCE_FAMILY.OTHER, action: "JEV_GAP_ROUTE" },
  { step: 10, family: SOURCE_FAMILY.OTHER, action: "STOP_PUBLIC_DATA_CEILING" },
]);

export function classifyUrlAuthority(url = "") {
  const u = String(url || "").toLowerCase();
  if (!u) return SOURCE_AUTHORITY.TIER_E_WEAK_MARKETING;
  if (/cvent\.com\/venues\//.test(u)) return SOURCE_AUTHORITY.TIER_B_STRUCTURED_VENUE;
  if (
    /marriott\.com|hilton\.com|hyatt\.com|ihg\.com|radissonhotels|choicehotels|accor|stregis|westin|ritzcarlton/.test(
      u
    )
  ) {
    return SOURCE_AUTHORITY.TIER_A_OFFICIAL;
  }
  if (/\.pdf(\?|$)/.test(u) && /(fact|meeting|banquet|floor|sales)/i.test(u)) {
    return SOURCE_AUTHORITY.TIER_A_OFFICIAL;
  }
  if (/visit|cvb|convention|turismo|tourism/.test(u)) {
    return SOURCE_AUTHORITY.TIER_C_INSTITUTIONAL;
  }
  if (/tripadvisor|booking\.com|expedia|hotels\.com/.test(u)) {
    return SOURCE_AUTHORITY.TIER_E_WEAK_MARKETING;
  }
  return SOURCE_AUTHORITY.TIER_D_AGGREGATOR;
}

export function classifySourceFamily(url = "", hint = "") {
  const rawUrl = String(url || "").toLowerCase();
  const hintL = String(hint || "").toLowerCase();
  const u = `${rawUrl} ${hintL}`;
  if (/cvent\.com/.test(u)) return SOURCE_FAMILY.CVENT;
  if (/\.pdf(\?|$)/.test(rawUrl)) return SOURCE_FAMILY.OFFICIAL_PDF;
  if (/tripadvisor|booking\.com|expedia|hotels\.com|flightbridge|aaa\.com/.test(rawUrl)) {
    return SOURCE_FAMILY.OTHER;
  }
  if (/cvb|convention.?bureau|visit[a-z-]/.test(u)) return SOURCE_FAMILY.CVB;
  if (/turismo|tourism.?board|destination/.test(u)) return SOURCE_FAMILY.TOURISM_AUTHORITY;
  // First-party only when hostname is an official brand/operator property domain
  if (
    /(?:^|[/.])(marriott\.com|hilton\.com|hyatt\.com|ihg\.com|radissonhotels(?:\.com)?|choicehotels\.com|accor\.com|stregis\.com|westin\.com|ritzcarlton\.com)/.test(
      rawUrl
    )
  ) {
    return SOURCE_FAMILY.FIRST_PARTY;
  }
  return SOURCE_FAMILY.OTHER;
}

/**
 * RESEARCHED_EMPTY is only valid after the bounded ladder reached secondary structured
 * sources (or ceiling) — first-party-only empty must continue.
 */
export function mayDeclareResearchedEmpty(sourcesAttempted = []) {
  const families = new Set(
    (sourcesAttempted || []).map((s) => s.family || classifySourceFamily(s.url, s.role))
  );
  const firstParty = families.has(SOURCE_FAMILY.FIRST_PARTY);
  const secondary =
    families.has(SOURCE_FAMILY.CVENT) ||
    families.has(SOURCE_FAMILY.CVB) ||
    families.has(SOURCE_FAMILY.VENUE_DIRECTORY) ||
    families.has(SOURCE_FAMILY.OFFICIAL_PDF) ||
    families.has(SOURCE_FAMILY.INSTITUTIONAL);
  return firstParty && secondary;
}

export function resolveResearchDepth({
  populated = false,
  sourcesAttempted = [],
  conflictUnresolved = false,
  providerFailed = false,
  parserFailed = false,
  ladderExhausted = false,
} = {}) {
  if (providerFailed) return RESEARCH_DEPTH.FAILED_PROVIDER;
  if (parserFailed && !populated) return RESEARCH_DEPTH.FAILED_PARSER;
  if (populated && conflictUnresolved) return RESEARCH_DEPTH.CONFLICT_UNRESOLVED;
  if (populated) return RESEARCH_DEPTH.POPULATED;

  const families = (sourcesAttempted || []).map(
    (s) => s.family || classifySourceFamily(s.url, s.role)
  );
  const hasFirst = families.includes(SOURCE_FAMILY.FIRST_PARTY);
  const hasSecondary = families.some((f) =>
    [
      SOURCE_FAMILY.CVENT,
      SOURCE_FAMILY.CVB,
      SOURCE_FAMILY.VENUE_DIRECTORY,
      SOURCE_FAMILY.OFFICIAL_PDF,
      SOURCE_FAMILY.INSTITUTIONAL,
    ].includes(f)
  );
  const hasBroader = families.some((f) =>
    [SOURCE_FAMILY.TOURISM_AUTHORITY, SOURCE_FAMILY.OPERATOR, SOURCE_FAMILY.TRADE_SOURCE].includes(f)
  );

  if (ladderExhausted && mayDeclareResearchedEmpty(sourcesAttempted)) {
    return RESEARCH_DEPTH.PUBLIC_DATA_CEILING;
  }
  if (hasBroader || (hasFirst && hasSecondary && ladderExhausted)) {
    return RESEARCH_DEPTH.BROADER_PUBLIC_RESEARCHED;
  }
  if (hasSecondary) return RESEARCH_DEPTH.SECONDARY_STRUCTURED_RESEARCHED;
  if (hasFirst) return RESEARCH_DEPTH.FIRST_PARTY_RESEARCHED;
  return RESEARCH_DEPTH.NOT_RESEARCHED;
}

/**
 * Map evidence-depth → durable HI domain status for completeness gate.
 */
export function researchDepthToDomainStatus(depth) {
  switch (depth) {
    case RESEARCH_DEPTH.POPULATED:
    case RESEARCH_DEPTH.CONFLICT_UNRESOLVED:
      return "POPULATED";
    case RESEARCH_DEPTH.PUBLIC_DATA_CEILING:
      return "PUBLIC_DATA_CEILING";
    case RESEARCH_DEPTH.BROADER_PUBLIC_RESEARCHED:
    case RESEARCH_DEPTH.SECONDARY_STRUCTURED_RESEARCHED:
      // Ladder completed without facts → treat as researched empty only if mayDeclare
      return "RESEARCHED_EMPTY";
    case RESEARCH_DEPTH.FAILED_PROVIDER:
    case RESEARCH_DEPTH.FAILED_PARSER:
      return "ERROR";
    case RESEARCH_DEPTH.FIRST_PARTY_RESEARCHED:
      // First-party only without secondary = still incomplete research for empty domains
      return "NOT_RESEARCHED";
    default:
      return "NOT_RESEARCHED";
  }
}
