/**
 * Geographic relevance gate — quarantine wrong-market results BEFORE hotel-fit
 * scoring or draft card generation.
 *
 * Shared-market reuse requires an explicit supported fit assessment; automatic
 * cross-market promotion is forbidden.
 */

export const GEOGRAPHIC_RELEVANCE_GATE_VERSION = "gdi-geographic-relevance-gate-v1";

export const GEO_DISPOSITION = Object.freeze({
  IN_TERRITORY: "in_territory",
  SHARED_MARKET_NEEDS_FIT: "shared_market_needs_explicit_fit",
  WRONG_MARKET_QUARANTINED: "wrong_market_quarantined",
  UNRESOLVED_GEO: "unresolved_geo",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function blobOf(candidate = {}) {
  return [
    candidate.location,
    candidate.market,
    candidate.city,
    candidate.venue,
    candidate.title,
    candidate.eventName,
    candidate.organizationName,
    candidate.localConnection,
    candidate.localPresence,
    candidate.marketConnection,
    candidate.hotelOpportunityThesis,
    candidate.hotelFitRationale,
    candidate.summaryWhat,
    candidate.whyRelevantToHotel,
    candidate.datedGrowthEvidence,
    ...(Array.isArray(candidate.evidenceLinks) ? candidate.evidenceLinks : []),
    ...(Array.isArray(candidate.sourceUrls) ? candidate.sourceUrls : []),
    candidate.officialSource,
    candidate.sourceUrl,
    candidate.rawParallel ? JSON.stringify(candidate.rawParallel) : "",
  ]
    .map(text)
    .filter(Boolean)
    .join(" | ")
    .toLowerCase();
}

/** Markets foreign to San Diego / Southern California coastal hotels. */
const FOREIGN_TO_SAN_DIEGO = [
  /\bbethesda\b/,
  /\bmaryland\b/,
  /\bmd\b(?![a-z])/,
  /\bdmv\b/,
  /\bnih\b/,
  /\bpooks\s*hill\b/,
  /\brockville\b/,
  /\bchevy\s*chase\b/,
  /\barlington,?\s*va\b/,
  /\bwashington,?\s*dc\b/,
  /\bdistrict of columbia\b/,
  /\bmayflower hotel\b/,
  /\bmarriott\.com\/en-us\/hotels\/wasbt\b/,
  /\bnew york\b/,
  /\btimes square\b/,
  /\bmanhattan\b/,
  /\bvancouver\b/,
  /\bwalter\s*reed\b/,
  /\bmedxellence\b/,
];

/** Markets foreign to downtown DC / Mayflower catchment. */
const FOREIGN_TO_DC = [
  /\bsan diego\b/,
  /\bsandiego\b/,
  /\bharbor drive\b/,
  /\bgaslamp\b/,
  /\bla jolla\b/,
  /\bcoronado\b/,
  /\bmission bay\b/,
  /\bwyndham san diego\b/,
  /\bnew york\b/,
  /\btimes square\b/,
  /\bmanhattan\b/,
  /\bvancouver\b/,
  /\bbethesda marriott\b/,
  /\bpooks\s*hill\b/,
];

const SAN_DIEGO_PATTERNS = [
  /\bsan diego\b/,
  /\bsandiego\b/,
  /\bla jolla\b/,
  /\bcoronado\b/,
  /\bchula vista\b/,
  /\bcarlsbad\b/,
  /\bencinitas\b/,
  /\bocean beach\b/,
  /\bdowntown san diego\b/,
  /\bharbor drive\b/,
  /\bmission bay\b/,
  /\bgaslamp\b/,
  /\blittle italy\b/,
  /\bconvention center\b/,
];

const DC_PATTERNS = [
  /\bwashington,?\s*d\.?c\.?\b/,
  /\bdistrict of columbia\b/,
  /\bdowntown (washington|dc)\b/,
  /\bconnecticut avenue\b/,
  /\bdupont\b/,
  /\bfoggy bottom\b/,
  /\bnational mall\b/,
  /\bcapitol hill\b/,
  /\bpennsylvania avenue\b/,
  /\bg st(?:reet)?\s*nw\b/,
];

function foreignPatternsForHotel(hotelConfig = {}, hotelId = "") {
  const id = text(hotelId || hotelConfig.hotelId);
  const label = text(hotelConfig.demandTerritory?.label || hotelConfig.city).toLowerCase();
  if (id === "recnPB3Q02NO8Ma1o" || /san diego/.test(label)) {
    return FOREIGN_TO_SAN_DIEGO;
  }
  if (id === "rec0eTEnCqlsGJJt9" || /washington|dc|connecticut/.test(label)) {
    return FOREIGN_TO_DC;
  }
  // Generic: treat neither SD nor DC hardcodes as default foreign
  return [
    /\bbethesda marriott\b/,
    /\bpooks\s*hill\b/,
    /\bnew york\b/,
    /\btimes square\b/,
    /\bvancouver\b/,
  ];
}

function localPatternsForHotel(hotelConfig = {}, hotelId = "") {
  const id = text(hotelId || hotelConfig.hotelId);
  const label = text(hotelConfig.demandTerritory?.label || hotelConfig.city).toLowerCase();
  if (id === "recnPB3Q02NO8Ma1o" || /san diego/.test(label)) {
    return SAN_DIEGO_PATTERNS;
  }
  if (id === "rec0eTEnCqlsGJJt9" || /washington|dc|connecticut/.test(label)) {
    return DC_PATTERNS;
  }
  return [];
}

function territoryTokens(hotelConfig = {}) {
  const out = [];
  const label = text(hotelConfig.demandTerritory?.label);
  const city = text(hotelConfig.city || hotelConfig.capabilityProfile?.city);
  const includes = Array.isArray(hotelConfig.demandTerritory?.includes)
    ? hotelConfig.demandTerritory.includes
    : [];
  if (label) out.push(label.toLowerCase());
  if (city) out.push(city.toLowerCase());
  for (const line of includes) {
    const short = text(line)
      .replace(/^(PRIMARY|SECONDARY):\s*/i, "")
      .split(/[,/]/)[0]
      .trim()
      .toLowerCase();
    if (short) out.push(short);
  }
  return [...new Set(out.filter(Boolean))];
}

/**
 * Evaluate one candidate against hotel territory.
 */
export function assessGeographicRelevance(candidate, hotelConfig = {}, opts = {}) {
  const hotelId = text(opts.hotelId || hotelConfig.hotelId);
  const blob = blobOf(candidate);
  const tokens = territoryTokens(hotelConfig);
  const foreignPatterns = foreignPatternsForHotel(hotelConfig, hotelId);
  const homePatterns = localPatternsForHotel(hotelConfig, hotelId);
  const foreignHits = foreignPatterns.filter((re) => re.test(blob)).map((re) =>
    String(re)
  );
  const localHits = [
    ...homePatterns.filter((re) => re.test(blob)),
    ...tokens.filter((t) => t.length > 3 && blob.includes(t)),
  ];

  // Explicit wrong-hotel naming (e.g. model invents Bethesda Marriott as target)
  const inventedWrongTarget =
    /\bbethesda marriott\b/.test(blob) &&
    hotelId === "recnPB3Q02NO8Ma1o";

  if (inventedWrongTarget || (foreignHits.length && !localHits.length)) {
    return {
      disposition: GEO_DISPOSITION.WRONG_MARKET_QUARANTINED,
      foreignHits,
      localHits,
      quarantine: true,
      reason: inventedWrongTarget
        ? "model_or_context_substituted_wrong_hotel_market"
        : "location_evidence_outside_hotel_territory",
      sharedMarketReuseAllowed: false,
    };
  }

  if (foreignHits.length && localHits.length) {
    // Local office in hotel territory + HQ/activity elsewhere is NOT automatic exclusion.
    // Accept when localConnection/local address tokens clearly match hotel territory.
    const hasExplicitLocalOffice =
      /\b(office|headquarters|hq|suite|floor|nw|broadway|pennsylvania|connecticut|harbor)\b/i.test(
        blob
      ) && localHits.length > 0;
    if (hasExplicitLocalOffice || opts.acceptLocalOfficeDespiteForeignHq === true) {
      return {
        disposition: GEO_DISPOSITION.IN_TERRITORY,
        foreignHits,
        localHits,
        quarantine: false,
        reason:
          "local_office_or_activity_in_hotel_territory_despite_other_geo_mentions",
        sharedMarketReuseAllowed: false,
      };
    }
    return {
      disposition: GEO_DISPOSITION.SHARED_MARKET_NEEDS_FIT,
      foreignHits,
      localHits,
      quarantine: true,
      reason: "mixed_geo_signals_require_explicit_supported_fit",
      sharedMarketReuseAllowed: false,
    };
  }

  if (localHits.length) {
    return {
      disposition: GEO_DISPOSITION.IN_TERRITORY,
      foreignHits,
      localHits,
      quarantine: false,
      reason: "location_matches_hotel_territory_tokens",
      sharedMarketReuseAllowed: false,
    };
  }

  return {
    disposition: GEO_DISPOSITION.UNRESOLVED_GEO,
    foreignHits,
    localHits,
    quarantine: true,
    reason: "insufficient_location_evidence_to_confirm_hotel_territory",
    sharedMarketReuseAllowed: false,
  };
}

/**
 * Partition candidates; quarantined rows never enter fit/card generation pools.
 */
export function quarantineWrongMarketCandidates(
  candidates = [],
  hotelConfig = {},
  opts = {}
) {
  const accepted = [];
  const quarantined = [];
  for (const c of candidates || []) {
    const geo = assessGeographicRelevance(c, hotelConfig, opts);
    const row = {
      ...c,
      geographicRelevance: geo,
      publicationBlocked: true,
    };
    if (geo.quarantine) {
      quarantined.push({
        ...row,
        quarantineStatus: "WRONG_MARKET_OR_UNRESOLVED_GEO",
        gdiClassifierResult: "QUARANTINED_GEO",
        publicationBlockReason: geo.reason,
        note: "Quarantined before hotel-fit scoring / card generation — not a San Diego prospect",
      });
    } else {
      accepted.push(row);
    }
  }
  return {
    version: GEOGRAPHIC_RELEVANCE_GATE_VERSION,
    hotelId: opts.hotelId || hotelConfig.hotelId || null,
    acceptedCount: accepted.length,
    quarantinedCount: quarantined.length,
    accepted,
    quarantined,
  };
}
