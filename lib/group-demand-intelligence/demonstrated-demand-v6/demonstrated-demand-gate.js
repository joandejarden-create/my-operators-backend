/**
 * isGdiDemonstratedDemandLead() — stronger than SIGNAL / thin RESEARCH_LEAD.
 * Requires validated competitor-use evidence (DIRECT/STRONG) + ≥2 demand signals.
 */

export const DEMONSTRATED_SUPPORT = Object.freeze({
  HISTORIC_COMPETITOR_HOTEL_USE: "HISTORIC_COMPETITOR_HOTEL_USE",
  OFFICIAL_ACCOMMODATION_HOUSING: "OFFICIAL_ACCOMMODATION_HOUSING",
  REPEAT_EVENT_SERIES: "REPEAT_EVENT_SERIES",
  NAMED_ORGANIZER_BUYER: "NAMED_ORGANIZER_BUYER",
  FUTURE_DECISION_WINDOW: "FUTURE_DECISION_WINDOW",
  PROCUREMENT_RFP_SIGNAL: "PROCUREMENT_RFP_SIGNAL",
  TRAVELING_DELEGATION: "TRAVELING_DELEGATION",
  CREW_WORKFORCE_PATTERN: "CREW_WORKFORCE_PATTERN",
  GROUP_BLOCK_OR_OFFICIAL_HOTEL_LIST: "GROUP_BLOCK_OR_OFFICIAL_HOTEL_LIST",
  COMPETITOR_REPEAT_HISTORY: "COMPETITOR_REPEAT_HISTORY",
  AGENCY_DMC_PLACEMENT: "AGENCY_DMC_PLACEMENT",
});

const LISTICLE_OR_NOISE_ORG_RE =
  /\b(best (luxury )?hotels|top hotels|hotels?:|wedding wednesday|things to do|where to stay|photo gallery|deals? & reviews|honeymooning at|over-the-top|office block hotels)\b/i;
const GENERIC_REJECT_RE =
  /\b(company expansion|headquarters|about us|stock price|earnings|construction begins|appoints ceo)\b/i;

function blob(c = {}) {
  return [
    c.title,
    c.organizationName || c.organization,
    c.eventProgram,
    c.summaryWhat,
    c.fact,
    c.snippet,
    c.officialSource || c.source,
  ]
    .map((x) => String(x || ""))
    .join(" ");
}

function isValidEntity(c = {}) {
  const org = String(c.organizationName || c.organization || "").trim();
  if (org.length < 6) return false;
  if (LISTICLE_OR_NOISE_ORG_RE.test(org)) return false;
  if (/^https?:/i.test(org)) return false;
  // Reject bare hotel names as "organization"
  if (/^(the\s+)?[\w\s&'-]+hotel\b/i.test(org) && !/\b(association|society|congress|federation|foundation|agency|university)\b/i.test(org)) {
    return false;
  }
  // Need at least 2 tokens or known org type
  const tokens = org.split(/\s+/).filter(Boolean);
  if (tokens.length < 2 && !/\b(Inc|LLC|Ltd|Association|Society|Federation)\b/i.test(org)) {
    return false;
  }
  return true;
}

function collectSupport(c = {}, b = "") {
  const s = [];
  const evidence = String(c.evidenceClass || "");
  if (
    evidence === "DIRECT_CONFIRMED" ||
    evidence === "STRONG_ASSOCIATION" ||
    c.competitorHotel ||
    c.historicCompetitorHotels
  ) {
    s.push(DEMONSTRATED_SUPPORT.HISTORIC_COMPETITOR_HOTEL_USE);
  }
  if (
    c.lodgingEvidence ||
    /\b(official hotel|host hotel|hotel block|room block|housing|accommodation|hébergement|alojamiento)\b/i.test(b)
  ) {
    s.push(DEMONSTRATED_SUPPORT.OFFICIAL_ACCOMMODATION_HOUSING);
  }
  if (
    c.repeatCadence &&
    !/ONE_OFF|UNKNOWN/i.test(c.repeatCadence) ||
    /\b(annual|series|édition|biennial|recurring|rotation)\b/i.test(b)
  ) {
    s.push(DEMONSTRATED_SUPPORT.REPEAT_EVENT_SERIES);
  }
  if (
    isValidEntity(c) &&
    /\b(association|society|federation|foundation|congress|agency|secretariat|organizer|organiser|university|committee)\b/i.test(
      b
    )
  ) {
    s.push(DEMONSTRATED_SUPPORT.NAMED_ORGANIZER_BUYER);
  } else if (isValidEntity(c) && String(c.organizationName || c.organization || "").split(/\s+/).length >= 3) {
    s.push(DEMONSTRATED_SUPPORT.NAMED_ORGANIZER_BUYER);
  }
  if (
    c.eventStartDate ||
    c.nextKnownCycle ||
    c.nextExpectedCycle ||
    c.nextDecisionWindow ||
    /\b(202[6-9]|203[0-2]|site selection|RFP|host proposal|upcoming)\b/i.test(b)
  ) {
    s.push(DEMONSTRATED_SUPPORT.FUTURE_DECISION_WINDOW);
  }
  if (/\b(rfp|tender|procurement|licitación|appel d['']offres)\b/i.test(b)) {
    s.push(DEMONSTRATED_SUPPORT.PROCUREMENT_RFP_SIGNAL);
  }
  if (/\b(delegation|pavilion|visiting (team|cohort)|exhibitor)\b/i.test(b)) {
    s.push(DEMONSTRATED_SUPPORT.TRAVELING_DELEGATION);
  }
  if (/\b(crew|workforce|contractor|commissioning|production team)\b/i.test(b)) {
    s.push(DEMONSTRATED_SUPPORT.CREW_WORKFORCE_PATTERN);
  }
  if (/\b(hotel block|official hotel|preferred hotel|housing list|hotel list)\b/i.test(b)) {
    s.push(DEMONSTRATED_SUPPORT.GROUP_BLOCK_OR_OFFICIAL_HOTEL_LIST);
  }
  const hotels = Array.isArray(c.historicHotels)
    ? c.historicHotels
    : String(c.historicHotels || c.historicCompetitorHotels || "")
        .split("|")
        .filter(Boolean);
  if (hotels.length >= 2 || /YEARS_SEEN|repeat/i.test(String(c.repeatHistory || ""))) {
    s.push(DEMONSTRATED_SUPPORT.COMPETITOR_REPEAT_HISTORY);
  }
  if (/\b(DMC|housing bureau|event agency|travel agency|médical communications)\b/i.test(b) || c.agency) {
    s.push(DEMONSTRATED_SUPPORT.AGENCY_DMC_PLACEMENT);
  }
  return [...new Set(s)];
}

/**
 * @returns {{ ok, class, reasons, supportSignals, supportCount }}
 */
export function isGdiDemonstratedDemandLead(candidate = {}, opts = {}) {
  const b = blob(candidate);
  const evidence = String(candidate.evidenceClass || "");

  // Only DIRECT / STRONG competitor-use can become demonstrated-demand
  if (
    evidence &&
    evidence !== "DIRECT_CONFIRMED" &&
    evidence !== "STRONG_ASSOCIATION" &&
    !candidate.feedsDeeperResearch
  ) {
    return {
      ok: false,
      class: "NOT_DEMONSTRATED",
      reasons: ["EVIDENCE_NOT_DIRECT_OR_STRONG"],
      supportSignals: [],
      supportCount: 0,
    };
  }
  if (!evidence && !candidate.competitorHotel && !candidate.historicCompetitorHotels) {
    return {
      ok: false,
      class: "NOT_DEMONSTRATED",
      reasons: ["NO_COMPETITOR_USE_EVIDENCE"],
      supportSignals: [],
      supportCount: 0,
    };
  }

  if (GENERIC_REJECT_RE.test(b) && !/hotel block|accommodation|housing/i.test(b)) {
    return {
      ok: false,
      class: "REJECTED",
      reasons: ["GENERIC_ACTIVITY"],
      supportSignals: [],
      supportCount: 0,
    };
  }

  if (!isValidEntity(candidate)) {
    return {
      ok: false,
      class: "REJECTED",
      reasons: ["ENTITY_INVALID_OR_LISTICLE"],
      supportSignals: [],
      supportCount: 0,
    };
  }

  const groupMotion =
    /\b(conference|congress|meeting|summit|tournament|wedding|incentive|training|delegation|crew|housing|hotel block|accommodation|kickoff|retreat)\b/i.test(
      b
    ) || Boolean(candidate.groupType || candidate.groupMotion || candidate.opportunityType);
  if (!groupMotion) {
    return {
      ok: false,
      class: "NOT_DEMONSTRATED",
      reasons: ["NO_REAL_GROUP_HOTEL_MOTION"],
      supportSignals: [],
      supportCount: 0,
    };
  }

  const geo = opts.geoTokens || [];
  const marketOk =
    !geo.length ||
    geo.some((t) => b.toLowerCase().includes(String(t).toLowerCase())) ||
    Boolean(candidate.market || candidate.lodgingMarket || candidate.eventLocationSummary);
  if (!marketOk) {
    return {
      ok: false,
      class: "NOT_DEMONSTRATED",
      reasons: ["MARKET_RELEVANCE_WEAK"],
      supportSignals: [],
      supportCount: 0,
    };
  }

  const support = collectSupport(candidate, b);
  if (support.length < 2) {
    return {
      ok: false,
      class: "NOT_DEMONSTRATED",
      reasons: ["FEWER_THAN_TWO_SUPPORTING_SIGNALS", ...support],
      supportSignals: support,
      supportCount: support.length,
    };
  }

  return {
    ok: true,
    class: "DEMONSTRATED_DEMAND_LEAD",
    reasons: ["VALID_ENTITY", "GROUP_HOTEL_MOTION", "MARKET_RELEVANCE", ...support],
    supportSignals: support,
    supportCount: support.length,
  };
}
