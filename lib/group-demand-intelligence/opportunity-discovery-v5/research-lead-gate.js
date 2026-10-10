/**
 * isGdiResearchLeadWorthPursuing() — admission to targeted research (not readiness).
 * Stricter than V4: requires ≥2 supporting hotel-demand signals.
 */

export const LEAD_CLASS = Object.freeze({
  SIGNAL_ONLY: "SIGNAL_ONLY",
  RESEARCH_LEAD: "RESEARCH_LEAD",
  REJECTED: "REJECTED",
  DUPLICATE: "DUPLICATE",
});

export const SUPPORT_SIGNAL = Object.freeze({
  LODGING_HINT: "LODGING_HINT",
  BUYER_ORGANIZER_HINT: "BUYER_ORGANIZER_HINT",
  REPEAT_ROTATION_SIGNAL: "REPEAT_ROTATION_SIGNAL",
  PROCUREMENT_RFP_SIGNAL: "PROCUREMENT_RFP_SIGNAL",
  HOUSING_ACCOMMODATION_SIGNAL: "HOUSING_ACCOMMODATION_SIGNAL",
  KNOWN_GROUP_TRAVEL_PATTERN: "KNOWN_GROUP_TRAVEL_PATTERN",
  COMPETITOR_HOTEL_USE: "COMPETITOR_HOTEL_USE",
  NAMED_DELEGATION_CREW_TEAM: "NAMED_DELEGATION_CREW_TEAM",
  FUTURE_DECISION_WINDOW: "FUTURE_DECISION_WINDOW",
  EVENT_SERIES_OVERNIGHT_DEMAND: "EVENT_SERIES_OVERNIGHT_DEMAND",
});

const NOISE_RE =
  /\b(booking\.com|expedia|tripadvisor|trivago|indeed\.|jobs?\.|airbnb|vrbo|tempslibre|vol pas cher|jettours)\b/i;
const LODGING_RE =
  /\b(room block|host hotel|housing|accommodation|hébergement|alojamiento|hotel block|preferred hotel|overflow|stay[- ]to[- ]play|official hotel|group lodging|visitor lodging|staff lodging|crew (hotel|accommodation)|workforce lodging)\b/i;
const HOUSING_URL_RE =
  /\/(accommodation|housing|hotels?|hébergement|alojamiento|hotel-block|room-block)\b/i;
const PROC_RE =
  /\b(rfp|tender|licitación|appel d['']offres|procurement|marché public|hotel tender|accommodation tender|group housing)\b/i;
const FUTURE_RE = /\b(202[6-9]|203[0-2]|upcoming|next (year|cycle)|édition|site selection|host proposal|bidding)\b/i;
const GROUP_RE =
  /\b(congress|congrès|conference|summit|meeting|kickoff|retreat|delegation|tournament|symposium|convention|expo|incentive|training|advisory board|investigator|crew|workforce)\b/i;
const SERIES_RE = /\b(annual|series|édition|rotat|biennial|recurring|future host|site selection)\b/i;
const BUYER_RE =
  /\b(organizer|organiser|secretariat|housing bureau|DMC|meeting planner|procurement|federation|association|agency|travel office|program office)\b/i;
const DELEGATION_RE =
  /\b(delegation|pavilion|crew|production team|contractor|commissioning|visiting cohort|youth team|broadcast)\b/i;
const COMPETITOR_RE =
  /\b(marriott|hilton|hyatt|sheraton|ibis|novotel|fairmont|host hotel|stayed at|accommodated at)\b/i;
const REJECT_GENERIC_RE =
  /\b(things to do|sightseeing|appoints ceo|stock price|earnings|headquarters only|about us)\b/i;

function blob(c = {}) {
  return [
    c.title,
    c.organizationName || c.organization,
    c.summaryWhat,
    c.snippet,
    c.officialSource || c.source || c.url,
    c.hotelOpportunityThesis,
    c.queryLocalized,
  ]
    .map((x) => String(x || ""))
    .join(" ");
}

/** Soft signals alone cannot admit — need ≥1 hotel-demand motion signal. */
const MOTION_SUPPORT = new Set([
  SUPPORT_SIGNAL.LODGING_HINT,
  SUPPORT_SIGNAL.HOUSING_ACCOMMODATION_SIGNAL,
  SUPPORT_SIGNAL.PROCUREMENT_RFP_SIGNAL,
  SUPPORT_SIGNAL.COMPETITOR_HOTEL_USE,
  SUPPORT_SIGNAL.NAMED_DELEGATION_CREW_TEAM,
  SUPPORT_SIGNAL.BUYER_ORGANIZER_HINT,
  SUPPORT_SIGNAL.REPEAT_ROTATION_SIGNAL,
  SUPPORT_SIGNAL.EVENT_SERIES_OVERNIGHT_DEMAND,
]);

function collectSupport(c = {}, b = "") {
  const s = [];
  const url = String(c.officialSource || c.source || c.url || "");
  if (LODGING_RE.test(b) || c.lodgingEvidence) s.push(SUPPORT_SIGNAL.LODGING_HINT);
  if (HOUSING_URL_RE.test(url) || /\bhousing bureau|official accommodation|hébergement officiel\b/i.test(b)) {
    s.push(SUPPORT_SIGNAL.HOUSING_ACCOMMODATION_SIGNAL);
  }
  // Buyer hint requires buyer-function language — not a SERP title fragment alone
  if (BUYER_RE.test(b) || /\b(association|society|federation|secretariat|housing bureau|DMC)\b/i.test(b)) {
    s.push(SUPPORT_SIGNAL.BUYER_ORGANIZER_HINT);
  }
  if (SERIES_RE.test(b) || c.rotates || c.signalType === "ROTATION_SERIES") {
    s.push(SUPPORT_SIGNAL.REPEAT_ROTATION_SIGNAL);
  }
  if (PROC_RE.test(b) || c.engine === "PROCUREMENT" || c.demandEngine === "PROCUREMENT_RFP") {
    s.push(SUPPORT_SIGNAL.PROCUREMENT_RFP_SIGNAL);
  }
  // Group travel pattern only with lodging/housing/procurement language — not bare "conference"
  if (GROUP_RE.test(b) && (LODGING_RE.test(b) || PROC_RE.test(b) || HOUSING_URL_RE.test(url) || DELEGATION_RE.test(b))) {
    s.push(SUPPORT_SIGNAL.KNOWN_GROUP_TRAVEL_PATTERN);
  }
  if (COMPETITOR_RE.test(b) || c.competitorHotel) s.push(SUPPORT_SIGNAL.COMPETITOR_HOTEL_USE);
  if (DELEGATION_RE.test(b)) s.push(SUPPORT_SIGNAL.NAMED_DELEGATION_CREW_TEAM);
  // Future window: year alone is insufficient — need decision/site/RFP/series language
  if (
    c.eventStartDate ||
    /site selection|RFP|tender|decision|host city|host proposal|bidding|upcoming|next (year|cycle)/i.test(b) ||
    (FUTURE_RE.test(b) && (LODGING_RE.test(b) || PROC_RE.test(b) || SERIES_RE.test(b)))
  ) {
    s.push(SUPPORT_SIGNAL.FUTURE_DECISION_WINDOW);
  }
  if (SERIES_RE.test(b) && (LODGING_RE.test(b) || PROC_RE.test(b))) {
    s.push(SUPPORT_SIGNAL.EVENT_SERIES_OVERNIGHT_DEMAND);
  }
  return [...new Set(s)];
}

/**
 * @returns {{ ok, class, reasons, supportSignals, supportCount }}
 */
export function isGdiResearchLeadWorthPursuing(candidate = {}, opts = {}) {
  const b = blob(candidate);
  const geoTokens = opts.geoTokens || [];

  if (opts.isDuplicate || candidate.duplicateCanonical) {
    return { ok: false, class: LEAD_CLASS.DUPLICATE, reasons: ["DUPLICATE"], supportSignals: [], supportCount: 0 };
  }
  if (NOISE_RE.test(b)) {
    return { ok: false, class: LEAD_CLASS.REJECTED, reasons: ["NOISE_DIRECTORY"], supportSignals: [], supportCount: 0 };
  }
  if (REJECT_GENERIC_RE.test(b) && !LODGING_RE.test(b)) {
    return {
      ok: false,
      class: LEAD_CLASS.REJECTED,
      reasons: ["GENERIC_ACTIVITY_NO_GROUP_MOTION"],
      supportSignals: [],
      supportCount: 0,
    };
  }
  if (/HISTORICAL_ONLY/i.test(String(candidate.timingState || ""))) {
    return { ok: false, class: LEAD_CLASS.REJECTED, reasons: ["HISTORICAL_ONLY"], supportSignals: [], supportCount: 0 };
  }

  const org = String(candidate.organizationName || candidate.organization || "").trim();
  const title = String(candidate.title || "").trim();
  const entityOk =
    (org.length >= 4 && !/^https?/i.test(org)) ||
    (title.length >= 10 && GROUP_RE.test(title));
  if (!entityOk) {
    return { ok: false, class: LEAD_CLASS.REJECTED, reasons: ["ENTITY_WEAK"], supportSignals: [], supportCount: 0 };
  }

  const marketOk =
    !geoTokens.length ||
    geoTokens.some((t) => b.toLowerCase().includes(String(t).toLowerCase())) ||
    Boolean(candidate.originMarket || candidate.lodgingMarket || candidate.eventLocationSummary) ||
    Boolean(candidate.feederMarket);
  if (!marketOk) {
    return {
      ok: false,
      class: LEAD_CLASS.SIGNAL_ONLY,
      reasons: ["MARKET_RELEVANCE_WEAK"],
      supportSignals: [],
      supportCount: 0,
    };
  }

  const futureGroup =
    FUTURE_RE.test(b) ||
    GROUP_RE.test(b) ||
    Boolean(candidate.eventStartDate || candidate.eventYear) ||
    /PROCUREMENT|ROTATION|COMPETITIVE/i.test(String(candidate.engine || candidate.signalType || ""));
  if (!futureGroup) {
    return {
      ok: false,
      class: LEAD_CLASS.SIGNAL_ONLY,
      reasons: ["NO_PLAUSIBLE_FUTURE_GROUP_MOTION"],
      supportSignals: [],
      supportCount: 0,
    };
  }

  const support = collectSupport(candidate, b);
  if (support.length < 2) {
    return {
      ok: false,
      class: LEAD_CLASS.SIGNAL_ONLY,
      reasons: ["FEWER_THAN_TWO_SUPPORTING_SIGNALS", ...support],
      supportSignals: support,
      supportCount: support.length,
    };
  }
  const hasMotion = support.some((x) => MOTION_SUPPORT.has(x));
  if (!hasMotion) {
    return {
      ok: false,
      class: LEAD_CLASS.SIGNAL_ONLY,
      reasons: ["NO_HOTEL_DEMAND_MOTION_SUPPORT", ...support],
      supportSignals: support,
      supportCount: support.length,
    };
  }

  return {
    ok: true,
    class: LEAD_CLASS.RESEARCH_LEAD,
    reasons: ["VALID_ENTITY", "MARKET_RELEVANCE", "FUTURE_GROUP_MOTION", ...support],
    supportSignals: support,
    supportCount: support.length,
  };
}

/**
 * Promote research lead → candidate opportunity after page validation.
 */
export function isGdiCandidateOpportunity(lead = {}, pageEvidence = {}) {
  const org = lead.organizationName || lead.organization;
  const motion = lead.groupMotion || lead.opportunityType || pageEvidence.groupMotion;
  const future =
    lead.eventStartDate ||
    lead.eventYear ||
    pageEvidence.eventStartDate ||
    pageEvidence.futureCycleEvidenceState ||
    lead.timingState;
  const lodging =
    lead.lodgingEvidence ||
    pageEvidence.lodgingEvidence ||
    pageEvidence.lodgingClass;
  const fit = Number(lead.hotelFitScore ?? lead.defaultFitScore ?? 0) >= 45;
  const buyer =
    lead.buyerEntity ||
    lead.organizationContactUrl ||
    pageEvidence.organizationContactUrl ||
    pageEvidence.functionalContactEmail ||
    lead.buyerType;

  const missing = [];
  if (!org) missing.push("ORGANIZATION");
  if (!motion && !/OVERFLOW|HOUSING|GROUP|PRIMARY|FUTURE/i.test(String(lead.opportunityType || ""))) {
    missing.push("GROUP_MOTION");
  }
  if (!future) missing.push("FUTURE_DECISION_POINT");
  if (!lodging && !/LODGING|HOUSING|PROCUREMENT/i.test((lead.supportSignals || []).join("|"))) {
    missing.push("LODGING_NEED");
  }
  if (!fit) missing.push("HOTEL_FIT");
  if (!buyer) missing.push("BUYER_PATH");

  // Candidate requires org + motion + future + (lodging OR procurement) + fit; buyer path preferred
  const ok =
    Boolean(org) &&
    Boolean(motion || lead.opportunityType) &&
    Boolean(future) &&
    fit &&
    (Boolean(lodging) ||
      (lead.supportSignals || []).includes(SUPPORT_SIGNAL.PROCUREMENT_RFP_SIGNAL) ||
      (lead.supportSignals || []).includes(SUPPORT_SIGNAL.LODGING_HINT)) &&
    (Boolean(buyer) || (lead.supportSignals || []).includes(SUPPORT_SIGNAL.BUYER_ORGANIZER_HINT));

  return { ok, missing, class: ok ? "CANDIDATE_OPPORTUNITY" : "RESEARCH_LEAD_INCOMPLETE" };
}
