/**
 * Commercially aware, question-conditioned source selection for the GDI Research Supervisor.
 * Ranks unread URLs by relevance to the current research intent, not HTML order or generic page prestige.
 * Does not call providers, reserve budget, or change commercial/publication fields.
 */

/** Local mirror of planner SOURCE_FAMILY strings — avoids circular import with dynamic-research-planner-v1. */
const SOURCE_FAMILY = Object.freeze({
  ORGANIZER_PUBLICATION: "organizer_publication",
  HISTORICAL_MEETING_PROGRAM: "historical_meeting_program",
  ACCOMMODATION_DOCUMENT: "accommodation_document",
  PARTNER_ORGANIZATION: "partner_organization",
  VERIFIED_CONTACT_SOURCE: "verified_contact_source",
});

export const COMMERCIAL_SOURCE_SELECTION_VERSION = "gdi-commercial-source-selection-v1";

export const RESEARCH_INTENT = Object.freeze({
  OPPORTUNITY_DISCOVERY: "OPPORTUNITY_DISCOVERY",
  COMMERCIAL_QUALIFICATION: "COMMERCIAL_QUALIFICATION",
  CONTACT_DEVELOPMENT: "CONTACT_DEVELOPMENT",
  COMPETITIVE_INTELLIGENCE: "COMPETITIVE_INTELLIGENCE",
  FUTURE_CYCLE_RESEARCH: "FUTURE_CYCLE_RESEARCH",
});

export const COMMERCIAL_FACT = Object.freeze({
  HOTEL_PLACEMENT_OR_CONTRACTING: "hotel_placement_or_contracting",
  NAMED_ACCOMMODATION_PARTNERS: "named_accommodation_partners",
  RECENT_INCENTIVE_PROGRAMMES: "recent_incentive_programmes",
  CURRENT_OR_UPCOMING_GROUP_ACTIVITY: "current_or_upcoming_group_activity",
  DECISION_MAKER_OR_CONTACT_ROUTE: "decision_maker_or_contact_route",
  CORPORATE_OR_ASSOCIATION_CLIENTS: "corporate_or_association_clients",
  FUTURE_DEMAND_POTENTIAL: "future_demand_potential",
  TARGET_HOTEL_COMMERCIAL_SUITABILITY: "target_hotel_commercial_suitability",
});

/**
 * Long-form investigations should consider unresolved questions across these commercial dimensions.
 * Not every dimension must be researched when evidence is already sufficient or VoI is low.
 */
export const COMMERCIAL_RESEARCH_DIMENSION = Object.freeze({
  HOTEL_PLACEMENT: "actual_hotel_placement",
  HISTORICAL_PROGRAMS: "historical_programs",
  CORPORATE_OR_ASSOCIATION_CLIENTS: "relevant_corporate_or_association_clients",
  ACCOMMODATION_PARTNERS: "accommodation_partners",
  SOURCING_AND_CONTRACTING: "sourcing_and_contracting_responsibility",
  DECISION_MAKERS_AND_CONTACTS: "decision_makers_and_contact_routes",
  FUTURE_DEMAND_POTENTIAL: "future_demand_potential",
  TARGET_HOTEL_SUITABILITY: "target_hotel_commercial_suitability",
});

export const ENTITY_RELATIONSHIP = Object.freeze({
  SAME_ORGANIZATION_PAGE: "SAME_ORGANIZATION_PAGE",
  /** Named on organizer/same-publisher pages only — a lead, not independent verification. */
  RELATED_ENTITY_LEAD: "RELATED_ENTITY_LEAD",
  /**
   * @deprecated Misused historically for same-publisher hotel mentions.
   * Prefer RELATED_ENTITY_LEAD until an independent publisher confirms the relationship.
   * Kept as an alias so older offline fixtures can migrate.
   */
  INDEPENDENT_CORROBORATION: "RELATED_ENTITY_LEAD",
});

export const RELATIONSHIP_VERIFICATION = Object.freeze({
  ORGANIZER_CLAIMED: "ORGANIZER_CLAIMED",
  LEAD_UNVERIFIED: "LEAD_UNVERIFIED",
  INDEPENDENTLY_VERIFIED: "INDEPENDENTLY_VERIFIED",
  INDEPENDENT_CONFIRMATION_UNAVAILABLE: "INDEPENDENT_CONFIRMATION_UNAVAILABLE",
});

export const STOPPING_TRIGGER = Object.freeze({
  RESEARCH_SUFFICIENT: "RESEARCH_SUFFICIENT",
  BUYER_ENGAGEMENT_MORE_USEFUL: "BUYER_ENGAGEMENT_MORE_USEFUL",
  RETRIEVAL_CAP_REACHED: "RETRIEVAL_CAP_REACHED",
  SUBSTANTIVE_QUESTION_CAP_REACHED: "SUBSTANTIVE_QUESTION_CAP_REACHED",
  BUDGET_EXHAUSTED: "BUDGET_EXHAUSTED",
  NO_UNREAD_COMMERCIAL_SOURCE: "NO_UNREAD_COMMERCIAL_SOURCE",
  PROVIDER_BLOCKED: "PROVIDER_BLOCKED",
  DEAD_END_LEAD: "DEAD_END_LEAD",
  /** Soft ceiling — commercially incomplete; not exhaustion. */
  ITERATION_CEILING_HIT: "ITERATION_CEILING_HIT",
  /** Authorized related-entity leads remain unexecuted. */
  RELATED_ENTITY_FOLLOWUPS_PENDING: "RELATED_ENTITY_FOLLOWUPS_PENDING",
  /** Paid/provider denied; public path may still have unread commercial sources. */
  PROVIDER_RESTRICTED_NOT_EXHAUSTION: "PROVIDER_RESTRICTED_NOT_EXHAUSTION",
});

const TECHNICAL_PATTERNS = [
  { id: "wordpress_oembed", pattern: /\/wp-json\/oembed\b|\/oembed\/1\.0\/embed\b/i },
  { id: "wordpress_json_api", pattern: /\/wp-json(?:\/|$|\?)/i },
  { id: "rss_or_feed", pattern: /\/feed(?:\/|$|\?)|\/rss(?:\/|$|\?)|\.xml($|\?)/i },
  { id: "auth_or_admin", pattern: /\/wp-admin\b|\/wp-login\b|\/login\b|\/signin\b|\/account\b|\/cart\b|\/checkout\b/i },
  { id: "tracking_or_analytics", pattern: /google-analytics|googletagmanager|hotjar|segment\.io|doubleclick|facebook\.com\/tr|\/pixel\b/i },
  { id: "share_or_social_widget", pattern: /addtoany|sharethis|platform\.twitter|linkedin\.com\/share|facebook\.com\/sharer|mailto:/i },
  { id: "asset_or_static", pattern: /\.(?:css|js|map|woff2?|ttf|eot|ico|jpg|jpeg|png|gif|svg|webp)(?:$|\?)/i },
  { id: "xmlrpc_or_sitemap_meta", pattern: /\/xmlrpc\.php\b|\/sitemap(?:_index)?\.xml\b/i },
];

const STRUCTURED_EVIDENCE_ALLOW = [
  { id: "open_api_events", pattern: /\/api\/(?:v\d+\/)?(?:events|meetings|programmes?|accommodation|hotels)\b/i },
  { id: "json_ld_events_feed", pattern: /\/events\.json\b|\/meetings\.json\b/i },
];

/** Intent → surface weights. Higher = stronger fit for that unresolved question. No Principal URL is hardcoded as winner. */
const INTENT_SURFACE_WEIGHTS = Object.freeze({
  [RESEARCH_INTENT.OPPORTUNITY_DISCOVERY]: Object.freeze({
    news: 10,
    case_study: 6,
    destination_london: 8,
    destination: 5,
    who_we_are: 2,
    what_we_do: 3,
    contact: 1,
    partner: 4,
  }),
  [RESEARCH_INTENT.COMMERCIAL_QUALIFICATION]: Object.freeze({
    case_study: 10,
    who_we_are: 8,
    what_we_do: 9,
    destination_london: 4,
    destination: 3,
    news: 3,
    contact: 2,
    partner: 5,
  }),
  [RESEARCH_INTENT.CONTACT_DEVELOPMENT]: Object.freeze({
    contact: 12,
    who_we_are: 9,
    team: 10,
    what_we_do: 3,
    news: 2,
    case_study: 1,
    destination_london: 1,
    destination: 1,
    partner: 2,
  }),
  [RESEARCH_INTENT.COMPETITIVE_INTELLIGENCE]: Object.freeze({
    news: 7,
    case_study: 8,
    destination_london: 6,
    destination: 5,
    partner: 7,
    who_we_are: 3,
    what_we_do: 4,
    contact: 1,
  }),
  [RESEARCH_INTENT.FUTURE_CYCLE_RESEARCH]: Object.freeze({
    news: 9,
    destination_london: 5,
    destination: 4,
    case_study: 3,
    who_we_are: 1,
    what_we_do: 2,
    contact: 1,
    partner: 3,
  }),
});

const INTENT_DEFAULT_FACTS = Object.freeze({
  [RESEARCH_INTENT.OPPORTUNITY_DISCOVERY]: [
    COMMERCIAL_FACT.RECENT_INCENTIVE_PROGRAMMES,
    COMMERCIAL_FACT.CURRENT_OR_UPCOMING_GROUP_ACTIVITY,
    COMMERCIAL_FACT.NAMED_ACCOMMODATION_PARTNERS,
  ],
  [RESEARCH_INTENT.COMMERCIAL_QUALIFICATION]: [
    COMMERCIAL_FACT.HOTEL_PLACEMENT_OR_CONTRACTING,
    COMMERCIAL_FACT.NAMED_ACCOMMODATION_PARTNERS,
  ],
  [RESEARCH_INTENT.CONTACT_DEVELOPMENT]: [
    COMMERCIAL_FACT.DECISION_MAKER_OR_CONTACT_ROUTE,
  ],
  [RESEARCH_INTENT.COMPETITIVE_INTELLIGENCE]: [
    COMMERCIAL_FACT.NAMED_ACCOMMODATION_PARTNERS,
    COMMERCIAL_FACT.HOTEL_PLACEMENT_OR_CONTRACTING,
    COMMERCIAL_FACT.RECENT_INCENTIVE_PROGRAMMES,
  ],
  [RESEARCH_INTENT.FUTURE_CYCLE_RESEARCH]: [
    COMMERCIAL_FACT.CURRENT_OR_UPCOMING_GROUP_ACTIVITY,
    COMMERCIAL_FACT.RECENT_INCENTIVE_PROGRAMMES,
  ],
});

function text(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

export function normalizeCandidateUrl(value = "", baseUrl = "") {
  const raw = String(value || "").trim();
  if (!raw || raw.startsWith("#") || raw.toLowerCase().startsWith("javascript:")) return null;
  try {
    const href = new URL(raw, baseUrl || "https://example.invalid/").href;
    return href.split("#")[0].replace(/\/+$/, "") || href;
  } catch {
    return null;
  }
}

export function canonicalSourceKey(url = "") {
  const normalized = normalizeCandidateUrl(url);
  if (!normalized) return "";
  try {
    const parsed = new URL(normalized);
    const host = parsed.hostname.replace(/^www\./i, "").toLowerCase();
    let path = parsed.pathname.replace(/\/+$/, "").toLowerCase() || "/";
    path = path.replace(/\/index(?:\.(?:html?|php))?$/i, "") || "/";
    const params = [...parsed.searchParams.entries()]
      .filter(([key]) => !/^(utm_|fbclid|gclid|mc_|ref$)/i.test(key))
      .sort(([a], [b]) => a.localeCompare(b));
    const query = params.length ? `?${params.map(([k, v]) => `${k}=${v}`).join("&")}` : "";
    return `${host}${path}${query}`;
  } catch {
    return normalized.toLowerCase();
  }
}

/** Registrable-ish publisher host (strips www.). Distinct from sourceFamily labels. */
export function resolveSourceOwnerHost(url = "") {
  try {
    return new URL(String(url || "").trim()).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

/**
 * Publishing organization / ownership metadata for a retrieved URL.
 * sourceFamily remains a content-type label and must not be treated as independence.
 */
export function resolvePublishingOrganization({
  url = "",
  organizationName = "",
  organizationHost = "",
  knownPublisherMap = {},
} = {}) {
  const host = resolveSourceOwnerHost(url);
  const orgHost = text(organizationHost).replace(/^www\./i, "").toLowerCase();
  const mapped = knownPublisherMap[host] || null;
  const sameAsOrganizer = Boolean(host && orgHost && (host === orgHost || host.endsWith(`.${orgHost}`) || orgHost.endsWith(`.${host}`)));
  let publishingOrganization = mapped;
  if (!publishingOrganization) {
    if (sameAsOrganizer) publishingOrganization = organizationName || orgHost || host;
    else if (/rosewoodhotels\.com/i.test(host)) publishingOrganization = "Rosewood Hotels";
    else if (/thesavoy\.co\.uk|fairmont\.com/i.test(host)) publishingOrganization = "The Savoy / Fairmont";
    else if (/hilton\.com/i.test(host)) publishingOrganization = "Hilton";
    else if (/nobuhotels\.com/i.test(host)) publishingOrganization = "Nobu Hotels";
    else publishingOrganization = host || "unknown_publisher";
  }
  return {
    url: String(url || ""),
    sourceOwnerHost: host,
    publishingOrganization,
    organizationHost: orgHost || null,
    samePublisherAsOrganizer: sameAsOrganizer,
    independentOfOrganizer: Boolean(host) && !sameAsOrganizer,
  };
}

export function isIndependentPublisherUrl(url = "", organizationHost = "") {
  return resolvePublishingOrganization({ url, organizationHost }).independentOfOrganizer === true;
}

/**
 * Only after an independently owned publisher page is accepted may a relationship be independently verified.
 * A Principal page that names The Savoy is ORGANIZER_CLAIMED / LEAD_UNVERIFIED — never independently verified.
 */
export function assessRelationshipVerification({
  provenanceUrl = "",
  confirmingUrl = null,
  organizationHost = "",
  relationshipMentionedOnOrganizer = false,
  independentPublisherConfirms = false,
  independentSearchAttempted = false,
  independentEvidenceFound = false,
} = {}) {
  const provenance = resolvePublishingOrganization({ url: provenanceUrl, organizationHost });
  if (confirmingUrl && isIndependentPublisherUrl(confirmingUrl, organizationHost) && independentPublisherConfirms) {
    return {
      verificationStatus: RELATIONSHIP_VERIFICATION.INDEPENDENTLY_VERIFIED,
      independentlyVerified: true,
      provenancePublisher: provenance.publishingOrganization,
      confirmingPublisher: resolvePublishingOrganization({ url: confirmingUrl, organizationHost }).publishingOrganization,
      note: "Independent publisher evidence confirms the named relationship.",
    };
  }
  if (independentSearchAttempted && !independentEvidenceFound) {
    return {
      verificationStatus: RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE,
      independentlyVerified: false,
      provenancePublisher: provenance.publishingOrganization,
      confirmingPublisher: null,
      note: "Independent publisher confirmation was sought and is not available from accepted evidence.",
    };
  }
  if (provenance.samePublisherAsOrganizer || relationshipMentionedOnOrganizer) {
    return {
      verificationStatus: RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
      independentlyVerified: false,
      provenancePublisher: provenance.publishingOrganization,
      confirmingPublisher: null,
      note: "Organizer-owned mention is a lead only; independent publisher evidence is required before independent verification.",
    };
  }
  return {
    verificationStatus: RELATIONSHIP_VERIFICATION.ORGANIZER_CLAIMED,
    independentlyVerified: false,
    provenancePublisher: provenance.publishingOrganization,
    confirmingPublisher: null,
    note: "Claim remains organizer-attributed until independently published evidence is accepted.",
  };
}

const DIMENSION_QUESTION_TEMPLATES = Object.freeze({
  [COMMERCIAL_RESEARCH_DIMENSION.HOTEL_PLACEMENT]:
    "What demonstrated hotel-placement or bedroom contracting has the organizer published?",
  [COMMERCIAL_RESEARCH_DIMENSION.HISTORICAL_PROGRAMS]:
    "Which historical group or incentive programmes are documented with dates, scale, or hotel bases?",
  [COMMERCIAL_RESEARCH_DIMENSION.CORPORATE_OR_ASSOCIATION_CLIENTS]:
    "Which corporate or association clients are named in accepted evidence for London programmes?",
  [COMMERCIAL_RESEARCH_DIMENSION.ACCOMMODATION_PARTNERS]:
    "Which accommodation partners are named, and which relationships remain organizer-claimed only?",
  [COMMERCIAL_RESEARCH_DIMENSION.SOURCING_AND_CONTRACTING]:
    "Who holds hotel sourcing and contracting responsibility for London incentive delivery?",
  [COMMERCIAL_RESEARCH_DIMENSION.DECISION_MAKERS_AND_CONTACTS]:
    "What decision-maker or contact routes are published for commercial introduction?",
  [COMMERCIAL_RESEARCH_DIMENSION.FUTURE_DEMAND_POTENTIAL]:
    "Is there accepted evidence of upcoming group/incentive demand timing beyond historical capability?",
  [COMMERCIAL_RESEARCH_DIMENSION.TARGET_HOTEL_SUITABILITY]:
    "What accepted evidence supports or limits commercial suitability of the target hotel for this organizer?",
});

/**
 * Build coverage of commercial dimensions from accepted claims / researched questions.
 * Does not force research of every dimension when VoI is low.
 */
export function assessCommercialDimensionCoverage({
  acceptedClaims = [],
  substantiveQuestions = [],
  unresolvedGaps = [],
} = {}) {
  const textBlob = [
    ...acceptedClaims.map((item) => `${item.topic || ""} ${item.statement || ""}`),
    ...substantiveQuestions.map((item) => `${item.question || ""} ${item.dimension || ""}`),
    ...unresolvedGaps,
  ].join(" ").toLowerCase();

  const covered = [];
  const unresolved = [];
  const rules = [
    { dimension: COMMERCIAL_RESEARCH_DIMENSION.HOTEL_PLACEMENT, pattern: /hotel sourcing|contract negotiation|placement|bedroom|room block|accommodation partner/i },
    { dimension: COMMERCIAL_RESEARCH_DIMENSION.HISTORICAL_PROGRAMS, pattern: /incentive|seven-day|five-night|programme|program|historical/i },
    { dimension: COMMERCIAL_RESEARCH_DIMENSION.CORPORATE_OR_ASSOCIATION_CLIENTS, pattern: /client|association|relativity|meeting space|corporate/i },
    { dimension: COMMERCIAL_RESEARCH_DIMENSION.ACCOMMODATION_PARTNERS, pattern: /rosewood|savoy|hilton|bankside|accommodation partner|named hotel/i },
    { dimension: COMMERCIAL_RESEARCH_DIMENSION.SOURCING_AND_CONTRACTING, pattern: /hotel sourcing|contract negotiation|sourcing and contracting/i },
    { dimension: COMMERCIAL_RESEARCH_DIMENSION.DECISION_MAKERS_AND_CONTACTS, pattern: /contact|decision.?maker|@|phone|020\s?\d/i },
    { dimension: COMMERCIAL_RESEARCH_DIMENSION.FUTURE_DEMAND_POTENTIAL, pattern: /upcoming|future demand|current or upcoming|2026|accepting/i },
    { dimension: COMMERCIAL_RESEARCH_DIMENSION.TARGET_HOTEL_SUITABILITY, pattern: /nobu|portman|target.?hotel|suitability|marylebone|fit/i },
  ];
  for (const rule of rules) {
    const hit = rule.pattern.test(textBlob)
      || substantiveQuestions.some((item) => item.dimension === rule.dimension);
    if (hit) covered.push(rule.dimension);
    else unresolved.push(rule.dimension);
  }
  return {
    dimensions: Object.values(COMMERCIAL_RESEARCH_DIMENSION),
    covered,
    unresolved,
    nextPreferred: unresolved[0] || null,
    nextPreferredQuestion: unresolved[0] ? DIMENSION_QUESTION_TEMPLATES[unresolved[0]] : null,
    mustResearchAll: false,
  };
}

export function buildStopRecord({
  researchDisposition = null,
  commercialNextAction = null,
  stoppingTrigger = null,
  unresolvedQuestions = [],
  remainingAuthorizedBudgetUsd = null,
  retrievalCount = null,
  maxRetrievals = null,
  note = null,
} = {}) {
  const retrievalCapHit = Number.isFinite(retrievalCount)
    && Number.isFinite(maxRetrievals)
    && retrievalCount >= maxRetrievals;
  const trigger = stoppingTrigger
    || (retrievalCapHit ? STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED : null);
  return {
    researchDisposition: researchDisposition || null,
    commercialNextAction: commercialNextAction || null,
    stoppingTrigger: trigger,
    unresolvedQuestions: [...(unresolvedQuestions || [])],
    remainingAuthorizedBudgetUsd: remainingAuthorizedBudgetUsd == null ? null : Number(remainingAuthorizedBudgetUsd),
    note: note
      || (trigger === STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED
        ? "Retrieval cap reached is not research exhaustion; further public leads may remain untested."
        : null),
    retrievalCapIsNotResearchExhaustion: trigger === STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED,
  };
}

export function classifyTechnicalSource(url = "") {
  const value = String(url || "");
  for (const allowed of STRUCTURED_EVIDENCE_ALLOW) {
    if (allowed.pattern.test(value)) {
      return { rejected: false, technical: true, allowStructuredEvidence: true, reason: null, classId: allowed.id };
    }
  }
  for (const rule of TECHNICAL_PATTERNS) {
    if (rule.pattern.test(value)) {
      return { rejected: true, technical: true, allowStructuredEvidence: false, reason: rule.id, classId: rule.id };
    }
  }
  return { rejected: false, technical: false, allowStructuredEvidence: false, reason: null, classId: "content_page" };
}

/**
 * Resolve the research intent from an explicit intent or the unresolved question text.
 */
export function resolveResearchIntent({
  intent = null,
  researchQuestion = "",
  factsSought = null,
} = {}) {
  const explicit = text(intent).toUpperCase();
  if (Object.values(RESEARCH_INTENT).includes(explicit)) {
    return {
      intent: explicit,
      inferred: false,
      researchQuestion: text(researchQuestion),
      factsSought: Array.isArray(factsSought) && factsSought.length ? factsSought : INTENT_DEFAULT_FACTS[explicit],
    };
  }
  const q = text(researchQuestion).toLowerCase();
  let resolved = RESEARCH_INTENT.OPPORTUNITY_DISCOVERY;
  if (/decision[- ]maker|contact|who (?:to )?email|sourcing (?:desk|lead|manager)|hotel-sourcing decision/i.test(q)) {
    resolved = RESEARCH_INTENT.CONTACT_DEVELOPMENT;
  } else if (/historical|placement capability|verify.*(?:hotel|accommodation)|contracting capability|what-we-do|past (?:programme|program|incentive)/i.test(q)) {
    resolved = RESEARCH_INTENT.COMMERCIAL_QUALIFICATION;
  } else if (/competitor|rival hotel|comp set|which hotels? (?:they|principal) (?:use|used|prefer)/i.test(q)) {
    resolved = RESEARCH_INTENT.COMPETITIVE_INTELLIGENCE;
  } else if (/next cycle|future cycle|later announcement|2028|2029|scheduled revisit/i.test(q)) {
    resolved = RESEARCH_INTENT.FUTURE_CYCLE_RESEARCH;
  } else if (/current|upcoming|discover|incentive|accommodation program|programme|bedroom demand|group activit/i.test(q)) {
    resolved = RESEARCH_INTENT.OPPORTUNITY_DISCOVERY;
  }
  return {
    intent: resolved,
    inferred: true,
    researchQuestion: text(researchQuestion),
    factsSought: Array.isArray(factsSought) && factsSought.length ? factsSought : INTENT_DEFAULT_FACTS[resolved],
  };
}

function detectSurfaces(url = "") {
  const path = String(url || "").toLowerCase();
  const surfaces = [];
  if (/\/news\b/i.test(path)) surfaces.push("news");
  if (/case-stud/i.test(path)) surfaces.push("case_study");
  if (/\/our-destinations\/london\b/i.test(path)) surfaces.push("destination_london");
  else if (/destination/i.test(path)) surfaces.push("destination");
  if (/\/who-we-are\b/i.test(path)) surfaces.push("who_we_are");
  if (/\/what-we-do\b/i.test(path)) surfaces.push("what_we_do");
  if (/\/contact/i.test(path)) surfaces.push("contact");
  if (/\/(?:team|people|staff|leadership)\b/i.test(path)) surfaces.push("team");
  if (/partner|hotel|accommodation|housing/i.test(path)) surfaces.push("partner");
  return surfaces;
}

function intentRelevanceScore(url, intent) {
  const weights = INTENT_SURFACE_WEIGHTS[intent] || INTENT_SURFACE_WEIGHTS[RESEARCH_INTENT.OPPORTUNITY_DISCOVERY];
  const surfaces = detectSurfaces(url);
  let score = 0;
  const hits = [];
  for (const surface of surfaces) {
    const weight = Number(weights[surface] || 0);
    if (weight > 0) {
      score += weight;
      hits.push(`intent_${intent.toLowerCase()}_surface_${surface}_+${weight}`);
    }
  }
  // Mild penalty when a page is a weak fit for the active intent (e.g. who-we-are during opportunity discovery).
  if (intent === RESEARCH_INTENT.OPPORTUNITY_DISCOVERY && /\/who-we-are\b/i.test(url) && !/news|destination|case-stud/i.test(url)) {
    score -= 1;
    hits.push("weak_fit_penalty_capabilities_for_discovery");
  }
  if (intent === RESEARCH_INTENT.CONTACT_DEVELOPMENT && /\/news\b/i.test(url) && !/contact|who-we-are|team/i.test(url)) {
    score -= 2;
    hits.push("weak_fit_penalty_news_for_contact");
  }
  if (intent === RESEARCH_INTENT.COMMERCIAL_QUALIFICATION && /\/news\b/i.test(url) && !/case-stud|who-we-are|what-we-do/i.test(url)) {
    score -= 1;
    hits.push("weak_fit_penalty_news_for_historical_qualification");
  }
  return { score, hits, surfaces };
}

function questionTokenBoost(url, researchQuestion = "") {
  const q = text(researchQuestion).toLowerCase();
  if (!q) return { score: 0, hits: [] };
  const path = String(url || "").toLowerCase();
  let score = 0;
  const hits = [];
  if (/london/.test(q) && /london/.test(path)) {
    score += 3;
    hits.push("question_token_london");
  }
  if (/incentive|programme|program|accommodation/.test(q) && /news|case-stud|destination|accommodation|incentive/.test(path)) {
    score += 3;
    hits.push("question_token_programme_or_accommodation");
  }
  if (/decision|contact|sourcing/.test(q) && /contact|who-we-are|team|people/.test(path)) {
    score += 3;
    hits.push("question_token_contact_or_decision_maker");
  }
  if (/historical|placement|capability|contract/.test(q) && /who-we-are|what-we-do|case-stud/.test(path)) {
    score += 3;
    hits.push("question_token_historical_capability");
  }
  return { score, hits };
}

function inferSourceFamily(url = "") {
  if (/\.pdf($|\?)/i.test(url)) return SOURCE_FAMILY.ACCOMMODATION_DOCUMENT;
  if (/case-stud|programme|program|incentive|\/news\b|20\d{2}/i.test(url)) return SOURCE_FAMILY.HISTORICAL_MEETING_PROGRAM;
  if (/partner|destination|hotel|accommodation|housing/i.test(url)) return SOURCE_FAMILY.PARTNER_ORGANIZATION;
  if (/contact|team|staff|people/i.test(url)) return SOURCE_FAMILY.VERIFIED_CONTACT_SOURCE;
  return SOURCE_FAMILY.ORGANIZER_PUBLICATION;
}

function hotelRelevance(url, hotelName = "", market = "") {
  let score = 0;
  const hits = [];
  if (market && new RegExp(String(market).split(/\s+/)[0], "i").test(url)) {
    score += 1;
    hits.push("market_token_in_url");
  }
  if (/london|marylebone|portman|west-end|westend/i.test(url)) {
    score += 1;
    hits.push("hotel_catchment_geography");
  }
  if (hotelName && /nobu/i.test(hotelName) && /nobu/i.test(url)) {
    score += 3;
    hits.push("names_target_hotel");
  }
  return { score, hits };
}

/**
 * Filter then rank candidates for the current research intent / unresolved question.
 * Input URL order must not change the winner when scores are equal (canonical tie-break).
 */
export function selectCommercialResearchSource({
  candidates = [],
  investigatedUrls = [],
  baseUrl = "",
  intent = null,
  researchQuestion = "",
  factsSought = null,
  hotelName = "",
  hotelMarket = "",
  organizationName = "",
  estimatedUsdByUrl = {},
  requireUnread = true,
} = {}) {
  const resolved = resolveResearchIntent({ intent, researchQuestion, factsSought });
  const held = new Set((investigatedUrls || []).map(canonicalSourceKey).filter(Boolean));
  const facts = resolved.factsSought.map((fact) => text(fact));
  const considered = [];
  const rejected = [];
  const ranked = [];
  const seenCanonical = new Set();

  for (const raw of candidates) {
    const input = typeof raw === "string" ? { url: raw } : (raw || {});
    const url = normalizeCandidateUrl(input.url, baseUrl || input.baseUrl || "");
    if (!url) {
      rejected.push({ url: input.url || null, reason: "invalid_url" });
      continue;
    }
    considered.push(url);
    const technical = classifyTechnicalSource(url);
    if (technical.rejected) {
      rejected.push({ url, reason: technical.reason, detail: "technical_or_low_information_endpoint" });
      continue;
    }
    const key = canonicalSourceKey(url);
    if (seenCanonical.has(key)) {
      rejected.push({ url, reason: "duplicate_or_canonical_equivalent", canonicalKey: key });
      continue;
    }
    seenCanonical.add(key);
    if (requireUnread && held.has(key)) {
      rejected.push({ url, reason: "already_investigated", canonicalKey: key });
      continue;
    }
    if (organizationName && !new RegExp(String(organizationName).split(/\s+/)[0], "i").test(url) && input.requireOrgHost === true) {
      rejected.push({ url, reason: "unrelated_to_organization_host" });
      continue;
    }

    const intentScore = intentRelevanceScore(url, resolved.intent);
    const questionBoost = questionTokenBoost(url, resolved.researchQuestion);
    const hotel = hotelRelevance(url, hotelName, hotelMarket);
    const estimatedUsd = Number(estimatedUsdByUrl[url] ?? input.estimatedUsd ?? 0);
    const costPenalty = estimatedUsd > 0 ? Math.min(2, estimatedUsd * 200) : 0;
    const independence = held.size ? 2 : 1;
    const leadPotential = intentScore.surfaces.some((surface) => ["news", "destination_london", "case_study", "partner", "who_we_are"].includes(surface)) ? 2 : 0;
    const authority = technical.allowStructuredEvidence ? 1 : 2;
    // Intent relevance is the primary factor (weighted ahead of generic authority/independence).
    const score = Number((
      (intentScore.score * 2)
      + questionBoost.score
      + hotel.score
      + independence
      + leadPotential
      + authority
      - costPenalty
    ).toFixed(3));

    ranked.push({
      url,
      canonicalKey: key,
      score,
      intent: resolved.intent,
      surfaces: intentScore.surfaces,
      sourceFamily: input.sourceFamily || inferSourceFamily(url),
      estimatedUsd,
      factsSought: facts,
      rationale: [
        `research_intent_${resolved.intent}`,
        ...intentScore.hits,
        ...questionBoost.hits,
        ...hotel.hits,
        independence ? "independent_of_held_evidence" : null,
        leadPotential ? "may_open_related_leads" : null,
        estimatedUsd === 0 ? "zero_retrieval_cost" : `estimated_cost_${estimatedUsd}`,
        technical.allowStructuredEvidence ? "structured_endpoint_allowed_for_material_evidence" : "content_page",
      ].filter(Boolean),
      whySelectedCouldAnswer: `For intent ${resolved.intent}, unread ${inferSourceFamily(url).replace(/_/g, " ")} at ${url} is more likely than technical metadata to answer: ${resolved.researchQuestion || facts.join(", ")}.`,
    });
  }

  ranked.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return a.canonicalKey.localeCompare(b.canonicalKey);
  });

  const selected = ranked[0] || null;
  const minimumScore = 8;
  if (!selected || selected.score < minimumScore) {
    return {
      version: COMMERCIAL_SOURCE_SELECTION_VERSION,
      intent: resolved.intent,
      researchQuestion: resolved.researchQuestion,
      selected: null,
      nextAction: null,
      stop: true,
      stopReason: selected ? "no_candidate_above_commercial_threshold" : "no_commercially_relevant_unread_source",
      considered,
      rejected,
      ranking: ranked,
      factsSought: facts,
      selectionRationale: selected
        ? `Best candidate ${selected.url} scored ${selected.score} for ${resolved.intent}, below threshold ${minimumScore}.`
        : `No unread commercially relevant source remained for ${resolved.intent}.`,
      canIntroduceNewResearchDirections: false,
      relatedEntityFollowUps: [],
      publicationEligibilityChanged: false,
      commercialStageChanged: false,
    };
  }

  return {
    version: COMMERCIAL_SOURCE_SELECTION_VERSION,
    intent: resolved.intent,
    researchQuestion: resolved.researchQuestion,
    selected,
    nextAction: {
      provider: "direct_public",
      url: selected.url,
      estimatedUsd: selected.estimatedUsd,
      sourceFamily: selected.sourceFamily,
      researchIntent: resolved.intent,
      factSought: facts.join("; "),
      expectedCommercialValue: `Would test whether public pages answer [${resolved.intent}] ${resolved.researchQuestion || facts.join(", ")} for ${hotelName || "the target hotel"}.`,
      evidenceSought: facts.join("; "),
      whySourceMightContainIt: selected.whySelectedCouldAnswer,
      stoppingCriteria: "Read the selected unread commercial source once. Do not treat technical metadata as accommodation demand.",
      executable: true,
      selectionRationale: selected.rationale,
    },
    stop: false,
    stopReason: null,
    considered,
    rejected,
    ranking: ranked,
    factsSought: facts,
    selectionRationale: `Selected ${selected.url} (score ${selected.score}) for ${resolved.intent} because: ${selected.rationale.join("; ")}.`,
    canIntroduceNewResearchDirections: selected.surfaces.some((surface) => ["news", "destination_london", "case_study", "partner", "who_we_are"].includes(surface)),
    relatedEntityFollowUps: [],
    publicationEligibilityChanged: false,
    commercialStageChanged: false,
  };
}

/**
 * From retrieved page text, propose related-entity follow-ups.
 * Same-host programme pages → SAME_ORGANIZATION_PAGE.
 * Hotels/clients named on the organizer's site → RELATED_ENTITY_LEAD (LEAD_UNVERIFIED).
 * Never label a same-publisher mention as independently verified.
 */
export function proposeRelatedEntityFollowUps({
  findingsText = "",
  sourceUrl = "",
  organizationHost = "",
  organizationName = "Principal. London DMC",
  investigatedUrls = [],
  researchIntent = RESEARCH_INTENT.OPPORTUNITY_DISCOVERY,
  entityUrlHints = {},
} = {}) {
  const page = String(findingsText || "");
  const provenanceMeta = resolvePublishingOrganization({
    url: sourceUrl,
    organizationHost,
    organizationName,
  });
  const sourceHost = provenanceMeta.sourceOwnerHost;
  const orgHost = text(organizationHost).replace(/^www\./i, "").toLowerCase() || sourceHost;
  const held = new Set((investigatedUrls || []).map(canonicalSourceKey));
  const followUps = [];
  const hintFor = (name) => {
    if (!name) return null;
    if (entityUrlHints[name]) return entityUrlHints[name];
    const key = Object.keys(entityUrlHints || {}).find((item) => new RegExp(item.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(name));
    return key ? entityUrlHints[key] : null;
  };

  const hotelMatches = page.match(/\b(?:Rosewood London|The Savoy|Bankside Hotel|Hilton|Nobu Hotel London Portman Square|[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,3}\s+Hotel)\b/g) || [];
  for (const name of [...new Set(hotelMatches)].slice(0, 5)) {
    const suggestedUrl = hintFor(name);
    const suggestedIndependent = suggestedUrl ? isIndependentPublisherUrl(suggestedUrl, orgHost) : false;
    const verification = assessRelationshipVerification({
      provenanceUrl: sourceUrl,
      organizationHost: orgHost,
      relationshipMentionedOnOrganizer: provenanceMeta.samePublisherAsOrganizer || !provenanceMeta.independentOfOrganizer,
      confirmingUrl: null,
      independentPublisherConfirms: false,
    });
    followUps.push({
      entityType: "hotel_or_accommodation_partner",
      entityName: name,
      relationship: ENTITY_RELATIONSHIP.RELATED_ENTITY_LEAD,
      verificationStatus: verification.verificationStatus,
      independentlyVerified: false,
      commercialDimension: COMMERCIAL_RESEARCH_DIMENSION.ACCOMMODATION_PARTNERS,
      provenanceUrl: sourceUrl,
      publishingOrganization: provenanceMeta.publishingOrganization,
      sourceOwnerHost: provenanceMeta.sourceOwnerHost,
      suggestedUrl: suggestedUrl || null,
      suggestedUrlIndependentPublisher: suggestedIndependent,
      researchQuestion: `Does an independently published source (not a Principal webpage) corroborate ${name} as a Principal accommodation or programme partner?`,
      researchIntent: RESEARCH_INTENT.COMPETITIVE_INTELLIGENCE,
      note: "Named hotel on an organizer page is a RELATED_ENTITY_LEAD only. Principal describing The Savoy is not independent Savoy corroboration.",
    });
  }

  const programmeMatches = page.match(/\b(?:[A-Z][^.]{0,40}\bincentive\b[^.]{0,40}|Capital Experience|five-night London incentive)\b/gi) || [];
  for (const name of [...new Set(programmeMatches.map((item) => item.replace(/\s+/g, " ").trim()))].slice(0, 3)) {
    followUps.push({
      entityType: "programme",
      entityName: name,
      relationship: ENTITY_RELATIONSHIP.SAME_ORGANIZATION_PAGE,
      verificationStatus: RELATIONSHIP_VERIFICATION.ORGANIZER_CLAIMED,
      independentlyVerified: false,
      commercialDimension: COMMERCIAL_RESEARCH_DIMENSION.HISTORICAL_PROGRAMS,
      provenanceUrl: sourceUrl,
      publishingOrganization: provenanceMeta.publishingOrganization,
      sourceOwnerHost: provenanceMeta.sourceOwnerHost,
      researchQuestion: `Does Principal publish further detail on “${name}” that names current accommodation partners or bedroom commitments?`,
      researchIntent: researchIntent === RESEARCH_INTENT.CONTACT_DEVELOPMENT ? RESEARCH_INTENT.OPPORTUNITY_DISCOVERY : researchIntent,
      note: "Another Principal page about the same programme is same-organization evidence, not independent corroboration.",
    });
  }

  const clientMatches = page.match(/\b(?:Relativity|Meeting Space|[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,2})\b(?=[^.]{0,40}\b(?:client|partner|forum)\b)/g) || [];
  for (const name of [...new Set(clientMatches)].slice(0, 3)) {
    if (/^Principal$/i.test(name)) continue;
    followUps.push({
      entityType: "client_or_partner_organization",
      entityName: name,
      relationship: ENTITY_RELATIONSHIP.RELATED_ENTITY_LEAD,
      verificationStatus: RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
      independentlyVerified: false,
      commercialDimension: COMMERCIAL_RESEARCH_DIMENSION.CORPORATE_OR_ASSOCIATION_CLIENTS,
      provenanceUrl: sourceUrl,
      publishingOrganization: provenanceMeta.publishingOrganization,
      sourceOwnerHost: provenanceMeta.sourceOwnerHost,
      researchQuestion: `Does ${name} publish its own account of a London programme that Principal supported, including hotel placement?`,
      researchIntent: RESEARCH_INTENT.COMPETITIVE_INTELLIGENCE,
      note: "Client/partner lead requires that organization's own publication — not another Principal page — before independent verification.",
    });
  }

  // Drop follow-ups that only point back to already-held canonical keys when a URL is supplied later.
  return followUps.filter((item) => !held.has(canonicalSourceKey(item.suggestedUrl || ""))).map((item) => ({
    ...item,
    organizationHost: orgHost,
    sourceHost,
    sameOrganizationAsSource: sourceHost && orgHost && sourceHost === orgHost,
  }));
}

export function extractCandidateUrlsFromHtml(html = "", baseUrl = "") {
  const hrefs = [...String(html || "").matchAll(/href=["']([^"']+)["']/gi)].map((match) => match[1]);
  const out = [];
  const seen = new Set();
  for (const href of hrefs) {
    const url = normalizeCandidateUrl(href, baseUrl);
    if (!url) continue;
    const key = canonicalSourceKey(url);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(url);
  }
  return out;
}

export function selectCommercialSourceFromPage({
  pageHtml = "",
  pageText = "",
  baseUrl = "",
  investigatedUrls = [],
  intent = null,
  researchQuestion = "",
  factsSought,
  hotelName,
  hotelMarket,
  organizationName,
  extraCandidates = [],
} = {}) {
  const extracted = extractCandidateUrlsFromHtml(pageHtml || pageText, baseUrl);
  const candidates = [...extracted, ...extraCandidates];
  const selection = selectCommercialResearchSource({
    candidates,
    investigatedUrls,
    baseUrl,
    intent,
    researchQuestion,
    factsSought,
    hotelName,
    hotelMarket,
    organizationName,
  });
  const relatedEntityFollowUps = proposeRelatedEntityFollowUps({
    findingsText: pageText || pageHtml,
    sourceUrl: baseUrl,
    organizationHost: organizationName ? undefined : undefined,
    investigatedUrls,
    researchIntent: selection.intent,
  });
  return {
    ...selection,
    relatedEntityFollowUps,
  };
}

/** @deprecated Prefer resolveResearchIntent + intent-specific facts. Kept for callers. */
export function principalNobuFactsSought() {
  return INTENT_DEFAULT_FACTS[RESEARCH_INTENT.OPPORTUNITY_DISCOVERY];
}

/**
 * V2 question banks — prefer commercial-dimension diversity over contact-only loops.
 * Does not force an artificial checklist; VoI still decides what to research next.
 */
export const ACCOUNT_LED_RESEARCH_QUESTIONS_V2 = Object.freeze([
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.SOURCING_AND_CONTRACTING, question: "What evidence shows actual group-accommodation sourcing or hotel-selection influence?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.HOTEL_PLACEMENT, question: "What demonstrated hotel placement or bedroom contracting has been published?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.HISTORICAL_PROGRAMS, question: "Which historical group or incentive programmes are documented with scale or hotel bases?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.ACCOMMODATION_PARTNERS, question: "Which accommodation partners are named, and are they organizer-claimed only?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.CORPORATE_OR_ASSOCIATION_CLIENTS, question: "Which relevant clients or industries are named for group programmes?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.TARGET_HOTEL_SUITABILITY, question: "What accepted evidence supports geographic and positioning fit for the target hotel?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.FUTURE_DEMAND_POTENTIAL, question: "Is there evidence of recurring or upcoming programmes beyond historical capability?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.DECISION_MAKERS_AND_CONTACTS, question: "What decision-maker or usable contact routes are published for commercial introduction?" },
  { dimension: "account_development_approach", question: "What account-development approach is appropriate given verified facts (not invented RFPs)?" },
]);

export const EVENT_LED_RESEARCH_QUESTIONS_V2 = Object.freeze([
  { dimension: "meeting_identity", question: "What verifies the meeting identity and dates?" },
  { dimension: "destination_host", question: "What destination and host-hotel information is published?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.HOTEL_PLACEMENT, question: "What credible overnight-demand mechanism is evidenced?" },
  { dimension: "buyer_or_organizer", question: "Who is the buyer or organizing body?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.SOURCING_AND_CONTRACTING, question: "What is the accommodation sourcing status (unknown ≠ open)?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.ACCOMMODATION_PARTNERS, question: "Which competitive hotels are named, and does that leave future-cycle or overflow room?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.FUTURE_DEMAND_POTENTIAL, question: "What future meeting cycles are evidenced?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.DECISION_MAKERS_AND_CONTACTS, question: "What relevant contacts are published?" },
  { dimension: "hotel_selection_timing", question: "When is hotel selection likely to occur?" },
  { dimension: COMMERCIAL_RESEARCH_DIMENSION.TARGET_HOTEL_SUITABILITY, question: "What hotel-specific demand-capture opportunities remain?" },
]);

/**
 * Prefer next question from uncovered commercial dimensions; avoid contact-only clustering.
 */
export function selectDiverseResearchQuestionV2({
  pathway = "ACCOUNT_LED",
  coveredDimensions = [],
  recentQuestionDimensions = [],
} = {}) {
  const bank = pathway === "EVENT_LED" || pathway === "COMPETITIVE_FUTURE_CYCLE"
    ? EVENT_LED_RESEARCH_QUESTIONS_V2
    : ACCOUNT_LED_RESEARCH_QUESTIONS_V2;
  const covered = new Set((coveredDimensions || []).map(String));
  const recent = new Set((recentQuestionDimensions || []).map(String));
  const contactDim = COMMERCIAL_RESEARCH_DIMENSION.DECISION_MAKERS_AND_CONTACTS;
  const recentContactHeavy = [...recent].filter((d) => d === contactDim).length >= 2;

  const preferred = bank.find((item) => {
    const dim = String(item.dimension);
    if (covered.has(dim)) return false;
    if (recentContactHeavy && dim === contactDim) return false;
    return true;
  }) || bank.find((item) => !recent.has(String(item.dimension))) || bank[0];

  return {
    version: "gdi-research-question-diversity-v2",
    pathway,
    selected: preferred,
    avoidContactOnlyClustering: true,
    mustCompleteArtificialChecklist: false,
  };
}
