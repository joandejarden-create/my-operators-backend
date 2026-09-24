/**
 * GDI Contact Intelligence V1.1 — official source path recovery + dual extract.
 *
 * WHO-first. Recover official domains / pages before people search.
 * Jev contact routing is evaluated in SHADOW only (see contact-jev-shadow-v1-1.js).
 * No Surfe. No invented people. Do not downgrade valid FUNCTIONAL paths.
 */

import {
  extractContactIntelligenceFromSource,
  applySourceContactExtractionToOpportunity,
  CONTACT_DATA_ORIGIN,
} from "./extract-contact-intelligence-from-source.js";
import {
  CONTACT_TIER,
  classifyContactTier,
} from "./contact-tiers-v1-2.js";
import { hasNamedPerson } from "./contact-resolution.js";

export const DOMAIN_CONFIDENCE = Object.freeze({
  OFFICIAL_CONFIRMED: "OFFICIAL_CONFIRMED",
  OFFICIAL_LIKELY: "OFFICIAL_LIKELY",
  AMBIGUOUS: "AMBIGUOUS",
  NO_OFFICIAL_DOMAIN: "NO_OFFICIAL_DOMAIN",
});

export const ORG_FAMILY = Object.freeze({
  ASSOCIATION: "ASSOCIATION",
  UNIVERSITY: "UNIVERSITY",
  CORPORATE: "CORPORATE",
  GOVERNMENT: "GOVERNMENT",
  VENUE: "VENUE",
  SPORTS: "SPORTS",
  OTHER: "OTHER",
});

export const CONTACT_SOURCE_PATH = Object.freeze({
  OFFICIAL_STAFF: "OFFICIAL_STAFF",
  EVENT_PROGRAM_PAGE: "EVENT_PROGRAM_PAGE",
  HOUSING: "HOUSING",
  REGISTRATION: "REGISTRATION",
  VENUE_SALES: "VENUE_SALES",
  DEPARTMENT_PAGE: "DEPARTMENT_PAGE",
  PDF_PROSPECTUS: "PDF_PROSPECTUS",
  DOMAIN_RESOLUTION: "DOMAIN_RESOLUTION",
  GENERAL_OFFICIAL_CONTACT: "GENERAL_OFFICIAL_CONTACT",
  NONE: "NONE",
});

export const CONTACT_FOLLOWUP_TYPE = Object.freeze({
  SEARCH_NAMED_OWNER: "SEARCH_NAMED_OWNER",
  SEARCH_FUNCTIONAL_CONTACT: "SEARCH_FUNCTIONAL_CONTACT",
  SEARCH_OFFICIAL_DOMAIN: "SEARCH_OFFICIAL_DOMAIN",
  SEARCH_STAFF_DIRECTORY: "SEARCH_STAFF_DIRECTORY",
  SEARCH_PROGRAM_OFFICE: "SEARCH_PROGRAM_OFFICE",
  STOP: "STOP",
});

export const NAMED_PERSON_WORTH = Object.freeze({
  YES: "YES",
  NO: "NO",
  UNCERTAIN: "UNCERTAIN",
});

export const FUNCTIONAL_SUFFICIENCY = Object.freeze({
  SUFFICIENT: "SUFFICIENT",
  IMPROVABLE: "IMPROVABLE",
  WEAK: "WEAK",
  UNKNOWN: "UNKNOWN",
});

export const STOP_CONTACT_RESEARCH = Object.freeze({
  CONTINUE: "CONTINUE",
  STOP_SUFFICIENT_PATH: "STOP_SUFFICIENT_PATH",
  STOP_NO_PUBLIC_EVIDENCE: "STOP_NO_PUBLIC_EVIDENCE",
  STOP_LOW_VALUE: "STOP_LOW_VALUE",
});

export const RECOVERY_RESULT = Object.freeze({
  NAMED_CONTACT_FOUND: "NAMED_CONTACT_FOUND",
  FUNCTIONAL_CONTACT_FOUND: "FUNCTIONAL_CONTACT_FOUND",
  BETTER_SOURCE_FOUND: "BETTER_SOURCE_FOUND",
  NO_IMPROVEMENT: "NO_IMPROVEMENT",
  FALSE_LEAD: "FALSE_LEAD",
  STOPPED: "STOPPED",
});

const BAD_OFFICIAL_PATH_RE =
  /\/(privacy|terms|cookie|legal|login|signin|careers|investor|gdpr|accessibility)(\/|$|\.)/i;

const HOTEL_BRAND_HOST_RE =
  /(^|\.)(marriott|hilton|hyatt|ihg|accor|wyndham|choicehotels|radisson)|marriott|hiltonhotels|hyatt\.com|ihg\.com/i;

const DEFAULT_BUDGET = Object.freeze({
  maxDomainQueries: 4,
  maxAdditionalFetches: 4,
  fetchTimeoutMs: 20000,
});

export function isAcceptableOfficialUrl(url = "", opportunity = {}) {
  const s = clean(url);
  if (!/^https?:\/\//i.test(s)) return false;
  if (BAD_OFFICIAL_PATH_RE.test(s)) return false;
  const host = hostnameOf(s);
  if (!host) return false;
  // "Near Marriott HQ" style watches must not adopt the hotel brand domain as organizer
  if (
    HOTEL_BRAND_HOST_RE.test(host) &&
    /\b(near|adjacent|corridor|hq[- ]?adjacent)\b/i.test(opportunity.title || "")
  ) {
    return false;
  }
  return true;
}

function clean(s) {
  return String(s || "").trim();
}

export function hostnameOf(url) {
  try {
    return new URL(String(url)).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return null;
  }
}

export function classifyOrgFamily(opportunity = {}) {
  const blob = [
    opportunity.organizationName,
    opportunity.organization,
    opportunity.title,
    opportunity.opportunityType,
    opportunity.demandFamily,
    opportunity.programType,
  ]
    .map(clean)
    .join(" ")
    .toLowerCase();

  if (/university|alumni|college|school of |campus|georgetown|maryland|umd\b/.test(blob)) {
    return ORG_FAMILY.UNIVERSITY;
  }
  if (/soccer|tournament|athletic|sports|cup\b|championship/.test(blob)) {
    return ORG_FAMILY.SPORTS;
  }
  if (/\bnrc\b|nih\b|government|federal|agency|sbpo|regulatory/.test(blob)) {
    return ORG_FAMILY.GOVERNMENT;
  }
  if (/hotel|marriott|venue|club of |private event|banquet|catering/.test(blob)) {
    return ORG_FAMILY.VENUE;
  }
  if (/inc\.|corp|company|hq\b|corporate|headquarters/.test(blob)) {
    return ORG_FAMILY.CORPORATE;
  }
  if (/association|society|conference|meeting|congress|annual/.test(blob)) {
    return ORG_FAMILY.ASSOCIATION;
  }
  return ORG_FAMILY.OTHER;
}

/**
 * Playbook path templates by org family (relative paths on official domain).
 */
export function contactPlaybookPaths(orgFamily) {
  const common = ["/contact", "/about/contact", "/about-us/contact"];
  const map = {
    [ORG_FAMILY.ASSOCIATION]: [
      "/staff",
      "/about/staff",
      "/our-team",
      "/meetings",
      "/events",
      "/conference",
      "/housing",
      "/registration",
      "/education",
      ...common,
    ],
    [ORG_FAMILY.UNIVERSITY]: [
      "/alumni",
      "/alumni/events",
      "/advancement",
      "/events",
      "/conference-services",
      "/staff-directory",
      "/directory",
      ...common,
    ],
    [ORG_FAMILY.CORPORATE]: [
      "/meetings",
      "/events",
      "/travel",
      "/about",
      "/contact-us",
      ...common,
    ],
    [ORG_FAMILY.GOVERNMENT]: [
      "/public-meetings",
      "/conferences",
      "/training",
      "/about",
      ...common,
    ],
    [ORG_FAMILY.VENUE]: [
      "/events",
      "/private-events",
      "/group-sales",
      "/meetings",
      "/catering",
      ...common,
    ],
    [ORG_FAMILY.SPORTS]: [
      "/hotels",
      "/housing",
      "/travel",
      "/contact",
      "/about",
      "/staff",
      ...common,
    ],
    [ORG_FAMILY.OTHER]: ["/staff", "/meetings", "/events", "/housing", ...common],
  };
  return map[orgFamily] || map[ORG_FAMILY.OTHER];
}

export function pickKnownOfficialUrls(opportunity = {}) {
  const urls = [];
  for (const u of [
    opportunity.officialSource,
    opportunity.discoverySource,
    opportunity.lodgingEvidenceUrl,
    opportunity.contactOfficialUrl,
    opportunity.primaryContact?.sourceUrl,
    ...(Array.isArray(opportunity.sources) ? opportunity.sources.map((s) => s.url || s) : []),
  ]) {
    const s = clean(u);
    if (/^https?:\/\//i.test(s) && !urls.includes(s)) urls.push(s);
  }
  return urls.slice(0, 6);
}

/**
 * Resolve domain confidence from already-known URLs (no network).
 */
export function resolveDomainFromOpportunity(opportunity = {}) {
  const urls = pickKnownOfficialUrls(opportunity).filter((u) =>
    isAcceptableOfficialUrl(u, opportunity)
  );
  if (!urls.length) {
    return {
      host: null,
      seedUrl: null,
      confidence: DOMAIN_CONFIDENCE.NO_OFFICIAL_DOMAIN,
      source: "none",
      urls: [],
    };
  }
  const host = hostnameOf(urls[0]);
  const govOrEdu = /\.(gov|edu)(\.|$)/i.test(host || "");
  const orgIsh = /\.(org)(\.|$)/i.test(host || "");
  return {
    host,
    seedUrl: urls[0],
    confidence:
      govOrEdu || orgIsh
        ? DOMAIN_CONFIDENCE.OFFICIAL_CONFIRMED
        : DOMAIN_CONFIDENCE.OFFICIAL_LIKELY,
    source: "opportunity_urls",
    urls,
  };
}

/**
 * Deterministic GDI contact routing (production path). Jev shadows this.
 */
export function computeGdiContactRouting(opportunity = {}, domainState = {}) {
  const tier = classifyContactTier(opportunity);
  const family = classifyOrgFamily(opportunity);
  const hasDomain =
    domainState.confidence === DOMAIN_CONFIDENCE.OFFICIAL_CONFIRMED ||
    domainState.confidence === DOMAIN_CONFIDENCE.OFFICIAL_LIKELY;
  const named = hasNamedPerson(opportunity.primaryContact || { name: opportunity.primaryContactName });
  const hasFunctionalEmail = /^(events?|meetings?|conference|housing|registration|sales|groupsales)@/i.test(
    opportunity.primaryContactEmail || opportunity.primaryContact?.email || ""
  );
  const functionalOk =
    tier === CONTACT_TIER.FUNCTIONAL_CONTACT ||
    (tier === CONTACT_TIER.ORGANIZATION_PATH && Boolean(opportunity.officialSource || domainState.seedUrl));

  let sourcePath = CONTACT_SOURCE_PATH.NONE;
  let followup = CONTACT_FOLLOWUP_TYPE.STOP;
  let namedWorth = NAMED_PERSON_WORTH.UNCERTAIN;
  let functionalSufficiency = FUNCTIONAL_SUFFICIENCY.UNKNOWN;
  let stop = STOP_CONTACT_RESEARCH.CONTINUE;

  if (!hasDomain) {
    sourcePath = CONTACT_SOURCE_PATH.DOMAIN_RESOLUTION;
    followup = CONTACT_FOLLOWUP_TYPE.SEARCH_OFFICIAL_DOMAIN;
    functionalSufficiency = FUNCTIONAL_SUFFICIENCY.WEAK;
    stop = STOP_CONTACT_RESEARCH.CONTINUE;
  } else if (tier === CONTACT_TIER.NAMED_DIRECT) {
    sourcePath = CONTACT_SOURCE_PATH.NONE;
    followup = CONTACT_FOLLOWUP_TYPE.STOP;
    namedWorth = NAMED_PERSON_WORTH.NO;
    functionalSufficiency = FUNCTIONAL_SUFFICIENCY.SUFFICIENT;
    stop = STOP_CONTACT_RESEARCH.STOP_SUFFICIENT_PATH;
  } else if (tier === CONTACT_TIER.FUNCTIONAL_CONTACT && (hasFunctionalEmail || functionalOk)) {
    // Strong functional — optional named pursuit only for high commercial value
    functionalSufficiency = FUNCTIONAL_SUFFICIENCY.SUFFICIENT;
    namedWorth =
      /ACTIONABLE|HIGH|PURSUE/i.test(
        `${opportunity.bookingWindowStatus || ""} ${opportunity.priority || ""} ${opportunity.opportunityQualification || ""}`
      )
        ? NAMED_PERSON_WORTH.UNCERTAIN
        : NAMED_PERSON_WORTH.NO;
    sourcePath =
      family === ORG_FAMILY.VENUE
        ? CONTACT_SOURCE_PATH.VENUE_SALES
        : family === ORG_FAMILY.UNIVERSITY
          ? CONTACT_SOURCE_PATH.DEPARTMENT_PAGE
          : CONTACT_SOURCE_PATH.OFFICIAL_STAFF;
    followup =
      namedWorth === NAMED_PERSON_WORTH.NO
        ? CONTACT_FOLLOWUP_TYPE.STOP
        : CONTACT_FOLLOWUP_TYPE.SEARCH_STAFF_DIRECTORY;
    stop =
      namedWorth === NAMED_PERSON_WORTH.NO
        ? STOP_CONTACT_RESEARCH.STOP_SUFFICIENT_PATH
        : STOP_CONTACT_RESEARCH.CONTINUE;
  } else if (tier === CONTACT_TIER.NAMED_PARTIAL) {
    sourcePath = CONTACT_SOURCE_PATH.GENERAL_OFFICIAL_CONTACT;
    followup = CONTACT_FOLLOWUP_TYPE.STOP;
    namedWorth = NAMED_PERSON_WORTH.NO; // already have named; Surfe on-demand for HOW
    functionalSufficiency = FUNCTIONAL_SUFFICIENCY.SUFFICIENT;
    stop = STOP_CONTACT_RESEARCH.STOP_SUFFICIENT_PATH;
  } else if (tier === CONTACT_TIER.ORGANIZATION_PATH || tier === CONTACT_TIER.NO_CONTACT) {
    functionalSufficiency = FUNCTIONAL_SUFFICIENCY.WEAK;
    namedWorth = NAMED_PERSON_WORTH.YES;
    if (/housing|hotel|accommodat|lodging/i.test(opportunity.officialSource || "")) {
      sourcePath = CONTACT_SOURCE_PATH.HOUSING;
      followup = CONTACT_FOLLOWUP_TYPE.SEARCH_FUNCTIONAL_CONTACT;
    } else if (family === ORG_FAMILY.VENUE) {
      sourcePath = CONTACT_SOURCE_PATH.VENUE_SALES;
      followup = CONTACT_FOLLOWUP_TYPE.SEARCH_FUNCTIONAL_CONTACT;
    } else if (family === ORG_FAMILY.UNIVERSITY) {
      sourcePath = CONTACT_SOURCE_PATH.DEPARTMENT_PAGE;
      followup = CONTACT_FOLLOWUP_TYPE.SEARCH_PROGRAM_OFFICE;
    } else {
      sourcePath = CONTACT_SOURCE_PATH.OFFICIAL_STAFF;
      followup = CONTACT_FOLLOWUP_TYPE.SEARCH_STAFF_DIRECTORY;
    }
    stop = STOP_CONTACT_RESEARCH.CONTINUE;
  } else {
    sourcePath = CONTACT_SOURCE_PATH.GENERAL_OFFICIAL_CONTACT;
    followup = CONTACT_FOLLOWUP_TYPE.SEARCH_FUNCTIONAL_CONTACT;
    stop = STOP_CONTACT_RESEARCH.CONTINUE;
  }

  // Watchlist / low-value: stop early
  if (/watch|monitoring|tbd|needs account/i.test(opportunity.title || "") && tier === CONTACT_TIER.NO_CONTACT) {
    // still try domain recovery once; do not force named
    namedWorth = NAMED_PERSON_WORTH.UNCERTAIN;
  }

  return {
    orgFamily: family,
    currentTier: tier,
    sourcePath,
    followupType: followup,
    namedPersonWorthPursuing: namedWorth,
    functionalPathSufficient: functionalSufficiency,
    stopContactResearch: stop,
    namedAlready: named,
  };
}

export function buildCandidateRecoveryUrls({
  domainState,
  orgFamily,
  sourcePath,
  maxUrls = 4,
} = {}) {
  const host = domainState?.host;
  const existing = domainState?.urls || [];
  if (!host && !existing.length) return [];

  const out = [...existing];
  if (host) {
    const base = `https://${host}`;
    const paths = contactPlaybookPaths(orgFamily);
    // Prefer paths matching sourcePath
    const prefer = [];
    if (sourcePath === CONTACT_SOURCE_PATH.HOUSING) {
      prefer.push(...paths.filter((p) => /hous|hotel|travel|lodg/i.test(p)));
    } else if (sourcePath === CONTACT_SOURCE_PATH.REGISTRATION) {
      prefer.push(...paths.filter((p) => /regist/i.test(p)));
    } else if (sourcePath === CONTACT_SOURCE_PATH.VENUE_SALES) {
      prefer.push(...paths.filter((p) => /event|group|sales|cater/i.test(p)));
    } else if (sourcePath === CONTACT_SOURCE_PATH.OFFICIAL_STAFF) {
      prefer.push(...paths.filter((p) => /staff|team|directory|people/i.test(p)));
    } else if (sourcePath === CONTACT_SOURCE_PATH.DEPARTMENT_PAGE) {
      prefer.push(...paths.filter((p) => /alumni|advancement|department|program|conference/i.test(p)));
    }
    const ordered = [...prefer, ...paths.filter((p) => !prefer.includes(p))];
    for (const p of ordered) {
      const u = `${base}${p}`;
      if (!out.includes(u)) out.push(u);
    }
  }
  return out.slice(0, maxUrls);
}

async function fetchHtml(url, timeoutMs = 20000) {
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, error: "no_url", fetchesUsed: 0, url };
  }
  try {
    const r = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(timeoutMs),
      headers: { "User-Agent": "DealalityGDI-ContactSourceRecovery/1.1" },
    });
    const html = await r.text();
    return {
      ok: r.status >= 200 && r.status < 400,
      status: r.status,
      finalUrl: r.url,
      html,
      fetchesUsed: 1,
      url,
      sourceType: classifyFetchedSourceType(r.url || url, html),
    };
  } catch (err) {
    return { ok: false, error: err.message || String(err), fetchesUsed: 1, url };
  }
}

export function classifyFetchedSourceType(url = "", html = "") {
  const blob = `${url} ${String(html).slice(0, 2000)}`.toLowerCase();
  if (/\.pdf($|\?)/i.test(url)) return "PDF_PROSPECTUS";
  if (/hous|hotel|accommodat|lodging|travel/.test(blob)) return "HOUSING";
  if (/registrat|rsvp|sign[- ]?up/.test(blob)) return "REGISTRATION";
  if (/staff|directory|our[- ]?team|leadership/.test(blob)) return "OFFICIAL_STAFF";
  if (/private[- ]?event|group[- ]?sales|banquet|catering/.test(blob)) return "VENUE_SALES";
  if (/alumni|advancement|department|program/.test(blob)) return "DEPARTMENT_PAGE";
  if (/contact/.test(blob)) return "OFFICIAL_CONTACT_PAGE";
  if (/event|meeting|conference|tournament/.test(blob)) return "EVENT_PROGRAM_PAGE";
  return "GENERAL_OFFICIAL";
}

/**
 * Optional network domain resolution via native-who domain resolver.
 * Fails soft when SerpAPI unavailable.
 */
export async function resolveOfficialDomainNetwork(opportunity = {}, opts = {}) {
  const maxQueries = opts.maxDomainQueries ?? DEFAULT_BUDGET.maxDomainQueries;
  try {
    const { resolveOfficialDomainsV7 } = await import(
      "./contact-candidate/native-who-v3/domain-resolver-v7.js"
    );
    const resolved = await resolveOfficialDomainsV7(
      {
        organization: opportunity.organizationName || opportunity.organization || opportunity.title,
        opportunityName: opportunity.title,
        knownOfficialUrl: opportunity.officialSource || opportunity.discoverySource || null,
      },
      { maxQueries }
    );
    const top = (resolved.domains || []).find(
      (d) => d?.host && isAcceptableOfficialUrl(d.seedUrl || `https://${d.host}`, opportunity)
    );
    if (!top?.host) {
      return {
        host: null,
        seedUrl: null,
        confidence: DOMAIN_CONFIDENCE.NO_OFFICIAL_DOMAIN,
        source: "network_empty_or_rejected",
        urls: [],
        queries: resolved.queries || [],
      };
    }
    const seedUrl = top.seedUrl || `https://${top.host}`;
    if (!isAcceptableOfficialUrl(seedUrl, opportunity)) {
      return {
        host: null,
        seedUrl: null,
        confidence: DOMAIN_CONFIDENCE.AMBIGUOUS,
        source: "network_rejected_brand_bleed",
        urls: [],
        queries: resolved.queries || [],
      };
    }
    const confidence =
      top.score >= 6
        ? DOMAIN_CONFIDENCE.OFFICIAL_CONFIRMED
        : top.score >= 3
          ? DOMAIN_CONFIDENCE.OFFICIAL_LIKELY
          : DOMAIN_CONFIDENCE.AMBIGUOUS;
    return {
      host: top.host,
      seedUrl,
      confidence,
      source: "network_resolver",
      urls: [seedUrl].filter(Boolean),
      queries: resolved.queries || [],
      domains: resolved.domains || [],
    };
  } catch (err) {
    return {
      host: null,
      seedUrl: null,
      confidence: DOMAIN_CONFIDENCE.NO_OFFICIAL_DOMAIN,
      source: "network_error",
      error: err.message || String(err),
      urls: [],
      queries: [],
    };
  }
}

function classifyRecoveryResult(beforeTier, afterTier, extraction) {
  if (beforeTier === afterTier) {
    return extraction?.hasContactClues ? RECOVERY_RESULT.NO_IMPROVEMENT : RECOVERY_RESULT.NO_IMPROVEMENT;
  }
  if (
    afterTier === CONTACT_TIER.NAMED_DIRECT ||
    afterTier === CONTACT_TIER.NAMED_PARTIAL
  ) {
    return RECOVERY_RESULT.NAMED_CONTACT_FOUND;
  }
  if (afterTier === CONTACT_TIER.FUNCTIONAL_CONTACT) {
    return RECOVERY_RESULT.FUNCTIONAL_CONTACT_FOUND;
  }
  if (
    beforeTier === CONTACT_TIER.NO_CONTACT &&
    afterTier === CONTACT_TIER.ORGANIZATION_PATH
  ) {
    return RECOVERY_RESULT.BETTER_SOURCE_FOUND;
  }
  return RECOVERY_RESULT.BETTER_SOURCE_FOUND;
}

/**
 * Run official source path recovery + dual contact extraction for one opportunity.
 *
 * @param {object} opportunity
 * @param {object} [opts]
 * @param {boolean} [opts.allowNetworkDomainResolution=true]
 * @param {object} [opts.budget]
 * @param {function} [opts.fetchPage] — injectable for tests
 */
export async function recoverOfficialContactSources(opportunity = {}, opts = {}) {
  const budget = { ...DEFAULT_BUDGET, ...(opts.budget || {}) };
  const fetchPage = opts.fetchPage || ((url) => fetchHtml(url, budget.fetchTimeoutMs));
  const beforeTier = classifyContactTier(opportunity);

  let domainState = resolveDomainFromOpportunity(opportunity);
  let domainQueries = 0;

  if (
    domainState.confidence === DOMAIN_CONFIDENCE.NO_OFFICIAL_DOMAIN &&
    opts.allowNetworkDomainResolution !== false &&
    (beforeTier === CONTACT_TIER.NO_CONTACT ||
      beforeTier === CONTACT_TIER.ORGANIZATION_PATH ||
      beforeTier === CONTACT_TIER.GENERIC_ONLY)
  ) {
    const net = await resolveOfficialDomainNetwork(opportunity, {
      maxDomainQueries: budget.maxDomainQueries,
    });
    domainQueries = (net.queries || []).length;
    if (net.host) domainState = { ...net, urls: net.urls || [] };
  }

  const gdiRoute = computeGdiContactRouting(opportunity, domainState);

  if (gdiRoute.stopContactResearch === STOP_CONTACT_RESEARCH.STOP_SUFFICIENT_PATH) {
    return {
      opportunity,
      beforeTier,
      afterTier: beforeTier,
      improved: false,
      domainState,
      gdiRoute,
      pagesFetched: [],
      sourceYield: [],
      result: RECOVERY_RESULT.STOPPED,
      metrics: {
        domainQueries,
        additionalFetches: 0,
        pagesWithClues: 0,
        namedPeople: 0,
        functionalContacts: 0,
        publicEmails: 0,
        publicPhones: 0,
      },
      extraction: null,
    };
  }

  const candidates = buildCandidateRecoveryUrls({
    domainState,
    orgFamily: gdiRoute.orgFamily,
    sourcePath: gdiRoute.sourcePath,
    maxUrls: budget.maxAdditionalFetches,
  }).filter((u) => isAcceptableOfficialUrl(u, opportunity));

  // Ambiguous / rejected domains: do not invent officialSource
  if (
    domainState.confidence === DOMAIN_CONFIDENCE.AMBIGUOUS ||
    domainState.confidence === DOMAIN_CONFIDENCE.NO_OFFICIAL_DOMAIN
  ) {
    return {
      opportunity,
      beforeTier,
      afterTier: beforeTier,
      improved: false,
      domainState,
      gdiRoute,
      pagesFetched: [],
      sourceYield: [],
      result:
        gdiRoute.stopContactResearch !== STOP_CONTACT_RESEARCH.CONTINUE
          ? RECOVERY_RESULT.STOPPED
          : RECOVERY_RESULT.NO_IMPROVEMENT,
      metrics: {
        domainQueries,
        additionalFetches: 0,
        pagesWithClues: 0,
        namedPeople: 0,
        functionalContacts: 0,
        publicEmails: 0,
        publicPhones: 0,
      },
      extraction: null,
      contactDataOrigin: CONTACT_DATA_ORIGIN.PUBLIC_SOURCE,
    };
  }

  const pagesFetched = [];
  const sourceYield = [];
  let bestExtraction = null;
  let working = { ...opportunity };
  let additionalFetches = 0;

  // Always re-extract from known URLs first (already-paid reuse when content fetched)
  for (const url of candidates) {
    if (additionalFetches >= budget.maxAdditionalFetches) break;
    const page = await fetchPage(url);
    additionalFetches += page.fetchesUsed || 0;
    pagesFetched.push({
      url,
      ok: page.ok,
      status: page.status,
      finalUrl: page.finalUrl || url,
      sourceType: page.sourceType || classifyFetchedSourceType(url),
      error: page.error || null,
    });
    if (!page.ok) continue;
    if (!isAcceptableOfficialUrl(page.finalUrl || url, opportunity)) continue;

    const extraction = extractContactIntelligenceFromSource({
      source: {
        url: page.finalUrl || url,
        sourceType: page.sourceType || "official_web",
      },
      pageContent: page.html || "",
      opportunityContext: working,
    });

    sourceYield.push({
      sourceType: page.sourceType || classifyFetchedSourceType(url),
      url: page.finalUrl || url,
      named: extraction.metrics?.namedPeople || 0,
      functional: extraction.metrics?.functionalContacts || 0,
      emails: extraction.metrics?.publicEmails || 0,
      phones: extraction.metrics?.publicPhones || 0,
      hasClues: Boolean(extraction.hasContactClues),
    });

    if (extraction.hasContactClues) {
      const applied = applySourceContactExtractionToOpportunity(working, extraction);
      // Never downgrade named → functional
      const nextTier = classifyContactTier(applied.opportunity);
      const prevRank = tierRank(classifyContactTier(working));
      const nextRank = tierRank(nextTier);
      if (nextRank >= prevRank) {
        working = {
          ...applied.opportunity,
          officialSource: working.officialSource || page.finalUrl || url,
          contactOfficialUrl:
            working.contactOfficialUrl ||
            extraction.contactPages?.[0]?.url ||
            page.finalUrl ||
            url,
          officialDomain: domainState.host || working.officialDomain || null,
          domainConfidence: domainState.confidence,
        };
        bestExtraction = extraction;
      }

      // Stop early on named or strong functional
      if (
        nextTier === CONTACT_TIER.NAMED_DIRECT ||
        nextTier === CONTACT_TIER.NAMED_PARTIAL ||
        (nextTier === CONTACT_TIER.FUNCTIONAL_CONTACT &&
          gdiRoute.namedPersonWorthPursuing !== NAMED_PERSON_WORTH.YES)
      ) {
        break;
      }
    } else if (!working.officialSource && page.ok) {
      // At least attach official path
      working = {
        ...working,
        officialSource: page.finalUrl || url,
        officialDomain: domainState.host || null,
        domainConfidence: domainState.confidence,
      };
    }
  }

  // If we resolved a domain but still no officialSource, set seed (quality-gated)
  if (
    !working.officialSource &&
    domainState.seedUrl &&
    isAcceptableOfficialUrl(domainState.seedUrl, opportunity)
  ) {
    working = {
      ...working,
      officialSource: domainState.seedUrl,
      officialDomain: domainState.host,
      domainConfidence: domainState.confidence,
    };
  }

  const afterTier = classifyContactTier(working);
  const tierImproved = tierRank(afterTier) > tierRank(beforeTier);
  const sourceAdded =
    !opportunity.officialSource &&
    Boolean(working.officialSource) &&
    isAcceptableOfficialUrl(working.officialSource, opportunity);
  // Path recovery counts as improvement when tier rises, or NO_CONTACT gains an official path
  const improved =
    tierImproved ||
    (beforeTier === CONTACT_TIER.NO_CONTACT && sourceAdded) ||
    (sourceAdded && Boolean(bestExtraction?.hasContactClues));

  const metrics = {
    domainQueries,
    additionalFetches,
    pagesWithClues: sourceYield.filter((s) => s.hasClues).length,
    namedPeople: sourceYield.reduce((n, s) => n + s.named, 0),
    functionalContacts: sourceYield.reduce((n, s) => n + s.functional, 0),
    publicEmails: sourceYield.reduce((n, s) => n + s.emails, 0),
    publicPhones: sourceYield.reduce((n, s) => n + s.phones, 0),
  };

  return {
    opportunity: working,
    beforeTier,
    afterTier,
    improved,
    domainState,
    gdiRoute,
    pagesFetched,
    sourceYield,
    result: improved
      ? classifyRecoveryResult(beforeTier, afterTier, bestExtraction)
      : gdiRoute.stopContactResearch !== STOP_CONTACT_RESEARCH.CONTINUE
        ? RECOVERY_RESULT.STOPPED
        : RECOVERY_RESULT.NO_IMPROVEMENT,
    metrics,
    extraction: bestExtraction,
    contactDataOrigin: CONTACT_DATA_ORIGIN.PUBLIC_SOURCE,
  };
}

function tierRank(tier) {
  const order = {
    [CONTACT_TIER.NO_CONTACT]: 0,
    [CONTACT_TIER.GENERIC_ONLY]: 1,
    [CONTACT_TIER.ORGANIZATION_PATH]: 2,
    [CONTACT_TIER.FUNCTIONAL_CONTACT]: 3,
    [CONTACT_TIER.NAMED_PARTIAL]: 4,
    [CONTACT_TIER.NAMED_DIRECT]: 5,
  };
  return order[tier] ?? 0;
}

export function summarizeWeakContactAudit(opportunities = []) {
  return opportunities.map((o) => {
    const tier = classifyContactTier(o);
    const domain = resolveDomainFromOpportunity(o);
    const route = computeGdiContactRouting(o, domain);
    return {
      opportunityId: o.id,
      title: o.title,
      organization: o.organizationName || o.organization || null,
      programOrEvent: o.title,
      currentTier: tier,
      currentContact: o.primaryContactName || o.primaryContact?.name || null,
      role: o.primaryContactRole || o.primaryContact?.role || null,
      officialUrls: pickKnownOfficialUrls(o),
      domainConfidence: domain.confidence,
      orgFamily: route.orgFamily,
      whyWeak:
        tier === CONTACT_TIER.NO_CONTACT
          ? "No usable public contact path; often missing official URL"
          : tier === CONTACT_TIER.ORGANIZATION_PATH
            ? "Organization/official path only — no named or functional inbox"
            : tier === CONTACT_TIER.FUNCTIONAL_CONTACT
              ? "Functional path present; named owner not confirmed"
              : "Named person without direct public reachability",
      nextBestPath: route.sourcePath,
      followupType: route.followupType,
      stop: route.stopContactResearch,
    };
  });
}
