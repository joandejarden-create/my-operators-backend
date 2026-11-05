/**
 * AVA Digital Authority Discovery V2
 *
 * Multi-strategy public backlink discovery with URL identity graph matching.
 * Uses existing approved Exa search when EXA_API_KEY is present.
 * Never invents backlinks. Never calls Ahrefs/Semrush without founder auth.
 */
import {
  DIGITAL_AUTHORITY_FINDING_CLASSES,
  scoreDigitalAuthorityOpportunity,
  emptyDigitalAuthorityBlock,
  finalizeDigitalAuthorityBlock,
} from "./digital-authority-v1.js";
import {
  buildPropertyUrlIdentityGraph,
  classifyDestinationAgainstGraph,
  hostOf,
  isBrandCorporateHomepage,
  normalizeUrlForMatch,
  PROPERTY_URL_CLASSES,
} from "./property-url-identity-graph-v1.js";

export const DIGITAL_AUTHORITY_V2_CONTRACT = "AVA_DIGITAL_AUTHORITY_V2";

export const RESEARCH_COVERAGE_STATUSES = Object.freeze({
  RESEARCH_SUFFICIENT_NO_ADDITIONAL_LINKS_VERIFIED:
    "RESEARCH_SUFFICIENT_NO_ADDITIONAL_LINKS_VERIFIED",
  RESEARCH_INCOMPLETE: "RESEARCH_INCOMPLETE",
  ACCESS_RESTRICTED: "ACCESS_RESTRICTED",
  PROVIDER_COVERAGE_LIMITATION: "PROVIDER_COVERAGE_LIMITATION",
});

const SKIP_REFERRER_HOSTS = new Set([
  "google.com",
  "bing.com",
  "duckduckgo.com",
  "yahoo.com",
  "facebook.com",
  "instagram.com",
  "twitter.com",
  "x.com",
  "linkedin.com",
  "youtube.com",
  "booking.com",
  "expedia.com",
  "hotels.com",
  "tripadvisor.com",
  "kayak.com",
  "trivago.com",
]);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export async function fetchHtml(url, { timeoutMs = 20000 } = {}) {
  const started = Date.now();
  try {
    const res = await fetch(url, {
      redirect: "follow",
      headers: {
        "User-Agent":
          "DealalityAvaDigitalAuthority/2.0 (+https://www.dealality.com; read-only research)",
        Accept: "text/html,application/xhtml+xml,*/*",
      },
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    return {
      ok: res.ok,
      status: res.status,
      finalUrl: res.url,
      body: text,
      elapsedMs: Date.now() - started,
      error: null,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      finalUrl: url,
      body: "",
      elapsedMs: Date.now() - started,
      error: String(err?.message || err),
    };
  }
}

/** Extract absolute hrefs + anchor text from HTML. */
export function extractAnchors(html, baseUrl) {
  const out = [];
  const re = /<a\b([^>]*?)>([\s\S]*?)<\/a>/gi;
  let m;
  while ((m = re.exec(html || ""))) {
    const attrs = m[1] || "";
    const inner = String(m[2] || "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    const hrefMatch = attrs.match(/\bhref\s*=\s*["']([^"']+)["']/i);
    if (!hrefMatch) continue;
    let href = hrefMatch[1].trim();
    if (!href || href.startsWith("#") || href.startsWith("mailto:") || href.startsWith("tel:")) {
      continue;
    }
    try {
      href = new URL(href, baseUrl).href;
    } catch {
      continue;
    }
    out.push({ href, anchorText: inner.slice(0, 200) });
  }
  return out;
}

export async function exaSearch(query, { numResults = 8, apiKey } = {}) {
  const key = apiKey || process.env.EXA_API_KEY;
  if (!key) {
    return { ok: false, error: "EXA_API_KEY_MISSING", results: [], query };
  }
  const res = await fetch("https://api.exa.ai/search", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": key,
    },
    body: JSON.stringify({
      query,
      type: "auto",
      numResults,
      contents: {
        text: { maxCharacters: 500 },
        highlights: true,
      },
    }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    return {
      ok: false,
      error: `EXA_HTTP_${res.status}`,
      body: body.slice(0, 400),
      results: [],
      query,
    };
  }
  const data = await res.json();
  return {
    ok: true,
    query,
    results: (data.results || []).map((r) => ({
      url: r.url,
      title: r.title,
      score: r.score,
      text: r.text || (r.highlights || []).join(" "),
    })),
  };
}

export function buildDiscoveryQueries({ hotelName, graph, competitors = [], demandSegments = [] }) {
  const queries = [];
  const hosts = (graph?.urls || [])
    .filter((u) =>
      [
        PROPERTY_URL_CLASSES.HOTEL_OWNED_SITE,
        PROPERTY_URL_CLASSES.BRAND_PROPERTY_PAGE,
        PROPERTY_URL_CLASSES.CANONICAL_PROPERTY_URL,
        PROPERTY_URL_CLASSES.ALTERNATE_VERIFIED_URL,
      ].includes(u.urlClass)
    )
    .map((u) => u.host)
    .filter(Boolean);
  const uniqueHosts = [...new Set(hosts)];

  // Strategy A — direct backlink-oriented
  for (const h of uniqueHosts.slice(0, 3)) {
    queries.push({
      strategy: "A_DIRECT_HOTEL_BACKLINKS",
      query: `"${h}" (directory OR tourism OR "visitor bureau" OR CVB OR guide OR marina OR golf OR spa OR meetings)`,
    });
    queries.push({
      strategy: "A_DIRECT_HOTEL_BACKLINKS",
      query: `link to ${h} hotel OR resort OR marriott OR hilton OR curio`,
    });
  }

  // Strategy B — hotel mentions
  queries.push({
    strategy: "B_HOTEL_MENTIONS",
    query: `"${hotelName}" (official site OR website OR marriott.com OR hilton.com OR booking)`,
  });
  queries.push({
    strategy: "B_HOTEL_MENTIONS",
    query: `"${hotelName}" (tourism OR destination OR "things to do" OR meetings OR wedding OR golf)`,
  });

  // Strategy C — competitor backlinks
  for (const c of competitors.slice(0, 5)) {
    const name = typeof c === "string" ? c : c.name;
    if (!name) continue;
    queries.push({
      strategy: "C_COMPETITOR_BACKLINKS",
      query: `"${name}" (official website OR marriott.com/hotels OR hilton.com OR tourism directory)`,
      competitorName: name,
    });
  }

  // Strategy D — destination / industry
  const destHints = demandSegments.length
    ? demandSegments.join(" OR ")
    : "tourism OR CVB OR marina OR golf OR spa OR meetings OR wedding";
  queries.push({
    strategy: "D_INDUSTRY_DESTINATION",
    query: `"${hotelName}" (${destHints})`,
  });

  return queries;
}

function shouldSkipReferrer(url) {
  const h = hostOf(url);
  if (!h) return true;
  if (SKIP_REFERRER_HOSTS.has(h)) return true;
  if ([...SKIP_REFERRER_HOSTS].some((s) => h.endsWith(`.${s}`))) return true;
  if (/exa\.ai|datahc\.com|brands\.datahc/i.test(h)) return true;
  return false;
}

const BRAND_FAMILY_HOST_RE =
  /(fiestamericana|fiestainntravel|fiestaamericanatravelty|posadas\.com|marriott\.com|hilton\.com|achmhotels|sheraton\.marriott)/i;

/** Third-party backlinks only — exclude brand-family self-links and tool hosts. */
export function isThirdPartyPropertyBacklink(linkingPageUrl, hotelDestinationUrl) {
  const ref = hostOf(linkingPageUrl);
  const dest = hostOf(hotelDestinationUrl || "");
  if (!ref || !dest) return false;
  if (ref === dest) return false;
  if (/exa\.ai|datahc\.com/i.test(ref) || /datahc\.com/i.test(hotelDestinationUrl || "")) {
    return false;
  }
  if (BRAND_FAMILY_HOST_RE.test(ref) && BRAND_FAMILY_HOST_RE.test(dest)) {
    return false;
  }
  return true;
}

/**
 * Inspect a candidate page for property-matching outbound links.
 */
export async function inspectCandidatePage(pageUrl, graph, meta = {}) {
  if (shouldSkipReferrer(pageUrl)) {
    return { inspected: false, skipped: true, reason: "SKIP_REFERRER_HOST" };
  }
  const page = await fetchHtml(pageUrl);
  if (!page.ok) {
    return {
      inspected: true,
      accessible: false,
      status: page.status,
      error: page.error,
      findings: [],
    };
  }
  const anchors = extractAnchors(page.body, page.finalUrl || pageUrl);
  const findings = [];
  const matchedDests = new Set();

  for (const a of anchors) {
    if (isBrandCorporateHomepage(a.href)) continue;
    const cls = classifyDestinationAgainstGraph(a.href, graph);
    if (!cls.match) continue;
    const key = `${normalizeUrlForMatch(pageUrl)}|${normalizeUrlForMatch(a.href)}`;
    if (matchedDests.has(key)) continue;
    matchedDests.add(key);
    findings.push({
      findingClass: DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_BACKLINK,
      referringOrganization: meta.organization || hostOf(pageUrl),
      linkingPageUrl: page.finalUrl || pageUrl,
      hotelDestinationUrl: a.href,
      destinationUrlClass: cls.urlClass,
      anchorText: a.anchorText,
      hyperlinkToOfficialSite: true,
      verificationMethod: "HTTP_HTML_ANCHOR_EXTRACT",
      verificationDate: new Date().toISOString().slice(0, 10),
      evidenceUrl: page.finalUrl || pageUrl,
      commercialRelevance: meta.commercialRelevance || null,
      strategy: meta.strategy || null,
    });
  }

  // Mention without property link
  const hotelName = graph?.hotelName || "";
  const textHit =
    hotelName &&
    new RegExp(
      hotelName
        .split(/\s+/)
        .slice(0, 3)
        .map((w) => w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
        .join(".{0,40}"),
      "i"
    ).test(page.body);

  if (!findings.length && textHit) {
    findings.push({
      findingClass: DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_EXTERNAL_MENTION,
      referringOrganization: meta.organization || hostOf(pageUrl),
      linkingPageUrl: page.finalUrl || pageUrl,
      hotelDestinationUrl: null,
      hyperlinkToOfficialSite: false,
      verificationMethod: "HTTP_HTML_TEXT_MENTION",
      verificationDate: new Date().toISOString().slice(0, 10),
      evidenceUrl: page.finalUrl || pageUrl,
      note: "Hotel mentioned in page text; no property-specific hyperlink confirmed.",
      strategy: meta.strategy || null,
    });
  }

  return {
    inspected: true,
    accessible: true,
    status: page.status,
    finalUrl: page.finalUrl,
    anchorCount: anchors.length,
    findings,
  };
}

/**
 * Run bounded multi-strategy discovery for one hotel.
 */
export async function runDigitalAuthorityDiscoveryV2(input = {}) {
  const {
    payload,
    identityExtras = {},
    competitors = [],
    demandSegments = [],
    maxSearches = 12,
    maxPagesInspected = 28,
    delayMs = 350,
    paidResearchAuthorized = false,
  } = input;

  if (paidResearchAuthorized) {
    // Explicitly unused in this public V2 path
  }

  const hotelName = payload?.property?.name || identityExtras.hotelName;
  const propertyId = payload?.property?.propertyId;
  const graph = buildPropertyUrlIdentityGraph(payload, identityExtras);
  const coverage = {
    searchesAttempted: 0,
    searchesCompleted: 0,
    strategiesCompleted: new Set(),
    candidatePagesDiscovered: 0,
    candidatePagesInspected: 0,
    referringDomainsEvaluated: new Set(),
    verifiedBacklinks: 0,
    verifiedCompetitorBacklinks: 0,
    verifiedExternalMentions: 0,
    potentialOpportunities: 0,
    inaccessiblePages: 0,
    exaErrors: [],
  };

  const sources = [];
  const competitorBacklinks = [];
  const competitorGaps = [];
  const opportunities = [];
  const searchLog = [];
  const seenPages = new Set();
  const seenSourceKeys = new Set();

  const pushSource = (f) => {
    const key = `${f.findingClass}|${normalizeUrlForMatch(f.linkingPageUrl)}|${normalizeUrlForMatch(
      f.hotelDestinationUrl || ""
    )}`;
    if (seenSourceKeys.has(key)) return;
    seenSourceKeys.add(key);
    // Never classify first-party as external mention/backlink from third party
    if (
      f.findingClass === DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_EXTERNAL_MENTION ||
      f.findingClass === DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_BACKLINK
    ) {
      const refHost = hostOf(f.linkingPageUrl);
      const destHosts = new Set((graph.urls || []).map((u) => u.host));
      if (destHosts.has(refHost)) {
        return; // first-party page
      }
    }
    sources.push(f);
    if (f.findingClass === DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_BACKLINK) {
      if (!isThirdPartyPropertyBacklink(f.linkingPageUrl, f.hotelDestinationUrl)) {
        f.findingClass =
          DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_EXTERNAL_MENTION;
        f.hyperlinkToOfficialSite = false;
        f.note =
          (f.note ? `${f.note} ` : "") +
          "Reclassified: first-party brand-family or low-quality referrer is not a third-party property backlink.";
        coverage.verifiedExternalMentions++;
        sources.push(f);
        return;
      }
      coverage.verifiedBacklinks++;
    }
    if (f.findingClass === DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_EXTERNAL_MENTION) {
      coverage.verifiedExternalMentions++;
    }
  };

  const queries = buildDiscoveryQueries({
    hotelName,
    graph,
    competitors,
    demandSegments,
  }).slice(0, maxSearches);

  for (const q of queries) {
    coverage.searchesAttempted++;
    const search = await exaSearch(q.query, { numResults: 8 });
    searchLog.push({
      strategy: q.strategy,
      query: q.query,
      ok: search.ok,
      resultCount: search.results?.length || 0,
      error: search.error || null,
      competitorName: q.competitorName || null,
    });
    if (!search.ok) {
      coverage.exaErrors.push({ query: q.query, error: search.error });
      await sleep(delayMs);
      continue;
    }
    coverage.searchesCompleted++;
    coverage.strategiesCompleted.add(q.strategy);

    for (const r of search.results || []) {
      if (!r.url || seenPages.has(normalizeUrlForMatch(r.url))) continue;
      if (shouldSkipReferrer(r.url)) continue;
      // Skip property's own pages as referrers
      const refHost = hostOf(r.url);
      if ((graph.urls || []).some((u) => u.host === refHost)) continue;

      seenPages.add(normalizeUrlForMatch(r.url));
      coverage.candidatePagesDiscovered++;

      if (coverage.candidatePagesInspected >= maxPagesInspected) continue;

      const inspection = await inspectCandidatePage(r.url, graph, {
        strategy: q.strategy,
        organization: r.title || hostOf(r.url),
        commercialRelevance: q.strategy,
      });
      coverage.candidatePagesInspected++;
      coverage.referringDomainsEvaluated.add(hostOf(r.url));

      if (inspection.accessible === false) coverage.inaccessiblePages++;
      for (const f of inspection.findings || []) {
        if (q.strategy === "C_COMPETITOR_BACKLINKS" && q.competitorName) {
          // For competitor strategy: if page links to competitor destination, record separately
          // Property matches still go to sources; competitor link detection needs competitor graph
          if (f.findingClass === DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_BACKLINK) {
            pushSource(f);
          } else if (f.findingClass === DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_EXTERNAL_MENTION) {
            // Competitor mention pages are context only unless they link to subject
            pushSource(f);
          }
        } else {
          pushSource(f);
        }
      }
      await sleep(delayMs);
    }
    await sleep(delayMs);
  }

  // Strategy E — gap candidates from competitor backlinks discovered as opportunities
  // When we have mentions without links on high-credibility destination hosts → opportunity
  for (const s of sources) {
    if (
      s.findingClass === DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_EXTERNAL_MENTION &&
      /cvb|tourism|visit-|visitpalma|convention|destination|golf|marina/i.test(
        s.linkingPageUrl || ""
      )
    ) {
      opportunities.push({
        opportunityId: `da2-opp-${hostOf(s.linkingPageUrl)}-${coverage.potentialOpportunities}`,
        title: `Request official property URL on ${s.referringOrganization || hostOf(s.linkingPageUrl)}`,
        organization: s.referringOrganization || hostOf(s.linkingPageUrl),
        findingClass: DIGITAL_AUTHORITY_FINDING_CLASSES.POTENTIAL_BACKLINK_OPPORTUNITY,
        targetOrganizationUrl: s.linkingPageUrl,
        whyItMatters:
          "This source already references the hotel. Adding a correct property-specific hyperlink strengthens destination discovery without inventing new directories.",
        recommendedAction:
          "Submit the approved brand property URL and/or hotel-operated website for inclusion as the official link.",
        scoreFactors: {
          demandRelevance: 8,
          sourceCredibility: 8,
          competitiveDifferentiation: 6,
          implementationLikelihood: 7,
          discoveryValue: 7,
        },
        evidenceLinks: [{ label: "Referring page", url: s.linkingPageUrl }],
      });
      coverage.potentialOpportunities++;
    }
  }

  for (const o of opportunities) {
    o.priorityScore = scoreDigitalAuthorityOpportunity(o.scoreFactors);
  }

  const strategiesDone = [...coverage.strategiesCompleted];
  let researchCoverageStatus =
    RESEARCH_COVERAGE_STATUSES.RESEARCH_SUFFICIENT_NO_ADDITIONAL_LINKS_VERIFIED;
  if (coverage.searchesCompleted < Math.min(6, queries.length) || coverage.exaErrors.length > 3) {
    researchCoverageStatus = RESEARCH_COVERAGE_STATUSES.RESEARCH_INCOMPLETE;
  } else if (coverage.inaccessiblePages > coverage.candidatePagesInspected * 0.5) {
    researchCoverageStatus = RESEARCH_COVERAGE_STATUSES.ACCESS_RESTRICTED;
  } else if (coverage.verifiedBacklinks === 0 && coverage.candidatePagesInspected >= 8) {
    researchCoverageStatus =
      RESEARCH_COVERAGE_STATUSES.RESEARCH_SUFFICIENT_NO_ADDITIONAL_LINKS_VERIFIED;
  }

  const verifiedBacklinks = sources.filter(
    (s) => s.findingClass === DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_BACKLINK
  );
  const mentions = sources.filter(
    (s) => s.findingClass === DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_EXTERNAL_MENTION
  );

  const executiveFinding = verifiedBacklinks.length
    ? `Public V2 discovery verified ${verifiedBacklinks.length} property-specific third-party hyperlink(s) and ${mentions.length} external reference(s) without inventing paid-provider inventory.`
    : `Public V2 discovery completed ${coverage.searchesCompleted} searches and inspected ${coverage.candidatePagesInspected} candidate pages without verifying additional property-specific third-party hyperlinks beyond prior known destination listings. Mentions without links remain classified separately.`;

  let block = {
    ...emptyDigitalAuthorityBlock({
      propertyId,
      hotelName,
      website: payload?.property?.website,
    }),
    contract: DIGITAL_AUTHORITY_V2_CONTRACT,
    researchMode: "PUBLIC_PARTIAL_V2",
    paidProviderUsed: false,
    paidResearchUsd: 0,
    inventoryCompleteness: `Public multi-strategy discovery V2 — ${researchCoverageStatus}. Not a comprehensive Ahrefs/Semrush inventory.`,
    executiveFinding,
    competitiveComparison:
      competitorBacklinks.length > 0
        ? `Verified ${competitorBacklinks.length} competitor backlink example(s) in this public pass.`
        : "Competitor website-backlink superiority was not established from public HTML in this pass where peer outbound links were not confirmed.",
    sources,
    competitorBacklinks,
    competitorGaps,
    opportunities: opportunities.slice(0, 8),
    actions: [],
    propertyUrlIdentityGraph: graph,
    discoveryCoverage: {
      ...coverage,
      strategiesCompleted: strategiesDone,
      referringDomainsEvaluated: [...coverage.referringDomainsEvaluated],
      researchCoverageStatus,
      searchLog,
    },
    methodologyNotes: [
      ...(emptyDigitalAuthorityBlock({}).methodologyNotes || []),
      "V2 uses Exa public search + direct HTML href inspection against a property URL identity graph.",
      "Brand corporate homepages are not counted as property-specific backlink destinations.",
      "Hotel-operated and brand property URLs are tracked separately.",
    ],
  };

  // Build one high-priority action when opportunities exist
  if (opportunities.length) {
    const top = opportunities.sort((a, b) => (b.priorityScore || 0) - (a.priorityScore || 0))[0];
    block.actions = [
      {
        actionId: `AVA-${String(propertyId || "HOTEL")
          .replace(/^adp_/, "")
          .slice(0, 8)
          .toUpperCase()
          .replace(/[^A-Z0-9]/g, "")}-DA2-001`,
        headline: top.title,
        priority: "HIGH",
        priorityWhy: top.whyItMatters,
        whatWeFound: executiveFinding,
        whyItMatters: top.whyItMatters,
        recommendedSteps: [
          top.recommendedAction,
          "Confirm brand vs hotel-operated URL for the listing.",
          "Archive before/after evidence of the live hyperlink.",
        ],
        accountableOwner: "Hotel Sales & Marketing",
        supportingParties: "Brand content / destination partnership owner",
        suggestedTimeframe: "Submit within 30 days of fact pack approval",
        claimType: "OPTIONAL_COMMERCIAL_OPPORTUNITY",
        evidenceLinks: top.evidenceLinks || [],
      },
    ];
  }

  block = finalizeDigitalAuthorityBlock(block, { paidResearchAuthorized: false });
  block.contract = DIGITAL_AUTHORITY_V2_CONTRACT;

  return {
    contract: DIGITAL_AUTHORITY_V2_CONTRACT,
    digitalAuthority: block,
    propertyUrlIdentityGraph: graph,
    coverage: block.discoveryCoverage,
    paidResearchUsd: 0,
  };
}
