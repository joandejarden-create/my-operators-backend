import { ownershipQueries, ownershipSearchResults, rankOwnershipSources, ownershipDocumentPassages, ownershipCandidates } from "./ownership-research-planning.js";
/**
 * Ownership → contact research handoff (evaluation staging).
 *
 * Gap fixed: Contact Intelligence V1.3/V1.4 filtered hotels with
 * return_to_ownership_lane=true (and stopped after OWNER_DOMAIN_UNRESOLVED)
 * without invoking further research. This module continues inside the existing
 * SerpAPI + Context.dev + discoverOwnerPersonPath architecture.
 *
 * Staging only — does not mutate HOTEL_TO_OWNER / Census canonical owner fields.
 */

import {
  contextDevSearch,
  contextDevScrapeMarkdown,
  contextDevExtract,
  OWNERSHIP_CONTACT_EXTRACT_SCHEMA,
  isContextDevConfigured,
} from "../../context-dev/client.js";
import {
  createContextDevCreditLedger,
  CONTEXT_DEV_CREDIT_COSTS,
} from "../../context-dev/credit-ledger.js";
import {
  discoverOwnerPersonPath,
  verifyOwnershipPath,
} from "./owner-person-discovery.js";
import { serpGoogle } from "./live-native-discovery.js";
import {
  ATTRIBUTION,
  CHANNEL_KIND,
  PROPERTY_RELEVANCE,
  USAGE_RIGHTS,
  ROLE_CURRENCY,
} from "./vocabulary.js";
import { createChannel, createEvidenceRef, createPersonContact } from "./contact-record.js";
import { classifyLeadershipCategory, LEADERSHIP_CATEGORY } from "./leadership-extraction.js";
import { validateLinkedInIdentifier } from "./provider-submission-gate.js";
import { gateProviderCandidates } from "./fullenrich-gated-submit.js";
import { extractContactsFromHtml } from "./live-native-discovery.js";

export const OWNERSHIP_CONTACT_HANDOFF_VERSION = "ownership-contact-research-handoff-v1";

const ROLE_PRIORITY_RE =
  /development|acquisition|invest|founder|ceo|chief|president|chairman|director|cfo|asset|owner|comprador|invers|desarrollo|managing/i;

function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return "";
  }
}

function asHttps(url) {
  const u = String(url || "").trim();
  if (!u) return null;
  if (u.startsWith("http")) return u;
  return `https://${u}`;
}

/** discoverOwnerPersonPath returns { url, evidence } — normalize to string URL. */
function unwrapDomain(value) {
  if (!value) return null;
  if (typeof value === "string") return asHttps(value);
  if (typeof value === "object" && value.url) return asHttps(value.url);
  return null;
}

/** Third-party directories / data vendors — leads only, never owner company domain. */
const DIRECTORY_DOMAIN_HOSTS = new Set([
  "emis.com",
  "zoominfo.com",
  "crunchbase.com",
  "bloomberg.com",
  "dnb.com",
  "opencorporates.com",
  "signalhire.com",
  "rocketreach.co",
  "apollo.io",
  "linkedin.com",
  "facebook.com",
  "instagram.com",
  "tripadvisor.com",
  "booking.com",
  "travellife.ca",
  "wikipedia.org",
  "whoistheownerof.com",
  "whoownsthebrand.com",
  "ownershipdata.com",
  "marriott.com",
  "ihg.com",
  "hilton.com",
  "hcareers.com",
  "indeed.com",
  "glassdoor.com",
  "panoramaturisticomex.com.mx",
]);

function isDirectoryOrAggregatorHost(host) {
  const h = String(host || "").toLowerCase();
  if (!h) return true;
  if (DIRECTORY_DOMAIN_HOSTS.has(h)) return true;
  if ([...DIRECTORY_DOMAIN_HOSTS].some((d) => h === d || h.endsWith(`.${d}`))) return true;
  return false;
}

function stripHonorificOwnerName(name) {
  return String(name || "")
    .replace(/^(?:Englishman|British\s+businessman|businessman|Mr\.?|Mrs\.?|Ms\.?)\s+/i, "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 120);
}

/** Reject scrape/snippet chrome mistaken for owner legal names. */
function isImplausibleOwnerDisplayName(name) {
  const n = String(name || "").trim();
  if (n.length < 4 || n.length > 90) return true;
  if (/^(of|the|a|an|and|for|to|in|on|by)\b/i.test(n)) return true;
  if (/ownership\s+database|who\s+owns|verified\s+company|biggest\s+brands|cookie|privacy\s+policy/i.test(n)) {
    return true;
  }
  if ((n.match(/[A-Z]/g) || []).length > 20 && !/\s/.test(n)) return true;
  return false;
}

function isLowQualityDomain(url) {
  const h = hostOf(url);
  if (!h) return true;
  if (
    /careers|jobs|hcareers|indeed|glassdoor|linkedin|facebook|twitter|wikipedia|tripadvisor|booking\.com|travellife|top100influential|panorama/i.test(
      h
    )
  ) {
    return true;
  }
  if (isDirectoryOrAggregatorHost(h)) return true;
  if (/\.gob\.mx$|\.gov$|\.gov\./i.test(h)) return true;
  if (/linkedin\.com|facebook\.com|twitter\.com|wikipedia\.org/i.test(h)) return true;
  if (/\/transparencia\/|\/licencia\/|descarga\/L\//i.test(url)) return true;
  if (/\.pdf($|\?)/i.test(url)) return true;
  return false;
}

/**
 * Detect similarly-named wrong org from extract/scrape text
 * (e.g. Morocco car rental vs Mexico hotel owner).
 */
export function assessOrgDomainMatch({
  ownerDisplayName,
  pageCompanyName,
  pageText,
  host,
} = {}) {
  const owner = String(ownerDisplayName || "").toLowerCase();
  const company = String(pageCompanyName || "").toLowerCase();
  const blob = String(pageText || "").toLowerCase();
  const h = String(host || "").toLowerCase();

  if (isDirectoryOrAggregatorHost(h)) {
    return {
      ok: false,
      reason: "DIRECTORY_NOT_FIRST_PARTY_OWNER_DOMAIN",
      detail: h,
    };
  }

  const carRental =
    /location de voiture|car rental|agence de location|rent[- ]a[- ]car|alquiler de autos/i.test(
      `${company} ${blob}`
    );
  const moroccoOnly =
    /\bagadir\b|\bmorocco\b|\bmaroc\b|\+212\b/i.test(`${company} ${blob}`) &&
    !/canc[uú]n|guadalajara|m[eé]xico|mexico|hospitality management|real inn|hotel management/i.test(
      `${company} ${blob}`
    );
  if (carRental || (moroccoOnly && /alliance/i.test(owner))) {
    return {
      ok: false,
      reason: "REJECTED_SIMILARLY_NAMED_ORGANIZATION",
      detail: carRental ? "car_rental_not_hotel_owner" : "geography_business_mismatch",
    };
  }

  // Alliance Mexico hospitality must not accept generic "Alliance HM" without hospitality/Mexico signal
  if (/alliance/i.test(owner) && /alliance/i.test(`${company} ${h}`)) {
    const hospitalityOk =
      /hospitality|hotel management|real inn|canc[uú]n|guadalajara|m[eé]xico|mexico|llc/i.test(
        `${company} ${blob}`
      );
    if (!hospitalityOk && (carRental || moroccoOnly || /location|voiture|agadir/i.test(blob))) {
      return {
        ok: false,
        reason: "REJECTED_SIMILARLY_NAMED_ORGANIZATION",
        detail: "alliance_name_without_hospitality_mexico_signal",
      };
    }
  }

  // Host should share an owner token when we claim a first-party company domain.
  // Blocks job boards / unrelated sites that merely mention the org name in copy.
  const ownerTok = String(ownerDisplayName || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 5 && !["management", "hospitality", "company", "group", "hotels", "hotel"].includes(t));
  const hostCompact = h.replace(/[^a-z0-9]/g, "");
  const hostHasOwnerToken =
    ownerTok.length === 0 ||
    ownerTok.some((t) => hostCompact.includes(t) || h.includes(t));
  const pageNamesOwnerStrongly =
    /alliance hospitality management|inmobiliaria hnf/i.test(`${company} ${blob}`) &&
    !/careers|jobs|hiring|neuron|hcareers/i.test(`${company} ${blob} ${h}`);
  if (!hostHasOwnerToken && !pageNamesOwnerStrongly) {
    return {
      ok: false,
      reason: "REJECTED_HOST_DOES_NOT_MATCH_OWNER",
      detail: `host=${h}; owner_tokens=${ownerTok.join(",")}`,
    };
  }

  return { ok: true, reason: null };
}

async function chargedContext(ledger, kind, cost, meta, fn) {
  const gate = ledger.charge(kind, cost, meta);
  if (!gate.ok) {
    return { ok: false, budget_blocked: true, error: gate, data: null };
  }
  const result = await fn();
  // Validation / client errors are not billed by Context.dev — refund local ledger.
  if (
    !result.ok &&
    ((result.error?.class === "VALIDATION_OR_CLIENT" && result.error?.status == null) ||
      result.error?.error_code === "INPUT_VALIDATION_ERROR" ||
      result.error?.key_metadata?.credits_consumed === 0)
  ) {
    ledger.charge(`${kind}_refund`, -Number(cost), {
      ...meta,
      refund: true,
      reason: result.error?.class || result.error?.message || "validation_no_charge",
    });
  }
  return { ...result, budget_blocked: false, credit_entry: gate.entry };
}

function pickSearchUrls(results = [], { excludeHosts = [], preferTokens = [] } = {}) {
  const out = [];
  for (const r of results) {
    const url = r.url || r.link;
    if (!url || !/^https?:\/\//i.test(url)) continue;
    const h = hostOf(url);
    if (excludeHosts.some((x) => h === x || h.endsWith(`.${x}`))) continue;
    if (/linkedin\.com\/(posts|pulse)/i.test(url)) continue;
    let score = 0;
    const blob = `${r.title || ""} ${r.snippet || r.description || ""} ${url}`.toLowerCase();
    for (const t of preferTokens) {
      if (t && blob.includes(String(t).toLowerCase())) score += 2;
    }
    if (/about|contact|team|leadership|investor|owner|adquiere|adquis|compra/i.test(blob)) score += 1;
    out.push({ url, title: r.title || null, snippet: r.snippet || r.description || null, score, host: h });
  }
  return out.sort((a, b) => b.score - a.score);
}

function executiveToPerson(ex, { ownerId, ownerName, hotelName, sourceUrl, newlyResearched }) {
  const title = ex.title || "";
  const cat = classifyLeadershipCategory(title);
  const functionally = ROLE_PRIORITY_RE.test(title) || cat === LEADERSHIP_CATEGORY.DEVELOPMENT_ASSET;
  const channels = [];
  if (ex.linkedin_url && /linkedin\.com\/in\//i.test(ex.linkedin_url)) {
    channels.push(
      createChannel({
        kind: CHANNEL_KIND.PERSON_LINKEDIN,
        value: ex.linkedin_url,
        display_label: "Professional profile",
        attribution: ATTRIBUTION.NAMED_PERSON,
        usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
        evidence: [
          createEvidenceRef({
            source_title: "Research extract / corroboration",
            source_url: sourceUrl || null,
            source_type: "live_research",
            observed_at: new Date().toISOString(),
          }),
        ],
      })
    );
  }
  return createPersonContact({
    display_name: ex.full_name || ex.display_name,
    title,
    organization_entity_id: ownerId,
    organization_name: ownerName,
    role_currency: ROLE_CURRENCY.UNKNOWN,
    property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
    channels,
    evidence: [
      createEvidenceRef({
        source_title: "Live ownership/contact research extract",
        source_url: sourceUrl || ex.source_page_url || null,
        source_type: "live_research",
        observed_at: new Date().toISOString(),
        excerpt: (ex.biography_excerpt || ex.claim || title || "").slice(0, 400),
      }),
    ],
    why_relevant: functionally
      ? `Commercially relevant title (${title}) at ${ownerName} for ${hotelName}.`
      : `Named at ${ownerName}; commercial relevance of role not fully confirmed.`,
    publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
    provenance: {
      evidenced_or_inferred: "EVIDENCED",
      basis: "live_research_extract",
      newly_researched: Boolean(newlyResearched),
      functionally_relevant: functionally,
      leadership_category: cat,
      evaluation_staging: true,
    },
  });
}

/**
 * Continue research for one hotel case within allotted budgets.
 */
export async function researchHotelOwnershipContactPath(caseInput = {}, budgets = {}) {
  const started = Date.now();
  const hotel = caseInput.hotel || {};
  const serpBudget = {
    max: Number(budgets.serpapi_max ?? 10),
    used: 0,
    usd: 0,
  };
  const ledger = createContextDevCreditLedger({
    budgetCredits: Number(budgets.context_dev_max ?? 20),
    label: `handoff:${hotel.hotel_id}`,
  });
  const calls = [];
  const unresolved = [];
  const sources = [];
  const hotelSiteChannels = [];
  const newly = {
    ownership_claims: [],
    domain: null,
    people: [],
    notes: [],
  };

  const excludeHosts = [
    ...(caseInput.forbidden_org_hosts || []),
    "marriott.com",
    "ihg.com",
    "hilton.com",
    "hyatt.com",
    "booking.com",
    "tripadvisor.com",
    "facebook.com",
    "instagram.com",
  ];

  let ownership = caseInput.ownership_seed || {
    owner_entity_id: caseInput.owner_entity_id || null,
    owner_display_name: caseInput.owner_display_name || null,
    owner_role: caseInput.owner_role || null,
    classification: caseInput.classification || "UNRESOLVED",
    operator_name: caseInput.operator_name || null,
    brand_name: caseInput.brand_name || null,
    evidence_note: null,
    confidence: null,
    return_to_ownership_lane: !caseInput.owner_entity_id,
    ownership_vs_operator: null,
  };

  // ——— Phase A: ownership resolution when surface missing ———
  // Prefer founder-supplied inspect URLs before open search (resume / lead follow-through).
  const inspectUrls = Array.isArray(caseInput.inspect_urls) ? caseInput.inspect_urls.filter(Boolean) : [];
  for (const rawUrl of inspectUrls.slice(0, 4)) {
    if (!isContextDevConfigured() || !ledger.canAfford(CONTEXT_DEV_CREDIT_COSTS.scrape_markdown)) break;
    const url = asHttps(rawUrl);
    if (!url || excludeHosts.some((h) => hostOf(url) === h || hostOf(url).endsWith(`.${h}`))) continue;
    const scraped = await chargedContext(
      ledger,
      "scrape_markdown",
      CONTEXT_DEV_CREDIT_COSTS.scrape_markdown,
      { url, phase: "inspect_seed" },
      () => contextDevScrapeMarkdown({ url })
    );
    calls.push({ provider: "context_dev", kind: "inspect_seed_scrape", url, ok: scraped.ok });
    if (!scraped.ok) continue;
    const md = String(
      typeof scraped.data === "string" ? scraped.data : scraped.data?.markdown || scraped.data?.content || ""
    );
    sources.push({
      kind: "inspect_seed",
      url,
      excerpt: md.slice(0, 1500),
      observed_at: new Date().toISOString(),
    });
    const claimBlob = md.slice(0, 4000);
    const ownedBy = claimBlob.match(
      /(?:owned by|adquirid[oa] por|propiedad de|purchased by|acquired by|sold to|owner[:\s]+)\s*([A-Z][^.\n|]{3,80})/i
    );
    const bought = claimBlob.match(
      /(?:Englishman|businessman)?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z'’-]+){1,3})\s+bought\s+(?:the\s+)?(?:property|hotel|resort)/i
    );
    const match = ownedBy || bought;
    if (match && !ownership.owner_display_name) {
      const stagedName = stripHonorificOwnerName(match[1]);
      if (isImplausibleOwnerDisplayName(stagedName) || /whoistheownerof|whoownsthebrand/i.test(url)) {
        newly.notes.push(`rejected_implausible_owner_name:${stagedName}:${url}`);
      } else {
      ownership = {
        ...ownership,
        owner_display_name: stagedName,
        owner_entity_id: ownership.owner_entity_id || `staged_${hotel.hotel_id}`,
        owner_role: "UNRESOLVED",
        classification: "OWNER_CANDIDATE",
        confidence: "PROBABLE",
        evidence_note: `Staged from inspect seed ${url} — historical purchase ≠ current ownership until corroborated; not canonical write.`,
        return_to_ownership_lane: true,
        ownership_vs_operator: "OWNER_CLAIM_STAGED_OPERATOR_NOT_ASSUMED",
        historical_vs_current: bought ? "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK" : "UNKNOWN",
        newly_researched: true,
      };
      newly.notes.push(`staged_owner_from_inspect_seed:${url}`);
      }
    }
    // Capture year/date cues for historical vs current assessment
    const yearHit = claimBlob.match(
      /(?:bought|acquired|purchased|opened|founded)[^.]*?\b(19\d{2}|20[0-2]\d)\b/i
    );
    if (yearHit) {
      newly.notes.push(`ownership_claim_year_cue:${yearHit[1]}:${url}`);
      ownership.claim_year_cue = yearHit[1];
    }
    if (/as owner and chairman|remains (?:the )?owner|still owned by|current owner/i.test(claimBlob)) {
      ownership.historical_vs_current = "CURRENT_OWNERSHIP_LANGUAGE_PRESENT";
      newly.notes.push(`current_ownership_language:${url}`);
    }
    // Hotel first-party pages → corporate/form/office fallback (not owner domain by default)
    const hotelHosts = new Set(
      [
        caseInput.hotel_site_host,
        hotel.hotel_name?.toLowerCase().includes("blue waters") ? "bluewaters.net" : null,
        hotel.hotel_name?.toLowerCase().includes("copper") ? "copperandlumberhotel.com" : null,
        hotel.hotel_name?.toLowerCase().includes("hammock") ? "hammockcove.com" : null,
      ].filter(Boolean)
    );
    if (hotelHosts.has(hostOf(url)) || /contact|about|privacy|legal/i.test(url)) {
      const extracted = extractContactsFromHtml(md, url, { hotelName: hotel.hotel_name });
      for (const em of (extracted.emails || []).slice(0, 3)) {
        hotelSiteChannels.push(
          createChannel({
            kind: CHANNEL_KIND.ORG_EMAIL,
            value: em,
            display_label: "Hotel/site corporate email (not proven owner-personal)",
            attribution: ATTRIBUTION.ORGANIZATION,
            property_relevance: PROPERTY_RELEVANCE.PROPERTY,
            usage_rights: USAGE_RIGHTS.PUBLIC_SAFE,
            evidence: [
              createEvidenceRef({
                source_title: "Hotel first-party page",
                source_url: url,
                source_type: "first_party_or_corp",
                observed_at: new Date().toISOString(),
              }),
            ],
            customer_caveat: "Property/operator contact route — do not treat as evidenced owner personal email.",
          })
        );
      }
      for (const ph of (extracted.phones || []).slice(0, 2)) {
        hotelSiteChannels.push(
          createChannel({
            kind: CHANNEL_KIND.SWITCHBOARD,
            value: ph,
            display_label: "Hotel/site switchboard",
            attribution: ATTRIBUTION.ORGANIZATION,
            property_relevance: PROPERTY_RELEVANCE.PROPERTY,
            usage_rights: USAGE_RIGHTS.PUBLIC_SAFE,
            evidence: [
              createEvidenceRef({
                source_title: "Hotel first-party page",
                source_url: url,
                source_type: "first_party_or_corp",
                observed_at: new Date().toISOString(),
              }),
            ],
          })
        );
      }
      if (!hotelSiteChannels.some((c) => c.kind === CHANNEL_KIND.ORG_WEBSITE) && hotelHosts.has(hostOf(url))) {
        hotelSiteChannels.push(
          createChannel({
            kind: CHANNEL_KIND.ORG_WEBSITE,
            value: `https://${hostOf(url)}/`,
            display_label: "Hotel first-party website (operator/brand surface — not owner domain by default)",
            attribution: ATTRIBUTION.ORGANIZATION,
            property_relevance: PROPERTY_RELEVANCE.PROPERTY,
            usage_rights: USAGE_RIGHTS.PUBLIC_SAFE,
          })
        );
      }
    }
  }

  for (const eq of (caseInput.extra_serp_queries || []).slice(0, 4)) {
    if (serpBudget.used >= serpBudget.max) break;
    const cost = { serpapi_searches: 0, serpapi_usd: 0 };
    const serp = await serpGoogle(eq, cost, {
      hl: hotel.language === "en" ? "en" : "es",
      gl: hotel.language === "en" ? "us" : "mx",
      num: 8,
    });
    serpBudget.used += cost.serpapi_searches;
    serpBudget.usd += cost.serpapi_usd;
    calls.push({
      provider: "serpapi",
      kind: "extra_follow_up",
      query: eq,
      results: serp.organic?.length || 0,
    });
    sources.push({
      kind: "serp_extra_follow_up",
      query: eq,
      organic: (serp.organic || []).slice(0, 8),
    });
    newly.ownership_claims.push(
      ...(serp.organic || []).slice(0, 5).map((r) => ({
        title: r.title,
        url: r.link || r.url,
        snippet: r.snippet,
        provider: "serpapi",
        follow_up: true,
      }))
    );
  }

  if (!ownership.owner_entity_id || ownership.return_to_ownership_lane) {
    const q = `"${hotel.hotel_name}" (owner OR owned OR acquired OR acquisition OR propietario OR adquirió OR compra) -tripadvisor -booking`;
    if (serpBudget.used < serpBudget.max) {
      const cost = { serpapi_searches: 0, serpapi_usd: 0 };
      const serp = await serpGoogle(q, cost, {
        hl: hotel.language === "pt" ? "pt" : hotel.language === "en" ? "en" : "es",
        gl: hotel.country === "Brazil" ? "br" : hotel.language === "en" ? "us" : "mx",
        num: 10,
      });
      serpBudget.used += cost.serpapi_searches;
      serpBudget.usd += cost.serpapi_usd;
      calls.push({ provider: "serpapi", kind: "ownership_search", query: q, results: serp.organic?.length || 0 });
      sources.push({ kind: "serp_ownership", query: q, organic: (serp.organic || []).slice(0, 8) });
      newly.ownership_claims.push(
        ...(serp.organic || []).slice(0, 5).map((r) => ({
          title: r.title,
          url: r.link || r.url,
          snippet: r.snippet,
          provider: "serpapi",
        }))
      );
      // Stage owner candidates from SERP snippets (leads only — corroborate via scrape/extract).
      if (!ownership.owner_display_name) {
        for (const r of serp.organic || []) {
          const blob = `${r.title || ""} ${r.snippet || ""}`;
          const m = blob.match(
            /(?:owned by|sold to|purchased by|acquired by)\s+(?:British businessman\s+|businessman\s+|Englishman\s+)?([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z'’-]+){0,3})/
          );
          const bought =
            !m &&
            blob.match(
              /(?:Englishman|businessman)?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z'’-]+){1,3})\s+bought\s+(?:the\s+)?(?:property|hotel|resort)/i
            );
          const match = m || bought;
          if (match) {
            const name = match[1].trim().replace(/\s+/g, " ").slice(0, 120);
            if (name.length >= 5 && !/^(in|the|a|an|and|for|to)\b/i.test(name)) {
              ownership = {
                ...ownership,
                owner_display_name: stripHonorificOwnerName(name),
                owner_entity_id: ownership.owner_entity_id || `staged_${hotel.hotel_id}`,
                owner_role: "UNRESOLVED",
                classification: "OWNER_CANDIDATE",
                confidence: "PROBABLE",
                evidence_note: `Staged from SerpAPI ownership snippet (${r.link || r.url}) — historical purchase language may not equal current ownership; not canonical write.`,
                return_to_ownership_lane: true,
                ownership_vs_operator: "OWNER_CLAIM_STAGED_OPERATOR_NOT_ASSUMED",
                historical_vs_current: bought ? "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK" : "UNKNOWN",
                newly_researched: true,
              };
              newly.notes.push(`staged_owner_from_serp_snippet:${r.link || r.url}`);
              break;
            }
          }
        }
      }
    }

    const visitedOwnershipUrls = new Set(inspectUrls);
    for (const cq of ownershipQueries(hotel)) {
      if (!isContextDevConfigured() || !ledger.canAfford(ledger.estimateSearchCost(10) + CONTEXT_DEV_CREDIT_COSTS.scrape_markdown)) break;
      const search = await chargedContext(ledger, "search", ledger.estimateSearchCost(10), { query: cq }, () => contextDevSearch({ query: cq, numResults: 10 }));
      const mapped = search.ok ? ownershipSearchResults(search.data) : null;
      calls.push({ provider: "context_dev", kind: "search", query: cq, ok: search.ok, result_count: mapped?.length ?? null, error: search.error || (!mapped ? "UNEXPECTED_SEARCH_RESPONSE_SHAPE" : null) });
      if (!mapped) { unresolved.push(search.ok ? "UNEXPECTED_SEARCH_RESPONSE_SHAPE" : "CONTEXT_SEARCH_FAILED"); continue; }
      sources.push({ kind: "context_ownership_search", query: cq, results: mapped });
      newly.ownership_claims.push(...mapped.map((r) => ({ ...r, evidence_level: "SEARCH_LEAD" })));
      // Brand/manager press may report an actual buyer; exclude it as an OWNER DOMAIN only.
      const ranked = rankOwnershipSources(mapped, hotel).filter((r) => !visitedOwnershipUrls.has(r.url));
      for (const u of ranked.slice(0, 2)) {
        if (!ledger.canAfford(CONTEXT_DEV_CREDIT_COSTS.scrape_markdown)) break;
        visitedOwnershipUrls.add(u.url);
        const scraped = await chargedContext(ledger, "scrape_markdown", CONTEXT_DEV_CREDIT_COSTS.scrape_markdown, {url:u.url}, () => contextDevScrapeMarkdown({url:u.url}));
        const md = typeof scraped.data === "string" ? scraped.data : scraped.data?.markdown || scraped.data?.content || "";
        calls.push({provider:"context_dev",kind:"scrape",url:u.url,ok:scraped.ok,characters:String(md).length});
        if (!scraped.ok || !md) { unresolved.push("OWNERSHIP_DOCUMENT_UNREADABLE"); continue; }
        const document = ownershipDocumentPassages(md, hotel);
        sources.push({kind:"context_scrape_ownership",url:u.url,...document,observed_at:new Date().toISOString()});
        for (const candidate of ownershipCandidates(document)) {
          if (isImplausibleOwnerDisplayName(candidate.name)) continue;
          newly.ownership_claims.push({...candidate,url:u.url,evidence_level:"DOCUMENT_CANDIDATE"});
          if (!ownership.owner_display_name) ownership = {
            ...ownership, owner_display_name:candidate.name,
            owner_entity_id: `staged_${hotel.hotel_id}`, classification:"OWNER_CANDIDATE", owner_role:"UNRESOLVED",
            historical_vs_current:"UNRESOLVED", return_to_ownership_lane:true,
            evidence_refs:[{source_url:u.url,excerpt:candidate.excerpt,source_type:"RETRIEVED_DOCUMENT"}],
            evidence_note:"Document candidate requires relationship and currency adjudication; not canonical ownership.",newly_researched:true
          };
        }
      }
      // A candidate still needs adjudication; follow-up queries remain bounded by the same ledger.
    }

    if (!ownership.owner_display_name) {
      unresolved.push("OWNER_STILL_UNRESOLVED_AFTER_RESEARCH");
    }
  } else {
    ownership = {
      ...ownership,
      ownership_vs_operator:
        ownership.ownership_vs_operator ||
        "SURFACE_ECONOMIC_OWNER_PRESENT_OPERATOR_BRAND_SEPARATED_IN_FIXTURES",
      newly_researched: false,
    };
  }

  // ——— Phase B: domain + people via existing discoverOwnerPersonPath (SerpAPI) ———
  // Skip entirely when serpapi_max=0 — Phase C Context.dev continues domain/people research.
  let domainResult = null;
  if (ownership.owner_display_name && serpBudget.max > 0 && serpBudget.used < serpBudget.max) {
    const remainingSerp = Math.max(1, Math.min(5, serpBudget.max - serpBudget.used));
    domainResult = await discoverOwnerPersonPath({
      owner_entity_id: ownership.owner_entity_id,
      owner_display_name: ownership.owner_display_name,
      hotels: [hotel],
      focus_hotel_id: hotel.hotel_id,
      focus_hotel_name: hotel.hotel_name,
      domain_hypotheses: caseInput.domain_hypotheses || [],
      person_hypotheses: caseInput.person_hypotheses || [],
      forbidden_org_hosts: caseInput.forbidden_org_hosts || [],
      max_serp_queries: remainingSerp,
    });
    serpBudget.used += Number(domainResult.cost?.serpapi_searches || 0);
    serpBudget.usd += Number(domainResult.cost?.serpapi_usd || 0);
    calls.push({
      provider: "serpapi+fetch",
      kind: "discoverOwnerPersonPath",
      cost: domainResult.cost,
      unresolved: domainResult.unresolved_reasons,
      domain: domainResult.confirmed_company_domain,
    });
    if (domainResult.confirmed_company_domain) {
      const raw = unwrapDomain(domainResult.confirmed_company_domain);
      if (raw && !isLowQualityDomain(raw)) {
        newly.domain = {
          url: raw,
          host: hostOf(raw),
          from: "discoverOwnerPersonPath",
          newly_researched: true,
          evidence: domainResult.confirmed_company_domain?.evidence || null,
        };
      } else if (raw) {
        newly.notes.push(`rejected_low_quality_domain:${raw}`);
        unresolved.push(
          isDirectoryOrAggregatorHost(hostOf(raw))
            ? "DIRECTORY_NOT_FIRST_PARTY_OWNER_DOMAIN"
            : "OWNER_DOMAIN_REJECTED_LOW_QUALITY"
        );
      }
    }
  } else if (ownership.owner_display_name && serpBudget.max <= 0) {
    calls.push({
      provider: "serpapi",
      kind: "discoverOwnerPersonPath_skipped",
      reason: "serpapi_max_0_use_context_dev_phase_c",
    });
  }

  let confirmedDomain = newly.domain?.url || null;
  let people = [...(domainResult?.people || [])];

  // ——— Phase C: Context.dev continuation when domain/people still weak ———
  if (ownership.owner_display_name && isContextDevConfigured()) {
    if (!confirmedDomain && ledger.canAfford(ledger.estimateSearchCost(10))) {
      const dq =
        caseInput.domain_search_query ||
        `"${ownership.owner_display_name}" (official website OR "about us" OR contacto OR "quiénes somos")`;
      const search = await chargedContext(ledger, "search", ledger.estimateSearchCost(10), { query: dq }, () =>
        contextDevSearch({
          query: dq,
          numResults: 10,
          excludeDomains: excludeHosts,
        })
      );
      calls.push({ provider: "context_dev", kind: "domain_search", ok: search.ok, query: dq });
      if (search.ok) {
        const candidates = pickSearchUrls(search.data?.results || [], {
          excludeHosts,
          preferTokens: String(ownership.owner_display_name)
            .split(/\s+/)
            .filter((w) => w.length > 3)
            .slice(0, 4),
        });
        for (const c of candidates.slice(0, 3)) {
          if (!ledger.canAfford(CONTEXT_DEV_CREDIT_COSTS.scrape_markdown)) break;
          if (/linkedin\.com/i.test(c.url)) continue;
          const scraped = await chargedContext(
            ledger,
            "scrape_markdown",
            CONTEXT_DEV_CREDIT_COSTS.scrape_markdown,
            { url: c.url },
            () => contextDevScrapeMarkdown({ url: c.url })
          );
          calls.push({ provider: "context_dev", kind: "domain_scrape", url: c.url, ok: scraped.ok });
          if (!scraped.ok) continue;
          const md = String(
            typeof scraped.data === "string"
              ? scraped.data
              : scraped.data?.markdown || scraped.data?.content || ""
          );
          const nameBits = String(ownership.owner_display_name)
            .toLowerCase()
            .split(/\s+/)
            .filter((w) => w.length > 3);
          const hit = nameBits.filter((b) => md.toLowerCase().includes(b)).length;
          if (hit >= Math.min(2, nameBits.length) || /alliance hospitality|inmobiliaria hnf|blue waters/i.test(md)) {
            if (isLowQualityDomain(c.url)) {
              newly.notes.push(`rejected_low_quality_domain_candidate:${c.url}`);
              continue;
            }
            // Press/article pages mentioning the owner are leads, not company domains.
            if (/travellife|tripadvisor|booking\.com|wikipedia|medium\.com|linkedin\.com/i.test(c.host)) {
              newly.notes.push(`rejected_press_or_directory_as_owner_domain:${c.url}`);
              continue;
            }
            const orgMatch = assessOrgDomainMatch({
              ownerDisplayName: ownership.owner_display_name,
              pageCompanyName: c.title,
              pageText: md.slice(0, 3000),
              host: c.host,
            });
            if (!orgMatch.ok) {
              newly.notes.push(`domain_candidate_org_mismatch:${orgMatch.reason}:${c.url}`);
              newly.rejected_domains = newly.rejected_domains || [];
              newly.rejected_domains.push({ host: c.host, url: c.url, reason: orgMatch.reason });
              continue;
            }
            confirmedDomain = asHttps(`https://${c.host}/`);
            newly.domain = {
              url: confirmedDomain,
              host: c.host,
              from: "context_dev_scrape_corroboration",
              newly_researched: true,
              title: c.title,
              source_page: c.url,
            };
            sources.push({
              kind: "context_domain_confirm",
              url: confirmedDomain,
              source_page: c.url,
              excerpt: md.slice(0, 800),
              observed_at: new Date().toISOString(),
            });
            break;
          }
        }
      }
    }

    if (confirmedDomain && !isLowQualityDomain(confirmedDomain) && ledger.canAfford(CONTEXT_DEV_CREDIT_COSTS.extract)) {
      const root = `https://${hostOf(confirmedDomain)}/`;
      if (!hostOf(confirmedDomain)) {
        unresolved.push("DOMAIN_HOST_PARSE_FAILED");
      } else {
      const extracted = await chargedContext(
        ledger,
        "extract",
        CONTEXT_DEV_CREDIT_COSTS.extract,
        { url: root },
        () =>
          contextDevExtract({
            url: root,
            schema: OWNERSHIP_CONTACT_EXTRACT_SCHEMA,
            maxPages: 4,
            maxDepth: 2,
            factCheck: true,
            instructions: `Extract the organization identity for ${ownership.owner_display_name}. Distinguish owner vs operator vs brand. List current executives with titles. Prefer development, acquisitions, investment, founder, CEO. Do not invent emails. Note similarly named organizations that are NOT this entity.`,
          })
      );
      calls.push({
        provider: "context_dev",
        kind: "extract",
        url: root,
        ok: extracted.ok,
        error: extracted.error || null,
      });
      if (extracted.ok) {
        const data = extracted.data?.data || extracted.data?.result || extracted.data || {};
        const companyName = data.company_legal_or_trade_name || null;
        const claimText = [
          companyName,
          ...(data.ownership_or_portfolio_claims || []).map((c) =>
            typeof c === "string" ? c : c?.claim || c?.text || JSON.stringify(c)
          ),
          ...(data.executives || []).map((e) => `${e.full_name || ""} ${e.title || ""}`),
        ].join(" ");
        const orgMatch = assessOrgDomainMatch({
          ownerDisplayName: ownership.owner_display_name,
          pageCompanyName: companyName,
          pageText: claimText,
          host: hostOf(confirmedDomain),
        });
        sources.push({
          kind: "context_extract",
          url: root,
          company: companyName,
          executives: (data.executives || []).slice(0, 8),
          ownership_claims: (data.ownership_or_portfolio_claims || []).slice(0, 6),
          org_domain_match: orgMatch,
          observed_at: new Date().toISOString(),
        });
        if (!orgMatch.ok) {
          newly.notes.push(`domain_rejected_after_extract:${orgMatch.reason}:${hostOf(confirmedDomain)}`);
          unresolved.push(orgMatch.reason);
          newly.rejected_domains = newly.rejected_domains || [];
          newly.rejected_domains.push({
            host: hostOf(confirmedDomain),
            url: confirmedDomain,
            reason: orgMatch.reason,
            detail: orgMatch.detail,
          });
          confirmedDomain = null;
          newly.domain = null;
        } else {
          for (const ex of data.executives || []) {
            if (!ex.full_name) continue;
            const person = executiveToPerson(ex, {
              ownerId: ownership.owner_entity_id,
              ownerName: ownership.owner_display_name,
              hotelName: hotel.hotel_name,
              sourceUrl: ex.source_page_url || root,
              newlyResearched: true,
            });
            people.push(person);
            newly.people.push({
              name: person.display_name,
              title: person.title,
              newly_researched: true,
            });
          }
          if (companyName && !ownership.owner_legal_name) {
            ownership.owner_legal_name = companyName;
          }
        }
      } else if (extracted.budget_blocked) {
        unresolved.push("CONTEXT_DEV_BUDGET_EXHAUSTED_EXTRACT");
      } else {
        unresolved.push(`CONTEXT_DEV_EXTRACT_FAILED:${extracted.error?.class || extracted.error?.message || "unknown"}`);
      }
      } // end hostOf else
    }

    // Promote independently confirmed LinkedIn hypotheses even when Serp budget is exhausted.
    for (const hp of (caseInput.person_hypotheses || []).slice(0, 4)) {
      if (!hp.display_name || hp.deceased || hp.former_affiliation) {
        if (hp.deceased || hp.former_affiliation) {
          newly.notes.push(`person_hypothesis_excluded_deceased_or_former:${hp.display_name}`);
        }
        continue;
      }
      if (!(hp.linkedin && /linkedin\.com\/in\//i.test(hp.linkedin))) continue;
      if (!hp.affiliation_evidence?.length) { newly.notes.push(`hypothesis_needs_captured_affiliation:${hp.display_name}`); continue; }
      const liGate = validateLinkedInIdentifier({
        personName: hp.display_name,
        linkedinUrl: hp.linkedin,
        orgName: ownership.owner_display_name,
        roleTitle: hp.title,
        independentlyConfirmed: true,
        evidenceNote: hp.why_relevant_hypothesis || ownership.owner_display_name,
        profileCompany: ownership.owner_display_name,
      });
      if (liGate.decision !== "ALLOW") {
        newly.notes.push(`linkedin_hypothesis_omitted:${hp.display_name}:${liGate.reason || liGate.detail}`);
        continue;
      }
      let existing = people.find(
        (p) => String(p.display_name).toLowerCase() === String(hp.display_name).toLowerCase()
      );
      if (!existing) {
        existing = createPersonContact({
          display_name: hp.display_name,
          title: hp.title || null,
          organization_entity_id: ownership.owner_entity_id,
          organization_name: ownership.owner_display_name,
          role_currency: ROLE_CURRENCY.UNKNOWN,
          property_relevance: PROPERTY_RELEVANCE.OWNER_ORG,
          channels: [],
          evidence: [
            createEvidenceRef({
              source_title: "Independently confirmed professional identity",
              source_url: hp.linkedin,
              source_type: "live_research",
              observed_at: new Date().toISOString(),
              excerpt: (hp.why_relevant_hypothesis || "").slice(0, 400),
            }),
          ],
          why_relevant: hp.why_relevant_hypothesis || `Evidenced principal for ${ownership.owner_display_name}`,
          publication_label: "EVIDENCED_ORG_PERSON_CANDIDATE",
          provenance: {
            evidenced_or_inferred: "EVIDENCED",
            basis: "independently_confirmed_linkedin_plus_owner_hypothesis",
            newly_researched: true,
            evaluation_staging: true,
          },
        });
        people.push(existing);
        newly.people.push({
          name: existing.display_name,
          title: existing.title,
          newly_researched: true,
          chain: "person_without_corporate_website",
        });
      } else {
        existing.publication_label = "EVIDENCED_ORG_PERSON_CANDIDATE";
        existing.provenance = {
          ...(existing.provenance || {}),
          evidenced_or_inferred: "EVIDENCED",
          basis: "independently_confirmed_linkedin_plus_owner_hypothesis",
          newly_researched: true,
          evaluation_staging: true,
        };
        newly.people.push({
          name: existing.display_name,
          title: existing.title,
          newly_researched: true,
          chain: "promoted_hypothesis_via_confirmed_linkedin",
        });
      }
      const liCh = (existing.channels || []).find((c) => /LINKEDIN/i.test(c.kind));
      if (!liCh) {
        existing.channels = existing.channels || [];
        existing.channels.push(
          createChannel({
            kind: CHANNEL_KIND.PERSON_LINKEDIN,
            value: hp.linkedin,
            display_label: "Independently confirmed professional profile",
            attribution: ATTRIBUTION.NAMED_PERSON,
            usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
          })
        );
      } else {
        liCh.value = hp.linkedin;
        liCh.display_label = "Independently confirmed professional profile";
      }
    }

    for (const hp of (caseInput.person_hypotheses || []).slice(0, 2)) {
      if (serpBudget.used >= serpBudget.max) break;
      if (!hp.display_name || hp.deceased || hp.former_affiliation) continue;
      const lqOwner =
        String(ownership.owner_display_name || "")
          .replace(/\s*\/\s*owner path/i, "")
          .replace(/\s+/g, " ")
          .trim() || ownership.owner_display_name;
      const lq = `"${hp.display_name}" "${lqOwner}" site:linkedin.com/in`;
      const cost = { serpapi_searches: 0, serpapi_usd: 0 };
      const serp = await serpGoogle(lq, cost, { hl: "en", gl: "us", num: 5 });
      serpBudget.used += cost.serpapi_searches;
      serpBudget.usd += cost.serpapi_usd;
      calls.push({ provider: "serpapi", kind: "linkedin_corroboration", query: lq, results: serp.organic?.length || 0 });
      // Only accept a Serp LinkedIn hit that passes name + org context gate — never substitute another person.
      let acceptedUrl = null;
      for (const hit of serp.organic || []) {
        const url = hit.link || hit.url;
        if (!url || !/linkedin\.com\/in\//i.test(url)) continue;
        const liGate = validateLinkedInIdentifier({
          personName: hp.display_name,
          linkedinUrl: url,
          orgName: ownership.owner_display_name,
          roleTitle: hp.title,
          profileHeadline: hit.title || hit.snippet || null,
          independentlyConfirmed: false,
          evidenceNote: `${hit.title || ""} ${hit.snippet || ""}`,
        });
        if (liGate.decision === "ALLOW") {
          acceptedUrl = url;
          break;
        }
        newly.notes.push(`linkedin_serp_omitted:${hp.display_name}:${liGate.reason || liGate.detail}`);
      }
      if (!acceptedUrl) continue;
      const url = acceptedUrl;
      const existing = people.find(
        (p) => String(p.display_name).toLowerCase() === String(hp.display_name).toLowerCase()
      );
      if (existing) {
        const hasLi = (existing.channels || []).some((c) => /LINKEDIN/i.test(c.kind));
        if (!hasLi) {
          existing.channels = existing.channels || [];
          existing.channels.push(
            createChannel({
              kind: CHANNEL_KIND.PERSON_LINKEDIN,
              value: url,
              display_label: "Professional profile (Serp corroboration, gated)",
              attribution: ATTRIBUTION.NAMED_PERSON,
              usage_rights: USAGE_RIGHTS.INTERNAL_ONLY,
            })
          );
        }
        if (existing.publication_label === "HYPOTHESIS_PENDING_CORROBORATION") {
          newly.notes.push(`linkedin_serp_does_not_auto_promote_hypothesis:${hp.display_name}`);
        }
      } else {
        newly.notes.push(`linkedin_serp_no_existing_person_row_omitted_create:${hp.display_name}`);
      }
      sources.push({
        kind: "linkedin_serp_corroboration",
        person: hp.display_name,
        url,
        gated: true,
        observed_at: new Date().toISOString(),
      });
    }
  } else if (!isContextDevConfigured()) {
    unresolved.push("CONTEXT_DEV_API_KEY_MISSING");
  }

  const byName = new Map();
  for (const p of people) {
    const key = String(p.display_name || "")
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "");
    if (!key) continue;
    const prev = byName.get(key);
    const score = (x) =>
      (x.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE" ? 4 : 0) +
      (x.provenance?.functionally_relevant ? 2 : 0) +
      (ROLE_PRIORITY_RE.test(x.title || "") ? 2 : 0);
    if (!prev || score(p) > score(prev)) byName.set(key, p);
  }
  const peopleOut = [...byName.values()]
    .filter((p) => !p.deceased && !p.former_affiliation && !p.provenance?.deceased)
    .sort((a, b) => Number(ROLE_PRIORITY_RE.test(b.title || "")) - Number(ROLE_PRIORITY_RE.test(a.title || "")))
    .slice(0, 5);

  const rejectedHosts = (newly.rejected_domains || []).map((d) => d.host).filter(Boolean);
  if (!confirmedDomain) unresolved.push("OWNER_DOMAIN_UNRESOLVED");
  if (!peopleOut.filter((p) => p.publication_label !== "HYPOTHESIS_PENDING_CORROBORATION").length) {
    unresolved.push("NO_EVIDENCED_RELEVANT_PERSON");
  }

  const domainHost = confirmedDomain ? hostOf(confirmedDomain) : null;
  const evidencedPeople = peopleOut.filter(
    (p) =>
      p.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE" &&
      p.display_name &&
      !p.deceased &&
      !p.former_affiliation
  );
  const hasIndependentlyConfirmedLinkedIn = evidencedPeople.some((p) =>
    (p.channels || []).some(
      (c) =>
        /LINKEDIN/i.test(c.kind || "") &&
        /linkedin\.com\/in\//i.test(c.value || "") &&
        /independently confirmed/i.test(c.display_label || "")
    )
  );
  // Domain optional when LinkedIn-only alternative chain has independently confirmed LI.
  const qualifies =
    evidencedPeople.length > 0 &&
    (Boolean(domainHost) || hasIndependentlyConfirmedLinkedIn) &&
    !(domainHost && rejectedHosts.includes(domainHost));

  return {
    version: OWNERSHIP_CONTACT_HANDOFF_VERSION,
    hotel_id: hotel.hotel_id,
    hotel_name: hotel.hotel_name,
    elapsed_ms: Date.now() - started,
    ownership: {
      ...ownership,
      staged_only: true,
      canonical_owner_write: false,
    },
    confirmed_company_domain: confirmedDomain,
    confirmed_company_domain_host: domainHost,
    rejected_domains: newly.rejected_domains || [],
    people: peopleOut,
    organization_contact_route: (() => {
      const base = domainResult?.organization_contact_route || null;
      const merged = [...(base?.channels || []), ...hotelSiteChannels];
      if (!merged.length) return base;
      return {
        ...(base || {}),
        channels: merged,
        note: base?.note || "Includes hotel-site corporate fallbacks when present; not owner-personal emails.",
      };
    })(),
    newly_researched: newly,
    previously_cached_used: {
      owner_from_surface: Boolean(caseInput.owner_entity_id && !ownership.newly_researched),
      person_hypotheses: (caseInput.person_hypotheses || []).map((p) => p.display_name),
    },
    sources,
    calls,
    budgets: {
      context_dev: ledger.snapshot(),
      serpapi: { used: serpBudget.used, max: serpBudget.max, usd: Number(serpBudget.usd.toFixed(4)) },
    },
    unresolved_reasons: [...new Set(unresolved)],
    qualifies_for_fullenrich: qualifies,
    alternative_chain_no_corporate_website: Boolean(
      qualifies && !domainHost && hasIndependentlyConfirmedLinkedIn
    ),
    write_guarantees: {
      canonical_hotel_to_owner_mutations: 0,
      census_owner_mutations: 0,
      evaluation_staging_allowed: true,
      customer_publication: "BLOCKED",
    },
  };
}

export function buildEnrichmentSubjectsFromResearch(researchRows, { maxPeople = 10, maxPerOwner = 2 } = {}) {
  const subjects = [];
  const rejected = [];
  const perOwner = new Map();
  for (const row of researchRows) {
    if (!row.qualifies_for_fullenrich && !row.alternative_chain_no_corporate_website) {
      // still allow alternative-chain rows flagged qualifies
    }
    if (!row.qualifies_for_fullenrich) continue;
    const ownerKey = row.ownership?.owner_entity_id || row.confirmed_company_domain_host || row.hotel_id;
    const people = (row.people || []).filter(
      (p) =>
        p.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE" &&
        !p.deceased &&
        !p.former_affiliation
    );
    const ordered = [
      ...people.filter((p) => ROLE_PRIORITY_RE.test(p.title || "")),
      ...people.filter((p) => !ROLE_PRIORITY_RE.test(p.title || "")),
    ];
    const rejectedDomainHosts = [
      ...((row.rejected_domains || []).map((d) => d.host || d)),
      "alliancehm.com",
      "emis.com",
    ].filter(Boolean);
    const forbidden = row.forbidden_org_hosts || [];
    for (const p of ordered) {
      if (subjects.length >= maxPeople) break;
      if ((perOwner.get(ownerKey) || 0) >= maxPerOwner) break;
      const parts = String(p.display_name).trim().split(/\s+/);
      const liChannel = (p.channels || []).find((c) => /LINKEDIN/i.test(c.kind));
      const linkedin = liChannel?.value || null;
      const liIndependentlyConfirmed = /independently confirmed/i.test(liChannel?.display_label || "");
      const domainHost = row.confirmed_company_domain_host || null;
      const domainStatus =
        !domainHost
          ? "ABSENT"
          : rejectedDomainHosts.includes(domainHost)
            ? "REJECTED"
            : "CONFIRMED";
      const candidate = {
        id: `${row.hotel_id}__${parts.join("_")}`.slice(0, 90),
        subject_id: `${row.hotel_id}__${parts.join("_")}`.slice(0, 90),
        hotel_id: row.hotel_id,
        hotel_name: row.hotel_name,
        person: {
          display_name: p.display_name,
          first_name: parts[0],
          last_name: parts.slice(1).join(" ") || parts[0],
          title: p.title,
          identity_supported: true,
          publication_label: p.publication_label,
          why_relevant: p.why_relevant,
          deceased: Boolean(p.deceased),
          former_affiliation: Boolean(p.former_affiliation),
        },
        organization: {
          name: row.ownership?.owner_display_name,
          entity_id: row.ownership?.owner_entity_id,
          relationship_supported: Boolean(row.ownership?.owner_display_name),
          rejected: false,
        },
        identifiers: {
          domain:
            domainHost && domainStatus === "CONFIRMED"
              ? { value: domainHost, status: "CONFIRMED", independently_supported: true, evidence_refs: row.domain_evidence || [] }
              : domainHost
                ? { value: domainHost, status: domainStatus }
                : { status: "ABSENT" },
          linkedin_url: linkedin
            ? {
                value: linkedin,
                independently_confirmed: liIndependentlyConfirmed,
                evidence_note: p.why_relevant || row.ownership?.owner_display_name,
                profile_company: row.ownership?.owner_display_name,
              }
            : null,
        },
        rejected_domains: rejectedDomainHosts,
        forbidden_owner_domain_hosts: forbidden,
        custom: { subject_id: `${row.hotel_id}__${parts.join("_")}`.slice(0, 90), hotel_id: row.hotel_id || "" },
        // flat fields for evaluateFeRow / commercial table
        full_name: p.display_name,
        first_name: parts[0],
        last_name: parts.slice(1).join(" ") || parts[0],
        organization_name: row.ownership?.owner_display_name,
        owner_entity_id: row.ownership?.owner_entity_id,
        domain: domainStatus === "CONFIRMED" ? domainHost : null,
        confirmed_title: p.title,
        linkedin_url: linkedin && /linkedin\.com\/in\//i.test(linkedin) ? linkedin : null,
        baseline_email: null,
        newly_researched: Boolean(p.provenance?.newly_researched),
        role_evidence: (p.evidence || [])[0] || null,
      };
      // Owner-person paid enrich requires corroborated affiliation + hotel→owner evidence.
      // Surfe/FullEnrich discovery search must not use OWNER_PERSON_ENRICHMENT_WORKFLOW.
      const ownerClass = String(
        row.ownership?.classification || row.ownership?.owner_role || row.ownership?.relationship_primary || ""
      ).toUpperCase();
      const ownerClassOk =
        !ownerClass ||
        ["PROPERTY_OWNER", "ECONOMIC_OWNER_OR_SPONSOR", "OWNER", "STAGED_FROM_LIVE_RESEARCH"].includes(ownerClass) ||
        !["BRAND", "OPERATOR", "REGISTERED_BUSINESS", "UNRESOLVED"].includes(ownerClass);
      const ownerEvidenceRefs = row.ownership?.evidence_refs || [];

      const affCorroborated = Boolean(p.provenance?.independently_corroborated);
      const gated = gateProviderCandidates(
        [
          {
            ...candidate,
            workflow: "owner_person_enrichment",
            hotel_to_owner: {
              supported: Boolean(row.ownership?.owner_display_name) && ownerClassOk,
              relationship_class: ownerClass || null,
              evidence_refs: ownerEvidenceRefs,
            },
            affiliation_corroboration: {
              status: affCorroborated ? "CORROBORATED" : p.affiliation_status || "SURFE_ONLY",
              source_class:
                p.affiliation_source_class ||
                (affCorroborated ? "CREDIBLE_INDEPENDENT" : "SURFE_ONLY"),
              independently_corroborated: affCorroborated,
              evidence_refs: p.evidence || [],
              role_relevant: undefined,
            },
          },
        ],
        { provider: "fullenrich", workflow: "owner_person_enrichment" }
      );
      if (gated.allowed.length) {
        const allowed = gated.allowed[0];
        subjects.push({
          ...allowed,
          first_name: allowed.submit_row.first_name,
          last_name: allowed.submit_row.last_name,
          domain: allowed.submit_row.domain || null,
          company_name: allowed.submit_row.company_name || null,
          linkedin_url: allowed.submit_row.linkedin_url || null,
          // keep organization as gate object; commercial/eval use organization_name
          organization: candidate.organization,
          organization_name: candidate.organization_name,
          gate: allowed.gate,
          submit_row: allowed.submit_row,
        });
        perOwner.set(ownerKey, (perOwner.get(ownerKey) || 0) + 1);
      } else {
        rejected.push(gated.rejected[0]);
      }
    }
  }
  return { subjects, rejected_pre_submission: rejected };
}

export { verifyOwnershipPath };
