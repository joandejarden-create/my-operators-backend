/**
 * Live private-event venue discovery — portable (hotel market label + geo + archetype).
 * No hotel-name or city hardcodes in production logic.
 *
 * Pipeline: archetype query mix → SerpAPI → page fetch → OpenAI venue-fact extract
 * → buildVenueEntity candidates. Fixtures are never success evidence.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import {
  ARCHETYPE_VENUE_MIX,
  HOTEL_ARCHETYPE_PE,
  VENUE_TYPE,
  ON_SITE_LODGING_STATUS,
} from "./constants.js";
import { resolveHotelArchetype } from "./hotel-context.js";
import { buildVenueEntity, normalizeVenueType } from "./venue-entity.js";

export const LIVE_VENUE_DISCOVERY_V1_2 = "gdi_private_events_live_venue_discovery_v1_2";

const EXTRACT_MODEL =
  process.env.GDI_PE_EXTRACT_MODEL ||
  process.env.GDI_NATIVE_EXTRACT_MODEL ||
  process.env.OPENAI_MODEL ||
  "gpt-4o-mini";

const VENUE_TYPE_QUERY = Object.freeze({
  [VENUE_TYPE.WEDDING_VENUE]: "wedding venue",
  [VENUE_TYPE.COUNTRY_CLUB]: "country club wedding events",
  [VENUE_TYPE.HISTORIC_ESTATE]: "historic estate wedding venue",
  [VENUE_TYPE.GARDEN]: "garden wedding venue",
  [VENUE_TYPE.MUSEUM]: "museum private events venue",
  [VENUE_TYPE.BANQUET_HALL]: "banquet hall wedding",
  [VENUE_TYPE.RELIGIOUS_VENUE]: "church synagogue temple wedding venue",
  [VENUE_TYPE.PRIVATE_CLUB]: "private club events venue",
  [VENUE_TYPE.CONFERENCE_EVENT_CENTER]: "event center private events",
  [VENUE_TYPE.SOCIAL_EVENT_SPACE]: "private event space",
  [VENUE_TYPE.EVENT_VENUE]: "private event venue",
  [VENUE_TYPE.WINERY]: "winery wedding venue",
  [VENUE_TYPE.OTHER]: "private event venue",
});

function clean(s) {
  return String(s || "").trim();
}

function hasSerpKey() {
  return Boolean(
    String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim()
  );
}

function hasOpenAiKey() {
  return Boolean(String(process.env.OPENAI_API_KEY || "").trim());
}

function domainFromUrl(url) {
  try {
    return new URL(String(url)).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return null;
  }
}

/** Skip directory/noise hosts for official-source preference. */
const DIRECTORY_HOST_RE =
  /theknot|weddingwire|zola|herecomestheguide|eventective|peerspace|gigsalad|yelp\.|tripadvisor|facebook\.|instagram\.|linkedin\.|maps\.google|google\.com\/maps|bing\.com|tripadvisor/i;

function isDirectoryHost(url) {
  const d = domainFromUrl(url);
  return d ? DIRECTORY_HOST_RE.test(d) : false;
}

/**
 * Build portable discovery queries from hotel market label + archetype.
 * @param {{ marketLabel: string, city?: string, region?: string, hotel?: object, maxQueries?: number }} opts
 */
export function buildVenueDiscoveryQueries({
  marketLabel,
  city = null,
  region = null,
  hotel = {},
  maxQueries = 12,
} = {}) {
  const market = clean(marketLabel) || [clean(city), clean(region)].filter(Boolean).join(", ");
  if (!market) {
    throw new Error("marketLabel_or_city_region_required");
  }
  const archetype = resolveHotelArchetype(hotel);
  const mix =
    ARCHETYPE_VENUE_MIX[archetype] ||
    ARCHETYPE_VENUE_MIX[HOTEL_ARCHETYPE_PE.SUBURBAN_FULL_SERVICE];

  const queries = [];
  for (const vt of mix) {
    const phrase = VENUE_TYPE_QUERY[vt] || "private event venue";
    queries.push({
      q: `${phrase} ${market}`,
      venueTypeHint: vt,
      family: "archetype_mix",
    });
  }
  // Broad coverage families (still market-parameterized)
  const extras = [
    `wedding venues near ${market}`,
    `historic estate wedding ${market}`,
    `country club private events ${market}`,
    `museum special events venue ${market}`,
    `garden wedding venue ${market}`,
    `banquet hall wedding ${market}`,
  ];
  for (const q of extras) {
    queries.push({ q, venueTypeHint: null, family: "coverage" });
  }

  // Dedupe by query string
  const seen = new Set();
  const out = [];
  for (const item of queries) {
    const key = item.q.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
    if (out.length >= maxQueries) break;
  }
  return { archetype, marketLabel: market, queries: out };
}

async function serpOrganic(query, { num = 8 } = {}) {
  const result = await serpapiSearch({
    engine: "google",
    q: query,
    num,
    hl: "en",
    gl: "us",
  });
  if (!result.ok) {
    return { ok: false, error: result.error, organic: [], charged: 0 };
  }
  const organic = (result.data?.organic_results || []).map((r) => ({
    title: r.title || null,
    url: r.link || r.url || null,
    snippet: r.snippet || null,
  }));
  return { ok: true, organic, charged: result.charged ?? 1 };
}

async function fetchPage(url, { maxChars = 8000 } = {}) {
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, url, error: "bad_url" };
  }
  const page = await fetchResearchPage(url, { timeoutMs: 20000 });
  if (!page.ok) {
    return { ok: false, url, error: page.error || `status_${page.status}` };
  }
  const text = htmlToSearchableText(page.text).replace(/\s+/g, " ").trim();
  return {
    ok: true,
    url: page.url || url,
    text: text.slice(0, maxChars),
    directory: isDirectoryHost(page.url || url),
  };
}

async function callOpenAiJson(system, user, { maxTokens = 2500 } = {}) {
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: EXTRACT_MODEL,
      temperature: 0.1,
      max_tokens: maxTokens,
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
    }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(json.error?.message || `openai_http_${res.status}`);
    err.status = res.status;
    throw err;
  }
  const content = json.choices?.[0]?.message?.content;
  if (!content) throw new Error("openai_empty_content");
  return { parsed: JSON.parse(content), usage: json.usage || null };
}

const SYSTEM_VENUE_LIST = `You extract REAL private-event / wedding venues from search results.
Return JSON: { "venues": [ { "venueName", "venueType", "city", "region", "website", "snippetEvidence", "identityConfidence": "HIGH"|"MEDIUM"|"LOW" } ] }
Rules:
- Only real venue businesses / estates / clubs / museums / gardens / event spaces.
- Prefer official websites over directories when URLs are present.
- venueType one of: WEDDING_VENUE, COUNTRY_CLUB, HISTORIC_ESTATE, MUSEUM, GARDEN, WINERY, BANQUET_HALL, RELIGIOUS_VENUE, PRIVATE_CLUB, CONFERENCE_EVENT_CENTER, SOCIAL_EVENT_SPACE, EVENT_VENUE, OTHER
- Do not invent venues. identityConfidence LOW if name/url uncertain.
- Skip hotels that are primarily lodging (unless they are event venues with clear private-event offering only as listing noise — prefer non-hotel venues).`;

const SYSTEM_VENUE_FACTS = `You extract verified venue facts from page text for hotel group-demand partnership research.
Return JSON with fields (omit unknown): {
  venueName, address, city, region, country, website, venueType,
  maxCapacity, minCapacity, onSiteGuestrooms, onSiteLodgingStatus,
  weddingsAdvertised, privateEventsAdvertised, estimatedAnnualPrivateEvents,
  preferredHotelListed, exclusiveHotelRelationship, hotelPartners: string[],
  eventContact, eventContactRole, partnerStatus,
  lodgingEvidence, partnerEvidence, exclusiveEvidence, capacityEvidenceStatus,
  sourceAuthority: "OFFICIAL_VENUE"|"CREDIBLE_DIRECTORY"|"SECONDARY"|"UNKNOWN",
  keyFactsOfficialConfirmed: "YES"|"NO"|"PARTIAL",
  identityConfidence: "HIGH"|"MEDIUM"|"LOW"
}
Rules:
- onSiteLodgingStatus: NO_LODGING | LIMITED_LODGING | ADEQUATE_LODGING | UNKNOWN
- partnerStatus: NO_PARTNER_FOUND | NONEXCLUSIVE_PARTNERS_FOUND | PREFERRED_PARTNER_FOUND | EXCLUSIVE_PARTNER_FOUND | UNKNOWN
- exclusiveHotelRelationship MUST default false. Set true ONLY with explicit exclusive/sole hotel language; quote that language in exclusiveEvidence. Preferred hotels are NOT exclusive.
- Do NOT invent capacity, guestrooms, annual counts, or hotel partners.
- UNKNOWN is preferred over fabrication.
- Do not extract personal couple/guest contact data.`;

/**
 * Map extract → venue entity + research metadata.
 */
export function mapExtractToVenue(raw = {}, { sourceUrls = [], discovery = {} } = {}) {
  const website = clean(raw.website) || sourceUrls.find((u) => !isDirectoryHost(u)) || sourceUrls[0] || null;
  // Fail-closed: exclusive only when evidence is explicit.
  const exclusiveEvidence = clean(raw.exclusiveEvidence || raw.partnerEvidence);
  const exclusiveClaim = Boolean(raw.exclusiveHotelRelationship);
  const exclusiveOk =
    exclusiveClaim &&
    /\bexclusive\b|\bsole\b|\bonly\s+hotel\b|\bonly\s+preferred\s+hotel\b/i.test(
      exclusiveEvidence || ""
    );

  const entity = buildVenueEntity({
    venueName: raw.venueName,
    venueType: normalizeVenueType(raw.venueType || discovery.venueTypeHint),
    address: raw.address,
    city: raw.city || discovery.city,
    region: raw.region || discovery.region,
    country: raw.country || "US",
    website,
    lat: raw.lat,
    long: raw.long,
    maxCapacity: raw.maxCapacity,
    minCapacity: raw.minCapacity,
    onSiteGuestrooms: raw.onSiteGuestrooms,
    onSiteLodgingStatus: raw.onSiteLodgingStatus || ON_SITE_LODGING_STATUS.UNKNOWN,
    weddingsAdvertised: raw.weddingsAdvertised,
    privateEventsAdvertised: raw.privateEventsAdvertised,
    estimatedAnnualPrivateEvents: raw.estimatedAnnualPrivateEvents,
    preferredHotelListed: raw.preferredHotelListed,
    exclusiveHotelRelationship: exclusiveOk,
    hotelPartners: raw.hotelPartners,
    eventContact: raw.eventContact,
    eventContactRole: raw.eventContactRole,
    sourceUrls,
    lastVerifiedAt: new Date().toISOString(),
  });

  const identityConfidence = String(raw.identityConfidence || "MEDIUM").toUpperCase();
  return {
    ...entity,
    research: {
      identityConfidence,
      partnerStatus: clean(raw.partnerStatus) || "UNKNOWN",
      sourceAuthority: clean(raw.sourceAuthority) || "UNKNOWN",
      keyFactsOfficialConfirmed: clean(raw.keyFactsOfficialConfirmed) || "NO",
      lodgingEvidence: clean(raw.lodgingEvidence) || null,
      partnerEvidence: clean(raw.partnerEvidence) || null,
      exclusiveEvidence: exclusiveEvidence || null,
      capacityEvidenceStatus: clean(raw.capacityEvidenceStatus) || "UNKNOWN",
      officialWebsite: Boolean(website && !isDirectoryHost(website)),
    },
  };
}

/**
 * Lightweight address geocode (Nominatim). Fail soft — never invent coords.
 */
export async function geocodeVenueAddress(venue = {}) {
  const q = [venue.address, venue.city, venue.region, venue.country || "US"]
    .map(clean)
    .filter(Boolean)
    .join(", ");
  if (!q || q.length < 8) return null;
  try {
    const url = new URL("https://nominatim.openstreetmap.org/search");
    url.searchParams.set("q", q);
    url.searchParams.set("format", "json");
    url.searchParams.set("limit", "1");
    const res = await fetch(url.toString(), {
      headers: {
        Accept: "application/json",
        "User-Agent": "DealalityGdiPrivateEvents/1.2 (research-canary)",
      },
    });
    if (!res.ok) return null;
    const json = await res.json();
    const hit = Array.isArray(json) ? json[0] : null;
    if (!hit?.lat || !hit?.lon) return null;
    return { lat: Number(hit.lat), long: Number(hit.lon), geocodeQ: q };
  } catch {
    return null;
  }
}

/**
 * Live discover private-event venues near a hotel market.
 */
export async function discoverLiveVenues({
  hotel = {},
  marketLabel,
  city = null,
  region = null,
  maxVenues = 28,
  maxQueries = 12,
  maxPagesPerVenue = 2,
  enrichTopN = 28,
} = {}) {
  if (!hasSerpKey()) {
    const err = new Error("SERPAPI_KEY_required_for_pe_live_venue_discovery");
    err.code = "SERPAPI_KEY_required";
    throw err;
  }
  if (!hasOpenAiKey()) {
    const err = new Error("OPENAI_API_KEY_required_for_pe_live_venue_discovery");
    err.code = "OPENAI_API_KEY_required";
    throw err;
  }

  const plan = buildVenueDiscoveryQueries({
    marketLabel,
    city,
    region,
    hotel,
    maxQueries,
  });

  const ledger = {
    version: LIVE_VENUE_DISCOVERY_V1_2,
    serpQueries: 0,
    serpCharged: 0,
    pagesFetched: 0,
    pagesFailed: 0,
    openaiCalls: 0,
    errors: [],
    queries: [],
  };

  const hitMap = new Map(); // domain|name → { title, urls, snippets, venueTypeHint }

  for (const item of plan.queries) {
    try {
      const serp = await serpOrganic(item.q, { num: 8 });
      ledger.serpQueries += 1;
      ledger.serpCharged += serp.charged || 0;
      ledger.queries.push({
        q: item.q,
        ok: serp.ok,
        organic: (serp.organic || []).length,
        error: serp.error || null,
      });
      if (!serp.ok) {
        ledger.errors.push({ stage: "serp", q: item.q, error: serp.error });
        continue;
      }
      for (const row of serp.organic || []) {
        if (!row.url || !row.title) continue;
        const domain = domainFromUrl(row.url) || row.url;
        // Prefer grouping by domain when official-ish; else title
        const key = isDirectoryHost(row.url)
          ? `t:${clean(row.title).toLowerCase().slice(0, 60)}`
          : `d:${domain}`;
        const prev = hitMap.get(key) || {
          titles: [],
          urls: [],
          snippets: [],
          venueTypeHint: item.venueTypeHint,
        };
        if (row.title) prev.titles.push(row.title);
        if (row.url) prev.urls.push(row.url);
        if (row.snippet) prev.snippets.push(row.snippet);
        if (!prev.venueTypeHint && item.venueTypeHint) {
          prev.venueTypeHint = item.venueTypeHint;
        }
        hitMap.set(key, prev);
      }
    } catch (err) {
      ledger.errors.push({
        stage: "serp",
        q: item.q,
        error: err?.message || String(err),
      });
    }
  }

  // First-pass list extract from organic titles/snippets (batched)
  const organicDigest = [...hitMap.entries()].slice(0, 60).map(([key, v]) => ({
    key,
    titles: [...new Set(v.titles)].slice(0, 3),
    urls: [...new Set(v.urls)].slice(0, 4),
    snippets: v.snippets.slice(0, 3),
    venueTypeHint: v.venueTypeHint,
  }));

  let listVenues = [];
  try {
    const listed = await callOpenAiJson(
      SYSTEM_VENUE_LIST,
      JSON.stringify({
        marketLabel: plan.marketLabel,
        organicDigest,
      })
    );
    ledger.openaiCalls += 1;
    listVenues = Array.isArray(listed.parsed?.venues) ? listed.parsed.venues : [];
  } catch (err) {
    ledger.errors.push({
      stage: "list_extract",
      error: err?.message || String(err),
    });
  }

  // Enrich each candidate with official page facts
  const enriched = [];
  const weak = [];
  for (const cand of listVenues.slice(0, enrichTopN)) {
    if (enriched.length >= maxVenues) break;
    const name = clean(cand.venueName);
    if (!name) continue;

    const urls = [];
    if (cand.website) urls.push(cand.website);
    // attach matching organic urls by name token
    const nameKey = name.toLowerCase().slice(0, 24);
    for (const dig of organicDigest) {
      const blob = `${dig.titles.join(" ")} ${dig.urls.join(" ")}`.toLowerCase();
      if (blob.includes(nameKey) || nameKey.split(" ").some((t) => t.length > 4 && blob.includes(t))) {
        urls.push(...dig.urls);
      }
    }
    const uniqueUrls = [...new Set(urls.filter(Boolean))];
    const officialFirst = [
      ...uniqueUrls.filter((u) => !isDirectoryHost(u)),
      ...uniqueUrls.filter((u) => isDirectoryHost(u)),
    ].slice(0, maxPagesPerVenue);

    const pages = [];
    for (const url of officialFirst) {
      const page = await fetchPage(url);
      if (page.ok) {
        ledger.pagesFetched += 1;
        pages.push(page);
      } else {
        ledger.pagesFailed += 1;
      }
    }

    let facts = { ...cand };
    if (pages.length) {
      try {
        const factRes = await callOpenAiJson(
          SYSTEM_VENUE_FACTS,
          JSON.stringify({
            candidate: cand,
            pages: pages.map((p) => ({
              url: p.url,
              directory: p.directory,
              text: p.text.slice(0, 6000),
            })),
          })
        );
        ledger.openaiCalls += 1;
        facts = { ...cand, ...(factRes.parsed || {}) };
      } catch (err) {
        ledger.errors.push({
          stage: "fact_extract",
          venue: name,
          error: err?.message || String(err),
        });
      }
    }

    const mapped = mapExtractToVenue(facts, {
      sourceUrls: [...new Set([...officialFirst, ...(pages.map((p) => p.url) || [])])],
      discovery: {
        city: city || cand.city,
        region: region || cand.region,
        venueTypeHint: cand.venueType,
      },
    });

    // Geocode when address known and coords missing (bounded)
    if (
      mapped.address &&
      (!Number.isFinite(mapped.lat) || !Number.isFinite(mapped.long))
    ) {
      const geo = await geocodeVenueAddress(mapped);
      if (geo) {
        mapped.lat = geo.lat;
        mapped.long = geo.long;
        mapped.research = {
          ...(mapped.research || {}),
          geocoded: true,
          geocodeQ: geo.geocodeQ,
        };
      }
      await new Promise((r) => setTimeout(r, 1100)); // Nominatim politeness
    }

    const conf = mapped.research?.identityConfidence || "MEDIUM";
    if (conf === "LOW") {
      weak.push({ action: "SKIP_LOW_IDENTITY", venue: mapped });
      continue;
    }
    if (!mapped.venueName) {
      weak.push({ action: "SKIP_NO_NAME", venue: mapped });
      continue;
    }
    enriched.push(mapped);
  }

  // Dedupe by venueId / domain
  const byId = new Map();
  for (const v of enriched) {
    const prior = byId.get(v.venueId);
    if (!prior) {
      byId.set(v.venueId, v);
      continue;
    }
    byId.set(v.venueId, buildVenueEntity(v, prior));
  }

  return {
    version: LIVE_VENUE_DISCOVERY_V1_2,
    marketLabel: plan.marketLabel,
    archetype: plan.archetype,
    venues: [...byId.values()].slice(0, maxVenues),
    weak,
    ledger,
    queryPlan: plan.queries,
  };
}

export {
  isDirectoryHost,
  domainFromUrl,
  hasSerpKey as peLiveHasSerpKey,
  hasOpenAiKey as peLiveHasOpenAiKey,
};
