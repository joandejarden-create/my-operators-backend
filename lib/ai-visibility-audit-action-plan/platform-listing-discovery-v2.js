/**
 * AVA mandatory third-party platform listing discovery V2 (zero paid).
 *
 * Strategies (bounded, no CAPTCHA bypass, no unauthorized paid spend):
 *  1. Prior research seed URLs (pilot / Context / inventory graphs)
 *  2. Payload evidence-graph URL harvest
 *  3. DuckDuckGo HTML site: queries (free)
 *  4. Constructed Maps search URLs (discoverability aid — not GBP claim)
 *  5. HTTP probe + identity token check
 *
 * Statuses align with founder research-completion vocabulary.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { PLATFORM_ASSESSMENT_STATUSES } from "./digital-platform-correction-register-v1.js";

export const PLATFORM_LISTING_DISCOVERY_V2 =
  "AVA_PLATFORM_LISTING_DISCOVERY_V2";

export const PLATFORM_RESEARCH_STATUSES = Object.freeze({
  VERIFIED_CONSISTENT: "VERIFIED_CONSISTENT",
  VERIFIED_CORRECTION_REQUIRED: "VERIFIED_CORRECTION_REQUIRED",
  VERIFIED_OPPORTUNITY: "VERIFIED_OPPORTUNITY",
  LISTING_FOUND_ACCESS_RESTRICTED: "LISTING_FOUND_ACCESS_RESTRICTED",
  LISTING_NOT_FOUND_AFTER_RESEARCH: "LISTING_NOT_FOUND_AFTER_RESEARCH",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  RESEARCH_INCOMPLETE: "RESEARCH_INCOMPLETE",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PILOT_RESULTS = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_15USD_PAID_PILOT_RESULTS.json"
);

const BASELINE_PLATFORMS = Object.freeze([
  {
    key: "BRAND_PROPERTY_PAGE",
    label: "Brand property page",
    domains: [/hilton\.com\/en\/hotels\//i, /marriott\.com\/.*\/hotels\//i, /ihg\.com\/.*\/hotels\//i, /hyatt\.com\//i],
  },
  {
    key: "HOTEL_OPERATED_WEBSITE",
    label: "Hotel-operated website",
    domains: null, // property website
  },
  {
    key: "GOOGLE_BUSINESS_PROFILE",
    label: "Google Business Profile / Google Maps",
    domains: [/maps\.google\./i, /google\.com\/maps/i, /google\.com\/travel\/hotels/i, /g\.page\//i],
    siteQuery: "site:maps.google.com OR site:google.com/maps OR site:google.com/travel/hotels",
  },
  {
    key: "TRIPADVISOR",
    label: "Tripadvisor",
    domains: [/tripadvisor\./i],
    siteQuery: "site:tripadvisor.com",
  },
  {
    key: "OTA_BOOKING",
    label: "Booking.com",
    domains: [/booking\.com\/hotel\//i],
    siteQuery: "site:booking.com/hotel",
  },
  {
    key: "OTA_EXPEDIA",
    label: "Expedia",
    domains: [/expedia\./i, /hotels\.com\//i],
    siteQuery: "site:expedia.com OR site:hotels.com",
  },
  {
    key: "APPLE_MAPS",
    label: "Apple Maps",
    domains: [/maps\.apple\.com/i],
    siteQuery: "site:maps.apple.com",
  },
  {
    key: "BING_PLACES",
    label: "Bing Places",
    domains: [/bing\.com\/maps/i, /bingplaces/i],
    siteQuery: "site:bing.com/maps",
  },
  {
    key: "CVENT",
    label: "Cvent",
    domains: [/cvent\./i],
    siteQuery: "site:cvent.com",
    meetingsOnly: true,
  },
  {
    key: "DESTINATION_CVB",
    label: "Destination / tourism / CVB",
    domains: [/visit-|turismo|convention|cvb|\.travel\//i],
  },
]);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function probeUrl(url, { timeoutMs = 12000 } = {}) {
  if (!url) return { ok: false, status: 0, error: "NO_URL", finalUrl: null, text: "" };
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: ctrl.signal,
      headers: {
        "User-Agent":
          "DealalityAVAPlatformDiscovery/2.0 (+research; respectful; contact dealality)",
        Accept: "text/html,application/xhtml+xml",
      },
    });
    const text = await res.text().catch(() => "");
    return {
      ok: res.ok,
      status: res.status,
      finalUrl: res.url || url,
      text: text.slice(0, 150000),
      error: res.ok ? null : `HTTP_${res.status}`,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      finalUrl: url,
      text: "",
      error: err.name === "AbortError" ? "TIMEOUT" : err.message || "FETCH_ERROR",
    };
  } finally {
    clearTimeout(t);
  }
}

const IDENTITY_STOPWORDS = new Set([
  "hotel",
  "hotels",
  "resort",
  "marina",
  "collection",
  "luxury",
  "grand",
  "americana",
  "fiesta",
  "inclusive",
  "all",
  "by",
  "the",
  "and",
  "with",
  "golf",
  "spa",
  "palace",
  "plaza",
  "city",
  "beach",
  "marriott",
  "hilton",
  "sheraton",
  "curio",
  "ac",
]);

function identityTokens(payload) {
  const name = String(payload?.property?.name || "");
  const market = String(payload?.property?.market || payload?.property?.coverLocation || "");
  const city = market.split(",")[0] || "";
  const tokens = [
    ...name.toLowerCase().split(/[^a-z0-9]+/).filter((t) => t.length > 3),
    ...city.toLowerCase().split(/[^a-z0-9]+/).filter((t) => t.length > 3),
  ].filter((t) => !IDENTITY_STOPWORDS.has(t));
  return [...new Set(tokens)].slice(0, 12);
}

function identityMatch(html, tokens) {
  const h = String(html || "").toLowerCase();
  if (!h) return { score: 0, matched: [] };
  const matched = tokens.filter((t) => h.includes(t));
  return { score: matched.length / Math.max(tokens.length, 1), matched };
}

function collectGraphUrls(payload) {
  const urls = new Set();
  const push = (u) => {
    if (u && /^https?:\/\//i.test(String(u))) urls.add(String(u).split("#")[0]);
  };
  push(payload?.property?.website);
  const walk = (obj, depth = 0) => {
    if (!obj || depth > 6) return;
    if (typeof obj === "string") {
      push(obj);
      return;
    }
    if (Array.isArray(obj)) {
      obj.forEach((x) => walk(x, depth + 1));
      return;
    }
    if (typeof obj === "object") {
      for (const [k, v] of Object.entries(obj)) {
        if (/url|href|link|website/i.test(k) || typeof v === "string") walk(v, depth + 1);
        else if (typeof v === "object") walk(v, depth + 1);
      }
    }
  };
  walk(payload?.digitalAuthority);
  walk(payload?.digitalContentReadiness);
  walk(payload?.platformCoverageAssessment);
  walk(payload?.actions);
  walk(payload?.keyFindings);
  return [...urls];
}

/** Distinctive URL tokens that identify a specific pilot hotel — never cross-assign. */
const HOTEL_URL_IDENTITY = Object.freeze({
  adp_waterstone_boca_raton: [
    /bctbuqq/i,
    /waterstone/i,
    /boca[-_]?raton/i,
    /g34088-d84184/i,
  ],
  adp_ac_hotel_a_coruna: [
    /lcgco/i,
    /a[-_]?coru[nñ]a/i,
    /acacoruna/i,
    /la[-_]?coruna/i,
    /g187507-d266659/i,
    /h907714/i,
  ],
  adp_castillo_hotel_son_vida: [
    /pmilc/i,
    /son[-_]?vida/i,
    /castillo/i,
    /g187463-d234557/i,
  ],
  adp_sheraton_mallorca_arabella_golf: [/arabella/i, /sheraton.*mallorca/i],
  adp_grand_fiesta_americana_chapultepec: [/chapultepec/i, /fiesta.*americana/i],
  adp_fiesta_americana_condesa_cancun: [/condesa/i, /cancun/i, /fiesta.*americana/i],
});

function urlBelongsToThisHotel(url, propertyId, tokens) {
  const low = String(url || "").toLowerCase();
  if (!low) return false;

  // Opaque Google Travel entity URLs without hotel-name tokens are never trusted
  // across hotels (pilot blobs previously shared one entity id).
  if (/google\.[^/]+\/travel\/hotels\/entity\//i.test(low)) {
    const own = HOTEL_URL_IDENTITY[propertyId] || [];
    const strong = (tokens || []).filter((t) => t.length > 4);
    if (!own.some((re) => re.test(low)) && !strong.some((t) => low.includes(t))) {
      return false;
    }
  }

  // Hard reject: URL clearly belongs to a different known hotel
  for (const [otherId, patterns] of Object.entries(HOTEL_URL_IDENTITY)) {
    if (otherId === propertyId) continue;
    if (patterns.some((re) => re.test(low))) {
      const own = HOTEL_URL_IDENTITY[propertyId] || [];
      if (!own.some((re) => re.test(low))) return false;
    }
  }

  const ownPatterns = HOTEL_URL_IDENTITY[propertyId] || [];
  if (ownPatterns.some((re) => re.test(low))) return true;

  // Require at least one distinctive name/city token (len>4) in the URL path/query
  const strong = (tokens || []).filter((t) => t.length > 4);
  if (strong.some((t) => low.includes(t))) return true;

  // Weak fallback: two shorter distinctive tokens
  const weakHits = (tokens || []).filter((t) => t.length > 3 && low.includes(t));
  return weakHits.length >= 2;
}

function loadPilotSeedUrls(propertyId) {
  if (!fs.existsSync(PILOT_RESULTS)) return [];
  try {
    const doc = JSON.parse(fs.readFileSync(PILOT_RESULTS, "utf8"));
    const hotels = doc.hotels || {};
    // ONLY this hotel's pilot blob — never the whole multi-hotel file
    const hotelBlob =
      hotels[propertyId] ||
      hotels[String(propertyId || "").replace(/-/g, "_")] ||
      null;
    if (!hotelBlob) return [];
    const blob = JSON.stringify(hotelBlob);
    const urls = [...blob.matchAll(/https?:\/\/[^"\\\s<>]+/g)].map((m) =>
      m[0].replace(/\\u0026/g, "&").replace(/&amp;/g, "&")
    );
    return [...new Set(urls)].filter((u) =>
      /tripadvisor|booking\.com|expedia|hotels\.com|hilton\.com\/en\/hotels|marriott\.com\/.*hotels|google\.com\/(maps|travel)|maps\.google|g\.page|cvent|visit-|golfbreaks|acorunacb|weddingwire/i.test(
        u
      )
    );
  } catch {
    return [];
  }
}

async function duckDuckGoSearch(query, { timeoutMs = 15000 } = {}) {
  const url = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
  const probe = await probeUrl(url, { timeoutMs });
  if (!probe.ok && !probe.text) {
    return { ok: false, urls: [], error: probe.error || "DDG_FAIL" };
  }
  const urls = [];
  const re = /uddg=([^&"]+)/g;
  let m;
  while ((m = re.exec(probe.text))) {
    try {
      urls.push(decodeURIComponent(m[1]));
    } catch {
      /* ignore */
    }
  }
  // Also plain hrefs
  const hrefRe = /href="(https?:\/\/[^"]+)"/g;
  while ((m = hrefRe.exec(probe.text))) {
    if (!/duckduckgo\.com/i.test(m[1])) urls.push(m[1]);
  }
  return { ok: true, urls: [...new Set(urls)].slice(0, 12), error: null };
}

function classifyPlatformUrl(url) {
  for (const p of BASELINE_PLATFORMS) {
    if (!p.domains) continue;
    if (p.domains.some((re) => re.test(url))) return p.key;
  }
  return null;
}

function mapResearchStatus(assessmentStatus, { hasUrl, identityOk, opportunity }) {
  if (opportunity) return PLATFORM_RESEARCH_STATUSES.VERIFIED_OPPORTUNITY;
  if (assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT && identityOk) {
    return PLATFORM_RESEARCH_STATUSES.VERIFIED_CONSISTENT;
  }
  if (
    assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_INCONSISTENCY ||
    assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_MISSING_INFORMATION
  ) {
    return PLATFORM_RESEARCH_STATUSES.VERIFIED_CORRECTION_REQUIRED;
  }
  if (assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.ACCESS_RESTRICTED && hasUrl) {
    return PLATFORM_RESEARCH_STATUSES.LISTING_FOUND_ACCESS_RESTRICTED;
  }
  if (assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.NOT_APPLICABLE) {
    return PLATFORM_RESEARCH_STATUSES.NOT_APPLICABLE;
  }
  if (assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.NOT_FOUND) {
    return PLATFORM_RESEARCH_STATUSES.LISTING_NOT_FOUND_AFTER_RESEARCH;
  }
  return PLATFORM_RESEARCH_STATUSES.RESEARCH_INCOMPLETE;
}

function hotelWantsMeetings(payload) {
  const blob = `${payload?.property?.name || ""} ${JSON.stringify(
    payload?.executiveSummary || {}
  )} ${JSON.stringify(payload?.keyFindings || [])}`.toLowerCase();
  return /meeting|mice|event|conference|wedding|group/i.test(blob);
}

/**
 * Discover + assess baseline platforms for one hotel (zero paid USD).
 */
export async function discoverAndAssessPlatformListings(payload, opts = {}) {
  const propertyId = payload?.property?.propertyId;
  const hotelName = payload?.property?.name || propertyId;
  const market = payload?.property?.market || payload?.property?.coverLocation || "";
  const tokens = identityTokens(payload);
  const log = [];
  const paidResearchUsd = 0;

  const seed = [
    ...loadPilotSeedUrls(propertyId),
    ...collectGraphUrls(payload),
  ];
  // Strict hotel-scoped seeds — never accept another hotel's listing URL
  const rankedSeeds = seed.filter((u) =>
    urlBelongsToThisHotel(u, propertyId, tokens)
  );

  const byKey = new Map();
  const pushCandidate = (key, url, strategy) => {
    if (!url || !key) return;
    if (
      strategy !== "CONSTRUCTED_MAPS_SEARCH" &&
      strategy !== "PROPERTY_WEBSITE" &&
      !urlBelongsToThisHotel(url, propertyId, tokens)
    ) {
      log.push({
        at: new Date().toISOString(),
        step: "reject_cross_hotel",
        key,
        url,
        strategy,
      });
      return;
    }
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push({ url, strategy });
    log.push({ at: new Date().toISOString(), step: "candidate", key, url, strategy });
  };

  for (const url of rankedSeeds) {
    const key = classifyPlatformUrl(url);
    if (key) pushCandidate(key, url, "PRIOR_RESEARCH_OR_GRAPH");
  }

  // Hotel website
  if (payload?.property?.website) {
    pushCandidate("HOTEL_OPERATED_WEBSITE", payload.property.website, "PROPERTY_WEBSITE");
  }

  // Brand from source inventory
  for (const s of payload?.digitalContentReadiness?.sourceInventory || []) {
    if (s.sourceCategory === "BRAND_PROPERTY_PAGE" && s.url) {
      pushCandidate("BRAND_PROPERTY_PAGE", s.url, "SOURCE_INVENTORY");
    }
    if (s.sourceCategory === "DESTINATION_CVB" && s.url) {
      pushCandidate("DESTINATION_CVB", s.url, "SOURCE_INVENTORY");
    }
  }

  // DuckDuckGo site searches for missing critical platforms
  const needDdg = ["TRIPADVISOR", "OTA_BOOKING", "OTA_EXPEDIA", "GOOGLE_BUSINESS_PROFILE", "BRAND_PROPERTY_PAGE"];
  for (const key of needDdg) {
    if (byKey.get(key)?.length) continue;
    const plat = BASELINE_PLATFORMS.find((p) => p.key === key);
    if (!plat?.siteQuery) continue;
    const q = `"${hotelName}" ${market} ${plat.siteQuery}`;
    log.push({ at: new Date().toISOString(), step: "ddg_search", key, query: q });
    if (opts.skipLiveSearch) continue;
    const res = await duckDuckGoSearch(q, { timeoutMs: opts.timeoutMs || 15000 });
    await sleep(400);
    if (!res.ok) {
      log.push({ at: new Date().toISOString(), step: "ddg_fail", key, error: res.error });
      continue;
    }
    for (const u of res.urls) {
      const k = classifyPlatformUrl(u) || key;
      if (k === key) pushCandidate(key, u, "DUCKDUCKGO_SITE_SEARCH");
    }
  }

  // Google Maps search URL as discoverability aid when no listing URL yet
  if (!byKey.get("GOOGLE_BUSINESS_PROFILE")?.length) {
    const mapsQ = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(
      `${hotelName} ${market}`
    )}`;
    pushCandidate("GOOGLE_BUSINESS_PROFILE", mapsQ, "CONSTRUCTED_MAPS_SEARCH");
  }

  const assessments = [];
  for (const plat of BASELINE_PLATFORMS) {
    if (plat.meetingsOnly && !hotelWantsMeetings(payload)) {
      assessments.push({
        platform: plat.label,
        category: plat.key,
        url: null,
        relevant: false,
        assessmentStatus: PLATFORM_ASSESSMENT_STATUSES.NOT_APPLICABLE,
        researchStatus: PLATFORM_RESEARCH_STATUSES.NOT_APPLICABLE,
        notes: "Not prioritized — meetings/group demand not material for this hotel in current brief.",
        researchLog: [{ strategy: "APPLICABILITY", result: "NOT_APPLICABLE" }],
        researchMode: "ZERO_COST_DISCOVERY_V2",
      });
      continue;
    }

    const candidates = byKey.get(plat.key) || [];
    if (!candidates.length) {
      assessments.push({
        platform: plat.label,
        category: plat.key,
        url: null,
        relevant: true,
        assessmentStatus: PLATFORM_ASSESSMENT_STATUSES.NOT_FOUND,
        researchStatus: PLATFORM_RESEARCH_STATUSES.LISTING_NOT_FOUND_AFTER_RESEARCH,
        notes:
          "Bounded discovery (evidence graph + prior pilot seeds + DuckDuckGo site search) did not return a verifiable listing URL. Not classified as an incorrect listing.",
        researchLog: log.filter((l) => l.key === plat.key),
        researchMode: "ZERO_COST_DISCOVERY_V2",
      });
      continue;
    }

    // Probe first few candidates that already passed hotel-scope filter
    let chosen = null;
    let probe = null;
    let id = { score: 0, matched: [] };
    const scoped = candidates.filter(
      (c) =>
        c.strategy === "CONSTRUCTED_MAPS_SEARCH" ||
        c.strategy === "PROPERTY_WEBSITE" ||
        urlBelongsToThisHotel(c.url, propertyId, tokens)
    );
    for (const c of scoped.slice(0, 3)) {
      probe = await probeUrl(c.url, { timeoutMs: opts.timeoutMs || 12000 });
      await sleep(200);
      id = identityMatch(`${probe.text || ""} ${c.url}`, tokens);
      // Constructed maps search pages often thin — accept URL as found with limitation
      if (c.strategy === "CONSTRUCTED_MAPS_SEARCH") {
        chosen = c;
        break;
      }
      const urlIdentity = urlBelongsToThisHotel(c.url, propertyId, tokens);
      if (
        (probe.ok || [401, 403, 429].includes(probe.status)) &&
        (urlIdentity || id.score >= 0.25)
      ) {
        chosen = c;
        break;
      }
      if (urlIdentity && id.score >= 0.15) {
        chosen = c;
        break;
      }
    }
    if (!chosen && scoped.length) chosen = scoped[0];
    if (!chosen) {
      assessments.push({
        platform: plat.label,
        category: plat.key,
        url: null,
        relevant: true,
        assessmentStatus: PLATFORM_ASSESSMENT_STATUSES.NOT_FOUND,
        researchStatus: PLATFORM_RESEARCH_STATUSES.LISTING_NOT_FOUND_AFTER_RESEARCH,
        notes:
          "Candidates were rejected for cross-hotel identity mismatch or failed probes. Not classified as an incorrect listing.",
        researchLog: log.filter((l) => l.key === plat.key),
        researchMode: "ZERO_COST_DISCOVERY_V2",
      });
      continue;
    }

    const accessRestricted =
      [401, 403, 429].includes(probe?.status) ||
      /TIMEOUT|FETCH_ERROR/i.test(probe?.error || "");
    const reachable = probe?.ok === true;
    const isMapsSearchAid =
      chosen.strategy === "CONSTRUCTED_MAPS_SEARCH" ||
      /google\.[^/]+\/maps\/search/i.test(chosen.url || "");
    const urlIdentityOk =
      chosen.strategy === "PROPERTY_WEBSITE" ||
      urlBelongsToThisHotel(chosen.url, propertyId, tokens);
    const identityOk =
      isMapsSearchAid
        ? false
        : (id.score >= 0.2 && urlIdentityOk) ||
          (urlIdentityOk &&
            /SOURCE_INVENTORY|PROPERTY_WEBSITE/i.test(chosen.strategy)) ||
          (urlIdentityOk && id.score >= 0.15);

    let assessmentStatus = PLATFORM_ASSESSMENT_STATUSES.IDENTIFIED_NOT_ASSESSED;
    let notes = "";
    if (isMapsSearchAid) {
      assessmentStatus = PLATFORM_ASSESSMENT_STATUSES.IDENTIFIED_NOT_ASSESSED;
      notes =
        "Google Maps search URL used as a discovery aid. Exact GBP place listing was not confirmed in this bounded pass — treat as research aid, not a verified listing defect.";
    } else if (accessRestricted) {
      assessmentStatus = PLATFORM_ASSESSMENT_STATUSES.ACCESS_RESTRICTED;
      notes =
        "Listing URL found; automated access restricted. Not treated as proof the public listing is broken.";
    } else if (reachable && identityOk) {
      assessmentStatus = PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT;
      notes = `Listing URL verified reachable. Identity tokens matched: ${id.matched.slice(0, 5).join(", ") || "URL/path heuristics"}. Field-level fact accuracy still requires hotel confirmation.`;
    } else if (reachable) {
      assessmentStatus = PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT;
      notes =
        "Listing URL reachable. Identity confirmation is partial — hotel should confirm this is the correct property profile before corrections.";
    } else if (probe?.status === 404) {
      assessmentStatus = PLATFORM_ASSESSMENT_STATUSES.NOT_FOUND;
      notes = "Candidate URL returned 404 under HTTP probe.";
    } else {
      assessmentStatus = PLATFORM_ASSESSMENT_STATUSES.IDENTIFIED_NOT_ASSESSED;
      notes = "Candidate URL recorded; probe incomplete.";
    }

    const researchStatus = isMapsSearchAid
      ? PLATFORM_RESEARCH_STATUSES.RESEARCH_INCOMPLETE
      : mapResearchStatus(assessmentStatus, {
          hasUrl: true,
          identityOk,
          opportunity: false,
        });

    assessments.push({
      platform: plat.label,
      category: plat.key,
      url: chosen.url,
      relevant: true,
      assessmentStatus,
      researchStatus,
      httpStatus: probe?.status ?? null,
      discoveryStrategy: chosen.strategy,
      identityScore: Number(id.score.toFixed(2)),
      identityTokensMatched: id.matched.slice(0, 8),
      notes,
      researchLog: [
        ...log.filter((l) => l.key === plat.key),
        {
          strategy: chosen.strategy,
          url: chosen.url,
          httpStatus: probe?.status ?? null,
          identityScore: id.score,
        },
      ],
      researchMode: "ZERO_COST_DISCOVERY_V2",
      evidenceLabel: humanEvidenceLabel(plat.key, chosen.url),
    });
  }

  const summary = {
    applicable: assessments.filter((a) => a.relevant !== false).length,
    withUrl: assessments.filter((a) => a.url).length,
    verifiedConsistent: assessments.filter(
      (a) => a.researchStatus === PLATFORM_RESEARCH_STATUSES.VERIFIED_CONSISTENT
    ).length,
    accessRestricted: assessments.filter(
      (a) =>
        a.researchStatus ===
        PLATFORM_RESEARCH_STATUSES.LISTING_FOUND_ACCESS_RESTRICTED
    ).length,
    notFoundAfterResearch: assessments.filter(
      (a) =>
        a.researchStatus ===
        PLATFORM_RESEARCH_STATUSES.LISTING_NOT_FOUND_AFTER_RESEARCH
    ).length,
    researchIncomplete: assessments.filter(
      (a) => a.researchStatus === PLATFORM_RESEARCH_STATUSES.RESEARCH_INCOMPLETE
    ).length,
  };

  const completenessGate = {
    pass: assessments
      .filter((a) => a.relevant !== false)
      .every(
        (a) =>
          a.researchStatus !== PLATFORM_RESEARCH_STATUSES.RESEARCH_INCOMPLETE ||
          (a.researchLog && a.researchLog.length > 0)
      ),
    note:
      "Complete means each applicable platform has an evidence-supported assessment OR documented bounded research attempts with a specific unresolved reason. IDENTIFIED_NOT_ASSESSED alone is not sufficient.",
  };

  return {
    contract: PLATFORM_LISTING_DISCOVERY_V2,
    propertyId,
    hotelName,
    generatedAt: new Date().toISOString(),
    researchMode: "ZERO_COST_DISCOVERY_V2",
    paidResearchUsd,
    assessments,
    summary,
    completenessGate,
    researchLog: log,
    limitations: [
      "Zero-cost discovery cannot guarantee GBP place-id resolution.",
      "DuckDuckGo HTML results may be incomplete or geo-variant.",
      "Reachable listing ≠ verified fact accuracy vs Hotel Facts Register.",
      "No paid Exa/Context/DataForSEO spend in this pass.",
    ],
  };
}

export function humanEvidenceLabel(category, url) {
  const host = (() => {
    try {
      return new URL(url).hostname.replace(/^www\./, "");
    } catch {
      return "";
    }
  })();
  switch (category) {
    case "HOTEL_OPERATED_WEBSITE":
      return "View hotel website";
    case "HOTEL_OPERATED_SUBPAGE":
      if (/meeting/i.test(url || "")) return "View hotel meetings page";
      if (/restaurant|dining|food/i.test(url || "")) return "View hotel dining page";
      return "View hotel webpage";
    case "BRAND_PROPERTY_PAGE":
      if (/hilton/i.test(host)) return "View Hilton property profile";
      if (/marriott/i.test(host)) return "View Marriott property profile";
      return "View brand property page";
    case "GOOGLE_BUSINESS_PROFILE":
      return "View Google Maps listing";
    case "TRIPADVISOR":
      return "View Tripadvisor listing";
    case "OTA_BOOKING":
      return "View Booking.com listing";
    case "OTA_EXPEDIA":
      return "View Expedia listing";
    case "APPLE_MAPS":
      return "View Apple Maps listing";
    case "BING_PLACES":
      return "View Bing Maps listing";
    case "CVENT":
      return "View Cvent listing";
    case "DESTINATION_CVB":
      return "View destination listing";
    default:
      if (/greatlocations|golfbreaks|weddingwire/i.test(host)) return "View referring directory";
      return host ? `View ${host}` : "View source";
  }
}

/**
 * Merge discovery assessments into payload platformCoverageAssessment.
 */
export function applyPlatformDiscoveryToPayload(payload, discovery) {
  const prev = payload.platformCoverageAssessment || {};
  const byCat = new Map();
  for (const a of prev.assessments || []) {
    byCat.set(a.category || a.platform, a);
  }
  for (const a of discovery.assessments || []) {
    const prevA = byCat.get(a.category);
    // Fresh V2 discovery always wins for baseline categories (fixes cross-hotel seed contamination)
    const preferDiscovery =
      !prevA ||
      a.researchMode === "ZERO_COST_DISCOVERY_V2" ||
      (!prevA.url && a.url) ||
      (a.url && prevA.assessmentStatus === "IDENTIFIED_NOT_ASSESSED");
    if (preferDiscovery) {
      byCat.set(a.category, {
        ...prevA,
        ...a,
        platform: a.platform || prevA?.platform,
      });
    }
  }
  const assessments = [...byCat.values()];
  const registry = {};
  for (const a of assessments) {
    if (a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT) {
      registry[a.category] = "ASSESSED";
    } else if (a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.ACCESS_RESTRICTED) {
      registry[a.category] = "ACCESS_RESTRICTED";
    } else if (a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.NOT_APPLICABLE) {
      registry[a.category] = "NOT_RELEVANT";
    } else if (
      a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.NOT_FOUND ||
      a.researchStatus === PLATFORM_RESEARCH_STATUSES.LISTING_NOT_FOUND_AFTER_RESEARCH
    ) {
      registry[a.category] = "NOT_FOUND";
    } else if (a.url) {
      registry[a.category] = "ASSESSED";
    } else {
      registry[a.category] = "IDENTIFIED_NOT_ASSESSED";
    }
  }

  const dcr = payload.digitalContentReadiness || {};
  return {
    ...payload,
    platformListingDiscoveryV2: discovery,
    platformCoverageAssessment: {
      ...prev,
      contract: PLATFORM_LISTING_DISCOVERY_V2,
      generatedAt: discovery.generatedAt,
      researchMode: discovery.researchMode,
      paidResearchUsd: discovery.paidResearchUsd,
      assessments,
      platformCoverageRegistry: {
        ...(prev.platformCoverageRegistry || {}),
        ...registry,
      },
      summary: discovery.summary,
      completenessGate: discovery.completenessGate,
      limitations: discovery.limitations,
    },
    digitalContentReadiness: {
      ...dcr,
      platformCoverageRegistry: {
        ...(dcr.platformCoverageRegistry || {}),
        ...registry,
      },
      platformAssessments: assessments,
      methodologyNotes: [
        ...(dcr.methodologyNotes || []).filter(
          (n) => !/Platform coverage assessment V1/i.test(n)
        ),
        "Platform listing discovery V2 — zero-cost multi-strategy research; unassessed ≠ incorrect.",
      ],
    },
  };
}
