/**
 * Phase 4I — Sitemap index + limited child sitemap review (URLs only, report-only).
 */

import { readFileSync } from "fs";
import { gunzipSync } from "zlib";
import { extractSitemapLocs, isUrlAllowedByRobots } from "./sources/brand-directory.js";
import { getSourceRiskLevel } from "./source-policy.js";
import { SOURCE_TYPES } from "./fields.js";
import { normalizeBrandKey } from "./brand-setup-cala-inventory.js";

const USER_AGENT = "DealalityPropertyPathAnalysis/1.0 (research; report-only)";
const FETCH_TIMEOUT_MS = 120000;

export const URL_CLASSES = {
  PROPERTY: "likely_property_url",
  BRAND: "likely_brand_url",
  CITY: "likely_city_or_destination_url",
  MARKETING: "likely_marketing_url",
  UNKNOWN: "unknown",
};

export const NEXT_ACTIONS = {
  FEASIBLE: "property_url_extraction_feasible",
  MANUAL: "needs_manual_sitemap_review",
  PERMISSIONED: "permissioned_export_preferred",
  NOT_RECOMMENDED: "not_recommended",
};

const CHILD_SITEMAP_PRIORITY = [
  /propertysitemap/i,
  /citysrp|city-srp/i,
  /statebrand/i,
  /brandsearch/i,
  /countrysearch/i,
  /enausitemap|en-us/i,
  /encasitemap/i,
];

const CHILD_SITEMAP_HINT =
  /hotel|property|properties|location|locations|citysrp|statebrand|brandsearch|countrysearch/i;
const CHILD_SITEMAP_SKIP =
  /blog|career|press|news|image|static|css|js|legal|privacy|amenitysearch|meetingevents|landingpages|explore/i;

const BOOKING_BLOCK = /\/(book|booking|reserve|checkout|availability|rates?)\b/i;

const PROPERTY_PATH =
  /\/hotels?\/[a-z0-9][a-z0-9-]{2,}|\/[a-z]{2}(?:-[a-z]{2})?\/[a-z0-9-]+\/hotel|property-details|\/hotel\//i;

/** Choice corporate pattern: /{region}/{city}/{brand-slug}-hotels/{propertyId} */
const CHOICE_PROPERTY_PATH =
  /^https:\/\/(?:www\.)?choicehotels\.com\/[^/]+\/[^/]+\/[^/]+\/[a-z0-9]{3,8}$/i;

const BRAND_PATH = /^\/[a-z0-9-]{2,}\/?$/i;

const CITY_PATH =
  /\/(destinations?|cities|locations?|regions?|states?)\/|\/en-[a-z]{2}\/[a-z-]+\/?$/i;

const MARKETING_PATH =
  /\/(about|offers|promo|rewards|franchise|development|media|contact|faq|terms|privacy)\b/i;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchText(url, fetchFn = globalThis.fetch) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetchFn(url, {
      signal: ctrl.signal,
      headers: { "User-Agent": USER_AGENT, Accept: "application/xml,text/xml,text/plain" },
      redirect: "follow",
    });
    clearTimeout(timer);
    if (!res.ok) return { ok: false, status: res.status, text: "" };
    const buf = Buffer.from(await res.arrayBuffer());
    const isGzip =
      url.toLowerCase().endsWith(".gz") ||
      (res.headers.get("content-type") || "").includes("gzip");
    const text = isGzip ? gunzipSync(buf).toString("utf8") : buf.toString("utf8");
    return { ok: true, status: res.status, text, finalUrl: res.url, gzip: isGzip };
  } catch (err) {
    clearTimeout(timer);
    return { ok: false, error: err.message || String(err) };
  }
}

export function slugFromBrandName(name) {
  return normalizeBrandKey(name).replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

export function buildBrandSlugIndex(seeds) {
  const index = new Map();
  for (const seed of seeds) {
    const brand = String(seed.brand || "").trim();
    if (!brand) continue;
    let pathSlug = "";
    try {
      const u = new URL(seed.sourceUrl || "");
      const parts = u.pathname.split("/").filter(Boolean);
      pathSlug = parts[parts.length - 1] || "";
      if (pathSlug === "" && parts.length === 0 && u.hostname.includes("woodspring")) {
        pathSlug = "woodspring";
      }
    } catch {
      /* ignore */
    }
    const entry = {
      brand,
      brandStatus: seed.brandStatus || "",
      sourceUrl: seed.sourceUrl || "",
      pathSlug: normalizeBrandKey(pathSlug),
      nameSlug: slugFromBrandName(brand),
    };
    const keys = new Set([entry.pathSlug, entry.nameSlug].filter(Boolean));
    for (const k of keys) {
      if (!index.has(k)) index.set(k, []);
      index.get(k).push(entry);
    }
  }
  return index;
}

export function isChildSitemapRelevant(childUrl) {
  const lower = childUrl.toLowerCase();
  if (CHILD_SITEMAP_SKIP.test(lower)) return false;
  return CHILD_SITEMAP_HINT.test(lower);
}

export function rankChildSitemap(childUrl) {
  const lower = childUrl.toLowerCase();
  for (let i = 0; i < CHILD_SITEMAP_PRIORITY.length; i++) {
    if (CHILD_SITEMAP_PRIORITY[i].test(lower)) return i;
  }
  return 99;
}

export function classifySitemapUrl(url) {
  if (BOOKING_BLOCK.test(url)) return URL_CLASSES.MARKETING;

  if (CHOICE_PROPERTY_PATH.test(url)) return URL_CLASSES.PROPERTY;

  let pathname = "";
  try {
    pathname = new URL(url).pathname;
  } catch {
    return URL_CLASSES.UNKNOWN;
  }

  if (PROPERTY_PATH.test(pathname) || /\/[a-z]{2}\/[a-z0-9-]+-[a-z0-9-]+/.test(pathname)) {
    if (!MARKETING_PATH.test(pathname)) return URL_CLASSES.PROPERTY;
  }
  if (CITY_PATH.test(pathname)) return URL_CLASSES.CITY;
  if (MARKETING_PATH.test(pathname)) return URL_CLASSES.MARKETING;
  if (BRAND_PATH.test(pathname) || pathname.split("/").filter(Boolean).length <= 1) {
    return URL_CLASSES.BRAND;
  }
  return URL_CLASSES.UNKNOWN;
}

export function inferUrlParts(url) {
  let pathname = "";
  let host = "";
  try {
    const u = new URL(url);
    pathname = u.pathname;
    host = u.hostname.replace(/^www\./, "");
  } catch {
    return { brandSlug: "", country: "", state: "", city: "", hotelSlug: "" };
  }

  const segments = pathname.split("/").filter(Boolean);
  let brandSlug = "";
  let country = "";
  let state = "";
  let city = "";
  let hotelSlug = "";

  if (host.includes("choicehotels") && segments.length >= 4) {
    country = segments[0];
    city = segments[1];
    brandSlug = segments[2].replace(/-hotels?$/i, "");
    hotelSlug = segments[3];
    return {
      brandSlug: normalizeBrandKey(brandSlug),
      country,
      state: "",
      city,
      hotelSlug: normalizeBrandKey(hotelSlug),
    };
  }

  if (host.includes("woodspring") && segments.length === 0) {
    brandSlug = "woodspring";
  }

  const locale = segments[0]?.match(/^en-[a-z]{2}$/i);
  if (locale) {
    country = segments[0].slice(3).toUpperCase();
    segments.shift();
  }

  if (segments[0] === "hotels" || segments[0] === "hotel") {
    hotelSlug = segments.slice(1).join("/");
    brandSlug = segments[1] || "";
  } else if (segments.length >= 2 && /hotel|property/i.test(pathname)) {
    hotelSlug = segments[segments.length - 1];
    brandSlug = segments[0];
  } else if (segments.length === 1) {
    brandSlug = segments[0];
  } else if (segments.length >= 2) {
    brandSlug = segments[0];
    const tail = segments[segments.length - 1];
    if (tail.includes("-")) {
      const parts = tail.split("-");
      if (parts.length >= 2) {
        city = parts.slice(0, -1).join(" ");
        state = parts[parts.length - 1].length <= 3 ? parts[parts.length - 1] : "";
      }
      hotelSlug = tail;
    }
  }

  return {
    brandSlug: normalizeBrandKey(brandSlug),
    country,
    state: state.toUpperCase(),
    city,
    hotelSlug: normalizeBrandKey(hotelSlug),
  };
}

export function matchUrlToChoiceBrand(inferred, brandIndex) {
  const candidates = [];
  for (const key of [inferred.brandSlug, inferred.hotelSlug]) {
    if (!key) continue;
    const hits = brandIndex.get(key);
    if (hits) candidates.push(...hits);
    for (const [slug, entries] of brandIndex.entries()) {
      if (slug && (key.includes(slug) || slug.includes(key))) {
        candidates.push(...entries);
      }
    }
  }
  const seen = new Set();
  const unique = [];
  for (const c of candidates) {
    if (!seen.has(c.brand)) {
      seen.add(c.brand);
      unique.push(c.brand);
    }
  }
  return unique;
}

export function loadBrandSeedsFile(path) {
  const data = JSON.parse(readFileSync(path, "utf8"));
  return Array.isArray(data) ? data : data.seeds || [];
}

/**
 * @param {object} opts
 */
export async function reviewSitemapIndex(opts) {
  const {
    sitemapUrl,
    seeds,
    maxChildSitemaps = 5,
    maxUrls = 500,
    fetchFn = globalThis.fetch,
    delayMs = 300,
  } = opts;

  const brandIndex = buildBrandSlugIndex(seeds);
  const seedBrands = seeds.map((s) => s.brand).filter(Boolean);

  const originCache = new Map();
  async function allowedCached(url) {
    try {
      const origin = new URL(url).origin;
      if (!originCache.has(origin)) {
        originCache.set(origin, await isUrlAllowedByRobots(url, fetchFn));
      }
      return originCache.get(origin);
    } catch {
      return true;
    }
  }

  const allowed = await allowedCached(sitemapUrl);
  if (!allowed) {
    return {
      ok: false,
      error: "robots.txt disallows sitemap index URL",
      recommendedNextAction: NEXT_ACTIONS.NOT_RECOMMENDED,
    };
  }

  const indexRes = await fetchText(sitemapUrl, fetchFn);
  if (!indexRes.ok) {
    return {
      ok: false,
      error: indexRes.error || `HTTP ${indexRes.status}`,
      recommendedNextAction: NEXT_ACTIONS.MANUAL,
    };
  }

  const allChildren = extractSitemapLocs(indexRes.text).filter((u) =>
    /\.xml/i.test(u)
  );
  const relevantChildren = allChildren
    .filter(isChildSitemapRelevant)
    .sort((a, b) => rankChildSitemap(a) - rankChildSitemap(b));
  const toInspect = (
    relevantChildren.length ? relevantChildren : allChildren
  ).slice(0, maxChildSitemaps);

  const classCounts = {
    [URL_CLASSES.PROPERTY]: 0,
    [URL_CLASSES.BRAND]: 0,
    [URL_CLASSES.CITY]: 0,
    [URL_CLASSES.MARKETING]: 0,
    [URL_CLASSES.UNKNOWN]: 0,
  };

  const parsedUrls = [];
  const matchedBrands = new Set();
  let totalParsed = 0;

  for (const childUrl of toInspect) {
    const childAllowed = await allowedCached(childUrl);
    if (!childAllowed) continue;
    const childRes = await fetchText(childUrl, fetchFn);
    await sleep(delayMs);
    if (!childRes.ok) continue;

    const locs = extractSitemapLocs(childRes.text);
    for (const url of locs) {
      if (totalParsed >= maxUrls) break;
      if (BOOKING_BLOCK.test(url)) continue;
      totalParsed++;
      const urlClass = classifySitemapUrl(url);
      classCounts[urlClass] = (classCounts[urlClass] || 0) + 1;
      const inferred = inferUrlParts(url);
      const matched = matchUrlToChoiceBrand(inferred, brandIndex);
      matched.forEach((b) => matchedBrands.add(b));

      if (parsedUrls.length < maxUrls) {
        parsedUrls.push({
          url,
          urlClass,
          inferredBrandSlug: inferred.brandSlug,
          inferredHotelSlug: inferred.hotelSlug,
          inferredCountry: inferred.country,
          inferredState: inferred.state,
          inferredCity: inferred.city,
          matchedChoiceBrands: matched,
          childSitemap: childUrl,
        });
      }
    }
    if (totalParsed >= maxUrls) break;
  }

  const propertySamples = parsedUrls
    .filter((r) => r.urlClass === URL_CLASSES.PROPERTY)
    .slice(0, 25);

  const unmatchedBrands = seedBrands.filter((b) => !matchedBrands.has(b));

  let recommendedNextAction = NEXT_ACTIONS.MANUAL;
  if (classCounts[URL_CLASSES.PROPERTY] >= 50 && matchedBrands.size >= 5) {
    recommendedNextAction = NEXT_ACTIONS.FEASIBLE;
  } else if (classCounts[URL_CLASSES.PROPERTY] >= 10) {
    recommendedNextAction = NEXT_ACTIONS.MANUAL;
  } else if (classCounts[URL_CLASSES.PROPERTY] < 5) {
    recommendedNextAction = NEXT_ACTIONS.PERMISSIONED;
  }

  const sourceRiskNotes = [
    `Source type: ${SOURCE_TYPES.BRAND_DIRECTORY}; risk: ${getSourceRiskLevel(SOURCE_TYPES.BRAND_DIRECTORY)}.`,
    "URLs parsed from sitemap XML only — no property HTML fetched.",
    "No rates, availability, reviews, or booking endpoints stored.",
    "Human review required before Candidate ingest per source policy.",
  ].join(" ");

  return {
    ok: true,
    sitemapUrl,
    parentCompany: opts.parentCompany || "",
    batchId: opts.batchId || "",
    seedBrandCount: seeds.length,
    childSitemapsFound: allChildren.length,
    childSitemapsRelevant: relevantChildren.length,
    childSitemapsInspected: toInspect.length,
    childSitemapUrlsInspected: toInspect,
    childSitemapUrlsFoundSample: allChildren.slice(0, 30),
    totalUrlsParsed: totalParsed,
    urlClassCounts: classCounts,
    likelyPropertyUrlCount: classCounts[URL_CLASSES.PROPERTY],
    likelyBrandUrlCount: classCounts[URL_CLASSES.BRAND],
    likelyCityDestinationUrlCount: classCounts[URL_CLASSES.CITY],
    likelyMarketingUrlCount: classCounts[URL_CLASSES.MARKETING],
    matchedChoiceBrandCount: matchedBrands.size,
    matchedChoiceBrands: [...matchedBrands].sort(),
    unmatchedChoiceBrands: unmatchedBrands,
    propertyUrlSamples: propertySamples,
    samplePropertyUrlPatterns: [
      ...new Set(
        propertySamples.map((p) => {
          try {
            return new URL(p.url).pathname.replace(/\/[a-z0-9-]{8,}$/i, "/{hotel-slug}");
          } catch {
            return p.url;
          }
        })
      ),
    ].slice(0, 15),
    sourceRiskNotes,
    recommendedNextAction,
    parsedUrlRows: parsedUrls,
  };
}

export const SITEMAP_REVIEW_CSV_COLUMNS = [
  "url",
  "urlClass",
  "inferredBrandSlug",
  "inferredHotelSlug",
  "inferredCountry",
  "inferredState",
  "inferredCity",
  "matchedChoiceBrands",
  "childSitemap",
];

export function sitemapRowToCsv(r) {
  return {
    url: r.url,
    urlClass: r.urlClass,
    inferredBrandSlug: r.inferredBrandSlug,
    inferredHotelSlug: r.inferredHotelSlug,
    inferredCountry: r.inferredCountry,
    inferredState: r.inferredState,
    inferredCity: r.inferredCity,
    matchedChoiceBrands: (r.matchedChoiceBrands || []).join("; "),
    childSitemap: r.childSitemap,
  };
}
