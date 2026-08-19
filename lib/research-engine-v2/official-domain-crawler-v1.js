/**
 * Generic official-domain crawler for first-party hotel directories.
 * Expands WHERE we look — not WHAT we accept.
 * Never bypasses auth. Never treats OTAs as HIGH brand evidence.
 */
import {
  SOURCE_DISCOVERY_STATE,
} from "./source-acquisition-registry-v1.js";

export const OFFICIAL_DOMAIN_CRAWLER_VERSION = "official-domain-crawler-v1";

export const OTA_AND_FORBIDDEN_HOSTS = Object.freeze([
  "booking.com",
  "expedia.com",
  "hotels.com",
  "tripadvisor.com",
  "kayak.com",
  "trivago.com",
  "agoda.com",
  "hotelscombined.com",
  "google.com",
  "facebook.com",
  "instagram.com",
  "maps.google.com",
]);

export const CALA_COUNTRY_HINTS = Object.freeze([
  "mexico",
  "brazil",
  "colombia",
  "peru",
  "chile",
  "argentina",
  "dominican",
  "costa-rica",
  "costa rica",
  "panama",
  "jamaica",
  "bahamas",
  "ecuador",
  "uruguay",
  "guatemala",
  "honduras",
  "nicaragua",
  "salvador",
  "bolivia",
  "paraguay",
  "puerto-rico",
  "barbados",
  "aruba",
  "curacao",
]);

const DEFAULT_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (compatible; DealalityCensusBot/1.0; +https://dealality.com)",
  Accept: "text/html,application/xhtml+xml,application/xml,text/plain,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
};

export const LIVE_OFFICIAL_COMPANIES = Object.freeze([
  { id: "marriott", company: "Marriott", domain: "marriott.com", adapter: "marriott_sitemap" },
  { id: "hilton", company: "Hilton", domain: "hilton.com", adapter: "hilton_locations" },
  { id: "ihg", company: "IHG", domain: "ihg.com", adapter: "ihg_destination" },
  { id: "hyatt", company: "Hyatt", domain: "hyatt.com", adapter: "generic_sitemap", sitemap: "https://www.hyatt.com/sitemap.xml" },
  { id: "accor", company: "Accor", domain: "all.accor.com", adapter: "accor_catalog" },
  { id: "wyndham", company: "Wyndham", domain: "wyndhamhotels.com", adapter: "wyndham_sitemap" },
  { id: "choice", company: "Choice", domain: "choicehotels.com", adapter: "choice_regional" },
  { id: "best_western", company: "Best Western", domain: "bestwestern.com", adapter: "generic_sitemap", sitemap: "https://www.bestwestern.com/sitemap.xml" },
  { id: "radisson", company: "Radisson", domain: "radissonhotels.com", adapter: "generic_sitemap", sitemap: "https://www.radissonhotels.com/sitemap.xml" },
  { id: "melia", company: "Melia", domain: "melia.com", adapter: "generic_sitemap", sitemap: "https://www.melia.com/sitemap.xml" },
  { id: "barcelo", company: "Barcelo", domain: "barcelo.com", adapter: "generic_sitemap", sitemap: "https://www.barcelo.com/sitemap.xml" },
  { id: "riu", company: "RIU", domain: "riu.com", adapter: "generic_sitemap", sitemap: "https://www.riu.com/sitemap.xml" },
  { id: "iberostar", company: "Iberostar", domain: "iberostar.com", adapter: "generic_sitemap", sitemap: "https://www.iberostar.com/sitemap.xml" },
  { id: "palladium", company: "Palladium", domain: "palladiumhotelgroup.com", adapter: "generic_sitemap", sitemap: "https://www.palladiumhotelgroup.com/sitemap.xml" },
  { id: "minor", company: "Minor Hotels", domain: "minorhotels.com", adapter: "generic_sitemap", sitemap: "https://www.minorhotels.com/sitemap.xml" },
  { id: "four_seasons", company: "Four Seasons", domain: "fourseasons.com", adapter: "generic_sitemap", sitemap: "https://www.fourseasons.com/sitemap.xml" },
  { id: "rosewood", company: "Rosewood", domain: "rosewoodhotels.com", adapter: "generic_sitemap", sitemap: "https://www.rosewoodhotels.com/sitemap.xml" },
  { id: "mandarin_oriental", company: "Mandarin Oriental", domain: "mandarinoriental.com", adapter: "generic_sitemap", sitemap: "https://www.mandarinoriental.com/sitemap.xml" },
  { id: "aman", company: "Aman", domain: "aman.com", adapter: "generic_sitemap", sitemap: "https://www.aman.com/sitemap.xml" },
  { id: "kerzner", company: "Kerzner", domain: "atlantis.com", adapter: "generic_sitemap", sitemap: "https://www.atlantis.com/sitemap.xml" },
  { id: "bahia_principe", company: "Bahia Principe", domain: "bahia-principe.com", adapter: "generic_sitemap", sitemap: "https://www.bahia-principe.com/sitemap.xml" },
  { id: "hard_rock", company: "Hard Rock", domain: "hardrockhotels.com", adapter: "generic_sitemap", sitemap: "https://www.hardrockhotels.com/sitemap.xml" },
  { id: "karisma", company: "Karisma", domain: "karismahotels.com", adapter: "generic_sitemap", sitemap: "https://www.karismahotels.com/sitemap.xml" },
  { id: "posadas", company: "Grupo Posadas", domain: "posadas.com", adapter: "generic_sitemap", sitemap: "https://www.posadas.com/sitemap.xml" },
  { id: "presidente", company: "Grupo Presidente", domain: "hotelespresidente.com", adapter: "generic_sitemap", sitemap: "https://www.hotelespresidente.com/sitemap.xml" },
  { id: "gaviota", company: "Gaviota", domain: "gaviota-grupo.com", adapter: "generic_sitemap", sitemap: "https://www.gaviota-grupo.com/sitemap.xml" },
  { id: "cubanacan", company: "Cubanacan", domain: "cubanacan.cu", adapter: "generic_sitemap", sitemap: "https://www.cubanacan.cu/sitemap.xml" },
  { id: "gran_caribe", company: "Gran Caribe", domain: "gran-caribe.com", adapter: "generic_sitemap", sitemap: "https://www.gran-caribe.com/sitemap.xml" },
]);

export function hostFromUrl(url) {
  try {
    return new URL(String(url || "").trim()).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

export function isForbiddenHost(urlOrHost) {
  const host = String(urlOrHost || "").includes("://")
    ? hostFromUrl(urlOrHost)
    : String(urlOrHost || "").replace(/^www\./i, "").toLowerCase();
  return OTA_AND_FORBIDDEN_HOSTS.some(
    (d) => host === d || host.endsWith(`.${d}`)
  );
}

export function looksLikePropertyUrl(url) {
  const s = String(url || "").toLowerCase();
  if (!s || isForbiddenHost(s)) return false;
  if (/sitemap|\.xsd\b|\/cdn-cgi\/|wp-json|\/cart\b/i.test(s)) return false;
  return (
    /\/hotels?\//i.test(s) ||
    /\/hotel\//i.test(s) ||
    /hoteldetail/i.test(s) ||
    /\/overview\/?$/i.test(s) ||
    /\/destinations?\//i.test(s) ||
    /ficha|fact-?sheet/i.test(s)
  );
}

export function extractLocsFromXml(xml) {
  const out = [];
  const re = /<loc>\s*([^<\s]+)\s*<\/loc>/gi;
  let m;
  while ((m = re.exec(String(xml || "")))) {
    out.push(m[1].trim());
  }
  return out;
}

export function extractJsonLdHotels(html) {
  const hotels = [];
  const blocks =
    String(html || "").match(
      /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
    ) || [];
  for (const block of blocks) {
    const inner = block.replace(/<\/?script[^>]*>/gi, "").trim();
    try {
      const json = JSON.parse(inner);
      const arr = Array.isArray(json)
        ? json
        : json?.["@graph"]
          ? json["@graph"]
          : [json];
      for (const obj of arr) {
        if (!obj || typeof obj !== "object") continue;
        const types = Array.isArray(obj["@type"]) ? obj["@type"] : [obj["@type"]];
        if (!types.some((t) => /Hotel|LodgingBusiness|Resort/i.test(String(t)))) {
          continue;
        }
        const addr = obj.address || {};
        hotels.push({
          name: String(obj.name || "").trim() || null,
          url: String(obj.url || "").trim() || null,
          brand: String(obj.brand?.name || obj.brand || "").trim() || null,
          city: String(addr.addressLocality || "").trim() || null,
          state: String(addr.addressRegion || "").trim() || null,
          country: String(addr.addressCountry?.name || addr.addressCountry || "").trim() || null,
          address: String(addr.streetAddress || "").trim() || null,
          postal: String(addr.postalCode || "").trim() || null,
          phone: String(obj.telephone || "").trim() || null,
        });
      }
    } catch {
      // ignore malformed JSON-LD
    }
  }
  return hotels;
}

export function extractFactSheetLinks(html, baseUrl) {
  const out = [];
  const re = /href=["']([^"']+\.(?:pdf))["']/gi;
  let m;
  while ((m = re.exec(String(html || "")))) {
    const href = m[1];
    if (!/fact|ficha|brochure|media-kit|sales-kit|meetings/i.test(href)) continue;
    try {
      out.push(new URL(href, baseUrl).toString());
    } catch {
      // skip
    }
  }
  return out;
}

export function classifyBlockedResponse(status, html) {
  const t = String(html || "").slice(0, 2000).toLowerCase();
  if (status === 401 || status === 403) return SOURCE_DISCOVERY_STATE.TEMP_BLOCKED;
  if (status === 429) return SOURCE_DISCOVERY_STATE.TEMP_BLOCKED;
  if (/captcha|access denied|robot check|cf-browser-verification/i.test(t)) {
    return SOURCE_DISCOVERY_STATE.TEMP_BLOCKED;
  }
  if (status === 404) return SOURCE_DISCOVERY_STATE.RETRY_LATER;
  return null;
}

/**
 * Fetch helper with timeout. Does not follow into auth walls.
 */
export async function fetchOfficialText(url, opts = {}) {
  const timeoutMs = Number(opts.timeoutMs || 20000);
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      headers: { ...DEFAULT_HEADERS, ...(opts.headers || {}) },
      redirect: "follow",
      signal: opts.signal || ctrl.signal,
    });
    const text = await res.text();
    return {
      ok: res.ok,
      status: res.status,
      url: res.url || url,
      text,
      blocked: classifyBlockedResponse(res.status, text),
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      text: "",
      error: String(err?.message || err).slice(0, 180),
      blocked: SOURCE_DISCOVERY_STATE.TEMP_BLOCKED,
    };
  } finally {
    clearTimeout(timer);
  }
}

function prefersCala(url) {
  const s = String(url || "").toLowerCase();
  return CALA_COUNTRY_HINTS.some((h) => s.includes(h));
}

/**
 * Crawl an official domain via sitemap + JSON-LD sampling.
 */
export async function crawlOfficialDomain(companyCfg, opts = {}) {
  const maxSitemapFiles = Number(opts.maxSitemapFiles || 12);
  const maxPropertyUrls = Number(opts.maxPropertyUrls || 400);
  const maxPageFetches = Number(opts.maxPageFetches || 40);
  const delayMs = Number(opts.delayMs || 400);
  const fetchFn = opts.fetchFn || fetchOfficialText;
  const sleepFn =
    opts.sleepFn || ((ms) => new Promise((r) => setTimeout(r, ms)));

  const seed = companyCfg.sitemap || `https://www.${companyCfg.domain}/sitemap.xml`;
  const requests = [];
  const errors = [];
  const propertyUrls = [];
  const properties = [];
  let blockedState = null;

  const index = await fetchFn(seed, opts);
  requests.push({ url: seed, status: index.status });
  if (index.blocked) {
    return {
      ok: false,
      company: companyCfg.company,
      domain: companyCfg.domain,
      state: index.blocked,
      requests: requests.length,
      properties: [],
      property_urls: [],
      errors: [index.error || `blocked_${index.status}`],
    };
  }
  if (!index.ok) {
    errors.push(`sitemap_http_${index.status}`);
    return {
      ok: false,
      company: companyCfg.company,
      domain: companyCfg.domain,
      state: SOURCE_DISCOVERY_STATE.RETRY_LATER,
      requests: requests.length,
      properties: [],
      property_urls: [],
      errors,
    };
  }

  let locs = extractLocsFromXml(index.text);
  const sitemapFiles = locs.filter((u) => /\.xml(\.gz)?(\?|$)/i.test(u)).slice(0, maxSitemapFiles);
  const direct = locs.filter((u) => looksLikePropertyUrl(u));
  propertyUrls.push(...direct);

  for (let i = 0; i < sitemapFiles.length; i++) {
    if (propertyUrls.length >= maxPropertyUrls) break;
    if (delayMs) await sleepFn(delayMs);
    const sm = await fetchFn(sitemapFiles[i], opts);
    requests.push({ url: sitemapFiles[i], status: sm.status });
    if (sm.blocked) {
      blockedState = sm.blocked;
      break;
    }
    if (!sm.ok) {
      errors.push(`child_sitemap_${sm.status}`);
      continue;
    }
    const child = extractLocsFromXml(sm.text).filter((u) => looksLikePropertyUrl(u));
    const calaFirst = [
      ...child.filter(prefersCala),
      ...child.filter((u) => !prefersCala(u)),
    ];
    for (const u of calaFirst) {
      if (propertyUrls.length >= maxPropertyUrls) break;
      if (!isForbiddenHost(u)) propertyUrls.push(u);
    }
  }

  const uniqueUrls = [...new Set(propertyUrls)].slice(0, maxPropertyUrls);
  const sample = uniqueUrls.filter(prefersCala).slice(0, maxPageFetches);
  const sampleFallback = sample.length
    ? sample
    : uniqueUrls.slice(0, Math.min(12, maxPageFetches));

  for (let i = 0; i < sampleFallback.length; i++) {
    if (delayMs) await sleepFn(delayMs);
    const page = await fetchFn(sampleFallback[i], opts);
    requests.push({ url: sampleFallback[i], status: page.status });
    if (page.blocked) {
      blockedState = blockedState || page.blocked;
      continue;
    }
    if (!page.ok) {
      errors.push(`page_${page.status}`);
      continue;
    }
    const hotels = extractJsonLdHotels(page.text);
    if (hotels.length) {
      for (const h of hotels) {
        properties.push({
          company: companyCfg.company,
          brand: h.brand || companyCfg.company,
          name: h.name,
          url: h.url || sampleFallback[i],
          city: h.city,
          country: h.country,
          state: h.state,
          address: h.address,
          postal: h.postal,
          phone: h.phone,
          property_code: null,
          source_type: "official_jsonld",
        });
      }
    } else {
      properties.push({
        company: companyCfg.company,
        brand: companyCfg.company,
        name: null,
        url: sampleFallback[i],
        city: null,
        country: null,
        state: null,
        address: null,
        postal: null,
        phone: null,
        property_code: null,
        source_type: "official_property_url_only",
      });
    }
  }

  return {
    ok: true,
    company: companyCfg.company,
    domain: companyCfg.domain,
    state: blockedState || SOURCE_DISCOVERY_STATE.DISCOVERING,
    requests: requests.length,
    pages_discovered: uniqueUrls.length,
    properties,
    property_urls: uniqueUrls,
    errors,
  };
}
