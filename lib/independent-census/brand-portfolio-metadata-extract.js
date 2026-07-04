/**
 * Official brand/portfolio property page metadata extract (report-only by default).
 * Single-page fetch only — no crawl, no booking/rate endpoints, no raw HTML storage.
 */

import { readFileSync } from "fs";
import * as cheerio from "cheerio";
import { CALA_FILTER_STATUS } from "./brand-directory-property-url-extract.js";
import { parseChoicePropertyUrl } from "./brand-directory-property-url-extract.js";
import { isUrlAllowedByRobots } from "./sources/brand-directory.js";
import { normalizePropertyUrl } from "./choice-property-id-reconciliation.js";
import {
  normalizeCountry,
  normalizeKey,
  normalizeText,
  normalizePhone,
  websiteHost,
} from "./match-current-census.js";

/** Align with sitemap property-path tooling (Choice allows this identifier). */
export const USER_AGENT =
  "DealalityPropertyPathAnalysis/1.0 (research; report-only)";

export const FETCH_TIMEOUT_MS = 45000;
export const MAX_HTML_BYTES = 2_500_000;

/** Path segments that must not be fetched. */
const BLOCKED_PATH_RE =
  /\/(rates|reservations|booking|confirmation|guestdata|modify)(\/|$)/i;

const BLOCKED_QUERY_KEYS =
  /^(rate|rates|price|avail|availability|book|booking|reservation|confirm|modify|guest)/i;

const HOTEL_SCHEMA_TYPES = new Set([
  "hotel",
  "lodgingbusiness",
  "motel",
  "resort",
  "hostel",
  "guesthouse",
  "apartment",
  "place",
]);

export function loadPropertyUrlReport(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const rows = Array.isArray(data.propertyRows) ? data.propertyRows : [];
  return { data, rows };
}

export function filterChoiceCalaIncludedRows(rows, parentCompany) {
  const parentK = normalizeKey(parentCompany);
  return rows.filter((r) => {
    if (r.calaFilterStatus !== CALA_FILTER_STATUS.INCLUDED) return false;
    if (parentK && !normalizeKey(r.parentCompany).includes(parentK)) {
      if (!normalizeKey(r.parentCompany).includes("choice")) return false;
    }
    const url = normalizeText(r.propertyUrl);
    return !!url && url.startsWith("http");
  });
}

export function isPropertyUrlFetchAllowed(url) {
  try {
    const u = new URL(url);
    if (u.search && u.search.length > 1) {
      for (const [k] of u.searchParams) {
        if (BLOCKED_QUERY_KEYS.test(k)) {
          return { allowed: false, reason: `Blocked query parameter: ${k}` };
        }
      }
    }
    if (BLOCKED_PATH_RE.test(u.pathname)) {
      return { allowed: false, reason: "Blocked booking/rate path segment" };
    }
    if (!parseChoicePropertyUrl(url)) {
      return { allowed: false, reason: "Not a canonical Choice property URL path" };
    }
    return { allowed: true, reason: "" };
  } catch {
    return { allowed: false, reason: "Invalid URL" };
  }
}

function parseJsonLdBlocks(html) {
  const blocks = [];
  const re =
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = re.exec(html))) {
    const raw = m[1].trim();
    if (!raw) continue;
    try {
      blocks.push(JSON.parse(raw));
    } catch {
      /* ignore malformed JSON-LD */
    }
  }
  return blocks;
}

function flattenJsonLd(node, out = []) {
  if (!node) return out;
  if (Array.isArray(node)) {
    for (const item of node) out.push(...flattenJsonLd(item, []));
    return out;
  }
  if (typeof node === "object") {
    if (node["@graph"]) flattenJsonLd(node["@graph"], out);
    else out.push(node);
    for (const v of Object.values(node)) {
      if (v && typeof v === "object") flattenJsonLd(v, out);
    }
  }
  return out;
}

function schemaTypeMatches(types) {
  const list = Array.isArray(types) ? types : [types];
  return list.some((t) => {
    const k = normalizeKey(String(t).replace(/^.*\//, ""));
    return HOTEL_SCHEMA_TYPES.has(k);
  });
}

function pickHotelJsonLd(blocks) {
  const flat = [];
  for (const b of blocks) flat.push(...flattenJsonLd(b));
  return (
    flat.find((n) => schemaTypeMatches(n["@type"])) ||
    flat.find((n) => n.name && (n.address || n.geo || n.telephone)) ||
    null
  );
}

function parsePostalAddress(addr) {
  if (!addr || typeof addr !== "object") return {};
  if (typeof addr === "string") return { address: normalizeText(addr) };
  return {
    address: normalizeText(
      [addr.streetAddress, addr.addressLocality, addr.addressRegion, addr.postalCode]
        .filter(Boolean)
        .join(", ")
    ),
    city: normalizeText(addr.addressLocality),
    stateOrRegion: normalizeText(addr.addressRegion),
    postalCode: normalizeText(addr.postalCode),
    country: normalizeText(addr.addressCountry),
  };
}

function parseGeo(geo) {
  if (!geo || typeof geo !== "object") return { latitude: null, longitude: null };
  const lat = Number(geo.latitude ?? geo.lat);
  const lng = Number(geo.longitude ?? geo.lng ?? geo.lon);
  return {
    latitude: Number.isFinite(lat) ? lat : null,
    longitude: Number.isFinite(lng) ? lng : null,
  };
}

function metaContent($, names) {
  for (const name of names) {
    const el = $(`meta[property="${name}"], meta[name="${name}"]`).first();
    const c = el.attr("content");
    if (c) return normalizeText(c);
  }
  return "";
}

export function extractMetadataFromHtml(html, pageUrl, seed = {}) {
  const $ = cheerio.load(html, { decodeEntities: true });
  const fieldsFound = [];
  const warnings = [];

  const jsonLd = pickHotelJsonLd(parseJsonLdBlocks(html));
  let officialHotelName = "";
  let address = "";
  let city = "";
  let stateOrRegion = "";
  let country = "";
  let postalCode = "";
  let latitude = null;
  let longitude = null;
  let phone = "";
  let website = "";

  if (jsonLd) {
    if (jsonLd.name) {
      officialHotelName = normalizeText(jsonLd.name);
      fieldsFound.push("jsonLd.name");
    }
    const addr = parsePostalAddress(jsonLd.address);
    if (addr.address) {
      address = addr.address;
      fieldsFound.push("jsonLd.address");
    }
    if (addr.city) {
      city = addr.city;
      fieldsFound.push("jsonLd.city");
    }
    if (addr.stateOrRegion) {
      stateOrRegion = addr.stateOrRegion;
      fieldsFound.push("jsonLd.region");
    }
    if (addr.country) {
      country = addr.country;
      fieldsFound.push("jsonLd.country");
    }
    if (addr.postalCode) {
      postalCode = addr.postalCode;
      fieldsFound.push("jsonLd.postalCode");
    }
    const geo = parseGeo(jsonLd.geo);
    if (geo.latitude != null) {
      latitude = geo.latitude;
      longitude = geo.longitude;
      fieldsFound.push("jsonLd.geo");
    }
    if (jsonLd.telephone) {
      phone = normalizeText(jsonLd.telephone);
      fieldsFound.push("jsonLd.telephone");
    }
    if (jsonLd.url) {
      website = normalizeText(jsonLd.url);
      fieldsFound.push("jsonLd.url");
    }
  }

  const ogTitle = metaContent($, ["og:title", "twitter:title"]);
  const metaTitle = $("title").first().text();
  const h1 = $("h1").first().text();
  if (!officialHotelName && ogTitle) {
    officialHotelName = ogTitle.split("|")[0].trim();
    fieldsFound.push("meta.og:title");
  }
  if (!officialHotelName && metaTitle) {
    officialHotelName = metaTitle.split("|")[0].trim();
    fieldsFound.push("meta.title");
  }
  if (!officialHotelName && h1) {
    officialHotelName = normalizeText(h1);
    fieldsFound.push("visible.h1");
    warnings.push("Used visible H1; prefer JSON-LD when available.");
  }

  if (!country) {
    country = metaContent($, ["og:country", "geo.region"]);
    if (country) fieldsFound.push("meta.country");
  }

  const latMeta = metaContent($, ["place:location:latitude", "og:latitude"]);
  const lngMeta = metaContent($, ["place:location:longitude", "og:longitude"]);
  if (latitude == null && latMeta && lngMeta) {
    latitude = Number(latMeta);
    longitude = Number(lngMeta);
    if (Number.isFinite(latitude)) fieldsFound.push("meta.geo");
  }

  if (!phone) {
    phone = metaContent($, ["og:phone_number", "contact:phone_number"]);
    if (phone) fieldsFound.push("meta.phone");
  }

  if (!website) {
    const ogUrl = metaContent($, ["og:url"]);
    website = ogUrl || pageUrl;
    if (ogUrl) fieldsFound.push("meta.og:url");
  }

  const brand =
    seed.inferredBrandName ||
    seed.matchedBrandSetupBrand ||
    seed.brandSlug ||
    "";
  const parentCompany = seed.parentCompany || "Choice Hotels International";

  if (!country && seed.inferredCountry) {
    country = seed.inferredCountry;
    fieldsFound.push("sitemap.inferredCountry");
  }
  if (!city && seed.citySlug) {
    city = seed.citySlug.replace(/-/g, " ");
    fieldsFound.push("sitemap.citySlug");
  }

  const uniqueFields = [...new Set(fieldsFound)];
  let extractionConfidence = "none";
  if (officialHotelName && latitude != null && longitude != null && country) {
    extractionConfidence = "high";
  } else if (officialHotelName && country && (city || address)) {
    extractionConfidence = "medium";
  } else if (officialHotelName) {
    extractionConfidence = "low";
  }

  return {
    officialHotelName,
    brand,
    parentCompany,
    officialPropertyUrl: pageUrl,
    propertyId: seed.propertyId || "",
    address,
    city,
    stateOrRegion,
    country: normalizeCountry(country) || country,
    postalCode,
    latitude,
    longitude,
    phone: normalizePhone(phone) || phone,
    website: website || pageUrl,
    sourceFetchedAt: new Date().toISOString(),
    extractionConfidence,
    fieldsFound: uniqueFields,
    extractionWarnings: warnings,
  };
}

const robotsCacheByOrigin = new Map();

async function isUrlAllowedByRobotsCached(url, fetchFn = globalThis.fetch) {
  let origin;
  try {
    origin = new URL(url).origin;
  } catch {
    return false;
  }
  if (robotsCacheByOrigin.has(origin)) {
    return robotsCacheByOrigin.get(origin);
  }
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 15000);
  try {
    const allowed = await isUrlAllowedByRobots(url, (u, init = {}) =>
      fetchFn(u, { ...init, signal: ctrl.signal })
    );
    robotsCacheByOrigin.set(origin, allowed);
    return allowed;
  } catch {
    robotsCacheByOrigin.set(origin, true);
    return true;
  } finally {
    clearTimeout(timer);
  }
}

export async function fetchPropertyPageHtml(
  url,
  fetchFn = globalThis.fetch,
  userAgent = USER_AGENT
) {
  const allowed = await isUrlAllowedByRobotsCached(url, fetchFn);
  if (!allowed) {
    return { ok: false, error: "robots.txt disallows URL", robotsBlocked: true };
  }

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetchFn(url, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        "User-Agent": userAgent,
        Accept: "text/html,application/xhtml+xml",
      },
    });
    clearTimeout(timer);

    if (!res.ok) {
      return { ok: false, status: res.status, error: `HTTP ${res.status}` };
    }

    const ct = (res.headers.get("content-type") || "").toLowerCase();
    if (!ct.includes("html") && !ct.includes("text")) {
      return { ok: false, error: `Non-HTML content-type: ${ct}` };
    }

    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length > MAX_HTML_BYTES) {
      return { ok: false, error: `Response too large (${buf.length} bytes)` };
    }

    const finalUrl = res.url || url;
    const pathCheck = isPropertyUrlFetchAllowed(finalUrl);
    if (!pathCheck.allowed) {
      return { ok: false, error: pathCheck.reason, redirectedBlocked: true };
    }

    return { ok: true, html: buf.toString("utf8"), finalUrl };
  } catch (err) {
    clearTimeout(timer);
    return { ok: false, error: err.message || String(err) };
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {object} opts
 */
export async function runBrandPortfolioMetadataExtract(opts) {
  const userAgent = opts.userAgent || USER_AGENT;
  const { rows } = loadPropertyUrlReport(opts.propertyUrlReportPath);
  const filtered = filterChoiceCalaIncludedRows(rows, opts.parentCompany);
  const maxPages =
    opts.maxPages != null ? Number(opts.maxPages) : filtered.length;
  const toProcess = filtered.slice(0, maxPages);

  const results = [];
  const policyWarnings = [];
  let robotsBlocked = 0;
  let fetchFailed = 0;
  let extractedOk = 0;
  let http403Count = 0;
  const delayMs = Number(opts.requestDelayMs) || 600;

  for (let i = 0; i < toProcess.length; i++) {
    const row = toProcess[i];
    const url = normalizeText(row.propertyUrl);
    const parsed = parseChoicePropertyUrl(url);

    const urlCheck = isPropertyUrlFetchAllowed(url);
    if (!urlCheck.allowed) {
      results.push({
        propertyId: row.propertyId,
        officialPropertyUrl: url,
        fetchStatus: "skipped_blocked_url",
        error: urlCheck.reason,
        calaFilterStatus: row.calaFilterStatus,
        inferredCountry: row.inferredCountry,
        targetBrand: row.matchedBrandSetupBrand || row.inferredBrandName,
      });
      continue;
    }

    if (i > 0 && delayMs > 0) await sleep(delayMs);

    const fetchResult = await fetchPropertyPageHtml(url, opts.fetchFn, userAgent);
    if (!fetchResult.ok) {
      if (fetchResult.robotsBlocked) robotsBlocked++;
      else fetchFailed++;
      if (fetchResult.status === 403) http403Count++;
      results.push({
        propertyId: row.propertyId,
        officialPropertyUrl: url,
        fetchStatus: fetchResult.robotsBlocked
          ? "robots_disallowed"
          : "fetch_failed",
        error: fetchResult.error,
        httpStatus: fetchResult.status,
        calaFilterStatus: row.calaFilterStatus,
        inferredCountry: row.inferredCountry,
        targetBrand: row.matchedBrandSetupBrand || row.inferredBrandName,
      });
      continue;
    }

    const meta = extractMetadataFromHtml(fetchResult.html, fetchResult.finalUrl, {
      ...row,
      ...parsed,
      propertyId: row.propertyId || parsed?.propertyId,
      citySlug: row.citySlug || parsed?.citySlug,
    });

    if (meta.extractionConfidence !== "none") extractedOk++;

    results.push({
      propertyId: row.propertyId || parsed?.propertyId,
      officialPropertyUrl: fetchResult.finalUrl,
      fetchStatus: "ok",
      ...meta,
      calaFilterStatus: row.calaFilterStatus,
      inferredCountry: row.inferredCountry,
      targetBrand: meta.brand,
      sourceRecordId: row.sourceRecordId,
    });
  }

  const fieldCoverage = {
    name: results.filter((r) => r.officialHotelName).length,
    address: results.filter((r) => r.address).length,
    city: results.filter((r) => r.city).length,
    country: results.filter((r) => r.country).length,
    latLng: results.filter(
      (r) => r.latitude != null && r.longitude != null
    ).length,
    phone: results.filter((r) => r.phone).length,
    brand: results.filter((r) => r.brand).length,
  };

  if (!opts.sourcePolicyApproved) {
    policyWarnings.push(
      "source_policy_not_approved: report-only; no Airtable writes without --source-policy-approved and --apply"
    );
  }
  policyWarnings.push(
    "no_raw_html_stored",
    "single_page_fetch_only",
    "no_rates_booking_reviews_photos"
  );
  if (http403Count > 0) {
    policyWarnings.push(
      `http_403_count_${http403Count}: site may rate-limit; retry later with --request-delay-ms 2000 or approved user-agent`
    );
  }

  const apply = !!opts.apply && !!opts.sourcePolicyApproved;
  let writtenCount = 0;
  if (apply && !opts.sourcePolicyApproved) {
    throw new Error("--apply requires --source-policy-approved");
  }

  return {
    batchId: opts.batchId,
    parentCompany: opts.parentCompany || "Choice Hotels International",
    propertyUrlReportPath: opts.propertyUrlReportPath,
    calaIncludedInReport: filtered.length,
    pagesAttempted: toProcess.length,
    pagesExtractedOk: extractedOk,
    robotsBlocked,
    fetchFailed,
    http403Count,
    userAgent,
    fieldCoverage,
    sourcePolicyWarnings: policyWarnings,
    results,
    apply,
    writtenCount,
    dryRun: !apply,
    airtableWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    verifiedTableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    ratesBookingReviewsPhotosFetched: false,
    privateApiUsed: false,
    rawHtmlStored: false,
  };
}

export const EXTRACT_CSV_COLUMNS = [
  "propertyId",
  "officialPropertyUrl",
  "fetchStatus",
  "officialHotelName",
  "brand",
  "parentCompany",
  "address",
  "city",
  "stateOrRegion",
  "country",
  "postalCode",
  "latitude",
  "longitude",
  "phone",
  "website",
  "extractionConfidence",
  "fieldsFound",
  "extractionWarnings",
  "error",
];

export function extractRowToCsv(row) {
  return {
    propertyId: row.propertyId || "",
    officialPropertyUrl: row.officialPropertyUrl || "",
    fetchStatus: row.fetchStatus || "",
    officialHotelName: row.officialHotelName || "",
    brand: row.brand || "",
    parentCompany: row.parentCompany || "",
    address: row.address || "",
    city: row.city || "",
    stateOrRegion: row.stateOrRegion || "",
    country: row.country || "",
    postalCode: row.postalCode || "",
    latitude: row.latitude ?? "",
    longitude: row.longitude ?? "",
    phone: row.phone || "",
    website: row.website || "",
    extractionConfidence: row.extractionConfidence || "",
    fieldsFound: Array.isArray(row.fieldsFound)
      ? row.fieldsFound.join(";")
      : row.fieldsFound || "",
    extractionWarnings: Array.isArray(row.extractionWarnings)
      ? row.extractionWarnings.join(";")
      : row.extractionWarnings || "",
    error: row.error || "",
  };
}
