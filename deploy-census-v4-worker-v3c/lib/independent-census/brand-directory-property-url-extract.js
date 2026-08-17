/**
 * Phase 4J — Extract property URLs from brand-directory sitemap XML (report-only).
 */

import { readFileSync } from "fs";
import { gunzipSync } from "zlib";
import { extractSitemapLocs, isUrlAllowedByRobots } from "./sources/brand-directory.js";
import { SOURCE_TYPES } from "./fields.js";
import { normalizeBrandKey } from "./brand-setup-cala-inventory.js";
import {
  buildBrandSlugIndex,
  slugFromBrandName,
  loadBrandSeedsFile,
} from "./brand-directory-sitemap-review.js";

const USER_AGENT = "DealalityPropertyPathAnalysis/1.0 (research; report-only)";
const FETCH_TIMEOUT_MS = 180000;

export const CHOICE_PROPERTY_PATH =
  /^https:\/\/(?:www\.)?choicehotels\.com\/[^/]+\/[^/]+\/[^/]+\/[a-z0-9]{3,8}$/i;

export const CALA_FILTER_STATUS = {
  INCLUDED: "included",
  UNCERTAIN: "uncertain",
  EXCLUDED: "excluded_non_cala",
};

export const RECOMMENDED_ACTIONS = {
  READY: "ready_for_candidate_review",
  COUNTRY_REVIEW: "needs_country_review",
  UNMATCHED_BRAND: "unmatched_brand_review",
  EXCLUDE_NON_CALA: "exclude_non_cala",
  HOLD_POLICY: "hold_for_source_policy_review",
};

export const SOURCE_NAME = "Choice Hotels Sitemap";
export const SOURCE_LICENSE = "source_specific_terms";
export const SOURCE_POLICY = "review_required";

/** CALA country/region URL segments (normalized slugs). */
export const CALA_COUNTRY_SEGMENTS = new Set(
  [
    "mexico",
    "dominican-republic",
    "puerto-rico",
    "jamaica",
    "bahamas",
    "barbados",
    "aruba",
    "curacao",
    "costa-rica",
    "panama",
    "colombia",
    "peru",
    "chile",
    "argentina",
    "brazil",
    "guatemala",
    "honduras",
    "nicaragua",
    "el-salvador",
    "trinidad-and-tobago",
    "belize",
    "cayman-islands",
    "turks-and-caicos",
    "st-lucia",
    "st-maarten",
    "saint-martin",
    "antigua-and-barbuda",
    "grenada",
    "guyana",
    "suriname",
    "uruguay",
    "paraguay",
    "ecuador",
    "bolivia",
    "venezuela",
    "cuba",
    "haiti",
    "martinique",
    "guadeloupe",
    "bonaire",
    "sint-maarten",
    "us-virgin-islands",
    "british-virgin-islands",
  ].map(normalizeBrandKey)
);

/** Known non-CALA segments (US states, Canada provinces, major non-LATAM countries). */
export const NON_CALA_SEGMENTS = new Set(
  [
    "united-states",
    "usa",
    "us",
    "canada",
    "alberta",
    "british-columbia",
    "ontario",
    "quebec",
    "united-kingdom",
    "england",
    "scotland",
    "wales",
    "ireland",
    "france",
    "germany",
    "austria",
    "italy",
    "spain",
    "portugal",
    "netherlands",
    "belgium",
    "switzerland",
    "poland",
    "czech-republic",
    "hungary",
    "romania",
    "sweden",
    "norway",
    "denmark",
    "finland",
    "greece",
    "turkey",
    "israel",
    "uae",
    "united-arab-emirates",
    "saudi-arabia",
    "qatar",
    "india",
    "china",
    "japan",
    "south-korea",
    "thailand",
    "vietnam",
    "indonesia",
    "malaysia",
    "philippines",
    "singapore",
    "australia",
    "new-zealand",
    "northern-territory",
    "new-south-wales",
    "queensland",
    "victoria",
    "western-australia",
    "south-australia",
    "tasmania",
    "australian-capital-territory",
    "australia-capital-territory",
    "andorra",
    "monaco",
    "luxembourg",
    "iceland",
    "russia",
    "ukraine",
    "south-africa",
    "egypt",
    "morocco",
    "kenya",
    "nigeria",
    // US states / territories (Choice uses state slug as first path segment)
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "district-of-columbia",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new-hampshire",
    "new-jersey",
    "new-mexico",
    "new-york",
    "north-carolina",
    "north-dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode-island",
    "south-carolina",
    "south-dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west-virginia",
    "wisconsin",
    "wyoming",
    "american-samoa",
    "guam",
    "northern-mariana-islands",
  ].map(normalizeBrandKey)
);

const SEGMENT_TO_COUNTRY_LABEL = {
  "dominican-republic": "Dominican Republic",
  mexico: "Mexico",
  "puerto-rico": "Puerto Rico",
  jamaica: "Jamaica",
  bahamas: "Bahamas",
  barbados: "Barbados",
  aruba: "Aruba",
  curacao: "Curaçao",
  "costa-rica": "Costa Rica",
  panama: "Panama",
  colombia: "Colombia",
  peru: "Peru",
  chile: "Chile",
  argentina: "Argentina",
  brazil: "Brazil",
  guatemala: "Guatemala",
  honduras: "Honduras",
  nicaragua: "Nicaragua",
  "el-salvador": "El Salvador",
  "trinidad-and-tobago": "Trinidad and Tobago",
  belize: "Belize",
  "cayman-islands": "Cayman Islands",
  "turks-and-caicos": "Turks and Caicos",
  "st-lucia": "St. Lucia",
  cuba: "Cuba",
  haiti: "Haiti",
  ecuador: "Ecuador",
  bolivia: "Bolivia",
  venezuela: "Venezuela",
  uruguay: "Uruguay",
  paraguay: "Paraguay",
  guyana: "Guyana",
  suriname: "Suriname",
  grenada: "Grenada",
};

export async function fetchSitemapXml(url, fetchFn = globalThis.fetch) {
  const allowed = await isUrlAllowedByRobots(url, fetchFn);
  if (!allowed) {
    return { ok: false, error: "robots.txt disallows URL" };
  }

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetchFn(url, {
      signal: ctrl.signal,
      headers: { "User-Agent": USER_AGENT, Accept: "application/xml,text/xml" },
      redirect: "follow",
    });
    clearTimeout(timer);
    if (!res.ok) return { ok: false, status: res.status };
    const buf = Buffer.from(await res.arrayBuffer());
    const isGzip =
      url.toLowerCase().endsWith(".gz") ||
      (res.headers.get("content-type") || "").includes("gzip");
    const text = isGzip ? gunzipSync(buf).toString("utf8") : buf.toString("utf8");
    return { ok: true, text, gzip: isGzip };
  } catch (err) {
    clearTimeout(timer);
    return { ok: false, error: err.message || String(err) };
  }
}

export function parseChoicePropertyUrl(url) {
  if (!CHOICE_PROPERTY_PATH.test(url)) return null;
  try {
    const u = new URL(url);
    const segments = u.pathname.split("/").filter(Boolean);
    if (segments.length < 4) return null;
    const countryOrRegionSegment = segments[0];
    const citySlug = segments[1];
    const brandSlugRaw = segments[2];
    const propertyId = segments[3];
    const brandSlug = normalizeBrandKey(brandSlugRaw.replace(/-hotels?$/i, ""));
    return {
      propertyUrl: u.href,
      countryOrRegionSegment,
      citySlug,
      brandSlugRaw,
      brandSlug,
      propertyId,
      inferredBrandName: titleCaseFromSlug(brandSlug),
    };
  } catch {
    return null;
  }
}

function titleCaseFromSlug(slug) {
  return slug
    .split("-")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

export function classifyCalaSegment(segment) {
  const key = normalizeBrandKey(segment);
  if (CALA_COUNTRY_SEGMENTS.has(key)) {
    return {
      calaFilterStatus: CALA_FILTER_STATUS.INCLUDED,
      inferredCountry: SEGMENT_TO_COUNTRY_LABEL[key] || titleCaseFromSlug(key),
    };
  }
  if (NON_CALA_SEGMENTS.has(key)) {
    return {
      calaFilterStatus: CALA_FILTER_STATUS.EXCLUDED,
      inferredCountry: SEGMENT_TO_COUNTRY_LABEL[key] || titleCaseFromSlug(key),
    };
  }
  return {
    calaFilterStatus: CALA_FILTER_STATUS.UNCERTAIN,
    inferredCountry: SEGMENT_TO_COUNTRY_LABEL[key] || titleCaseFromSlug(key),
  };
}

export function matchBrandSlugToSetup(brandSlug, brandIndex) {
  if (!brandSlug) return { matchedBrandSetupBrand: "", matchMethod: "none" };

  const direct = brandIndex.get(brandSlug);
  if (direct?.length) {
    return { matchedBrandSetupBrand: direct[0].brand, matchMethod: "exact_slug" };
  }

  for (const [slug, entries] of brandIndex.entries()) {
    if (!slug) continue;
    if (brandSlug.includes(slug) || slug.includes(brandSlug)) {
      return { matchedBrandSetupBrand: entries[0].brand, matchMethod: "partial_slug" };
    }
  }

  return { matchedBrandSetupBrand: "", matchMethod: "none" };
}

function resolveRecommendedAction(row) {
  if (row.calaFilterStatus === CALA_FILTER_STATUS.EXCLUDED) {
    return RECOMMENDED_ACTIONS.EXCLUDE_NON_CALA;
  }
  if (row.calaFilterStatus === CALA_FILTER_STATUS.UNCERTAIN) {
    return RECOMMENDED_ACTIONS.COUNTRY_REVIEW;
  }
  if (!row.matchedBrandSetupBrand) {
    return RECOMMENDED_ACTIONS.UNMATCHED_BRAND;
  }
  return RECOMMENDED_ACTIONS.READY;
}

function buildNotes(row) {
  const parts = [
    "URL-only sitemap extract; no property HTML.",
    `CALA: ${row.calaFilterStatus}.`,
    row.matchedBrandSetupBrand
      ? `Matched Brand Setup: ${row.matchedBrandSetupBrand}.`
      : "No Brand Setup brand match for slug.",
    "Candidate ingest blocked until source policy sign-off.",
  ];
  return parts.join(" ");
}

/**
 * @param {object} opts
 */
export async function extractChoicePropertyUrls(opts) {
  const {
    propertySitemapUrl,
    parentCompany = "Choice Hotels International",
    seeds,
    regionFilter = "",
    countryFilter = "",
    maxUrls = null,
    fetchFn = globalThis.fetch,
  } = opts;

  const brandIndex = buildBrandSlugIndex(seeds);
  const seedBrandNames = seeds.map((s) => s.brand).filter(Boolean);

  const fetchRes = await fetchSitemapXml(propertySitemapUrl, fetchFn);
  if (!fetchRes.ok) {
    return { ok: false, error: fetchRes.error || "Sitemap fetch failed" };
  }

  const locs = extractSitemapLocs(fetchRes.text);
  const rows = [];
  const byBrand = {};
  const bySegment = {};
  let calaIncluded = 0;
  let calaUncertain = 0;
  let calaExcluded = 0;
  const matchedBrands = new Set();

  for (const url of locs) {
    if (maxUrls != null && rows.length >= maxUrls) break;
    const parsed = parseChoicePropertyUrl(url);
    if (!parsed) continue;

    const cala = classifyCalaSegment(parsed.countryOrRegionSegment);
    const brandMatch = matchBrandSlugToSetup(parsed.brandSlug, brandIndex);

    const row = {
      parentCompany,
      matchedBrandSetupBrand: brandMatch.matchedBrandSetupBrand,
      inferredBrandName: parsed.inferredBrandName,
      brandSlug: parsed.brandSlug,
      propertyUrl: parsed.propertyUrl,
      propertyId: parsed.propertyId,
      countryOrRegionSegment: parsed.countryOrRegionSegment,
      inferredCountry: cala.inferredCountry,
      citySlug: parsed.citySlug,
      calaFilterStatus: cala.calaFilterStatus,
      sourceName: SOURCE_NAME,
      sourceType: SOURCE_TYPES.BRAND_DIRECTORY,
      sourceLicense: SOURCE_LICENSE,
      sourcePolicy: SOURCE_POLICY,
      sourceUrl: propertySitemapUrl,
      sourceRecordId: `choice-sitemap-${parsed.propertyId}`,
      requiresManualReview: true,
      brandMatchMethod: brandMatch.matchMethod,
    };

    row.recommendedAction = resolveRecommendedAction(row);
    row.notes = buildNotes(row);

    if (countryFilter) {
      const cf = normalizeBrandKey(countryFilter);
      const seg = normalizeBrandKey(parsed.countryOrRegionSegment);
      if (cf !== seg && !normalizeBrandKey(cala.inferredCountry).includes(cf)) {
        continue;
      }
    }

    if (regionFilter && normalizeBrandKey(regionFilter) === "cala") {
      if (cala.calaFilterStatus === CALA_FILTER_STATUS.EXCLUDED) {
        calaExcluded++;
        if (opts.includeExcludedInOutput === false) continue;
      } else if (cala.calaFilterStatus === CALA_FILTER_STATUS.UNCERTAIN) {
        calaUncertain++;
      } else {
        calaIncluded++;
      }
    } else {
      if (cala.calaFilterStatus === CALA_FILTER_STATUS.INCLUDED) calaIncluded++;
      else if (cala.calaFilterStatus === CALA_FILTER_STATUS.UNCERTAIN) calaUncertain++;
      else calaExcluded++;
    }

    if (row.matchedBrandSetupBrand) matchedBrands.add(row.matchedBrandSetupBrand);

    byBrand[row.matchedBrandSetupBrand || "(unmatched)"] =
      (byBrand[row.matchedBrandSetupBrand || "(unmatched)"] || 0) + 1;
    bySegment[parsed.countryOrRegionSegment] =
      (bySegment[parsed.countryOrRegionSegment] || 0) + 1;

    rows.push(row);
  }

  const unmatchedBrands = seedBrandNames.filter((b) => !matchedBrands.has(b));

  let recommendedNextAction = RECOMMENDED_ACTIONS.HOLD_POLICY;
  if (calaIncluded >= 100 && matchedBrands.size >= 10) {
    recommendedNextAction = "match_to_osm_candidates_next";
  }

  const calaLikelyRows = rows.filter(
    (r) => r.calaFilterStatus === CALA_FILTER_STATUS.INCLUDED
  );

  return {
    ok: true,
    propertySitemapUrl,
    parentCompany,
    totalSitemapLocs: locs.length,
    totalPropertyUrlsParsed: rows.length,
    calaIncludedCount: calaIncluded,
    calaUncertainCount: calaUncertain,
    excludedNonCalaCount: calaExcluded,
    calaLikelyPropertyUrlCount: calaIncluded,
    matchedBrandSetupBrandCount: matchedBrands.size,
    unmatchedBrandCount: unmatchedBrands.length,
    matchedBrandSetupBrands: [...matchedBrands].sort(),
    unmatchedBrandSetupBrands: unmatchedBrands,
    countByBrand: byBrand,
    countByCountryOrRegionSegment: bySegment,
    sampleUrls: calaLikelyRows.slice(0, 25).map((r) => r.propertyUrl),
    recommendedNextAction,
    sourceRiskNotes:
      "brand_directory source_specific_terms; review_required; URL-only extract; no STR/CoStar; no property HTML.",
    rows,
  };
}

export const EXTRACT_CSV_COLUMNS = [
  "Parent Company",
  "Matched Brand Setup Brand",
  "Inferred Brand Name",
  "Brand Slug",
  "Property URL",
  "Property ID",
  "Country / Region Segment",
  "Inferred Country",
  "City Slug",
  "CALA Filter Status",
  "Source Name",
  "Source Type",
  "Source URL",
  "Source Record ID",
  "Requires Manual Review",
  "Recommended Action",
  "Notes",
];

export function extractRowToCsv(r) {
  return {
    "Parent Company": r.parentCompany,
    "Matched Brand Setup Brand": r.matchedBrandSetupBrand,
    "Inferred Brand Name": r.inferredBrandName,
    "Brand Slug": r.brandSlug,
    "Property URL": r.propertyUrl,
    "Property ID": r.propertyId,
    "Country / Region Segment": r.countryOrRegionSegment,
    "Inferred Country": r.inferredCountry,
    "City Slug": r.citySlug,
    "CALA Filter Status": r.calaFilterStatus,
    "Source Name": r.sourceName,
    "Source Type": r.sourceType,
    "Source URL": r.sourceUrl,
    "Source Record ID": r.sourceRecordId,
    "Requires Manual Review": r.requiresManualReview ? "yes" : "no",
    "Recommended Action": r.recommendedAction,
    Notes: r.notes,
  };
}

export { loadBrandSeedsFile };
