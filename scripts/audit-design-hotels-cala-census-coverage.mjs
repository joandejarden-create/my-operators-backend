#!/usr/bin/env node
/**
 * Audit: Design Hotels CALA properties (designhotels.com sitemap) vs Hotel Census.
 *
 *   node scripts/audit-design-hotels-cala-census-coverage.mjs
 *   node scripts/audit-design-hotels-cala-census-coverage.mjs --csv
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { COUNTRY_CONFIG_LIST } from "../lib/radar-buildout/country-configs.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const SITEMAP_URL = "https://www.designhotels.com/sitemap.xml";
const OUT_JSON = join("reports", "design-hotels-cala-census-audit.json");
const OUT_CSV = join("reports", "design-hotels-cala-census-gaps.csv");
const WRITE_CSV = process.argv.includes("--csv");

/** Design Hotels URL country slug → Census country label */
const SLUG_TO_CENSUS_COUNTRY = {
  mexico: "Mexico",
  colombia: "Colombia",
  brazil: "Brazil",
  argentina: "Argentina",
  chile: "Chile",
  peru: "Peru",
  ecuador: "Ecuador",
  bolivia: "Bolivia",
  uruguay: "Uruguay",
  paraguay: "Paraguay",
  venezuela: "Venezuela",
  "costa-rica": "Costa Rica",
  panama: "Panama",
  guatemala: "Guatemala",
  honduras: "Honduras",
  "el-salvador": "El Salvador",
  nicaragua: "Nicaragua",
  belize: "Belize",
  cuba: "Cuba",
  jamaica: "Jamaica",
  bahamas: "Bahamas",
  "puerto-rico": "Puerto Rico",
  "dominican-republic": "Dominican Republic",
  "saint-lucia": "Saint Lucia",
  "saint-lucia": "Saint Lucia",
  grenada: "Grenada",
  barbados: "Barbados",
  aruba: "Aruba",
  curacao: "Curaçao",
  "trinidad-and-tobago": "Trinidad and Tobago",
  "cayman-islands": "Cayman Islands",
  "turks-and-caicos": "Turks & Caicos",
  "antigua-and-barbuda": "Antigua and Barbuda",
  "saint-vincent-and-the-grenadines": "Saint Vincent and the Grenadines",
  dominica: "Dominica",
  "saint-kitts-and-nevis": "Saint Kitts and Nevis",
  "british-virgin-islands": "British Virgin Islands",
  haiti: "Haiti",
  "us-virgin-islands": "U.S. Virgin Islands",
  martinique: "Martinique",
  guadeloupe: "Guadeloupe",
  bonaire: "Bonaire",
  suriname: "Suriname",
  guyana: "Guyana",
};

const CALA_COUNTRY_SET = new Set(COUNTRY_CONFIG_LIST);

function normalizeName(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function slugFromUrl(url) {
  const parts = new URL(url).pathname.split("/").filter(Boolean);
  // hotels / country / [region] / city / slug
  if (parts[0] !== "hotels" || parts.length < 4) return "";
  return parts[parts.length - 1];
}

function countrySlugFromUrl(url) {
  const parts = new URL(url).pathname.split("/").filter(Boolean);
  return parts[1] || "";
}

function censusCountryFromSlug(slug) {
  const key = String(slug || "").toLowerCase();
  if (SLUG_TO_CENSUS_COUNTRY[key]) return SLUG_TO_CENSUS_COUNTRY[key];
  // fallback: title case hyphenated
  const guess = key
    .split("-")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
  return CALA_COUNTRY_SET.has(guess) ? guess : guess;
}

function isCalaCountry(countryLabel) {
  return CALA_COUNTRY_SET.has(countryLabel);
}

const SUBPAGE_SLUGS = new Set([
  "rooms-rates",
  "location-details",
  "gallery",
  "dining",
  "wellness",
  "meetings",
  "experiences",
  "offers",
  "reviews",
]);

function propertyBaseKey(url) {
  const parts = new URL(url).pathname.split("/").filter(Boolean);
  if (parts[0] !== "hotels" || parts.length < 4) return "";
  const last = parts[parts.length - 1].toLowerCase();
  if (SUBPAGE_SLUGS.has(last)) return parts.slice(0, -1).join("/");
  return parts.join("/");
}

function parseSitemapPropertyUrls(xml) {
  const locs = [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1].trim());
  /** @type {Map<string, string>} */
  const byBase = new Map();
  for (const u of locs) {
    try {
      const p = new URL(u).pathname;
      const parts = p.split("/").filter(Boolean);
      if (parts[0] !== "hotels" || parts.length < 4) continue;
      const last = parts[parts.length - 1].toLowerCase();
      if (SUBPAGE_SLUGS.has(last)) continue;
      const base = propertyBaseKey(u);
      if (base) byBase.set(base, u.endsWith("/") ? u : `${u}/`);
    } catch {
      /* skip */
    }
  }
  return [...byBase.values()];
}

function extractHotelNameFromUrl(url) {
  const slug = slugFromUrl(url);
  return slug
    .split("-")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

async function fetchSitemapHotels() {
  const res = await fetch(SITEMAP_URL, {
    headers: { "User-Agent": "Mozilla/5.0 (compatible; DealalityAudit/1.0)" },
  });
  if (!res.ok) throw new Error(`sitemap http_${res.status}`);
  const xml = await res.text();
  const urls = parseSitemapPropertyUrls(xml);
  return urls.map((propertyUrl) => {
    const countrySlug = countrySlugFromUrl(propertyUrl);
    const censusCountry = censusCountryFromSlug(countrySlug);
    return {
      propertyUrl,
      countrySlug,
      censusCountry,
      slug: slugFromUrl(propertyUrl),
      nameGuess: extractHotelNameFromUrl(propertyUrl),
      isCala: isCalaCountry(censusCountry),
    };
  });
}

function isDesignHotelsCensusCandidate(fields) {
  const name = String(fields?.name || "");
  const affiliation = String(fields?.[CENSUS_FIELDS.affiliation] || fields?.Affiliation || "");
  const website = String(fields?.Website || "");
  return (
    /design hotels/i.test(affiliation) ||
    /designhotels\.com/i.test(website)
  );
}

function scoreMatch(source, census) {
  const srcName = normalizeName(source.nameGuess);
  const srcSlug = normalizeName(source.slug.replace(/-/g, " "));
  const censusName = normalizeName(census.fields.name);
  const censusUrl = String(census.fields.Website || "").toLowerCase();
  const srcUrl = source.propertyUrl.toLowerCase();

  if (censusUrl && (censusUrl === srcUrl || censusUrl.includes(source.slug))) {
    return { score: 100, reason: "website_exact" };
  }
  if (censusName === srcName || censusName === srcSlug) {
    return { score: 95, reason: "name_exact" };
  }
  if (censusName.includes(srcSlug) || srcSlug.includes(censusName)) {
    return { score: 85, reason: "name_contains" };
  }
  if (censusName.includes(srcName) || srcName.includes(censusName)) {
    return { score: 80, reason: "name_guess_contains" };
  }
  // token overlap (ignore generic hotel words)
  const stop = new Set(["hotel", "hotels", "the", "and", "member", "design", "boutique", "spa"]);
  const srcTokens = new Set(
    (srcName + " " + srcSlug)
      .split(" ")
      .filter((t) => t.length > 2 && !stop.has(t))
  );
  const censusTokens = censusName.split(" ").filter((t) => t.length > 2 && !stop.has(t));
  let overlap = 0;
  for (const t of censusTokens) if (srcTokens.has(t)) overlap++;
  if (overlap >= 2) return { score: 60 + overlap * 5, reason: "token_overlap" };
  if (overlap === 1 && censusTokens.length <= 2 && srcTokens.size <= 2) {
    // Avoid false positives like slug "wake-biohotel" → census "Wake"
    const onlyToken = censusTokens[0] || "";
    if (onlyToken.length >= 5 || srcSlug.includes(onlyToken)) {
      return { score: 65, reason: "single_token_short_name" };
    }
  }
  return { score: 0, reason: "none" };
}

async function loadCensusCalaRows() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        "Website",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.city,
        CENSUS_FIELDS.status,
      ],
    })
    .all();

  return records.filter((r) => isCalaCountry(r.fields[CENSUS_FIELDS.country]));
}

async function main() {
  mkdirSync("reports", { recursive: true });

  console.log("Fetching Design Hotels sitemap…");
  const allHotels = await fetchSitemapHotels();
  const calaHotels = allHotels.filter((h) => h.isCala);
  const nonCalaMapped = allHotels.filter((h) => !h.isCala && SLUG_TO_CENSUS_COUNTRY[h.countrySlug]);

  console.log(`Sitemap property URLs: ${allHotels.length}`);
  console.log(`CALA (country-config match): ${calaHotels.length}`);

  console.log("\nLoading CALA Hotel Census rows…");
  const censusRows = await loadCensusCalaRows();
  const censusDesignTagged = censusRows.filter((r) => isDesignHotelsCensusCandidate(r.fields));
  console.log(`Census CALA rows: ${censusRows.length}`);
  console.log(`Census CALA rows name-tagged Design Hotels: ${censusDesignTagged.length}`);

  /** @type {object[]} */
  const matched = [];
  /** @type {object[]} */
  const gaps = [];
  const usedCensus = new Set();

  for (const source of calaHotels) {
    let best = null;
    for (const rec of censusRows) {
      if (rec.fields[CENSUS_FIELDS.country] !== source.censusCountry) continue;
      const { score, reason } = scoreMatch(source, rec);
      if (score >= 60 && (!best || score > best.score)) {
        best = { rec, score, reason };
      }
    }
    if (best) {
      usedCensus.add(best.rec.id);
      matched.push({
        propertyUrl: source.propertyUrl,
        slug: source.slug,
        censusCountry: source.censusCountry,
        censusRecordId: best.rec.id,
        censusName: best.rec.fields.name,
        matchScore: best.score,
        matchReason: best.reason,
        censusWebsite: best.rec.fields.Website || "",
      });
    } else {
      gaps.push({
        propertyUrl: source.propertyUrl,
        slug: source.slug,
        nameGuess: source.nameGuess,
        censusCountry: source.censusCountry,
        countrySlug: source.countrySlug,
      });
    }
  }

  const censusOnly = censusRows
    .filter((r) => !usedCensus.has(r.id))
    .filter((r) => {
      // Census-only list: Affiliation-tagged Design Hotels not matched to sitemap
      const affiliation = String(
        r.fields[CENSUS_FIELDS.affiliation] || r.fields?.Affiliation || ""
      );
      return /design hotels/i.test(affiliation);
    })
    .map((r) => ({
      censusRecordId: r.id,
      censusName: r.fields.name,
      censusCountry: r.fields[CENSUS_FIELDS.country],
      censusCity: r.fields[CENSUS_FIELDS.city] || "",
      website: r.fields.Website || "",
    }));

  const byCountry = {};
  for (const h of calaHotels) {
    byCountry[h.censusCountry] = byCountry[h.censusCountry] || { total: 0, matched: 0, gaps: 0 };
    byCountry[h.censusCountry].total++;
  }
  for (const m of matched) {
    if (byCountry[m.censusCountry]) byCountry[m.censusCountry].matched++;
  }
  for (const g of gaps) {
    if (byCountry[g.censusCountry]) byCountry[g.censusCountry].gaps++;
  }

  const report = {
    generatedAt: new Date().toISOString(),
    source: SITEMAP_URL,
    calaDefinition: "lib/radar-buildout/country-configs.js COUNTRY_CONFIG_LIST",
    sitemapPropertyCount: allHotels.length,
    calaPropertyCount: calaHotels.length,
    censusCalaDesignHotelsCount: censusDesignTagged.length,
    censusCalaRowCount: censusRows.length,
    matchedCount: matched.length,
    gapCount: gaps.length,
    censusOnlyCount: censusOnly.length,
    coveragePct:
      calaHotels.length > 0 ? Math.round((matched.length / calaHotels.length) * 1000) / 10 : 0,
    byCountry,
    gaps,
    matched,
    censusOnly,
    nonCalaSlugWarnings: nonCalaMapped.slice(0, 20),
  };

  writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));

  if (WRITE_CSV) {
    writeCsv(OUT_CSV, gaps, [
      "censusCountry",
      "slug",
      "nameGuess",
      "propertyUrl",
      "countrySlug",
    ]);
  }

  console.log("\n=== Design Hotels CALA vs Census ===");
  console.log(`CALA on designhotels.com: ${calaHotels.length}`);
  console.log(`Matched in census:        ${matched.length} (${report.coveragePct}%)`);
  console.log(`Gaps (not in census):     ${gaps.length}`);
  console.log(`Census-only (extra):      ${censusOnly.length}`);
  console.log(`\nWrote ${OUT_JSON}`);
  if (WRITE_CSV) console.log(`Wrote ${OUT_CSV}`);

  if (gaps.length) {
    console.log("\nSample gaps:");
    for (const g of gaps.slice(0, 15)) {
      console.log(`  [${g.censusCountry}] ${g.nameGuess} — ${g.propertyUrl}`);
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
