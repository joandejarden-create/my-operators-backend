/**
 * Design Hotels (designhotels.com) → Hotel Census match helpers.
 */

import { COUNTRY_CONFIG_LIST } from "./radar-buildout/country-configs.js";
import { CENSUS_FIELDS } from "./hotel-census/fields.js";

export const DESIGN_HOTELS_SITEMAP_URL = "https://www.designhotels.com/sitemap.xml";
export const DESIGN_HOTELS_AFFILIATION = "Design Hotels";
export const DESIGN_HOTELS_PARENT_COMPANY = "Marriott International";

export const SLUG_TO_CENSUS_COUNTRY = {
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

/** Destination / city index pages — not individual hotels. */
export const DESTINATION_SLUG_BLOCKLIST = new Set([
  "la-paz",
  "valle-de-guadalupe",
  "puerto-escondido",
  "la-punta-zicatela",
  "monte-gordo",
  "ixtapa-zihuatanejo",
  "merida",
  "oaxaca",
  "guadalajara",
  "leon",
  "puebla",
  "tulum",
  "playa-del-carmen",
  "mexico-city",
  "san-miguel-de-allende",
  "san-jose-del-cabo",
  "bacalar",
  "queretaro",
  "medellin",
  "quito",
  "nosara",
  "monteverde",
  "antigua-guatemala",
  "st-george-s-grenada",
  "soufriere",
  "la-paz",
]);

/** Slug → census record ID steward overrides (audit false negatives). */
export const STEWARD_CENSUS_RECORD_BY_SLUG = {
  habita: "recxPk7vq5OYDU4Uy",
  carlota: "rec1PXkMUZKr5SvTO",
  laluna: "recaokE8psIbuVeuH",
  "lo-sereno": "rec1ybTMCedaIh0sZ",
  "elena-de-cobre": "recGnLZK24av3ayRc",
  "hotel-matilda": "recSA1Fdhn4A0ocy4",
  "otro-oaxaca": "recKXf2LSje5f0kZ8",
  "rosas-and-xocolate": "recFSeOMQvWrFK6Em",
  "hotel-la-semilla": "reczyAyzSdBgnbqQp",
  "hotelito-at-musa": "recFS4to73D6ZG0fG",
};

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

export function normalizeName(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function slugFromPropertyUrl(url) {
  const parts = new URL(url).pathname.split("/").filter(Boolean);
  if (parts[0] !== "hotels" || parts.length < 4) return "";
  return parts[parts.length - 1];
}

export function countrySlugFromPropertyUrl(url) {
  const parts = new URL(url).pathname.split("/").filter(Boolean);
  return parts[1] || "";
}

export function censusCountryFromSlug(slug) {
  const key = String(slug || "").toLowerCase();
  if (SLUG_TO_CENSUS_COUNTRY[key]) return SLUG_TO_CENSUS_COUNTRY[key];
  const guess = key
    .split("-")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
  return CALA_COUNTRY_SET.has(guess) ? guess : guess;
}

export function isCalaCountry(countryLabel) {
  return CALA_COUNTRY_SET.has(countryLabel);
}

function propertyBaseKey(url) {
  const parts = new URL(url).pathname.split("/").filter(Boolean);
  if (parts[0] !== "hotels" || parts.length < 4) return "";
  const last = parts[parts.length - 1].toLowerCase();
  if (SUBPAGE_SLUGS.has(last)) return parts.slice(0, -1).join("/");
  return parts.join("/");
}

export function parseDesignHotelsSitemapPropertyUrls(xml) {
  const locs = [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1].trim());
  /** @type {Map<string, string>} */
  const byBase = new Map();
  for (const u of locs) {
    try {
      const parts = new URL(u).pathname.split("/").filter(Boolean);
      if (parts[0] !== "hotels" || parts.length < 4) continue;
      const last = parts[parts.length - 1].toLowerCase();
      if (SUBPAGE_SLUGS.has(last)) continue;
      if (DESTINATION_SLUG_BLOCKLIST.has(last)) continue;
      const base = propertyBaseKey(u);
      if (base) byBase.set(base, u.endsWith("/") ? u : `${u}/`);
    } catch {
      /* skip */
    }
  }
  return [...byBase.values()];
}

export async function fetchDesignHotelsCalaProperties(fetchFn = globalThis.fetch) {
  const res = await fetchFn(DESIGN_HOTELS_SITEMAP_URL, {
    headers: { "User-Agent": "Mozilla/5.0 (compatible; DealalityAudit/1.0)" },
  });
  if (!res.ok) throw new Error(`design_hotels_sitemap_http_${res.status}`);
  const xml = await res.text();
  return parseDesignHotelsSitemapPropertyUrls(xml)
    .map((propertyUrl) => {
      const countrySlug = countrySlugFromPropertyUrl(propertyUrl);
      const censusCountry = censusCountryFromSlug(countrySlug);
      const slug = slugFromPropertyUrl(propertyUrl);
      return {
        propertyUrl,
        countrySlug,
        censusCountry,
        slug,
        nameGuess: slug
          .split("-")
          .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
          .join(" "),
        isCala: isCalaCountry(censusCountry),
      };
    })
    .filter((h) => h.isCala);
}

export function scoreDesignHotelsCensusMatch(source, census) {
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
  // Avoid "Wake" matching "wake-biohotel"
  const minLen = Math.min(censusName.length, srcSlug.length);
  const maxLen = Math.max(censusName.length, srcSlug.length);
  if (maxLen > 0 && minLen / maxLen < 0.55) {
    return { score: 0, reason: "name_length_mismatch" };
  }
  if (censusName.includes(srcSlug) || srcSlug.includes(censusName)) {
    return { score: 85, reason: "name_contains" };
  }
  if (censusName.includes(srcName) || srcName.includes(censusName)) {
    return { score: 80, reason: "name_guess_contains" };
  }

  const stop = new Set(["hotel", "hotels", "the", "and", "member", "design", "boutique", "spa"]);
  const srcTokens = new Set(
    (srcName + " " + srcSlug)
      .split(" ")
      .filter((t) => t.length > 2 && !stop.has(t))
  );
  const censusTokens = censusName.split(" ").filter((t) => t.length > 2 && !stop.has(t));
  let overlap = 0;
  for (const t of censusTokens) if (srcTokens.has(t)) overlap++;
  if (overlap >= 2) return { score: 70, reason: "token_overlap" };
  return { score: 0, reason: "none" };
}

/**
 * @param {import('airtable').Record[]} censusRows CALA census records
 * @param {object[]} sourceProperties from fetchDesignHotelsCalaProperties
 * @param {{ minScore?: number }} [opts]
 */
export function planDesignHotelsAffiliationUpdates(censusRows, sourceProperties, opts = {}) {
  const minScore = opts.minScore ?? 85;
  /** @type {Map<string, object>} */
  const byRecordId = new Map();

  for (const source of sourceProperties) {
    const stewardId = STEWARD_CENSUS_RECORD_BY_SLUG[source.slug];
    if (stewardId) {
      const rec = censusRows.find((r) => r.id === stewardId);
      if (rec) {
        byRecordId.set(stewardId, {
          censusRecordId: stewardId,
          censusName: rec.fields.name,
          censusCountry: rec.fields[CENSUS_FIELDS.country],
          currentAffiliation: rec.fields[CENSUS_FIELDS.affiliation] || "",
          currentParentCompany: rec.fields[CENSUS_FIELDS.parentCompany] || "",
          currentWebsite: rec.fields.Website || "",
          propertyUrl: source.propertyUrl,
          slug: source.slug,
          matchScore: 100,
          matchReason: "steward_record_override",
        });
        continue;
      }
    }

    let best = null;
    for (const rec of censusRows) {
      if (rec.fields[CENSUS_FIELDS.country] !== source.censusCountry) continue;
      const { score, reason } = scoreDesignHotelsCensusMatch(source, rec);
      if (score >= minScore && (!best || score > best.score)) {
        best = { rec, score, reason };
      }
    }
    if (!best) continue;

    const existing = byRecordId.get(best.rec.id);
    if (existing && existing.matchScore >= best.score) continue;

    byRecordId.set(best.rec.id, {
      censusRecordId: best.rec.id,
      censusName: best.rec.fields.name,
      censusCountry: best.rec.fields[CENSUS_FIELDS.country],
      currentAffiliation: best.rec.fields[CENSUS_FIELDS.affiliation] || "",
      currentParentCompany: best.rec.fields[CENSUS_FIELDS.parentCompany] || "",
      currentWebsite: best.rec.fields.Website || "",
      propertyUrl: source.propertyUrl,
      slug: source.slug,
      matchScore: best.score,
      matchReason: best.reason,
    });
  }

  // Census rows already tagged Design Hotels via Affiliation (may be off sitemap)
  for (const rec of censusRows) {
    const affiliation = String(rec.fields[CENSUS_FIELDS.affiliation] || "").trim();
    if (affiliation !== DESIGN_HOTELS_AFFILIATION) continue;
    if (byRecordId.has(rec.id)) continue;
    const name = String(rec.fields.name || "");
    byRecordId.set(rec.id, {
      censusRecordId: rec.id,
      censusName: name,
      censusCountry: rec.fields[CENSUS_FIELDS.country],
      currentAffiliation: rec.fields[CENSUS_FIELDS.affiliation] || "",
      currentParentCompany: rec.fields[CENSUS_FIELDS.parentCompany] || "",
      currentWebsite: rec.fields.Website || "",
      propertyUrl: "",
      slug: "",
      matchScore: 100,
      matchReason: "census_affiliation_design_hotels",
    });
  }

  return [...byRecordId.values()];
}

export function buildDesignHotelsCensusPatch(row, { fillWebsite = true, fillParent = true } = {}) {
  /** @type {Record<string, string>} */
  const fields = {};
  const currentAff = String(row.currentAffiliation || "").trim();
  const protectedAffiliations = new Set([
    "Autograph Collection",
    "Tribute Portfolio",
    "Luxury Collection",
    "W Hotels",
    "Westin",
    "Le Meridien",
  ]);
  if (protectedAffiliations.has(currentAff)) {
    return fields;
  }
  if (currentAff !== DESIGN_HOTELS_AFFILIATION) {
    fields[CENSUS_FIELDS.affiliation] = DESIGN_HOTELS_AFFILIATION;
  }
  if (
    fillParent &&
    !String(row.currentParentCompany || "").trim()
  ) {
    fields[CENSUS_FIELDS.parentCompany] = DESIGN_HOTELS_PARENT_COMPANY;
  }
  if (
    fillWebsite &&
    row.propertyUrl &&
    !String(row.currentWebsite || "").trim()
  ) {
    fields.Website = row.propertyUrl;
  }
  return fields;
}
