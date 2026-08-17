/**
 * Plan + validate SLH CALA Hotel Census creates for directory gaps.
 */

import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "./fields.js";
import { countryToDealalityRegion } from "./region.js";
import { countryToSubContinent } from "./geography-enrichment-contract.js";
import { normalizeNameKey } from "./choice-census-manual-additions.js";
import {
  SLH_AFFILIATION,
  SLH_PARENT_COMPANY,
  fetchSlhCalaProperties,
  planSlhAffiliationUpdates,
  normalizeSlhName,
} from "../slh-census-enrichment.js";
import { CENSUS_DESCRIPTION_FIELD } from "./hilton-description-enrichment-contract.js";

export { HOTEL_CENSUS_TABLE, normalizeNameKey };

export const MAP_SLH_CENSUS_MANUAL = {
  name: CENSUS_FIELDS.name,
  affiliation: CENSUS_FIELDS.affiliation,
  parentCompany: CENSUS_FIELDS.parentCompany,
  status: CENSUS_FIELDS.status,
  city: CENSUS_FIELDS.city,
  country: CENSUS_FIELDS.country,
  region: CENSUS_FIELDS.region,
  subContinent: CENSUS_FIELDS.subContinent,
  market: CENSUS_FIELDS.market,
  submarket: CENSUS_FIELDS.submarket,
  chainScale: CENSUS_FIELDS.chainScale,
  operationType: CENSUS_FIELDS.operationType,
  projectPhase: CENSUS_FIELDS.projectPhase,
  location: CENSUS_FIELDS.location,
  website: "Website",
  latitude: "Latitude",
  longitude: "Longitude",
  hotelDescription: CENSUS_DESCRIPTION_FIELD,
};

/** Steward false-negatives / creates forced even if a weak match existed. */
export const SLH_FORCE_CREATE_SLUGS = new Set([
  "la-valise-san-miguel-de-allende",
  "hotel-de-la-soledad",
]);

function cleanCity(city, country) {
  const raw = String(city || "").trim();
  if (!raw) return "";
  // Bad parse artifacts like "NiCaribbean"
  if (/caribbean|america|latin/i.test(raw) && !/[a-z]{3,}/i.test(raw.replace(/caribbean/i, ""))) {
    return "";
  }
  if (normalizeNameKey(raw) === normalizeNameKey(country)) return "";
  return raw;
}

/**
 * @param {ReturnType<import("../slh-census-enrichment.js").normalizeSlhHotelItem>} source
 */
export function slhSourceToManualRow(source) {
  const city = cleanCity(source.city, source.censusCountry);
  const lat = Number(source.lat);
  const lng = Number(source.lng);
  return {
    portfolioKey: source.slug || source.id,
    name: source.title,
    city: city || source.censusCountry,
    country: source.censusCountry,
    website: source.propertyUrl,
    market: city || undefined,
    submarket: city || undefined,
    latitude: Number.isFinite(lat) ? lat : undefined,
    longitude: Number.isFinite(lng) ? lng : undefined,
    description: source.description || "",
    keyFeatures: source.keyFeatures || [],
    sourceId: source.id,
    notes: "SLH CALA directory gap create from slh.com hotelsearchresults API.",
  };
}

/**
 * @param {object} row
 */
export function rowToAirtableFields(row) {
  const F = MAP_SLH_CENSUS_MANUAL;
  /** @type {Record<string, unknown>} */
  const fields = {
    [F.name]: row.name,
    [F.city]: row.city,
    [F.country]: row.country,
    [F.region]: countryToDealalityRegion(row.country),
    [F.subContinent]: countryToSubContinent(row.country),
    [F.affiliation]: SLH_AFFILIATION,
    [F.parentCompany]: SLH_PARENT_COMPANY,
    [F.status]: ["Open"],
    [F.projectPhase]: "Open",
    [F.website]: row.website,
    [F.operationType]: "Independent",
    [F.chainScale]: "Independent",
  };
  if (row.market) fields[F.market] = row.market;
  if (row.submarket) fields[F.submarket] = row.submarket;
  if (row.location) fields[F.location] = row.location;
  if (Number.isFinite(row.latitude)) fields[F.latitude] = row.latitude;
  if (Number.isFinite(row.longitude)) fields[F.longitude] = row.longitude;
  if (row.description) fields[F.hotelDescription] = String(row.description).slice(0, 2000);
  return fields;
}

/**
 * @param {object} row
 */
export function validateSlhCensusManualRow(row) {
  const errors = [];
  if (!row.portfolioKey?.trim()) errors.push("portfolioKey required");
  if (!row.name?.trim()) errors.push("name required");
  if (!row.city?.trim()) errors.push("city required");
  if (!row.country?.trim()) errors.push("country required");
  if (!row.website?.trim()) errors.push("website required");
  if (!/slh\.com\/hotels\//i.test(row.website)) {
    errors.push("website must be an slh.com property URL");
  }
  return { pass: errors.length === 0, errors };
}

function websiteSlug(url) {
  try {
    const path = new URL(url).pathname.replace(/\/+$/, "");
    return path.split("/").filter(Boolean).pop() || "";
  } catch {
    return "";
  }
}

/**
 * @param {import('airtable').Records<any>} records
 * @param {object} row
 */
export function findDuplicateCandidates(records, row) {
  const nameKey = normalizeNameKey(row.name);
  const countryKey = normalizeNameKey(row.country);
  const slug = websiteSlug(row.website).toLowerCase();

  return records.filter((rec) => {
    const f = rec.fields;
    const recWebsite = String(f.Website ?? "").toLowerCase();
    if (slug && recWebsite.includes(slug)) return true;
    if (normalizeNameKey(f.name) === nameKey && normalizeNameKey(f.country) === countryKey) {
      return true;
    }
    // Normalized soft-name equality (accents stripped via normalizeNameKey already)
    if (
      normalizeSlhName(f.name) === normalizeSlhName(row.name) &&
      normalizeNameKey(f.country) === countryKey
    ) {
      return true;
    }
    return false;
  });
}

/**
 * Build create plan for SLH CALA hotels missing from census.
 * @param {import('airtable').Record[]} censusRows
 * @param {{ onProgress?: (msg: string) => void }} [opts]
 */
export async function planSlhCensusManualAdditions(censusRows, opts = {}) {
  const properties = await fetchSlhCalaProperties({ onProgress: opts.onProgress });
  const planned = planSlhAffiliationUpdates(censusRows, properties, { minScore: 80 });

  const matchedSlugs = new Set(planned.matches.map((m) => m.slug));
  const stewardFalsePositiveSlugs = new Set(
    planned.stewardReview
      .filter((r) => r.reviewReason === "below_auto_apply_threshold")
      .map((r) => r.slug)
  );

  /** @type {object[]} */
  const createRows = [];
  /** @type {object[]} */
  const skippedMatched = [];

  for (const source of properties) {
    if (matchedSlugs.has(source.slug) && !SLH_FORCE_CREATE_SLUGS.has(source.slug)) {
      skippedMatched.push({ slug: source.slug, title: source.title, reason: "already_matched" });
      continue;
    }
    // Force-create steward false matches + unmatched + force list
    const force = SLH_FORCE_CREATE_SLUGS.has(source.slug) || stewardFalsePositiveSlugs.has(source.slug);
    const unmatched = planned.unmatchedSources.some((u) => u.slug === source.slug);
    if (!force && !unmatched) continue;

    const row = slhSourceToManualRow(source);
    const v = validateSlhCensusManualRow(row);
    if (!v.pass) {
      skippedMatched.push({ slug: source.slug, title: source.title, reason: "validation_failed", errors: v.errors });
      continue;
    }
    createRows.push(row);
  }

  // Ambiguous exact-name duplicates: update Affiliation on all census dupes instead of create
  const duplicateAffiliationUpdates = planned.stewardReview.filter(
    (r) => r.reviewReason === "ambiguous_match_margin"
  );

  return {
    generatedAt: new Date().toISOString(),
    slhCalaProperties: properties.length,
    createRows,
    skippedMatched,
    duplicateAffiliationUpdates,
    stewardReview: planned.stewardReview,
    propertiesBySlug: Object.fromEntries(properties.map((p) => [p.slug, p])),
  };
}
