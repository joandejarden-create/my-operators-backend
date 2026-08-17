/**
 * Plan Design Hotels CALA census content backfill from designhotels.com overview pages.
 */

import {
  fetchDesignHotelsCalaProperties,
  DESIGN_HOTELS_AFFILIATION,
  isCalaCountry,
  scoreDesignHotelsCensusMatch,
} from "../design-hotels-census-enrichment.js";
import {
  fetchDesignHotelsHotelContent,
  designHotelsOverviewUrl,
} from "../design-hotels-hotel-content-fetch.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import {
  buildDesignHotelsContentPatch,
  designHotelsRowNeedsContent,
  MAP_DESIGN_HOTELS_CENSUS_CONTENT,
} from "./design-hotels-census-content-contract.js";
import { isBlankCensusValue } from "./brand-directory-enrichment-contract.js";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {import('airtable').Record} record
 * @param {object[]} sitemapProps
 */
function pickWebsite(record, sitemapProps) {
  const website = String(record.fields.Website || "").trim();
  if (website.includes("designhotels.com/hotels/")) return website;

  let best = null;
  for (const source of sitemapProps) {
    if (source.censusCountry !== record.fields[CENSUS_FIELDS.country]) continue;
    const { score } = scoreDesignHotelsCensusMatch(source, record);
    if (score >= 85 && (!best || score > best.score)) best = { score, url: source.propertyUrl };
  }
  if (best) return best.url;

  // Name-only fallback for census rows missing Website
  const nameKey = String(record.fields.name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ");
  for (const source of sitemapProps) {
    if (source.censusCountry !== record.fields[CENSUS_FIELDS.country]) continue;
    const slugKey = source.slug.replace(/-/g, " ");
    if (nameKey.includes(slugKey) || slugKey.split(" ").every((t) => t.length > 2 && nameKey.includes(t))) {
      return source.propertyUrl;
    }
  }

  return "";
}

/**
 * @param {object} [opts]
 * @param {string[]} [opts.recordIds]
 * @param {number} [opts.limit]
 * @param {number} [opts.fetchDelayMs]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function planDesignHotelsCensusContentBackfill(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of Object.values(MAP_DESIGN_HOTELS_CENSUS_CONTENT)) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }
  if (!selectFields.includes("Website")) selectFields.push("Website");

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: selectFields, pageSize: 100 })
    .all();

  let targets = records.filter(
    (r) =>
      isCalaCountry(r.fields[CENSUS_FIELDS.country]) &&
      r.fields[CENSUS_FIELDS.affiliation] === DESIGN_HOTELS_AFFILIATION &&
      designHotelsRowNeedsContent(r.fields)
  );

  if (opts.recordIds?.length) {
    const want = new Set(opts.recordIds);
    targets = targets.filter((r) => want.has(r.id));
  }
  if (opts.limit > 0) targets = targets.slice(0, opts.limit);

  const sitemapProps = await fetchDesignHotelsCalaProperties();

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];
  /** @type {object[]} */
  const fetchErrors = [];

  for (const rec of targets) {
    const website = pickWebsite(rec, sitemapProps);
    const overviewUrl = designHotelsOverviewUrl(website);
    if (!overviewUrl) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        skipReason: "missing_design_hotels_url",
      });
      continue;
    }

    opts.onProgress?.(`Fetch ${rec.fields.name}…`);
    const content = await fetchDesignHotelsHotelContent(overviewUrl, {
      fetchDelayMs: opts.fetchDelayMs,
    });

    if (content.accessDenied) {
      fetchErrors.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        overviewUrl,
        errors: content.errors,
      });
      continue;
    }

    const applyFields = buildDesignHotelsContentPatch(rec.fields, content, selectFields);
    if (!Object.keys(applyFields).length) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        skipReason: "no_fill_blank_fields",
        overviewUrl,
        parsed: {
          descriptionLen: content.description.length,
          amenitiesCount: content.amenities.length,
          rooms: content.rooms,
        },
      });
      continue;
    }

    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      overviewUrl,
      marshaCode: content.marshaCode,
      marriottUrl: content.marriottUrl,
      descriptionSuggested: content.description,
      amenitiesTextSuggested: content.amenitiesText,
      roomsSuggested: content.rooms,
      applyFields,
      fetchErrors: content.errors,
    });

    if (opts.fetchDelayMs > 0) await sleep(opts.fetchDelayMs);
  }

  return {
    generatedAt: new Date().toISOString(),
    censusRowsScanned: targets.length,
    readyToApply: planRows.length,
    skipped,
    fetchErrors,
    planRows,
  };
}

/**
 * Audit blank content fields for CALA Design Hotels.
 */
export async function auditDesignHotelsCensusContentBlanks() {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    CENSUS_FIELDS.name,
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.affiliation,
    ...Object.values(MAP_DESIGN_HOTELS_CENSUS_CONTENT),
    "Website",
  ];

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields, pageSize: 100 })
    .all();

  const rows = records.filter(
    (r) =>
      isCalaCountry(r.fields[CENSUS_FIELDS.country]) &&
      r.fields[CENSUS_FIELDS.affiliation] === DESIGN_HOTELS_AFFILIATION
  );

  /** @type {Record<string, number>} */
  const blankCounts = {};
  for (const f of Object.values(MAP_DESIGN_HOTELS_CENSUS_CONTENT)) {
    blankCounts[f] = rows.filter((r) => isBlankCensusValue(r.fields[f])).length;
  }

  return {
    total: rows.length,
    needsContent: rows.filter((r) => designHotelsRowNeedsContent(r.fields)).length,
    blankCounts,
  };
}
